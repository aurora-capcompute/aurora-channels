package httpserver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-channels/internal/assembly"
	"github.com/aurora-capcompute/aurora-dispatchers-llm/openaillm"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"
	"github.com/aurora-capcompute/aurora-stores/memory"
)

func TestRESTAndSSELifecycle(t *testing.T) {
	llmServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{
				"message": map[string]string{
					"content": `[{"action":"final","content":{"answer":"server answer"}}]`,
				},
			}},
		})
	}))
	t.Cleanup(llmServer.Close)

	brains, err := assembly.SingleBrainProvider(buildGuest(t))
	if err != nil {
		t.Fatalf("brain provider: %v", err)
	}
	dispatchers := assembly.NewDispatcherProvider(
		registry.Services{},
		openaillm.Registration{},
	)
	store := memory.NewStore()
	runtime, err := aurora.NewRuntime(context.Background(), aurora.Config{
		Brains:       brains,
		Dispatchers:  dispatchers,
		StateStore:   store,
		TaskStore:    store,
		SessionStore: memory.NewSessionStore[string, aurora.RunContext](),
		TaskSecret:   []byte("test-task-secret"),
		IDSource:     sequentialIDs(),
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	httpServer := httptest.NewServer(New(runtime).Handler())
	defer httpServer.Close()

	thread := requestJSON[aurora.ThreadSnapshot](t, http.MethodPost, httpServer.URL+"/v1/threads",
		map[string]any{"manifest": map[string]any{"version": 2, "tools": []any{
			map[string]any{"name": "llm", "type": "core.openaiApi", "hidden": true, "settings": map[string]any{
				"base_url": llmServer.URL, "api_key_env": "OPENAI_API_KEY", "api_key_optional": true,
				"default_model": "test", "require_approval": false,
			}},
		}}},
		http.StatusCreated)
	if thread.ID == "" {
		t.Fatal("thread id is empty")
	}
	brainList := requestJSON[struct {
		Brains []aurora.BrainArtifact `json:"brains"`
	}](t, http.MethodGet, httpServer.URL+"/v1/brains", nil, http.StatusOK)
	if len(brainList.Brains) != 1 || brainList.Brains[0].Digest == "" {
		t.Fatalf("brains = %+v", brainList.Brains)
	}

	response, err := http.Get(httpServer.URL + "/v1/threads/" + thread.ID + "/events")
	if err != nil {
		t.Fatalf("open events: %v", err)
	}
	reader := bufio.NewReader(response.Body)
	eventLine, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read event line: %v", err)
	}
	if strings.TrimSpace(eventLine) != "event: snapshot" {
		t.Fatalf("event line = %q, want snapshot", eventLine)
	}
	_ = response.Body.Close()

	run := requestJSON[aurora.RunSnapshot](t, http.MethodPost,
		httpServer.URL+"/v1/threads/"+thread.ID+"/messages",
		map[string]string{"content": "hello"},
		http.StatusAccepted,
	)
	completed := waitForRun(t, httpServer.URL, run.ID)
	if completed.Answer != "server answer" {
		t.Fatalf("answer = %q", completed.Answer)
	}

	journal := requestJSON[struct {
		Entries []aurora.JournalEntry `json:"entries"`
	}](t, http.MethodGet, httpServer.URL+"/v1/runs/"+run.ID+"/journal", nil, http.StatusOK)
	if len(journal.Entries) != 1 || journal.Entries[0].Call.Name != "llm.chat" {
		t.Fatalf("journal = %+v", journal.Entries)
	}
	tasks := requestJSON[struct {
		Tasks []aurora.TaskSnapshot `json:"tasks"`
	}](t, http.MethodGet, httpServer.URL+"/v1/runs/"+run.ID+"/tasks", nil, http.StatusOK)
	if len(tasks.Tasks) != 0 {
		t.Fatalf("tasks = %+v", tasks.Tasks)
	}

	gotThread := requestJSON[aurora.ThreadSnapshot](t, http.MethodGet,
		httpServer.URL+"/v1/threads/"+thread.ID, nil, http.StatusOK)
	if len(gotThread.History) != 2 {
		t.Fatalf("history length = %d, want 2", len(gotThread.History))
	}
}

func requestJSON[T any](t *testing.T, method string, target string, body any, wantStatus int) T {
	t.Helper()
	var requestBody bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&requestBody).Encode(body); err != nil {
			t.Fatalf("encode request: %v", err)
		}
	}
	request, err := http.NewRequest(method, target, &requestBody)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != wantStatus {
		var failure any
		_ = json.NewDecoder(response.Body).Decode(&failure)
		t.Fatalf("status = %d, want %d; body=%v", response.StatusCode, wantStatus, failure)
	}
	var value T
	if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return value
}

func waitForRun(t *testing.T, baseURL string, runID string) aurora.RunSnapshot {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		run := requestJSON[aurora.RunSnapshot](t, http.MethodGet, baseURL+"/v1/runs/"+runID, nil, http.StatusOK)
		if run.Status == aurora.RunCompleted {
			return run
		}
		if run.Status == aurora.RunFailed {
			t.Fatalf("run failed: %s", run.Error)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("run did not complete")
	return aurora.RunSnapshot{}
}

func sequentialIDs() func(string) (string, error) {
	var next atomic.Int32
	return func(prefix string) (string, error) {
		return fmt.Sprintf("%s%d", prefix, next.Add(1)), nil
	}
}

func buildGuest(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("tinygo"); err != nil {
		t.Skip("tinygo not found")
	}
	wasmPath := filepath.Join(t.TempDir(), "agent.wasm")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx,
		"tinygo", "build",
		"-target", "wasip1",
		"-buildmode=c-shared",
		"-tags", "tinygo",
		"-o", wasmPath,
		"./agent",
	)
	cmd.Dir = "../../../aurora-brains"
	cmd.Env = append(os.Environ(),
		"XDG_CACHE_HOME="+t.TempDir(),
		"GOCACHE="+filepath.Join(t.TempDir(), "go-build"),
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build guest: %v\n%s", err, strings.TrimSpace(string(out)))
	}
	return wasmPath
}
