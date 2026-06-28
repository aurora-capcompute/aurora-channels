package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-channels/internal/telegram"
)

type mockRuntime struct {
	mu      sync.Mutex
	threads map[string]aurora.ThreadSnapshot
	nextID  int

	onCreateRun     func(threadID, message string) (aurora.RunSnapshot, error)
	ListThreadsFunc func() []aurora.ThreadSummary
	subscribers     map[string][]chan aurora.Event
}

func newMockRuntime() *mockRuntime {
	return &mockRuntime{
		threads:     make(map[string]aurora.ThreadSnapshot),
		subscribers: make(map[string][]chan aurora.Event),
	}
}

func (m *mockRuntime) CreateThread(manifest aurora.Manifest, tags map[string]string) (aurora.ThreadSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	id := fmt.Sprintf("thr_%d", m.nextID)
	snap := aurora.ThreadSnapshot{
		ThreadSummary: aurora.ThreadSummary{ID: id, Manifest: manifest, Tags: tags},
	}
	m.threads[id] = snap
	return snap, nil
}

func (m *mockRuntime) CreateRun(threadID string, message string, overrides []aurora.CapabilityConfig) (aurora.RunSnapshot, error) {
	if m.onCreateRun != nil {
		return m.onCreateRun(threadID, message)
	}
	m.mu.Lock()
	m.nextID++
	runID := fmt.Sprintf("run_%d", m.nextID)
	m.mu.Unlock()
	return aurora.RunSnapshot{ID: runID, ThreadID: threadID, Message: message, Status: aurora.RunQueued}, nil
}

func (m *mockRuntime) Subscribe(threadID string) (aurora.Event, <-chan aurora.Event, func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan aurora.Event, 32)
	m.subscribers[threadID] = append(m.subscribers[threadID], ch)
	snap := aurora.Event{Type: "snapshot", Data: m.threads[threadID]}
	return snap, ch, func() {}, nil
}

func (m *mockRuntime) Emit(threadID string, event aurora.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ch := range m.subscribers[threadID] {
		ch <- event
	}
}

func (m *mockRuntime) ListThreads() []aurora.ThreadSummary {
	if m.ListThreadsFunc != nil {
		return m.ListThreadsFunc()
	}
	return nil
}
func (m *mockRuntime) Brains() []aurora.BrainArtifact                              { return nil }
func (m *mockRuntime) SetBrains(context.Context, []aurora.BrainSource) error       { return nil }
func (m *mockRuntime) GetThread(string) (aurora.ThreadSnapshot, error)              { return aurora.ThreadSnapshot{}, nil }
func (m *mockRuntime) GetRun(string) (aurora.RunSnapshot, error)                   { return aurora.RunSnapshot{}, nil }
func (m *mockRuntime) Journal(string) ([]aurora.JournalEntry, error)               { return nil, nil }
func (m *mockRuntime) CallGraph(string) (aurora.RunGraphNode, error)               { return aurora.RunGraphNode{}, nil }
func (m *mockRuntime) ThreadGraph(string) (aurora.ThreadGraph, error)              { return aurora.ThreadGraph{}, nil }
func (m *mockRuntime) Tasks(string) ([]aurora.TaskSnapshot, error)                 { return nil, nil }
func (m *mockRuntime) ResolveTask(string, string, aurora.Resolution) (aurora.TaskSnapshot, error) {
	return aurora.TaskSnapshot{}, nil
}
func (m *mockRuntime) Stop(string) (aurora.RunSnapshot, error) { return aurora.RunSnapshot{}, nil }
func (m *mockRuntime) Retry(string, aurora.RetryMode, []aurora.CapabilityConfig) (aurora.RunSnapshot, error) {
	return aurora.RunSnapshot{}, nil
}
func (m *mockRuntime) Close(context.Context) error { return nil }

type fakeTelegram struct {
	mu       sync.Mutex
	messages []sentMessage
	server   *httptest.Server
}

type sentMessage struct {
	ChatID int64
	Text   string
}

func newFakeTelegram() *fakeTelegram {
	ft := &fakeTelegram{}
	mux := http.NewServeMux()
	mux.HandleFunc("/botTEST_TOKEN/getUpdates", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": []any{}})
	})
	mux.HandleFunc("/botTEST_TOKEN/sendMessage", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ChatID int64  `json:"chat_id"`
			Text   string `json:"text"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		ft.mu.Lock()
		ft.messages = append(ft.messages, sentMessage{ChatID: body.ChatID, Text: body.Text})
		ft.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{
			"ok": true,
			"result": map[string]any{
				"message_id": 1,
				"chat":       map[string]any{"id": body.ChatID},
				"text":       body.Text,
			},
		})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": true})
	})
	ft.server = httptest.NewServer(mux)
	return ft
}

func (ft *fakeTelegram) Messages() []sentMessage {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	return append([]sentMessage(nil), ft.messages...)
}

func TestBridgeCompletedRunSendsAnswer(t *testing.T) {
	rt := newMockRuntime()
	ft := newFakeTelegram()
	defer ft.server.Close()

	bot := &telegram.Bot{}
	*bot = *telegram.NewBot("TEST_TOKEN")
	setBotBaseURL(bot, ft.server.URL)

	store, err := OpenStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	b := New(Config{
		Runtime:         rt,
		Bot:             bot,
		Store:           store,
		DefaultManifest: aurora.Manifest{Version: aurora.ManifestVersion},
	})

	msg := &telegram.Message{
		MessageID: 1,
		Chat:      telegram.Chat{ID: 42, Type: "private"},
		Text:      "hello",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.handleMessage(ctx, msg)

	b.mu.Lock()
	threadID, found := b.chatThreads[42]
	b.mu.Unlock()
	if !found {
		t.Fatal("thread not created for chat")
	}

	rt.Emit(threadID, aurora.Event{
		Type: "run.updated",
		Data: aurora.RunSnapshot{
			ID:       "run_1",
			ThreadID: threadID,
			Status:   aurora.RunCompleted,
			Answer:   "Hello back!",
		},
	})

	deadline := time.After(2 * time.Second)
	for {
		msgs := ft.Messages()
		for _, m := range msgs {
			if m.ChatID == 42 && m.Text == "Hello back!" {
				return
			}
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for answer; got messages: %v", ft.Messages())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestBridgeSubscribesBeforeCreateRun(t *testing.T) {
	rt := newMockRuntime()
	ft := newFakeTelegram()
	defer ft.server.Close()

	var subscribedBeforeRun bool
	rt.onCreateRun = func(threadID, message string) (aurora.RunSnapshot, error) {
		rt.mu.Lock()
		subscribedBeforeRun = len(rt.subscribers[threadID]) > 0
		rt.mu.Unlock()
		return aurora.RunSnapshot{ID: "run_1", ThreadID: threadID, Status: aurora.RunQueued}, nil
	}

	bot := telegram.NewBot("TEST_TOKEN")
	setBotBaseURL(bot, ft.server.URL)

	store, err := OpenStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	b := New(Config{
		Runtime:         rt,
		Bot:             bot,
		Store:           store,
		DefaultManifest: aurora.Manifest{Version: aurora.ManifestVersion},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.handleMessage(ctx, &telegram.Message{
		MessageID: 1,
		Chat:      telegram.Chat{ID: 42, Type: "private"},
		Text:      "hello",
	})

	if !subscribedBeforeRun {
		t.Fatal("Subscribe was not called before CreateRun")
	}
}

func TestBridgeNewThreadPerChat(t *testing.T) {
	rt := newMockRuntime()
	ft := newFakeTelegram()
	defer ft.server.Close()

	bot := telegram.NewBot("TEST_TOKEN")
	setBotBaseURL(bot, ft.server.URL)

	store, err := OpenStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	b := New(Config{
		Runtime:         rt,
		Bot:             bot,
		Store:           store,
		DefaultManifest: aurora.Manifest{Version: aurora.ManifestVersion},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Two different chats each get their own thread.
	b.handleMessage(ctx, &telegram.Message{Chat: telegram.Chat{ID: 1, Type: "private"}, Text: "hi"})
	b.handleMessage(ctx, &telegram.Message{Chat: telegram.Chat{ID: 2, Type: "private"}, Text: "hi"})

	b.mu.Lock()
	thr1, ok1 := b.chatThreads[1]
	thr2, ok2 := b.chatThreads[2]
	b.mu.Unlock()

	if !ok1 || !ok2 {
		t.Fatal("expected threads for both chats")
	}
	if thr1 == thr2 {
		t.Fatal("expected distinct threads for distinct chats")
	}

	// Tag must carry the chat binding.
	rt.mu.Lock()
	snap1 := rt.threads[thr1]
	rt.mu.Unlock()
	if snap1.Tags[tagChatID] != "1" {
		t.Fatalf("expected tag %s=1, got %q", tagChatID, snap1.Tags[tagChatID])
	}
}

func TestBridgeRestoresThreadsFromLog(t *testing.T) {
	// Simulate a runtime that already has a thread from a previous run,
	// tagged with a chat ID. New() should seed chatThreads from ListThreads.
	rt := newMockRuntime()
	rt.threads["thr_existing"] = aurora.ThreadSnapshot{
		ThreadSummary: aurora.ThreadSummary{
			ID:   "thr_existing",
			Tags: map[string]string{tagChatID: "99"},
		},
	}
	rt.ListThreadsFunc = func() []aurora.ThreadSummary {
		return []aurora.ThreadSummary{rt.threads["thr_existing"].ThreadSummary}
	}

	ft := newFakeTelegram()
	defer ft.server.Close()
	bot := telegram.NewBot("TEST_TOKEN")
	setBotBaseURL(bot, ft.server.URL)

	store, err := OpenStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	b := New(Config{
		Runtime:         rt,
		Bot:             bot,
		Store:           store,
		DefaultManifest: aurora.Manifest{Version: aurora.ManifestVersion},
	})

	b.mu.Lock()
	threadID, found := b.chatThreads[99]
	b.mu.Unlock()

	if !found || threadID != "thr_existing" {
		t.Fatalf("expected chatThreads[99]=thr_existing, got %q found=%v", threadID, found)
	}
}

func setBotBaseURL(bot *telegram.Bot, url string) {
	bot.SetBaseURL(url)
}
