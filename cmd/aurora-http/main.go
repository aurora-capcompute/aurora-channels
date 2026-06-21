package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"aurora-capcompute/aurora"
	"aurora-channels/internal/httpserver"
	"aurora-dispatchers/llm"
	"aurora-dispatchers/mcp"
	aurorasqlite "aurora-stores/sqlite"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	llmClient, err := llmFromEnv()
	if err != nil {
		return err
	}
	store, err := aurorasqlite.Open(envDefault("AURORA_DB", "aurora.db"))
	if err != nil {
		return fmt.Errorf("open durable store: %w", err)
	}
	brains, err := brainRegistryFromEnv()
	if err != nil {
		_ = store.Close()
		return err
	}
	mcpServers, err := mcpServersFromEnv()
	if err != nil {
		_ = store.Close()
		return err
	}
	runtime, err := aurora.NewRuntime(ctx, aurora.Config{
		Brains:     brains,
		LLM:        llmClient,
		TenantID:   envDefault("AURORA_TENANT_ID", aurora.DefaultTenantID),
		Store:      store,
		TaskSecret: []byte(envDefault("AURORA_WEBHOOK_SECRET", "aurora-local-development-webhook-secret")),
		MCPServers: mcpServers,
	})
	if err != nil {
		_ = store.Close()
		return fmt.Errorf("create agent runtime: %w", err)
	}

	address := envDefault("AURORA_SERVER_ADDR", "127.0.0.1:8080")
	srv := &http.Server{
		Addr:              address,
		Handler:           httpserver.New(runtime).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	errs := make(chan error, 1)
	go func() {
		log.Printf("Aurora server listening on http://%s", address)
		errs <- srv.ListenAndServe()
	}()

	select {
	case err := <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			_ = runtime.Close(context.Background())
			return err
		}
		return nil
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return httpserver.Shutdown(shutdownCtx, srv, runtime)
	}
}

func mcpServersFromEnv() (map[string]mcp.ServerConfig, error) {
	raw := strings.TrimSpace(os.Getenv("AURORA_MCP_SERVERS"))
	if raw == "" {
		return nil, nil
	}
	var servers map[string]mcp.ServerConfig
	if err := json.Unmarshal([]byte(raw), &servers); err != nil {
		return nil, fmt.Errorf("decode AURORA_MCP_SERVERS: %w", err)
	}
	for id, server := range servers {
		if strings.TrimSpace(server.ID) == "" {
			server.ID = id
		}
		servers[id] = server
	}
	return servers, nil
}

func brainRegistryFromEnv() (*aurora.BrainRegistry, error) {
	raw := strings.TrimSpace(os.Getenv("AURORA_BRAINS"))
	if raw == "" {
		return aurora.SingleBrainRegistry(
			envDefault("AURORA_GUEST_WASM", "../aurora-brains/agent/agent.wasm"),
		)
	}
	var paths map[string]string
	if err := json.Unmarshal([]byte(raw), &paths); err != nil {
		return nil, fmt.Errorf("decode AURORA_BRAINS: %w", err)
	}
	return aurora.NewBrainRegistry(envDefault("AURORA_DEFAULT_BRAIN", aurora.DefaultBrainID), paths)
}

func llmFromEnv() (llm.Client, error) {
	switch strings.ToLower(envDefault("AURORA_LLM", "fake")) {
	case "fake":
		return llm.NewFakeClient(os.Getenv("AURORA_FAKE_READ_URL")), nil
	case "openai":
		return llm.NewOpenAIClient(llm.OpenAIConfigFromEnv())
	default:
		return nil, fmt.Errorf("unsupported AURORA_LLM: %s", os.Getenv("AURORA_LLM"))
	}
}

func envDefault(name string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
