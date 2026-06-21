package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"aurora-capcompute/aurora"
	"aurora-channels/internal/bridge"
	"aurora-channels/internal/httpserver"
	"aurora-channels/internal/telegram"
	"aurora-dispatchers/llm"
	"aurora-dispatchers/mcp"
	"aurora-dispatchers/registry"
	k8s "aurora-k8s/k8s"
	aurorasqlite "aurora-stores/sqlite"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	token := os.Getenv("TELEGRAM_BOT_TOKEN")
	if token == "" {
		return fmt.Errorf("TELEGRAM_BOT_TOKEN is required")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	llmClient, err := llmFromEnv()
	if err != nil {
		return err
	}

	brains, err := brainRegistryFromEnv()
	if err != nil {
		return err
	}

	dbPath := envOr("AURORA_DB", "aurora.db")
	store, err := aurorasqlite.Open(dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer store.Close()

	var mcpServers map[string]mcp.ServerConfig
	if raw := os.Getenv("AURORA_MCP_SERVERS"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &mcpServers); err != nil {
			return fmt.Errorf("parse AURORA_MCP_SERVERS: %w", err)
		}
	}

	rt, err := aurora.NewRuntime(ctx, aurora.Config{
		Brains:     brains,
		LLM:        llmClient,
		TenantID:   envOr("AURORA_TENANT_ID", ""),
		Store:      store,
		TaskSecret: []byte(os.Getenv("AURORA_WEBHOOK_SECRET")),
		MCPServers: mcpServers,
		DispatcherRegistry: registry.New(
			registry.InternetRegistration{},
			registry.MCPRegistration{},
			k8s.Registration{},
		),
	})
	if err != nil {
		return fmt.Errorf("create runtime: %w", err)
	}
	defer rt.Close(context.Background())

	if addr := os.Getenv("AURORA_SERVER_ADDR"); addr != "" {
		srv := httpserver.New(rt)
		httpServer := &http.Server{Addr: addr, Handler: srv.Handler()}
		go func() {
			log.Printf("HTTP server on %s", addr)
			if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
				log.Printf("HTTP server: %v", err)
			}
		}()
		defer httpServer.Shutdown(context.Background())
	}

	telegramDB := envOr("AURORA_TELEGRAM_DB", "aurora-telegram.db")
	bridgeStore, err := bridge.OpenStore(telegramDB)
	if err != nil {
		return fmt.Errorf("open bridge store: %w", err)
	}
	defer bridgeStore.Close()

	manifest := defaultManifest()

	bot := telegram.NewBot(token)
	b := bridge.New(bridge.Config{
		Runtime:         rt,
		Bot:             bot,
		Store:           bridgeStore,
		DefaultManifest: manifest,
	})

	log.Printf("Telegram bot started")
	return b.Run(ctx)
}

func llmFromEnv() (llm.Client, error) {
	switch strings.ToLower(envOr("AURORA_LLM", "fake")) {
	case "fake":
		return llm.NewFakeClient(envOr("AURORA_FAKE_READ_URL", "https://example.com")), nil
	case "openai":
		return llm.NewOpenAIClient(llm.OpenAIConfigFromEnv())
	default:
		return nil, fmt.Errorf("unknown AURORA_LLM value: %s", os.Getenv("AURORA_LLM"))
	}
}

func brainRegistryFromEnv() (*aurora.BrainRegistry, error) {
	if raw := os.Getenv("AURORA_BRAINS"); raw != "" {
		var paths map[string]string
		if err := json.Unmarshal([]byte(raw), &paths); err != nil {
			return nil, fmt.Errorf("parse AURORA_BRAINS: %w", err)
		}
		return aurora.NewBrainRegistry(envOr("AURORA_DEFAULT_BRAIN", ""), paths)
	}
	wasmPath := envOr("AURORA_GUEST_WASM", "../aurora-brains/agent/agent.wasm")
	return aurora.SingleBrainRegistry(wasmPath)
}

func defaultManifest() aurora.Manifest {
	if raw := os.Getenv("AURORA_THREAD_MANIFEST"); raw != "" {
		var m aurora.Manifest
		if err := json.Unmarshal([]byte(raw), &m); err == nil {
			return m
		}
	}
	return aurora.Manifest{Version: aurora.ManifestVersion}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
