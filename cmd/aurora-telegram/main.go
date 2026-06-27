package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-channels/internal/assembly"
	"github.com/aurora-capcompute/aurora-channels/internal/bridge"
	"github.com/aurora-capcompute/aurora-channels/internal/httpserver"
	"github.com/aurora-capcompute/aurora-channels/internal/telegram"
	k8s "github.com/aurora-capcompute/aurora-dispatchers-k8s/k8s"
	"github.com/aurora-capcompute/aurora-dispatchers-llm/openaillm"
	"github.com/aurora-capcompute/aurora-dispatchers/mcp"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"
	"github.com/aurora-capcompute/aurora-stores/memory"
	aurorasqlite "github.com/aurora-capcompute/aurora-stores/sqlite"
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

	dispatchers := assembly.NewDispatcherProvider(
		registry.Services{MCPServers: mcpServers},
		openaillm.Registration{},
		registry.InternetRegistration{},
		registry.MCPRegistration{},
		k8s.Registration{},
	)
	rt, err := aurora.NewRuntime(ctx, aurora.Config{
		Brains:       brains,
		Dispatchers:  dispatchers,
		StateStore:   store,
		TaskStore:    store,
		SessionStore: memory.NewSessionStore[string, aurora.RunContext](),
		TenantID:     envOr("AURORA_TENANT_ID", aurora.DefaultTenantID),
		TaskSecret:   []byte(envOr("AURORA_WEBHOOK_SECRET", "aurora-local-development-webhook-secret")),
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

func brainRegistryFromEnv() (aurora.BrainProvider, error) {
	if raw := os.Getenv("AURORA_BRAINS"); raw != "" {
		var paths map[string]string
		if err := json.Unmarshal([]byte(raw), &paths); err != nil {
			return nil, fmt.Errorf("parse AURORA_BRAINS: %w", err)
		}
		return assembly.NewBrainProvider(envOr("AURORA_DEFAULT_BRAIN", aurora.DefaultBrainID), paths)
	}
	wasmPath := envOr("AURORA_GUEST_WASM", "../aurora-brains/agent/agent.wasm")
	return assembly.SingleBrainProvider(wasmPath)
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
