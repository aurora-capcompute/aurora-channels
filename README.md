# aurora-channels

Communication channel integrations for Aurora.

## HTTP Server

The REST/SSE server that `aurora-ui` connects to:

```sh
sh ../aurora-brains/agent/build.sh
AURORA_LLM=openai go run ./cmd/aurora-http
```

Start the UI alongside:

```sh
cd ../aurora-ui
npm start
```

Configuration:

- `AURORA_SERVER_ADDR`: listen address, default `127.0.0.1:8080`.
- `AURORA_LLM`: `fake` or `openai`, default `fake`.
- `AURORA_DB`: Aurora SQLite path, default `aurora.db`.
- `AURORA_GUEST_WASM`: Wasm brain path, default `../aurora-brains/agent/agent.wasm`.
- `AURORA_MCP_SERVERS`: optional JSON object of MCP server configs.
- `AURORA_BRAINS`: optional JSON object mapping brain IDs to Wasm paths.
- `AURORA_WEBHOOK_SECRET`: HMAC secret for task tokens.

## Telegram

```sh
sh ../aurora-brains/agent/build.sh
TELEGRAM_BOT_TOKEN=<token> AURORA_LLM=fake go run ./cmd/aurora-telegram
```

Optionally start the HTTP server alongside for the debug UI:

```sh
TELEGRAM_BOT_TOKEN=<token> AURORA_LLM=openai AURORA_SERVER_ADDR=127.0.0.1:8080 go run ./cmd/aurora-telegram
```

Configuration:

- `TELEGRAM_BOT_TOKEN` (required): Telegram Bot API token.
- `AURORA_LLM`: `fake` or `openai`, default `fake`.
- `AURORA_DB`: Aurora SQLite path, default `aurora.db`.
- `AURORA_TELEGRAM_DB`: bridge persistence, default `aurora-telegram.db`.
- `AURORA_GUEST_WASM`: Wasm brain path, default `../aurora-brains/agent/agent.wasm`.
- `AURORA_THREAD_MANIFEST`: JSON manifest for new threads.
- `AURORA_SERVER_ADDR`: if set, also start HTTP server for the debug UI.
- `AURORA_MCP_SERVERS`: optional JSON object of MCP server configs.
- `AURORA_BRAINS`: optional JSON object mapping brain IDs to Wasm paths.
- `AURORA_WEBHOOK_SECRET`: HMAC secret for task tokens.
