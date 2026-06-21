package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"aurora-capcompute/aurora"
	"aurora-channels/internal/telegram"
)

type Config struct {
	Runtime         aurora.Runtime
	Bot             *telegram.Bot
	Store           *BridgeStore
	DefaultManifest aurora.Manifest
}

type Bridge struct {
	runtime         aurora.Runtime
	bot             *telegram.Bot
	store           *BridgeStore
	defaultManifest aurora.Manifest

	mu            sync.Mutex
	subscriptions map[string]func()
}

func New(config Config) *Bridge {
	return &Bridge{
		runtime:         config.Runtime,
		bot:             config.Bot,
		store:           config.Store,
		defaultManifest: config.DefaultManifest,
		subscriptions:   make(map[string]func()),
	}
}

func (b *Bridge) Run(ctx context.Context) error {
	var offset int64
	for {
		select {
		case <-ctx.Done():
			b.unsubscribeAll()
			return ctx.Err()
		default:
		}
		updates, err := b.bot.GetUpdates(offset, 30)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("telegram poll: %v", err)
			continue
		}
		for _, update := range updates {
			if update.UpdateID >= offset {
				offset = update.UpdateID + 1
			}
			if update.Message != nil && update.Message.Chat.Type == "private" && update.Message.Text != "" {
				b.handleMessage(ctx, update.Message)
			}
			if update.CallbackQuery != nil {
				b.handleCallback(ctx, update.CallbackQuery)
			}
		}
	}
}

func (b *Bridge) handleMessage(ctx context.Context, msg *telegram.Message) {
	chatID := msg.Chat.ID

	threadID, found, err := b.store.ThreadForChat(chatID)
	if err != nil {
		log.Printf("store lookup chat %d: %v", chatID, err)
		return
	}
	if !found {
		thread, createErr := b.runtime.CreateThread(b.defaultManifest)
		if createErr != nil {
			log.Printf("create thread for chat %d: %v", chatID, createErr)
			b.bot.SendMessage(chatID, "Failed to create conversation.", nil)
			return
		}
		threadID = thread.ID
		if err := b.store.SaveChatThread(chatID, threadID); err != nil {
			log.Printf("save chat-thread mapping: %v", err)
		}
	}

	b.subscribeThread(ctx, chatID, threadID)

	_, err = b.runtime.CreateRun(threadID, msg.Text, nil)
	if err != nil {
		if errors.Is(err, aurora.ErrConflict) {
			b.bot.SendMessage(chatID, "Still working on your previous request.", nil)
			return
		}
		log.Printf("create run for chat %d: %v", chatID, err)
		b.bot.SendMessage(chatID, "Failed to start processing.", nil)
		return
	}
}

func (b *Bridge) subscribeThread(ctx context.Context, chatID int64, threadID string) {
	b.mu.Lock()
	if unsub, exists := b.subscriptions[threadID]; exists {
		unsub()
	}
	b.mu.Unlock()

	_, events, unsubscribe, err := b.runtime.Subscribe(threadID)
	if err != nil {
		log.Printf("subscribe thread %s: %v", threadID, err)
		return
	}

	b.mu.Lock()
	b.subscriptions[threadID] = unsubscribe
	b.mu.Unlock()

	go b.consumeEvents(ctx, chatID, threadID, events, unsubscribe)
}

func (b *Bridge) consumeEvents(ctx context.Context, chatID int64, threadID string, events <-chan aurora.Event, unsubscribe func()) {
	defer func() {
		unsubscribe()
		b.mu.Lock()
		delete(b.subscriptions, threadID)
		b.mu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			b.handleEvent(chatID, event)
			if isTerminalRunEvent(event) {
				return
			}
		}
	}
}

func (b *Bridge) handleEvent(chatID int64, event aurora.Event) {
	switch event.Type {
	case "run.updated":
		b.handleRunUpdated(chatID, event)
	case "task.created":
		b.handleTaskCreated(chatID, event)
	case "task.updated":
		b.handleTaskUpdated(chatID, event)
	}
}

func (b *Bridge) handleRunUpdated(chatID int64, event aurora.Event) {
	raw, err := json.Marshal(event.Data)
	if err != nil {
		return
	}
	var run aurora.RunSnapshot
	if err := json.Unmarshal(raw, &run); err != nil {
		return
	}

	switch aurora.RunStatus(run.Status) {
	case aurora.RunCompleted:
		for _, chunk := range chunkMessage(run.Answer, 4096) {
			b.bot.SendMessage(chatID, chunk, nil)
		}
	case aurora.RunFailed:
		errMsg := run.Error
		if errMsg == "" {
			errMsg = "unknown error"
		}
		b.bot.SendMessage(chatID, fmt.Sprintf("Error: %s", errMsg), nil)
	case aurora.RunStopped:
		b.bot.SendMessage(chatID, "Run was stopped.", nil)
	case aurora.RunInterrupted:
		b.bot.SendMessage(chatID, "Run was interrupted.", nil)
	}
}

func (b *Bridge) handleTaskCreated(chatID int64, event aurora.Event) {
	raw, err := json.Marshal(event.Data)
	if err != nil {
		return
	}
	var task aurora.TaskSnapshot
	if err := json.Unmarshal(raw, &task); err != nil {
		return
	}

	summary := task.Summary
	if summary == "" {
		summary = "A task requires your approval."
	}

	keyboard := &telegram.InlineKeyboardMarkup{
		InlineKeyboard: [][]telegram.InlineKeyboardButton{
			{
				{Text: "Approve", CallbackData: "a:" + task.ID},
				{Text: "Deny", CallbackData: "d:" + task.ID},
			},
		},
	}

	sent, err := b.bot.SendMessage(chatID, summary, keyboard)
	if err != nil {
		log.Printf("send task message: %v", err)
		return
	}

	if err := b.store.SaveTaskToken(task.ID, chatID, sent.MessageID, task.WebhookToken); err != nil {
		log.Printf("save task token: %v", err)
	}
}

func (b *Bridge) handleTaskUpdated(chatID int64, event aurora.Event) {
	raw, err := json.Marshal(event.Data)
	if err != nil {
		return
	}
	var task aurora.TaskSnapshot
	if err := json.Unmarshal(raw, &task); err != nil {
		return
	}

	token, found, err := b.store.GetTaskToken(task.ID)
	if err != nil || !found {
		return
	}

	b.bot.EditMessageReplyMarkup(token.ChatID, token.MessageID, nil)
}

func (b *Bridge) handleCallback(ctx context.Context, query *telegram.CallbackQuery) {
	parts := strings.SplitN(query.Data, ":", 2)
	if len(parts) != 2 {
		b.bot.AnswerCallbackQuery(query.ID, "Invalid action.")
		return
	}
	action, taskID := parts[0], parts[1]

	token, found, err := b.store.GetTaskToken(taskID)
	if err != nil || !found {
		b.bot.AnswerCallbackQuery(query.ID, "Task not found.")
		return
	}

	var decision string
	switch action {
	case "a":
		decision = "approved"
	case "d":
		decision = "denied"
	default:
		b.bot.AnswerCallbackQuery(query.ID, "Unknown action.")
		return
	}

	resolution := aurora.Resolution{
		Decision: aurora.TaskState(decision),
		Actor:    "telegram-bot",
		Reason:   fmt.Sprintf("%s via Telegram", capitalize(decision)),
	}

	_, err = b.runtime.ResolveTask(taskID, token.WebhookToken, resolution)
	if err != nil {
		b.bot.AnswerCallbackQuery(query.ID, "Resolution failed.")
		log.Printf("resolve task %s: %v", taskID, err)
		return
	}

	b.bot.AnswerCallbackQuery(query.ID, capitalize(decision)+".")
	if query.Message != nil {
		b.bot.EditMessageReplyMarkup(query.Message.Chat.ID, query.Message.MessageID, nil)
	}
}

func (b *Bridge) unsubscribeAll() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, unsub := range b.subscriptions {
		unsub()
	}
	b.subscriptions = make(map[string]func())
}

func isTerminalRunEvent(event aurora.Event) bool {
	if event.Type != "run.updated" {
		return false
	}
	raw, err := json.Marshal(event.Data)
	if err != nil {
		return false
	}
	var run struct {
		Status aurora.RunStatus `json:"status"`
	}
	if err := json.Unmarshal(raw, &run); err != nil {
		return false
	}
	switch run.Status {
	case aurora.RunCompleted, aurora.RunFailed, aurora.RunStopped, aurora.RunInterrupted:
		return true
	}
	return false
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

func chunkMessage(text string, limit int) []string {
	if len(text) <= limit {
		return []string{text}
	}
	var chunks []string
	for len(text) > 0 {
		if len(text) <= limit {
			chunks = append(chunks, text)
			break
		}
		cut := limit
		if idx := strings.LastIndex(text[:cut], "\n\n"); idx > limit/4 {
			cut = idx + 2
		} else if idx := strings.LastIndex(text[:cut], "\n"); idx > limit/4 {
			cut = idx + 1
		}
		chunks = append(chunks, text[:cut])
		text = text[cut:]
	}
	return chunks
}
