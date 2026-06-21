package telegram

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Bot struct {
	token   string
	baseURL string
	client  *http.Client
}

func NewBot(token string) *Bot {
	return &Bot{
		token:   token,
		baseURL: "https://api.telegram.org",
		client:  &http.Client{Timeout: 60 * time.Second},
	}
}

func (b *Bot) SetBaseURL(url string) {
	b.baseURL = url
}

func (b *Bot) GetUpdates(offset int64, timeout int) ([]Update, error) {
	body := map[string]any{
		"offset":          offset,
		"timeout":         timeout,
		"allowed_updates": []string{"message", "callback_query"},
	}
	var updates []Update
	if err := b.call("getUpdates", body, &updates); err != nil {
		return nil, err
	}
	return updates, nil
}

func (b *Bot) SendMessage(chatID int64, text string, replyMarkup *InlineKeyboardMarkup) (Message, error) {
	body := map[string]any{
		"chat_id": chatID,
		"text":    text,
	}
	if replyMarkup != nil {
		body["reply_markup"] = replyMarkup
	}
	var msg Message
	if err := b.call("sendMessage", body, &msg); err != nil {
		return Message{}, err
	}
	return msg, nil
}

func (b *Bot) EditMessageReplyMarkup(chatID int64, messageID int, markup *InlineKeyboardMarkup) error {
	body := map[string]any{
		"chat_id":    chatID,
		"message_id": messageID,
	}
	if markup != nil {
		body["reply_markup"] = markup
	}
	return b.call("editMessageReplyMarkup", body, nil)
}

func (b *Bot) AnswerCallbackQuery(callbackQueryID string, text string) error {
	body := map[string]any{
		"callback_query_id": callbackQueryID,
	}
	if text != "" {
		body["text"] = text
	}
	return b.call("answerCallbackQuery", body, nil)
}

func (b *Bot) call(method string, body map[string]any, result any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", method, err)
	}
	url := fmt.Sprintf("%s/bot%s/%s", b.baseURL, b.token, method)
	resp, err := b.client.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("%s: %w", method, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("%s read body: %w", method, err)
	}
	var apiResp apiResponse
	if err := json.Unmarshal(raw, &apiResp); err != nil {
		return fmt.Errorf("%s decode: %w", method, err)
	}
	if !apiResp.OK {
		return fmt.Errorf("%s: %s", method, apiResp.Description)
	}
	if result != nil && len(apiResp.Result) > 0 {
		if err := json.Unmarshal(apiResp.Result, result); err != nil {
			return fmt.Errorf("%s decode result: %w", method, err)
		}
	}
	return nil
}
