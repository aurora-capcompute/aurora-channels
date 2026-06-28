package bridge

import (
	"database/sql"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type BridgeStore struct {
	db *sql.DB
}

func OpenStore(path string) (*BridgeStore, error) {
	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS task_tokens (
	task_id       TEXT PRIMARY KEY,
	chat_id       INTEGER NOT NULL,
	message_id    INTEGER NOT NULL,
	webhook_token TEXT NOT NULL,
	created_at    TEXT NOT NULL
)`); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &BridgeStore{db: db}, nil
}

func (s *BridgeStore) Close() error {
	return s.db.Close()
}

func (s *BridgeStore) SaveTaskToken(taskID string, chatID int64, messageID int, webhookToken string) error {
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO task_tokens (task_id, chat_id, message_id, webhook_token, created_at) VALUES (?, ?, ?, ?, ?)`,
		taskID, chatID, messageID, webhookToken, time.Now().UTC().Format(time.RFC3339),
	)
	return err
}

type TaskToken struct {
	TaskID       string
	ChatID       int64
	MessageID    int
	WebhookToken string
}

func (s *BridgeStore) GetTaskToken(taskID string) (TaskToken, bool, error) {
	var t TaskToken
	err := s.db.QueryRow(
		`SELECT task_id, chat_id, message_id, webhook_token FROM task_tokens WHERE task_id=?`,
		taskID,
	).Scan(&t.TaskID, &t.ChatID, &t.MessageID, &t.WebhookToken)
	if err == sql.ErrNoRows {
		return TaskToken{}, false, nil
	}
	return t, err == nil, err
}
