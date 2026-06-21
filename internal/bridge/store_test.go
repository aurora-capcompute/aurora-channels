package bridge

import "testing"

func TestStoreThreadMapping(t *testing.T) {
	store, err := OpenStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	_, found, err := store.ThreadForChat(42)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected no thread for unknown chat")
	}

	if err := store.SaveChatThread(42, "thr_1"); err != nil {
		t.Fatal(err)
	}

	threadID, found, err := store.ThreadForChat(42)
	if err != nil {
		t.Fatal(err)
	}
	if !found || threadID != "thr_1" {
		t.Fatalf("got threadID=%q found=%v", threadID, found)
	}
}

func TestStoreTaskToken(t *testing.T) {
	store, err := OpenStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	_, found, err := store.GetTaskToken("task_1")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected no token for unknown task")
	}

	if err := store.SaveTaskToken("task_1", 42, 100, "secret-token"); err != nil {
		t.Fatal(err)
	}

	token, found, err := store.GetTaskToken("task_1")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected token to be found")
	}
	if token.TaskID != "task_1" || token.ChatID != 42 || token.MessageID != 100 || token.WebhookToken != "secret-token" {
		t.Fatalf("got %+v", token)
	}
}
