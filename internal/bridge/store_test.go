package bridge

import "testing"

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
