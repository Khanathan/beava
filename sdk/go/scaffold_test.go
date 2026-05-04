package beava_test

import (
	"context"
	"testing"

	beava "github.com/beava-dev/beava/sdk/go"
)

func TestNewAppStoresURL(t *testing.T) {
	app, err := beava.NewApp(context.Background(), "http://127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewApp: %v", err)
	}
	if got, want := app.URL(), "http://127.0.0.1:0"; got != want {
		t.Fatalf("URL: got %q, want %q", got, want)
	}
}
