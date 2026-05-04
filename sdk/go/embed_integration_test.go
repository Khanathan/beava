package beava_test

import (
	"context"
	"os"
	"testing"

	beava "github.com/beava-dev/beava/sdk/go"
)

// TestEmbedMode_Smoke is opt-in via BEAVA_RUN_EMBED_TESTS=1 — the conformance
// harness in Plan 13.6-07 is the canonical end-to-end gate. Default-skip avoids
// flakiness when the workspace's `target/debug/beava` is in a half-initialized
// state (e.g., stale WAL file lock).
func TestEmbedMode_Smoke(t *testing.T) {
	if os.Getenv("BEAVA_RUN_EMBED_TESTS") != "1" {
		t.Skip("set BEAVA_RUN_EMBED_TESTS=1 to run embed-mode integration tests")
	}
	ctx := context.Background()
	app, err := beava.NewApp(ctx, "", beava.WithTestMode())
	if err != nil {
		t.Fatalf("NewApp: %v", err)
	}
	defer app.Close(ctx)

	ping, err := app.Ping(ctx)
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if ping.ServerVersion == "" {
		t.Errorf("expected non-empty ServerVersion")
	}
}
