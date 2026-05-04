package beava

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// Register submits a batch of pre-compiled JSON descriptors. force / dry_run flags
// are wire-level (snake_case in the body); use WithForce / WithDryRun.
func (a *App) Register(ctx context.Context, descriptors []Descriptor, opts ...RegisterOption) (*RegisterResult, error) {
	cfg := registerConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	t, err := a.ensureReady(ctx)
	if err != nil {
		return nil, err
	}
	body := map[string]any{
		"nodes":   descriptors,
		"force":   cfg.force,
		"dry_run": cfg.dryRun,
	}
	raw, err := t.Request(ctx, http.MethodPost, "/register", body)
	if err != nil {
		return nil, err
	}
	var out RegisterResult
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("beava: parse register response: %w", err)
	}
	return &out, nil
}

// Push fire-and-forget pushes an event (acks=1).
func (a *App) Push(ctx context.Context, eventName string, fields map[string]any) (*PushResult, error) {
	t, err := a.ensureReady(ctx)
	if err != nil {
		return nil, err
	}
	body := map[string]any{"fields": fields}
	raw, err := t.Request(ctx, http.MethodPost, "/push/"+url.PathEscape(eventName), body)
	if err != nil {
		return nil, err
	}
	var out PushResult
	if len(raw) == 0 {
		return &out, nil
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("beava: parse push response: %w", err)
	}
	return &out, nil
}

// PushSync — durable-push semantics (acks=all). v0 delegates to Push because
// OpPushSync is RESERVED per docs/wire-spec.md.
func (a *App) PushSync(ctx context.Context, eventName string, fields map[string]any) (*PushResult, error) {
	return a.Push(ctx, eventName, fields)
}

// Get returns the per-entity feature row. Cold-start returns an empty map
// (not nil and not an error).
func (a *App) Get(ctx context.Context, table string, key any) (FeatureResult, error) {
	return a.getInternal(ctx, table, key)
}

// GetGlobal returns the global-table row (key="") per ADR-003. The wire body
// is identical to Get(table, ""); Go provides a separate method (rather than
// arity overloading) to honor static-typing convention.
func (a *App) GetGlobal(ctx context.Context, table string) (FeatureResult, error) {
	return a.getInternal(ctx, table, "")
}

func (a *App) getInternal(ctx context.Context, table string, key any) (FeatureResult, error) {
	t, err := a.ensureReady(ctx)
	if err != nil {
		return nil, err
	}
	body := map[string]any{"table": table, "key": key}
	raw, err := t.Request(ctx, http.MethodPost, "/get", body)
	if err != nil {
		return nil, err
	}
	out := FeatureResult{}
	if len(raw) == 0 || string(raw) == "{}" {
		return out, nil
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("beava: parse get response: %w", err)
	}
	if out == nil {
		out = FeatureResult{}
	}
	return out, nil
}

// BatchGet performs multiple feature lookups in one round-trip. v0 has no
// partial success — any bad entry rejects the whole batch with *RegistrationError.
func (a *App) BatchGet(ctx context.Context, requests []GetRequest) ([]FeatureResult, error) {
	t, err := a.ensureReady(ctx)
	if err != nil {
		return nil, err
	}
	raw, err := t.Request(ctx, http.MethodPost, "/batch-get", map[string]any{"requests": requests})
	if err != nil {
		return nil, err
	}
	// Server may return either a top-level array or {"results": [...]}; handle both.
	var topArr []FeatureResult
	if err := json.Unmarshal(raw, &topArr); err == nil && topArr != nil {
		return topArr, nil
	}
	var wrapper struct {
		Results []FeatureResult `json:"results"`
	}
	if err := json.Unmarshal(raw, &wrapper); err != nil {
		return nil, fmt.Errorf("beava: parse batch-get response: %w", err)
	}
	return wrapper.Results, nil
}

// Reset clears all server state. Server returns 403 unless test_mode is enabled
// per Phase 13.4 D-03; the structured error surfaces verbatim.
func (a *App) Reset(ctx context.Context) error {
	t, err := a.ensureReady(ctx)
	if err != nil {
		return err
	}
	_, err = t.Request(ctx, http.MethodPost, "/reset", map[string]any{})
	return err
}

// Ping calls GET /health and returns server / registry version.
func (a *App) Ping(ctx context.Context) (*PingResult, error) {
	t, err := a.ensureReady(ctx)
	if err != nil {
		return nil, err
	}
	raw, err := t.Request(ctx, http.MethodGet, "/health", nil)
	if err != nil {
		return nil, err
	}
	var out PingResult
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("beava: parse ping response: %w", err)
	}
	return &out, nil
}
