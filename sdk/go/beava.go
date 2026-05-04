// Package beava is the Beava real-time feature server SDK for Go.
//
// Per Phase 13.6 scope amendment 2026-05-03, this SDK is COMMUNICATE-ONLY:
// it pushes events, registers pre-compiled JSON descriptors, and reads
// features. Pipeline authoring (decorators, expressions, op helpers) lives
// in the Python SDK only.
package beava

import (
	"context"
	"time"
)

// AppOption is a functional option for App construction.
type AppOption func(*appConfig)

type appConfig struct {
	timeout  time.Duration
	testMode bool
	binary   string
}

// WithTimeout sets the transport-level I/O timeout (default 30s).
func WithTimeout(d time.Duration) AppOption {
	return func(c *appConfig) { c.timeout = d }
}

// WithTestMode enables test-mode in embed mode (mirrors Python `bv.App(test_mode=True)`).
// Ignored with a no-op for non-embed transports.
func WithTestMode() AppOption {
	return func(c *appConfig) { c.testMode = true }
}

// WithBinaryPath overrides the embed-mode binary discovery path.
func WithBinaryPath(p string) AppOption {
	return func(c *appConfig) { c.binary = p }
}

// App is the Beava client. Wire methods (Register/Push/Get/BatchGet/Reset/Ping)
// land in Plan 13.6-04 + 13.6-06.
type App struct {
	url string
	cfg appConfig
	// transport-level fields land in Plan 13.6-04
}

// NewApp constructs a Beava client. URL controls transport selection:
//   - "http://..." / "https://..." → HTTP/JSON transport
//   - "tcp://..."                  → custom-framed TCP transport
//   - ""                           → embed mode (spawn local beava binary)
//
// Plan 13.6-04 implements the actual transports; this scaffold only stores the URL.
func NewApp(_ context.Context, url string, opts ...AppOption) (*App, error) {
	cfg := appConfig{timeout: 30 * time.Second}
	for _, o := range opts {
		o(&cfg)
	}
	return &App{url: url, cfg: cfg}, nil
}

// URL returns the URL passed to NewApp (transport scheme + host).
func (a *App) URL() string { return a.url }
