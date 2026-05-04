// Package beava is the Beava real-time feature server SDK for Go.
//
// Per Phase 13.6 scope amendment 2026-05-03, this SDK is COMMUNICATE-ONLY:
// it pushes events, registers pre-compiled JSON descriptors, and reads
// features. Pipeline authoring (decorators, expressions, op helpers) lives
// in the Python SDK only.
package beava

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
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

// transport is the internal interface bridging app methods and wire backends.
type transport interface {
	Request(ctx context.Context, method, path string, body any) ([]byte, error)
	Close(ctx context.Context) error
}

// tcpAdapter wraps tcpTransport with a uniform Request signature so HTTP-style
// (method, path, body) calls map to wire opcodes.
type tcpAdapter struct {
	t *tcpTransport
}

func (a *tcpAdapter) Request(ctx context.Context, method, path string, body any) ([]byte, error) {
	op, err := routeToOpcode(method, path)
	if err != nil {
		return nil, err
	}
	payload := body
	if op == OpPush {
		eventName := strings.TrimPrefix(path, "/push/")
		decoded, derr := url.PathUnescape(eventName)
		if derr == nil {
			eventName = decoded
		}
		merged := map[string]any{"event_name": eventName}
		if bodyMap, ok := body.(map[string]any); ok {
			for k, v := range bodyMap {
				merged[k] = v
			}
		}
		payload = merged
	}
	return a.t.Send(ctx, op, payload)
}

func (a *tcpAdapter) Close(ctx context.Context) error { return a.t.Close(ctx) }

func routeToOpcode(method, path string) (uint16, error) {
	switch {
	case method == "GET" && path == "/health":
		return OpPing, nil
	case method == "POST" && path == "/register":
		return OpRegister, nil
	case method == "POST" && strings.HasPrefix(path, "/push/"):
		return OpPush, nil
	case method == "POST" && path == "/get":
		return OpGet, nil
	case method == "POST" && path == "/batch-get":
		return OpBatchGet, nil
	case method == "POST" && path == "/reset":
		return OpReset, nil
	default:
		return 0, fmt.Errorf("beava: tcp adapter has no opcode for %s %s", method, path)
	}
}

// App is the Beava client.
type App struct {
	url string
	cfg appConfig

	mu          sync.Mutex
	transport   transport
	embedHandle *SpawnedServer
	closed      bool
}

// NewApp constructs a Beava client. URL controls transport selection:
//   - "http://..." / "https://..." → HTTP/JSON transport
//   - "tcp://..."                  → custom-framed TCP transport
//   - ""                           → embed mode (spawn local beava binary on first call)
func NewApp(_ context.Context, rawURL string, opts ...AppOption) (*App, error) {
	cfg := appConfig{timeout: 30 * time.Second}
	for _, o := range opts {
		o(&cfg)
	}
	a := &App{url: rawURL, cfg: cfg}
	switch {
	case strings.HasPrefix(rawURL, "http://") || strings.HasPrefix(rawURL, "https://"):
		a.transport = newHTTPTransport(rawURL, cfg.timeout)
	case strings.HasPrefix(rawURL, "tcp://"):
		u, err := url.Parse(rawURL)
		if err != nil {
			return nil, fmt.Errorf("beava: parse tcp url: %w", err)
		}
		port := u.Port()
		if port == "" {
			return nil, fmt.Errorf("beava: tcp url missing port: %s", rawURL)
		}
		t, err := newTCPTransport(u.Hostname(), port, cfg.timeout)
		if err != nil {
			return nil, err
		}
		a.transport = &tcpAdapter{t: t}
	case rawURL == "":
		// Embed mode — defer spawn until first call.
		a.transport = nil
	default:
		return nil, fmt.Errorf("beava: unsupported URL scheme: %q", rawURL)
	}
	return a, nil
}

// URL returns the URL passed to NewApp.
func (a *App) URL() string { return a.url }

// ensureReady spawns the embed-mode server on first call when in embed mode.
func (a *App) ensureReady(ctx context.Context) (transport, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil, fmt.Errorf("beava: App has been closed")
	}
	if a.transport != nil {
		return a.transport, nil
	}
	handle, err := SpawnEmbeddedServer(ctx, SpawnOptions{TestMode: a.cfg.testMode})
	if err != nil {
		return nil, err
	}
	a.embedHandle = handle
	a.transport = newHTTPTransport(handle.HTTPURL, a.cfg.timeout)
	return a.transport, nil
}

// Close is idempotent. In embed mode it terminates the spawned subprocess.
func (a *App) Close(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	if a.transport != nil {
		_ = a.transport.Close(ctx)
		a.transport = nil
	}
	if a.embedHandle != nil {
		_ = a.embedHandle.Teardown(5 * time.Second)
		a.embedHandle = nil
	}
	return nil
}
