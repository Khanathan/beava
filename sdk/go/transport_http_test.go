package beava

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHttpTransportRegisterSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/register" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"status":"ok","registry_version":1}`))
	}))
	defer srv.Close()

	tr := newHTTPTransport(srv.URL, 5*time.Second)
	raw, err := tr.Request(context.Background(), "POST", "/register", map[string]any{"nodes": []any{}})
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	var got RegisterResult
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Status != "ok" || got.RegistryVersion != 1 {
		t.Errorf("got %+v", got)
	}
}

func TestHttpTransportStructuredError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"error":{"code":"unsupported_node_kind","path":"nodes[0]","reason":"table is not supported in v0"}}`))
	}))
	defer srv.Close()

	tr := newHTTPTransport(srv.URL, 5*time.Second)
	_, err := tr.Request(context.Background(), "POST", "/register", map[string]any{"nodes": []any{map[string]any{}}})
	var regErr *RegistrationError
	if !errors.As(err, &regErr) {
		t.Fatalf("expected *RegistrationError, got %T: %v", err, err)
	}
	if regErr.Code != "unsupported_node_kind" {
		t.Errorf("code: got %q", regErr.Code)
	}
}
