package beava_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"

	beava "github.com/beava-dev/beava/sdk/go"
)

type mockServer struct {
	mu         sync.Mutex
	lastMethod string
	lastPath   string
	lastBody   []byte
	status     int
	respBody   []byte
}

func newMockServer() (*mockServer, *httptest.Server) {
	m := &mockServer{status: 200, respBody: []byte("{}")}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		m.mu.Lock()
		m.lastMethod = r.Method
		m.lastPath = r.URL.Path
		m.lastBody = body
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(m.status)
		_, _ = w.Write(m.respBody)
		m.mu.Unlock()
	}))
	return m, srv
}

func (m *mockServer) setReply(status int, body string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.status = status
	m.respBody = []byte(body)
}

func (m *mockServer) capture() (string, string, []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastMethod, m.lastPath, m.lastBody
}

func newAppOrFatal(t *testing.T, url string, opts ...beava.AppOption) *beava.App {
	t.Helper()
	app, err := beava.NewApp(context.Background(), url, opts...)
	if err != nil {
		t.Fatalf("NewApp: %v", err)
	}
	return app
}

func TestApp_Ping(t *testing.T) {
	m, srv := newMockServer()
	defer srv.Close()
	m.setReply(200, `{"server_version":"v0","registry_version":1}`)
	app := newAppOrFatal(t, srv.URL)
	defer app.Close(context.Background())

	ping, err := app.Ping(context.Background())
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if ping.ServerVersion != "v0" || ping.RegistryVersion != 1 {
		t.Errorf("got %+v", ping)
	}
	method, path, _ := m.capture()
	if method != "GET" || path != "/health" {
		t.Errorf("expected GET /health, got %s %s", method, path)
	}
}

func TestApp_Register(t *testing.T) {
	m, srv := newMockServer()
	defer srv.Close()
	m.setReply(200, `{"status":"ok","registry_version":2}`)
	app := newAppOrFatal(t, srv.URL)
	defer app.Close(context.Background())

	descriptors := []beava.Descriptor{
		{"kind": "event", "name": "Click"},
	}
	res, err := app.Register(context.Background(), descriptors, beava.WithForce())
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if res.Status != "ok" || res.RegistryVersion != 2 {
		t.Errorf("got %+v", res)
	}
	method, path, body := m.capture()
	if method != "POST" || path != "/register" {
		t.Errorf("expected POST /register, got %s %s", method, path)
	}
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("body unmarshal: %v", err)
	}
	if parsed["force"] != true {
		t.Errorf("force not set: %+v", parsed)
	}
	if parsed["dry_run"] != false {
		t.Errorf("dry_run should be false: %+v", parsed)
	}
}

func TestApp_Push(t *testing.T) {
	m, srv := newMockServer()
	defer srv.Close()
	m.setReply(200, `{"ack_lsn":42,"registry_version":3}`)
	app := newAppOrFatal(t, srv.URL)
	defer app.Close(context.Background())

	res, err := app.Push(context.Background(), "Click", map[string]any{"user": "alice", "n": 1})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if res.AckLsn != 42 {
		t.Errorf("ack_lsn: got %d", res.AckLsn)
	}
	method, path, body := m.capture()
	if method != "POST" || path != "/push/Click" {
		t.Errorf("expected POST /push/Click, got %s %s", method, path)
	}
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("body unmarshal: %v", err)
	}
	if fields, ok := parsed["fields"].(map[string]any); !ok || fields["user"] != "alice" {
		t.Errorf("fields: %+v", parsed)
	}
}

func TestApp_PushSync_DelegatesToPush(t *testing.T) {
	m, srv := newMockServer()
	defer srv.Close()
	m.setReply(200, `{"ack_lsn":43,"registry_version":3}`)
	app := newAppOrFatal(t, srv.URL)
	defer app.Close(context.Background())

	_, err := app.PushSync(context.Background(), "Click", map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("PushSync: %v", err)
	}
	_, path, _ := m.capture()
	if path != "/push/Click" {
		t.Errorf("PushSync should delegate to /push/Click, got %s", path)
	}
}

func TestApp_Get_PerEntity(t *testing.T) {
	m, srv := newMockServer()
	defer srv.Close()
	m.setReply(200, `{"c":7}`)
	app := newAppOrFatal(t, srv.URL)
	defer app.Close(context.Background())

	row, err := app.Get(context.Background(), "UserCounts", "alice")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if c, _ := row["c"].(float64); c != 7 {
		t.Errorf("row: %+v", row)
	}
	method, path, body := m.capture()
	if method != "POST" || path != "/get" {
		t.Errorf("expected POST /get, got %s %s", method, path)
	}
	var parsed map[string]any
	_ = json.Unmarshal(body, &parsed)
	if parsed["table"] != "UserCounts" || parsed["key"] != "alice" {
		t.Errorf("body: %+v", parsed)
	}
}

func TestApp_Get_ColdStart(t *testing.T) {
	m, srv := newMockServer()
	defer srv.Close()
	m.setReply(200, `{}`)
	app := newAppOrFatal(t, srv.URL)
	defer app.Close(context.Background())

	row, err := app.Get(context.Background(), "X", "y")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if row == nil {
		t.Errorf("cold-start should return empty map, not nil")
	}
	if len(row) != 0 {
		t.Errorf("cold-start map should be empty: %+v", row)
	}
}

func TestApp_Get_CompositeKey(t *testing.T) {
	m, srv := newMockServer()
	defer srv.Close()
	m.setReply(200, `{}`)
	app := newAppOrFatal(t, srv.URL)
	defer app.Close(context.Background())

	_, err := app.Get(context.Background(), "T", []any{"a", int64(42), true})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	_, _, body := m.capture()
	var parsed map[string]any
	_ = json.Unmarshal(body, &parsed)
	keyArr, ok := parsed["key"].([]any)
	if !ok {
		t.Fatalf("key should be array, got %T: %+v", parsed["key"], parsed)
	}
	if !reflect.DeepEqual(keyArr, []any{"a", float64(42), true}) {
		t.Errorf("key: got %+v", keyArr)
	}
}

func TestApp_GetGlobal(t *testing.T) {
	m, srv := newMockServer()
	defer srv.Close()
	m.setReply(200, `{"total":99}`)
	app := newAppOrFatal(t, srv.URL)
	defer app.Close(context.Background())

	row, err := app.GetGlobal(context.Background(), "Total")
	if err != nil {
		t.Fatalf("GetGlobal: %v", err)
	}
	if row["total"] != float64(99) {
		t.Errorf("row: %+v", row)
	}
	_, _, body := m.capture()
	var parsed map[string]any
	_ = json.Unmarshal(body, &parsed)
	if parsed["table"] != "Total" || parsed["key"] != "" {
		t.Errorf("global body should have key='': %+v", parsed)
	}
}

func TestApp_BatchGet(t *testing.T) {
	m, srv := newMockServer()
	defer srv.Close()
	m.setReply(200, `[{"a":1},{},{"b":2}]`)
	app := newAppOrFatal(t, srv.URL)
	defer app.Close(context.Background())

	rows, err := app.BatchGet(context.Background(), []beava.GetRequest{
		{Table: "T1", Key: "a"},
		{Table: "T2", Key: "b"},
		{Table: "T3", Key: "c"},
	})
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows: %+v", rows)
	}
	if rows[0]["a"] != float64(1) || len(rows[1]) != 0 || rows[2]["b"] != float64(2) {
		t.Errorf("rows: %+v", rows)
	}
	_, path, _ := m.capture()
	if path != "/batch-get" {
		t.Errorf("path: %s", path)
	}
}

func TestApp_BatchGet_NoPartialSuccess(t *testing.T) {
	m, srv := newMockServer()
	defer srv.Close()
	m.setReply(400, `{"error":{"code":"unknown_table","path":"requests[1].table","reason":"T2 not registered"}}`)
	app := newAppOrFatal(t, srv.URL)
	defer app.Close(context.Background())

	_, err := app.BatchGet(context.Background(), []beava.GetRequest{
		{Table: "T1", Key: "a"},
		{Table: "T2", Key: "b"},
	})
	var regErr *beava.RegistrationError
	if !errors.As(err, &regErr) {
		t.Fatalf("expected *RegistrationError, got %T: %v", err, err)
	}
	if regErr.Code != "unknown_table" {
		t.Errorf("code: %s", regErr.Code)
	}
}

func TestApp_Reset_TestMode(t *testing.T) {
	m, srv := newMockServer()
	defer srv.Close()
	m.setReply(200, `{}`)
	app := newAppOrFatal(t, srv.URL)
	defer app.Close(context.Background())

	if err := app.Reset(context.Background()); err != nil {
		t.Errorf("Reset: %v", err)
	}
	method, path, _ := m.capture()
	if method != "POST" || path != "/reset" {
		t.Errorf("got %s %s", method, path)
	}
}

func TestApp_Reset_Forbidden(t *testing.T) {
	m, srv := newMockServer()
	defer srv.Close()
	m.setReply(403, `{"error":{"code":"reset_forbidden","reason":"test_mode required"}}`)
	app := newAppOrFatal(t, srv.URL)
	defer app.Close(context.Background())

	err := app.Reset(context.Background())
	var regErr *beava.RegistrationError
	if !errors.As(err, &regErr) {
		t.Fatalf("expected *RegistrationError, got %T: %v", err, err)
	}
	if regErr.Code != "reset_forbidden" {
		t.Errorf("code: %s", regErr.Code)
	}
}

func TestApp_Close_Idempotent(t *testing.T) {
	_, srv := newMockServer()
	defer srv.Close()
	app := newAppOrFatal(t, srv.URL)

	if err := app.Close(context.Background()); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := app.Close(context.Background()); err != nil {
		t.Errorf("second Close: %v", err)
	}
}
