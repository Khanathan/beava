package beava

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type httpTransport struct {
	baseURL string
	client  *http.Client
}

func newHTTPTransport(baseURL string, timeout time.Duration) *httpTransport {
	return &httpTransport{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: timeout},
	}
}

// Request issues an HTTP request and returns the raw response body bytes on 2xx
// or a *RegistrationError parsed from the structured error envelope on non-2xx.
func (t *httpTransport) Request(ctx context.Context, method, path string, body any) ([]byte, error) {
	var rdr io.Reader
	if body != nil && method != http.MethodGet {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("beava: marshal request body: %w", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, t.baseURL+path, rdr)
	if err != nil {
		return nil, fmt.Errorf("beava: build http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("beava: http request: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("beava: read http response: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		var envelope struct {
			Error struct {
				Code   string            `json:"code"`
				Path   string            `json:"path"`
				Reason string            `json:"reason"`
				Errors []ValidationError `json:"errors"`
			} `json:"error"`
		}
		_ = json.Unmarshal(raw, &envelope)
		code := envelope.Error.Code
		if code == "" {
			code = "http_error"
		}
		msg := envelope.Error.Reason
		if msg == "" {
			msg = fmt.Sprintf("http %d", resp.StatusCode)
		}
		return nil, &RegistrationError{
			Code:    code,
			Path:    envelope.Error.Path,
			Message: msg,
			Errors:  envelope.Error.Errors,
		}
	}
	return raw, nil
}

func (t *httpTransport) Close(_ context.Context) error { return nil }
