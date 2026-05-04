package beava

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// pendingRequest is one in-flight TCP request awaiting a response on its channel.
type pendingRequest struct {
	respCh chan tcpResponse
}

type tcpResponse struct {
	op  uint16
	raw []byte
	err error
}

type tcpTransport struct {
	conn    net.Conn
	timeout time.Duration

	// queue is the FIFO of pending requests.
	mu      sync.Mutex
	queue   []*pendingRequest
	closed  bool
	closeCh chan struct{}
}

func newTCPTransport(host, port string, timeout time.Duration) (*tcpTransport, error) {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), timeout)
	if err != nil {
		return nil, fmt.Errorf("beava: tcp dial: %w", err)
	}
	t := &tcpTransport{
		conn:    conn,
		timeout: timeout,
		closeCh: make(chan struct{}),
	}
	go t.readerLoop()
	return t, nil
}

func (t *tcpTransport) readerLoop() {
	defer t.failAllPending(errors.New("tcp connection closed"))
	for {
		lengthBuf := make([]byte, 4)
		if _, err := io.ReadFull(t.conn, lengthBuf); err != nil {
			return
		}
		bodyLen := binary.BigEndian.Uint32(lengthBuf)
		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(t.conn, body); err != nil {
			return
		}
		// body is [u16 op][u8 ct][payload...]
		if len(body) < 3 {
			return
		}
		op := binary.BigEndian.Uint16(body[0:2])
		// ct := body[2] // unused at this layer
		payload := body[3:]

		t.mu.Lock()
		if len(t.queue) == 0 {
			t.mu.Unlock()
			// spurious frame; drop conn
			return
		}
		pr := t.queue[0]
		t.queue = t.queue[1:]
		t.mu.Unlock()

		if op == OpErrorResponse {
			var envelope struct {
				Error struct {
					Code   string            `json:"code"`
					Path   string            `json:"path"`
					Reason string            `json:"reason"`
					Errors []ValidationError `json:"errors"`
				} `json:"error"`
			}
			_ = json.Unmarshal(payload, &envelope)
			code := envelope.Error.Code
			if code == "" {
				code = "wire_error"
			}
			msg := envelope.Error.Reason
			if msg == "" {
				msg = "wire error"
			}
			pr.respCh <- tcpResponse{
				op:  op,
				err: &RegistrationError{Code: code, Path: envelope.Error.Path, Message: msg, Errors: envelope.Error.Errors},
			}
		} else {
			pr.respCh <- tcpResponse{op: op, raw: payload}
		}
	}
}

func (t *tcpTransport) failAllPending(err error) {
	t.mu.Lock()
	pending := t.queue
	t.queue = nil
	t.closed = true
	t.mu.Unlock()
	for _, pr := range pending {
		pr.respCh <- tcpResponse{err: err}
	}
}

// Send sends a wire frame and awaits the response. Returns the raw payload
// bytes from a non-error response, or *RegistrationError on OpErrorResponse.
func (t *tcpTransport) Send(ctx context.Context, op uint16, body any) ([]byte, error) {
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("beava: marshal tcp body: %w", err)
		}
	}
	frame := EncodeFrame(op, CtJSON, payload)

	pr := &pendingRequest{respCh: make(chan tcpResponse, 1)}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, errors.New("beava: tcp transport closed")
	}
	t.queue = append(t.queue, pr)
	t.mu.Unlock()

	if _, err := t.conn.Write(frame); err != nil {
		return nil, fmt.Errorf("beava: tcp write: %w", err)
	}

	timer := time.NewTimer(t.timeout)
	defer timer.Stop()
	select {
	case resp := <-pr.respCh:
		if resp.err != nil {
			return nil, resp.err
		}
		return resp.raw, nil
	case <-timer.C:
		return nil, fmt.Errorf("beava: tcp request timed out after %s", t.timeout)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *tcpTransport) Close(_ context.Context) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.mu.Unlock()
	return t.conn.Close()
}
