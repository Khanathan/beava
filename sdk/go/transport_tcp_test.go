package beava

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// startEchoServer spins up a TCP server that reads incoming wire frames and
// replies with OpGetResponse + a JSON body containing a sequence number.
func startEchoServer(t *testing.T) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	stopCh := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		seq := 0
		for {
			lengthBuf := make([]byte, 4)
			if _, err := io.ReadFull(conn, lengthBuf); err != nil {
				return
			}
			bodyLen := binary.BigEndian.Uint32(lengthBuf)
			body := make([]byte, bodyLen)
			if _, err := io.ReadFull(conn, body); err != nil {
				return
			}
			seq++
			respBody, _ := json.Marshal(map[string]any{"seq": seq})
			out := EncodeFrame(OpGetResponse, CtJSON, respBody)
			_, _ = conn.Write(out)
			select {
			case <-stopCh:
				return
			default:
			}
		}
	}()
	return ln.Addr().String(), func() { close(stopCh); _ = ln.Close() }
}

func TestTCPTransportFIFO(t *testing.T) {
	addr, stop := startEchoServer(t)
	defer stop()

	host, port := strings.Split(addr, ":")[0], strings.Split(addr, ":")[1]
	tr, err := newTCPTransport(host, port, 5*time.Second)
	if err != nil {
		t.Fatalf("newTCPTransport: %v", err)
	}
	defer tr.Close(context.Background())

	var wg sync.WaitGroup
	results := make([]int, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			raw, err := tr.Send(context.Background(), OpPing, map[string]any{})
			if err != nil {
				t.Errorf("send %d: %v", idx, err)
				return
			}
			var resp struct {
				Seq int `json:"seq"`
			}
			if err := json.Unmarshal(raw, &resp); err != nil {
				t.Errorf("unmarshal %d: %v", idx, err)
				return
			}
			results[idx] = resp.Seq
		}(i)
		// stagger goroutine creation to ensure deterministic queue order
		time.Sleep(5 * time.Millisecond)
	}
	wg.Wait()

	for i, got := range results {
		if got != i+1 {
			t.Errorf("seq[%d]: got %d, want %d (FIFO violated)", i, got, i+1)
		}
	}
}
