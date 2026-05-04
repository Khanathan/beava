package beava

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// discoverBinary locates the beava binary using the 4-step order documented
// in python/beava/_embed.py.
func discoverBinary() (string, error) {
	searched := []string{}

	// Step 1: BEAVA_BINARY env var.
	if envVal := os.Getenv("BEAVA_BINARY"); envVal != "" {
		searched = append(searched, envVal)
		if info, err := os.Stat(envVal); err == nil && !info.IsDir() && info.Mode()&0o111 != 0 {
			return envVal, nil
		}
		return "", &BinaryNotFoundError{
			Searched: searched,
			Reason:   fmt.Sprintf("BEAVA_BINARY=%q is set but the path is not an executable file", envVal),
		}
	}

	// Step 2: beava on PATH.
	if p, err := exec.LookPath("beava"); err == nil {
		return p, nil
	}

	// Step 3: walk parents for target/debug/beava.
	cwd, _ := os.Getwd()
	dir := cwd
	for {
		candidate := filepath.Join(dir, "target", "debug", "beava")
		searched = append(searched, candidate)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() && info.Mode()&0o111 != 0 {
			return candidate, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	return "", &BinaryNotFoundError{
		Searched: searched,
		Reason: "beava binary not found. Install with one of:\n" +
			"  brew install beava\n" +
			"  pip install beava[server]\n" +
			"  docker pull beava/beava\n" +
			"Or set BEAVA_BINARY=/path/to/beava.",
	}
}

// SpawnedServer holds the spawned embed-mode process plus discovered URLs.
type SpawnedServer struct {
	Cmd     *exec.Cmd
	HTTPURL string
	TCPURL  string

	stdoutPipe io.ReadCloser
	once       sync.Once
}

// SpawnOptions configure embed-mode startup.
type SpawnOptions struct {
	StartupTimeout time.Duration // default 5s
	TestMode       bool
}

// SpawnEmbeddedServer launches a local beava server on ephemeral ports and
// blocks until both server.http_bound and server.tcp_bound events arrive.
func SpawnEmbeddedServer(ctx context.Context, opts SpawnOptions) (*SpawnedServer, error) {
	timeout := opts.StartupTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	binary, err := discoverBinary()
	if err != nil {
		return nil, err
	}

	cmd := exec.CommandContext(ctx, binary, "--config", "/dev/null")
	cmd.Env = append(os.Environ(),
		"BEAVA_LISTEN_ADDR=127.0.0.1:0",
		"BEAVA_TCP_PORT=0",
		"BEAVA_DEV_ENDPOINTS=1",
	)
	if opts.TestMode {
		cmd.Env = append(cmd.Env, "BEAVA_TEST_MODE=1")
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("beava: stdout pipe: %w", err)
	}
	cmd.Stderr = nil

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("beava: spawn: %w", err)
	}

	type result struct {
		http, tcp string
		err       error
	}
	resCh := make(chan result, 1)

	go func() {
		scanner := bufio.NewScanner(stdout)
		var httpAddr, tcpAddr string
		for scanner.Scan() {
			line := scanner.Bytes()
			var rec struct {
				Kind string `json:"kind"`
				Addr string `json:"addr"`
			}
			if err := json.Unmarshal(line, &rec); err != nil {
				continue
			}
			switch rec.Kind {
			case "server.http_bound":
				httpAddr = rec.Addr
			case "server.tcp_bound":
				tcpAddr = rec.Addr
			}
			if httpAddr != "" && tcpAddr != "" {
				resCh <- result{http: httpAddr, tcp: tcpAddr}
				return
			}
		}
		resCh <- result{err: fmt.Errorf("beava: stdout closed before bind events arrived")}
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case r := <-resCh:
		if r.err != nil {
			_ = cmd.Process.Kill()
			return nil, r.err
		}
		return &SpawnedServer{
			Cmd:        cmd,
			HTTPURL:    "http://" + r.http,
			TCPURL:     "tcp://" + r.tcp,
			stdoutPipe: stdout,
		}, nil
	case <-timer.C:
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("beava: embed-mode server did not bind within %s", timeout)
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		return nil, ctx.Err()
	}
}

// Teardown sends SIGTERM, then SIGKILL after `timeout`.
func (s *SpawnedServer) Teardown(timeout time.Duration) error {
	if s == nil || s.Cmd == nil || s.Cmd.Process == nil {
		return nil
	}
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	_ = s.Cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- s.Cmd.Wait() }()
	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		_ = s.Cmd.Process.Kill()
		<-done
		return nil
	}
}
