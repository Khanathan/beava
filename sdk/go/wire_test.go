package beava

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestFrameCodecRoundTrip(t *testing.T) {
	payload := []byte(`{"fields":{"a":1}}`)
	frame := EncodeFrame(OpPush, CtJSON, payload)
	op, ct, got, err := DecodeFrame(frame)
	if err != nil {
		t.Fatalf("DecodeFrame: %v", err)
	}
	if op != OpPush {
		t.Errorf("op: got 0x%04x, want 0x%04x", op, OpPush)
	}
	if ct != CtJSON {
		t.Errorf("ct: got 0x%02x, want 0x%02x", ct, CtJSON)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("payload: got %q, want %q", got, payload)
	}
}

func TestFrameCodecEmptyPayloadIsThreeBytes(t *testing.T) {
	frame := EncodeFrame(OpPing, CtJSON, nil)
	if got, want := binary.BigEndian.Uint32(frame[:4]), uint32(3); got != want {
		t.Errorf("length header: got %d, want %d (length excludes itself)", got, want)
	}
	if got, want := len(frame), 4+3; got != want {
		t.Errorf("frame length: got %d, want %d", got, want)
	}
}
