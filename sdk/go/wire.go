package beava

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	OpPing          uint16 = 0x0000
	OpRegister      uint16 = 0x0001
	OpPush          uint16 = 0x0010
	OpGet           uint16 = 0x0020
	OpGetResponse   uint16 = 0x0023
	OpBatchGet      uint16 = 0x0024
	OpReset         uint16 = 0x0040
	OpErrorResponse uint16 = 0xFFFF
	CtJSON          uint8  = 0x01
)

// ErrFrameTruncated is returned when DecodeFrame receives fewer bytes than the
// minimum 7-byte header.
var ErrFrameTruncated = errors.New("frame too short")

// EncodeFrame: [u32 length BE][u16 op BE][u8 content_type][payload]. length = 3 + len(payload).
func EncodeFrame(op uint16, contentType uint8, payload []byte) []byte {
	bodyLen := 3 + len(payload)
	out := make([]byte, 4+bodyLen)
	binary.BigEndian.PutUint32(out[0:4], uint32(bodyLen))
	binary.BigEndian.PutUint16(out[4:6], op)
	out[6] = contentType
	copy(out[7:], payload)
	return out
}

// DecodeFrame parses a complete single frame from buf. The caller is responsible for
// supplying a complete frame (i.e., len(buf) == 4 + length-header). Streaming framing
// is in transport_tcp.go.
func DecodeFrame(buf []byte) (op uint16, contentType uint8, payload []byte, err error) {
	if len(buf) < 7 {
		err = fmt.Errorf("%w: %d bytes (minimum 7)", ErrFrameTruncated, len(buf))
		return
	}
	bodyLen := binary.BigEndian.Uint32(buf[0:4])
	if uint32(len(buf)) != 4+bodyLen {
		err = fmt.Errorf("frame length mismatch: header says %d, got %d", 4+bodyLen, len(buf))
		return
	}
	op = binary.BigEndian.Uint16(buf[4:6])
	contentType = buf[6]
	payload = buf[7:]
	return
}
