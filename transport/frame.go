package transport

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	frameHello byte = iota + 1
	frameMessages
	frameSnapOffer
	frameSnapHave
	frameSnapChunk
	frameSnapDone
	frameSnapResult
)

const (
	frameHeader = 9
	maxFrame    = 64 << 20
	version     = 1
)

const (
	streamMessages byte = iota
	streamSnapshot
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

var errFrame = errors.New("transport: corrupt frame")

func writeFrame(w *bufio.Writer, typ byte, payload []byte) error {
	var h [frameHeader]byte
	binary.LittleEndian.PutUint32(h[0:], uint32(len(payload)))
	h[4] = typ
	crc := crc32.Update(crc32.Checksum(h[4:5], castagnoli), castagnoli, payload)
	binary.LittleEndian.PutUint32(h[5:], crc)
	if _, err := w.Write(h[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func readFrame(r *bufio.Reader) (byte, []byte, error) {
	var h [frameHeader]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return 0, nil, err
	}
	n := binary.LittleEndian.Uint32(h[0:])
	if n > maxFrame {
		return 0, nil, fmt.Errorf("%w: %d bytes", errFrame, n)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	crc := crc32.Update(crc32.Checksum(h[4:5], castagnoli), castagnoli, payload)
	if crc != binary.LittleEndian.Uint32(h[5:]) {
		return 0, nil, errFrame
	}
	return h[4], payload, nil
}

func expectFrame(r *bufio.Reader, typ byte) ([]byte, error) {
	t, p, err := readFrame(r)
	if err != nil {
		return nil, err
	}
	if t != typ {
		return nil, fmt.Errorf("%w: got frame type %d, want %d", errFrame, t, typ)
	}
	return p, nil
}

type hello struct {
	kind     byte
	from, to string
}

func (h hello) encode() []byte {
	b := []byte{version, h.kind}
	b = appendString(b, h.from)
	return appendString(b, h.to)
}

func decodeHello(p []byte) (hello, error) {
	d := decoder{b: p}
	if d.byte() != version {
		return hello{}, fmt.Errorf("transport: unsupported protocol version")
	}
	h := hello{kind: d.byte(), from: d.string(), to: d.string()}
	return h, d.err
}
