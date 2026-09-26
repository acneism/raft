package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"

	"github.com/gliedabrennung/raft"
	"github.com/gliedabrennung/raft/internal/fsx"
)

const (
	segMagic   = "RAFTWAL1"
	headerSize = 64
	recHeader  = 8
	entryHead  = 17
	maxRecord  = 1 << 30
)

type segment struct {
	f     *os.File
	path  string
	epoch uint64
	first uint64
	seed  uint32
	end   int64
	crc   uint32
	dirty bool
}

func segName(epoch, first uint64) string { return fmt.Sprintf("%016x-%016x.seg", epoch, first) }

func parseSegName(name string) (epoch, first uint64, ok bool) {
	if !strings.HasSuffix(name, ".seg") {
		return 0, 0, false
	}
	_, err := fmt.Sscanf(name, "%016x-%016x.seg", &epoch, &first)
	return epoch, first, err == nil
}

func createSegment(dir string, epoch, first uint64, seed uint32, size int64) (*segment, error) {
	path := filepath.Join(dir, segName(epoch, first))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	h := make([]byte, headerSize)
	copy(h, segMagic)
	binary.LittleEndian.PutUint64(h[8:], epoch)
	binary.LittleEndian.PutUint64(h[16:], first)
	binary.LittleEndian.PutUint32(h[24:], seed)
	binary.LittleEndian.PutUint32(h[28:], crc32.Checksum(h[:28], castagnoli))
	err = fsx.Preallocate(f, size)
	if err == nil {
		_, err = f.WriteAt(h, 0)
	}
	if err == nil {
		err = f.Sync()
	}
	if err == nil {
		err = fsx.SyncDir(dir)
	}
	if err != nil {
		f.Close()
		os.Remove(path)
		return nil, err
	}
	return &segment{f: f, path: path, epoch: epoch, first: first, seed: seed, end: headerSize, crc: seed}, nil
}

func openSegment(path string) (*segment, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	h := make([]byte, headerSize)
	if _, err := f.ReadAt(h, 0); err != nil || string(h[:8]) != segMagic ||
		crc32.Checksum(h[:28], castagnoli) != binary.LittleEndian.Uint32(h[28:]) {
		f.Close()
		return nil, fmt.Errorf("wal: %s: bad segment header", filepath.Base(path))
	}
	s := &segment{
		f:     f,
		path:  path,
		epoch: binary.LittleEndian.Uint64(h[8:]),
		first: binary.LittleEndian.Uint64(h[16:]),
		seed:  binary.LittleEndian.Uint32(h[24:]),
		end:   headerSize,
	}
	s.crc = s.seed
	return s, nil
}

func (s *segment) close() error { return s.f.Close() }

func (s *segment) remove() error {
	s.f.Close()
	return os.Remove(s.path)
}

func appendRecord(buf []byte, prev uint32, e *raft.Entry) ([]byte, uint32) {
	start := len(buf)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(entryHead+len(e.Data)))
	buf = binary.LittleEndian.AppendUint32(buf, 0)
	buf = binary.LittleEndian.AppendUint64(buf, e.Index)
	buf = binary.LittleEndian.AppendUint64(buf, e.Term)
	buf = append(buf, byte(e.Type))
	buf = append(buf, e.Data...)
	crc := crc32.Update(prev, castagnoli, buf[start:start+4])
	crc = crc32.Update(crc, castagnoli, buf[start+recHeader:])
	binary.LittleEndian.PutUint32(buf[start+4:], crc)
	return buf, crc
}

func decodeEntry(p []byte) raft.Entry {
	e := raft.Entry{
		Index: binary.LittleEndian.Uint64(p),
		Term:  binary.LittleEndian.Uint64(p[8:]),
		Type:  raft.EntryType(p[16]),
	}
	if len(p) > entryHead {
		e.Data = append([]byte(nil), p[entryHead:]...)
	}
	return e
}

type scanned struct {
	off  int64
	size uint32
	prev uint32
	crc  uint32
	e    raft.Entry
}

func (s *segment) scan(fn func(r scanned) bool) error {
	st, err := s.f.Stat()
	if err != nil {
		return err
	}
	size := st.Size()
	var hdr [recHeader]byte
	var payload []byte
	off, crc := int64(headerSize), s.seed
	for off+recHeader <= size {
		if _, err := s.f.ReadAt(hdr[:], off); err != nil {
			return err
		}
		n := binary.LittleEndian.Uint32(hdr[:])
		if n < entryHead || n > maxRecord || off+recHeader+int64(n) > size {
			break
		}
		if cap(payload) < int(n) {
			payload = make([]byte, n)
		}
		payload = payload[:n]
		if _, err := s.f.ReadAt(payload, off+recHeader); err != nil {
			return err
		}
		next := crc32.Update(crc, castagnoli, hdr[:4])
		next = crc32.Update(next, castagnoli, payload)
		if next != binary.LittleEndian.Uint32(hdr[4:]) {
			break
		}
		r := scanned{off: off, size: n, prev: crc, crc: next, e: decodeEntry(payload)}
		if !fn(r) {
			break
		}
		off += recHeader + int64(n)
		crc = next
	}
	s.end, s.crc = off, crc
	return nil
}

func (s *segment) read(off int64, size uint32) (raft.Entry, error) {
	p := make([]byte, size)
	if _, err := s.f.ReadAt(p, off+recHeader); err != nil {
		return raft.Entry{}, err
	}
	return decodeEntry(p), nil
}
