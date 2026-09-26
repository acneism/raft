package transport

import (
	"bufio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/gliedabrennung/raft"
	"github.com/gliedabrennung/raft/internal/fsx"
)

const chunkSize = 1 << 20

type SnapshotFile struct {
	Name string
	Size int64
	CRC  uint32
}

func Manifest(dir string) ([]SnapshotFile, error) {
	des, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []SnapshotFile
	for _, de := range des {
		if !de.Type().IsRegular() {
			continue
		}
		f, err := os.Open(filepath.Join(dir, de.Name()))
		if err != nil {
			return nil, err
		}
		h := crc32.New(castagnoli)
		n, err := io.Copy(h, f)
		f.Close()
		if err != nil {
			return nil, err
		}
		out = append(out, SnapshotFile{Name: de.Name(), Size: n, CRC: h.Sum32()})
	}
	return out, nil
}

func fileCRC(path string) (uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	h := crc32.New(castagnoli)
	if _, err := io.Copy(h, f); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}

func validName(name string) bool {
	return name != "" && name != "." && name != ".." && filepath.Base(name) == name && filepath.IsLocal(name)
}

func IncomingDir(root string, s raft.SnapshotMeta) string {
	return filepath.Join(root, fmt.Sprintf("incoming-%016x-%016x", s.Term, s.Index))
}

func encodeOffer(m *raft.Message, files []SnapshotFile) []byte {
	b := appendBytes(nil, AppendMessage(nil, m))
	b = appendUvarint(b, uint64(len(files)))
	for _, f := range files {
		b = appendString(b, f.Name)
		b = appendUvarint(b, uint64(f.Size))
		b = appendUvarint(b, uint64(f.CRC))
	}
	return b
}

func decodeOffer(p []byte) (raft.Message, []SnapshotFile, error) {
	d := decoder{b: p}
	m, err := DecodeMessage(d.bytes())
	if err != nil {
		return m, nil, err
	}
	n := d.count(3)
	files := make([]SnapshotFile, 0, n)
	for range n {
		files = append(files, SnapshotFile{Name: d.string(), Size: int64(d.uvarint()), CRC: uint32(d.uvarint())})
	}
	if d.err == nil && len(d.b) != 0 {
		d.fail()
	}
	return m, files, d.err
}

func (t *Transport) SendSnapshot(m raft.Message, dir string) {
	t.mu.Lock()
	busy := t.snaps[m.To]
	t.snaps[m.To] = true
	t.mu.Unlock()
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		if busy {
			t.cfg.Handler.ReportSnapshot(m.To, true)
			return
		}
		err := t.sendSnapshot(m, dir)
		t.mu.Lock()
		delete(t.snaps, m.To)
		t.mu.Unlock()
		t.cfg.Handler.ReportSnapshot(m.To, err != nil)
	}()
}

func (t *Transport) sendSnapshot(m raft.Message, dir string) error {
	p := t.peer(m.To)
	if p == nil || m.Snapshot == nil {
		return errors.New("transport: bad snapshot message")
	}
	files, err := Manifest(dir)
	if err != nil {
		return err
	}
	c, r, w, err := t.dial(m.To, p.address(), streamSnapshot)
	if err != nil {
		return err
	}
	defer t.untrack(c)
	c.SetDeadline(time.Now().Add(t.cfg.IOTimeout))
	if err := writeFrame(w, frameSnapOffer, encodeOffer(&m, files)); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	hp, err := expectFrame(r, frameSnapHave)
	if err != nil {
		return err
	}
	d := decoder{b: hp}
	have := make([]int64, len(files))
	for i := range have {
		have[i] = int64(d.uvarint())
	}
	if d.err != nil {
		return d.err
	}
	buf := make([]byte, chunkSize)
	for i, f := range files {
		off := have[i]
		if off > f.Size {
			return fmt.Errorf("transport: receiver has %d bytes of %s, more than %d", off, f.Name, f.Size)
		}
		if off == f.Size {
			continue
		}
		fh, err := os.Open(filepath.Join(dir, f.Name))
		if err != nil {
			return err
		}
		err = sendFile(c, w, fh, i, off, f.Size, buf, t.cfg.IOTimeout)
		fh.Close()
		if err != nil {
			return err
		}
	}
	c.SetDeadline(time.Now().Add(t.cfg.IOTimeout))
	if err := writeFrame(w, frameSnapDone, nil); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	var total int64
	for _, f := range files {
		total += f.Size
	}
	c.SetReadDeadline(time.Now().Add(t.cfg.IOTimeout + time.Duration(total/(50<<20))*time.Second))
	res, err := expectFrame(r, frameSnapResult)
	if err != nil {
		return err
	}
	if len(res) == 0 || res[0] != 1 {
		return fmt.Errorf("transport: snapshot rejected by %s: %s", m.To, res[min(1, len(res)):])
	}
	return nil
}

func sendFile(c net.Conn, w *bufio.Writer, f *os.File, idx int, off, size int64, buf []byte, timeout time.Duration) error {
	var payload []byte
	for off < size {
		n, err := f.ReadAt(buf[:min(int64(len(buf)), size-off)], off)
		if n == 0 && err != nil {
			return err
		}
		payload = appendUvarint(payload[:0], uint64(idx))
		payload = appendUvarint(payload, uint64(off))
		payload = append(payload, buf[:n]...)
		c.SetWriteDeadline(time.Now().Add(timeout))
		if err := writeFrame(w, frameSnapChunk, payload); err != nil {
			return err
		}
		off += int64(n)
	}
	return nil
}

func (t *Transport) receiveSnapshot(c net.Conn, r *bufio.Reader, from raft.NodeID) {
	w := bufio.NewWriter(c)
	c.SetDeadline(time.Now().Add(t.cfg.IOTimeout))
	p, err := expectFrame(r, frameSnapOffer)
	if err != nil {
		return
	}
	m, files, err := decodeOffer(p)
	if err != nil || m.Type != raft.MsgSnap || m.Snapshot == nil || m.From != from || m.To != t.cfg.ID {
		return
	}
	dir, err := t.receiveFiles(c, r, w, *m.Snapshot, files)
	result := []byte{1}
	if err != nil {
		result = append([]byte{0}, err.Error()...)
	}
	c.SetWriteDeadline(time.Now().Add(t.cfg.IOTimeout))
	if writeFrame(w, frameSnapResult, result) != nil || w.Flush() != nil || err != nil {
		return
	}
	t.cfg.Handler.ReceiveSnapshot(m, dir)
}

func (t *Transport) receiveFiles(c net.Conn, r *bufio.Reader, w *bufio.Writer, s raft.SnapshotMeta, files []SnapshotFile) (string, error) {
	names := make([]string, len(files))
	for i, f := range files {
		if !validName(f.Name) || f.Size < 0 || slices.Contains(names[:i], f.Name) {
			return "", fmt.Errorf("transport: invalid snapshot file %q", f.Name)
		}
		names[i] = f.Name
	}
	dir := IncomingDir(t.cfg.SnapshotDir, s)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	des, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	for _, de := range des {
		if !slices.Contains(names, de.Name()) {
			os.RemoveAll(filepath.Join(dir, de.Name()))
		}
	}
	fhs := make([]*os.File, len(files))
	defer func() {
		for _, fh := range fhs {
			if fh != nil {
				fh.Close()
			}
		}
	}()
	next := make([]int64, len(files))
	var have []byte
	for i, f := range files {
		fh, err := os.OpenFile(filepath.Join(dir, f.Name), os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			return "", err
		}
		fhs[i] = fh
		st, err := fh.Stat()
		if err != nil {
			return "", err
		}
		if st.Size() > f.Size {
			if err := fh.Truncate(0); err != nil {
				return "", err
			}
		} else {
			next[i] = st.Size()
		}
		have = appendUvarint(have, uint64(next[i]))
	}
	if err := writeFrame(w, frameSnapHave, have); err != nil {
		return "", err
	}
	if err := w.Flush(); err != nil {
		return "", err
	}
	for done := false; !done; {
		c.SetReadDeadline(time.Now().Add(t.cfg.IOTimeout))
		typ, p, err := readFrame(r)
		if err != nil {
			return "", err
		}
		switch typ {
		case frameSnapDone:
			done = true
		case frameSnapChunk:
			d := decoder{b: p}
			i, off := d.uvarint(), int64(d.uvarint())
			if d.err != nil || i >= uint64(len(files)) || off != next[i] || off+int64(len(d.b)) > files[i].Size {
				return "", errors.New("transport: unexpected snapshot chunk")
			}
			if _, err := fhs[i].WriteAt(d.b, off); err != nil {
				return "", err
			}
			next[i] += int64(len(d.b))
		default:
			return "", fmt.Errorf("transport: unexpected frame %d during snapshot", typ)
		}
	}
	c.SetDeadline(time.Time{})
	for i, f := range files {
		if next[i] != f.Size {
			return "", fmt.Errorf("transport: %s incomplete: %d of %d bytes", f.Name, next[i], f.Size)
		}
		if err := fhs[i].Sync(); err != nil {
			return "", err
		}
		crc, err := fileCRC(filepath.Join(dir, f.Name))
		if err != nil {
			return "", err
		}
		if crc != f.CRC {
			fhs[i].Truncate(0)
			return "", fmt.Errorf("transport: %s checksum mismatch", f.Name)
		}
	}
	return dir, fsx.SyncDir(dir)
}
