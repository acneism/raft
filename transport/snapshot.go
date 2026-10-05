package transport

import (
	"bufio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/acneism/raft"
	"github.com/acneism/raft/internal/fsx"
)

const chunkSize = 1 << 20

type SnapshotFile struct {
	Name string
	Size int64
	CRC  uint32
}

func Manifest(dir string) ([]SnapshotFile, error) {
	var out []SnapshotFile
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		h := crc32.New(castagnoli)
		n, err := io.Copy(h, f)
		f.Close()
		if err != nil {
			return err
		}
		out = append(out, SnapshotFile{Name: filepath.ToSlash(rel), Size: n, CRC: h.Sum32()})
		return nil
	})
	return out, err
}

func validName(name string) bool {
	return name != "" && name != "." && !strings.ContainsAny(name, `\:`) && filepath.IsLocal(filepath.FromSlash(name)) &&
		filepath.ToSlash(filepath.Clean(filepath.FromSlash(name))) == name
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
		if err != nil && t.allow("send snapshot "+string(m.To)) {
			t.cfg.Logger.Warn("cannot send a snapshot to a peer", "peer", m.To, "index", m.Snapshot.Index, "err", err)
		}
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
		fh, err := os.Open(filepath.Join(dir, filepath.FromSlash(f.Name)))
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
	fail := func(err error) {
		if t.allow("receive snapshot " + string(from)) {
			t.cfg.Logger.Warn("cannot receive a snapshot from a peer", "peer", from, "err", err)
		}
	}
	w := bufio.NewWriter(c)
	c.SetDeadline(time.Now().Add(t.cfg.IOTimeout))
	p, err := expectFrame(r, frameSnapOffer)
	if err != nil {
		fail(err)
		return
	}
	m, files, err := decodeOffer(p)
	if err == nil && (m.Type != raft.MsgSnap || m.Snapshot == nil || m.From != from || m.To != t.cfg.ID) {
		err = fmt.Errorf("transport: unexpected %v from %s to %s in a snapshot offer", m.Type, m.From, m.To)
	}
	if err != nil {
		fail(err)
		return
	}
	dir, err := t.receiveFiles(c, r, w, *m.Snapshot, files)
	result := []byte{1}
	if err != nil {
		result = append([]byte{0}, err.Error()...)
	}
	c.SetWriteDeadline(time.Now().Add(t.cfg.IOTimeout))
	werr := writeFrame(w, frameSnapResult, result)
	if werr == nil {
		werr = w.Flush()
	}
	if err == nil {
		err = werr
	}
	if err != nil {
		fail(err)
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
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err == nil && !slices.Contains(names, filepath.ToSlash(rel)) {
			err = os.Remove(path)
		}
		return err
	})
	if err != nil {
		return "", err
	}
	dirs := []string{dir}
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
		path := filepath.Join(dir, filepath.FromSlash(f.Name))
		if parent := filepath.Dir(path); !slices.Contains(dirs, parent) {
			if err := os.MkdirAll(parent, 0o700); err != nil {
				return "", err
			}
			dirs = append(dirs, parent)
		}
		fh, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
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
	crcs := make([]uint32, len(files))
	for i := range files {
		if next[i] > 0 {
			h := crc32.New(castagnoli)
			if _, err := io.Copy(h, io.NewSectionReader(fhs[i], 0, next[i])); err != nil {
				return "", err
			}
			crcs[i] = h.Sum32()
		}
	}
	fw := newFileWriter(fhs)
	err = func() error {
		for {
			c.SetReadDeadline(time.Now().Add(t.cfg.IOTimeout))
			typ, p, err := readFrame(r)
			if err != nil {
				return err
			}
			switch typ {
			case frameSnapDone:
				return nil
			case frameSnapChunk:
				d := decoder{b: p}
				i, off := d.uvarint(), int64(d.uvarint())
				if d.err != nil || i >= uint64(len(files)) || off != next[i] || off+int64(len(d.b)) > files[i].Size {
					return errors.New("transport: unexpected snapshot chunk")
				}
				crcs[i] = crc32.Update(crcs[i], castagnoli, d.b)
				if err := fw.write(int(i), off, d.b); err != nil {
					return err
				}
				next[i] += int64(len(d.b))
			default:
				return fmt.Errorf("transport: unexpected frame %d during snapshot", typ)
			}
		}
	}()
	if werr := fw.close(); werr != nil {
		err = werr
	}
	if err != nil {
		return "", err
	}
	c.SetDeadline(time.Time{})
	for i, f := range files {
		if next[i] != f.Size {
			return "", fmt.Errorf("transport: %s incomplete: %d of %d bytes", f.Name, next[i], f.Size)
		}
		if crcs[i] != f.CRC {
			fhs[i].Truncate(0)
			return "", fmt.Errorf("transport: %s checksum mismatch", f.Name)
		}
		if err := fhs[i].Sync(); err != nil {
			return "", err
		}
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := fsx.SyncDir(dirs[i]); err != nil {
			return "", err
		}
	}
	return dir, nil
}

const syncEvery = 32 << 20

type fileChunk struct {
	i   int
	off int64
	b   []byte
}

type fileWriter struct {
	fhs    []*os.File
	chunks chan fileChunk
	done   chan error
	failed atomic.Bool
}

func newFileWriter(fhs []*os.File) *fileWriter {
	w := &fileWriter{fhs: fhs, chunks: make(chan fileChunk, 16), done: make(chan error, 1)}
	go w.run()
	return w
}

func (w *fileWriter) run() {
	var err error
	unsynced := make([]int64, len(w.fhs))
	for ch := range w.chunks {
		if err != nil {
			continue
		}
		if _, err = w.fhs[ch.i].WriteAt(ch.b, ch.off); err == nil {
			if unsynced[ch.i] += int64(len(ch.b)); unsynced[ch.i] >= syncEvery {
				unsynced[ch.i] = 0
				err = w.fhs[ch.i].Sync()
			}
		}
		if err != nil {
			w.failed.Store(true)
		}
	}
	w.done <- err
}

func (w *fileWriter) write(i int, off int64, b []byte) error {
	if w.failed.Load() {
		return errors.New("transport: writing snapshot files failed")
	}
	w.chunks <- fileChunk{i: i, off: off, b: b}
	return nil
}

func (w *fileWriter) close() error {
	close(w.chunks)
	return <-w.done
}
