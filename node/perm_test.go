package node_test

import (
	"io/fs"
	"path/filepath"
	"runtime"
	"testing"
)

func checkPrivate(t *testing.T, root string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		return
	}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		want := fs.FileMode(0o600)
		if d.IsDir() {
			want = 0o700
		}
		if perm := info.Mode().Perm(); perm != want {
			t.Errorf("%s has mode %v, want %v", path, perm, want)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
