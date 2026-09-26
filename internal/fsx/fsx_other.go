//go:build !linux && !windows

package fsx

import "os"

func Datasync(f *os.File) error { return f.Sync() }

func Preallocate(f *os.File, size int64) error { return f.Truncate(size) }

func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
