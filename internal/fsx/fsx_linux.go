package fsx

import (
	"os"
	"syscall"
)

func Datasync(f *os.File) error { return syscall.Fdatasync(int(f.Fd())) }

func Preallocate(f *os.File, size int64) error {
	if err := syscall.Fallocate(int(f.Fd()), 0, 0, size); err == nil {
		return nil
	}
	return f.Truncate(size)
}

func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
