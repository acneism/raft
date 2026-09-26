package fsx

import "os"

func Datasync(f *os.File) error { return f.Sync() }

func Preallocate(f *os.File, size int64) error { return f.Truncate(size) }

func SyncDir(string) error { return nil }
