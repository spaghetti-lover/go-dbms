package kv

import (
	"os"

	"github.com/spaghetti-lover/go-db/internal/wal"
)

type WALBPTreeEngine struct {
	Tree    *BPTreeEngine
	WALFile *os.File
}

func NewWALBPTreeEngine(dataFile, walFile string) (*WALBPTreeEngine, error) {
	tree, err := NewBPTreeEngine(dataFile)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(walFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	// Replay WAL
	entries, _ := wal.ReadAllWAL(f)
	for _, e := range entries {
		if e.Op == 0 {
			tree.Set(e.Key, e.Value)
		} else {
			tree.Del(e.Key)
		}
	}
	// Truncate WAL sau khi replay (optional)
	f.Truncate(0)
	f.Seek(0, 0)
	return &WALBPTreeEngine{Tree: tree, WALFile: f}, nil
}

func (e *WALBPTreeEngine) Set(key, val []byte) error {
	err := wal.WriteWAL(e.WALFile, &wal.WALEntry{Op: 0, Key: key, Value: val})
	if err != nil {
		return err
	}
	return e.Tree.Set(key, val)
}

func (e *WALBPTreeEngine) Del(key []byte) (bool, error) {
	err := wal.WriteWAL(e.WALFile, &wal.WALEntry{Op: 1, Key: key})
	if err != nil {
		return false, err
	}
	return e.Tree.Del(key)
}

// TODO: Get, Scan, Close implementations
