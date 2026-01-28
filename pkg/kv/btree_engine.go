package kv

import "github.com/spaghetti-lover/go-db/internal/storage/index/bptree_disk"

type BPTreeEngine struct {
	tree *bptree_disk.BPlusTree
}

func NewBPTreeEngine(file string) (*BPTreeEngine, error) {
	tree, err := bptree_disk.Open(file)
	if err != nil {
		return nil, err
	}
	return &BPTreeEngine{tree: tree}, nil
}

func (e *BPTreeEngine) Get(key []byte) ([]byte, bool) {
	kv, err := e.tree.Find(key)
	if err != nil {
		return nil, false
	}
	return kv.Value(), true
}

func (e *BPTreeEngine) Set(key, val []byte) error {
	return e.tree.Set(key, val)
}

func (e *BPTreeEngine) Del(key []byte) (bool, error) {
	return e.tree.Delete(key)
}

func (e *BPTreeEngine) Scan(startKey, endKey []byte, fn func(key, val []byte) bool) error {
	return e.tree.Scan(startKey, endKey, fn)
}

func (e *BPTreeEngine) Close() error {
	return e.tree.Close()
}
