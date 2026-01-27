package bptree_disk

import "github.com/spaghetti-lover/go-db/internal/storage/disk"

func (t *BPlusTree) Insert(key, value []byte) error

func (t *BPlusTree) insertRecursive(nodePID uint64, key *disk.KeyEntry, kv *disk.KeyVal) (InsertResult, error)
