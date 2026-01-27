package bptree_disk

import "github.com/spaghetti-lover/go-db/internal/storage/disk"

func (t *BPlusTree) Delete(key []byte) (bool, error)
func (t *BPlusTree) deleteRecursive(nodePID uint64, key *disk.KeyEntry) (DeleteResult, error)
