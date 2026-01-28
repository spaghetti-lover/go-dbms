package bptree_disk

import (
	"github.com/spaghetti-lover/go-db/internal/storage/disk"
)

func (t *BPlusTree) Find(key []byte) (*disk.KeyVal, error) {
	currPID, err := t.rootPID()
	if err != nil {
		return nil, err
	}

	searchKV := disk.NewKeyValFromBytes(key, nil)

	for {
		node, _, err := t.loadNode(currPID)
		if err != nil {
			return nil, err
		}

		// Leaf
		if node.IsLeaf() {
			leaf := node.(*disk.LeafPage)

			pos := leaf.FindLastLE(&searchKV)
			if pos >= 0 && leaf.KVs[pos].Compare(&searchKV) == 0 {
				kv := leaf.KVs[pos] // copy
				return &kv, nil
			}

			return nil, disk.ErrKeyNotFound
		}

		// Internal
		internal := node.(*disk.InternalPage)
		pos := internal.FindLastLE(&disk.KeyEntry{
			KeyLen: searchKV.KeyLen,
			Key:    searchKV.Key,
		})

		currPID = internal.Children[pos+1]
	}
}
