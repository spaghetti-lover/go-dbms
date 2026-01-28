package bptree_disk

import "github.com/spaghetti-lover/go-db/internal/storage/disk"

type BIter struct {
	tree    *BPlusTree
	leafPID uint64
	leaf    *disk.LeafPage
	buf     []byte
	idx     int
	valid   bool
}

// Valid returns whether the iterator is valid
func (it *BIter) Valid() bool {
	return it != nil && it.valid
}

// Deref returns the current key-value pair the iterator is pointing to
func (it *BIter) Deref() *disk.KeyVal {
	if !it.Valid() {
		return nil
	}
	return &it.leaf.KVs[it.idx]
}

// Next: advances the iterator to the next key-value pair
func (it *BIter) Next() {
	if !it.Valid() {
		return
	}

	it.idx++

	if it.idx < int(it.leaf.NKV) {
		return
	}

	// move to next leaf
	nextPID := it.leaf.Header.NextPagePointer
	if nextPID == 0 {
		it.valid = false
		return
	}

	leaf, buf, err := it.tree.loadLeaf(nextPID)
	if err != nil {
		it.valid = false
		return
	}

	it.leafPID = nextPID
	it.leaf = leaf
	it.buf = buf
	it.idx = 0
	it.valid = true
}