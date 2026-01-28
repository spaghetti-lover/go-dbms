package bptree_disk

import "bytes"

// Scan performs a range scan from startKey to endKey, invoking fn for each key-value pair.
func (bt *BPlusTree) Scan(startKey, endKey []byte, fn func(key, val []byte) bool) error {
	iter := bt.SeekGE(startKey)
	for iter.Valid() {
		kv := iter.Deref()
		if kv == nil {
			break
		}
		if endKey != nil && bytes.Compare(kv.Key[:kv.KeyLen], endKey) > 0 {
			break
		}
		if !fn(kv.Key[:kv.KeyLen], kv.Val[:kv.ValLen]) {
			break
		}
		iter.Next()
	}
	return nil
}
