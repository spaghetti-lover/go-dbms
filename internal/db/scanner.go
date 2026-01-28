package db

import (
	"bytes"

	"github.com/spaghetti-lover/go-db/internal/storage/index/bptree_disk"
)

type Scanner struct {
	iter     *bptree_disk.BIter
	db       *DB
	tableDef *TableDef
	indexDef *IndexDef // nil = primary scan
	endKey   []byte    // nil = no upper bound
}

// Valid returns true if the iterators is valid and not past endKey
func (s *Scanner) Valid() bool {
	if !s.iter.Valid() {
		return false
	}
	if s.endKey == nil {
		return true
	}
	return bytes.Compare(s.iter.Deref().Key[:s.iter.Deref().KeyLen], s.endKey) <= 0
}

// Next advances the iterator
func (s *Scanner) Next() {
	s.iter.Next()
}

// Deref returns the current record
func (s *Scanner) Deref() (*Record, error) {
	if s.indexDef == nil {
		// Primary scan: decode directly
		kv := s.iter.Deref()
		rec, err := decodeRecord(s.tableDef, kv.Key[:kv.KeyLen], kv.Val[:kv.ValLen])
		if err != nil {
			return nil, err
		}
		return rec, nil
	}
	// Secondary index scan: extract PK from index key, fetch value from primary
	idxKey := s.iter.Deref().Key[:s.iter.Deref().KeyLen]
	pk := extractPrimaryKeyFromIndexKey(idxKey, s.tableDef)
	val, ok := s.db.KV.Get(pk)
	if !ok {
		panic("conrrupted index: PK not found")
	}
	rec, err := decodeRecord(s.tableDef, pk, val)
	if err != nil {
		return nil, err
	}
	return rec, nil
}

func makeMinPK(n int) []Value {
	vals := make([]Value, n)
	for i := 0; i < n; i++ {
		vals[i] = NewBytesValue([]byte{})
	}
	return vals
}

func makeMaxPK(n int) []Value {
	vals := make([]Value, n)
	for i := 0; i < n; i++ {
		vals[i] = NewBytesValue(bytes.Repeat([]byte{0xFF}, 16))
	}
	return vals
}
