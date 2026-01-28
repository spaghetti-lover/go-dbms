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
	startKey []byte    // prefix for validation
	endKey   []byte    // nil = no upper bound
}

// Valid returns true if the iterators is valid and not past endKey
func (s *Scanner) Valid() bool {
	if !s.iter.Valid() {
		return false
	}
	kv := s.iter.Deref()
	key := kv.GetRightAlignedKey()
	// Check that key starts with the correct prefix
	if len(key) == 0 || len(s.startKey) == 0 || key[0] != s.startKey[0] {
		return false
	}
	if s.endKey == nil {
		return true
	}
	return bytes.Compare(key, s.endKey) <= 0
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
		key := kv.GetRightAlignedKey()
		val := kv.GetRightAlignedValue()
		rec, err := decodeRecord(s.tableDef, key, val)
		if err != nil {
			return nil, err
		}
		return rec, nil
	}
	// Secondary index scan: extract PK from index key, fetch value from primary
	kv := s.iter.Deref()
	idxKey := kv.GetRightAlignedKey()
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

func makeMinPK(tdef *TableDef) []Value {
	vals := make([]Value, tdef.PKeyN)
	for i := 0; i < int(tdef.PKeyN); i++ {
		switch tdef.Types[i] {
		case ValueInt64:
			vals[i] = NewInt64Value(0)
		case ValueBytes:
			vals[i] = NewBytesValue([]byte{})
		}
	}
	return vals
}

func makeMaxPK(tdef *TableDef) []Value {
	vals := make([]Value, tdef.PKeyN)
	for i := 0; i < int(tdef.PKeyN); i++ {
		switch tdef.Types[i] {
		case ValueInt64:
			vals[i] = NewInt64Value(0x7FFFFFFFFFFFFFFF) // max int64
		case ValueBytes:
			vals[i] = NewBytesValue(bytes.Repeat([]byte{0xFF}, 16))
		}
	}
	return vals
}
