package db

import (
	"errors"

	"github.com/spaghetti-lover/go-db/pkg/kv"
)

var (
	ErrNotFound = errors.New("record not found")
	ErrConflict = errors.New("record already exists")
)

type DB struct {
	KV kv.KV
}

// Reorder record columns to match table definition
func reorderRecord(tdef *TableDef, rec *Record) error {
	if len(rec.Cols) != len(tdef.Cols) {
		return errors.New("column count mismatch")
	}

	ordered := make([]Value, len(tdef.Cols))
	for i, col := range tdef.Cols {
		found := false

		for j, rcol := range rec.Cols {
			if col == rcol {
				ordered[i] = rec.Vals[j]
				found = true
				break
			}
		}

		if !found {
			return errors.New("missing column: " + col)
		}
	}

	rec.Cols = tdef.Cols
	rec.Vals = ordered
	return nil
}

// Get record by its primary key
func (db *DB) getByDef(tdef *TableDef, rec *Record) error {
	// Reorder record columns
	if err := reorderRecord(tdef, rec); err != nil {
		return err
	}

	// Encode key from primary key values
	key := encodeKey(tdef.Prefix, rec.Vals[:tdef.PKeyN])

	// Get from KV store
	raw, ok := db.KV.Get(key)
	if !ok {
		return ErrNotFound
	}

	// Decode values
	vals, err := decodeValue(raw)
	if err != nil {
		return err
	}

	// Fill in record values
	copy(rec.Vals[tdef.PKeyN:], vals)
	return nil
}

func (db *DB) insertByDef(tdef *TableDef, rec *Record) error {
	// Reorder record columns
	if err := reorderRecord(tdef, rec); err != nil {
		return err
	}

	// Check for conflict
	key := encodeKey(tdef.Prefix, rec.Vals[:tdef.PKeyN])

	if _, ok := db.KV.Get(key); ok {
		return ErrConflict
	}

	// Encode value
	val := encodeValue(rec.Vals[tdef.PKeyN:])

	// Set in KV store
	return db.KV.Set(key, val)
}

func (db *DB) updateByDef(tdef *TableDef, rec *Record) error {
	// Reorder record columns
	if err := reorderRecord(tdef, rec); err != nil {
		return err
	}

	// Encode key from primary key values
	key := encodeKey(tdef.Prefix, rec.Vals[:tdef.PKeyN])

	// Check existence
	if _, ok := db.KV.Get(key); !ok {
		return ErrNotFound
	}

	// Encode value
	val := encodeValue(rec.Vals[tdef.PKeyN:])

	// Set in KV store
	return db.KV.Set(key, val)
}

func (db *DB) deleteByDef(tdef *TableDef, rec *Record) error {
	// Reorder record columns
	if err := reorderRecord(tdef, rec); err != nil {
		return err
	}

	// Encode key from primary key values
	key := encodeKey(tdef.Prefix, rec.Vals[:tdef.PKeyN])

	// Delete from KV store
	ok, err := db.KV.Del(key)
	if err != nil {
		return err
	}

	if !ok {
		return ErrNotFound
	}

	return nil
}

func (db *DB) Get(table *TableDef, rec *Record) error {
	return db.getByDef(table, rec)
}

func (db *DB) Insert(table *TableDef, rec *Record) error {
	return db.insertByDef(table, rec)
}

func (db *DB) Update(table *TableDef, rec *Record) error {
	return db.updateByDef(table, rec)
}

func (db *DB) Upsert(table *TableDef, rec *Record) error {
	err := db.updateByDef(table, rec)
	if err == ErrNotFound {
		return db.insertByDef(table, rec)
	}
	return err
}

func (db *DB) Delete(table *TableDef, rec *Record) error {
	return db.deleteByDef(table, rec)
}
