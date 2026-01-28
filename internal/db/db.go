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

	// Encode key from primare *Record key values
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

func getTableDef(db *DB, table string) *TableDef {
	switch table {
	case MetaTable.Name:
		return MetaTable
	case TableCatalog.Name:
		return TableCatalog
	default:
		return nil
	}
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

func (db *DB) scanByDef(tdef *TableDef, startRec, endRec *Record, fn func(rec *Record) bool) error {
	var startKey, endKey []byte
	if startRec != nil {
		if err := reorderRecord(tdef, startRec); err != nil {
			return err
		}
		startKey = encodeKey(tdef.Prefix, startRec.Vals[:tdef.PKeyN])
	}
	if endRec != nil {
		if err := reorderRecord(tdef, endRec); err != nil {
			return err
		}
		endKey = encodeKey(tdef.Prefix, endRec.Vals[:tdef.PKeyN])
	}

	return db.KV.Scan(startKey, endKey, func(key, val []byte) bool {
		rec := &Record{
			Cols: append([]string{}, tdef.Cols...),
			Vals: make([]Value, len(tdef.Cols)),
		}
		keyVals, err := decodeValue(key)
		if err != nil {
			return false
		}
		for i := 0; i < int(tdef.PKeyN); i++ {
			rec.Vals[i] = keyVals[i]
		}
		valVals, err := decodeValue(val)
		if err != nil {
			return false
		}
		for i := int(tdef.PKeyN); i < len(tdef.Cols); i++ {
			rec.Vals[i] = valVals[i-int(tdef.PKeyN)]
		}
		return fn(rec)
	})
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

func (db *DB) Scan(table string, startRec, endRec *Record, fn func(rec *Record) bool) error {
	tdef := getTableDef(db, table)
	if tdef == nil {
		return errors.New("unknown table: " + table)
	}

	return db.scanByDef(tdef, startRec, endRec, fn)
}
