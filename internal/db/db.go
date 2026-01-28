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
	KV        kv.KV
	TableDefs map[string]*TableDef
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
	if err := db.KV.Set(key, val); err != nil {
		return err
	}

	// Insert secondary indexes
	pkVals := rec.Vals[:tdef.PKeyN]
	for _, idx := range tdef.Indexes {
		idxVals := extractIndexedValues(&idx, rec)
		// Index key = index prefix + indexed cols + primary key
		idxKey := encodeKey(idx.Prefix, append(idxVals, pkVals...))
		if err := db.KV.Set(idxKey, nil); err != nil {
			return err
		}
	}

	return nil
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

	// Update secondary indexes
	pkVals := rec.Vals[:tdef.PKeyN]
	for _, idx := range tdef.Indexes {
		idxKey := encodeIndexKey(&idx, rec, pkVals)
		if err := db.KV.Set(idxKey, []byte{}); err != nil {
			return err
		}
	}

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

	//Take old record
	oldRec := &Record{
		Cols: append([]string{}, tdef.Cols...),
		Vals: make([]Value, len(tdef.Cols)),
	}
	copy(oldRec.Vals, rec.Vals)
	if err := db.getByDef(tdef, oldRec); err != nil {
		return err
	}

	// delete secondary indexes
	pkVals := rec.Vals[:tdef.PKeyN]
	for _, idx := range tdef.Indexes {
		idxVals := extractIndexedValues(&idx, oldRec)
		idxKey := encodeKey(idx.Prefix, append(idxVals, pkVals...))
		_, _ = db.KV.Del(idxKey)
	}

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

func (db *DB) Scan(table string, startRec, endRec *Record, fn func(rec *Record) bool) error {
	tdef := db.TableDefs[table]
	if tdef == nil {
		return errors.New("unknown table: " + table)
	}

	scanner, err := db.NewScanner(tdef, nil, startRec, endRec)
	if err != nil {
		return err
	}

	for scanner.Valid() {
		ok, err := scanner.Deref()
		if err != nil {
			return err
		}
		if !fn(ok) {
			break
		}
		scanner.Next()
	}

	return nil
}

func (db *DB) NewScanner(tdef *TableDef, indexDef *IndexDef, startRec, endRec *Record) (*Scanner, error) {
	var startKey, endKey []byte
	if indexDef == nil {
		// Primary scan
		if startRec != nil {
			if err := reorderRecord(tdef, startRec); err != nil {
				return nil, err
			}
			startKey = encodeKey(tdef.Prefix, startRec.Vals[:tdef.PKeyN])
		} else {
			startKey = []byte{tdef.Prefix}
		}
		if endRec != nil {
			if err := reorderRecord(tdef, endRec); err != nil {
				return nil, err
			}
			endKey = encodeKey(tdef.Prefix, endRec.Vals[:tdef.PKeyN])
		}
	} else {
		// Secondary index scan
		idxVals := extractIndexedValues(indexDef, startRec) // take indexed cols only
		pkMin := makeMinPK(tdef)
		startKey = encodeKey(indexDef.Prefix, append(idxVals, pkMin...))

		idxValsEnd := extractIndexedValues(indexDef, endRec)
		pkMax := makeMaxPK(tdef)
		endKey = encodeKey(indexDef.Prefix, append(idxValsEnd, pkMax...))
	}

	engine, ok := db.KV.Engine.(*kv.BPTreeEngine)
	if !ok {
		return nil, errors.New("engine does not support range scan")
	}

	iter := engine.Tree.SeekGE(startKey)

	return &Scanner{
		iter:     iter,
		db:       db,
		tableDef: tdef,
		indexDef: indexDef,
		startKey: startKey,
		endKey:   endKey,
	}, nil
}
