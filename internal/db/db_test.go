package db

import (
	"testing"

	"github.com/spaghetti-lover/go-db/pkg/kv"
	"github.com/stretchr/testify/assert"
)

func setupTestDB(t *testing.T) *DB {
	fileName := "test_db.db"
	tree, err := kv.NewBPTreeEngine(fileName)
	assert.NoError(t, err)
	kv := kv.NewKV(tree)
	db := &DB{KV: *kv, TableDefs: map[string]*TableDef{}}
	tdef := &TableDef{
		Name:   "People",
		Cols:   []string{"id", "name", "age"},
		Types:  []ValueType{ValueInt64, ValueBytes, ValueInt64},
		PKeyN:  1,
		Prefix: 1,
	}
	db.TableDefs["People"] = tdef
	return db
}

func TestDBScan_Primary(t *testing.T) {
	db := setupTestDB(t)
	tdef := db.TableDefs["People"]
	assert.NotNil(t, tdef)

	_ = db.Insert(tdef, &Record{Cols: []string{"id", "name", "age"}, Vals: []Value{NewInt64Value(1), NewBytesValue([]byte("Alice")), NewInt64Value(30)}})
	_ = db.Insert(tdef, &Record{Cols: []string{"id", "name", "age"}, Vals: []Value{NewInt64Value(2), NewBytesValue([]byte("Bob")), NewInt64Value(25)}})
	_ = db.Insert(tdef, &Record{Cols: []string{"id", "name", "age"}, Vals: []Value{NewInt64Value(3), NewBytesValue([]byte("Charlie")), NewInt64Value(35)}})

	var names []string
	err := db.Scan("People", nil, nil, func(r *Record) bool {
		names = append(names, string(r.Vals[1].Bytes))
		return true
	})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"Alice", "Bob", "Charlie"}, names)
}

func TestDBScan_Secondary(t *testing.T) {
	db := setupTestDB(t)
	tdef := db.TableDefs["People"]
	tdef.Indexes = []IndexDef{
		{
			Name:   "idx_name",
			Cols:   []string{"name"},
			Prefix: 2,
		},
	}

	_ = db.Insert(tdef, &Record{Cols: []string{"id", "name", "age"}, Vals: []Value{NewInt64Value(1), NewBytesValue([]byte("Alice")), NewInt64Value(30)}})
	_ = db.Insert(tdef, &Record{Cols: []string{"id", "name", "age"}, Vals: []Value{NewInt64Value(2), NewBytesValue([]byte("Bob")), NewInt64Value(25)}})
	_ = db.Insert(tdef, &Record{Cols: []string{"id", "name", "age"}, Vals: []Value{NewInt64Value(3), NewBytesValue([]byte("Charlie")), NewInt64Value(35)}})

	// Scan with index "name" = "Bob"
	start := &Record{Cols: []string{"name"}, Vals: []Value{NewBytesValue([]byte("Bob"))}}
	end := &Record{Cols: []string{"name"}, Vals: []Value{NewBytesValue([]byte("Bob"))}}

	scanner, err := db.NewScanner(tdef, &tdef.Indexes[0], start, end)
	assert.NoError(t, err)

	var ids []int64
	for scanner.Valid() {
		rec, err := scanner.Deref()
		assert.NoError(t, err)
		ids = append(ids, rec.Vals[0].I64)
		scanner.Next()
	}
	assert.Equal(t, []int64{2}, ids)
}
