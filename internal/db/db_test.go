package db

import (
	"os"
	"testing"

	"github.com/spaghetti-lover/go-db/pkg/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestDB(t *testing.T) *DB {
	fileName := "test_db.db"
	// Clean up old test file
	os.Remove(fileName)

	tree, err := kv.NewBPTreeEngine(fileName)
	require.NoError(t, err)
	kvStore := kv.NewKV(tree)
	db := &DB{KV: *kvStore, TableDefs: map[string]*TableDef{}}
	tdef := &TableDef{
		Name:   "People",
		Cols:   []string{"id", "name", "age"},
		Types:  []ValueType{ValueInt64, ValueBytes, ValueInt64},
		PKeyN:  1,
		Prefix: 1,
	}
	db.TableDefs["People"] = tdef

	// Cleanup after test
	t.Cleanup(func() {
		os.Remove(fileName)
	})

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

func TestInsert(t *testing.T) {
	db := setupTestDB(t)
	tdef := db.TableDefs["People"]

	rec := &Record{
		Cols: []string{"id", "name", "age"},
		Vals: []Value{NewInt64Value(1), NewBytesValue([]byte("Alice")), NewInt64Value(30)},
	}

	err := db.Insert(tdef, rec)
	assert.NoError(t, err)

	// Verify insertion by scanning
	var found bool
	err = db.Scan("People", nil, nil, func(r *Record) bool {
		if r.Vals[0].I64 == 1 {
			assert.Equal(t, "Alice", string(r.Vals[1].Bytes))
			assert.Equal(t, int64(30), r.Vals[2].I64)
			found = true
		}
		return true
	})
	assert.NoError(t, err)
	assert.True(t, found, "Inserted record should be found")
}

func TestUpdate(t *testing.T) {
	db := setupTestDB(t)
	tdef := db.TableDefs["People"]

	// Insert initial record
	rec := &Record{
		Cols: []string{"id", "name", "age"},
		Vals: []Value{NewInt64Value(1), NewBytesValue([]byte("Alice")), NewInt64Value(30)},
	}
	err := db.Insert(tdef, rec)
	require.NoError(t, err)

	// Update the record
	updatedRec := &Record{
		Cols: []string{"id", "name", "age"},
		Vals: []Value{NewInt64Value(1), NewBytesValue([]byte("Alice Updated")), NewInt64Value(31)},
	}
	err = db.Update(tdef, updatedRec)
	assert.NoError(t, err)

	// Verify update
	var found bool
	err = db.Scan("People", nil, nil, func(r *Record) bool {
		if r.Vals[0].I64 == 1 {
			assert.Equal(t, "Alice Updated", string(r.Vals[1].Bytes))
			assert.Equal(t, int64(31), r.Vals[2].I64)
			found = true
		}
		return true
	})
	assert.NoError(t, err)
	assert.True(t, found, "Updated record should be found")
}

func TestDelete(t *testing.T) {
	db := setupTestDB(t)
	tdef := db.TableDefs["People"]

	// Insert record
	rec := &Record{
		Cols: []string{"id", "name", "age"},
		Vals: []Value{NewInt64Value(1), NewBytesValue([]byte("Alice")), NewInt64Value(30)},
	}
	err := db.Insert(tdef, rec)
	require.NoError(t, err)

	// Delete the record
	err = db.Delete(tdef, rec)
	assert.NoError(t, err)

	// Verify deletion
	var found bool
	err = db.Scan("People", nil, nil, func(r *Record) bool {
		if r.Vals[0].I64 == 1 {
			found = true
		}
		return true
	})
	assert.NoError(t, err)
	assert.False(t, found, "Deleted record should not be found")
}
