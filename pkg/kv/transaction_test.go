package kv

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestKV(t *testing.T) *KV {
	fileName := "test_tx.db"
	os.Remove(fileName)

	engine, err := NewBPTreeEngine(fileName)
	require.NoError(t, err)

	kv := NewKV(engine)

	t.Cleanup(func() {
		kv.Close()
		os.Remove(fileName)
	})

	return kv
}

func TestTransaction_BasicCommit(t *testing.T) {
	kv := setupTestKV(t)

	// Start transaction
	tx := &KVTX{}
	kv.Begin(tx)

	// Write within transaction
	err := tx.Set([]byte("key1"), []byte("value1"))
	require.NoError(t, err)

	err = tx.Set([]byte("key2"), []byte("value2"))
	require.NoError(t, err)

	// Commit
	err = kv.Commit(tx)
	require.NoError(t, err)

	// Verify data persisted
	val, ok := kv.Get([]byte("key1"))
	assert.True(t, ok)
	assert.Equal(t, []byte("value1"), val)

	val, ok = kv.Get([]byte("key2"))
	assert.True(t, ok)
	assert.Equal(t, []byte("value2"), val)
}

func TestTransaction_Rollback(t *testing.T) {
	kv := setupTestKV(t)

	// Insert initial data
	err := kv.Set([]byte("key1"), []byte("original"))
	require.NoError(t, err)

	// Start transaction
	tx := &KVTX{}
	kv.Begin(tx)

	// Write within transaction
	err = tx.Set([]byte("key1"), []byte("modified"))
	require.NoError(t, err)

	// Abort instead of commit
	kv.Abort(tx)

	// Verify original data preserved
	val, ok := kv.Get([]byte("key1"))
	assert.True(t, ok)
	assert.Equal(t, []byte("original"), val)
}

func TestTransaction_ReadYourOwnWrites(t *testing.T) {
	kv := setupTestKV(t)

	// Insert initial data
	err := kv.Set([]byte("key1"), []byte("original"))
	require.NoError(t, err)

	// Start transaction
	tx := &KVTX{}
	kv.Begin(tx)

	// Read original
	val, ok := tx.Get([]byte("key1"))
	assert.True(t, ok)
	assert.Equal(t, []byte("original"), val)

	// Write within transaction
	err = tx.Set([]byte("key1"), []byte("modified"))
	require.NoError(t, err)

	// Should read own write
	val, ok = tx.Get([]byte("key1"))
	assert.True(t, ok)
	assert.Equal(t, []byte("modified"), val)

	// Delete within transaction
	err = tx.Del([]byte("key1"))
	require.NoError(t, err)

	// Should see deletion
	val, ok = tx.Get([]byte("key1"))
	assert.False(t, ok)

	kv.Commit(tx)
}

func TestTransaction_DeleteInsideTransaction(t *testing.T) {
	kv := setupTestKV(t)

	// Insert initial data
	err := kv.Set([]byte("key1"), []byte("value1"))
	require.NoError(t, err)

	// Start transaction and delete
	tx := &KVTX{}
	kv.Begin(tx)

	err = tx.Del([]byte("key1"))
	require.NoError(t, err)

	err = kv.Commit(tx)
	require.NoError(t, err)

	// Verify deletion
	_, ok := kv.Get([]byte("key1"))
	assert.False(t, ok)
}

func TestTransaction_ConflictDetection(t *testing.T) {
	kv := setupTestKV(t)

	// Insert initial data
	err := kv.Set([]byte("key1"), []byte("original"))
	require.NoError(t, err)

	// Start TX1
	tx1 := &KVTX{}
	kv.Begin(tx1)

	// TX1 reads key1
	val, ok := tx1.Get([]byte("key1"))
	assert.True(t, ok)
	assert.Equal(t, []byte("original"), val)

	// TX2 modifies and commits before TX1
	tx2 := &KVTX{}
	kv.Begin(tx2)
	err = tx2.Set([]byte("key1"), []byte("modified_by_tx2"))
	require.NoError(t, err)
	err = kv.Commit(tx2)
	require.NoError(t, err)

	// TX1 tries to commit - should conflict
	err = tx1.Set([]byte("key1"), []byte("modified_by_tx1"))
	require.NoError(t, err)
	err = kv.Commit(tx1)
	assert.ErrorIs(t, err, ErrTxConflict)
}

func TestTransaction_NoConflictOnDifferentKeys(t *testing.T) {
	kv := setupTestKV(t)

	// Insert initial data
	err := kv.Set([]byte("key1"), []byte("value1"))
	require.NoError(t, err)
	err = kv.Set([]byte("key2"), []byte("value2"))
	require.NoError(t, err)

	// TX1 reads key1
	tx1 := &KVTX{}
	kv.Begin(tx1)
	_, _ = tx1.Get([]byte("key1"))

	// TX2 modifies key2 (different key)
	tx2 := &KVTX{}
	kv.Begin(tx2)
	err = tx2.Set([]byte("key2"), []byte("modified"))
	require.NoError(t, err)
	err = kv.Commit(tx2)
	require.NoError(t, err)

	// TX1 should commit successfully (no conflict)
	err = tx1.Set([]byte("key1"), []byte("also_modified"))
	require.NoError(t, err)
	err = kv.Commit(tx1)
	assert.NoError(t, err)
}

func TestTransaction_ConcurrentAccess(t *testing.T) {
	kv := setupTestKV(t)

	var wg sync.WaitGroup
	successCount := 0
	conflictCount := 0
	var mu sync.Mutex

	// Run 10 concurrent transactions
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			tx := &KVTX{}
			kv.Begin(tx)

			// Read shared key
			tx.Get([]byte("counter"))

			// Write to shared key
			tx.Set([]byte("counter"), []byte{byte(id)})

			err := kv.Commit(tx)

			mu.Lock()
			if err == nil {
				successCount++
			} else {
				conflictCount++
			}
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	// At least one should succeed, others may conflict
	assert.GreaterOrEqual(t, successCount, 1)
	t.Logf("Success: %d, Conflicts: %d", successCount, conflictCount)
}

func TestTransaction_AbortedTxOperations(t *testing.T) {
	kv := setupTestKV(t)

	tx := &KVTX{}
	kv.Begin(tx)
	kv.Abort(tx)

	// Operations on aborted TX should fail
	_, ok := tx.Get([]byte("key"))
	assert.False(t, ok)

	err := tx.Set([]byte("key"), []byte("val"))
	assert.ErrorIs(t, err, ErrTxAborted)

	err = tx.Del([]byte("key"))
	assert.ErrorIs(t, err, ErrTxAborted)

	err = kv.Commit(tx)
	assert.ErrorIs(t, err, ErrTxAborted)
}
