package kv

import (
	"bytes"
	"errors"
)

// Flags for pending operations
const (
	FLAG_UPDATED byte = 1
	FLAG_DELETED byte = 2
)

var (
	ErrTxConflict = errors.New("transaction conflict detected")
	ErrTxAborted  = errors.New("transaction was aborted")
)

// pendingOp represents a pending write operation in a transaction
type pendingOp struct {
	flag  byte
	value []byte
}

// StoreKey represents a key for conflict detection
type StoreKey struct {
	key []byte
}

// CommittedTX represents a committed transaction for history tracking
type CommittedTX struct {
	version uint64
	writes  []StoreKey
}

// KVTX represents a transaction on the KV store
type KVTX struct {
	kv      *KV
	meta    []byte               // for rollback
	version uint64               // version when TX started
	pending map[string]pendingOp // pending writes in this TX
	reads   []StoreKey           // keys read (for conflict detection)
	aborted bool                 // whether TX was aborted
}

// Begin starts a new transaction
func (kv *KV) Begin(tx *KVTX) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	tx.kv = kv
	tx.version = kv.version
	tx.pending = make(map[string]pendingOp)
	tx.reads = nil
	tx.aborted = false
	tx.meta = loadMetaFromDisk(kv)
}

// Commit ends a transaction: commit updates; rollback on error
func (kv *KV) Commit(tx *KVTX) error {
	if tx.aborted {
		return ErrTxAborted
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Conflict detection
	if detectConflicts(kv, tx) {
		// Rollback
		writeMetaToDisk(kv, tx.meta)
		return ErrTxConflict
	}

	// Apply pending writes to storage
	var writes []StoreKey
	for keyStr, op := range tx.pending {
		key := []byte(keyStr)
		writes = append(writes, StoreKey{key: key})

		switch op.flag {
		case FLAG_UPDATED:
			if err := kv.Engine.Set(key, op.value); err != nil {
				writeMetaToDisk(kv, tx.meta)
				return err
			}
		case FLAG_DELETED:
			if _, err := kv.Engine.Del(key); err != nil {
				writeMetaToDisk(kv, tx.meta)
				return err
			}
		}
	}

	// Update version and history
	kv.version++
	kv.history = append(kv.history, CommittedTX{
		version: kv.version,
		writes:  writes,
	})

	// Trim old history (keep last 100 entries)
	if len(kv.history) > 100 {
		kv.history = kv.history[len(kv.history)-100:]
	}

	return nil
}

// Abort ends a transaction: rollback all changes
func (kv *KV) Abort(tx *KVTX) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	tx.aborted = true
	tx.pending = nil
	tx.reads = nil
	writeMetaToDisk(tx.kv, tx.meta)
}

// Get retrieves a value within a transaction
// Supports read-your-own-writes
func (tx *KVTX) Get(key []byte) ([]byte, bool) {
	if tx.aborted {
		return nil, false
	}

	keyStr := string(key)

	// Check pending writes first (read your own writes)
	if op, ok := tx.pending[keyStr]; ok {
		switch op.flag {
		case FLAG_UPDATED:
			return op.value, true
		case FLAG_DELETED:
			return nil, false
		}
	}

	// Track this read for conflict detection
	tx.reads = append(tx.reads, StoreKey{key: key})

	// Read from snapshot (current storage state at TX start)
	tx.kv.mu.RLock()
	defer tx.kv.mu.RUnlock()
	return tx.kv.Engine.Get(key)
}

// Set stores a value within a transaction
func (tx *KVTX) Set(key, val []byte) error {
	if tx.aborted {
		return ErrTxAborted
	}

	tx.pending[string(key)] = pendingOp{
		flag:  FLAG_UPDATED,
		value: val,
	}
	return nil
}

// Del deletes a key within a transaction
func (tx *KVTX) Del(key []byte) error {
	if tx.aborted {
		return ErrTxAborted
	}

	tx.pending[string(key)] = pendingOp{
		flag:  FLAG_DELETED,
		value: nil,
	}
	return nil
}

// Scan performs a range scan within a transaction
func (tx *KVTX) Scan(startKey, endKey []byte, fn func(key, val []byte) bool) error {
	if tx.aborted {
		return ErrTxAborted
	}

	tx.kv.mu.RLock()
	defer tx.kv.mu.RUnlock()

	return tx.kv.Engine.Scan(startKey, endKey, func(key, val []byte) bool {
		keyStr := string(key)
		tx.reads = append(tx.reads, StoreKey{key: key})

		// Check if this key has pending changes
		if op, ok := tx.pending[keyStr]; ok {
			switch op.flag {
			case FLAG_UPDATED:
				return fn(key, op.value)
			case FLAG_DELETED:
				return true // skip deleted keys, continue scan
			}
		}
		return fn(key, val)
	})
}

// detectConflicts checks if any read keys were modified by other transactions
func detectConflicts(kv *KV, tx *KVTX) bool {
	for i := len(kv.history) - 1; i >= 0; i-- {
		// Only check transactions committed after this TX started
		if !versionBefore(tx.version, kv.history[i].version) {
			break
		}

		if rangesOverlap(tx.reads, kv.history[i].writes) {
			return true
		}
	}
	return false
}

// versionBefore checks if v1 is before v2
func versionBefore(v1, v2 uint64) bool {
	return v1 < v2
}

// rangesOverlap checks if any keys in reads overlap with writes
func rangesOverlap(reads, writes []StoreKey) bool {
	for _, r := range reads {
		for _, w := range writes {
			if bytes.Equal(r.key, w.key) {
				return true
			}
		}
	}
	return false
}

// loadMetaFromDisk loads metadata for rollback
// TODO: Implement actual disk loading based on your storage format
func loadMetaFromDisk(kv *KV) []byte {
	// Placeholder: return current state snapshot identifier
	return nil
}

// writeMetaToDisk writes metadata for rollback
// TODO: Implement actual disk writing based on your storage format
func writeMetaToDisk(kv *KV, meta []byte) {
	// Placeholder: restore state from meta
}
