package bptree_disk

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBPlusTree_Scan(t *testing.T) {
	tree := setupBPlusTree(t)

	// insert
	for i := 1; i <= 5; i++ {
		k, v := kv(i)
		err := tree.Insert(k, v)
		assert.Equal(t, nil, err)
	}

	// scan and verify
	kStart, _ := kv(1)
	kEnd, _ := kv(5)

	collected := make([][]byte, 0)
	err := tree.Scan(kStart, kEnd, func(key, val []byte) bool {
		collected = append(collected, append([]byte{}, key...))
		return true
	})
	assert.Equal(t, nil, err)
}

func TestBPlusTree_Scan_StopEarly(t *testing.T) {
	tree := setupBPlusTree(t)

	// insert
	for i := 1; i <= 5; i++ {
		k, v := kv(i)
		err := tree.Insert(k, v)
		assert.Equal(t, nil, err)
	}

	// scan and verify
	kStart, _ := kv(1)
	kEnd, _ := kv(5)

	collected := make([][]byte, 0)
	err := tree.Scan(kStart, kEnd, func(key, val []byte) bool {
		collected = append(collected, append([]byte{}, key...))
		return len(collected) < 2 // stop after collecting 2 keys
	})
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(collected))
}
