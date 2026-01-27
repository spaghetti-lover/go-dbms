package bptree_disk

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBPlusTree_Set_Simple(t *testing.T) {
	tree := setupBPlusTree(t)

	// Set
	for i := 1; i <= 5; i++ {
		k, v := kv(i)
		err := tree.Set(k, v)
		assert.Equal(t, nil, err)
	}

	// Find
	for i := 1; i <= 5; i++ {
		k, v := kv(i)
		kv, err := tree.Find(k)
		assert.Equal(t, nil, err)
		assert.Equal(t, v, kv.Val[len(kv.Val)-int(kv.ValLen):])
	}
}

func TestBPlusTree_Set_UpdateExistingKey(t *testing.T) {
	tree := setupBPlusTree(t)

	// Set initial values
	for i := 1; i <= 5; i++ {
		k, v := kv(i)
		err := tree.Set(k, v)
		assert.Equal(t, nil, err)
	}

	// Update existing keys
	for i := 1; i <= 5; i++ {
		k := []byte{byte(i)}
		v := []byte{byte(i + 200)}
		err := tree.Set(k, v)
		assert.Equal(t, nil, err)
	}

	// Verify updates
	for i := 1; i <= 5; i++ {
		k := []byte{byte(i)}
		v := []byte{byte(i + 200)}
		kv, err := tree.Find(k)
		assert.Equal(t, nil, err)
		assert.Equal(t, v, kv.Val[len(kv.Val)-int(kv.ValLen):])
	}
}

func TestBPlusTree_Set_CauseSplit(t *testing.T) {
	tree := setupBPlusTree(t)

	// Set
	numSets := 20
	for i := 1; i <= numSets; i++ {
		k, v := kv(i)
		err := tree.Set(k, v)
		assert.Equal(t, nil, err)
	}

	// Find
	for i := 1; i <= numSets; i++ {
		k, v := kv(i)
		kv, err := tree.Find(k)
		assert.Equal(t, nil, err)
		assert.Equal(t, v, kv.Val[len(kv.Val)-int(kv.ValLen):])
	}
}