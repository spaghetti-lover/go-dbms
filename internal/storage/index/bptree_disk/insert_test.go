package bptree_disk

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBPlusTree_Insert_Simple(t *testing.T) {
	tree := setupBPlusTree(t)

	// Insert
	for i := 1; i <= 5; i++ {
		k, v := kv(i)
		err := tree.Insert(k, v)
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

func TestBPlusTree_Insert_DuplicateKey(t *testing.T) {
	tree := setupBPlusTree(t)

	// Insert
	for i := 1; i <= 5; i++ {
		k, v := kv(i)
		err := tree.Insert(k, v)
		assert.Equal(t, nil, err)
	}

	// Insert duplicate key
	k, v := kv(3)
	err := tree.Insert(k, v)
	assert.ErrorIs(t, err, ErrDuplicateKey)
}

func TestBPlusTree_Insert_CauseSplit(t *testing.T) {
	tree := setupBPlusTree(t)

	// Insert
	numInserts := 20
	for i := 1; i <= numInserts; i++ {
		k, v := kv(i)
		err := tree.Insert(k, v)
		assert.Equal(t, nil, err)
	}

	// Find
	for i := 1; i <= numInserts; i++ {
		k, v := kv(i)
		kv, err := tree.Find(k)
		assert.Equal(t, nil, err)
		assert.Equal(t, v, kv.Val[len(kv.Val)-int(kv.ValLen):])
	}
}