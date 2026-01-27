package bptree_disk

import (
	"testing"

	"github.com/spaghetti-lover/go-db/internal/storage/disk"
	"github.com/stretchr/testify/assert"
)

func TestBPlusTree_Find_Simple(t *testing.T) {
	tree := setupBPlusTree(t)

	// insert
	for i := 1; i <= 5; i++ {
		k, v := kv(i)
		err := tree.Insert(k, v)
		assert.Equal(t, nil, err)
	}

	// find
	for i := 1; i <= 5; i++ {
		k, v := kv(i)
		kv, err := tree.Find(k)
		assert.Equal(t, nil, err)
		assert.Equal(t, v, kv.Val[len(kv.Val)-int(kv.ValLen):])
	}

}

func TestBPlusTree_Find_NotFound(t *testing.T) {
	tree := setupBPlusTree(t)

	// insert
	for i := 1; i <= 5; i++ {
		k, v := kv(i)
		err := tree.Insert(k, v)
		assert.Equal(t, nil, err)
	}

	// find not exist key
	_, err := tree.Find([]byte{10})
	assert.ErrorIs(t, err, disk.ErrKeyNotFound)
}

func TestBPlusTree_Find_EmptyTree(t *testing.T) {
	tree := setupBPlusTree(t)

	// find in empty tree
	_, err := tree.Find([]byte{1})
	assert.ErrorIs(t, err, disk.ErrKeyNotFound)
}
