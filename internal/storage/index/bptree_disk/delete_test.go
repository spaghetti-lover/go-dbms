package bptree_disk

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/spaghetti-lover/go-db/internal/storage/disk"
)

func kv(key int) ([]byte, []byte) {
	return []byte{byte(key)}, []byte{byte(key + 100)}
}

var fileName = "test_db.db"

func setupBPlusTree(t *testing.T) *BPlusTree {
	allocator := disk.NewFileAllocator()
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	assert.NoError(t, err)
	pager := disk.NewPager(file, allocator)
	btree, err := NewBPlusTree(pager)
	assert.Equal(t, nil, err)

	meta := disk.NewMetaPage()
	buf := make([]byte, disk.BLOCK_SIZE)
	writer := bytes.NewBuffer(buf[:0])
	err = meta.WriteToBuffer(writer)
	assert.Equal(t, nil, err)

	err = pager.FlushPage(btree.metaPID)
	assert.Equal(t, nil, err)
	return btree
}

func TestBPlusTree_Delete_Simple(t *testing.T) {

	tree := setupBPlusTree(t)

	// insert
	for i := 1; i <= 5; i++ {
		k, v := kv(i)
		require.NoError(t, tree.Insert(k, v))
	}

	// delete
	ok, err := tree.Delete(disk.NewKeyEntryFromBytes([]byte{3}).Key[:])
	require.NoError(t, err)
	assert.True(t, ok)

	// not found after delete
	_, err = tree.Find(disk.NewKeyEntryFromBytes([]byte{3}).Key[:])
	assert.ErrorIs(t, err, disk.ErrKeyNotFound)

	// others still exist
	for _, i := range []int{1, 2, 4, 5} {
		_, err := tree.Find(disk.NewKeyEntryFromBytes([]byte{byte(i)}).Key[:])
		require.NoError(t, err)
	}
}

func TestBPlusTree_Delete_NotFound(t *testing.T) {
	tree := setupBPlusTree(t)

	k, v := kv(1)
	require.NoError(t, tree.Insert(k, v))

	ok, err := tree.Delete(disk.NewKeyEntryFromBytes([]byte{2}).Key[:])
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestBPlusTree_Delete_MergeAndRootShrink(t *testing.T) {
	tree := setupBPlusTree(t)

	// Insert multiple keys to cause splits
	numInserts := 20
	for i := 1; i <= numInserts+1; i++ {
		k, v := kv(i)
		require.NoError(t, tree.Insert(k, v))
	}

	// Delete keys to cause merges and root shrink
	for i := 1; i <= numInserts; i++ {
		ok, err := tree.Delete(disk.NewKeyEntryFromBytes([]byte{byte(i)}).Key[:])
		require.NoError(t, err)
		require.True(t, ok)
	}

	// Verify only the last key remains
	_, err := tree.Find(disk.NewKeyEntryFromBytes([]byte{byte(numInserts + 1)}).Key[:])
	require.NoError(t, err)

	// Root should be a leaf now
	rootPID, err := tree.rootPID()
	require.NoError(t, err)

	node, _, err := tree.loadNode(rootPID)
	require.NoError(t, err)
	assert.True(t, node.IsLeaf(), "root should shrink to leaf")
}
