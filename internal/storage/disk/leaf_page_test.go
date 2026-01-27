package disk

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func kv(i int) *KeyVal {
	kv := NewKeyValFromInt(int64(i), int64(i*10))
	return &kv
}

func TestNewLeafPage(t *testing.T) {
	leaf := NewLeafPage()

	assert.Equal(t, uint16(0), leaf.NKV)
	assert.Equal(t, PageTypeLeaf, int(leaf.Header.PageType))
	assert.Equal(t, uint64(0), leaf.Header.NextPagePointer)
}

func TestLeafPage_InsertKV_Ascending(t *testing.T) {
	leaf := NewLeafPage()

	leaf.InsertKV(kv(1))
	leaf.InsertKV(kv(3))
	leaf.InsertKV(kv(5))

	assert.Equal(t, uint16(3), leaf.NKV)

	assert.Equal(t, 0, leaf.KVs[0].Compare(kv(1)))
	assert.Equal(t, 0, leaf.KVs[1].Compare(kv(3)))
	assert.Equal(t, 0, leaf.KVs[2].Compare(kv(5)))
}

func TestLeafPage_InsertKV_Middle(t *testing.T) {
	leaf := NewLeafPage()

	leaf.InsertKV(kv(1))
	leaf.InsertKV(kv(5))
	leaf.InsertKV(kv(3))

	assert.Equal(t, uint16(3), leaf.NKV)

	assert.Equal(t, 0, leaf.KVs[0].Compare(kv(1)))
	assert.Equal(t, 0, leaf.KVs[1].Compare(kv(3)))
	assert.Equal(t, 0, leaf.KVs[2].Compare(kv(5)))
}

func TestLeafPage_FindLastLE(t *testing.T) {
	leaf := NewLeafPage()

	leaf.InsertKV(kv(10))
	leaf.InsertKV(kv(20))
	leaf.InsertKV(kv(30))

	assert.Equal(t, -1, leaf.FindLastLE(kv(5)))
	assert.Equal(t, 0, leaf.FindLastLE(kv(10)))
	assert.Equal(t, 1, leaf.FindLastLE(kv(25)))
	assert.Equal(t, 2, leaf.FindLastLE(kv(30)))
	assert.Equal(t, 2, leaf.FindLastLE(kv(40)))
}

func TestLeafPage_DelKV(t *testing.T) {
	leaf := NewLeafPage()

	leaf.InsertKV(kv(1))
	leaf.InsertKV(kv(3))
	leaf.InsertKV(kv(5))

	leaf.DelKV(kv(3))

	assert.Equal(t, uint16(2), leaf.NKV)
	assert.Equal(t, 0, leaf.KVs[0].Compare(kv(1)))
	assert.Equal(t, 0, leaf.KVs[1].Compare(kv(5)))
}

func TestLeafPage_Split(t *testing.T) {
	leaf := NewLeafPage()

	leaf.InsertKV(kv(1))
	leaf.InsertKV(kv(2))
	leaf.InsertKV(kv(3))
	leaf.InsertKV(kv(4))

	right, promoteKey := leaf.Split()

	// left: [1,2], right: [3,4]
	assert.Equal(t, uint16(2), leaf.NKV)
	assert.Equal(t, uint16(2), right.NKV)

	assert.Equal(t, 0, leaf.KVs[0].Compare(kv(1)))
	assert.Equal(t, 0, leaf.KVs[1].Compare(kv(2)))

	assert.Equal(t, 0, right.KVs[0].Compare(kv(3)))
	assert.Equal(t, 0, right.KVs[1].Compare(kv(4)))

	// promote key = first key of right leaf
	assert.Equal(t, 0, promoteKey.Compare(&KeyEntry{Key: right.KVs[0].Key}))
}

func TestLeafPage_Serialization(t *testing.T) {
	leaf := NewLeafPage()

	leaf.InsertKV(kv(10))
	leaf.InsertKV(kv(20))

	buf := new(bytes.Buffer)
	err := leaf.WriteToBuffer(buf)
	require.NoError(t, err)

	cloned := NewLeafPage()
	err = cloned.ReadFromBuffer(buf, true)
	require.NoError(t, err)

	assert.Equal(t, uint16(2), cloned.NKV)
	assert.Equal(t, 0, cloned.KVs[0].Compare(kv(10)))
	assert.Equal(t, 0, cloned.KVs[1].Compare(kv(20)))
}
