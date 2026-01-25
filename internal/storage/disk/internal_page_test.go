package disk

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInternalPage(t *testing.T) {
	node := NewBPlusTreeInternalPage()
	var c uint64 = 0
	key_3 := NewKeyEntryFromInt(3)
	node.InsertKV(&key_3, c)

	// [3]
	assert.Equal(t, uint16(1), node.nkey, "nkey should be 1")
	assert.Equal(t, 0, node.keys[0].compare(&key_3), "key[0] should be 3")

	key_10 := NewKeyEntryFromInt(10)
	node.InsertKV(&key_10, c)

	// [3, 10]
	assert.Equal(t, uint16(2), node.nkey, "nkey should be 2")
	assert.Equal(t, 0, node.keys[0].compare(&key_3), "key[0] should be 3")
	assert.Equal(t, 0, node.keys[1].compare(&key_10), "key[1] should be 10")

	key_5 := NewKeyEntryFromInt(5)
	node.InsertKV(&key_5, c)

	// [3, 5, 10]
	assert.Equal(t, uint16(3), node.nkey, "nkey should be 3")
	assert.Equal(t, 0, node.keys[0].compare(&key_3), "key[0] should be 3")
	assert.Equal(t, 0, node.keys[1].compare(&key_5), "key[1] should be 5")
	assert.Equal(t, 0, node.keys[2].compare(&key_10), "key[2] should be 10")

	key_12 := NewKeyEntryFromInt(12)
	node.InsertKV(&key_12, c)

	assert.Equal(t, uint16(4), node.nkey, "nkey should be 4")

	// [3, 5, 10, 12]
	newNode := node.Split()

	// [3, 5] [10, 12]
	assert.Equal(t, uint16(2), node.nkey, "node nkey should be 2 after split")
	assert.Equal(t, uint16(2), newNode.nkey, "newNode nkey should be 2 after split")

	assert.Equal(t, 0, node.keys[0].compare(&key_3), "node key[0] should be 3")
	assert.Equal(t, 0, node.keys[1].compare(&key_5), "node key[1] should be 5")

	assert.Equal(t, 0, newNode.keys[0].compare(&key_10), "newNode key[0] should be 10")
	assert.Equal(t, 0, newNode.keys[1].compare(&key_12), "newNode key[1] should be 12")

	buf := new(bytes.Buffer)
	err := node.writeToBuffer(buf)
	require.NoError(t, err, "writeToBuffer should not error")

	clonedNode := NewBPlusTreeInternalPage()
	err = clonedNode.readFromBuffer(buf, true)
	require.NoError(t, err, "readFromBuffer should not error")

	assert.Equal(t, uint16(2), clonedNode.nkey, "clonedNode nkey should be 2")
	assert.Equal(t, 0, clonedNode.keys[0].compare(&key_3), "clonedNode key[0] should be 3")
	assert.Equal(t, 0, clonedNode.keys[1].compare(&key_5), "clonedNode key[1] should be 5")
}

func TestNewIPage(t *testing.T) {
	node := NewBPlusTreeInternalPage()

	assert.Equal(t, uint16(0), node.nkey, "new page should have 0 keys")
	assert.Equal(t, uint8(1), node.header.pageType, "page type should be 1")
	assert.Equal(t, uint64(0), node.header.nextPagePointer, "next page pointer should be 0")
}

func TestKeyEntry_Compare(t *testing.T) {
	key3 := NewKeyEntryFromInt(3)
	key5 := NewKeyEntryFromInt(5)
	key3_dup := NewKeyEntryFromInt(3)

	assert.Equal(t, -1, key3.compare(&key5), "3 should be less than 5")
	assert.Equal(t, 1, key5.compare(&key3), "5 should be greater than 3")
	assert.Equal(t, 0, key3.compare(&key3_dup), "3 should equal 3")
}

func TestKeyEntry_Serialization(t *testing.T) {
	key := NewKeyEntryFromInt(12345)
	buf := new(bytes.Buffer)

	err := key.writeToBuffer(buf)
	require.NoError(t, err, "writeToBuffer should not error")

	var readKey KeyEntry
	err = readKey.readFromBuffer(buf)
	require.NoError(t, err, "readFromBuffer should not error")

	assert.Equal(t, 0, key.compare(&readKey), "serialized key should match original")
}

func TestBPlusTreeInternalPage_InsertKV_EmptyNode(t *testing.T) {
	node := NewBPlusTreeInternalPage()
	key := NewKeyEntryFromInt(5)
	var child uint64 = 100

	node.InsertKV(&key, child)

	assert.Equal(t, uint16(1), node.nkey, "nkey should be 1 after first insert")
	assert.Equal(t, 0, node.keys[0].compare(&key), "first key should match")
	assert.Equal(t, child, node.children[0], "first child should match")
}

func TestBPlusTreeInternalPage_InsertKV_MultipleAscending(t *testing.T) {
	node := NewBPlusTreeInternalPage()

	key1 := NewKeyEntryFromInt(1)
	key3 := NewKeyEntryFromInt(3)
	key5 := NewKeyEntryFromInt(5)

	node.InsertKV(&key1, 10)
	node.InsertKV(&key3, 30)
	node.InsertKV(&key5, 50)

	assert.Equal(t, uint16(3), node.nkey, "nkey should be 3")
	assert.Equal(t, 0, node.keys[0].compare(&key1), "keys should be in ascending order")
	assert.Equal(t, 0, node.keys[1].compare(&key3), "keys should be in ascending order")
	assert.Equal(t, 0, node.keys[2].compare(&key5), "keys should be in ascending order")
}

func TestBPlusTreeInternalPage_Split_EvenKeys(t *testing.T) {
	node := NewBPlusTreeInternalPage()

	key1 := NewKeyEntryFromInt(1)
	key2 := NewKeyEntryFromInt(2)
	key3 := NewKeyEntryFromInt(3)
	key4 := NewKeyEntryFromInt(4)

	node.InsertKV(&key1, 10)
	node.InsertKV(&key2, 20)
	node.InsertKV(&key3, 30)
	node.InsertKV(&key4, 40)

	newNode := node.Split()

	assert.Equal(t, uint16(2), node.nkey, "original node should have 2 keys")
	assert.Equal(t, uint16(2), newNode.nkey, "new node should have 2 keys")
	assert.Equal(t, 0, node.keys[0].compare(&key1), "first half should contain smaller keys")
	assert.Equal(t, 0, node.keys[1].compare(&key2), "first half should contain smaller keys")
	assert.Equal(t, 0, newNode.keys[0].compare(&key3), "second half should contain larger keys")
	assert.Equal(t, 0, newNode.keys[1].compare(&key4), "second half should contain larger keys")
}
