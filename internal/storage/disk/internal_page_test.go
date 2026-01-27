package disk

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInternalPage(t *testing.T) {
	node := NewInternalPage()
	var c uint64 = 0
	key_3 := NewKeyEntryFromInt(3)
	node.InsertKV(key_3, c)

	// [3]
	assert.Equal(t, uint16(1), node.NKeys, "nkey should be 1")
	assert.Equal(t, 0, node.Keys[0].Compare(key_3), "key[0] should be 3")
	key_10 := NewKeyEntryFromInt(10)
	node.InsertKV(key_10, c)

	// [3, 10]
	assert.Equal(t, uint16(2), node.NKeys, "nkey should be 2")
	assert.Equal(t, 0, node.Keys[0].Compare(key_3), "key[0] should be 3")
	assert.Equal(t, 0, node.Keys[1].Compare(key_10), "key[1] should be 10")

	key_5 := NewKeyEntryFromInt(5)
	node.InsertKV(key_5, c)

	// [3, 5, 10]
	assert.Equal(t, uint16(3), node.NKeys, "nkey should be 3")
	assert.Equal(t, 0, node.Keys[0].Compare(key_3), "key[0] should be 3")
	assert.Equal(t, 0, node.Keys[1].Compare(key_5), "key[1] should be 5")
	assert.Equal(t, 0, node.Keys[2].Compare(key_10), "key[2] should be 10")
	key_12 := NewKeyEntryFromInt(12)
	node.InsertKV(key_12, c)

	assert.Equal(t, uint16(4), node.NKeys, "nkey should be 4")

	// [3, 5, 10, 12]
	newNode, middleKey := node.Split()

	// [3, 5] [10, 12]
	assert.Equal(t, uint16(2), node.NKeys, "node nkey should be 2 after split")
	assert.Equal(t, uint16(2), newNode.NKeys, "newNode nkey should be 2 after split")

	assert.Equal(t, 0, node.Keys[0].Compare(key_3), "node key[0] should be 3")
	assert.Equal(t, 0, node.Keys[1].Compare(key_5), "node key[1] should be 5")

	assert.Equal(t, 0, newNode.Keys[0].Compare(key_10), "newNode key[0] should be 10")
	assert.Equal(t, 0, newNode.Keys[1].Compare(key_12), "newNode key[1] should be 12")

	assert.Equal(t, 0, middleKey.Compare(key_10), "middle key should be 10")

	buf := new(bytes.Buffer)
	err := node.WriteToBuffer(buf)
	require.NoError(t, err, "writeToBuffer should not error")

	clonedNode := NewInternalPage()
	err = clonedNode.ReadFromBuffer(buf, true)
	require.NoError(t, err, "readFromBuffer should not error")

	assert.Equal(t, uint16(2), clonedNode.NKeys, "clonedNode nkey should be 2")
	assert.Equal(t, 0, clonedNode.Keys[0].Compare(key_3), "clonedNode key[0] should be 3")
	assert.Equal(t, 0, clonedNode.Keys[1].Compare(key_5), "clonedNode key[1] should be 5")
}

func TestNewIPage(t *testing.T) {
	node := NewInternalPage()

	assert.Equal(t, uint16(0), node.NKeys, "new page should have 0 keys")
	assert.Equal(t, uint8(1), node.Header.PageType, "page type should be 1")
	assert.Equal(t, uint64(0), node.Header.NextPagePointer, "next page pointer should be 0")
}

func TestKeyEntry_Compare(t *testing.T) {
	key3 := NewKeyEntryFromInt(3)
	key5 := NewKeyEntryFromInt(5)
	key3_dup := NewKeyEntryFromInt(3)

	assert.Equal(t, -1, key3.Compare(key5), "3 should be less than 5")
	assert.Equal(t, 1, key5.Compare(key3), "5 should be greater than 3")
	assert.Equal(t, 0, key3.Compare(key3_dup), "3 should equal 3")
}

func TestKeyEntry_Serialization(t *testing.T) {
	key := NewKeyEntryFromInt(12345)
	buf := new(bytes.Buffer)

	err := key.writeToBuffer(buf)
	require.NoError(t, err, "writeToBuffer should not error")

	var readKey KeyEntry
	err = readKey.readFromBuffer(buf)
	require.NoError(t, err, "readFromBuffer should not error")

	assert.Equal(t, 0, key.Compare(&readKey), "serialized key should match original")
}

func TestInternalPage_InsertKV_AfterChildSplit(t *testing.T) {
	node := NewInternalPage()

	node.Children[0] = 10

	key := NewKeyEntryFromInt(5)
	rightChild := uint64(20)

	node.InsertKV(key, rightChild)

	assert.Equal(t, uint16(1), node.NKeys)

	assert.Equal(t, 0, node.Keys[0].Compare(key))
	assert.Equal(t, uint64(10), node.Children[0])
	assert.Equal(t, uint64(20), node.Children[1])
}

func TestInternalPage_InsertKV_MultipleSplitsAscending(t *testing.T) {
	node := NewInternalPage()

	// initial child
	node.Children[0] = 10

	k1 := NewKeyEntryFromInt(10)
	k2 := NewKeyEntryFromInt(20)
	k3 := NewKeyEntryFromInt(30)

	node.InsertKV(k1, 20)
	node.InsertKV(k2, 30)
	node.InsertKV(k3, 40)

	assert.Equal(t, uint16(3), node.NKeys)

	assert.Equal(t, 0, node.Keys[0].Compare(k1))
	assert.Equal(t, 0, node.Keys[1].Compare(k2))
	assert.Equal(t, 0, node.Keys[2].Compare(k3))

	assert.Equal(t, uint64(10), node.Children[0])
	assert.Equal(t, uint64(20), node.Children[1])
	assert.Equal(t, uint64(30), node.Children[2])
	assert.Equal(t, uint64(40), node.Children[3])
}

func TestInternalPage_InsertKV_Middle(t *testing.T) {
	node := NewInternalPage()

	node.Children[0] = 10

	k10 := NewKeyEntryFromInt(10)
	k30 := NewKeyEntryFromInt(30)
	k20 := NewKeyEntryFromInt(20)

	node.InsertKV(k10, 20)
	node.InsertKV(k30, 40)
	node.InsertKV(k20, 30)

	assert.Equal(t, uint16(3), node.NKeys)

	assert.Equal(t, 0, node.Keys[0].Compare(k10))
	assert.Equal(t, 0, node.Keys[1].Compare(k20))
	assert.Equal(t, 0, node.Keys[2].Compare(k30))

	assert.Equal(t, uint64(10), node.Children[0])
	assert.Equal(t, uint64(20), node.Children[1])
	assert.Equal(t, uint64(30), node.Children[2])
	assert.Equal(t, uint64(40), node.Children[3])
}

func TestInternalPage_Split(t *testing.T) {
	node := NewInternalPage()

	k1 := NewKeyEntryFromInt(1)
	k2 := NewKeyEntryFromInt(2)
	k3 := NewKeyEntryFromInt(3)
	k4 := NewKeyEntryFromInt(4)

	node.NKeys = 4
	node.Keys[0] = *k1
	node.Keys[1] = *k2
	node.Keys[2] = *k3
	node.Keys[3] = *k4

	node.Children[0] = 10
	node.Children[1] = 20
	node.Children[2] = 30
	node.Children[3] = 40
	node.Children[4] = 50

	right, _ := node.Split()

	assert.Equal(t, uint16(2), node.NKeys)
	assert.Equal(t, uint16(2), right.NKeys)

	assert.Equal(t, 0, node.Keys[0].Compare(k1))
	assert.Equal(t, 0, node.Keys[1].Compare(k2))

	assert.Equal(t, 0, right.Keys[0].Compare(k3))
	assert.Equal(t, 0, right.Keys[1].Compare(k4))

	assert.Equal(t, uint64(10), node.Children[0])
	assert.Equal(t, uint64(30), right.Children[0])
}
