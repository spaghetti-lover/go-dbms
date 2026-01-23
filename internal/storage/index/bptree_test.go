package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInternalInsertKV(t *testing.T) {
	root := NewBPlusTreeInternalNode()
	leaf1 := NewBPlusTreeLeafNode()
	leaf2 := NewBPlusTreeLeafNode()

	// Insert first key and child
	root.InsertKV(10, leaf1)
	assert.Equal(t, root.nkey, 1)
	assert.Equal(t, root.keys[0], 10)
	assert.Equal(t, root.children[1], leaf1)

	// Insert second key and child
	root.InsertKV(20, leaf2)
	assert.Equal(t, root.nkey, 2)
	assert.Equal(t, root.keys[1], 20)
	assert.Equal(t, root.children[2], leaf2)

	// Split
	leaf3 := NewBPlusTreeLeafNode()
	leaf4 := NewBPlusTreeLeafNode()
	leaf5 := NewBPlusTreeLeafNode()
	root.InsertKV(30, leaf3)
	root.InsertKV(40, leaf4)
	assert.Panics(t, func() { root.InsertKV(50, leaf5) })
}

func TestInternalNodeSplit(t *testing.T) {
	node := NewBPlusTreeInternalNode()

	// Setup: Create a full internal node
	node.keys = [MAX_KEYS]int{10, 20, 30, 40}
	node.nkey = MAX_KEYS

	// Split
	promotedKey, right := node.Split()

	// Verify promoted key
	assert.Equal(t, 30, promotedKey, "Middle key should be promoted")

	// Verify left node (original)
	assert.Equal(t, 2, node.nkey, "Left should have 2 keys")
	assert.Equal(t, 10, node.keys[0])
	assert.Equal(t, 20, node.keys[1])

	// Verify right node
	assert.Equal(t, 1, right.nkey, "Right should have 1 key")
	assert.Equal(t, 40, right.keys[0])
}

func TestLeafNodeInsertKV(t *testing.T) {
	leaf := NewBPlusTreeLeafNode()

	// Insert out of order
	// Expected: [5, 10, 20, 30]
	leaf.InsertKV(30, 300)
	leaf.InsertKV(10, 100)
	leaf.InsertKV(20, 200)
	leaf.InsertKV(5, 50)

	assert.Equal(t, 50, leaf.values[0])
	assert.Equal(t, 100, leaf.values[1])
	assert.Equal(t, 200, leaf.values[2])
	assert.Equal(t, 300, leaf.values[3])

	// Insert duplicated key
	// Expected: keys: [5, 10, 20, 30], values: [50, 150, 200, 300]
	leaf.InsertKV(10, 150)
	assert.Equal(t, 150, leaf.values[1])

	// Insert full node
	assert.Panics(t, func() { leaf.InsertKV(40, 400) })
}

func TestLeafNodeSplit(t *testing.T) {
	node := NewBPlusTreeLeafNode()

	// Setup: Create a full internal node
	node.keys = [MAX_KEYS]int{10, 20, 30, 40}
	node.nkey = MAX_CHILDREN

	// Split
	right := node.Split()

	// Verify left node (original)
	assert.Equal(t, 2, node.nkey, "Left should have 2 keys")
	assert.Equal(t, 10, node.keys[0])
	assert.Equal(t, 20, node.keys[1])

	// Verify right node
	assert.Equal(t, 2, right.nkey, "Right should have 2 key")
	assert.Equal(t, 30, right.keys[0])
	assert.Equal(t, 40, right.keys[1])
}

func TestBPlusTreeMultipleInserts(t *testing.T) {
	tree := NewBPlusTree()

	// Insert keys to trigger root split
	tree.Insert(10, 100)
	tree.Insert(20, 200)
	tree.Insert(30, 300)
	tree.Insert(40, 400)
	tree.Insert(25, 250) // Triggers split

	// Now root is internal with 2 leaf children

	// Insert more to fill one leaf and trigger another split
	tree.Insert(15, 150)
	tree.Insert(12, 120)

	// Verify structure
	root, ok := tree.root.(*BPlusTreeInternalNode)
	assert.True(t, ok, "Root should be internal")

	assert.Equal(t, 2, root.nkey)
	assert.Equal(t, 20, root.keys[0])
	assert.Equal(t, 30, root.keys[1])

	assert.Equal(t, 10, root.children[0].(*BPlusTreeLeafNode).keys[0])
	assert.Equal(t, 20, root.children[1].(*BPlusTreeLeafNode).keys[0])
	assert.Equal(t, 30, root.children[2].(*BPlusTreeLeafNode).keys[0])
}

func TestBPlusTreeInsertWithSplit(t *testing.T) {
	tree := NewBPlusTree()

	tree.Insert(10, 100)
	tree.Insert(20, 200)
	tree.Insert(30, 300)
	tree.Insert(40, 400)

	// Split case
	tree.Insert(25, 250)
	tree.Insert(35, 350)

	// Expected:
	// Left: [10, 20, 25], nkey=3
	// Right: [30, 35, 40], nkey=2
	// Verify root is now internal
	root, ok := tree.root.(*BPlusTreeInternalNode)
	assert.True(t, ok, "Root should be internal node after split")

	// Verify children
	leftChild, ok := root.children[0].(*BPlusTreeLeafNode)
	assert.True(t, ok, "Left child should be leaf")

	rightChild, ok := root.children[1].(*BPlusTreeLeafNode)
	assert.True(t, ok, "Right child should be leaf")

	// Verify keys in each leaf
	assert.Equal(t, 3, leftChild.nkey)
	assert.Equal(t, 10, leftChild.keys[0])
	assert.Equal(t, 20, leftChild.keys[1])
	assert.Equal(t, 25, leftChild.keys[2])

	assert.Equal(t, 3, rightChild.nkey)
	assert.Equal(t, 30, rightChild.keys[0])
	assert.Equal(t, 35, rightChild.keys[1])
	assert.Equal(t, 40, rightChild.keys[2])
}
