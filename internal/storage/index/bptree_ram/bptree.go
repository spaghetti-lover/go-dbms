package bptree_ram

import (
	"github.com/spaghetti-lover/go-db/internal/config"
	"github.com/spaghetti-lover/go-db/internal/utils"
)

const (
	MAX_KEYS     = config.MAX_KEYS
	MAX_CHILDREN = config.MAX_CHILDREN
)
type BPlusTree struct {
	root BPlusTreeNode
}

func NewBPlusTree() *BPlusTree {
	return &BPlusTree{
		root: NewBPlusTreeLeafNode(),
	}
}

func (tree *BPlusTree) Insert(key int, val int) {
	disSplit, sep, newNode := tree.insertHelper(tree.root, key, val)

	if disSplit {
		// Root split! Create new root
		newRoot := NewBPlusTreeInternalNode()
		newRoot.children[0] = tree.root
		newRoot.children[1] = newNode
		newRoot.keys[0] = sep
		newRoot.nkey = 1
		tree.root = newRoot
	}
}

func (tree *BPlusTree) insertHelper(node BPlusTreeNode, key, val int) (bool, int, BPlusTreeNode) {
	switch n := node.(type) {
	case *BPlusTreeLeafNode:
		return tree.insertIntoLeaf(n, key, val)
	case *BPlusTreeInternalNode:
		return tree.insertIntoInternal(n, key, val)
	}
	return false, 0, nil
}

func (tree *BPlusTree) insertIntoLeaf(n *BPlusTreeLeafNode, key, val int) (bool, int, BPlusTreeNode) {
	if !n.isFull() {
		n.InsertKV(key, val)
		return false, 0, nil
	}

	// Split and insert
	right := n.Split()
	if key < right.keys[0] {
		n.InsertKV(key, val)
	} else {
		right.InsertKV(key, val)
	}
	return true, right.keys[0], right
}

func (tree *BPlusTree) insertIntoInternal(n *BPlusTreeInternalNode, key, val int) (bool, int, BPlusTreeNode) {
	// Find child for insertion
	idx := utils.FindLastLE(n.keys[:], n.nkey, key)
	childIdx := idx + 1

	// Recursively insert
	didSplit, sep, newNode := tree.insertHelper(n.children[childIdx], key, val)
	if !didSplit {
		return false, 0, nil
	}

	// Handle child split
	if !n.isFull() {
		n.InsertKV(sep, newNode)
		return false, 0, nil
	}

	// This node is also full, split it
	promotedKey, rightChild := n.Split()
	if sep <= promotedKey {
		n.InsertKV(sep, newNode)
	} else {
		rightChild.InsertKV(sep, newNode)
	}
	return true, promotedKey, rightChild
}
