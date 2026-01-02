package index

import "github.com/spaghetti-lover/go-db/internal/utils"

const (
	maxKeys     = 4
	maxChildren = maxKeys + 1
)

type BPlusTreeNode interface {
}

type BPlusTreeInternalNode struct {
	nkey     int
	keys     [maxKeys]int
	children [maxChildren + 1]BPlusTreeNode
}

func NewBPlusTreeInternalNode() *BPlusTreeInternalNode {
	return &BPlusTreeInternalNode{}
}

func (n *BPlusTreeInternalNode) isFull() bool {
	return n.nkey >= maxKeys
}

func (n *BPlusTreeInternalNode) InsertKV(key int, child BPlusTreeNode) {
	idx := utils.FindLastLE(n.keys[:], n.nkey, key)

	if idx != -1 && n.keys[idx] == key {
		n.children[idx+1] = child
		return
	}

	if n.isFull() {
		panic("InsertKV called on full internal node")
	}

	for i := n.nkey; i > idx+1; i-- {
		n.keys[i] = n.keys[i-1]
		n.children[i+1] = n.children[i]
	}

	n.children[idx+2] = child
	n.keys[idx+1] = key
	n.nkey++
}

func (n *BPlusTreeInternalNode) Split() (int, *BPlusTreeInternalNode) {
	mid := n.nkey / 2
	promotedKey := n.keys[mid]

	right := NewBPlusTreeInternalNode()
	for i := mid + 1; i < n.nkey; i++ {
		right.keys[i-mid-1] = n.keys[i]
		right.children[i-mid-1] = n.children[i]
	}

	for i := mid + 1; i < n.nkey+1; i++ {
		right.children[i-mid-1] = n.children[i-1]
	}
	right.nkey = n.nkey - mid - 1
	n.nkey = mid

	return promotedKey, right
}

type BPlusTreeLeafNode struct {
	nkey   int
	keys   [maxKeys]int
	values [maxKeys]int
}

func NewBPlusTreeLeafNode() *BPlusTreeLeafNode {
	return &BPlusTreeLeafNode{}
}

func (n *BPlusTreeLeafNode) isFull() bool {
	return n.nkey >= maxKeys
}

func (n *BPlusTreeLeafNode) InsertKV(key int, val int) {
	idx := utils.FindLastLE(n.keys[:], n.nkey, key)

	// Update existing key
	if idx != -1 && key == n.keys[idx] {
		n.values[idx] = val
		return
	}

	if n.isFull() {
		panic("InsertKV called on full leaf node")
	}

	// Shift and insert
	if n.nkey < maxKeys {
		for i := n.nkey; i > idx+1; i-- {
			n.keys[i] = n.keys[i-1]
			n.values[i] = n.values[i-1]
		}

		n.values[idx+1] = val
		n.keys[idx+1] = key
		n.nkey++
	}
}

func (n *BPlusTreeLeafNode) Split() *BPlusTreeLeafNode {
	mid := n.nkey / 2
	right := NewBPlusTreeLeafNode()

	for i := mid; i < n.nkey; i++ {
		right.keys[i-mid] = n.keys[i]
		right.values[i-mid] = n.values[i]
	}

	right.nkey = n.nkey - mid
	n.nkey = mid
	return right
}

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
