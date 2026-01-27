package bptree_ram

import "github.com/spaghetti-lover/go-db/internal/utils"

type BPlusTreeInternalNode struct {
	nkey     int
	keys     [MAX_KEYS]int
	children [MAX_CHILDREN]BPlusTreeNode
}

func NewBPlusTreeInternalNode() *BPlusTreeInternalNode {
	return &BPlusTreeInternalNode{}
}

func (n *BPlusTreeInternalNode) isFull() bool {
	return n.nkey >= MAX_KEYS
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
	}

	for i := mid + 1; i <= n.nkey; i++ {
		right.children[i-mid-1] = n.children[i]
	}

	right.nkey = n.nkey - mid - 1
	n.nkey = mid

	return promotedKey, right
}
