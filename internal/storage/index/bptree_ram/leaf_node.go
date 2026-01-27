package bptree_ram

import "github.com/spaghetti-lover/go-db/internal/utils"

type BPlusTreeLeafNode struct {
	nkey   int
	keys   [MAX_KEYS]int
	values [MAX_KEYS]int
}

func NewBPlusTreeLeafNode() *BPlusTreeLeafNode {
	return &BPlusTreeLeafNode{}
}

func (n *BPlusTreeLeafNode) isFull() bool {
	return n.nkey >= MAX_KEYS
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
	if n.nkey < MAX_KEYS {
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
