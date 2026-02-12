package bptree_disk

import (
	"bytes"

	"github.com/spaghetti-lover/go-db/internal/storage/disk"
)

func (t *BPlusTree) Del(key []byte) (bool, error) {
	rootPID, err := t.rootPID()
	if err != nil {
		return false, err
	}

	keyEntry := disk.NewKeyEntryFromBytes(key)

	res, err := t.deleteRecursive(rootPID, keyEntry)
	if err != nil {
		if err == disk.ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}

	// root shrink: internal root with 0 key
	if res.Underflow {
		root, _, err := t.loadNode(rootPID)
		if err != nil {
			return false, err
		}

		// internal root with 0 key → promote only child
		if !root.IsLeaf() {
			internal := root.(*disk.InternalPage)
			if internal.NKeys == 0 {
				newRootPID := internal.Children[0]
				t.pager.FreePage(rootPID)
				if err := t.pager.Sync(); err != nil {
					return false, err
				}
				return true, t.setRootPID(newRootPID)
			}
		}
	}

	if err := t.pager.Sync(); err != nil {
		return false, err
	}
	return true, nil
}

func (t *BPlusTree) deleteRecursive(nodePID uint64, key *disk.KeyEntry) (DeleteResult, error) {
	node, buf, err := t.loadNode(nodePID)
	if err != nil {
		return DeleteResult{}, err
	}

	// ================= LEAF =================
	if node.IsLeaf() {
		leaf := node.(*disk.LeafPage)

		ok := leaf.DelKey(key)
		if !ok {
			return DeleteResult{}, disk.ErrKeyNotFound
		}

		// write back leaf
		writer := bytes.NewBuffer(buf[:0])
		if err := leaf.WriteToBuffer(writer); err != nil {
			return DeleteResult{}, err
		}
		if err := t.pager.FlushPage(nodePID); err != nil {
			return DeleteResult{}, err
		}
		// no underflow
		if leaf.NKV >= leaf.MinKeys() {
			return DeleteResult{Underflow: false}, nil
		}

		// underflow
		return DeleteResult{Underflow: true}, nil
	}

	// ================= INTERNAL =================
	internal := node.(*disk.InternalPage)

	// find child
	idx := internal.FindLastLE(key)
	childPID := internal.Children[idx+1]

	res, err := t.deleteRecursive(childPID, key)
	if err != nil {
		return DeleteResult{}, err
	}

	if !res.Underflow {
		return DeleteResult{Underflow: false}, nil
	}

	// reload child after modification
	childNode, childBuf, err := t.loadNode(childPID)
	if err != nil {
		return DeleteResult{}, err
	}

	// ================= HANDLE UNDERFLOW =================

	// -------- try borrow from LEFT --------
	if idx >= 0 {
		leftPID := internal.Children[idx]
		leftNode, leftBuf, err := t.loadNode(leftPID)
		if err != nil {
			return DeleteResult{}, err
		}

		if childNode.IsLeaf() {
			left := leftNode.(*disk.LeafPage)
			cur := childNode.(*disk.LeafPage)

			if left.NKV > left.MinKeys() {
				borrowFromLeftLeaf(left, cur, internal, idx)

				// flush pages
				writePage(t, leftPID, leftBuf, left)
				writePage(t, childPID, childBuf, cur)
				writePage(t, nodePID, buf, internal)

				return DeleteResult{Underflow: false}, nil
			}
		} else {
			left := leftNode.(*disk.InternalPage)
			cur := childNode.(*disk.InternalPage)

			if left.NKeys > left.MinKeys() {
				borrowFromLeftInternal(internal, idx, left, cur)

				writePage(t, leftPID, leftBuf, left)
				writePage(t, childPID, childBuf, cur)
				writePage(t, nodePID, buf, internal)

				return DeleteResult{Underflow: false}, nil
			}
		}
	}

	// -------- try borrow from RIGHT --------
	if idx+2 <= int(internal.NKeys) {
		rightPID := internal.Children[idx+2]
		rightNode, rightBuf, _ := t.loadNode(rightPID)

		if childNode.IsLeaf() {
			cur := childNode.(*disk.LeafPage)
			right := rightNode.(*disk.LeafPage)

			if right.NKV > right.MinKeys() {
				borrowFromRightLeaf(cur, right, internal, idx+1)

				writePage(t, childPID, childBuf, cur)
				writePage(t, rightPID, rightBuf, right)
				writePage(t, nodePID, buf, internal)

				return DeleteResult{Underflow: false}, nil
			}
		} else {
			cur := childNode.(*disk.InternalPage)
			right := rightNode.(*disk.InternalPage)

			if right.NKeys > right.MinKeys() {
				borrowFromRightInternal(internal, idx+1, right, cur)

				writePage(t, childPID, childBuf, cur)
				writePage(t, rightPID, rightBuf, right)
				writePage(t, nodePID, buf, internal)

				return DeleteResult{Underflow: false}, nil
			}
		}
	}

	// -------- MERGE (must) --------
	if idx >= 0 {
		// merge into left
		leftPID := internal.Children[idx]
		leftNode, leftBuf, err := t.loadNode(leftPID)
		if err != nil {
			return DeleteResult{}, err
		}

		if childNode.IsLeaf() {
			mergeLeaf(internal, idx+1, leftNode.(*disk.LeafPage), childNode.(*disk.LeafPage))
		} else {
			mergeInternal(internal, idx+1, leftNode.(*disk.InternalPage), childNode.(*disk.InternalPage))
		}

		if err := writePage(t, leftPID, leftBuf, leftNode.(disk.Page)); err != nil {
			return DeleteResult{}, err
		}
		// Free the merged child page
		t.pager.FreePage(childPID)
	} else {
		// merge with right
		rightPID := internal.Children[idx+2]
		rightNode, _, err := t.loadNode(rightPID)
		if err != nil {
			return DeleteResult{}, err
		}

		if childNode.IsLeaf() {
			mergeLeaf(internal, idx+2, childNode.(*disk.LeafPage), rightNode.(*disk.LeafPage))
		} else {
			mergeInternal(internal, idx+2, childNode.(*disk.InternalPage), rightNode.(*disk.InternalPage))
		}

		if err := writePage(t, childPID, childBuf, childNode.(disk.Page)); err != nil {
			return DeleteResult{}, err
		}
		// Free the merged right page
		t.pager.FreePage(rightPID)
	}

	// write parent
	writer := bytes.NewBuffer(buf[:0])
	if err := internal.WriteToBuffer(writer); err != nil {
		return DeleteResult{}, err
	}
	if err := t.pager.FlushPage(nodePID); err != nil {
		return DeleteResult{}, err
	}

	// check parent underflow
	if internal.NKeys >= internal.MinKeys() {
		return DeleteResult{Underflow: false}, nil
	}

	return DeleteResult{Underflow: true}, nil
}

func borrowFromLeftInternal(parent *disk.InternalPage, idx int, left *disk.InternalPage, cur *disk.InternalPage) {
	// 1. shift cur keys + children right by 1
	for i := int(cur.NKeys); i > 0; i-- {
		cur.Keys[i] = cur.Keys[i-1]
	}
	for i := int(cur.NKeys) + 1; i > 0; i-- {
		cur.Children[i] = cur.Children[i-1]
	}

	// 2. bring separator key from parent down to cur
	cur.Keys[0] = parent.Keys[idx-1]
	cur.Children[0] = left.Children[left.NKeys]
	cur.NKeys++

	// 3. move left's last key up to parent
	parent.Keys[idx-1] = left.Keys[left.NKeys-1]

	left.Keys[left.NKeys-1] = disk.KeyEntry{}
	left.Children[left.NKeys] = 0

	// 4. shrink left
	left.NKeys--
}

func borrowFromRightInternal(parent *disk.InternalPage, idx int, right *disk.InternalPage, cur *disk.InternalPage) {
	// 1. bring separator key from parent down
	cur.Keys[cur.NKeys] = parent.Keys[idx]
	cur.Children[cur.NKeys+1] = right.Children[0]
	cur.NKeys++

	// 2. move right's first key up to parent
	parent.Keys[idx] = right.Keys[0]

	// 3. shift right left
	for i := 0; i < int(right.NKeys)-1; i++ {
		right.Keys[i] = right.Keys[i+1]
	}
	for i := 0; i < int(right.NKeys); i++ {
		right.Children[i] = right.Children[i+1]
	}

	right.Keys[right.NKeys-1] = disk.KeyEntry{}
	right.Children[right.NKeys] = 0

	right.NKeys--
}

func borrowFromLeftLeaf(left *disk.LeafPage, curr *disk.LeafPage, parent *disk.InternalPage, parentKeyIdx int) {
	// left must have >= min+1 kv

	// 1. shift curr right to make space at front
	for i := int(curr.NKV); i > 0; i-- {
		curr.KVs[i] = curr.KVs[i-1]
	}

	// 2. move last kv from left to curr[0]
	curr.KVs[0] = left.KVs[left.NKV-1]
	curr.NKV++

	// clear left slot
	left.KVs[left.NKV-1] = disk.KeyVal{}
	left.NKV--

	// 3. update separator key in parent
	// separator = first key of curr
	parent.Keys[parentKeyIdx] = *disk.NewKeyEntryFromKeyVal(&curr.KVs[0])
}
func mergeInternal(parent *disk.InternalPage, idx int, left *disk.InternalPage, cur *disk.InternalPage) {
	// 1. bring separator key down from parent to left
	left.Keys[left.NKeys] = parent.Keys[idx-1]
	left.NKeys++

	// 2. copy cur keys into left
	for i := 0; i < int(cur.NKeys); i++ {
		left.Keys[left.NKeys] = cur.Keys[i]
		left.NKeys++
	}

	// 3. copy cur children into left
	base := int(left.NKeys) - int(cur.NKeys) - 1
	for i := 0; i <= int(cur.NKeys); i++ {
		left.Children[base+1+i] = cur.Children[i]
	}

	// 4. remove key idx-1 and child idx from parent
	for i := idx - 1; i < int(parent.NKeys)-1; i++ {
		parent.Keys[i] = parent.Keys[i+1]
	}
	for i := idx; i < int(parent.NKeys); i++ {
		parent.Children[i] = parent.Children[i+1]
	}

	parent.NKeys--

	parent.Keys[parent.NKeys] = disk.KeyEntry{}
	parent.Children[parent.NKeys+1] = 0
}

func borrowFromRightLeaf(curr *disk.LeafPage, right *disk.LeafPage, parent *disk.InternalPage, parentKeyIdx int) {
	// right must have >= min+1 kv

	// 1. move first kv from right to end of curr
	curr.KVs[curr.NKV] = right.KVs[0]
	curr.NKV++

	// 2. shift right left
	for i := 0; i < int(right.NKV)-1; i++ {
		right.KVs[i] = right.KVs[i+1]
	}

	right.KVs[right.NKV-1] = disk.KeyVal{}
	right.NKV--

	// 3. update separator key in parent
	// separator = first key of right
	parent.Keys[parentKeyIdx] = *disk.NewKeyEntryFromKeyVal(&right.KVs[0])
}

func mergeLeaf(parent *disk.InternalPage, idx int, left *disk.LeafPage, cur *disk.LeafPage) {
	// 1. append all kvs from cur into left
	for i := 0; i < int(cur.NKV); i++ {
		left.KVs[left.NKV] = cur.KVs[i]
		left.NKV++
	}

	// 2. link leaf chain
	left.Header.NextPagePointer = cur.Header.NextPagePointer

	// 3. remove separator key (idx - 1) from parent
	for i := idx - 1; i < int(parent.NKeys)-1; i++ {
		parent.Keys[i] = parent.Keys[i+1]
	}

	// 4. remove child pointer (cur) from parent
	for i := idx; i < int(parent.NKeys); i++ {
		parent.Children[i] = parent.Children[i+1]
	}

	parent.Children[parent.NKeys] = 0
	parent.NKeys--
}

func updateSeparatorKey(parent *disk.InternalPage, childIdx int, newKey *disk.KeyEntry) {
	// parent.Keys[i] separates Children[i] | Children[i+1]
	// so update key at childIdx-1
	if childIdx > 0 {
		parent.Keys[childIdx-1] = *newKey
	}
}

func writePage(t *BPlusTree, pid uint64, buf []byte, node disk.Page) error {
	writer := bytes.NewBuffer(buf[:0])
	if err := node.WriteToBuffer(writer); err != nil {
		return err
	}
	return t.pager.FlushPage(pid)
}
