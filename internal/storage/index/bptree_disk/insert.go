package bptree_disk

import (
	"bytes"
	"fmt"

	"github.com/spaghetti-lover/go-db/internal/storage/disk"
)

var ErrDuplicateKey = fmt.Errorf("duplicate key")

func (t *BPlusTree) Insert(key, value []byte) error {
	kv := disk.NewKeyValFromBytes(key, value)
	keyEntry := disk.KeyEntry{Key: kv.Key}

	rootPID, err := t.rootPID()
	if err != nil {
		return err
	}

	res, err := t.insertRecursive(rootPID, &keyEntry, &kv)
	if err != nil {
		return err
	}

	// Root không split → done
	if !res.Split {
		return nil
	}

	// Root split → create new root
	newRoot := disk.NewInternalPage()

	// children: [oldRoot | newRight]
	newRoot.Children[0] = rootPID
	newRoot.Children[1] = res.NewPID
	newRoot.Keys[0] = *res.PromoteKey
	newRoot.NKeys = 1

	// allocate page
	newRootPID, buf := t.pager.NewPage()

	writer := bytes.NewBuffer(buf[:0])
	if err := newRoot.WriteToBuffer(writer); err != nil {
		return err
	}

	if err := t.pager.FlushPage(newRootPID); err != nil {
		return err
	}

	return t.setRootPID(newRootPID)
}

func (t *BPlusTree) insertRecursive(nodePID uint64, key *disk.KeyEntry, kv *disk.KeyVal) (InsertResult, error) {
	node, buf, err := t.loadNode(nodePID)
	if err != nil {
		return InsertResult{}, err
	}

	if node.IsLeaf() {
		leaf := node.(*disk.LeafPage)

		// Check duplicate key
		for i := 0; i < int(leaf.NKV); i++ {
			if leaf.KVs[i].Compare(kv) == 0 {
				return InsertResult{}, ErrDuplicateKey
			}
		}

		// 1. Insert KV into leaf
		leaf.InsertKV(kv)

		// 2. No overflow
		if !leaf.IsOverflow() {
			writer := bytes.NewBuffer(buf[:0])
			if err := leaf.WriteToBuffer(writer); err != nil {
				return InsertResult{}, err
			}
			if err := t.pager.FlushPage(nodePID); err != nil {
				return InsertResult{}, err
			}

			return InsertResult{Split: false}, nil
		}

		// 3. Overflow → split
		// ASSUME:
		//   rightLeaf, promoteKey := leaf.Split()
		//   promoteKey is FIRST KEY of right leaf
		rightLeaf, promoteKey := leaf.Split()

		rightPID, rightBuf := t.pager.NewPage()
		rightWriter := bytes.NewBuffer(rightBuf[:0])
		if err := rightLeaf.WriteToBuffer(rightWriter); err != nil {
			return InsertResult{}, err
		}

		// write left leaf back
		leftWriter := bytes.NewBuffer(buf[:0])
		if err := leaf.WriteToBuffer(leftWriter); err != nil {
			return InsertResult{}, err
		}

		if err := t.pager.FlushPage(nodePID); err != nil {
			return InsertResult{}, err
		}
		if err := t.pager.FlushPage(rightPID); err != nil {
			return InsertResult{}, err
		}

		return InsertResult{
			Split:      true,
			PromoteKey: promoteKey,
			NewPID:     rightPID,
		}, nil
	}

	internal := node.(*disk.InternalPage)

	// 1. Find child to descend
	idx := internal.FindLastLE(key)
	childPID := internal.Children[idx+1]

	// 2. Recurse
	res, err := t.insertRecursive(childPID, key, kv)
	if err != nil {
		return InsertResult{}, err
	}

	// 3. Child not split → done
	if !res.Split {
		return InsertResult{Split: false}, nil
	}

	// 4. absorb promoted key from child
	internal.InsertKV(res.PromoteKey, res.NewPID)

	// 5. no overflow
	if !internal.IsOverflow() {
		writer := bytes.NewBuffer(buf[:0])
		if err := internal.WriteToBuffer(writer); err != nil {
			return InsertResult{}, err
		}
		if err := t.pager.FlushPage(nodePID); err != nil {
			return InsertResult{}, err
		}

		return InsertResult{Split: false}, nil
	}

	// 6. overflow → split
	// ASSUME:
	//   rightInternal, promoteKey := internal.Split()
	rightInternal, promoteKey := internal.Split()

	rightPID, rightBuf := t.pager.NewPage()
	rightWriter := bytes.NewBuffer(rightBuf[:0])
	if err := rightInternal.WriteToBuffer(rightWriter); err != nil {
		return InsertResult{}, err
	}

	// write left internal back
	leftWriter := bytes.NewBuffer(buf[:0])
	if err := internal.WriteToBuffer(leftWriter); err != nil {
		return InsertResult{}, err
	}

	if err := t.pager.FlushPage(nodePID); err != nil {
		return InsertResult{}, err
	}
	if err := t.pager.FlushPage(rightPID); err != nil {
		return InsertResult{}, err
	}

	return InsertResult{
		Split:      true,
		PromoteKey: promoteKey,
		NewPID:     rightPID,
	}, nil
}
