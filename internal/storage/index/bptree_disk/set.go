package bptree_disk

import (
	"bytes"

	"github.com/spaghetti-lover/go-db/internal/storage/disk"
)

func (t *BPlusTree) Set(key, value []byte) error {
	kv := disk.NewKeyValFromBytes(key, value)

	rootPID, err := t.rootPID()
	if err != nil {
		return err
	}

	res, err := t.setRecursive(rootPID, &kv)
	if err != nil {
		return err
	}

	// root split
	if res.Split {
		newRoot := disk.NewInternalPage()

		newRoot.Children[0] = rootPID
		newRoot.Children[1] = res.NewPID
		newRoot.Keys[0] = *res.PromoteKey
		newRoot.NKeys = 1

		newRootPID, buf := t.pager.NewPage()
		writer := bytes.NewBuffer(buf[:0])
		if err := newRoot.WriteToBuffer(writer); err != nil {
			return err
		}
		if err := t.pager.FlushPage(newRootPID); err != nil {
			return err
		}

		err := t.setRootPID(newRootPID)
		if err != nil {
			return err
		}

		return t.pager.Sync()
	}

	return t.pager.Sync()
}

func (t *BPlusTree) setRecursive(nodePID uint64, kv *disk.KeyVal) (InsertResult, error) {

	node, buf, err := t.loadNode(nodePID)
	if err != nil {
		return InsertResult{}, err
	}

	if node.IsLeaf() {
		leaf := node.(*disk.LeafPage)

		pos := leaf.FindLastLE(kv)

		// UPDATE
		if pos >= 0 && leaf.KVs[pos].Compare(kv) == 0 {
			leaf.KVs[pos] = *kv

			writer := bytes.NewBuffer(buf[:0])
			if err := leaf.WriteToBuffer(writer); err != nil {
				return InsertResult{}, err
			}
			return InsertResult{}, t.pager.FlushPage(nodePID)
		}

		// INSERT
		leaf.InsertKV(kv)

		if !leaf.IsOverflow() {
			writer := bytes.NewBuffer(buf[:0])
			if err := leaf.WriteToBuffer(writer); err != nil {
				return InsertResult{}, err
			}
			return InsertResult{}, t.pager.FlushPage(nodePID)
		}

		// SPLIT
		rightLeaf, promoteKey := leaf.Split()

		rightPID, rightBuf := t.pager.NewPage()
		if err := rightLeaf.WriteToBuffer(bytes.NewBuffer(rightBuf[:0])); err != nil {
			return InsertResult{}, err
		}

		if err := leaf.WriteToBuffer(bytes.NewBuffer(buf[:0])); err != nil {
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

	keyEntry := disk.NewKeyEntryFromKeyVal(kv)

	idx := internal.FindLastLE(keyEntry)
	childPID := internal.Children[idx+1]

	res, err := t.setRecursive(childPID, kv)
	if err != nil {
		return InsertResult{}, err
	}

	if !res.Split {
		return InsertResult{}, nil
	}

	internal.InsertKV(res.PromoteKey, res.NewPID)

	if !internal.IsOverflow() {
		if err := internal.WriteToBuffer(bytes.NewBuffer(buf[:0])); err != nil {
			return InsertResult{}, err
		}
		return InsertResult{}, t.pager.FlushPage(nodePID)
	}

	rightInternal, promoteKey := internal.Split()

	rightPID, rightBuf := t.pager.NewPage()
	if err := rightInternal.WriteToBuffer(bytes.NewBuffer(rightBuf[:0])); err != nil {
		return InsertResult{}, err
	}

	if err := internal.WriteToBuffer(bytes.NewBuffer(buf[:0])); err != nil {
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
