package bptree_disk

import (
	"bytes"

	"github.com/spaghetti-lover/go-db/internal/storage/disk"
)

type BPlusTree struct {
	pager   *disk.Pager
	metaPID uint64
}

func NewBPlusTree(pager *disk.Pager) (*BPlusTree, error) {
	metaPID := uint64(0)

	buf, err := pager.FetchPage(metaPID)
	if err != nil {
		return nil, err
	}

	meta := &disk.MetaPage{}
	metaBuf := bytes.NewBuffer(buf)

	// Case 1: fresh file
	if err := meta.ReadFromBuffer(metaBuf); err != nil || meta.Magic != disk.META_MAGIC {
		meta := disk.NewMetaPage()

		// create root leaf
		rootPID, rootBuf := pager.NewPage()
		leaf := disk.NewLeafPage()

		rootWriter := bytes.NewBuffer(rootBuf[:0])
		if err := leaf.WriteToBuffer(rootWriter); err != nil {
			return nil, err
		}

		if err := pager.FlushPage(rootPID); err != nil {
			return nil, err
		}

		// update meta
		meta.RootPID = rootPID
		metaWriter := bytes.NewBuffer(buf[:0])
		if err := meta.WriteToBuffer(metaWriter); err != nil {
			return nil, err
		}

		if err := pager.FlushPage(metaPID); err != nil {
			return nil, err
		}

		return &BPlusTree{
			pager:   pager,
			metaPID: metaPID,
		}, nil
	}

	// Case 2: existing tree
	return &BPlusTree{
		pager:   pager,
		metaPID: metaPID,
	}, nil
}

func (t *BPlusTree) rootPID() (uint64, error) {
	meta, _, err := t.loadMeta()
	if err != nil {
		return 0, err
	}
	return meta.RootPID, nil
}

func (t *BPlusTree) setRootPID(pid uint64) error {
	meta, buf, err := t.loadMeta()
	if err != nil {
		return err
	}

	meta.RootPID = pid
	return t.flushMeta(meta, buf)
}

type InsertResult struct {
	Split      bool
	PromoteKey *disk.KeyEntry
	NewPID     uint64
}

type DeleteResult struct {
	NodePID      uint64
	PromotionKey disk.KeyEntry
}
