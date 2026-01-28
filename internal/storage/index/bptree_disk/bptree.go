package bptree_disk

import (
	"bytes"
	"os"

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

func Open(file string) (*BPlusTree, error) {
	allocator := disk.NewFileAllocator()
	f, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	pager := disk.NewPager(f, allocator)
	return NewBPlusTree(pager)
}

func (t *BPlusTree) Close() error {
	return t.pager.Close()
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

type MergeDir int

const (
	NoMerge MergeDir = iota
	MergeLeft
	MergeRight
)

type DeleteResult struct {
	Underflow bool
	MergeDir  MergeDir
}

// SeekGE positions the iterator at the first key >= target key
func (t *BPlusTree) SeekGE(key []byte) *BIter {
	rootPID, err := t.rootPID()
	if err != nil {
		return &BIter{valid: false}
	}

	kv := &disk.KeyEntry{
		KeyLen: uint16(len(key)),
	}

	disk.RightAlignCopy(kv.Key[:], key)

	pid := rootPID

	for {
		node, _, err := t.loadNode(pid)
		if err != nil {
			return &BIter{valid: false}
		}

		if node.IsLeaf() {
			leaf, buf, _ := t.loadLeaf(pid)

			idx := 0
			for idx < int(leaf.NKV) {
				entry := &disk.KeyEntry{KeyLen: leaf.KVs[idx].KeyLen, Key: leaf.KVs[idx].Key}
				if entry.Compare(kv) >= 0 {
					break
				}
				idx++
			}

			// Case 1: found key >= target
			if idx < int(leaf.NKV) {
				return &BIter{
					tree:    t,
					leafPID: pid,
					leaf:    leaf,
					buf:     buf,
					idx:     idx,
					valid:   true,
				}
			}

			// Case 2: not found, point to next leaf
			next := leaf.Header.NextPagePointer
			if next == 0 {
				return &BIter{valid: false}
			}

			leaf, buf, _ = t.loadLeaf(next)
			if leaf.NKV == 0 {
				return &BIter{valid: false}
			}

			return &BIter{
				tree:    t,
				leafPID: next,
				leaf:    leaf,
				buf:     buf,
				idx:     0,
				valid:   true,
			}
		}

		// internal node
		internal := node.(*disk.InternalPage)
		pos := internal.FindLastLE(kv)
		pid = internal.Children[pos+1]
	}
}
