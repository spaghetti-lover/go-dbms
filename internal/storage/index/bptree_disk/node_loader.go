package bptree_disk

import (
	"bytes"
	"fmt"

	"github.com/spaghetti-lover/go-db/internal/storage/disk"
)

type Node interface {
	IsLeaf() bool
}

func (t *BPlusTree) loadNode(pid uint64) (Node, []byte, error) {
	buf, err := t.pager.FetchPage(pid)
	if err != nil {
		return nil, nil, err
	}

	reader := bytes.NewBuffer(buf)

	var header disk.PageHeader
	if err := header.ReadFromBuffer(reader); err != nil {
		return nil, nil, err
	}

	switch header.PageType {
	case disk.PageTypeLeaf:
		return t.loadLeaf(pid)

	case disk.PageTypeInternal:
		return t.loadInternal(pid)

	default:
		return nil, nil, fmt.Errorf("unknown page type: %d", header.PageType)
	}
}

func (t *BPlusTree) loadInternal(pid uint64) (*disk.InternalPage, []byte, error) {
	buf, err := t.pager.FetchPage(pid)
	if err != nil {
		return nil, nil, err
	}

	page := &disk.InternalPage{}
	reader := bytes.NewBuffer(buf)

	if err := page.ReadFromBuffer(reader, true); err != nil {
		return nil, nil, err
	}

	return page, buf, nil
}

func (t *BPlusTree) loadLeaf(pid uint64) (*disk.LeafPage, []byte, error) {
	buf, err := t.pager.FetchPage(pid)
	if err != nil {
		return nil, nil, err
	}

	page := &disk.LeafPage{}
	reader := bytes.NewBuffer(buf)

	if err := page.ReadFromBuffer(reader, true); err != nil {
		return nil, nil, err
	}

	return page, buf, nil
}
