package bptree_disk

import (
	"bytes"

	"github.com/spaghetti-lover/go-db/internal/storage/disk"
)

func (t *BPlusTree) loadMeta() (*disk.MetaPage, []byte, error) {
	buf, err := t.pager.FetchPage(t.metaPID)
	if err != nil {
		return nil, nil, err
	}

	meta := &disk.MetaPage{}
	reader := bytes.NewBuffer(buf)

	if err := meta.ReadFromBuffer(reader); err != nil {
		return nil, nil, err
	}

	return meta, buf, nil
}

func (t *BPlusTree) flushMeta(meta *disk.MetaPage, buf []byte) error {
	writer := bytes.NewBuffer(buf[:0])

	if err := meta.WriteToBuffer(writer); err != nil {
		return err
	}

	return t.pager.FlushPage(t.metaPID)
}
