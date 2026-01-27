package bptree_disk

import "github.com/spaghetti-lover/go-db/internal/storage/disk"

func (t *BPlusTree) newInternalPage() (uint64, *disk.InternalPage, []byte) {
	pid, buf := t.pager.NewPage()
	page := disk.NewInternalPage()
	return pid, page, buf
}

func (t *BPlusTree) newLeafPage() (uint64, *disk.LeafPage, []byte) {
	pid, buf := t.pager.NewPage()
	page := disk.NewLeafPage()
	return pid, page, buf
}
