package disk

import (
	"errors"
	"io"
	"os"
)

// Pager manages page-level I/O and caching
type Pager struct {
	file      *os.File
	allocator *FileAllocator
	cache     map[uint64][]byte // pageID -> page buffer
}

// NewPager creates a pager bound to a file
func NewPager(file *os.File, allocator *FileAllocator) *Pager {
	return &Pager{
		file:      file,
		allocator: allocator,
		cache:     make(map[uint64][]byte),
	}
}

// NewPage allocates a new page and returns its ID and buffer
func (p *Pager) NewPage() (pageID uint64, buf []byte) {
	pageID = p.allocator.Allocate()
	buf = make([]byte, BLOCK_SIZE)

	p.cache[pageID] = buf
	return pageID, buf
}

// FetchPage retrieves a page buffer by its ID
func (p *Pager) FetchPage(pageID uint64) ([]byte, error) {
	// Cache hit
	if buf, ok := p.cache[pageID]; ok {
		return buf, nil
	}

	// Cache miss → read from disk
	buf := make([]byte, BLOCK_SIZE)
	offset := int64(BlockOffset(pageID))

	_, err := p.file.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	p.cache[pageID] = buf
	return buf, nil
}

// FlushPage writes a page buffer back to disk
// Not removing from cache for simplicity. Dirty handling can be added later.
func (p *Pager) FlushPage(pageID uint64) error {
	buf, ok := p.cache[pageID]
	if !ok {
		return errors.New("page not in cache")
	}

	offset := int64(BlockOffset(pageID))
	_, err := p.file.WriteAt(buf, offset)
	return err
}

func (p *Pager) Sync() error {
	return p.file.Sync()
}

// FreePage releases a page ID and removes it from cache
func (p *Pager) FreePage(pageID uint64) {
	delete(p.cache, pageID)
	p.allocator.Free(pageID)
}

// Close closes the underlying file
func (p *Pager) Close() error {
	return p.file.Close()
}
