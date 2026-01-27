package disk

import "os"

// FileAllocator manages allocation of fixed-size blocks.
// It works purely with block IDs, not byte offsets.
type FileAllocator struct {
	nextBlockID uint64   // Next never-used block ID
	freeList    []uint64 // Reusable block IDs
}

// NewFileAllocator creates a fresh allocator (empty file case)
func NewFileAllocator() *FileAllocator {
	return &FileAllocator{
		nextBlockID: 1,
		freeList:    make([]uint64, 0),
	}
}

// Allocate returns a block ID that can be written to.
func (a *FileAllocator) Allocate() uint64 {
	// Reuse from free list if possible
	if n := len(a.freeList); n > 0 {
		id := a.freeList[n-1] // LIFO is cache-friendly
		a.freeList = a.freeList[:n-1]
		return id
	}

	id := a.nextBlockID
	a.nextBlockID++
	return id
}

// Free releases a block ID for reuse.
func (a *FileAllocator) Free(blockID uint64) {
	a.freeList = append(a.freeList, blockID)
}

// BlockOffset converts a block ID to byte offset in file.
// Pager layer should use this.
func BlockOffset(blockID uint64) uint64 {
	return blockID * BLOCK_SIZE
}

// Persist allocator state to disk (MetaPage, header, etc.)
func (a *FileAllocator) WriteToFile(file *os.File) error {
	// TODO:
	// - serialize nextBlockID
	// - serialize freeList length + entries
	// - write to meta page (usually page 0)
	return nil
}

// Load allocator state from disk
func LoadFileAllocator(file *os.File) (*FileAllocator, error) {
	// TODO:
	// - read meta page
	// - restore nextBlockID
	// - restore freeList
	return &FileAllocator{}, nil
}
