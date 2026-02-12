package disk

const BLOCK_SIZE = 4096

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

// // Persist allocator state to disk (MetaPage, header, etc.)
// func (a *FileAllocator) WriteToFile(file *os.File) error {
// 	buf := new(bytes.Buffer)
// 	// - serialize nextBlockID
// 	if err := binary.Write(buf, binary.BigEndian, a.nextBlockID); err != nil {
// 		return err
// 	}
// 	// - serialize freeList length + entries
// 	freeLen := uint64(len(a.freeList))
// 	if err := binary.Write(buf, binary.BigEndian, freeLen); err != nil {
// 		return err
// 	}
// 	for _, id := range a.freeList {
// 		if err := binary.Write(buf, binary.BigEndian, id); err != nil {
// 			return err
// 		}
// 	}
// 	// - write to meta page (usually page 0)
// 	if _, err := file.Seek(0, io.SeekStart); err != nil {
// 		return err
// 	}

// 	data := buf.Bytes()
// 	if len(data) > BLOCK_SIZE {
// 		return io.ErrShortBuffer
// 	}
// 	padding := make([]byte, BLOCK_SIZE-len(data))
// 	if _, err := file.Write(append(data, padding...)); err != nil {
// 		return err
// 	}
// 	return nil
// }

// // Load allocator state from disk
// func LoadFileAllocator(file *os.File) (*FileAllocator, error) {
// 	buf := make([]byte, BLOCK_SIZE)
// 	if _, err := file.ReadAt(buf, 0); err != nil {
// 		return nil, err
// 	}
// 	r := bytes.NewReader(buf)
// 	var nextBlockID uint64
// 	if err := binary.Read(r, binary.BigEndian, &nextBlockID); err != nil {
// 		return nil, err
// 	}
// 	var freeLen uint64
// 	if err := binary.Read(r, binary.BigEndian, &freeLen); err != nil {
// 		return nil, err
// 	}
// 	freeList := make([]uint64, freeLen)
// 	for i := uint64(0); i < freeLen; i++ {
// 		if err := binary.Read(r, binary.BigEndian, &freeList[i]); err != nil {
// 			return nil, err
// 		}
// 	}
// 	return &FileAllocator{
// 		nextBlockID: nextBlockID,
// 		freeList:    freeList,
// 	}, nil
// }
