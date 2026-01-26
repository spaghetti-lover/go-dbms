package disk

import (
	"bytes"
	"fmt"
	"os"

	"github.com/spaghetti-lover/go-db/internal/config"
)

type FileAllocator struct {
	lastPointer uint64
}

// Always return a pointer to write data to
// <= 4096 bytes -> increase by 4096
func (a *FileAllocator) alloc() uint64 {
	oldPointer := a.lastPointer
	a.lastPointer += 4096
	return oldPointer
}

// TODO: Free to reuse memory

type InsertResult struct {
	nodePointer      uint64
	nodePromotionKey KeyEntry
	newNodePointer   uint64
	newPromotionKey  KeyEntry
}

type BPlusTreeDisk struct {
	fileName      string //always read from filename
	fileAllocator FileAllocator
	head          MetaPage
}

func NewBPlusTreeDisk(fileName string) BPlusTreeDisk {
	return BPlusTreeDisk{
		fileName: fileName,
		fileAllocator: FileAllocator{
			lastPointer: 4096,
		},
		head: MetaPage{
			header: PageHeader{
				pageType:        0,
				nextPagePointer: 0,
			},
		},
	}
}

// Reuse buffer style: buffer always of size 4096
func (tree *BPlusTreeDisk) readBlockAtPointer(pointer uint64, buffer *bytes.Buffer, file *os.File) error {
	buffer.Reset()
	data := make([]byte, 4096)

	_, err := file.ReadAt(data, int64(pointer))
	if err != nil {
		return err
	}

	buffer.Write(data)
	return nil
}

func (tree *BPlusTreeDisk) writeBufferToFile(buffer *bytes.Buffer, file *os.File) (uint64, error) {
	lastPointer := tree.fileAllocator.alloc()
	_, err := file.WriteAt(buffer.Bytes(), int64(lastPointer))
	if err != nil {
		return 0, err
	}

	return lastPointer, nil
}

func (tree *BPlusTreeDisk) writeBufferToFileFirst(buffer *bytes.Buffer, file *os.File) error {
	_, err := file.WriteAt(buffer.Bytes(), 0)
	if err != nil {
		return err
	}

	return nil
}

// Test: Keep key / val as int
func (tree *BPlusTreeDisk) Insert(insertKeyInt int, insertValueInt int) error {
	buffer := new(bytes.Buffer)
	insertKey := NewKeyEntryFromInt(int64(insertKeyInt))
	insertKV := NewKeyValFromInt(int64(insertKeyInt), int64(insertValueInt))

	// Step 1: Open file
	// os.O_RDWR: Mở file ở chế độ cả Đọc và Ghi
	// os.O_CREATE: Nếu file chưa tồn tại → Tạo file mới.
	// 6: Owner (read + write) 4: Group (read) 4: Other (read)
	file, err := os.OpenFile(tree.fileName, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return err
	}

	defer file.Close()

	// Check file size
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}

	// Step 2: Read data
	// Read MetaPage
	err = tree.readBlockAtPointer(0, buffer, file) // BUffer size = 4096
	if err != nil {
		return err
	}

	metaPage := MetaPage{}
	internalPage := BPlusTreeInternalPage{}

	// If not enough data for one page
	if fileInfo.Size() < 4096 {
		// Create MetaPage
		metaPage = NewMetaPage()
	} else {
		// Update MetaPage
		err = tree.readBlockAtPointer(0, buffer, file)
		if err != nil {
			return err
		}
		metaPage.readFromBuffer(buffer) // buffer size decrease

		// Read first Internal Page
		if metaPage.header.nextPagePointer != 0 {
			err := tree.readBlockAtPointer(metaPage.header.nextPagePointer, buffer, file)
			if err != nil {
				return err
			}
			internalPage.readFromBuffer(buffer, true)
		}
	}

	// Step 3: Insert sub structure
	insertResult, err := tree.insertRecursive(&internalPage, &insertKey, &insertKV, buffer, file)
	if err != nil {
		return err
	}

	// Step 4: Modify MetaPage and save to disk
	var firstInternalPagePointer uint64
	if insertResult.newNodePointer != 0 {
		// Insert a new page
		newFirstInternalPage := NewBPlusTreeInternalPage()
		newFirstInternalPage.nkey = 2
		newFirstInternalPage.keys[0] = insertResult.nodePromotionKey
		newFirstInternalPage.children[0] = insertResult.nodePointer
		newFirstInternalPage.keys[1] = insertResult.newPromotionKey
		newFirstInternalPage.children[1] = insertResult.newNodePointer

		firstInternalPagePointer, err = tree.saveNode(newFirstInternalPage, buffer, file)
		if err != nil {
			return err
		}
	} else {
		firstInternalPagePointer = insertResult.nodePointer
	}

	// Assume last step has the first internal page ptr
	metaPage.header.nextPagePointer = firstInternalPagePointer

	_, err = tree.saveNode(metaPage, buffer, file)
	if err != nil {
		return err
	}

	tree.writeBufferToFileFirst(buffer, file)

	return nil
}

func getKeyEntryFromKeyVal(kv *KeyVal) KeyEntry {
	return KeyEntry{
		len:  kv.keyLen,
		data: kv.key,
	}
}

func (tree *BPlusTreeDisk) insertRecursive(node any, insertKey *KeyEntry, insertKV *KeyVal, buffer *bytes.Buffer, file *os.File) (InsertResult, error) {
	// Insert a key value pair.
	// Current: [3] | 3 -> [(3,3), (5,5)]
	if convert, ok := node.(*BPlusTreeInternalPage); ok {
		pos := convert.FindLastLE(insertKey) // -> -1
		if convert.nkey == 0 {
			// Insert in the begining
			firstLeaf := NewLeafPage()
			firstLeaf.InsertKV(insertKV)

			_, err := tree.saveNode(firstLeaf, buffer, file)
			if err != nil {
				return InsertResult{}, err
			}

			leafPtr, err := tree.writeBufferToFile(buffer, file)
			if err != nil {
				return InsertResult{}, err
			}
			convert.InsertKV(insertKey, leafPtr)

			_, err = tree.saveNode(convert, buffer, file)
			if err != nil {
				return InsertResult{}, err
			}

			internalPtr, err := tree.writeBufferToFile(buffer, file)
			if err != nil {
				return InsertResult{}, err
			}

			return InsertResult{
				nodePointer:      internalPtr,
				nodePromotionKey: convert.keys[0],
				newNodePointer:   0,
				newPromotionKey:  KeyEntry{},
			}, nil
		} else {
			// Special process for -1 position
			if pos == -1 {
				pos = 0
			}
			child := convert.children[pos]
			tree.readBlockAtPointer(child, buffer, file)
			// Try to convert back to either leaf or internal
			header := PageHeader{}
			header.readFromBuffer(buffer)
			var childNode any
			if header.pageType == 1 {
				// Internal page
				ipage := BPlusTreeInternalPage{header: header}
				ipage.readFromBuffer(buffer, false)
				childNode = &ipage
			} else {
				// Leaf page
				lpage := BPlusTreeLeafPage{header: header}
				lpage.readFromBuffer(buffer, false)
				childNode = &lpage
			}
			// child -> [(2,2), (3,3), (5,5)]
			// Current: [3] -> [(2,2), (3,3), (5,5)]
			// Node -> any (*BTreeInternalNode / *BTreeLeafNode)
			// Child *Node -> Node
			insertResult, err := tree.insertRecursive(childNode, insertKey, insertKV, buffer, file)
			if err != nil {
				return InsertResult{}, err
			}
			convert.keys[pos] = insertResult.nodePromotionKey
			convert.children[pos] = insertResult.nodePointer
			// Current: [2] -> [(2,2), (3,3), (5,5)]
			// If need split, insert back to parent.
			if insertResult.newNodePointer != 0 {
				convert.InsertKV(&insertResult.newPromotionKey, insertResult.newNodePointer)
			}
			// After insert, check if need split.
			if convert.nkey == config.INTERNAL_MAX_KEY {
				newInternal := convert.Split()
				// Save current page
				_, err := tree.saveNode(convert, buffer, file)
				if err != nil {
					return InsertResult{}, err
				}

				oldPtr, err := tree.writeBufferToFile(buffer, file)
				if err != nil {
					return InsertResult{}, err
				}
				// Save new page
				_, err = tree.saveNode(newInternal, buffer, file)
				if err != nil {
					return InsertResult{}, err
				}

				newPtr, err := tree.writeBufferToFile(buffer, file)
				if err != nil {
					return InsertResult{}, err
				}
				return InsertResult{
					nodePointer:      oldPtr,
					nodePromotionKey: convert.keys[0],
					newNodePointer:   newPtr,
					newPromotionKey:  newInternal.keys[0],
				}, nil
			} else {
				// Save current page
				_, err = tree.saveNode(convert, buffer, file)
				if err != nil {
					return InsertResult{}, err
				}

				oldPtr, err := tree.writeBufferToFile(buffer, file)
				if err != nil {
					return InsertResult{}, err
				}
				return InsertResult{
					nodePointer:      oldPtr,
					nodePromotionKey: convert.keys[0],
					newNodePointer:   0,
					newPromotionKey:  KeyEntry{},
				}, nil
			}
		}
	} else {
		convert := node.(*BPlusTreeLeafPage)
		convert.InsertKV(insertKV)

		// After insert, check if need split.
		if convert.nKeyValue == LEAF_MAX_KV {
			newLeaf := convert.Split()
			// Save current page
			_, err := tree.saveNode(convert, buffer, file)
			if err != nil {
				return InsertResult{}, err
			}

			oldPtr, err := tree.writeBufferToFile(buffer, file)
			if err != nil {
				return InsertResult{}, err
			}
			// Save new page
			_, err = tree.saveNode(newLeaf, buffer, file)
			if err != nil {
				return InsertResult{}, err
			}

			newPtr, err := tree.writeBufferToFile(buffer, file)
			if err != nil {
				return InsertResult{}, err
			}
			return InsertResult{
				nodePointer:      oldPtr,
				nodePromotionKey: getKeyEntryFromKeyVal(&convert.keyValue[0]),
				newNodePointer:   newPtr,
				newPromotionKey:  getKeyEntryFromKeyVal(&newLeaf.keyValue[0]),
			}, nil
		} else {
			// Save current page
			_, err := tree.saveNode(convert, buffer, file)
			if err != nil {
				return InsertResult{}, err
			}

			oldPtr, err := tree.writeBufferToFile(buffer, file)
			if err != nil {
				return InsertResult{}, nil
			}

			return InsertResult{
				nodePointer:      oldPtr,
				nodePromotionKey: getKeyEntryFromKeyVal(&convert.keyValue[0]),
				newNodePointer:   0,
				newPromotionKey:  KeyEntry{},
			}, nil
		}
	}

}

func (tree *BPlusTreeDisk) saveNode(node any, buffer *bytes.Buffer, file *os.File) (uint64, error) {
	buffer.Reset()
	switch n := node.(type) {
	case *BPlusTreeInternalPage:
		n.writeToBuffer(buffer)
	case *BPlusTreeLeafPage:
		n.writeToBuffer(buffer)
	case MetaPage:
		n.writeToBuffer(buffer)
	default:
		return 0, fmt.Errorf("unknown node type")
	}

	return tree.writeBufferToFile(buffer, file)
}
