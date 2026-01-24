package disk

import (
	"bytes"
	"encoding/binary"

	"github.com/spaghetti-lover/go-db/internal/config"
)

// [header | u8 u8 | k0 k1 k2 ... | 0 0 0 0 0 0 ... ]
type BPlusTreeInternalPage struct {
	header   PageHeader
	nkey     uint16
	keys     [config.MAX_KEYS]KeyEntry
	children [config.MAX_CHILDREN]uint64
}

func (p *BPlusTreeInternalPage) writeToBuffer(buffer *bytes.Buffer) error {
	if err := p.header.writeToBuffer(buffer); err != nil {
		return err
	}

	if err := binary.Write(buffer, binary.BigEndian, p.nkey); err != nil {
		return err
	}

	for i := 0; i < int(p.nkey); i += 1 {
		if err := p.keys[i].writeToBuffer(buffer); err != nil {
			return err
		}
	}

	for i := 0; i <= int(p.nkey); i += 1 {
		if err := binary.Write(buffer, binary.BigEndian, p.children[i]); err != nil {
			return err
		}
	}

	return nil
}

func (p *BPlusTreeInternalPage) readFromBuffer(buffer *bytes.Buffer) error {
	if err := p.header.readFromBuffer(buffer); err != nil {
		return err
	}

	if err := binary.Read(buffer, binary.BigEndian, &p.nkey); err != nil {
		return err
	}

	for i := 0; i < int(p.nkey); i += 1 {
		if err := p.keys[i].readFromBuffer(buffer); err != nil {
			return err
		}
	}

	for i := 0; i <= int(p.nkey); i += 1 {
		if err := binary.Read(buffer, binary.BigEndian, &p.children[i]); err != nil {
			return err
		}
	}

	return nil
}

func NewBPlusTreeInternalPage() BPlusTreeInternalPage {

	var new_keys [config.MAX_KEYS]KeyEntry
	var new_children [config.MAX_CHILDREN]uint64

	return BPlusTreeInternalPage{
		nkey:     0,
		keys:     new_keys,
		children: new_children,
		header: PageHeader{
			pageType:        1,
			nextPagePointer: 0,
		},
	}
}

// Find last position so that the key <= find_key
func (node *BPlusTreeInternalPage) FindLastLE(findKey KeyEntry) int {
	pos := -1

	for i := 0; i < int(node.nkey); i++ {
		if node.keys[i].compare(&findKey) <= 0 {
			pos = i
		}
	}

	return pos
}

// Insert a key-children pair into the Internal Node
func (node *BPlusTreeInternalPage) InsertKV(insertKey KeyEntry, insertChild uint64) {
	// Find last less or equal as position to insert
	pos := node.FindLastLE(insertKey)

	for i := int(node.nkey); i > pos+1; i-- {
		node.keys[i] = node.keys[i-1]
	}

	for i := int(node.nkey) + 1; i > pos+1; i-- {
		node.children[i] = node.children[i-1]
	}

	node.keys[pos+1] = insertKey
	node.children[pos+1] = insertChild
	node.nkey += 1
}

// Split a node into 2 equal part
func (node *BPlusTreeInternalPage) Split() BPlusTreeInternalPage {
	var newKeys [config.MAX_KEYS]KeyEntry
	var newChildren [config.MAX_CHILDREN]uint64

	// Split in the middle
	pos := node.nkey / 2

	// [1, 2, 0, 0] -> pos = 2
	// [3, 4, 0, 0]

	for i := pos; i < node.nkey; i++ {
		newKeys[i-pos] = node.keys[i] // n[0] = n[2]
		newChildren[i-pos] = node.children[i]
		node.keys[i] = KeyEntry{}
		node.children[i] = 0
	}

	newChildren[node.nkey-pos] = node.children[node.nkey]
	node.children[node.nkey] = 0

	newNode := BPlusTreeInternalPage{
		header: PageHeader{
			pageType:        1,
			nextPagePointer: 0,
		},
		nkey:     node.nkey - pos,
		keys:     newKeys,
		children: newChildren,
	}

	node.nkey = pos
	return newNode
}
