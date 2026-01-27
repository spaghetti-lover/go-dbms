package disk

import (
	"bytes"
	"encoding/binary"

	"github.com/spaghetti-lover/go-db/internal/config"
)

// [header | u8 u8 | k0 k1 k2 ... | 0 0 0 0 0 0 ... ]
type InternalPage struct {
	Header   PageHeader
	NKeys    uint16
	Keys     [config.MAX_KEYS]KeyEntry
	Children [config.MAX_CHILDREN]uint64
}

func (p *InternalPage) WriteToBuffer(buffer *bytes.Buffer) error {
	if err := p.Header.WriteToBuffer(buffer); err != nil {
		return err
	}

	if err := binary.Write(buffer, binary.BigEndian, p.NKeys); err != nil {
		return err
	}

	for i := 0; i < int(p.NKeys); i += 1 {
		if err := p.Keys[i].writeToBuffer(buffer); err != nil {
			return err
		}
	}

	for i := 0; i <= int(p.NKeys); i += 1 {
		if err := binary.Write(buffer, binary.BigEndian, p.Children[i]); err != nil {
			return err
		}
	}

	return nil
}

func (p *InternalPage) ReadFromBuffer(buffer *bytes.Buffer, isReadHeader bool) error {
	if isReadHeader {
		if err := p.Header.ReadFromBuffer(buffer); err != nil {
			return err
		}
	}

	if err := binary.Read(buffer, binary.BigEndian, &p.NKeys); err != nil {
		return err
	}

	for i := 0; i < int(p.NKeys); i += 1 {
		if err := p.Keys[i].readFromBuffer(buffer); err != nil {
			return err
		}
	}

	for i := 0; i <= int(p.NKeys); i += 1 {
		if err := binary.Read(buffer, binary.BigEndian, &p.Children[i]); err != nil {
			return err
		}
	}

	return nil
}

func NewInternalPage() *InternalPage {

	var new_keys [config.MAX_KEYS]KeyEntry
	var new_children [config.MAX_CHILDREN]uint64

	return &InternalPage{
		NKeys:    0,
		Keys:     new_keys,
		Children: new_children,
		Header: PageHeader{
			PageType:        PageTypeInternal,
			NextPagePointer: 0,
		},
	}
}

// Find last position so that the key <= find_key
func (n *InternalPage) FindLastLE(key *KeyEntry) int {
	pos := -1
	for i := 0; i < int(n.NKeys); i++ {
		if n.Keys[i].Compare(key) > 0 {
			break
		}
		pos = i
	}
	return pos
}

// Insert a key-children pair into the Internal Node
func (n *InternalPage) InsertKV(key *KeyEntry, rightChild uint64) {
	pos := n.FindLastLE(key)

	// shift keys
	for i := int(n.NKeys); i > pos+1; i-- {
		n.Keys[i] = n.Keys[i-1]
	}

	// shift children
	for i := int(n.NKeys) + 1; i > pos+2; i-- {
		n.Children[i] = n.Children[i-1]
	}

	n.Keys[pos+1] = *key
	n.Children[pos+2] = rightChild
	n.NKeys++
}

// func (node *InternalPage) DelKVAtPos(pos int) {
// 	for i := pos; i < int(node.NKeys)-1; i++ {
// 		node.Keys[i] = node.Keys[i+1]
// 	}

// 	for i := pos + 1; i < int(node.NKeys); i++ {
// 		node.Children[i] = node.Children[i+1]
// 	}

// 	node.Keys[node.NKeys-1] = KeyEntry{}
// 	node.Children[node.NKeys] = 0
// 	node.NKeys -= 1
// }

// Split a node into 2 equal part
func (n *InternalPage) Split() (*InternalPage, *KeyEntry) {
	var newKeys [config.MAX_KEYS]KeyEntry
	var newChildren [config.MAX_CHILDREN]uint64

	mid := n.NKeys / 2

	middleKey := n.Keys[mid]

	// Move keys[mid..] → new node
	for i := mid; i < n.NKeys; i++ {
		newKeys[i-mid] = n.Keys[i]
		n.Keys[i] = KeyEntry{}
	}

	// Move children[mid..]
	for i := mid; i <= n.NKeys; i++ {
		newChildren[i-mid] = n.Children[i]
		n.Children[i] = 0
	}

	newNode := InternalPage{
		Header:   PageHeader{PageType: 1},
		NKeys:    n.NKeys - mid,
		Keys:     newKeys,
		Children: newChildren,
	}

	n.NKeys = mid
	return &newNode, &middleKey
}

func (p *InternalPage) IsLeaf() bool {
	return false
}

func (p *InternalPage) IsOverflow() bool {
	return p.NKeys > config.MAX_KEYS
}
