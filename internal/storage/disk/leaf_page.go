package disk

import (
	"bytes"
	"encoding/binary"

	"github.com/spaghetti-lover/go-db/internal/config"
)

const MAX_VAL_SIZE = config.MAX_VAL_SIZE
const LEAF_MAX_KV = config.LEAF_MAX_KV

// 3: [1, 7, 255]
// 0: [0 0 0 0 0 0 1 7 255]
type KeyVal struct {
	keyLen uint16
	valLen uint16
	key    [MAX_KEY_SIZE]uint8 // BigEndian storage
	val    [MAX_KEY_SIZE]uint8 // BigEndian storage
}

func NewKeyValFromInt(inputKey int64, inputVal int64) KeyVal {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, inputKey)
	dataSlice := buf.Bytes()
	keyLen := len(dataSlice)

	var key [MAX_KEY_SIZE]uint8

	for i := MAX_KEY_SIZE - keyLen; i < MAX_KEY_SIZE; i++ {
		key[i] = dataSlice[i-(MAX_KEY_SIZE-keyLen)] // data[10] = slice[0] | data[11] = slice[1]...
	}

	buf = new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, inputKey)
	dataSlice = buf.Bytes()
	valLen := len(dataSlice)

	var val [MAX_VAL_SIZE]uint8
	for i := MAX_VAL_SIZE - valLen; i < MAX_VAL_SIZE; i++ {
		key[i] = dataSlice[i-(MAX_VAL_SIZE-valLen)] // data[10] = slice[0] | data[11] = slice[1] ...
	}

	return KeyVal{
		keyLen: uint16(keyLen),
		valLen: uint16(valLen),
		key:    key,
		val:    val,
	}
}

func (k *KeyVal) writeToBuffer(buffer *bytes.Buffer) error {
	if err := binary.Write(buffer, binary.BigEndian, k.keyLen); err != nil {
		return err
	}

	if err := binary.Write(buffer, binary.BigEndian, k.valLen); err != nil {
		return err
	}

	for i := MAX_KEY_SIZE - int(k.keyLen); i < MAX_KEY_SIZE; i++ {
		if err := binary.Write(buffer, binary.BigEndian, k.key[i]); err != nil {
			return err
		}
	}

	for i := MAX_VAL_SIZE - int(k.valLen); i < MAX_VAL_SIZE; i++ {
		if err := binary.Write(buffer, binary.BigEndian, k.val[i]); err != nil {
			return err
		}
	}

	return nil
}

func (k *KeyVal) readFromBuffer(buffer *bytes.Buffer) error {
	if err := binary.Read(buffer, binary.BigEndian, k.keyLen); err != nil {
		return err
	}

	if err := binary.Read(buffer, binary.BigEndian, k.valLen); err != nil {
		return err
	}

	for i := MAX_KEY_SIZE - int(k.keyLen); i < MAX_KEY_SIZE; i++ {
		if err := binary.Read(buffer, binary.BigEndian, k.key[i]); err != nil {
			return err
		}
	}

	for i := MAX_VAL_SIZE - int(k.valLen); i < MAX_VAL_SIZE; i++ {
		if err := binary.Read(buffer, binary.BigEndian, k.val[i]); err != nil {
			return err
		}
	}

	return nil
}

func (k *KeyVal) compare(keyVal *KeyVal) int {
	for i := 0; i < MAX_KEY_SIZE; i += 1 {
		if k.key[i] < keyVal.key[i] {
			return -1
		}
		if k.key[i] > keyVal.key[i] {
			return 1
		}
	}

	return 0
}

type BPlusTreeLeafPage struct {
	header    PageHeader
	nKeyValue int
	keyValue  [LEAF_MAX_KV]KeyVal
}

func NewLeafPage() BPlusTreeLeafPage {
	var newKeyValue [LEAF_MAX_KV]KeyVal
	return BPlusTreeLeafPage{
		header: PageHeader{
			pageType:        2, // leaf node
			nextPagePointer: 0,
		},
		nKeyValue: 0,
		keyValue:  newKeyValue,
	}
}

func (p *BPlusTreeLeafPage) writeToBuffer(buffer *bytes.Buffer) error {
	if err := p.header.writeToBuffer(buffer); err != nil {
		return err
	}

	if err := binary.Write(buffer, binary.BigEndian, p.nKeyValue); err != nil {
		return err
	}

	for i := 0; i < int(p.nKeyValue); i += 1 {
		if err := p.keyValue[i].writeToBuffer(buffer); err != nil {
			return err
		}
	}

	return nil
}

func (p *BPlusTreeLeafPage) readFromBuffer(buffer *bytes.Buffer, isReadHeader bool) error {
	if isReadHeader {
		if err := p.header.readFromBuffer(buffer); err != nil {
			return err
		}
	}

	if err := binary.Read(buffer, binary.BigEndian, p.nKeyValue); err != nil {
		return err
	}

	for i := 0; i < int(p.nKeyValue); i += 1 {
		if err := p.keyValue[i].readFromBuffer(buffer); err != nil {
			return err
		}
	}

	return nil
}

// Find last position so that the key <= find_key
func (node *BPlusTreeLeafPage) FindLastLE(findKeyValue *KeyVal) int {
	pos := -1

	for i := 0; i < node.nKeyValue; i++ {
		if node.keyValue[i].compare(findKeyValue) == 1 {
			break
		}

		pos = i
	}

	return pos
}

// Insert a key-children pair into the Internal Node
func (node *BPlusTreeLeafPage) InsertKV(insertKV *KeyVal) {
	// Find last less or equal as position to insert
	pos := node.FindLastLE(insertKV)

	// [ 1,4,7,| | ] -> insert 3
	// [ 1,| |,4,7 ] -> insert 3
	for i := node.nKeyValue - 1; i > pos; i-- {
		node.keyValue[i+1] = node.keyValue[i]
	}

	node.keyValue[pos+1] = *insertKV
	// [1, 3, 4, 7]
	node.nKeyValue += 1
}

// Split a node into 2 equal part
func (node *BPlusTreeLeafPage) Split() BPlusTreeLeafPage {
	var newKV [LEAF_MAX_KV]KeyVal

	// Split in the middle
	pos := node.nKeyValue / 2

	// [ 1 , 2 , 0 , 0 ] -> pos = 2
	// [ 3 , 4 , 0 , 0 ]

	for i := pos; i < node.nKeyValue; i++ {
		newKV[i-pos] = node.keyValue[i]
		node.keyValue[i] = KeyVal{}
	}

	newNode := BPlusTreeLeafPage{
		nKeyValue: node.nKeyValue - pos,
		keyValue:  newKV,
	}

	node.nKeyValue = pos
	return newNode
}
