package disk

import (
	"bytes"
	"encoding/binary"
)

// For example:
// 3: [1, 7, 255]
// [0 0 0 0 0 0 1 7 255]
type KeyEntry struct {
	KeyLen uint16
	Key    [MAX_KEY_SIZE]byte
}

// input: 8
// output: [MAX_KEY_SIZE]uint8 {0,0,...,0,0,0,8}
func NewKeyEntryFromInt(v int64) *KeyEntry {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))

	var data [MAX_KEY_SIZE]byte
	RightAlignCopy(data[:], buf[:])

	return &KeyEntry{
		KeyLen: 8,
		Key:    data,
	}
}

// input: []byte{0, 0, 0, 0, 255, 255, 1, 2}
// output: [MAX_KEY_SIZE]uint8 {0,0,...,255,255,1,2}
func NewKeyEntryFromBytes(input []byte) *KeyEntry {
	var data [MAX_KEY_SIZE]byte
	RightAlignCopy(data[:], input)

	return &KeyEntry{
		KeyLen: uint16(len(input)),
		Key:    data,
	}
}

func NewKeyEntryFromKeyVal(kv *KeyVal) *KeyEntry {
	return &KeyEntry{
		KeyLen: kv.KeyLen,
		Key:    kv.Key,
	}
}

func (k *KeyEntry) writeToBuffer(buf *bytes.Buffer) error {
	if err := binary.Write(buf, binary.BigEndian, k.KeyLen); err != nil {
		return err
	}

	start := MAX_KEY_SIZE - int(k.KeyLen)
	_, err := buf.Write(k.Key[start:])
	return err
}

func (k *KeyEntry) readFromBuffer(buf *bytes.Buffer) error {
	if err := binary.Read(buf, binary.BigEndian, &k.KeyLen); err != nil {
		return err
	}

	start := MAX_KEY_SIZE - int(k.KeyLen)
	_, err := buf.Read(k.Key[start:])
	return err
}

func (k *KeyEntry) Compare(rhs *KeyEntry) int {
	kStart := MAX_KEY_SIZE - int(k.KeyLen)
	rStart := MAX_KEY_SIZE - int(rhs.KeyLen)
	kSlice := k.Key[kStart:]
	rSlice := rhs.Key[rStart:]

	minLen := len(kSlice)
	if len(rSlice) < minLen {
		minLen = len(rSlice)
	}
	for i := 0; i < minLen; i++ {
		if kSlice[i] < rSlice[i] {
			return -1
		} else if kSlice[i] > rSlice[i] {
			return 1
		}
	}

	// If equal, smaller length will be smaller
	if len(kSlice) < len(rSlice) {
		return -1
	} else if len(kSlice) > len(rSlice) {
		return 1
	}
	return 0
}
