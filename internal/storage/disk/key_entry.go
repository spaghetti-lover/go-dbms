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
	Key   [MAX_KEY_SIZE]byte
}

func NewKeyEntryFromInt(v int64) *KeyEntry {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(v))

	var data [MAX_KEY_SIZE]byte
	rightAlignCopy(data[:], buf[:])

	return &KeyEntry{
		KeyLen: 8,
		Key:   data,
	}
}

// our key: [0 0 0 0 255 255 1 2]
// input  : [0 0 0 0 0 0 1 2]
func NewKeyEntryFromBytes(input []byte) *KeyEntry {
	var data [MAX_KEY_SIZE]byte
	rightAlignCopy(data[:], input)

	return &KeyEntry{
		KeyLen: uint16(len(input)),
		Key:   data,
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
	for i := 0; i < MAX_KEY_SIZE; i++ {
		if k.Key[i] < rhs.Key[i] {
			return -1
		}
		if k.Key[i] > rhs.Key[i] {
			return 1
		}
	}
	return 0
}
