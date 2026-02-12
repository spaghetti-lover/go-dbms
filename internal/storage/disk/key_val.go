package disk

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const MAX_KEY_SIZE = 64
const MAX_VAL_SIZE = 64

var ErrKeyNotFound = fmt.Errorf("key not found")

// 3: [1, 7, 255]
// 0: [0 0 0 0 0 0 1 7 255]
type KeyVal struct {
	KeyLen uint16
	ValLen uint16
	Key    [MAX_KEY_SIZE]uint8 // BigEndian storage
	Val    [MAX_VAL_SIZE]uint8 // BigEndian storage
}

func (kv *KeyVal) GetRightAlignedKey() []byte {
	return kv.Key[MAX_KEY_SIZE-int(kv.KeyLen):]
}

func (kv *KeyVal) GetRightAlignedValue() []byte {
	return kv.Val[MAX_VAL_SIZE-int(kv.ValLen):]
}

// Value returns the value bytes (for backward compatibility)
func (kv *KeyVal) Value() []byte {
	return kv.GetRightAlignedValue()
}

// RightAlignCopy copies src to the end of dst slice
func RightAlignCopy(dst []byte, src []byte) {
	// For example:
	// dst := make([]byte, 8)
	// src := []byte{0x00, 0x00, 0x01, 0x2C}
	// RightAlignCopy(dst, src) = [0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x2C]
	copy(dst[len(dst)-len(src):], src)
}

// From int64 key/value (BigEndian, sortable)
func NewKeyValFromInt(k, v int64) KeyVal {
	var key [MAX_KEY_SIZE]uint8
	var val [MAX_VAL_SIZE]uint8

	var keyBuf [8]byte
	var valBuf [8]byte

	binary.BigEndian.PutUint64(keyBuf[:], uint64(k))
	binary.BigEndian.PutUint64(valBuf[:], uint64(v))

	RightAlignCopy(key[:], keyBuf[:])
	RightAlignCopy(val[:], valBuf[:])

	return KeyVal{
		KeyLen: 8,
		ValLen: 8,
		Key:    key,
		Val:    val,
	}
}

// From raw bytes
func NewKeyValFromBytes(k, v []byte) KeyVal {
	var key [MAX_KEY_SIZE]uint8
	var val [MAX_VAL_SIZE]uint8

	RightAlignCopy(key[:], k)
	RightAlignCopy(val[:], v)

	return KeyVal{
		KeyLen: uint16(len(k)),
		ValLen: uint16(len(v)),
		Key:    key,
		Val:    val,
	}
}

// Layout:
// [KeyLen][ValLen][Key bytes][Val bytes]
func (kv *KeyVal) writeToBuffer(buf *bytes.Buffer) error {
	if err := binary.Write(buf, binary.BigEndian, kv.KeyLen); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, kv.ValLen); err != nil {
		return err
	}

	buf.Write(kv.Key[MAX_KEY_SIZE-int(kv.KeyLen):])
	buf.Write(kv.Val[MAX_VAL_SIZE-int(kv.ValLen):])
	return nil
}

func (kv *KeyVal) readFromBuffer(buf *bytes.Buffer) error {
	if err := binary.Read(buf, binary.BigEndian, &kv.KeyLen); err != nil {
		return err
	}
	if err := binary.Read(buf, binary.BigEndian, &kv.ValLen); err != nil {
		return err
	}

	// zero old data
	kv.Key = [MAX_KEY_SIZE]uint8{}
	kv.Val = [MAX_VAL_SIZE]uint8{}

	buf.Read(kv.Key[MAX_KEY_SIZE-int(kv.KeyLen):])
	buf.Read(kv.Val[MAX_VAL_SIZE-int(kv.ValLen):])
	return nil
}

// Lexicographical compare (BigEndian sortable)
func (kv *KeyVal) Compare(other *KeyVal) int {
	kSlice := kv.Key[MAX_KEY_SIZE-int(kv.KeyLen):]
	oSlice := other.Key[MAX_KEY_SIZE-int(other.KeyLen):]
	minLen := len(kSlice)
	if len(oSlice) < minLen {
		minLen = len(oSlice)
	}
	for i := 0; i < minLen; i++ {
		if kSlice[i] < oSlice[i] {
			return -1
		} else if kSlice[i] > oSlice[i] {
			return 1
		}
	}
	if len(kSlice) < len(oSlice) {
		return -1
	} else if len(kSlice) > len(oSlice) {
		return 1
	}
	return 0
}
