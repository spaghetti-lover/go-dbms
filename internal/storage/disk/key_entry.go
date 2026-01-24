package disk

import (
	"bytes"
	"encoding/binary"
)

const MAX_KEY_SIZE = 8

// For example:
// 3: [1, 7, 255]
// [0 0 0 0 0 0 1 7 255]
type KeyEntry struct {
	len  uint16              // Actual length of key
	data [MAX_KEY_SIZE]uint8 // Data of key
}

func NewKeyEntryFromInt(input int64) KeyEntry {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, input); err != nil {
		panic(err)
	}
	data_slice := buf.Bytes()
	data_len := len(data_slice)

	var data [MAX_KEY_SIZE]uint8

	for i := MAX_KEY_SIZE - data_len; i < MAX_KEY_SIZE; i += 1 {
		// For example: MAX_KEY_SIZE = 8, data_len = 2 => data[6] = data_slice[0], data[7] = data_slice[1]
		data[i] = data_slice[i-(MAX_KEY_SIZE-data_len)]
	}

	return KeyEntry{
		len:  uint16(data_len),
		data: data,
	}
}

func (k *KeyEntry) writeToBuffer(buffer *bytes.Buffer) error {
	if err := binary.Write(buffer, binary.BigEndian, k.len); err != nil {
		return err
	}

	for i := uint16(0); i < k.len; i += 1 {
		if err := binary.Write(buffer, binary.BigEndian, k.data[MAX_KEY_SIZE-k.len+i]); err != nil {
			return err
		}
	}
	return nil
}

func (k *KeyEntry) readFromBuffer(buffer *bytes.Buffer) error {
	if err := binary.Read(buffer, binary.BigEndian, &k.len); err != nil {
		return err
	}

	for i := uint16(0); i < k.len; i += 1 {
		if err := binary.Read(buffer, binary.BigEndian, &k.data[MAX_KEY_SIZE-k.len+i]); err != nil {
			return err
		}
	}
	return nil
}

func (k *KeyEntry) compare(rhs *KeyEntry) int {
	res := 0

	for i := 0; i < MAX_KEY_SIZE; i += 1 {
		if k.data[i] < rhs.data[i] {
			return -1
		}

		if k.data[i] > rhs.data[i] {
			return 1
		}
	}

	return res
}
