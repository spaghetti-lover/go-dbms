package disk

import (
	"bytes"
	"encoding/binary"
)

// 0: Meta Page
// 1: Internal Page
// 2: Leaf Page
// ...: not support

type PageHeader struct {
	PageType        uint8
	NextPagePointer uint64
}

// Input: {page_type = 1, next = 1024}. Output: buffer = [ 1 0 0 0 0 0 0 255 255 ]
// int 183746238746
// Big Endian: [0, 0, 0, 42, 200, 33, 25, 26] // First byte is the largest
// Little Endian: [5 4 3 2 1 255 255 255 ... 0 0 0 0 0 0 0] // First byte is the smallest
// Big Endian is chosen so that page headers preserve natural ordering
// when compared lexicographically.
func (h *PageHeader) WriteToBuffer(buf *bytes.Buffer) error {
	if err := binary.Write(buf, binary.BigEndian, h.PageType); err != nil {
		return err
	}

	if err := binary.Write(buf, binary.BigEndian, h.NextPagePointer); err != nil {
		return err
	}

	return nil
}

func (h *PageHeader) ReadFromBuffer(buf *bytes.Buffer) error {
	if err := binary.Read(buf, binary.BigEndian, &h.PageType); err != nil {
		return err
	}

	if err := binary.Read(buf, binary.BigEndian, &h.NextPagePointer); err != nil {
		return err
	}

	return nil
}
