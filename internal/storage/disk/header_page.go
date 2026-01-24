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
	pageType        uint8
	nextPagePointer uint64
}

// Input: {page_type = 1, next = 1024}. Output: buffer = [ 1 0 0 0 0 0 0 255 255 ]
// int 183746238746
// Big Endian: [0, 0, 0, 42, 200, 33, 25, 26] // First byte is the largest
// Little Endian: [5 4 3 2 1 255 255 255 ... 0 0 0 0 0 0 0] // First byte is the smallest
// Big Endian is chosen so that page headers preserve natural ordering
// when compared lexicographically.
func (h *PageHeader) writeToBuffer(buffer *bytes.Buffer) error {
	if err := binary.Write(buffer, binary.BigEndian, h.pageType); err != nil {
		return err
	}

	if err := binary.Write(buffer, binary.BigEndian, h.nextPagePointer); err != nil {
		return err
	}

	return nil
}

func (h *PageHeader) readFromBuffer(buffer *bytes.Buffer) error {
	if err := binary.Read(buffer, binary.BigEndian, &h.pageType); err != nil {
		return err
	}

	if err := binary.Read(buffer, binary.BigEndian, &h.nextPagePointer); err != nil {
		return err
	}

	return nil
}
