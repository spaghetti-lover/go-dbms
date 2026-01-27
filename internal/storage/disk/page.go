package disk

import "bytes"

type Page interface {
	WriteToBuffer(buf *bytes.Buffer) error
	IsLeaf() bool
}
