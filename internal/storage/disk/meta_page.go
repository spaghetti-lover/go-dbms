package disk

import (
	"bytes"
	"encoding/binary"
)

const (
	PageTypeMeta     = 0
	PageTypeInternal = 1
	PageTypeLeaf     = 2
)

const META_MAGIC uint32 = 0xDBDBDBDB

type MetaPage struct {
	Header  PageHeader
	Magic  uint32
	RootPID uint64
}

func NewMetaPage() *MetaPage {
	return &MetaPage{
		Header: PageHeader{
			PageType:        PageTypeMeta,
			NextPagePointer: 0,
		},
		Magic:  META_MAGIC,
		RootPID: 0,
	}
}

func (p *MetaPage) WriteToBuffer(buf *bytes.Buffer) error {
	if err := p.Header.WriteToBuffer(buf); err != nil {
		return err
	}
	return binary.Write(buf, binary.BigEndian, p.RootPID)
}

func (p *MetaPage) ReadFromBuffer(buf *bytes.Buffer) error {
	if err := p.Header.ReadFromBuffer(buf); err != nil {
		return err
	}
	return binary.Read(buf, binary.BigEndian, &p.RootPID)
}
