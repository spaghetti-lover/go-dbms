package disk

import "bytes"

type MetaPage struct {
	header PageHeader
}

func NewMetaPage() MetaPage {
	return MetaPage{
		header: PageHeader{
			pageType: 0,
			nextPagePointer: 0,
		},
	}
}

func (p *MetaPage) writeToBuffer(buffer *bytes.Buffer) error {
	if err := p.header.writeToBuffer(buffer); err != nil {
		return err
	}
	return nil
}

func (p *MetaPage) readFromBuffer(buffer *bytes.Buffer) {
	p.header.readFromBuffer(buffer)
}
