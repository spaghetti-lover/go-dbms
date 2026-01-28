package disk

import (
	"bytes"
	"encoding/binary"
)

const LEAF_MAX_KV = (BLOCK_SIZE - 16 - 2) / (2 + 2 + MAX_KEY_SIZE + MAX_VAL_SIZE)

type LeafPage struct {
	Header PageHeader
	NKV    uint16
	KVs    [LEAF_MAX_KV]KeyVal
}

func NewLeafPage() *LeafPage {
	return &LeafPage{
		Header: PageHeader{
			PageType:        PageTypeLeaf,
			NextPagePointer: 0,
		},
		NKV: 0,
	}
}

func (p *LeafPage) WriteToBuffer(buf *bytes.Buffer) error {
	if err := p.Header.WriteToBuffer(buf); err != nil {
		return err
	}

	if err := binary.Write(buf, binary.BigEndian, p.NKV); err != nil {
		return err
	}

	for i := 0; i < int(p.NKV); i++ {
		if err := p.KVs[i].writeToBuffer(buf); err != nil {
			return err
		}
	}

	return nil
}

func (p *LeafPage) ReadFromBuffer(buf *bytes.Buffer, readHeader bool) error {
	if readHeader {
		if err := p.Header.ReadFromBuffer(buf); err != nil {
			return err
		}
	}

	if err := binary.Read(buf, binary.BigEndian, &p.NKV); err != nil {
		return err
	}

	for i := 0; i < int(p.NKV); i++ {
		if err := p.KVs[i].readFromBuffer(buf); err != nil {
			return err
		}
	}

	return nil
}

// Find last position so that the key <= find_key
func (p *LeafPage) FindLastLE(kv *KeyVal) int {
	pos := -1
	for i := 0; i < int(p.NKV); i++ {
		if p.KVs[i].Compare(kv) > 0 {
			break
		}
		pos = i
	}
	return pos
}

func (p *LeafPage) LowerBound(key *KeyEntry) int {
	l, r := 0, int(p.NKV)
	for l < r {
		m := (l + r) / 2
		entry := KeyEntry{KeyLen: p.KVs[m].KeyLen, Key: p.KVs[m].Key}
		if entry.Compare(key) < 0 {
			l = m + 1
		} else {
			r = m
		}
	}
	return l
}

// Insert a key-children pair into the Leaf Node
func (p *LeafPage) InsertKV(kv *KeyVal) bool {
	if p.NKV >= LEAF_MAX_KV {
		return false
	}
	pos := p.LowerBound(&KeyEntry{Key: kv.Key})

	for i := int(p.NKV); i > pos; i-- {
		p.KVs[i] = p.KVs[i-1]
	}
	p.KVs[pos] = *kv
	p.NKV++
	return true
}

// Delete a key val from Leaf Node
// Assume always able to find exact
func (p *LeafPage) DelKV(kv *KeyVal) {
	pos := p.FindLastLE(kv)

	for i := pos; i < int(p.NKV)-1; i++ {
		p.KVs[i] = p.KVs[i+1]
	}

	p.KVs[p.NKV-1] = KeyVal{}
	p.NKV--
}

// Split a node into 2 equal part
func (p *LeafPage) Split() (*LeafPage, *KeyEntry) {
	mid := int(p.NKV / 2)
	newLeaf := NewLeafPage()
	newLeaf.Header.NextPagePointer = p.Header.NextPagePointer
	newLeaf.NKV = p.NKV - uint16(mid)
	for i := 0; i < int(newLeaf.NKV); i++ {
		newLeaf.KVs[i] = p.KVs[mid+i]
		p.KVs[mid+i] = KeyVal{}
	}
	p.NKV = uint16(mid)
	sep := &KeyEntry{
		KeyLen: newLeaf.KVs[0].KeyLen,
		Key:    newLeaf.KVs[0].Key,
	}
	return newLeaf, sep
}

func (p *LeafPage) IsLeaf() bool {
	return true
}

func (p *LeafPage) IsOverflow() bool {
	return p.NKV > LEAF_MAX_KV
}

func (p *LeafPage) Delete(key *KeyEntry) bool {
	pos := -1
	for i := 0; i < int(p.NKV); i++ {
		entry := &KeyEntry{KeyLen: p.KVs[i].KeyLen, Key: p.KVs[i].Key}
		if entry.Compare(key) == 0 {
			pos = i
			break
		}
	}
	if pos == -1 {
		return false
	}
	for i := pos; i < int(p.NKV)-1; i++ {
		p.KVs[i] = p.KVs[i+1]
	}
	p.KVs[p.NKV-1] = KeyVal{}
	p.NKV--
	return true
}

func (p *LeafPage) MinKeys() uint16 {
	return LEAF_MAX_KV / 2
}

func (p *LeafPage) CanBorrow() bool {
	return p.NKV > p.MinKeys()
}
