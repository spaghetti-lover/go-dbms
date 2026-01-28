package wal

import (
	"encoding/binary"
	"os"
)

type WALEntry struct {
	Op    byte //0 = Set, 1 = Del
	Key   []byte
	Value []byte // only for Set
}

func WriteWAL(f *os.File, entry *WALEntry) error {
	// Format: [Op][keyLen][valLen][key][val]
	binary.Write(f, binary.BigEndian, entry.Op)
	binary.Write(f, binary.BigEndian, uint32(len(entry.Key)))
	binary.Write(f, binary.BigEndian, uint32(len(entry.Value)))
	f.Write(entry.Key)
	f.Write(entry.Value)
	return f.Sync()
}

func ReadAllWAL(f *os.File) ([]WALEntry, error) {
	var entries []WALEntry
	for {
		var op byte
		var klen, vlen uint32
		if err := binary.Read(f, binary.BigEndian, &op); err != nil {
			break
		}
		if err := binary.Read(f, binary.BigEndian, &klen); err != nil {
			break
		}
		if err := binary.Read(f, binary.BigEndian, &vlen); err != nil {
			break
		}
		key := make([]byte, klen)
		val := make([]byte, vlen)
		f.Read(key)
		f.Read(val)
		entries = append(entries, WALEntry{Op: op, Key: key, Value: val})
	}
	return entries, nil
}
