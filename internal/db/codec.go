package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

// Layout: | prefix (1) | [ TYPE | LEN | DATA ]... |
// TYPE: 1 byte
// LEN : 4 bytes (uint32)
// DATA:
// bytes → raw
// int64 → 8 bytes (big endian)

func encodeKey(prefix uint8, vals []Value) []byte {
	buf := bytes.NewBuffer(nil)
	buf.WriteByte(prefix)

	for _, v := range vals {
		buf.WriteByte(byte(v.Type))

		switch v.Type {
		case ValueBytes:
			if len(v.Bytes) > math.MaxUint32 {
				panic("value too large")
			}
			binary.Write(buf, binary.BigEndian, uint32(len(v.Bytes)))
			buf.Write(v.Bytes)

		case ValueInt64:
			binary.Write(buf, binary.BigEndian, uint32(8))
			binary.Write(buf, binary.BigEndian, v.I64)

		default:
			panic("unknown value type")
		}
	}
	return buf.Bytes()
}

func encodeValue(vals []Value) []byte {
	buf := bytes.NewBuffer(nil)

	for _, v := range vals {
		buf.WriteByte(byte(v.Type))

		switch v.Type {
		case ValueBytes:
			binary.Write(buf, binary.BigEndian, uint32(len(v.Bytes)))
			buf.Write(v.Bytes)

		case ValueInt64:
			binary.Write(buf, binary.BigEndian, uint32(8))
			binary.Write(buf, binary.BigEndian, v.I64)

		default:
			panic("unknown value type")
		}
	}
	return buf.Bytes()
}

func decodeValue(data []byte) ([]Value, error) {
	buf := bytes.NewReader(data)
	res := make([]Value, 0)

	for buf.Len() > 0 {
		t, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}

		var l uint32
		if err := binary.Read(buf, binary.BigEndian, &l); err != nil {
			return nil, err
		}

		switch ValueType(t) {
		case ValueBytes:
			b := make([]byte, l)
			if _, err := buf.Read(b); err != nil {
				return nil, err
			}
			res = append(res, NewBytesValue(b))

		case ValueInt64:
			if l != 8 {
				return nil, errors.New("invalid int64 length")
			}
			var v int64
			if err := binary.Read(buf, binary.BigEndian, &v); err != nil {
				return nil, err
			}
			res = append(res, NewInt64Value(v))
			// Decode values from byte slice.Format: [ TYPE | LEN | DATA ]...
		default:
			return nil, errors.New("unknown value type")
		}
	}
	return res, nil
}

func extractIndexedValues(idx *IndexDef, rec *Record) []Value {
	vals := make([]Value, len(idx.Cols))
	for i, col := range idx.Cols {
		found := false
		for j, rcol := range rec.Cols {
			if col == rcol {
				vals[i] = rec.Vals[j]
				found = true
				break
			}
		}
		if !found {
			panic("column not found in record: " + col)
		}
	}
	return vals
}

func encodeIndexKey(idx *IndexDef, rec *Record, pkVals []Value) []byte {
	vals := extractIndexedValues(idx, rec)
	vals = append(vals, pkVals...)
	return encodeKey(idx.Prefix, vals)
}

func decodeRecord(tdef *TableDef, key, val []byte) (*Record, error) {
	rec := &Record{
		Cols: append([]string{}, tdef.Cols...),
		Vals: make([]Value, len(tdef.Cols)),
	}

	keyVals, err := decodeValue(key[1:])
	if err != nil {
		return nil, err
	}
	for i := 0; i < int(tdef.PKeyN); i++ {
		rec.Vals[i] = keyVals[i]
	}
	valVals, err := decodeValue(val)
	if err != nil {
		panic(err)
	}
	for i := int(tdef.PKeyN); i < len(tdef.Cols); i++ {
		rec.Vals[i] = valVals[i-int(tdef.PKeyN)]
	}
	return rec, nil
}

func extractPrimaryKeyFromIndexKey(idxKey []byte, tdef *TableDef) []byte {
	// idxKey = | idxPrefix | indexedCols | primaryKey |
	// Take the last values as primary key
	// Assume encodeKey always preserves order and type
	// Need to know the number of PK and type of each PK
	// for simplicity, decode all and take last N
	vals, err := decodeValue(idxKey[1:])
	if err != nil {
		panic(err)
	}
	if len(vals) < int(tdef.PKeyN) {
		panic("not enough values in index key")
	}
	pkVals := vals[len(vals)-int(tdef.PKeyN):]
	return encodeKey(tdef.Prefix, pkVals)
}
