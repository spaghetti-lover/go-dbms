package db

import (
	"bytes"
	"encoding/binary"
	"errors"
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

		default:
			return nil, errors.New("unknown value type")
		}
	}
	return res, nil
}
