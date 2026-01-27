package db

type ValueType uint8

const (
	ValueBytes ValueType = iota + 1
	ValueInt64
)

type Value struct {
	Type  ValueType
	I64   int64
	Bytes []byte
}

func NewBytesValue(b []byte) Value {
	return Value{Type: ValueBytes, Bytes: b}
}

func NewInt64Value(v int64) Value {
	return Value{Type: ValueInt64, I64: v}
}

// A record represents one row
type Record struct {
	Cols []string
	Vals []Value
}

func NewRecord() *Record {
	return &Record{}
}

func (r *Record) Add(col string, v Value) *Record {
	r.Cols = append(r.Cols, col)
	r.Vals = append(r.Vals, v)
	return r
}
