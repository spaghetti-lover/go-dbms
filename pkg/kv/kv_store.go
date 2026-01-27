package kv

import "fmt"

type KVEngine interface {
	Get(key []byte) ([]byte, bool)
	Set(key, val []byte) error
	Del(key []byte) (bool, error)
	Close() error
}

type KV struct {
	fileName string
	engine   KVEngine
}

func NewKV(engine KVEngine) *KV {
	return &KV{engine: engine}
}

func (kv *KV) Get(key []byte) ([]byte, bool) {
	return kv.engine.Get(key)
}

func (kv *KV) Set(key, val []byte) error {
	return kv.engine.Set(key, val)
}

func (kv *KV) Del(key []byte) (bool, error) {
	return kv.engine.Del(key)
}

func (kv *KV) Open(engineType, fileName string) error {
	var engine KVEngine
	var err error

	switch engineType {
	case "bptree":
		engine, err = NewBPTreeEngine(fileName)
	default:
		return fmt.Errorf("unknown engine type: %s", engineType)
	}
	if err != nil {
		return err
	}
	kv.engine = engine
	kv.fileName = fileName
	return nil
}

func (kv *KV) Close() error {
	return kv.engine.Close()
}
