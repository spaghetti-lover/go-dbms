package kv

import (
	"fmt"
	"sync"
)

type KVEngine interface {
	Get(key []byte) ([]byte, bool)
	Set(key, val []byte) error
	Del(key []byte) (bool, error)
	Close() error
	Scan(startKey, endKey []byte, fn func(key, val []byte) bool) error
}

type KV struct {
	Filename string
	Engine   KVEngine

	// Transaction support
	mu      sync.RWMutex  // Thread safety
	version uint64        // Current version counter
	history []CommittedTX // History for conflict detection
}

func NewKV(engine KVEngine) *KV {
	return &KV{Engine: engine}
}

func (kv *KV) Get(key []byte) ([]byte, bool) {
	return kv.Engine.Get(key)
}

func (kv *KV) Set(key, val []byte) error {
	return kv.Engine.Set(key, val)
}

func (kv *KV) Del(key []byte) (bool, error) {
	return kv.Engine.Del(key)
}

func (kv *KV) Scan(startKey, endKey []byte, fn func(key, val []byte) bool) error {
	return kv.Engine.Scan(startKey, endKey, fn)
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
	kv.Engine = engine
	kv.Filename = fileName
	return nil
}

func (kv *KV) Close() error {
	return kv.Engine.Close()
}
