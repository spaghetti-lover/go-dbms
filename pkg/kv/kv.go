package kv

// type KV struct {
// 	fileName string
// 	tree     disk.BPlusTreeDisk
// }

// func (db *KV) Open() {
// 	// Load or create new
// 	db.tree = disk.NewBPlusTreeDisk(db.fileName)
// }

// func (db *KV) Get(key []byte)	([]byte, bool, error) {
// 	res, err := db.tree.Find(key)
// 	if res == nil {
// 		var valueBytes []byte = make([]byte, 0)
// 		return valueBytes, false, err
// 	}
// 	var valueBytes []byte = make([]byte, res.ValLen)
// 	for i := 0; i < int(res.ValLen); i += 1 {
// 		valueBytes[i] = res.Val[i+(disk.MAX_VAL_SIZE-int(res.ValLen))]
// 	}
// 	return valueBytes, true, err
// }

// func (db *KV) Set(key []byte, val []byte) error {
// 	return db.tree.Set(key, val)
// }

// func (db *KV) Del(key []byte) (bool, error) {
// 	return db.tree.Del(key)
// }
