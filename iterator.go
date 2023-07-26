package lovedb

import (
	"bytes"
	"lovedb/index"
)

// Iterator 面向用户的迭代器
type Iterator struct {
	indexIter index.Iterator //索引迭代器
	db        *DB
	options   IteratorOptions
}

// NewIterator 初始化迭代器
func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		db:        db,
		indexIter: indexIter,
		options:   opts,
	}
}

func (i *Iterator) Rewind() {
	i.indexIter.Rewind()
	i.skipToNext()
}

func (i *Iterator) Seek(key []byte) {
	i.indexIter.Seek(key)
	i.skipToNext()
}

func (i *Iterator) Next() {
	//k1 k2 j3 k4
	i.indexIter.Next()
	//next完了发现是j3，不符合前缀，就再跳一下，符合了直接return
	i.skipToNext()
}

func (i *Iterator) Valid() bool {
	return i.indexIter.Valid()
}

func (i *Iterator) Key() []byte {
	return i.indexIter.Key()
}

// Value 用户需要拿到的是整体的value而不是索引pos
func (i *Iterator) Value() ([]byte, error) {
	logRecordPos := i.indexIter.Value()
	i.db.mu.Lock()
	defer i.db.mu.Unlock()
	return i.db.getValueByPos(logRecordPos)
}

func (i *Iterator) Close() {
	i.indexIter.Close()
}

// 筛选用户提供的prefix条件
func (i *Iterator) skipToNext() {
	preFixLen := len(i.options.Prefix)
	//如果prefix为空，无需处理
	if preFixLen == 0 {
		return
	}

	//执行完函数以后发现是j3，不符合前缀，就再跳一下，符合了直接return，说白了就是让迭代器跳几格
	for ; i.indexIter.Valid(); i.indexIter.Next() {
		key := i.indexIter.Key()
		//
		if preFixLen <= len(key) && bytes.Compare(i.options.Prefix, key[:preFixLen]) == 0 {
			return
		}
	}
}
