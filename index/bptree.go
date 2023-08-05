package index

import (
	"go.etcd.io/bbolt"
	"lovedb/data"
	"path/filepath"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

//b+树索引
//封装了go.etcd.io/bbolt库

type BplusTree struct {
	//支持并发访问，不需要上锁
	tree *bbolt.DB
}

func NewBplusTree(dirPath string) *BplusTree {
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, nil)
	if err != nil {
		panic("failed to open bptree")
	}
	//创建对应的bucket
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}

	return &BplusTree{tree: bptree}
}

func (bp BplusTree) Put(key []byte, pos *data.LogRecordPos) bool {

}

func (bp BplusTree) Get(key []byte) *data.LogRecordPos {
	//TODO implement me
	panic("implement me")
}

func (bp BplusTree) Delete(key []byte) bool {
	//TODO implement me
	panic("implement me")
}

func (bp BplusTree) Size() int {
	//TODO implement me
	panic("implement me")
}

func (bp BplusTree) Iterator(reverse bool) Iterator {
	//TODO implement me
	panic("implement me")
}
