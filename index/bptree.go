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

func NewBplusTree(dirPath string, syncWrites bool) *BplusTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrites
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		panic("failed to open bptree")
	}
	//创建对应的bucket
	//update 是一个读写事务，里面进行对bucket的读写,结束会自动提交事务
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}

	return &BplusTree{tree: bptree}
}

func (bp *BplusTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	//先拿到旧值
	var oldValue []byte
	//先拿到bucket
	if err := bp.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		oldValue = bucket.Get(key)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value in bucket")
	}
	if len(oldValue) == 0 {
		return nil
	}
	return data.DecodeLogRecordPos(oldValue)
}

func (bp *BplusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	//只读事务
	if err := bp.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get value in bucket")
	}
	return pos
}

func (bp *BplusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	var oldValue []byte
	if err := bp.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if oldValue = bucket.Get(key); len(oldValue) != 0 {
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value in bucket")
	}
	if len(oldValue) == 0 {
		return nil, false
	}
	return data.DecodeLogRecordPos(oldValue), true
}

func (bp *BplusTree) Size() int {
	var size int
	if err := bp.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size in bucket")
	}
	return size
}

func (bp *BplusTree) Iterator(reverse bool) Iterator {
	//TODO implement me
	panic("implement me")
}

func (bp *BplusTree) Close() error {
	return bp.tree.Close()
}

// BptreeIterator B+树迭代器
type BptreeIterator struct {
	tx       *bbolt.Tx
	cursor   *bbolt.Cursor
	reverse  bool
	curKey   []byte
	curValue []byte
}

// newBptreeIterator 创建一个迭代器对象，该迭代器对象将能够按照指定的顺序遍历B树中的所有键值对。
func newBptreeIterator(tree *bbolt.DB, reverse bool) *BptreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a tx")
	}
	bp := &BptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	//因为key和value一开始是空的，所以这里会导致valid方法判断为无效，所以先跳到第一个key
	bp.Rewind()
	return bp
}

// Rewind 重新回到迭代器的起点
func (b *BptreeIterator) Rewind() {
	if b.reverse {
		b.curKey, b.curValue = b.cursor.Last()
	} else {
		b.curKey, b.curValue = b.cursor.First()
	}

}

// Seek 根据传入的 key 查找到第一个大于(或小于)等于的目标 key，根据从这个 key 开始遍历
func (b *BptreeIterator) Seek(key []byte) {
	b.curKey, b.curValue = b.cursor.Seek(key)
}

func (b *BptreeIterator) Next() {
	if b.reverse {
		b.curKey, b.curValue = b.cursor.Prev()
	} else {
		b.curKey, b.curValue = b.cursor.Next()
	}
}

// Valid 表示当前的索引是否有效，超过长度就无效了
func (b *BptreeIterator) Valid() bool {
	return len(b.curKey) != 0
}

func (b *BptreeIterator) Key() []byte {
	return b.curKey
}

func (b *BptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(b.curValue)
}

func (b *BptreeIterator) Close() {
	_ = b.tx.Rollback()
}
