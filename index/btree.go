package index

import (
	"github.com/google/btree"
	"lovedb/data"
	"sync"
)

// Btree Btree索引，内存用到的数据结构，主要封装了Google的Btree库
type Btree struct {
	tree *btree.BTree
	lock *sync.RWMutex //读并发安全，写并发不安全，需要加锁
}

// NewBtree 工厂模式实例,初始化Btree索引结构
func NewBtree() *Btree {
	return &Btree{
		tree: btree.New(32), //32表示叶子节点的数量控制
		lock: new(sync.RWMutex),
	}
}

func (b Btree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	b.lock.Lock()
	defer b.lock.Unlock()
	b.tree.ReplaceOrInsert(it)
	return true
}

func (b Btree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	bTreeItem := b.tree.Get(it)
	//读取到为空则返回空
	if bTreeItem == nil {
		return nil
	}
	//断言转换为item结构体取里面的pos返回
	return bTreeItem.(*Item).pos
}

func (b Btree) Delete(key []byte) bool {
	it := &Item{key: key}
	b.lock.Lock()
	defer b.lock.Unlock()
	oldItem := b.tree.Delete(it)
	//通过判断旧值存不存在而反应操作是否有效
	if oldItem == nil {
		return false
	}
	return true
}
