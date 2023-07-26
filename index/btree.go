package index

import (
	"bytes"
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

func (b *Btree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	b.lock.Lock()
	defer b.lock.Unlock()
	b.tree.ReplaceOrInsert(it)
	return true
}

func (b *Btree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	bTreeItem := b.tree.Get(it)
	//读取到为空则返回空
	if bTreeItem == nil {
		return nil
	}
	//断言转换为item结构体取里面的pos返回
	return bTreeItem.(*Item).pos
}

func (b *Btree) Size() int {
	return b.tree.Len()
}

func (b *Btree) Delete(key []byte) bool {
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

// Iterator 初始化迭代器
func (b *Btree) Iterator(reverse bool) Iterator {
	if b.tree == nil {
		return nil
	}
	b.lock.RLock()
	defer b.lock.RUnlock()
	return NewBTreeIterator(b.tree, reverse)
}

// BtreeIterator BTree索引迭代器
type BtreeIterator struct {
	currIndex int     //当前遍历的下标位置
	reverse   bool    //是否是反向遍历
	values    []*Item //key + value的索引信息
}

// NewBTreeIterator 创建一个迭代器对象，该迭代器对象将能够按照指定的顺序遍历B树中的所有键值对。
func NewBTreeIterator(tree *btree.BTree, reverse bool) *BtreeIterator {
	//用一个数组存放key和value
	var idx int
	values := make([]*Item, tree.Len())

	//定义闭包函数
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		//false的话会终止遍历，要获取所有数据直接返回true
		return true
	}
	//Descend和Ascend方法会依次调用saveValues函数来遍历B树中的所有键值
	if reverse {
		//倒序放
		tree.Descend(saveValues)
	} else {
		//正序放
		tree.Ascend(saveValues)
	}
	return &BtreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind 重新回到迭代器的起点
func (b *BtreeIterator) Rewind() {
	b.currIndex = 0
}

// Seek 根据传入的 key 查找到第一个大于(或小于)等于的目标 key，根据从这个 key 开始遍历
func (b *BtreeIterator) Seek(key []byte) {
	//由于b树是有序的结构，我们想找到第一个大于等于的key，使用二分查找即可
	b.binarySearch(key)
}

// binarySearch
func (b *BtreeIterator) binarySearch(key []byte) {
	n := len(b.values)
	l, r := 0, n-1
	if !b.reverse {
		for l < r {
			mid := (l + r) / 2
			if bytes.Compare(b.values[mid].key, key) >= 0 {
				r = mid
			} else {
				l = mid + 1
			}
		}
	} else {
		for l < r {
			mid := (l + r) / 2
			if bytes.Compare(b.values[mid].key, key) >= 0 {
				l = mid
			} else {
				r = mid - 1
			}
		}
	}

	b.currIndex = l
}

func (b *BtreeIterator) Next() {
	b.currIndex++
}

// Valid 表示当前的索引是否有效，超过长度就无效了
func (b *BtreeIterator) Valid() bool {
	return b.currIndex < len(b.values)
}

func (b *BtreeIterator) Key() []byte {
	return b.values[b.currIndex].key
}

func (b *BtreeIterator) Value() *data.LogRecordPos {
	return b.values[b.currIndex].pos
}

func (b *BtreeIterator) Close() {
	b.values = nil
}
