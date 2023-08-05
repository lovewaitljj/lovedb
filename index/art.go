package index

import (
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"lovedb/data"
	"sync"
)

// AdaptiveRadixTree 自适应基数树索引
// 主要封装了https://github.com/plar/go-adaptive-radix-tree 库
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

// NewART 初始化art树索引
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.Lock()
	defer art.lock.Unlock()
	art.tree.Insert(key, pos)
	return true
}

func (art AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	//value是一个空接口，可以通过断言转化为任意类型
	return value.(*data.LogRecordPos)

}

func (art AdaptiveRadixTree) Delete(key []byte) bool {
	art.lock.Lock()
	defer art.lock.Unlock()
	_, isDeleted := art.tree.Delete(key)
	return isDeleted
}

func (art AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return art.tree.Size()
}

func (art AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	if art.tree == nil {
		return nil
	}
	art.lock.RLock()
	defer art.lock.RUnlock()
	return NewArtIterator(art.tree, reverse)
}

// artIterator art索引迭代器
type artIterator struct {
	currIndex int     //当前遍历的下标位置
	reverse   bool    //是否是反向遍历
	values    []*Item //key + value的索引信息
}

// NewArtIterator 创建一个迭代器对象，该迭代器对象将能够按照指定的顺序遍历art中的所有键值对。
func NewArtIterator(tree goart.Tree, reverse bool) *artIterator {
	//用一个数组存放key和value
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())

	//定义闭包函数,规则是往里加入item，reverse的话就从后往前
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}
	//将规则传进来去遍历每一个node放入到values中
	tree.ForEach(saveValues)

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind 重新回到迭代器的起点
func (art artIterator) Rewind() {
	art.currIndex = 0
}

// Seek 根据传入的 key 查找到第一个大于(或小于)等于的目标 key，根据从这个 key 开始遍历
func (art artIterator) Seek(key []byte) {
	//由于b树是有序的结构，我们想找到第一个大于等于的key，使用二分查找即可
	art.binarySearch(key)
}

// binarySearch
func (art artIterator) binarySearch(key []byte) {
	n := len(art.values)
	l, r := 0, n-1
	if !art.reverse {
		for l < r {
			mid := (l + r) / 2
			if bytes.Compare(art.values[mid].key, key) >= 0 {
				r = mid
			} else {
				l = mid + 1
			}
		}
	} else {
		for l < r {
			mid := (l + r) / 2
			if bytes.Compare(art.values[mid].key, key) >= 0 {
				l = mid
			} else {
				r = mid - 1
			}
		}
	}

	art.currIndex = l
}

func (art artIterator) Next() {
	art.currIndex++
}

// Valid 表示当前的索引是否有效，超过长度就无效了
func (art artIterator) Valid() bool {
	return art.currIndex < len(art.values)
}

func (art artIterator) Key() []byte {
	return art.values[art.currIndex].key
}

func (art artIterator) Value() *data.LogRecordPos {
	return art.values[art.currIndex].pos
}

func (art artIterator) Close() {
	art.values = nil
}
