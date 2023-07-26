package index

import (
	"github.com/stretchr/testify/assert"
	"lovedb/data"
	"testing"
)

// 测试迭代器初始化
func TestBtree_Iterator(t *testing.T) {
	//1.空的情况
	b1 := NewBtree()
	iter1 := b1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	//2.有数据的情况
	b1.Put([]byte("k1"), &data.LogRecordPos{
		Fid:    1,
		Offset: 10,
	})
	iter2 := b1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	//3.多条数据的情况
	b1.Put([]byte("k3"), &data.LogRecordPos{
		Fid:    1,
		Offset: 10,
	})
	b1.Put([]byte("k4"), &data.LogRecordPos{
		Fid:    1,
		Offset: 10,
	})
	b1.Put([]byte("k2"), &data.LogRecordPos{
		Fid:    1,
		Offset: 10,
	})

	iter3 := b1.Iterator(false)
	//迭代器遍历整体,打印出的key是排序好的
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	//反向遍历
	iter4 := b1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

	//seek方法
	iter3.Rewind()
	iter3.Seek([]byte("k3"))
	assert.Equal(t, "k3", string(iter3.Key()))
	//seek反转情况
	iter4.Rewind()
	iter4.Seek([]byte("k3"))
	assert.Equal(t, "k3", string(iter4.Key()))

}
