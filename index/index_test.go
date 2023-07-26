package index

import (
	"github.com/stretchr/testify/assert"
	"lovedb/data"
	"testing"
)

// 用testify包来进行测试断言
func TestBtree_Put(t *testing.T) {
	bt := NewBtree()

	//插入nil
	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})

	assert.True(t, res1)

	//插入普通值
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	})
	assert.True(t, res2)
}

func TestBtree_Get(t *testing.T) {
	bt := NewBtree()

	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})

	assert.True(t, res1)
	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	})
	assert.True(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 3,
	})

	assert.True(t, res3)
	pos3 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos3.Fid)
	assert.Equal(t, int64(2), pos3.Offset)

}

func TestBtree_Delete(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res1)

	del1 := bt.Delete(nil)
	assert.True(t, del1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	})
	assert.True(t, res2)
	del2 := bt.Delete([]byte("a"))
	assert.True(t, del2)

}
