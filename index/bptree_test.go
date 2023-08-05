package index

import (
	"lovedb/data"
	"os"
	"path/filepath"
	"testing"
)

func TestBplusTree_Put(t *testing.T) {
	path := filepath.Join("../tmp")
	defer func() {
		//fixme 无法删除掉该文件
		err := os.RemoveAll(filepath.Join(path, bptreeIndexFileName))
		if err != nil {
			t.Log(err)
		}
	}()
	tree := NewBplusTree(path, false)
	tree.Put([]byte("aaaa"), &data.LogRecordPos{Fid: 2, Offset: 3})
	tree.Put([]byte("aaab"), &data.LogRecordPos{Fid: 23, Offset: 4})
}

func TestBplusTree_Get(t *testing.T) {
	path := filepath.Join("../tmp")
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBplusTree(path, false)
	tree.Put([]byte("aaaa"), &data.LogRecordPos{Fid: 123, Offset: 999})
	value := tree.Get([]byte("aaaa"))
	t.Log(value)
}
