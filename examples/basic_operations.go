package main

import (
	"fmt"
	bitcask "lovedb"
)

func main() {
	ops := bitcask.DefaultOptions
	ops.DirPath = "D:/git_space/lovedb/tmp"

	//fixme
	db, err := bitcask.Open(ops)
	if err != nil {
		panic(err)
	}

	//当目录为空时，put函数里面设置活跃文件部分新建了文件
	err = db.Put([]byte("name"), []byte("lovewait"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))

	//err = db.Delete([]byte("name"))
	//if err != nil {
	//	panic(err)
	//}
	//	panic: key not found in database
	val, err = db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))

}
