package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	//打开文件测试
	dataFile1, err := OpenDataFile("D:\\git_space\\lovedb\\tmp", 1)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	dataFile2, err := OpenDataFile("D:\\git_space\\lovedb\\tmp", 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)

	dataFile3, err := OpenDataFile("D:\\git_space\\lovedb\\tmp", 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile3)

}

func TestDataFile_Write(t *testing.T) {
	dataFile1, err := OpenDataFile("D:\\git_space\\lovedb\\tmp", 1)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	err = dataFile1.Write([]byte("aaa"))
	assert.Nil(t, err)
	err = dataFile1.Write([]byte("bbb"))
	assert.Nil(t, err)
	err = dataFile1.Write([]byte("ccc"))
	assert.Nil(t, err)

}

func TestDataFile_Close(t *testing.T) {
	dataFile1, err := OpenDataFile("D:\\git_space\\lovedb\\tmp", 123)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	err = dataFile1.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile1.Close()
	assert.Nil(t, err)

}

func TestDataFile_Sync(t *testing.T) {
	dataFile1, err := OpenDataFile("D:\\git_space\\lovedb\\tmp", 123)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	err = dataFile1.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile1.Sync()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile("..\\tmp", 444)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	//只有一条logRecord
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	err = dataFile.Write(res1)
	assert.Nil(t, err)
	readRec1, readSize1, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, rec1, readRec1)
	assert.Equal(t, n1, readSize1)

	//多条记录，从不同位置读取
	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("a new value come"),
		Type:  LogRecordNormal,
	}
	res2, n2 := EncodeLogRecord(rec2)
	err = dataFile.Write(res2)
	assert.Nil(t, err)
	readRec2, readSize2, err := dataFile.ReadLogRecord(readSize1)
	assert.Nil(t, err)
	assert.Equal(t, rec2, readRec2)
	assert.Equal(t, n2, readSize2)

	//数据在末尾且小于maxHeader的情况
	rec3 := &LogRecord{
		Key:   []byte("1"),
		Value: []byte(""),
		Type:  LogRecordDeleted,
	}
	res3, n3 := EncodeLogRecord(rec3)
	err = dataFile.Write(res3)
	assert.Nil(t, err)
	readRec3, readSize3, err := dataFile.ReadLogRecord(readSize1 + readSize2)
	assert.Nil(t, err)
	assert.Equal(t, rec3, readRec3)
	assert.Equal(t, n3, readSize3)
}
