package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	//正常情况
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))

	//value为空
	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	res2, n2 := EncodeLogRecord(rec2)
	//240712713
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))

	// 删除类型
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordDeleted,
	}
	res3, n3 := EncodeLogRecord(rec3)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
}

func TestDecodeLogRecordHeader(t *testing.T) {
	//[104 82 240 150 0 8 20 110 97 109 101 98 105 116 99 97 115 107 45 103 111]
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	h1, n1 := DecodeLogRecordHeader(headerBuf1)
	assert.NotNil(t, h1)
	assert.Equal(t, int64(7), n1)
	assert.Equal(t, uint32(2532332136), h1.crc)
	assert.Equal(t, LogRecordNormal, h1.recordType)
	assert.Equal(t, uint32(4), h1.keySize)
	assert.Equal(t, uint32(10), h1.valueSize)

	//2
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	h2, n2 := DecodeLogRecordHeader(headerBuf2)
	assert.NotNil(t, h2)
	assert.Equal(t, int64(7), n2)
	assert.Equal(t, uint32(240712713), h2.crc)
	assert.Equal(t, LogRecordNormal, h2.recordType)
	assert.Equal(t, uint32(4), h2.keySize)
	assert.Equal(t, uint32(0), h2.valueSize)

	//3 [43 153 86 17 1 8 20 ] 21  crc： 290887979
	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	h3, n3 := DecodeLogRecordHeader(headerBuf3)
	assert.NotNil(t, h3)
	assert.Equal(t, int64(7), n3)
	assert.Equal(t, uint32(290887979), h3.crc)
	assert.Equal(t, LogRecordDeleted, h3.recordType)
	assert.Equal(t, uint32(4), h3.keySize)
	assert.Equal(t, uint32(10), h3.valueSize)
}

func TestGetLogRecordCRC(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	headerBuf := []byte{104, 82, 240, 150, 0, 8, 20}
	crc1 := getLogRecordCRC(rec1, headerBuf[crc32.Size:])
	assert.Equal(t, uint32(2532332136), crc1)

	//2
	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := getLogRecordCRC(rec2, headerBuf2[crc32.Size:])
	assert.Equal(t, uint32(240712713), crc2)

	//3
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordDeleted,
	}
	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	crc3 := getLogRecordCRC(rec3, headerBuf3[crc32.Size:])
	assert.Equal(t, uint32(290887979), crc3)
}
