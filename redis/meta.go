package redis

import (
	"encoding/binary"
	"math"
)

const (
	maxMetaDataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaSize = binary.MaxVarintLen64 * 2

	initialListMark = math.MaxUint64 / 2
)

// 元数据
type metaData struct {
	dataType byte   //数据类型
	expire   int64  //过期时间
	version  int64  //版本号
	size     uint32 //key对应的数据量
	head     uint64 //list的头部索引
	tail     uint64 //list的尾部索引
}

func (md *metaData) encode() []byte {
	var size = maxMetaDataSize
	if md.dataType == List {
		size += extraListMetaSize
	}

	//编码
	buf := make([]byte, size)
	buf[0] = md.dataType
	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	//如果是list还需要编码head和list
	if md.dataType == List {
		index += binary.PutVarint(buf[index:], int64(md.head))
		index += binary.PutVarint(buf[index:], int64(md.tail))
	}
	return buf[:index]
}

// DecodeLogRecordPos 解码LogRecordPos
func decode(buf []byte) *metaData {
	dataType := buf[0]
	index := 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	index += n

	var head uint64
	var tail uint64
	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, _ = binary.Uvarint(buf[index:])
	}
	md := &metaData{
		dataType: buf[0],
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
	return md

}

type hashDataKey struct {
	key     []byte
	version int64
	field   []byte
}

// hash数据的编码
func (h hashDataKey) encode() []byte {
	buf := make([]byte, len(h.key)+len(h.field)+8)
	//key
	var index = 0
	copy(buf[index:index+len(h.key)], h.key)
	index += len(h.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64((h.version)))
	index += 8
	//field
	copy(buf[index:], h.field)
	return buf
}

type setDataKey struct {
	key     []byte
	version int64
	member  []byte
}

// set数据的编码

func (sk *setDataKey) encode() []byte {
	buf := make([]byte, len(sk.key)+len(sk.member)+8+4)
	// key
	var index = 0
	copy(buf[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sk.version))
	index += 8

	// member
	copy(buf[index:index+len(sk.member)], sk.member)
	index += len(sk.member)

	// member size,最后四个字节放入member长度
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sk.member)))

	return buf
}
