package redis

import (
	"encoding/binary"
	"errors"
	"lovedb"
	"time"
)

type redisDataType = byte

const (
	String redisDataType = iota
	Hash
	Set
	List
	ZSet
)

// 错误类型
var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

// RedisDataStructure redis数据结构服务
type RedisDataStructure struct {
	db *lovedb.DB
}

func NewRedisDataStructure(options lovedb.Options) (*RedisDataStructure, error) {
	db, err := lovedb.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{db: db}, nil
}

// ================== string数据结构 ==================

func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}
	//编码value ： type + expire + payload
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1
	var expire int64
	//当前时间+ttl = 过期时间
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)
	realVal := make([]byte, index+len(value))
	copy(realVal[:index], buf[:index])
	copy(realVal[:index], value)

	//调用接口写入
	return rds.db.Put(key, realVal)
}
func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	val, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}
	//先判断类型是不是string
	dataType := val[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}
	var index = 1
	//判断时间是否过期
	expire, n := binary.Varint(val[index:])
	if expire > 0 && time.Now().UnixNano() >= expire {
		return nil, nil
	}
	index += n
	return val[index:], nil
}

// ================== Hash数据结构 ==================

// HSet val 不存在的话不更新元数据，存在的话直接插入val即可
func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	//先查找元数据
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}
	//将key + field + version当作key
	hk := &hashDataKey{
		key:     key,
		field:   field,
		version: meta.version,
	}
	encKey := hk.encode()

	//先查找val是否存在，存在返回false
	var exist = true
	if _, err := rds.db.Get(encKey); err == lovedb.ErrKeyNotFound {
		exist = false
	}

	//开启原子写模式，保证元数据和真正的val是原子性
	wb := rds.db.NewWriteBatch(lovedb.DefaultWriteBatchOptions)

	//不存在就更新元数据
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	//之前有val，也可以去更新值
	_ = wb.Put(encKey, value)
	err = wb.Commit()
	if err != nil {
		return false, err
	}
	//如果返回true，则代表之前没有值，插入成功
	//如果返回false，则代表之前有值，更新成功
	return !exist, nil
}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	//先查找元数据
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}
	//将key + field + version当作key
	hk := &hashDataKey{
		key:     key,
		field:   field,
		version: meta.version,
	}
	encKey := hk.encode()

	return rds.db.Get(encKey)

}

// HDel 根据key和field去删除val
func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	//先查找元数据
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	//将key + field + version当作key
	hk := &hashDataKey{
		key:     key,
		field:   field,
		version: meta.version,
	}
	encKey := hk.encode()

	var exist = true
	if _, err := rds.db.Get(encKey); err == lovedb.ErrKeyNotFound {
		exist = false
	}
	if exist {
		wb := rds.db.NewWriteBatch(lovedb.DefaultWriteBatchOptions)
		meta.size--
		//修改元数据
		_ = wb.Put(key, meta.encode())
		_ = rds.db.Delete(encKey)
		if err := wb.Commit(); err != nil {
			return false, err
		}
	}

	//删除成功返回true
	//删除失败返回false
	return exist, nil

}

// ================== Set数据结构 ==================

// SAdd set添加元素，不可以添加重复元素，不会报错但是会返回false
func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	//先查找元数据
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}
	//构造key
	sk := &setDataKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	encKey := sk.encode()

	var ok bool
	//若encKey和之前添加的一样则直接返回false
	//可以添加多条记录，同一个key对应多个member，形成set数据结构
	if _, err := rds.db.Get(encKey); err == lovedb.ErrKeyNotFound {
		//开启原子写模式，保证元数据和真正的val是原子性
		wb := rds.db.NewWriteBatch(lovedb.DefaultWriteBatchOptions)
		//不存在就更新元数据
		meta.size++
		_ = wb.Put(key, meta.encode())
		//添加一条记录
		_ = wb.Put(encKey, nil)
		err = wb.Commit()
		if err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil
}

// SIsMember 判断用户传过来的member是否属于这个key
func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	//先查找元数据
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}
	//构造key
	sk := &setDataKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	encKey := sk.encode()
	_, err = rds.db.Get(encKey)
	if err != nil && err != lovedb.ErrKeyNotFound {
		return false, err
	}
	if err == lovedb.ErrKeyNotFound {
		return false, nil
	}
	return true, nil
}

// SRem 删除key对应的某个member
func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {
	//先查找元数据
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}
	//构造key
	sk := &setDataKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	encKey := sk.encode()
	_, err = rds.db.Get(encKey)
	if err != nil && err == lovedb.ErrKeyNotFound {
		return false, nil
	}
	//更新元数据
	wb := rds.db.NewWriteBatch(lovedb.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	//实则就删除某个值的记录
	_ = wb.Delete(encKey)
	err = wb.Commit()
	if err != nil {
		return false, err
	}
	return true, nil
}

// =====================工具方法===========================
func (rds RedisDataStructure) findMetaData(key []byte, dataType redisDataType) (*metaData, error) {
	metBuf, err := rds.db.Get(key)
	if err != nil && err != lovedb.ErrKeyNotFound {
		return nil, err
	}
	//定义返回值
	var meta *metaData
	var exist bool
	//如果key对应的元数据不存在，则新建这个元数据
	if err == lovedb.ErrKeyNotFound {
		exist = false
	} else {
		//找到元数据可以返回
		meta = decode(metBuf)
		//判断类型是否一致
		if dataType != meta.dataType {
			return nil, ErrWrongTypeOperation
		}
		//判断过期时间,过期的话key相当于没有找到元数据
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}
	//如果元数据不存在，我们就在此创建一个元数据（1.一开始set的时候没有元数据 2.get的时候过期了，创建一个新的元数据）
	if !exist {
		meta = &metaData{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		//如果是list的话，head和tail作用于中间值并向外扩散
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil

}
