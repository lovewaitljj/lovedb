package redis

import (
	"errors"
	"math"
)

// Del 删除元数据就可以，如果查到元数据不存在就返回
func (rds *RedisDataStructure) Del(key []byte) error {
	return rds.Del(key)
}

func (rds *RedisDataStructure) Type(key []byte) (redisDataType, error) {
	val, err := rds.db.Get(key)
	if err != nil {
		return math.MaxUint8, err
	}
	if len(val) == 0 {
		return 0, errors.New("value is null")
	}
	//第一个字节就是type
	return val[0], nil
}
