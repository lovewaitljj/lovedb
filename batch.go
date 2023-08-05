package lovedb

import (
	"encoding/binary"
	"lovedb/data"
	"sync"
	"sync/atomic"
)

var txnFinKey = []byte("txn-fin")

const nonTxSeqNo uint64 = 0

// WriteBatch 原子批量写数据，保证原子性
type WriteBatch struct {
	options       WriteBatchOption
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord //暂存用户写入的数据 key ->  logRecord
}

//初始化WriteBatch

func (db *DB) NewWriteBatch(opts WriteBatchOption) *WriteBatch {
	return &WriteBatch{
		options:       opts,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 向批次内写入数据，等到合适时机一起提交事务
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	//暂存logRecord
	logRecord := &data.LogRecord{Key: key, Value: value} //不指定默认为normal，因为值为0
	wb.pendingWrites[string(key)] = logRecord
	return nil
}
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	//数据不存在则直接返回
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		//批处理的put还未提交就被删除了
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		//删除已经被删除的，直接返回就可以
		return nil
	}

	//暂存logRecord
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pendingWrites[string(key)] = logRecord
	return nil

}

// Commit 提交事务：将暂存数据全部写到数据文件，并更新内存索引
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()
	//提交才会获取当前最新的事务序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	pos := make(map[string]*data.LogRecordPos)
	//开始写数据到数据文件当中
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   LogRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		//写入磁盘但先不更新内存索引，所以先存到pos中   key -> logRecordPos
		pos[string(record.Key)] = logRecordPos
	}
	//一条表示fin的记录，说明批量数据正确，若没有可能中间发生错误，全部丢弃
	finLogRecord := &data.LogRecord{
		Key:  LogRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordFinished,
	}
	_, err := wb.db.appendLogRecord(finLogRecord)
	if err != nil {
		return err
	}

	//根据配置判断是否持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		err := wb.db.activeFile.Sync()
		if err != nil {
			return err
		}
	}
	//更新内存索引即可
	for _, record := range wb.pendingWrites {
		position := pos[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			wb.db.index.Put(record.Key, position)
		}
		if record.Type == data.LogRecordDeleted {
			wb.db.index.Delete(record.Key)
		}
	}
	//重置暂存空间
	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// LogRecordKeyWithSeq 将 seqNo 与 key 组合成一个新的字节数组，并返回该组合后的键。
func LogRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	//使用变长编码存储seqNo
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// ParseLogRecordKey 解析key，拿到真正的key和事务id
func ParseLogRecordKey(key []byte) ([]byte, uint64) {
	seq, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seq
}
