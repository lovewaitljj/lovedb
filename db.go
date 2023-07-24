package lovedb

import (
	"errors"
	"io"
	"lovedb/data"
	"lovedb/index"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

//面向用户的操作接口

// DB bitcask存储引擎实例
type DB struct {
	options    Options
	mu         *sync.RWMutex
	fileIds    []int                     //文件们的id，由于加载文件的时候得到过，所以放入结构体里，用于加载索引的时候使用
	activeFile *data.DataFile            //当前活跃数据文件,可以用于写入
	olderFiles map[uint32]*data.DataFile //旧的数据文件，只能用于读
	index      index.Indexer             //内存索引
}

// Open 数据库启动时打开bitcask引擎实例
func Open(options Options) (*DB, error) {
	//对用户传过来的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	//对用户传过来的目录进行校验，如果不存在则创建目录
	//需要注意的是，checkOptions函数是校验用户的传递参数，而Stat函数是真正检查是否存在目录并返回信息
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) { //判断返回的错误是否表示目录不存在。
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//初始化db结构体
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		//根据用户传过来的类型而去创建相应的内存数据结构
		index: index.NewIndexer(options.IndexType),
	}

	//加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	//从数据文件中加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
}

// Put 写入key/value数据，key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	//判断key是否有效
	if len(key) == 0 {
		//一般通过判断别的方式而产生错误就需要自定义一些错误常量
		return ErrKeyIsEmpty
	}

	//构造LogRecord结构体
	log_record := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	//追加写入到当前活跃文件中
	pos, err := db.appendLogRecord(log_record)
	if err != nil {
		return err
	}

	//拿到内存索引以后更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// Delete 删除接口
func (db *DB) Delete(key []byte) error {
	//判断key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	//判断key是否存在，不存在则直接返回
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		//本来就要删除key，既然key本来就没有，那就省事了，这种情况也就不算是错误。
		return nil
	}

	//构造LogRecord结构体,删除的话不需要知道value值，删除这个key对应的记录就可以
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	//去索引内存中删除
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	logRecordPos := db.index.Get(key)
	//如果key不在内存索引中，则说明该key不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	//根据索引提供的id去找对应的文件
	var file *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		file = db.activeFile
	} else {
		file = db.olderFiles[logRecordPos.Fid]
	}

	//如果找不到文件则抛出相应错误
	if file == nil {
		return nil, ErrDataFileNotFound
	}
	//根据偏移量去读取响应数据并返回
	LogRecord, _, err := file.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	//需要判断这个LogRecord的类型是否是删除的记录
	if LogRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return LogRecord.Value, nil
}

// 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	//判断当前活跃数据文件是否存在，因为数据库在没有数据写入的时候是没有文件生成的
	//如果为空则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//将logRecord转化为字节数组写入到文件中
	encRecord, size := data.EncodeLogRecord(logRecord)

	//如果写入的数据已经达到了活跃文件的阈值，则关闭活跃文件并打开新的文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		//因为要关闭，所以要先将当前的活跃文件进行持久化
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		//当前活跃文件转化为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		//设置新的活跃文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//正式写入
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	//根据用户配置项决定是否持久化
	if db.options.SyncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	//构造返回的内存索引信息
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
	}
	return pos, nil
}

// 设置当前活跃文件
// 在并发访问该db实例并修改共同资源时，需要上互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1 //当前活跃文件已过期，设置它的下一个为活跃文件
	}
	//在配置文件给定的目录下，打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	//读取目录，并返回一个文件切片
	dirs, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	//遍历目录，找到所有以.data结尾的文件
	var fileIds []int
	for _, dir := range dirs {
		//如果是.data结尾的文件
		if strings.HasSuffix(dir.Name(), data.DataFileNameSuffix) {
			//00001.data ->  00001 -> 1
			splitName := strings.Split(dir.Name(), ".")
			fileId, err := strconv.Atoi(splitName[0])
			if err != nil {
				//文件已损坏
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}
	//拿到所有文件的切片，进行排序，因为最大的是我们要的活跃文件
	sort.Ints(fileIds)
	//排序之后可以赋值给结构体用于内存取所有文件
	db.fileIds = fileIds

	//遍历每个文件id并对文件进行打开操作
	for i, fileId := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fileId))
		if err != nil {
			return err
		}
		//打开每个文件并加入到旧文件map或者活跃文件当中
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		}
		//旧文件放到map里
		db.olderFiles[uint32(fileId)] = dataFile
	}
	return nil
}

//fixme 注释加上

// 从数据文件中加载索引
// 遍历文件中所有记录，并更新到索引上去
func (db *DB) loadIndexFromDataFiles() error {
	//如果是空文件则直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	//遍历所有文件取出所有文件当中的内容
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		//拿到当前文件
		var dataFile *data.DataFile
		if i == len(db.fileIds)-1 {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		//读取当前文件的所有的内容
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			//构建内存索引并保存
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}
			var ok bool
			if logRecord.Type == data.LogRecordDeleted {
				ok = db.index.Delete(logRecord.Key)
			} else {
				ok = db.index.Put(logRecord.Key, logRecordPos)
			}
			if !ok {
				return ErrIndexUpdateFailed
			}
			offset += size
		}
		//fixme 如果是当前活跃文件，更新这个文件的 WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	return nil
}

// 校验用户配置文件合法性
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}

	return nil
}
