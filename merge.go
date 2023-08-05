package lovedb

import (
	"io"
	"lovedb/data"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

// Merge 清理无效数据，生成hint文件
func (db *DB) Merge() error {
	//判断活跃文件为空，代表目录就是空的
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()

	//如果merge正在进行，则直接返回
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsProgress
	}

	db.isMerging = true

	defer func() {
		db.isMerging = false
	}()

	//持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	//将当前活跃转化为旧的,打开一个新的活跃文件
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}

	//取出所有需要merge的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}

	db.mu.Unlock()

	//将merge文件从小到大排序，依次merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergePath()

	//如果本身有这个目录，说明之前merge过，要先删除并创建
	if _, err := os.Stat(mergePath); err != nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	//新建merge目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	//在该目录打开一个临时bitcask示例用于merge
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	//merge发生错误之前的就不要sync，所以sync不需要一直有，最后来一次就可以
	mergeOptions.SyncWrite = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	//打开一个hint文件处理索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	//遍历处理每个数据文件
	for _, file := range mergeFiles {

		for {
			var offset int64 = 0
			logRecord, size, err := file.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			realKey, _ := ParseLogRecordKey(logRecord.Key)
			//拿到记录和内存索引中进行对比
			logRecordPos := db.index.Get(realKey)
			if logRecordPos != nil && logRecordPos.Fid == file.FileId && logRecordPos.Offset == offset {
				//往merge里面写,清除事务标记
				logRecord.Key = LogRecordKeyWithSeq(realKey, nonTxSeqNo)
				mergeRecordPos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				//将merge的pos写入到Hint文件中
				if err := hintFile.WriteHintRecord(realKey, mergeRecordPos); err != nil {
					return err
				}
			}
			offset += size
		}
	}

	//保证持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}
	//打开标示着merge完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	//记录当前db的活跃文件，这是第一个没有被merge的文件
	nonMergeId := db.activeFile.FileId
	//往这个文件里写一条数据记录相关信息
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeId))),
	}
	//写入到标识merge完成的文件中
	record, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(record); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}
	return nil
}

func (db *DB) getMergePath() string {
	// Dir返回路径除去最后一个路径元素的部分，即该路径最后一个元素所在的目录
	//D:/git_space/lovedb/tmp  ---->    D:/git_space/lovedb
	dir := path.Dir(path.Clean(db.options.DirPath))

	//D:/git_space/lovedb/tmp  ---->   tmp
	base := path.Base(db.options.DirPath)

	//D:/git_space/lovedb/tmp-merge
	return filepath.Join(dir, base+mergeDirName)
}

// 数据库启动时加载merge目录
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	//merge目录不存在直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	//查找是否有merge-finished的文件，判断是否处理完
	var mergeFinished bool
	var mergeFileNames []string
	for _, file := range dirEntries {
		if file.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		if file.Name() == data.SeqNoFileName {
			continue
		}
		mergeFileNames = append(mergeFileNames, file.Name())
	}

	//没有完成merge就直接返回
	if !mergeFinished {
		return nil
	}

	//用merge下的文件替代
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}
	//将旧的目录文件删掉,比nonMergeFileId更小的所有文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		//fixme
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}
	//将merge下的文件移动过去,包括hint文件
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.options.DirPath, fileName)
		err := os.Rename(srcPath, destPath)
		if err != nil {
			return err
		}
	}
	return nil
}

// 获取最近没有被merge的文件的id
func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

// 从hint去加载我们的索引
func (db *DB) loadIndexFromHint() error {
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return err
	}
	//打开hint索引文件
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}
	//读取hint文件，并更新到内存
	var offset int64 = 0
	for {
		logRecord, n, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		//解码拿到实际的索引信息
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += n
	}
	return nil
}
