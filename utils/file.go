package utils

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"
)

// DirSize 获取一个目录所有文件的大小
func DirSize(dirPath string) (int64, error) {
	var size int64
	//第二个参数info：一个 fs.FileInfo 接口类型的值，包含有关当前文件或目录的信息，如大小、是否为目录等。
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		//看看当前是文件还是目录，如果是文件加入到size中
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// AvailableDiskSize 获取磁盘剩余可用空间大小(windows版)
func AvailableDiskSize() (uint64, error) {
	var (
		modKernel32            = syscall.NewLazyDLL("kernel32.dll")
		procGetDiskFreeSpaceEx = modKernel32.NewProc("GetDiskFreeSpaceExW")
	)
	wd, _ := os.Getwd()
	directoryPath := filepath.VolumeName(wd)
	lpDirectoryName, err := syscall.UTF16PtrFromString(directoryPath)
	if err != nil {
		return 0, err
	}

	var freeBytesAvailableToCaller, totalNumberOfBytes, totalNumberOfFreeBytes int64
	ret, _, err := procGetDiskFreeSpaceEx.Call(
		uintptr(unsafe.Pointer(lpDirectoryName)),
		uintptr(unsafe.Pointer(&freeBytesAvailableToCaller)),
		uintptr(unsafe.Pointer(&totalNumberOfBytes)),
		uintptr(unsafe.Pointer(&totalNumberOfFreeBytes)),
	)
	if ret == 0 {
		return 0, err
	}

	return uint64(freeBytesAvailableToCaller), nil
}

// CopyFile 拷贝数据目录的方法
func CopyFile(src, dest string, exclude []string) error {
	//目标不存在的话就创建一个
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.Mkdir(dest, os.ModePerm); err != nil {
			return err
		}
	}

	//遍历源目录
	// tmp/a/11.data ----> /11.data
	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		//将前面的路径替换为空的就变为文件名了
		filename := strings.Replace(path, src, "", 1)
		if filename == "" {
			return nil
		}
		for _, e := range exclude {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}
		//如果是目录的话，创建一个
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dest, filename), info.Mode())
		}
		//读取文件并写到dest目录下
		data, err := os.ReadFile(filepath.Join(src, filename))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dest, filename), data, info.Mode())
	})
}
