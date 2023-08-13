package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"log"
	"lovedb"
	"net/http"
)

var db *lovedb.DB

func init() {
	var err error
	options := lovedb.DefaultOptions
	//dir := filepath.Join("D:\\git_space\\lovedb\\tmp")
	//options.DirPath = dir
	//fixme db为空
	db, _ = lovedb.Open(options)
	fmt.Println(db)
	if err != nil {
		panic(fmt.Sprintf("failed to open lovedb :%v", err))
	}
}

func main() {
	r := gin.Default()
	v1 := r.Group("/bitcask")
	{
		v1.POST("/put", HandlePut)
		v1.GET("/get", HandleGet)
		v1.DELETE("/delete", HandleDelete)
		v1.GET("/listkeys", HandleListKeys)
		v1.GET("/stat", HandleStat)
	}
	//启动web服务器
	err := r.Run(":8083")
	if err != nil {
		fmt.Println("Error:", err)
	}
}

// HandlePut fixme panic: CreateFile D:\git_space\lovedb\tmp\hint-index: The system cannot find the file specified.
func HandlePut(c *gin.Context) {
	var data map[string]string
	if err := c.ShouldBind(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	for key, value := range data {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to put kv in db"})
			log.Printf("failed to put kv in db: %v\n", err)
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"message": "Data stored successfully"})

}

func HandleGet(c *gin.Context) {
	key := c.Param("key")
	value, err := db.Get([]byte(key))
	if err != nil && err != lovedb.ErrKeyNotFound {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get kv in db"})
		log.Printf("failed to get kv in db: %v\n", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"value": string(value)})

}

func HandleDelete(c *gin.Context) {
	key := c.Param("key")
	err := db.Delete([]byte(key))
	if err != nil && err != lovedb.ErrKeyNotFound {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete kv in db"})
		log.Printf("failed to get kv in db: %v\n", err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"value": "ok"})

}

func HandleListKeys(c *gin.Context) {
	keys := db.ListKeys()
	var res []string
	for _, v := range keys {
		res = append(res, string(v))
	}
	c.JSON(http.StatusOK, gin.H{"keys": res})
}

func HandleStat(c *gin.Context) {
	Stat := db.Stat()
	c.JSON(http.StatusOK, gin.H{"stat": Stat})
}
