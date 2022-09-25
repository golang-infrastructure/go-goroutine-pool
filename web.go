package go_goroutine_pool

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

// TODO 自带一个简单的WEB监控
// TODO pool上也提供API能够获取得到一些统计信息

func startApiServer(address string) error {

	r := gin.Default()

	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	// json
	// text

	// html
	// DAG的可视化

	// TODO 这个统计指标能够接入到普罗米修斯的监控系统中以便实现更好的可视化

	return r.Run(address)
}
