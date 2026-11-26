package handler

import (
	"activity/mq"
	"activity/redisclient"
	"activity/repository"
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// Healthz 存活探针：进程活着即200
func Healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// Readyz 就绪探针：DB/Redis/MQ 全部可用才200，供负载均衡摘流
func Readyz(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	checks := map[string]string{}
	ok := true
	if sqlDB, err := repository.DB.DB(); err == nil {
		if err := sqlDB.PingContext(ctx); err != nil {
			checks["mysql"] = err.Error()
			ok = false
		}
	} else {
		checks["mysql"] = err.Error()
		ok = false
	}
	if err := redisclient.Rdb.Ping(ctx).Err(); err != nil {
		checks["redis"] = err.Error()
		ok = false
	}
	if !mq.IsConnected() {
		checks["rabbitmq"] = "disconnected"
		ok = false
	}
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "checks": checks})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}
