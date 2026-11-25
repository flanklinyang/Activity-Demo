package middleware

import (
	"activity/pkg"
	"activity/redisclient"
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"time"
)

func Limiter() gin.HandlerFunc {
	return func(c *gin.Context) {
		uidRaw, exists := c.Get("user_id")
		if !exists {
			c.Next()
			return
		}
		uid := uidRaw.(int64)
		key := fmt.Sprintf("limiter:user:%d", uid)
		count, err := redisclient.Rdb.Incr(context.Background(), key).Result()
		if err != nil {
			pkg.Fail(c, 500, "限流服务异常")
			c.Abort()
			return
		}
		if count == 1 {
			redisclient.Rdb.Expire(context.Background(), key, time.Second)
		}
		if count > 20 {
			pkg.Fail(c, 429, "请求过于频繁")
			c.Abort()
			return
		}
		c.Next()
	}
}
