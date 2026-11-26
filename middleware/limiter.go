package middleware

import (
	"activity/config"
	"activity/pkg"
	"activity/redisclient"
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

// limiterScript 固定窗口限流：INCR+EXPIRE 原子执行，避免两步非原子导致
// EXPIRE 丢失后 key 永不过期、用户被永久限流
var limiterScript = redis.NewScript(`
local c = redis.call("INCR", KEYS[1])
if c == 1 then
    redis.call("EXPIRE", KEYS[1], ARGV[1])
end
return c
`)

func limiterAllow(ctx context.Context, key string, limit int) (bool, error) {
	count, err := limiterScript.Run(ctx, redisclient.Rdb, []string{key}, 1).Int64()
	if err != nil {
		return false, err
	}
	return count <= int64(limit), nil
}

func userLimit() int {
	if config.Conf.Ratelimit.UserPerSec > 0 {
		return config.Conf.Ratelimit.UserPerSec
	}
	return 20
}

// Limiter 每用户固定窗口限流（默认20 req/s，可用 ratelimit.user_per_sec 配置）
func Limiter() gin.HandlerFunc {
	return func(c *gin.Context) {
		uidRaw, exists := c.Get("user_id")
		if !exists {
			c.Next()
			return
		}
		uid := uidRaw.(int64)
		ok, err := limiterAllow(c.Request.Context(), fmt.Sprintf("limiter:user:%d", uid), userLimit())
		if err != nil {
			pkg.Fail(c, 500, "限流服务异常")
			c.Abort()
			return
		}
		if !ok {
			pkg.Fail(c, 429, "请求过于频繁")
			c.Abort()
			return
		}
		c.Next()
	}
}
