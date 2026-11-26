package middleware

import (
	"activity/config"
	"activity/pkg"
	"fmt"

	"github.com/gin-gonic/gin"
)

// IPLimiter 按客户端IP的固定窗口限流（默认10 req/s，可用 ratelimit.ip_per_sec 配置），
// 用于注册/登录等公开接口，防撞库与密码爆破。
// ip_per_sec 设为 -1 关闭限流（压测核心链路时使用）。
func IPLimiter() gin.HandlerFunc {
	limit := config.Conf.Ratelimit.IPPerSec
	if limit < 0 {
		return func(c *gin.Context) { c.Next() }
	}
	if limit == 0 {
		limit = 10
	}
	return func(c *gin.Context) {
		ip := c.ClientIP()
		ok, err := limiterAllow(c.Request.Context(), fmt.Sprintf("limiter:ip:%s", ip), limit)
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
