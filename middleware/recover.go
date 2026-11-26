package middleware

import (
	"activity/internal/logger"
	"activity/pkg"
	"runtime/debug"

	"github.com/gin-gonic/gin"
)

func Recover() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				logger.L.Errorw("panic", "err", err, "stack", string(debug.Stack()))
				pkg.Fail(c, 500, "服务内部异常")
				c.Abort()
			}
		}()
		c.Next()
	}
}
