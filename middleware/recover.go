package middleware

import (
	"activity/pkg"

	"github.com/gin-gonic/gin"
)

func Recover() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				pkg.Fail(c, 500, "服务内部异常")
				c.Abort()
			}
		}()
		c.Next()
	}
}
