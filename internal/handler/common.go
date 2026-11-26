package handler

import (
	"activity/internal/logger"
	"activity/internal/service"
	"activity/pkg"
	"errors"

	"github.com/gin-gonic/gin"
)

// respondErr 业务错误原样返回给用户（400）；系统错误返回通用消息（500）
// 并记日志，避免内部错误细节泄露给调用方
func respondErr(c *gin.Context, err error) {
	var biz *service.BizError
	if errors.As(err, &biz) {
		pkg.Fail(c, 400, biz.Msg)
		return
	}
	logger.L.Errorw("系统错误", "method", c.Request.Method, "path", c.FullPath(), "err", err)
	pkg.Fail(c, 500, "服务繁忙，请稍后重试")
}
