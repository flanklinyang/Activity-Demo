package pkg

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type Resp struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

func Success(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, Resp{
		Code: 0,
		Msg:  "ok",
		Data: data,
	})
}

// Fail body结构不变（code/msg/data），HTTP状态码与业务code对应，
// 便于网关/监控按状态码统计
func Fail(c *gin.Context, code int, msg string) {
	c.JSON(httpStatus(code), Resp{
		Code: code,
		Msg:  msg,
		Data: nil,
	})
}

func httpStatus(code int) int {
	switch {
	case code == http.StatusUnauthorized:
		return http.StatusUnauthorized
	case code == http.StatusTooManyRequests:
		return http.StatusTooManyRequests
	case code >= 500:
		return http.StatusInternalServerError
	case code >= 400:
		return http.StatusBadRequest
	default:
		return http.StatusOK
	}
}
