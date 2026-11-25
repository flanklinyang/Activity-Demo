package pkg

import "github.com/gin-gonic/gin"

type Resp struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

func Success(c *gin.Context, data interface{}) {
	c.JSON(200, Resp{
		Code: 0,
		Msg:  "ok",
		Data: data,
	})
}

func Fail(c *gin.Context, code int, msg string) {
	c.JSON(200, Resp{
		Code: code,
		Msg:  msg,
		Data: nil,
	})
}
