package handler

import (
	"activity/internal/service"
	"activity/pkg"

	"github.com/gin-gonic/gin"
)

type RegisterReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type LoginReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func Register(c *gin.Context) {
	var req RegisterReq
	if err := c.ShouldBindJSON(&req); err != nil {
		pkg.Fail(c, 400, "参数错误")
		return
	}
	err := service.Register(req.Username, req.Password)
	if err != nil {
		pkg.Fail(c, 400, err.Error())
		return
	}
	pkg.Success(c, nil)
}

func Login(c *gin.Context) {
	var req LoginReq
	if err := c.ShouldBindJSON(&req); err != nil {
		pkg.Fail(c, 400, "参数错误")
		return
	}
	token, err := service.Login(req.Username, req.Password)
	if err != nil {
		pkg.Fail(c, 400, err.Error())
		return
	}
	pkg.Success(c, gin.H{"token": token})
}
