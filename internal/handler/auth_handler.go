package handler

import (
	"activity/internal/service"
	"activity/pkg"
	"unicode/utf8"

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
	if n := utf8.RuneCountInString(req.Username); n < 3 || n > 50 {
		pkg.Fail(c, 400, "用户名长度需为3-50个字符")
		return
	}
	// bcrypt 只取前72字节，超长必须拒绝避免截断歧义
	if n := len(req.Password); n < 6 || n > 72 {
		pkg.Fail(c, 400, "密码长度需为6-72个字符")
		return
	}
	if err := service.Register(req.Username, req.Password); err != nil {
		respondErr(c, err)
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
	if req.Username == "" || req.Password == "" {
		pkg.Fail(c, 400, "参数错误")
		return
	}
	token, err := service.Login(req.Username, req.Password)
	if err != nil {
		respondErr(c, err)
		return
	}
	pkg.Success(c, gin.H{"token": token})
}
