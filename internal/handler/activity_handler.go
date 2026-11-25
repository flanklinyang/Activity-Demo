package handler

import (
	"activity/internal/service"
	"activity/pkg"
	"strconv"

	"github.com/gin-gonic/gin"
)

type CreateActivityReq struct {
	Title      string `json:"title"`
	Content    string `json:"content"`
	TotalQuota int64  `json:"total_quota"`
}

func CreateActivity(c *gin.Context) {
	var req CreateActivityReq
	if err := c.ShouldBindJSON(&req); err != nil {
		pkg.Fail(c, 400, "参数错误")
		return
	}
	act, err := service.CreateActivity(c, req.Title, req.Content, req.TotalQuota)
	if err != nil {
		pkg.Fail(c, 400, err.Error())
		return
	}
	pkg.Success(c, act)
}

func SignUp(c *gin.Context) {
	activityId, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || activityId <= 0 {
		pkg.Fail(c, 400, "活动ID无效")
		return
	}
	userId, _ := c.Get("user_id")
	err = service.SignUpActivity(c, activityId, userId.(int64))
	if err != nil {
		pkg.Fail(c, 400, err.Error())
		return
	}
	pkg.Success(c, "报名请求提交成功")
}
