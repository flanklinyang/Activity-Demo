package handler

import (
	"activity/internal/service"
	"activity/pkg"
	"strconv"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
)

type CreateActivityReq struct {
	Title      string `json:"title"`
	Content    string `json:"content"`
	TotalQuota int64  `json:"total_quota"`
	StartTime  int64  `json:"start_time"` // Unix秒；报名开始时间（可选）
	EndTime    int64  `json:"end_time"`   // Unix秒；活动结束时间（可选，>0时对账在结束后清理运行期key）
}

func CreateActivity(c *gin.Context) {
	var req CreateActivityReq
	if err := c.ShouldBindJSON(&req); err != nil {
		pkg.Fail(c, 400, "参数错误")
		return
	}
	if n := utf8.RuneCountInString(req.Title); n < 1 || n > 128 {
		pkg.Fail(c, 400, "活动标题长度需为1-128个字符")
		return
	}
	if n := utf8.RuneCountInString(req.Content); n > 5000 {
		pkg.Fail(c, 400, "活动描述不能超过5000个字符")
		return
	}
	if req.TotalQuota < 1 || req.TotalQuota > 1000000 {
		pkg.Fail(c, 400, "活动名额需在1-1000000之间")
		return
	}
	if req.StartTime < 0 || req.EndTime < 0 {
		pkg.Fail(c, 400, "时间参数无效")
		return
	}
	if req.StartTime > 0 && req.EndTime > 0 && req.StartTime > req.EndTime {
		pkg.Fail(c, 400, "开始时间不能晚于结束时间")
		return
	}
	act, err := service.CreateActivity(c, req.Title, req.Content, req.TotalQuota, req.StartTime, req.EndTime)
	if err != nil {
		respondErr(c, err)
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
		respondErr(c, err)
		return
	}
	pkg.Success(c, "报名请求提交成功")
}
