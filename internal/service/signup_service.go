package service

import (
	"context"
	"errors"
	"fmt"

	"activity/internal/logger"
	"activity/internal/metrics"
	"activity/model"
	"activity/mq"
	"activity/redisclient"
	"activity/repository"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// 活动报名：Redis Lua扣库存（AOF fsync后响应=事实源），发送RabbitMQ消息异步落库
func SignUpActivity(ctx context.Context, activityId, userId int64) error {
	// 1. Lua脚本扣减名额 + 判断是否已报名
	luaScript := redis.NewScript(`
		local quotaKey = KEYS[1]
		local userSignKey = KEYS[2]
		local activityId = ARGV[1]
		local userId = ARGV[2]
		-- 判断是否已经报名
		if redis.call("SISMEMBER", userSignKey, userId) == 1 then
			return 1
		end
		local quota = redis.call("GET", quotaKey)
		if not quota then
			return 3 -- 活动不存在（名额key未初始化）
		end
		local remain = tonumber(quota)
		if remain == nil or remain <= 0 then
			return 2
		end
		redis.call("DECR", quotaKey)
		redis.call("SADD", userSignKey, userId)
		return 0
	`)
	quotaKey := fmt.Sprintf("activity:quota:%d", activityId)
	signSetKey := fmt.Sprintf("activity:sign:%d", activityId)
	keys := []string{quotaKey, signSetKey}
	resp, err := luaScript.Run(ctx, redisclient.Rdb, keys, activityId, userId).Int64()
	if err != nil {
		metrics.SignupTotal.WithLabelValues("error").Inc()
		return err
	}
	switch resp {
	case 1:
		metrics.SignupTotal.WithLabelValues("already_signed").Inc()
		return ErrAlreadySignedUp
	case 2:
		metrics.SignupTotal.WithLabelValues("quota_exhausted").Inc()
		return ErrQuotaExhausted
	case 3:
		metrics.SignupTotal.WithLabelValues("activity_missing").Inc()
		return ErrActivityMissing
	}
	metrics.SignupTotal.WithLabelValues("ok").Inc()

	// 2. 发送消息给RabbitMQ，异步写入数据库
	// Redis已持久化，报名事实成立；发布失败只告警，由定时对账兜底补发
	msg := &mq.SignUpMsg{
		ActivityId: activityId,
		StudentId:  userId,
	}
	if err := mq.Publish(ctx, msg); err != nil {
		logger.L.Errorw("报名消息发布失败，等待对账补发",
			"activityId", activityId, "userId", userId, "err", err)
	}
	return nil
}

// MQ消费回调：幂等写入报名记录，联合唯一索引兜底防重复
// 已报名人数不落库统计，由Redis报名集合的SCARD实时获取
func ConsumeSignUpMsg(ctx context.Context, msg *mq.SignUpMsg) error {
	signRecord := model.ActivitySignUp{
		ActivityId: msg.ActivityId,
		UserId:     msg.StudentId,
	}
	err := repository.DB.Create(&signRecord).Error
	if err == nil {
		return nil
	}
	// 唯一索引冲突=该报名已落库，幂等视为成功
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return nil
	}
	return err
}

// 创建活动，初始化Redis名额缓存
func CreateActivity(ctx context.Context, title, content string, total int64) (*model.Activity, error) {
	act := model.Activity{
		Title:      title,
		Content:    content,
		TotalQuota: total,
	}
	if err := repository.DB.Create(&act).Error; err != nil {
		return nil, err
	}
	key := fmt.Sprintf("activity:quota:%d", act.ID)
	if err := redisclient.Rdb.Set(ctx, key, total, 0).Err(); err != nil {
		// Redis写入失败：删除刚写入的DB记录，保证两边一致
		if delErr := repository.DB.Delete(&act).Error; delErr != nil {
			logger.L.Errorw("Redis写入失败且补偿删除DB失败",
				"activityId", act.ID, "redisErr", err, "delErr", delErr)
		}
		return nil, err
	}
	return &act, nil
}
