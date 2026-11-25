package service

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"activity/config"
	"activity/model"
	"activity/mq"
	"activity/redisclient"
	"activity/repository"
)

// StartReconcileLoop 启动定时对账：Redis有报名记录但MySQL没有的，补发MQ消息落库
func StartReconcileLoop() {
	interval := time.Duration(config.Conf.Reconcile.IntervalSecond) * time.Second
	if interval <= 0 {
		interval = time.Minute
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			if err := Reconcile(context.Background()); err != nil {
				log.Printf("对账失败: %v\n", err)
			}
			<-ticker.C
		}
	}()
}

// Reconcile 对全部活动执行一次对账
func Reconcile(ctx context.Context) error {
	if depth, err := mq.DeadLetterDepth(); err == nil && depth > 0 {
		log.Printf("警告: 死信队列积压 %d 条消息，需要人工重放\n", depth)
	}
	var activities []model.Activity
	if err := repository.DB.Find(&activities).Error; err != nil {
		return err
	}
	for i := range activities {
		if err := reconcileActivity(ctx, &activities[i]); err != nil {
			log.Printf("对账活动%d失败: %v\n", activities[i].ID, err)
		}
	}
	return nil
}

func reconcileActivity(ctx context.Context, act *model.Activity) error {
	quotaKey := fmt.Sprintf("activity:quota:%d", act.ID)
	exists, err := redisclient.Rdb.Exists(ctx, quotaKey).Result()
	if err != nil {
		return err
	}
	if exists == 0 {
		return nil // 名额未初始化（或已结束清理），跳过
	}
	// 业务队列有积压=存在在途消息，DB滞后属正常，跳过避免重复补发
	if depth, err := mq.QueueDepth(); err == nil && depth > 0 {
		return nil
	}
	signKey := fmt.Sprintf("activity:sign:%d", act.ID)

	// 快路径：DB记录数已覆盖Redis集合大小，说明已全部同步
	var dbCount int64
	if err := repository.DB.Model(&model.ActivitySignUp{}).Where("activity_id = ?", act.ID).Count(&dbCount).Error; err != nil {
		return err
	}
	scard, err := redisclient.Rdb.SCard(ctx, signKey).Result()
	if err != nil {
		return err
	}
	if dbCount >= scard {
		return nil
	}

	// 慢路径：算差集，对缺失记录补发消息
	redisUsers, err := scanSignSet(ctx, signKey)
	if err != nil {
		return err
	}
	var dbUsers []int64
	if err := repository.DB.Model(&model.ActivitySignUp{}).Where("activity_id = ?", act.ID).Pluck("user_id", &dbUsers).Error; err != nil {
		return err
	}
	dbSet := make(map[int64]struct{}, len(dbUsers))
	for _, uid := range dbUsers {
		dbSet[uid] = struct{}{}
	}
	for _, uid := range redisUsers {
		if _, ok := dbSet[uid]; ok {
			continue
		}
		msg := &mq.SignUpMsg{ActivityId: int64(act.ID), StudentId: uid}
		if err := mq.Publish(ctx, msg); err != nil {
			log.Printf("对账补发消息失败, activityId:%d userId:%d err:%v\n", act.ID, uid, err)
		} else {
			log.Printf("对账补发消息, activityId:%d userId:%d\n", act.ID, uid)
		}
	}
	return nil
}

// scanSignSet 分批扫描Redis报名集合，避免大集合阻塞
func scanSignSet(ctx context.Context, key string) ([]int64, error) {
	var users []int64
	var cursor uint64
	for {
		vals, next, err := redisclient.Rdb.SScan(ctx, key, cursor, "*", 1000).Result()
		if err != nil {
			return nil, err
		}
		for _, v := range vals {
			if uid, err := strconv.ParseInt(v, 10, 64); err == nil {
				users = append(users, uid)
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return users, nil
}
