package service

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"activity/config"
	"activity/internal/logger"
	"activity/internal/metrics"
	"activity/model"
	"activity/mq"
	"activity/redisclient"
	"activity/repository"
)

// StartReconcileLoop 启动定时对账：Redis有报名记录但MySQL没有的，补发MQ消息落库
func StartReconcileLoop(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(reconcileInterval())
		defer ticker.Stop()
		for {
			if err := Reconcile(ctx); err != nil {
				logger.L.Errorw("对账失败", "err", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func reconcileInterval() time.Duration {
	interval := time.Duration(config.Conf.Reconcile.IntervalSecond) * time.Second
	if interval <= 0 {
		interval = time.Minute
	}
	return interval
}

// Reconcile 对全部活动执行一次对账
func Reconcile(ctx context.Context) error {
	start := time.Now()
	defer func() {
		metrics.ReconcileDuration.Observe(time.Since(start).Seconds())
	}()
	var activities []model.Activity
	if err := repository.DB.Find(&activities).Error; err != nil {
		return err
	}
	// 近期有消费失败丢弃=DB写入不健康，跳过本轮补发，避免对账补发与消费重试互相放大
	if dropped, recent := mq.DroppedRecently(2 * reconcileInterval()); recent {
		logger.L.Warnw("近期有消息消费失败被丢弃，跳过本轮补发，等待DB恢复", "dropped", dropped)
	} else {
		for i := range activities {
			if err := reconcileActivity(ctx, &activities[i]); err != nil {
				logger.L.Errorw("对账活动失败", "activityId", activities[i].ID, "err", err)
			}
		}
	}
	// 孤儿key清理不补发MQ、无放大风险，不受丢弃门控影响
	if err := reconcileOrphans(ctx); err != nil {
		logger.L.Errorw("孤儿key清理失败", "err", err)
	}
	return nil
}

func reconcileActivity(ctx context.Context, act *model.Activity) error {
	quotaKey := fmt.Sprintf("activity:quota:%d", act.ID)
	signKey := fmt.Sprintf("activity:sign:%d", act.ID)
	// 已结束活动：key缺失视为已清理不复活；DB同步完成后清理运行期key
	ended := act.EndTime > 0 && time.Now().Unix() > act.EndTime

	exists, err := redisclient.Rdb.Exists(ctx, quotaKey).Result()
	if err != nil {
		return err
	}
	if exists == 0 && !ended {
		// 自愈：名额key缺失（创建时Redis写入失败或误删）时从DB恢复。
		// 安全前提：key缺失期间报名Lua返回「活动不可用」，不会有新报名；
		// 减去已报名集合大小防手动删key后名额被重置。
		scard, err := redisclient.Rdb.SCard(ctx, signKey).Result()
		if err != nil {
			return err
		}
		repairQuota := act.TotalQuota - scard
		if repairQuota < 0 {
			repairQuota = 0
		}
		if err := redisclient.Rdb.Set(ctx, quotaKey, repairQuota, 0).Err(); err != nil {
			return err
		}
		metrics.ReconcileActionsTotal.WithLabelValues("repair").Inc()
		logger.L.Infow("对账修复缺失名额key", "activityId", act.ID, "quota", repairQuota)
	}
	// 业务队列有积压=存在在途消息，DB滞后属正常，跳过避免重复补发
	if depth, err := mq.QueueDepth(); err == nil && depth > 0 {
		return nil
	}

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
		if ended {
			// 已结束且DB已同步：清理运行期key，报名历史以MySQL为准。
			// 幂等：下轮DEL返回0，不再打日志。key删除后报名Lua对已结束
			// 活动返回「活动不存在」，天然fail closed。
			if del, err := redisclient.Rdb.Del(ctx, quotaKey, signKey).Result(); err != nil {
				return err
			} else if del > 0 {
				metrics.ReconcileActionsTotal.WithLabelValues("expire").Inc()
				logger.L.Infow("对账清理已结束活动key", "activityId", act.ID)
			}
		}
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
			logger.L.Errorw("对账补发消息失败", "activityId", act.ID, "userId", uid, "err", err)
		} else {
			metrics.ReconcileActionsTotal.WithLabelValues("resend").Inc()
			logger.L.Infow("对账补发消息", "activityId", act.ID, "userId", uid)
		}
	}
	return nil
}

// reconcileOrphans 清理孤儿key：Redis有名额key但DB中活动不存在（含软删除）。
// 来源：手动造key或历史遗留（创建链路为MySQL单写+缓存预热，正常流程不产生孤儿）。
// 同时硬删孤儿活动的报名脏记录（孤儿key可被猜到ID报名成功）。
func reconcileOrphans(ctx context.Context) error {
	var ids []int64
	if err := repository.DB.Model(&model.Activity{}).Pluck("id", &ids).Error; err != nil {
		return err // DB不可用则保守跳过清理
	}
	dbSet := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		dbSet[id] = struct{}{}
	}
	var orphans []int64
	iter := redisclient.Rdb.Scan(ctx, 0, "activity:quota:*", 1000).Iterator()
	for iter.Next(ctx) {
		id, err := strconv.ParseInt(strings.TrimPrefix(iter.Val(), "activity:quota:"), 10, 64)
		if err != nil {
			continue
		}
		if _, ok := dbSet[id]; !ok {
			orphans = append(orphans, id)
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	for _, id := range orphans {
		if err := redisclient.Rdb.Del(ctx,
			fmt.Sprintf("activity:quota:%d", id),
			fmt.Sprintf("activity:sign:%d", id)).Err(); err != nil {
			logger.L.Errorw("清理孤儿key失败", "activityId", id, "err", err)
			continue
		}
		if err := repository.DB.Unscoped().Where("activity_id = ?", id).Delete(&model.ActivitySignUp{}).Error; err != nil {
			logger.L.Errorw("清理孤儿报名记录失败", "activityId", id, "err", err)
		}
		metrics.ReconcileActionsTotal.WithLabelValues("clean").Inc()
		logger.L.Infow("对账清理孤儿key", "activityId", id)
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
