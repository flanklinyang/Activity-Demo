package redisclient

import (
	"activity/config"
	"context"

	"github.com/redis/go-redis/v9"
)

var Rdb *redis.Client

func Init() error {
	// 哨兵模式：客户端经哨兵解析当前主，主从切换时自动跟随新主（切换窗口内命令失败，fail closed）
	if config.Conf.Redis.Mode == "sentinel" {
		Rdb = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    config.Conf.Redis.MasterName,
			SentinelAddrs: config.Conf.Redis.SentinelAddrs,
			Password:      config.Conf.Redis.Password,
			DB:            config.Conf.Redis.DB,
		})
	} else {
		Rdb = redis.NewClient(&redis.Options{
			Addr:     config.Conf.Redis.Addr,
			Password: config.Conf.Redis.Password,
			DB:       config.Conf.Redis.DB,
		})
	}
	return Rdb.Ping(context.Background()).Err()
}
