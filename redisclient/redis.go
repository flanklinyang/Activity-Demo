package redisclient

import (
	"activity/config"
	"context"

	"github.com/redis/go-redis/v9"
)

var Rdb *redis.Client

func Init() error {
	Rdb = redis.NewClient(&redis.Options{
		Addr:     config.Conf.Redis.Addr,
		Password: config.Conf.Redis.Password,
		DB:       config.Conf.Redis.DB,
	})
	return Rdb.Ping(context.Background()).Err()
}
