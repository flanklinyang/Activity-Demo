package config

import (
	"errors"
	"log"
	"strings"

	"github.com/spf13/viper"
)

// Conf 全局配置结构体
var Conf AppConfig

type AppConfig struct {
	Mysql struct {
		Dsn                   string `mapstructure:"dsn"`
		MaxOpenConns          int    `mapstructure:"max_open_conns"`
		MaxIdleConns          int    `mapstructure:"max_idle_conns"`
		ConnMaxLifetimeSecond int    `mapstructure:"conn_max_lifetime_second"`
	} `mapstructure:"mysql"`
	Redis struct {
		Mode          string   `mapstructure:"mode"` // single（默认）| sentinel
		Addr          string   `mapstructure:"addr"`
		Password      string   `mapstructure:"password"`
		DB            int      `mapstructure:"db"`
		MasterName    string   `mapstructure:"master_name"`
		SentinelAddrs []string `mapstructure:"sentinel_addrs"`
	} `mapstructure:"redis"`
	RabbitMQ struct {
		Addr                 string `mapstructure:"addr"`
		User                 string `mapstructure:"user"`
		Password             string `mapstructure:"password"`
		PublishTimeoutSecond int    `mapstructure:"publish_timeout_second"`
		ConsumerCount        int    `mapstructure:"consumer_count"`
	} `mapstructure:"rabbitmq"`
	Reconcile struct {
		IntervalSecond int `mapstructure:"interval_second"`
	} `mapstructure:"reconcile"`
	Ratelimit struct {
		UserPerSec int `mapstructure:"user_per_sec"`
		IPPerSec   int `mapstructure:"ip_per_sec"`
	} `mapstructure:"ratelimit"`
	Jwt struct {
		Secret     string `mapstructure:"secret"`
		ExpireHour int    `mapstructure:"expire_hour"`
	} `mapstructure:"jwt"`
	Server struct {
		Port string `mapstructure:"port"`
	} `mapstructure:"server"`
}

// Init 加载yaml配置，环境变量可覆盖（ACTIVITY_MYSQL_DSN 对应 mysql.dsn）
func Init() error {
	viper.SetConfigFile("config.yaml")
	if err := viper.ReadInConfig(); err != nil {
		return err
	}
	viper.SetEnvPrefix("ACTIVITY")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
	return viper.Unmarshal(&Conf)
}

// Validate 启动前校验配置：必填项缺失返回错误；弱JWT密钥仅告警（保持本地演示友好）
func Validate() error {
	if Conf.Mysql.Dsn == "" {
		return errors.New("mysql.dsn 未配置")
	}
	switch Conf.Redis.Mode {
	case "", "single":
		if Conf.Redis.Addr == "" {
			return errors.New("redis.addr 未配置")
		}
	case "sentinel":
		if Conf.Redis.MasterName == "" {
			return errors.New("redis.master_name 未配置")
		}
		if len(Conf.Redis.SentinelAddrs) == 0 {
			return errors.New("redis.sentinel_addrs 未配置")
		}
	default:
		return errors.New("redis.mode 仅支持 single/sentinel")
	}
	if Conf.RabbitMQ.Addr == "" {
		return errors.New("rabbitmq.addr 未配置")
	}
	if Conf.Jwt.Secret == "" {
		return errors.New("jwt.secret 未配置")
	}
	if Conf.Jwt.Secret == "change-me" || len(Conf.Jwt.Secret) < 32 {
		log.Println("警告: jwt.secret 为默认值或长度不足32，生产环境请更换强密钥")
	}
	return nil
}
