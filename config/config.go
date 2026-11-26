package config

import (
	"github.com/spf13/viper"
)

// Conf 全局配置结构体
var Conf AppConfig

type AppConfig struct {
	Mysql struct {
		Dsn          string `mapstructure:"dsn"`
		MaxOpenConns int    `mapstructure:"max_open_conns"`
		MaxIdleConns int    `mapstructure:"max_idle_conns"`
	} `mapstructure:"mysql"`
	Redis struct {
		Addr     string `mapstructure:"addr"`
		Password string `mapstructure:"password"`
		DB       int    `mapstructure:"db"`
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

// Init 加载yaml配置
func Init() error {
	viper.SetConfigFile("config.yaml")
	err := viper.ReadInConfig()
	if err != nil {
		return err
	}
	err = viper.Unmarshal(&Conf)
	return err
}
