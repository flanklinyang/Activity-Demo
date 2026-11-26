package repository

import (
	"activity/config"
	"activity/model"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var DB *gorm.DB

func InitDB() error {
	db, err := gorm.Open(mysql.Open(config.Conf.Mysql.Dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		return err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	sqlDB.SetMaxOpenConns(config.Conf.Mysql.MaxOpenConns)
	sqlDB.SetMaxIdleConns(config.Conf.Mysql.MaxIdleConns)
	// 连接最大存活时间小于MySQL wait_timeout，防服务端断开长闲连接后使用坏连接
	if config.Conf.Mysql.ConnMaxLifetimeSecond > 0 {
		sqlDB.SetConnMaxLifetime(time.Duration(config.Conf.Mysql.ConnMaxLifetimeSecond) * time.Second)
	}

	err = db.AutoMigrate(&model.User{}, &model.Activity{}, &model.ActivitySignUp{})
	if err != nil {
		return err
	}
	DB = db
	return nil
}
