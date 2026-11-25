package main

import (
	"activity/config"
	"activity/internal/handler"
	"activity/internal/service"
	"activity/middleware"
	"activity/mq"
	"activity/redisclient"
	"activity/repository"
	"log"

	"github.com/gin-gonic/gin"
)

func main() {
	// 初始化配置
	if err := config.Init(); err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}
	// 初始化数据库
	if err := repository.InitDB(); err != nil {
		log.Fatalf("数据库初始化失败: %v", err)
	}
	// redis
	if err := redisclient.Init(); err != nil {
		log.Fatalf("Redis初始化失败: %v", err)
	}
	// rabbitmq
	if err := mq.InitRabbitMQ(); err != nil {
		log.Fatalf("RabbitMQ初始化失败: %v", err)
	}
	// 启动消费者
	err := mq.StartConsumer(service.ConsumeSignUpMsg)
	if err != nil {
		log.Fatalf("启动消费者失败: %v", err)
	}
	// 启动定时对账：Redis -> MySQL 兜底同步
	service.StartReconcileLoop()

	r := gin.New()
	r.Use(middleware.Recover())

	// 公开路由
	public := r.Group("/api")
	{
		public.POST("/register", handler.Register)
		public.POST("/login", handler.Login)
	}

	// 需要登录鉴权
	api := r.Group("/api")
	api.Use(middleware.JwtAuth())
	api.Use(middleware.Limiter())
	{
		api.POST("/activity", handler.CreateActivity)
		api.POST("/activity/:id/signup", handler.SignUp)
	}

	log.Println("服务启动", config.Conf.Server.Port)
	log.Fatal(r.Run(config.Conf.Server.Port))
}
