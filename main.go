package main

import (
	"activity/config"
	"activity/internal/handler"
	"activity/internal/service"
	"activity/middleware"
	"activity/mq"
	"activity/redisclient"
	"activity/repository"
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	// 优雅退出：收到 SIGINT/SIGTERM 后停止接收请求并按序关闭依赖
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// 启动定时对账：Redis -> MySQL 兜底同步
	service.StartReconcileLoop(ctx)

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

	srv := &http.Server{
		Addr:    config.Conf.Server.Port,
		Handler: r,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("服务启动失败: %v", err)
		}
	}()
	log.Println("服务启动", config.Conf.Server.Port)

	<-ctx.Done()
	log.Println("收到退出信号，开始优雅关闭")

	// 1. 停止接收新请求，等待在途请求完成
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP优雅关闭超时: %v\n", err)
	}
	// 2. 关闭MQ（停止消费与重连）、Redis、MySQL
	mq.Close()
	_ = redisclient.Rdb.Close()
	if sqlDB, err := repository.DB.DB(); err == nil {
		_ = sqlDB.Close()
	}
	log.Println("已退出")
}
