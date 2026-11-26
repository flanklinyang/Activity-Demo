package main

import (
	"activity/config"
	"activity/internal/handler"
	"activity/internal/logger"
	"activity/internal/metrics"
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
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// 初始化配置
	if err := config.Init(); err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}
	// 启动前校验（必填项缺失直接退出；弱密钥告警）
	if err := config.Validate(); err != nil {
		log.Fatalf("配置校验失败: %v", err)
	}
	// 结构化日志（依赖配置，故先于其他组件）
	if err := logger.Init(); err != nil {
		log.Fatalf("日志初始化失败: %v", err)
	}
	defer logger.Sync()
	// 初始化数据库
	if err := repository.InitDB(); err != nil {
		logger.L.Fatalf("数据库初始化失败: %v", err)
	}
	// redis
	if err := redisclient.Init(); err != nil {
		logger.L.Fatalf("Redis初始化失败: %v", err)
	}
	// rabbitmq
	if err := mq.InitRabbitMQ(); err != nil {
		logger.L.Fatalf("RabbitMQ初始化失败: %v", err)
	}
	// 启动消费者
	err := mq.StartConsumer(service.ConsumeSignUpMsg)
	if err != nil {
		logger.L.Fatalf("启动消费者失败: %v", err)
	}
	// 优雅退出：收到 SIGINT/SIGTERM 后停止接收请求并按序关闭依赖
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// 启动定时对账：Redis -> MySQL 兜底同步
	service.StartReconcileLoop(ctx)

	r := gin.New()
	// 不信任任何代理头，ClientIP 只取真实对端地址，防 X-Forwarded-For 伪造绕过IP限流；
	// 部署在反向代理后时改为 r.SetTrustedProxies([]string{"代理IP"})
	_ = r.SetTrustedProxies(nil)
	r.Use(metrics.GinMiddleware())
	r.Use(middleware.Recover())

	// 监控与健康检查（不鉴权不限流）
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	r.GET("/healthz", handler.Healthz)
	r.GET("/readyz", handler.Readyz)

	// 公开路由（按IP限流，防撞库与密码爆破）
	public := r.Group("/api")
	public.Use(middleware.IPLimiter())
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
			logger.L.Fatalf("服务启动失败: %v", err)
		}
	}()
	logger.L.Infof("服务启动 %s", config.Conf.Server.Port)

	<-ctx.Done()
	logger.L.Info("收到退出信号，开始优雅关闭")

	// 1. 停止接收新请求，等待在途请求完成
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.L.Errorf("HTTP优雅关闭超时: %v", err)
	}
	// 2. 关闭MQ（停止消费与重连）、Redis、MySQL
	mq.Close()
	_ = redisclient.Rdb.Close()
	if sqlDB, err := repository.DB.DB(); err == nil {
		_ = sqlDB.Close()
	}
	logger.L.Info("已退出")
}
