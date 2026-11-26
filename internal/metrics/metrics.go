package metrics

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP请求总数",
	}, []string{"method", "route", "status"})

	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP请求耗时",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route"})

	SignupTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "signup_total",
		Help: "报名结果计数（ok/already_signed/quota_exhausted/activity_missing/error）",
	}, []string{"result"})

	MQPublishTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mq_publish_total",
		Help: "MQ发布结果计数（ok/fail）",
	}, []string{"result"})

	MQConsumeTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mq_consume_total",
		Help: "MQ消费结果计数（ok/retry/drop）",
	}, []string{"result"})

	ReconcileDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "reconcile_duration_seconds",
		Help:    "单轮对账耗时",
		Buckets: prometheus.DefBuckets,
	})

	ReconcileActionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "reconcile_actions_total",
		Help: "对账动作计数（repair/clean/resend/expire）",
	}, []string{"action"})

	QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "mq_queue_depth",
		Help: "业务队列积压消息数",
	})
)

// GinMiddleware HTTP请求计数与延迟统计
func GinMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		HTTPRequestsTotal.WithLabelValues(c.Request.Method, route, strconv.Itoa(c.Writer.Status())).Inc()
		HTTPRequestDuration.WithLabelValues(c.Request.Method, route).Observe(time.Since(start).Seconds())
	}
}
