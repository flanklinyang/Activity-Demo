package mq

import (
	"activity/config"
	"activity/internal/logger"
	"activity/internal/metrics"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// SignUpMsg 报名消息体，RetryCount 放消息体里，重新发布时才真实递增
type SignUpMsg struct {
	ActivityId int64 `json:"activity_id"`
	StudentId  int64 `json:"student_id"`
	RetryCount int32 `json:"retry_count"`
}

const (
	queueName     = "activity_signup_queue"
	exchangeName  = "activity_exchange"
	maxRetryCount = 3 // 最大重试3次，超过直接丢弃，报名事实在Redis，由定时对账补发
)

const publishPoolSize = 16

var (
	// conn/channel/pubPool 断线重连时整体原子替换，读写均走 connMu
	connMu  sync.RWMutex
	conn    *amqp091.Connection
	channel *amqp091.Channel
	// pubPool 发布通道池：每个通道独立串行确认，池化并行发布，避免单通道确认等待串行化
	pubPool []*pubChannel
	pubIdx  atomic.Uint64
	// droppedCount/lastDropAt 重试耗尽被丢弃的消息统计，供对账判断DB写入是否健康
	droppedCount atomic.Uint64
	lastDropAt   atomic.Int64

	stopCh   = make(chan struct{})
	stopOnce sync.Once
)

// pubChannel 一个独立发布通道，mu 保证该通道同一时刻只有一个待确认消息
type pubChannel struct {
	ch       *amqp091.Channel
	confirms chan amqp091.Confirmation
	mu       sync.Mutex
}

func rabbitURL() string {
	return fmt.Sprintf("amqp://%s:%s@%s/", config.Conf.RabbitMQ.User, config.Conf.RabbitMQ.Password, config.Conf.RabbitMQ.Addr)
}

// InitRabbitMQ 建立连接、声明交换机/队列/发布池，并启动断线重连监听
func InitRabbitMQ() error {
	newConn, err := amqp091.Dial(rabbitURL())
	if err != nil {
		return err
	}
	ch, pool, err := setup(newConn)
	if err != nil {
		_ = newConn.Close()
		return err
	}
	connMu.Lock()
	conn = newConn
	channel = ch
	pubPool = pool
	connMu.Unlock()
	go watchConnection()
	return nil
}

// setup 在给定连接上声明交换机/队列并创建发布通道池，返回局部变量由调用方原子替换
func setup(c *amqp091.Connection) (*amqp091.Channel, []*pubChannel, error) {
	ch, err := c.Channel()
	if err != nil {
		return nil, nil, err
	}
	// 声明业务交换机
	if err = ch.ExchangeDeclare(exchangeName, "direct", true, false, false, false, nil); err != nil {
		_ = ch.Close()
		return nil, nil, err
	}
	// 业务队列（无死信配置：重试耗尽直接丢弃，由定时对账兜底补发）
	if _, err = ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		return nil, nil, err
	}
	if err = ch.QueueBind(queueName, queueName, exchangeName, false, nil); err != nil {
		_ = ch.Close()
		return nil, nil, err
	}
	// 发布通道池：每个通道开启独立确认流
	var pool []*pubChannel
	for range publishPoolSize {
		pc, err := c.Channel()
		if err != nil {
			_ = ch.Close()
			closePubPool(pool)
			return nil, nil, err
		}
		if err = pc.Confirm(false); err != nil {
			_ = pc.Close()
			_ = ch.Close()
			closePubPool(pool)
			return nil, nil, err
		}
		pool = append(pool, &pubChannel{
			ch:       pc,
			confirms: pc.NotifyPublish(make(chan amqp091.Confirmation, 1)),
		})
	}
	return ch, pool, nil
}

func closePubPool(pool []*pubChannel) {
	for _, p := range pool {
		_ = p.ch.Close()
	}
}

// watchConnection 监听连接断开并驱动重连；每次重连成功后监听新连接
func watchConnection() {
	for {
		select {
		case <-stopCh:
			return
		default:
		}
		c := getConn()
		if c == nil {
			return
		}
		notify := c.NotifyClose(make(chan *amqp091.Error, 1))
		select {
		case err := <-notify:
			logger.L.Errorw("RabbitMQ连接断开，开始重连", "err", err)
			reconnect()
		case <-stopCh:
			return
		}
	}
}

// reconnect 指数退避重拨，成功后原子替换全局conn/channel/pubPool
func reconnect() {
	backoff := time.Second
	for {
		select {
		case <-stopCh:
			return
		default:
		}
		newConn, err := amqp091.Dial(rabbitURL())
		if err == nil {
			var ch *amqp091.Channel
			var pool []*pubChannel
			if ch, pool, err = setup(newConn); err == nil {
				connMu.Lock()
				old := conn
				conn = newConn
				channel = ch
				pubPool = pool
				connMu.Unlock()
				// 关闭旧连接：旧连接上的消费者通道随之退出，重建到新连接
				if old != nil {
					_ = old.Close()
				}
				logger.L.Infow("RabbitMQ重连成功")
				return
			}
			_ = newConn.Close()
		}
		logger.L.Errorw("RabbitMQ重连失败", "err", err, "backoff", backoff.String())
		select {
		case <-stopCh:
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
}

// Publish 发布报名消息并等待 broker 确认（确认=消息已落盘到队列）
func Publish(ctx context.Context, msg *SignUpMsg) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return publishWithConfirm(ctx, body)
}

// publishWithConfirm 从发布池取一个通道发送消息并等待该通道的确认或超时
func publishWithConfirm(ctx context.Context, body []byte) error {
	pool := getPubPool()
	if len(pool) == 0 {
		return errors.New("发布通道池未初始化")
	}
	pc := pool[int(pubIdx.Add(1)%uint64(len(pool)))]
	pc.mu.Lock()
	defer pc.mu.Unlock()

	publish := amqp091.Publishing{
		ContentType: "application/json",
		Body:        body,
	}
	if err := pc.ch.PublishWithContext(ctx, exchangeName, queueName, false, false, publish); err != nil {
		metrics.MQPublishTotal.WithLabelValues("fail").Inc()
		return err
	}

	timeout := 3 * time.Second
	if config.Conf.RabbitMQ.PublishTimeoutSecond > 0 {
		timeout = time.Duration(config.Conf.RabbitMQ.PublishTimeoutSecond) * time.Second
	}
	select {
	case conf := <-pc.confirms:
		if !conf.Ack {
			metrics.MQPublishTotal.WithLabelValues("fail").Inc()
			return errors.New("broker拒绝消息(nack)")
		}
		metrics.MQPublishTotal.WithLabelValues("ok").Inc()
		return nil
	case <-time.After(timeout):
		// 清掉可能迟到的确认，避免污染下一次等待
		select {
		case <-pc.confirms:
		default:
		}
		metrics.MQPublishTotal.WithLabelValues("fail").Inc()
		return errors.New("等待broker确认超时")
	}
}

// StartConsumer 启动多个独立消费者：每个消费者独占一个通道，RabbitMQ 轮询分发
func StartConsumer(callback func(ctx context.Context, msg *SignUpMsg) error) error {
	count := config.Conf.RabbitMQ.ConsumerCount
	if count <= 0 {
		count = 5
	}
	for i := 0; i < count; i++ {
		go consumeLoop(i, callback)
	}
	return nil
}

// consumeLoop 单消费者主循环：连接断开后自动重建（重连由 watchConnection 负责）
func consumeLoop(id int, callback func(ctx context.Context, msg *SignUpMsg) error) {
	for {
		select {
		case <-stopCh:
			return
		default:
		}
		c := getConn()
		if c == nil {
			if !sleepOrStop(3 * time.Second) {
				return
			}
			continue
		}
		ch, err := c.Channel()
		if err != nil {
			logger.L.Errorw("消费者创建通道失败", "consumer", id, "err", err)
			if !sleepOrStop(3 * time.Second) {
				return
			}
			continue
		}
		// 每通道预取20条；通道死亡时未确认消息由broker重新分发给其他消费者
		if err := ch.Qos(20, 0, false); err != nil {
			_ = ch.Close()
			logger.L.Errorw("消费者设置Qos失败", "consumer", id, "err", err)
			if !sleepOrStop(3 * time.Second) {
				return
			}
			continue
		}
		deliveries, err := ch.Consume(
			queueName,
			fmt.Sprintf("activity-consumer-%d", id),
			false, // false=手动ACK
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			_ = ch.Close()
			logger.L.Errorw("消费者订阅失败", "consumer", id, "err", err)
			if !sleepOrStop(3 * time.Second) {
				return
			}
			continue
		}
		logger.L.Infow("消费者已启动", "consumer", id)
		consumeDeliveries(id, deliveries, callback)
		_ = ch.Close()
		select {
		case <-stopCh:
			return
		default:
			logger.L.Infow("消费者通道断开，3秒后重建", "consumer", id)
			if !sleepOrStop(3 * time.Second) {
				return
			}
		}
	}
}

// sleepOrStop 等待指定时长，期间收到停止信号返回false
func sleepOrStop(d time.Duration) bool {
	select {
	case <-stopCh:
		return false
	case <-time.After(d):
		return true
	}
}

func consumeDeliveries(id int, deliveries <-chan amqp091.Delivery, callback func(ctx context.Context, msg *SignUpMsg) error) {
	for d := range deliveries {
		var signMsg SignUpMsg
		if err := json.Unmarshal(d.Body, &signMsg); err != nil {
			// 消息体解析失败，无法处理，直接ACK丢弃
			_ = d.Ack(false)
			continue
		}
		processDelivery(id, d, &signMsg, callback)
	}
}

func processDelivery(id int, d amqp091.Delivery, signMsg *SignUpMsg, callback func(ctx context.Context, msg *SignUpMsg) error) {
	defer func() {
		if r := recover(); r != nil {
			logger.L.Errorw("消费者处理消息panic，消息重回队列", "consumer", id, "err", r)
			_ = d.Nack(false, true)
		}
	}()
	if err := callback(context.Background(), signMsg); err != nil {
		if signMsg.RetryCount >= maxRetryCount {
			// 超过最大重试次数：Nack不重回队列，消息丢弃；报名事实仍在Redis，由定时对账补发
			droppedCount.Add(1)
			lastDropAt.Store(time.Now().Unix())
			metrics.MQConsumeTotal.WithLabelValues("drop").Inc()
			logger.L.Errorw("消息重试耗尽已丢弃，等待对账补发",
				"activityId", signMsg.ActivityId, "userId", signMsg.StudentId, "err", err)
			_ = d.Nack(false, false)
		} else {
			// 重试次数+1并重新发布（消息体重发，计数真实递增）
			signMsg.RetryCount++
			body, _ := json.Marshal(signMsg)
			if pubErr := publishWithConfirm(context.Background(), body); pubErr != nil {
				// 重新发布失败：Nack重回队列（保留原重试次数），等待broker重投后重试
				logger.L.Errorw("重试消息发布失败，原消息重回队列", "err", pubErr)
				_ = d.Nack(false, true)
			} else {
				metrics.MQConsumeTotal.WithLabelValues("retry").Inc()
				logger.L.Warnw("消费失败，重试",
					"retry", signMsg.RetryCount, "maxRetry", maxRetryCount, "err", err)
				_ = d.Ack(false)
			}
		}
	} else {
		// 消费成功，ACK
		metrics.MQConsumeTotal.WithLabelValues("ok").Inc()
		_ = d.Ack(false)
	}
}

// DroppedRecently 返回近期重试耗尽被丢弃的消息数和最近一次丢弃是否发生在 since 内
func DroppedRecently(since time.Duration) (uint64, bool) {
	t := time.Unix(lastDropAt.Load(), 0)
	return droppedCount.Load(), time.Since(t) < since
}

// QueueDepth 业务队列积压数量（同时刷新积压指标）
func QueueDepth() (int, error) {
	ch := getChannel()
	if ch == nil {
		return 0, errors.New("channel未初始化")
	}
	q, err := ch.QueueDeclarePassive(queueName, true, false, false, false, nil)
	if err != nil {
		return 0, err
	}
	metrics.QueueDepth.Set(float64(q.Messages))
	return q.Messages, nil
}

// IsConnected 连接是否存活（供就绪探针使用）
func IsConnected() bool {
	c := getConn()
	return c != nil && !c.IsClosed()
}

// Close 停止重连与消费，关闭连接
func Close() {
	stopOnce.Do(func() { close(stopCh) })
	connMu.Lock()
	defer connMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func getConn() *amqp091.Connection {
	connMu.RLock()
	defer connMu.RUnlock()
	return conn
}

func getChannel() *amqp091.Channel {
	connMu.RLock()
	defer connMu.RUnlock()
	return channel
}

func getPubPool() []*pubChannel {
	connMu.RLock()
	defer connMu.RUnlock()
	return pubPool
}
