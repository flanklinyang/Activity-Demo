package mq

import (
	"activity/config"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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
	dlxQueueName  = "activity_signup_dlx_queue"
	exchangeName  = "activity_exchange"
	dlxExchange   = "activity_dlx_exchange"
	maxRetryCount = 3 // 最大重试3次，超过进死信队列
)

const publishPoolSize = 16

var (
	conn    *amqp091.Connection
	channel *amqp091.Channel
	// pubPool 发布通道池：每个通道独立串行确认，池化并行发布，避免单通道确认等待串行化
	pubPool []*pubChannel
	pubIdx  atomic.Uint64
)

// pubChannel 一个独立发布通道，mu 保证该通道同一时刻只有一个待确认消息
type pubChannel struct {
	ch       *amqp091.Channel
	confirms chan amqp091.Confirmation
	mu       sync.Mutex
}

// InitRabbitMQ 初始化连接、交换机、队列、死信配置，并开启生产者确认模式
func InitRabbitMQ() error {
	url := fmt.Sprintf("amqp://%s:%s@%s/", config.Conf.RabbitMQ.User, config.Conf.RabbitMQ.Password, config.Conf.RabbitMQ.Addr)
	var err error
	conn, err = amqp091.Dial(url)
	if err != nil {
		return err
	}
	channel, err = conn.Channel()
	if err != nil {
		return err
	}

	// 声明死信交换机
	err = channel.ExchangeDeclare(dlxExchange, "direct", true, false, false, false, nil)
	if err != nil {
		return err
	}
	// 声明业务交换机
	err = channel.ExchangeDeclare(exchangeName, "direct", true, false, false, false, nil)
	if err != nil {
		return err
	}

	// 业务队列：绑定死信交换机
	args := amqp091.Table{
		"x-dead-letter-exchange":    dlxExchange,
		"x-dead-letter-routing-key": dlxQueueName,
	}
	_, err = channel.QueueDeclare(queueName, true, false, false, false, args)
	if err != nil {
		return err
	}
	err = channel.QueueBind(queueName, queueName, exchangeName, false, nil)
	if err != nil {
		return err
	}

	// 死信队列
	_, err = channel.QueueDeclare(dlxQueueName, true, false, false, false, nil)
	if err != nil {
		return err
	}
	err = channel.QueueBind(dlxQueueName, dlxQueueName, dlxExchange, false, nil)
	if err != nil {
		return err
	}

	// Qos预取，控制消费并发，最多同时处理10条消息，保护数据库
	err = channel.Qos(10, 0, false)
	if err != nil {
		return err
	}

	// 发布通道池：每个通道开启独立确认流
	for range publishPoolSize {
		ch, err := conn.Channel()
		if err != nil {
			return err
		}
		if err = ch.Confirm(false); err != nil {
			return err
		}
		pubPool = append(pubPool, &pubChannel{
			ch:       ch,
			confirms: ch.NotifyPublish(make(chan amqp091.Confirmation, 1)),
		})
	}
	return nil
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
	pc := pubPool[int(pubIdx.Add(1)%publishPoolSize)]
	pc.mu.Lock()
	defer pc.mu.Unlock()

	publish := amqp091.Publishing{
		ContentType: "application/json",
		Body:        body,
	}
	if err := pc.ch.PublishWithContext(ctx, exchangeName, queueName, false, false, publish); err != nil {
		return err
	}

	timeout := 3 * time.Second
	if config.Conf.RabbitMQ.PublishTimeoutSecond > 0 {
		timeout = time.Duration(config.Conf.RabbitMQ.PublishTimeoutSecond) * time.Second
	}
	select {
	case conf := <-pc.confirms:
		if !conf.Ack {
			return errors.New("broker拒绝消息(nack)")
		}
		return nil
	case <-time.After(timeout):
		// 清掉可能迟到的确认，避免污染下一次等待
		select {
		case <-pc.confirms:
		default:
		}
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

// consumeLoop 单消费者主循环：通道断开后自动重建
func consumeLoop(id int, callback func(ctx context.Context, msg *SignUpMsg) error) {
	for {
		ch, err := conn.Channel()
		if err != nil {
			log.Printf("消费者%d创建通道失败: %v, 3秒后重试\n", id, err)
			time.Sleep(3 * time.Second)
			continue
		}
		// 每通道预取20条；通道死亡时未确认消息由broker重新分发给其他消费者
		if err := ch.Qos(20, 0, false); err != nil {
			_ = ch.Close()
			log.Printf("消费者%d设置Qos失败: %v, 3秒后重试\n", id, err)
			time.Sleep(3 * time.Second)
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
			log.Printf("消费者%d订阅失败: %v, 3秒后重试\n", id, err)
			time.Sleep(3 * time.Second)
			continue
		}
		log.Printf("消费者%d已启动\n", id)
		consumeDeliveries(id, deliveries, callback)
		_ = ch.Close()
		log.Printf("消费者%d通道断开, 3秒后重建\n", id)
		time.Sleep(3 * time.Second)
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
			log.Printf("消费者%d处理消息panic: %v, 消息重回队列\n", id, r)
			_ = d.Nack(false, true)
		}
	}()
	if err := callback(context.Background(), signMsg); err != nil {
		if signMsg.RetryCount >= maxRetryCount {
			// 超过最大重试次数，Nack且不重回队列，由队列死信配置转入死信队列
			log.Printf("消息超过最大重试次数，进入死信队列, activityId:%d userId:%d err:%v\n", signMsg.ActivityId, signMsg.StudentId, err)
			_ = d.Nack(false, false)
		} else {
			// 重试次数+1并重新发布（消息体重发，计数真实递增）
			signMsg.RetryCount++
			body, _ := json.Marshal(signMsg)
			if pubErr := publishWithConfirm(context.Background(), body); pubErr != nil {
				// 重新发布失败：Nack重回队列（保留原重试次数），等待broker重投后重试
				log.Printf("重试消息发布失败: %v, 原消息重回队列\n", pubErr)
				_ = d.Nack(false, true)
			} else {
				log.Printf("消费失败，重试 %d/%d, err:%v\n", signMsg.RetryCount, maxRetryCount, err)
				_ = d.Ack(false)
			}
		}
	} else {
		// 消费成功，ACK
		_ = d.Ack(false)
	}
}

// DeadLetterDepth 死信队列积压数量
func DeadLetterDepth() (int, error) {
	q, err := channel.QueueDeclarePassive(dlxQueueName, true, false, false, false, nil)
	if err != nil {
		return 0, err
	}
	return q.Messages, nil
}

// QueueDepth 业务队列积压数量
func QueueDepth() (int, error) {
	q, err := channel.QueueDeclarePassive(queueName, true, false, false, false, nil)
	if err != nil {
		return 0, err
	}
	return q.Messages, nil
}

// RequeueDeadLetters 将死信队列的消息重新投递回业务队列（重试计数清零），用于故障恢复后手动重放
func RequeueDeadLetters(ctx context.Context) (int, error) {
	n := 0
	for {
		d, ok, err := channel.Get(dlxQueueName, false)
		if err != nil {
			return n, err
		}
		if !ok {
			return n, nil
		}
		var msg SignUpMsg
		if err := json.Unmarshal(d.Body, &msg); err != nil {
			_ = d.Ack(false)
			continue
		}
		msg.RetryCount = 0
		body, _ := json.Marshal(&msg)
		if err := publishWithConfirm(ctx, body); err != nil {
			_ = d.Nack(false, true)
			return n, err
		}
		_ = d.Ack(false)
		n++
	}
}

// Close 关闭连接
func Close() {
	for _, pc := range pubPool {
		_ = pc.ch.Close()
	}
	_ = channel.Close()
	_ = conn.Close()
}
