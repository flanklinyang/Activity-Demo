# activity —— Go 活动报名系统

高并发活动抢报系统：**Redis 为运行期事实源，RabbitMQ 异步落库，MySQL 最终一致，定时对账兜底**。

## 架构

```
                      报名请求
                         │
                    ┌────▼─────┐
                    │  Gin API │  JWT鉴权 + 每用户20req/s限流
                    └────┬─────┘
                         │  Lua原子: 查重(SISMEMBER) + 扣名额(DECR) + 登记(SADD)
                    ┌────▼─────┐
                    │   Redis  │  AOF fsync 落盘即报名成立（事实源）
                    └────┬─────┘
                         │  发布报名消息（16通道池 + 生产者确认）
                    ┌────▼─────┐
                    │ RabbitMQ │  消费失败重试3次，耗尽直接丢弃（留日志+计数）
                    └────┬─────┘
                         │  5个独立通道消费者，每通道预取20，手动ACK
                    ┌────▼─────┐
                    │   MySQL  │  联合唯一索引幂等，最终一致记录
                    └──────────┘

   定时对账（默认60s）：Redis报名集合 vs MySQL记录 → 差集补发MQ
   门控：近2×间隔内有消费丢弃则跳过本轮补发，防DB故障期间churn
```

## 核心设计

- **Redis 是运行期事实源**：Lua 脚本原子完成查重与扣名额，`appendfsync always` 落盘即报名成立；MySQL 只是最终一致记录
- **报名成功语义**：Redis 持久化成功即返回成功；MQ 发布失败只记日志，由对账兜底补发（避免"被告知失败但事实源记着成功"的矛盾）
- **异步落库幂等**：`(activity_id, user_id)` 联合唯一索引兜底防重复；重试计数放消息体（header 修改不生效）
- **重试耗尽即丢弃**：消息重试 3 次后丢弃并记录统计，恢复交给对账——不维护死信队列（本架构下死信无正确性职责：报名事实在 Redis，可被对账恢复）
- **对账丢弃门控**：近 2×间隔内发生过消费丢弃（DB 写入不健康）则跳过本轮补发，避免对账补发与消费重试互相放大
- **已报名人数**：直接读 Redis 报名集合 `SCARD`（O(1)），MySQL 不维护计数

## 技术栈

| 组件 | 选型 |
|---|---|
| Web 框架 | Gin |
| 数据库 | MySQL + GORM（AutoMigrate） |
| 缓存/事实源 | Redis（go-redis v9） |
| 消息队列 | RabbitMQ（amqp091-go，生产者确认 + 手动 ACK） |
| 鉴权 | JWT（golang-jwt/v5）+ bcrypt |
| 配置 | Viper（config.yaml） |

## 目录结构

```
main.go                 入口：初始化配置→DB→Redis→MQ→消费者→对账→路由
config/                 Viper 配置加载
model/                  User / Activity / ActivitySignUp
repository/             GORM 初始化 + AutoMigrate
redisclient/            Redis 客户端
mq/rabbitmq.go          发布通道池、消费者、重试与丢弃统计
internal/handler/       HTTP 处理器
internal/service/       报名(Lua扣名额) / 认证 / 对账
middleware/             JwtAuth / Limiter / Recover
pkg/response.go         统一响应 {code, msg, data}
loadtest/               压测客户端（注册→登录→建活动→报名→一致性校验）
docs/                   压测报告
```

## 快速开始

### 环境要求

- Go 1.25+
- MySQL、Redis、RabbitMQ

### 配置

1. **Redis 服务端必须开启 AOF 持久化**（代码正确性依赖此配置）：

   ```
   appendonly yes
   appendfsync always
   ```

2. 复制配置模板并填入真实值：

   ```bash
   cp config.yaml.example config.yaml
   ```

### 运行

```bash
go build -o activity.exe .
./activity.exe
```

启动后自动：声明队列/交换机、启动消费者、启动对账循环、监听 `:8080`。

### 验证

```bash
# 注册
curl -X POST http://127.0.0.1:8080/api/register -H "Content-Type: application/json" \
  -d '{"username":"alice","password":"123456"}'

# 登录拿 token
curl -X POST http://127.0.0.1:8080/api/login -H "Content-Type: application/json" \
  -d '{"username":"alice","password":"123456"}'

# 创建活动（token 替换为上一步返回值）
curl -X POST http://127.0.0.1:8080/api/activity \
  -H "Content-Type: application/json" -H "Authorization: Bearer <token>" \
  -d '{"title":"示例活动","content":"描述","total_quota":100}'

# 报名
curl -X POST http://127.0.0.1:8080/api/activity/1/signup -H "Authorization: Bearer <token>"
```

## API

统一响应格式：`{"code": 0, "msg": "ok", "data": ...}`，`code=0` 为成功。

| 方法 | 路径 | 说明 | 鉴权 |
|---|---|---|---|
| POST | `/api/register` | 注册，body: `{username, password}` | 无 |
| POST | `/api/login` | 登录，返回 JWT token | 无 |
| POST | `/api/activity` | 创建活动，body: `{title, content, total_quota}` | Bearer |
| POST | `/api/activity/:id/signup` | 报名（重复报名返回"你已经报名该活动"，名额不足返回"名额不足"） | Bearer |

业务接口有每用户 20 req/s 的 Redis 固定窗口限流。

## 压测

### 压测结果（docs/ 下两轮报告）

| 场景 | 结果 |
|---|---|
| 3万用户 / 1000 QPS / 30s | 通过，P95 7.7ms、P99 11ms |
| 6万用户 / 2000 QPS / 30s（容量余量） | 通过，P95 119.6ms、P99 231.8ms，报名结束后 17s 消化积压 |

一致性校验：无超卖、无重复、无遗漏。

### 压测工具

```bash
go run ./loadtest -users 30000 -qps 1000 -duration 30 \
  -dsn "root:xxx@tcp(127.0.0.1:3306)/activity_db?charset=utf8mb4&parseTime=True&loc=Local" \
  -mq-url "http://<mq-host>:15672/api/queues/%2F/activity_signup_queue" \
  -mq-pass <mq管理密码> -redis-pass <redis密码>
```

参数：`-users` 用户数（每人报名一次）、`-qps` 报名阶段目标 QPS、`-duration` 持续时间（秒）、`-dsn` MySQL 连接串（必填）、`-mq-url/-mq-user/-mq-pass` RabbitMQ 管理 API、`-redis-addr/-redis-pass` Redis。

流程：注册 → 登录 → 建活动 → 受控速率报名 → 等待队列排空落库 → 一致性校验（Redis 名额/报名集合 vs MySQL 记录数/去重数）。

## 已知边界

- **舒适区**：单实例持续 1000 QPS 无积压；2000 QPS 突发可扛（事后收敛），持续 2000 QPS 需扩容消费者（`consumer_count=10`）或批量插入
- **单点**：Redis/MySQL/RabbitMQ 均为单实例，规模扩大需考虑高可用（主从/哨兵/集群）
- **生产监控待补**：目前只有日志，无指标采集与告警
