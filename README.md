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
                    │          │  断线自动重连（指数退避1s~30s）
                    └────┬─────┘
                         │  5个独立通道消费者，每通道预取20，手动ACK
                    ┌────▼─────┐
                    │   MySQL  │  联合唯一索引幂等，最终一致记录
                    └──────────┘

   定时对账（默认60s）：Redis报名集合 vs MySQL记录 → 差集补发MQ
   门控：近2×间隔内有消费丢弃则跳过本轮补发，防DB故障期间churn
   自愈：修复缺失名额key（DB恢复）、清理孤儿key（含脏报名记录）

   监控与健康检查：GET /metrics（Prometheus） /healthz /readyz
```

## 核心设计

- **Redis 是运行期事实源**：Lua 脚本原子完成查重与扣名额，`appendfsync always` 落盘即报名成立；MySQL 只是最终一致记录
- **报名成功语义**：Redis 持久化成功即返回成功；MQ 发布失败只记日志，由对账兜底补发（避免"被告知失败但事实源记着成功"的矛盾）
- **创建链路一致性**：DB 插入 → Redis 名额初始化，Redis 失败删除 DB 记录补偿返回报错；对账自愈修复缺失名额 key（从 DB 恢复）并清理孤儿 key（含脏报名记录）
- **异步落库幂等**：`(activity_id, user_id)` 联合唯一索引兜底防重复；重试计数放消息体（header 修改不生效）
- **重试耗尽即丢弃**：消息重试 3 次后丢弃并记录统计，恢复交给对账——不维护死信队列（本架构下死信无正确性职责：报名事实在 Redis，可被对账恢复）
- **对账丢弃门控**：近 2×间隔内发生过消费丢弃（DB 写入不健康）则跳过本轮补发，避免对账补发与消费重试互相放大
- **已报名人数**：直接读 Redis 报名集合 `SCARD`（O(1)），MySQL 不维护计数
- **生产化配套**：优雅退出（信号→停止接收→按序关闭MQ/Redis/DB）、MQ 断线自动重连、结构化日志（zap JSON）、Prometheus 指标与健康检查、限流（每用户20/s + 公开接口每IP10/s）、输入校验与错误分类（业务错误/系统错误分离，真实 HTTP 状态码）

## 技术栈

| 组件 | 选型 |
|---|---|
| Web 框架 | Gin |
| 数据库 | MySQL + GORM（AutoMigrate，连接池含最大存活时间） |
| 缓存/事实源 | Redis（go-redis v9） |
| 消息队列 | RabbitMQ（amqp091-go，生产者确认 + 手动 ACK + 断线重连） |
| 鉴权 | JWT（golang-jwt/v5，HS256 算法白名单）+ bcrypt |
| 配置 | Viper（config.yaml + 环境变量覆盖 + 启动校验） |
| 日志 | zap（JSON 结构化） |
| 监控 | Prometheus client（/metrics + /healthz + /readyz） |

## 目录结构

```
main.go                 入口：配置校验→日志→DB→Redis→MQ→消费者→对账→路由→优雅退出
config/                 Viper 配置加载（环境变量覆盖 + 启动校验）
model/                  User / Activity / ActivitySignUp
repository/             GORM 初始化 + AutoMigrate（含 ConnMaxLifetime）
redisclient/            Redis 客户端
mq/rabbitmq.go          发布通道池、消费者、重试与丢弃统计、断线重连
internal/handler/       HTTP 处理器（业务/系统错误分类、健康检查）
internal/service/       报名(Lua扣名额) / 认证 / 对账(补发+自愈) / 业务错误定义
internal/logger/        zap 结构化日志
internal/metrics/       Prometheus 指标与 HTTP 中间件
middleware/             JwtAuth / Limiter(用户) / IPLimiter / Recover / 指标
pkg/response.go         统一响应 {code, msg, data}，真实 HTTP 状态码
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

   配置可用环境变量覆盖（键名点号转下划线），生产凭据建议走环境注入：

   ```bash
   export ACTIVITY_MYSQL_DSN="user:pwd@tcp(db-host:3306)/activity_db?charset=utf8mb4&parseTime=True&loc=Local"
   export ACTIVITY_REDIS_PASSWORD="..."
   export ACTIVITY_JWT_SECRET="<至少32字符的随机密钥>"
   ```

   启动时校验必填项；`jwt.secret` 为默认值或过短会打告警（生产必须更换）。

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

统一响应格式：`{"code": 0, "msg": "ok", "data": ...}`，`code=0` 为成功；HTTP 状态码与业务 code 对应（400/401/429/500），body 结构不变。

| 方法 | 路径 | 说明 | 鉴权 |
|---|---|---|---|
| POST | `/api/register` | 注册，body: `{username, password}`（用户名3-50字符，密码6-72字符） | 无 |
| POST | `/api/login` | 登录，返回 JWT token；失败统一返回"用户名或密码错误"（防账号枚举） | 无 |
| POST | `/api/activity` | 创建活动，body: `{title, content, total_quota}`（标题1-128字符，名额1-1000000） | Bearer |
| POST | `/api/activity/:id/signup` | 报名（重复报名/名额不足/活动不存在均返回对应业务错误） | Bearer |
| GET | `/metrics` | Prometheus 指标 | 无 |
| GET | `/healthz` | 存活探针 | 无 |
| GET | `/readyz` | 就绪探针（DB/Redis/MQ 全可用才200，供负载均衡摘流） | 无 |

限流（Redis 固定窗口）：业务接口每用户 20 req/s；注册/登录每 IP 10 req/s（防撞库与爆破），阈值可用 `ratelimit.*` 配置，`ip_per_sec: -1` 关闭 IP 限流。

业务错误（如"名额不足"）返回 400 + 原始信息；系统错误返回 500 + 通用"服务繁忙"（内部细节只进日志）。

## 监控

Prometheus 抓取 `GET /metrics`，核心指标：

- `http_requests_total` / `http_request_duration_seconds`：请求量与延迟（按路由）
- `signup_total{result}`：报名结果分布（成功/已报名/名额不足/活动不可用/错误）
- `mq_publish_total` / `mq_consume_total{result}`：MQ 发布与消费（成功/重试/丢弃）
- `reconcile_duration_seconds` / `reconcile_actions_total{action}`：对账耗时与动作（repair/clean/resend）
- `mq_queue_depth`：业务队列积压

报警建议：`mq_consume_total{result="drop"}` 或 `mq_queue_depth` 持续 > 0 说明 DB 写入不健康；`readyz` 非 200 应告警。

## 压测

### 压测结果（docs/ 下两轮报告）

| 场景 | 结果 |
|---|---|
| 3万用户 / 1000 QPS / 30s | 通过，P95 7.7ms、P99 11ms |
| 6万用户 / 2000 QPS / 30s（容量余量） | 通过，P95 119.6ms、P99 231.8ms，报名结束后 17s 消化积压 |

一致性校验：无超卖、无重复、无遗漏。

### 压测工具

压测注册/登录阶段为单IP高并发，会触发公开接口限流，压测前请将 `ratelimit.ip_per_sec` 设为 `-1` 关闭 IP 限流（压测目标为核心报名链路）：

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
- **单点**：Redis/MySQL/RabbitMQ 均为单实例，规模扩大需考虑高可用（主从/哨兵/集群）；Redis 主从切换下 AOF `always` 的持久化保证与主从复制语义需单独评估
- **创建链路残留窗口**：进程在「DB 插入后、Redis 写入前」崩溃，活动暂时不可报名（fail closed），一个对账周期内自动修复名额 key
- **对账清理假设**：孤儿 key 清理以「软删除的活动视为不存在」为前提；将来若引入活动结束后清理名额 key 的机制，修复逻辑需加 EndTime 门控
- **指标为单实例内存态**：进程重启计数归零，生产可接 Prometheus 持久化；日志为标准输出 JSON，接入采集器即可
