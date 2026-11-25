// 报名系统压测程序：注册 -> 登录 -> 建活动 -> 受控速率报名 -> 等待落库 -> 一致性校验
// 用法: go run ./loadtest -users 30000 -qps 1000 -duration 30 \
//   -dsn "root:xxx@tcp(127.0.0.1:3306)/activity_db?charset=utf8mb4&parseTime=True&loc=Local" \
//   -mq-url "http://host:15672/api/queues/%2F/activity_signup_queue" -mq-pass xxx -redis-pass xxx
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const (
	baseURL  = "http://127.0.0.1:8080"
	password = "123456" // 压测生成的统一用户密码，测试固定值非机密
)

var (
	userCount = flag.Int("users", 30000, "用户数（每人报名一次，即总请求数）")
	targetQPS = flag.Int("qps", 1000, "报名阶段目标QPS")
	duration  = flag.Int("duration", 30, "报名阶段持续时间(秒)")
	dsn       = flag.String("dsn", "", "MySQL DSN，落库校验用（必填）")
	mqURL     = flag.String("mq-url", "", "RabbitMQ管理API队列地址，如 http://host:15672/api/queues/%2F/activity_signup_queue")
	mqUser    = flag.String("mq-user", "admin", "RabbitMQ管理API用户名")
	mqPass    = flag.String("mq-pass", "", "RabbitMQ管理API密码")
	redisAddr = flag.String("redis-addr", "127.0.0.1:6379", "Redis地址")
	redisPass = flag.String("redis-pass", "", "Redis密码")
)

// 大连接池+keep-alive复用，避免每请求新建TCP连接耗尽客户端端口
var httpClient = &http.Client{
	Timeout: 20 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     90 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	},
}

func postJSON(path, token string, body any) (map[string]any, error) {
	b, _ := json.Marshal(body)
	req, err := http.NewRequest("POST", baseURL+path, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var m map[string]any
	json.Unmarshal(data, &m)
	return m, nil
}

type callResult struct {
	code    int // 0成功 1业务失败 2网络失败
	latency time.Duration
	errMsg  string
}

type errStats struct {
	mu    sync.Mutex
	count map[string]int
}

func (e *errStats) add(msg string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.count == nil {
		e.count = make(map[string]int)
	}
	kind := "其他"
	switch {
	case strings.Contains(msg, "refused"):
		kind = "connection refused"
	case strings.Contains(msg, "reset"):
		kind = "connection reset"
	case strings.Contains(msg, "EOF"):
		kind = "EOF"
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "Timeout"):
		kind = "timeout"
	case strings.Contains(msg, "assign requested address"):
		kind = "端口耗尽"
	}
	e.count[kind]++
}

func runWorkers(total, workers int, fn func(i int) callResult) []callResult {
	results := make([]callResult, total)
	var wg sync.WaitGroup
	ch := make(chan int)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range ch {
				results[i] = fn(i)
			}
		}()
	}
	go func() {
		for i := 0; i < total; i++ {
			ch <- i
		}
		close(ch)
	}()
	wg.Wait()
	return results
}

func percentiles(ds []time.Duration) (p50, p90, p95, p99, max time.Duration) {
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	n := len(ds)
	p := func(q float64) time.Duration {
		idx := int(q * float64(n))
		if idx >= n {
			idx = n - 1
		}
		return ds[idx]
	}
	return p(0.50), p(0.90), p(0.95), p(0.99), ds[n-1]
}

func main() {
	flag.Parse()
	if *dsn == "" {
		fmt.Println("缺少必填参数 -dsn（MySQL DSN），凭据不再硬编码，通过命令行传入")
		flag.Usage()
		return
	}
	n := *userCount
	fmt.Println("======== 报名系统正式压测 ========")
	fmt.Printf("参数: 用户数=%d 目标QPS=%d 持续=%ds\n", n, *targetQPS, *duration)
	fmt.Println("开始时间:", time.Now().Format("15:04:05"))

	// ---------- 阶段1: 注册 ----------
	t1 := time.Now()
	regResults := runWorkers(n, 100, func(i int) callResult {
		start := time.Now()
		m, err := postJSON("/api/register", "", map[string]string{
			"username": fmt.Sprintf("loadtest%04d", i),
			"password": password,
		})
		lat := time.Since(start)
		if err != nil {
			return callResult{2, lat, err.Error()}
		}
		if code, _ := m["code"].(float64); code == 0 {
			return callResult{0, lat, ""}
		}
		return callResult{1, lat, ""}
	})
	regOK, regFail, regExist := 0, 0, 0
	for _, r := range regResults {
		switch r.code {
		case 0:
			regOK++
		case 1:
			regExist++
		case 2:
			regFail++
		}
	}
	fmt.Printf("[注册] 新增=%d 已存在=%d 失败=%d 耗时=%.1fs 吞吐=%.1f/s\n",
		regOK, regExist, regFail, time.Since(t1).Seconds(), float64(n)/time.Since(t1).Seconds())
	if regFail > 0 {
		fmt.Println("注册存在网络失败，终止压测")
		return
	}

	// ---------- 阶段2: 登录拿token ----------
	t2 := time.Now()
	tokens := make([]string, n)
	loginResults := runWorkers(n, 100, func(i int) callResult {
		start := time.Now()
		m, err := postJSON("/api/login", "", map[string]string{
			"username": fmt.Sprintf("loadtest%04d", i),
			"password": password,
		})
		lat := time.Since(start)
		if err != nil {
			return callResult{2, lat, err.Error()}
		}
		if code, _ := m["code"].(float64); code == 0 {
			if data, ok := m["data"].(map[string]any); ok {
				if tok, ok := data["token"].(string); ok {
					tokens[i] = tok
					return callResult{0, lat, ""}
				}
			}
		}
		return callResult{1, lat, ""}
	})
	loginOK := 0
	for i, r := range loginResults {
		if r.code == 0 && tokens[i] != "" {
			loginOK++
		}
	}
	fmt.Printf("[登录] 成功=%d 失败=%d 耗时=%.1fs 吞吐=%.1f/s\n",
		loginOK, n-loginOK, time.Since(t2).Seconds(), float64(n)/time.Since(t2).Seconds())
	if loginOK < n {
		fmt.Println("登录不完整，终止压测")
		return
	}

	// ---------- 阶段3: 创建活动 ----------
	m, err := postJSON("/api/activity", tokens[0], map[string]any{
		"title":       fmt.Sprintf("loadtest-formal-%dqps", *targetQPS),
		"content":     "formal",
		"total_quota": n,
	})
	if err != nil {
		fmt.Println("创建活动失败:", err)
		return
	}
	data, _ := m["data"].(map[string]any)
	activityID := int64(data["ID"].(float64))
	fmt.Printf("[建活动] id=%d quota=%d\n", activityID, n)

	// ---------- 阶段4: 受控速率报名 ----------
	maxDepth := int64(0)
	var depthMu sync.Mutex
	stopDepth := make(chan struct{})
	var depthWG sync.WaitGroup
	depthWG.Add(1)
	go func() {
		defer depthWG.Done()
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stopDepth:
				return
			case <-tick.C:
				req, _ := http.NewRequest("GET", *mqURL, nil)
				req.SetBasicAuth(*mqUser, *mqPass)
				resp, err := httpClient.Do(req)
				if err != nil {
					continue
				}
				var q struct {
					Messages int `json:"messages"`
				}
				json.NewDecoder(resp.Body).Decode(&q)
				resp.Body.Close()
				depthMu.Lock()
				if int64(q.Messages) > maxDepth {
					maxDepth = int64(q.Messages)
				}
				depthMu.Unlock()
			}
		}
	}()

	var errs errStats
	signResults := make([]callResult, n)
	var signWG sync.WaitGroup
	jobCh := make(chan int, 2048)
	for w := 0; w < 300; w++ {
		signWG.Add(1)
		go func() {
			defer signWG.Done()
			for i := range jobCh {
				start := time.Now()
				m, err := postJSON(fmt.Sprintf("/api/activity/%d/signup", activityID), tokens[i], nil)
				lat := time.Since(start)
				if err != nil {
					errs.add(err.Error())
					signResults[i] = callResult{2, lat, err.Error()}
				} else if code, _ := m["code"].(float64); code == 0 {
					signResults[i] = callResult{0, lat, ""}
				} else {
					signResults[i] = callResult{1, lat, ""}
				}
			}
		}()
	}

	// 节奏器：按目标QPS分发请求，落后自动追赶
	t3 := time.Now()
	interval := time.Duration(float64(time.Second) / float64(*targetQPS))
	for i := 0; i < n; i++ {
		target := t3.Add(time.Duration(i) * interval)
		if d := time.Until(target); d > 0 {
			time.Sleep(d)
		}
		jobCh <- i
	}
	close(jobCh)
	signWG.Wait()
	elapsed := time.Since(t3)
	close(stopDepth)
	depthWG.Wait()

	signOK, signBizFail, signNetFail := 0, 0, 0
	var lats []time.Duration
	for _, r := range signResults {
		switch r.code {
		case 0:
			signOK++
			lats = append(lats, r.latency)
		case 1:
			signBizFail++
		case 2:
			signNetFail++
		}
	}
	p50, p90, p95, p99, maxLat := percentiles(lats)
	fmt.Printf("[报名] 目标QPS=%d 持续=%ds | 实际: 成功=%d 业务失败=%d 网络失败=%d 耗时=%.2fs 吞吐=%.0f/s\n",
		*targetQPS, *duration, signOK, signBizFail, signNetFail, elapsed.Seconds(), float64(n)/elapsed.Seconds())
	fmt.Printf("[报名延迟] P50=%v P90=%v P95=%v P99=%v Max=%v\n", p50, p90, p95, p99, maxLat)
	if signNetFail > 0 {
		errs.mu.Lock()
		fmt.Printf("[网络失败分类] %v\n", errs.count)
		errs.mu.Unlock()
	}

	// ---------- 阶段5: 等待队列排空 + DB落库 ----------
	db, err := gorm.Open(mysql.Open(*dsn), &gorm.Config{})
	if err != nil {
		fmt.Println("连接MySQL失败:", err)
		return
	}
	t4 := time.Now()
	drainedAt := time.Time{}
	queueDepth := 0
	for {
		req, _ := http.NewRequest("GET", *mqURL, nil)
		req.SetBasicAuth(*mqUser, *mqPass)
		resp, err := httpClient.Do(req)
		if err == nil {
			var q struct {
				Messages int `json:"messages"`
			}
			json.NewDecoder(resp.Body).Decode(&q)
			resp.Body.Close()
			queueDepth = q.Messages
		}
		var dbCount int64
		db.Table("activity_sign_ups").Where("activity_id = ?", activityID).Count(&dbCount)
		if queueDepth == 0 && drainedAt.IsZero() {
			drainedAt = time.Now()
		}
		if queueDepth == 0 && dbCount >= int64(signOK) {
			break
		}
		if time.Since(t4) > 300*time.Second {
			fmt.Printf("等待超时: 队列=%d 落库=%d 期望=%d\n", queueDepth, dbCount, signOK)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Printf("[落库] 队列排空耗时=%.1fs 全部落库耗时=%.1fs 峰值队列深度=%d\n",
		drainedAt.Sub(t3).Seconds(), time.Since(t3).Seconds(), maxDepth)

	// ---------- 阶段6: 一致性校验 ----------
	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr, Password: *redisPass, DB: 0})
	ctx := context.Background()
	quota, _ := rdb.Get(ctx, fmt.Sprintf("activity:quota:%d", activityID)).Int64()
	scard, _ := rdb.SCard(ctx, fmt.Sprintf("activity:sign:%d", activityID)).Result()
	var dbCount, dbDistinct int64
	db.Table("activity_sign_ups").Where("activity_id = ?", activityID).Count(&dbCount)
	db.Table("activity_sign_ups").Where("activity_id = ?", activityID).Distinct("user_id").Count(&dbDistinct)

	fmt.Println("-------- 一致性校验 --------")
	fmt.Printf("Redis剩余名额=%d (期望%d) | Redis报名集合=%d (期望%d) | DB记录=%d | DB去重=%d\n",
		quota, n-signOK, scard, signOK, dbCount, dbDistinct)
	ok := quota == int64(n-signOK) && scard == int64(signOK) && dbCount == int64(signOK) && dbDistinct == int64(signOK)
	if ok {
		fmt.Println("结论: 无超卖、无重复、无遗漏，最终一致 ✅")
	} else {
		fmt.Println("结论: 存在不一致 ❌")
	}
}
