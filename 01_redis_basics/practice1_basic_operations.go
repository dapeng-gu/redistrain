package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// Task 表示一个任务
type Task struct {
	ID       string                 `json:"id"`
	Type     string                 `json:"type"`
	Payload  map[string]interface{} `json:"payload"`
	MaxRetry int                    `json:"max_retry"`
	Retry    int                    `json:"retry"`
	Queue    string                 `json:"queue"`
}

// RedisBasicOperations 演示 Redis 基础数据结构操作
type RedisBasicOperations struct {
	client *redis.Client
	ctx    context.Context
}

func NewRedisBasicOperations() *RedisBasicOperations {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	return &RedisBasicOperations{
		client: rdb,
		ctx:    context.Background(),
	}
}

// 练习 1.1: String 操作 - 任务计数器
func (r *RedisBasicOperations) StringOperations() {
	fmt.Println("=== String 操作练习 ===")
	
	// 任务计数器
	key := "task:counter"
	
	// 原子递增
	count, err := r.client.Incr(r.ctx, key).Result()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("当前任务数量: %d\n", count)
	
	// 设置过期时间（24小时后重置计数器）
	r.client.Expire(r.ctx, key, 24*time.Hour)
	
	// 获取剩余过期时间
	ttl, _ := r.client.TTL(r.ctx, key).Result()
	fmt.Printf("计数器剩余时间: %v\n", ttl)
	
	// 批量递增
	r.client.IncrBy(r.ctx, key, 10)
	newCount, _ := r.client.Get(r.ctx, key).Int()
	fmt.Printf("批量递增后: %d\n", newCount)
}

// 练习 1.2: List 操作 - 任务队列
func (r *RedisBasicOperations) ListOperations() {
	fmt.Println("\n=== List 操作练习 ===")
	
	queueKey := "queue:default"
	
	// 创建几个示例任务
	tasks := []Task{
		{ID: "task1", Type: "email", Payload: map[string]interface{}{"to": "user@example.com"}},
		{ID: "task2", Type: "image_resize", Payload: map[string]interface{}{"url": "image.jpg"}},
		{ID: "task3", Type: "notification", Payload: map[string]interface{}{"message": "Hello"}},
	}
	
	// 入队操作 (LPUSH)
	for _, task := range tasks {
		taskData, _ := json.Marshal(task)
		r.client.LPush(r.ctx, queueKey, taskData)
		fmt.Printf("任务入队: %s\n", task.ID)
	}
	
	// 查看队列长度
	length, _ := r.client.LLen(r.ctx, queueKey).Result()
	fmt.Printf("队列长度: %d\n", length)
	
	// 查看队列内容（不移除）
	items, _ := r.client.LRange(r.ctx, queueKey, 0, -1).Result()
	fmt.Printf("队列内容: %v\n", len(items))
	
	// 出队操作 (RPOP)
	for i := 0; i < 2; i++ {
		taskData, err := r.client.RPop(r.ctx, queueKey).Result()
		if err == redis.Nil {
			fmt.Println("队列为空")
			break
		}
		
		var task Task
		json.Unmarshal([]byte(taskData), &task)
		fmt.Printf("任务出队: %s\n", task.ID)
	}
	
	// 阻塞出队 (BRPOP) - 超时时间 5 秒
	fmt.Println("等待新任务...")
	result, err := r.client.BRPop(r.ctx, 2*time.Second, queueKey).Result()
	if err == redis.Nil {
		fmt.Println("超时，没有新任务")
	} else {
		fmt.Printf("获取到任务: %s\n", result[1])
	}
}

// 练习 1.3: Hash 操作 - 任务详情存储
func (r *RedisBasicOperations) HashOperations() {
	fmt.Println("\n=== Hash 操作练习 ===")
	
	taskID := "task:123"
	taskKey := fmt.Sprintf("task:details:%s", taskID)
	
	// 存储任务详情
	taskDetails := map[string]interface{}{
		"id":         taskID,
		"type":       "email_delivery",
		"status":     "pending",
		"created_at": time.Now().Unix(),
		"retry":      0,
		"max_retry":  3,
	}
	
	// 批量设置字段
	r.client.HMSet(r.ctx, taskKey, taskDetails)
	fmt.Printf("任务详情已存储: %s\n", taskID)
	
	// 获取单个字段
	status, _ := r.client.HGet(r.ctx, taskKey, "status").Result()
	fmt.Printf("任务状态: %s\n", status)
	
	// 原子递增重试次数
	retryCount, _ := r.client.HIncrBy(r.ctx, taskKey, "retry", 1).Result()
	fmt.Printf("重试次数: %d\n", retryCount)
	
	// 获取所有字段
	allFields, _ := r.client.HGetAll(r.ctx, taskKey).Result()
	fmt.Printf("所有字段: %v\n", allFields)
	
	// 检查字段是否存在
	exists, _ := r.client.HExists(r.ctx, taskKey, "priority").Result()
	fmt.Printf("priority 字段存在: %v\n", exists)
	
	// 设置字段过期时间（整个 hash 的过期时间）
	r.client.Expire(r.ctx, taskKey, 1*time.Hour)
}

func main() {
	ops := NewRedisBasicOperations()
	
	// 清理之前的数据
	ops.client.FlushDB(ops.ctx)
	
	// 执行练习
	ops.StringOperations()
	ops.ListOperations()
	ops.HashOperations()
	
	fmt.Println("\n=== 基础操作练习完成 ===")
}
