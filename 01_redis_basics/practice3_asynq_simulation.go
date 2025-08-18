package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// TaskMessage 模拟 asynq 的任务消息结构
type TaskMessage struct {
	ID       string                 `json:"id"`
	Type     string                 `json:"type"`
	Payload  map[string]interface{} `json:"payload"`
	Queue    string                 `json:"queue"`
	Retry    int                    `json:"retry"`
	MaxRetry int                    `json:"max_retry"`
	Timeout  int64                  `json:"timeout"`
}

// AsynqSimulator 模拟 asynq 的核心功能
type AsynqSimulator struct {
	client *redis.Client
	ctx    context.Context
}

func NewAsynqSimulator() *AsynqSimulator {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   2, // 使用独立的数据库
	})

	return &AsynqSimulator{
		client: rdb,
		ctx:    context.Background(),
	}
}

// 生成 asynq 风格的键名
func (a *AsynqSimulator) getQueueKey(queue, suffix string) string {
	return fmt.Sprintf("asynq:{%s}:%s", queue, suffix)
}

func (a *AsynqSimulator) getTaskKey(queue, taskID string) string {
	return fmt.Sprintf("asynq:{%s}:t:%s", queue, taskID)
}

// 练习 3.1: 任务入队（模拟 asynq.Client.Enqueue）
func (a *AsynqSimulator) EnqueueTask(task TaskMessage) error {
	fmt.Printf("=== 任务入队: %s ===\n", task.ID)
	
	// 1. 将任务详情存储到 Hash
	taskKey := a.getTaskKey(task.Queue, task.ID)
	taskData := map[string]interface{}{
		"id":        task.ID,
		"type":      task.Type,
		"payload":   string(mustMarshal(task.Payload)),
		"queue":     task.Queue,
		"retry":     task.Retry,
		"max_retry": task.MaxRetry,
		"timeout":   task.Timeout,
		"state":     "pending",
		"created_at": time.Now().Unix(),
	}
	
	err := a.client.HMSet(a.ctx, taskKey, taskData).Err()
	if err != nil {
		return err
	}
	
	// 2. 将任务 ID 添加到 pending 队列
	pendingKey := a.getQueueKey(task.Queue, "pending")
	err = a.client.LPush(a.ctx, pendingKey, task.ID).Err()
	if err != nil {
		return err
	}
	
	fmt.Printf("任务已入队: %s -> %s\n", task.ID, pendingKey)
	return nil
}

// 练习 3.2: 延时任务入队
func (a *AsynqSimulator) EnqueueDelayedTask(task TaskMessage, delay time.Duration) error {
	fmt.Printf("=== 延时任务入队: %s (延时 %v) ===\n", task.ID, delay)
	
	// 1. 存储任务详情
	taskKey := a.getTaskKey(task.Queue, task.ID)
	taskData := map[string]interface{}{
		"id":           task.ID,
		"type":         task.Type,
		"payload":      string(mustMarshal(task.Payload)),
		"queue":        task.Queue,
		"retry":        task.Retry,
		"max_retry":    task.MaxRetry,
		"timeout":      task.Timeout,
		"state":        "scheduled",
		"created_at":   time.Now().Unix(),
		"scheduled_at": time.Now().Add(delay).Unix(),
	}
	
	err := a.client.HMSet(a.ctx, taskKey, taskData).Err()
	if err != nil {
		return err
	}
	
	// 2. 将任务添加到 scheduled 队列（使用执行时间作为分数）
	scheduledKey := a.getQueueKey(task.Queue, "scheduled")
	executeAt := time.Now().Add(delay).Unix()
	
	err = a.client.ZAdd(a.ctx, scheduledKey, redis.Z{
		Score:  float64(executeAt),
		Member: task.ID,
	}).Err()
	
	if err != nil {
		return err
	}
	
	fmt.Printf("延时任务已入队: %s -> %s (执行时间: %s)\n", 
		task.ID, scheduledKey, time.Unix(executeAt, 0).Format("15:04:05"))
	return nil
}

// 练习 3.3: 任务出队处理（模拟 asynq.Server 的 worker）
func (a *AsynqSimulator) DequeueTask(queue string) (*TaskMessage, error) {
	pendingKey := a.getQueueKey(queue, "pending")
	activeKey := a.getQueueKey(queue, "active")
	
	// 使用 BRPOPLPUSH 原子地从 pending 移动到 active
	taskID, err := a.client.BRPopLPush(a.ctx, pendingKey, activeKey, 5*time.Second).Result()
	if err == redis.Nil {
		return nil, fmt.Errorf("no tasks available")
	}
	if err != nil {
		return nil, err
	}
	
	// 获取任务详情
	taskKey := a.getTaskKey(queue, taskID)
	taskData, err := a.client.HGetAll(a.ctx, taskKey).Result()
	if err != nil {
		return nil, err
	}
	
	// 更新任务状态为 active
	a.client.HSet(a.ctx, taskKey, "state", "active", "started_at", time.Now().Unix())
	
	// 构造任务消息
	var payload map[string]interface{}
	json.Unmarshal([]byte(taskData["payload"]), &payload)
	
	task := &TaskMessage{
		ID:       taskData["id"],
		Type:     taskData["type"],
		Payload:  payload,
		Queue:    taskData["queue"],
		Retry:    parseInt(taskData["retry"]),
		MaxRetry: parseInt(taskData["max_retry"]),
		Timeout:  parseInt64(taskData["timeout"]),
	}
	
	fmt.Printf("任务出队: %s (从 %s 移动到 %s)\n", taskID, pendingKey, activeKey)
	return task, nil
}

// 练习 3.4: 任务完成处理
func (a *AsynqSimulator) CompleteTask(task *TaskMessage) error {
	fmt.Printf("=== 任务完成: %s ===\n", task.ID)
	
	activeKey := a.getQueueKey(task.Queue, "active")
	taskKey := a.getTaskKey(task.Queue, task.ID)
	
	// 从 active 队列移除任务
	err := a.client.LRem(a.ctx, activeKey, 1, task.ID).Err()
	if err != nil {
		return err
	}
	
	// 更新任务状态
	a.client.HMSet(a.ctx, taskKey, map[string]interface{}{
		"state":        "completed",
		"completed_at": time.Now().Unix(),
	})
	
	// 设置任务详情的过期时间（24小时后清理）
	a.client.Expire(a.ctx, taskKey, 24*time.Hour)
	
	fmt.Printf("任务已完成: %s\n", task.ID)
	return nil
}

// 练习 3.5: 任务失败重试
func (a *AsynqSimulator) RetryTask(task *TaskMessage, err error) error {
	fmt.Printf("=== 任务重试: %s (错误: %v) ===\n", task.ID, err)
	
	activeKey := a.getQueueKey(task.Queue, "active")
	taskKey := a.getTaskKey(task.Queue, task.ID)
	
	// 从 active 队列移除
	a.client.LRem(a.ctx, activeKey, 1, task.ID)
	
	// 增加重试次数
	task.Retry++
	
	if task.Retry >= task.MaxRetry {
		// 超过最大重试次数，移动到 archived
		return a.ArchiveTask(task, err)
	}
	
	// 计算重试延时（指数退避）
	retryDelay := time.Duration(task.Retry*task.Retry) * time.Second
	retryAt := time.Now().Add(retryDelay)
	
	// 更新任务信息
	a.client.HMSet(a.ctx, taskKey, map[string]interface{}{
		"state":      "retry",
		"retry":      task.Retry,
		"retry_at":   retryAt.Unix(),
		"last_error": err.Error(),
	})
	
	// 添加到重试队列
	retryKey := a.getQueueKey(task.Queue, "retry")
	a.client.ZAdd(a.ctx, retryKey, redis.Z{
		Score:  float64(retryAt.Unix()),
		Member: task.ID,
	})
	
	fmt.Printf("任务已加入重试队列: %s (第 %d 次重试，延时 %v)\n", 
		task.ID, task.Retry, retryDelay)
	return nil
}

// 任务归档
func (a *AsynqSimulator) ArchiveTask(task *TaskMessage, err error) error {
	fmt.Printf("=== 任务归档: %s ===\n", task.ID)
	
	taskKey := a.getTaskKey(task.Queue, task.ID)
	archivedKey := a.getQueueKey(task.Queue, "archived")
	
	// 更新任务状态
	a.client.HMSet(a.ctx, taskKey, map[string]interface{}{
		"state":       "archived",
		"archived_at": time.Now().Unix(),
		"final_error": err.Error(),
	})
	
	// 添加到归档队列
	a.client.ZAdd(a.ctx, archivedKey, redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: task.ID,
	})
	
	fmt.Printf("任务已归档: %s\n", task.ID)
	return nil
}

// 练习 3.6: 调度器 - 处理延时和重试任务
func (a *AsynqSimulator) ProcessScheduledTasks(queue string) {
	fmt.Println("=== 处理调度任务 ===")
	
	currentTime := float64(time.Now().Unix())
	
	// 处理延时任务
	scheduledKey := a.getQueueKey(queue, "scheduled")
	dueTasks, _ := a.client.ZRangeByScore(a.ctx, scheduledKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%f", currentTime),
	}).Result()
	
	for _, taskID := range dueTasks {
		// 移动到 pending 队列
		pendingKey := a.getQueueKey(queue, "pending")
		taskKey := a.getTaskKey(queue, taskID)
		
		a.client.ZRem(a.ctx, scheduledKey, taskID)
		a.client.LPush(a.ctx, pendingKey, taskID)
		a.client.HSet(a.ctx, taskKey, "state", "pending")
		
		fmt.Printf("延时任务已就绪: %s\n", taskID)
	}
	
	// 处理重试任务
	retryKey := a.getQueueKey(queue, "retry")
	retryTasks, _ := a.client.ZRangeByScore(a.ctx, retryKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%f", currentTime),
	}).Result()
	
	for _, taskID := range retryTasks {
		// 移动到 pending 队列
		pendingKey := a.getQueueKey(queue, "pending")
		taskKey := a.getTaskKey(queue, taskID)
		
		a.client.ZRem(a.ctx, retryKey, taskID)
		a.client.LPush(a.ctx, pendingKey, taskID)
		a.client.HSet(a.ctx, taskKey, "state", "pending")
		
		fmt.Printf("重试任务已就绪: %s\n", taskID)
	}
}

// 辅助函数
func mustMarshal(v interface{}) []byte {
	data, _ := json.Marshal(v)
	return data
}

func parseInt(s string) int {
	if s == "" {
		return 0
	}
	var i int
	fmt.Sscanf(s, "%d", &i)
	return i
}

func parseInt64(s string) int64 {
	if s == "" {
		return 0
	}
	var i int64
	fmt.Sscanf(s, "%d", &i)
	return i
}

func main() {
	sim := NewAsynqSimulator()
	
	// 清理数据
	sim.client.FlushDB(sim.ctx)
	
	// 创建示例任务
	task1 := TaskMessage{
		ID:       uuid.New().String(),
		Type:     "email_delivery",
		Payload:  map[string]interface{}{"to": "user@example.com", "subject": "Welcome"},
		Queue:    "default",
		MaxRetry: 3,
		Timeout:  30,
	}
	
	task2 := TaskMessage{
		ID:       uuid.New().String(),
		Type:     "image_resize",
		Payload:  map[string]interface{}{"url": "image.jpg", "size": "thumbnail"},
		Queue:    "default",
		MaxRetry: 5,
		Timeout:  60,
	}
	
	// 演示完整流程
	fmt.Println("=== asynq 模拟演示 ===\n")
	
	// 1. 入队普通任务
	sim.EnqueueTask(task1)
	
	// 2. 入队延时任务
	sim.EnqueueDelayedTask(task2, 3*time.Second)
	
	// 3. 处理调度任务
	time.Sleep(4 * time.Second)
	sim.ProcessScheduledTasks("default")
	
	// 4. 出队并处理任务
	for i := 0; i < 2; i++ {
		task, err := sim.DequeueTask("default")
		if err != nil {
			fmt.Printf("出队失败: %v\n", err)
			continue
		}
		
		// 模拟任务处理
		fmt.Printf("处理任务: %s (%s)\n", task.ID, task.Type)
		
		// 模拟任务成功/失败
		if i == 0 {
			sim.CompleteTask(task)
		} else {
			sim.RetryTask(task, fmt.Errorf("模拟处理失败"))
		}
	}
	
	fmt.Println("\n=== asynq 模拟演示完成 ===")
}
