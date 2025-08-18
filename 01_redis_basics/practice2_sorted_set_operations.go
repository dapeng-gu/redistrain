package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// DelayedTask 延时任务结构
type DelayedTask struct {
	ID          string                 `json:"id"`
	Type        string                 `json:"type"`
	Payload     map[string]interface{} `json:"payload"`
	ScheduledAt time.Time              `json:"scheduled_at"`
	Priority    int                    `json:"priority"`
}

// SortedSetOperations 演示 Sorted Set 在延时队列和优先队列中的应用
type SortedSetOperations struct {
	client *redis.Client
	ctx    context.Context
}

func NewSortedSetOperations() *SortedSetOperations {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   1, // 使用不同的数据库
	})

	return &SortedSetOperations{
		client: rdb,
		ctx:    context.Background(),
	}
}

// 练习 2.1: 延时队列实现
func (s *SortedSetOperations) DelayedQueueOperations() {
	fmt.Println("=== Sorted Set 延时队列练习 ===")
	
	scheduledKey := "queue:scheduled"
	
	// 创建延时任务
	tasks := []DelayedTask{
		{
			ID:          "delayed_task_1",
			Type:        "email_reminder",
			Payload:     map[string]interface{}{"user_id": 123},
			ScheduledAt: time.Now().Add(5 * time.Second),
		},
		{
			ID:          "delayed_task_2", 
			Type:        "cleanup",
			Payload:     map[string]interface{}{"table": "temp_data"},
			ScheduledAt: time.Now().Add(10 * time.Second),
		},
		{
			ID:          "delayed_task_3",
			Type:        "report_generation",
			Payload:     map[string]interface{}{"report_type": "daily"},
			ScheduledAt: time.Now().Add(15 * time.Second),
		},
	}
	
	// 将任务添加到延时队列（使用时间戳作为分数）
	for _, task := range tasks {
		taskData, _ := json.Marshal(task)
		score := float64(task.ScheduledAt.Unix())
		
		err := s.client.ZAdd(s.ctx, scheduledKey, redis.Z{
			Score:  score,
			Member: taskData,
		}).Err()
		
		if err != nil {
			log.Fatal(err)
		}
		
		fmt.Printf("延时任务已添加: %s, 执行时间: %s\n", 
			task.ID, task.ScheduledAt.Format("15:04:05"))
	}
	
	// 查看队列中的任务数量
	count, _ := s.client.ZCard(s.ctx, scheduledKey).Result()
	fmt.Printf("延时队列中任务数量: %d\n", count)
	
	// 查看所有延时任务（按时间排序）
	fmt.Println("\n所有延时任务（按执行时间排序）:")
	members, _ := s.client.ZRangeWithScores(s.ctx, scheduledKey, 0, -1).Result()
	for _, member := range members {
		executeTime := time.Unix(int64(member.Score), 0)
		fmt.Printf("  执行时间: %s, 任务: %s\n", 
			executeTime.Format("15:04:05"), member.Member.(string)[:50]+"...")
	}
	
	// 模拟检查到期任务
	s.checkDueTasks(scheduledKey)
}

// 检查并处理到期任务
func (s *SortedSetOperations) checkDueTasks(scheduledKey string) {
	fmt.Println("\n=== 检查到期任务 ===")
	
	currentTime := float64(time.Now().Unix())
	
	// 查询当前时间之前的所有任务
	dueTasks, err := s.client.ZRangeByScoreWithScores(s.ctx, scheduledKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%f", currentTime),
	}).Result()
	
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Printf("找到 %d 个到期任务\n", len(dueTasks))
	
	// 处理到期任务
	for _, task := range dueTasks {
		// 从延时队列中移除
		s.client.ZRem(s.ctx, scheduledKey, task.Member)
		
		// 解析任务
		var delayedTask DelayedTask
		json.Unmarshal([]byte(task.Member.(string)), &delayedTask)
		
		fmt.Printf("处理到期任务: %s\n", delayedTask.ID)
		
		// 这里可以将任务移动到 pending 队列
		// s.client.LPush(s.ctx, "queue:pending", task.Member)
	}
}

// 练习 2.2: 优先队列实现
func (s *SortedSetOperations) PriorityQueueOperations() {
	fmt.Println("\n=== Sorted Set 优先队列练习 ===")
	
	priorityKey := "queue:priority"
	
	// 创建不同优先级的任务
	priorityTasks := []struct {
		taskID   string
		priority int // 数字越小优先级越高
		taskType string
	}{
		{"urgent_task_1", 1, "critical_alert"},
		{"normal_task_1", 5, "email_send"},
		{"low_task_1", 10, "log_cleanup"},
		{"urgent_task_2", 1, "security_check"},
		{"normal_task_2", 5, "data_sync"},
	}
	
	// 添加任务到优先队列
	for _, task := range priorityTasks {
		taskData := fmt.Sprintf(`{"id":"%s","type":"%s","priority":%d}`, 
			task.taskID, task.taskType, task.priority)
		
		s.client.ZAdd(s.ctx, priorityKey, redis.Z{
			Score:  float64(task.priority),
			Member: taskData,
		})
		
		fmt.Printf("添加任务: %s, 优先级: %d\n", task.taskID, task.priority)
	}
	
	// 按优先级顺序获取任务（优先级高的先执行）
	fmt.Println("\n按优先级处理任务:")
	for {
		// 获取优先级最高的任务（分数最小的）
		tasks, err := s.client.ZRangeWithScores(s.ctx, priorityKey, 0, 0).Result()
		if err != nil || len(tasks) == 0 {
			break
		}
		
		task := tasks[0]
		
		// 从队列中移除任务
		s.client.ZRem(s.ctx, priorityKey, task.Member)
		
		fmt.Printf("处理任务: %s, 优先级: %.0f\n", 
			task.Member.(string), task.Score)
	}
}

// 练习 2.3: 任务重试队列
func (s *SortedSetOperations) RetryQueueOperations() {
	fmt.Println("\n=== Sorted Set 重试队列练习 ===")
	
	retryKey := "queue:retry"
	
	// 模拟失败任务需要重试
	failedTasks := []struct {
		taskID    string
		retryAt   time.Time
		attempt   int
		maxRetry  int
	}{
		{"failed_task_1", time.Now().Add(30 * time.Second), 1, 3},
		{"failed_task_2", time.Now().Add(60 * time.Second), 2, 3},
		{"failed_task_3", time.Now().Add(120 * time.Second), 1, 5},
	}
	
	// 添加失败任务到重试队列
	for _, task := range failedTasks {
		taskData := fmt.Sprintf(`{"id":"%s","attempt":%d,"max_retry":%d}`,
			task.taskID, task.attempt, task.maxRetry)
		
		retryScore := float64(task.retryAt.Unix())
		
		s.client.ZAdd(s.ctx, retryKey, redis.Z{
			Score:  retryScore,
			Member: taskData,
		})
		
		fmt.Printf("任务加入重试队列: %s, 重试时间: %s, 尝试次数: %d/%d\n",
			task.taskID, task.retryAt.Format("15:04:05"), task.attempt, task.maxRetry)
	}
	
	// 查看重试队列状态
	count, _ := s.client.ZCard(s.ctx, retryKey).Result()
	fmt.Printf("重试队列中任务数量: %d\n", count)
	
	// 获取需要重试的任务（当前时间之前的）
	currentTime := float64(time.Now().Unix())
	retryTasks, _ := s.client.ZRangeByScoreWithScores(s.ctx, retryKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%f", currentTime),
	}).Result()
	
	fmt.Printf("当前可重试任务数量: %d\n", len(retryTasks))
	
	// 模拟处理重试任务
	for _, task := range retryTasks {
		fmt.Printf("重试任务: %s\n", task.Member.(string))
		// 从重试队列移除
		s.client.ZRem(s.ctx, retryKey, task.Member)
	}
}

func main() {
	ops := NewSortedSetOperations()
	
	// 清理之前的数据
	ops.client.FlushDB(ops.ctx)
	
	// 执行练习
	ops.DelayedQueueOperations()
	ops.PriorityQueueOperations()
	ops.RetryQueueOperations()
	
	fmt.Println("\n=== Sorted Set 操作练习完成 ===")
}
