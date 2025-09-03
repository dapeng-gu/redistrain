package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// DelayTask 延迟任务结构
type DelayTask struct {
	QueueName string                 `json:"queue_name"` // 队列名称
	TaskName  string                 `json:"task_name"`  // 任务名称
	Payload   map[string]interface{} `json:"payload"`    // 任务数据
	DelayTime time.Time              `json:"delay_time"` // 延迟执行时间
	Created   time.Time              `json:"created"`    // 创建时间
}

// DelayQueue 延迟队列
type DelayQueue struct {
	client redis.UniversalClient
}

// NewDelayQueue 创建延迟队列实例
func NewDelayQueue(client redis.UniversalClient) *DelayQueue {
	return &DelayQueue{client: client}
}

// generateTaskKey 生成任务键: 队名:任务名:时间戳
func (dq *DelayQueue) generateTaskKey(queueName, taskName string, delayTime time.Time) string {
	timestamp := delayTime.Unix()
	return fmt.Sprintf("%s:%s:%d", queueName, taskName, timestamp)
}

// getDelaySetKey 获取延迟队列的 Sorted Set 键
func (dq *DelayQueue) getDelaySetKey() string {
	return "delay_queue:tasks"
}

// getTaskDataKey 获取任务数据的 Hash 键
func (dq *DelayQueue) getTaskDataKey() string {
	return "delay_queue:data"
}

// AddDelayTask 添加延迟任务
func (dq *DelayQueue) AddDelayTask(ctx context.Context, task *DelayTask) error {
	// 生成任务键
	taskKey := dq.generateTaskKey(task.QueueName, task.TaskName, task.DelayTime)
	
	// 序列化任务数据
	taskData, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("序列化任务失败: %w", err)
	}
	
	// 使用事务确保原子性
	pipe := dq.client.TxPipeline()
	
	// 将任务键添加到 Sorted Set，分数为时间戳
	pipe.ZAdd(ctx, dq.getDelaySetKey(), redis.Z{
		Score:  float64(task.DelayTime.Unix()),
		Member: taskKey,
	})
	
	// 将任务数据存储到 Hash
	pipe.HSet(ctx, dq.getTaskDataKey(), taskKey, taskData)
	
	// 执行事务
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("添加延迟任务失败: %w", err)
	}
	
	return nil
}

// GetReadyTasks 获取已到期的任务
func (dq *DelayQueue) GetReadyTasks(ctx context.Context, limit int64) ([]*DelayTask, error) {
	now := time.Now().Unix()
	
	// 从 Sorted Set 中获取分数小于等于当前时间的任务键
	taskKeys, err := dq.client.ZRangeByScore(ctx, dq.getDelaySetKey(), &redis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(now, 10),
		Count: limit,
	}).Result()
	
	if err != nil {
		return nil, fmt.Errorf("获取就绪任务失败: %w", err)
	}
	
	if len(taskKeys) == 0 {
		return []*DelayTask{}, nil
	}
	
	// 获取任务数据
	taskDataList, err := dq.client.HMGet(ctx, dq.getTaskDataKey(), taskKeys...).Result()
	if err != nil {
		return nil, fmt.Errorf("获取任务数据失败: %w", err)
	}
	
	var tasks []*DelayTask
	for i, data := range taskDataList {
		if data == nil {
			continue
		}
		
		var task DelayTask
		if err := json.Unmarshal([]byte(data.(string)), &task); err != nil {
			fmt.Printf("反序列化任务失败 %s: %v\n", taskKeys[i], err)
			continue
		}
		
		tasks = append(tasks, &task)
	}
	
	return tasks, nil
}

// RemoveTask 移除已处理的任务
func (dq *DelayQueue) RemoveTask(ctx context.Context, task *DelayTask) error {
	taskKey := dq.generateTaskKey(task.QueueName, task.TaskName, task.DelayTime)
	
	// 使用事务删除
	pipe := dq.client.TxPipeline()
	pipe.ZRem(ctx, dq.getDelaySetKey(), taskKey)
	pipe.HDel(ctx, dq.getTaskDataKey(), taskKey)
	
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("移除任务失败: %w", err)
	}
	
	return nil
}

// GetQueueSize 获取延迟队列大小
func (dq *DelayQueue) GetQueueSize(ctx context.Context) (int64, error) {
	return dq.client.ZCard(ctx, dq.getDelaySetKey()).Result()
}

// GetTasksByQueue 按队列名获取任务
func (dq *DelayQueue) GetTasksByQueue(ctx context.Context, queueName string) ([]*DelayTask, error) {
	// 获取所有任务键
	allKeys, err := dq.client.ZRange(ctx, dq.getDelaySetKey(), 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("获取任务键失败: %w", err)
	}
	
	// 过滤指定队列的任务键
	var queueKeys []string
	for _, key := range allKeys {
		// 检查键是否以指定队列名开头
		if len(key) > len(queueName) && key[:len(queueName)] == queueName && key[len(queueName)] == ':' {
			queueKeys = append(queueKeys, key)
		}
	}
	
	if len(queueKeys) == 0 {
		return []*DelayTask{}, nil
	}
	
	// 获取任务数据
	taskDataList, err := dq.client.HMGet(ctx, dq.getTaskDataKey(), queueKeys...).Result()
	if err != nil {
		return nil, fmt.Errorf("获取任务数据失败: %w", err)
	}
	
	var tasks []*DelayTask
	for _, data := range taskDataList {
		if data == nil {
			continue
		}
		
		var task DelayTask
		if err := json.Unmarshal([]byte(data.(string)), &task); err != nil {
			continue
		}
		
		tasks = append(tasks, &task)
	}
	
	return tasks, nil
}
