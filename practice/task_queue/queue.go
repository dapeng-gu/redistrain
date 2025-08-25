package task_queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Task struct {
	ID       string                 `json:"id"`
	Type     string                 `json:"type"`
	Payload  map[string]interface{} `json:"payload"`
	MaxRetry int                    `json:"max_retry"`
	Queue    string                 `json:"queue"`
	Created  time.Time              `json:"created"`
}

// TaskStorage Redis任务存储器
type TaskStorage struct {
	client redis.UniversalClient
}

// NewTaskStorage 创建新的任务存储器
func NewTaskStorage(client redis.UniversalClient) *TaskStorage {
	return &TaskStorage{client: client}
}

func (ts *TaskStorage) getTaskStorageKey(queueName string) string {
	return fmt.Sprintf("queue_task:%s", queueName)
}

func (ts *TaskStorage) getTaskKey(ID string) string {
	return fmt.Sprintf("task:%s", ID)
}

func (ts *TaskStorage) getQueueKey(queueName string) string {
	return fmt.Sprintf("queue:%s", queueName)
}

func (ts *TaskStorage) EnqueueTask(ctx context.Context, task *Task) error {
	taskKey := ts.getTaskKey(task.ID)
	queueKey := ts.getQueueKey(task.Queue)

	taskData, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("序列化任务失败: %w", err)
	}

	// 使用Lua脚本确保原子性
	result, err := enqueueScript.Run(ctx, ts.client, []string{ts.getTaskStorageKey(task.Queue), queueKey}, taskKey, taskData, task.ID).Result()
	if err != nil {
		return fmt.Errorf("任务入队失败: %w", err)
	}

	if result.(int64) == 0 {
		return fmt.Errorf("任务已存在: %s", task.ID)
	}

	return nil
}

func (ts *TaskStorage) DequeueTask(ctx context.Context, queueName string) (*Task, error) {
	queueKey := ts.getQueueKey(queueName)
	taskID, err := ts.client.RPop(ctx, queueKey).Result()
	if err != nil {
		return nil, err
	}

	taskKey := ts.getTaskKey(taskID)

	taskData, err := ts.client.HGet(ctx, ts.getTaskStorageKey(queueName), taskKey).Result()
	if err != nil {
		return nil, err
	}

	var task Task
	if err := json.Unmarshal([]byte(taskData), &task); err != nil {
		return nil, err
	}
	return &task, nil
}
