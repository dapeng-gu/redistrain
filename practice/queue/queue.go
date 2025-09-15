package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"practice/taskstruct"

	"practice/redisengine"

	"github.com/redis/go-redis/v9"
)

type Queue struct {
	name                           string
	redisEngine                    *redisengine.RedisEngine
	queue_type                     string
	enqueueScript                  *redis.Script
	dequeueScript                  *redis.Script
	enableDeduplication            bool
	deduplicationTTL               int
	enqueueWithDeduplicationScript *redis.Script
	aggregator                     *Aggregator
}

func NewQueue(name string, redisEngine *redisengine.RedisEngine) *Queue {
	return &Queue{
		name:          name,
		redisEngine:   redisEngine,
		queue_type:    "queue",
		enqueueScript: enqueueScript,
		dequeueScript: dequeueScript,
		aggregator:    NewAggregator(redisEngine, name),
	}
}

func NewQueueWithDeduplication(name string, redisEngine *redisengine.RedisEngine, ttl int) *Queue {
	return &Queue{
		name:                           name,
		redisEngine:                    redisEngine,
		queue_type:                     "queue",
		enqueueScript:                  enqueueScript,
		dequeueScript:                  dequeueScript,
		enableDeduplication:            true,
		deduplicationTTL:               ttl,
		enqueueWithDeduplicationScript: enqueueWithDeduplicationScript,
		aggregator:                     NewAggregator(redisEngine, name),
	}
}

func (q *Queue) GetQueueKey() string {
	return fmt.Sprintf("%s:%s", q.queue_type, q.name)
}

func (q *Queue) EnqueueTask(ctx context.Context, task *taskstruct.Task) error {
	taskKey := task.GetTaskKey()
	queueKey := q.GetQueueKey()

	taskData, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("序列化任务失败: %w", err)
	}

	if q.enableDeduplication {
		deduplicationKey := task.GetDeduplicationKey()
		result, err := q.redisEngine.RunScript(ctx, q.enqueueWithDeduplicationScript,
			[]string{q.redisEngine.GetName(), queueKey, deduplicationKey},
			taskKey, taskData, task.ID, q.deduplicationTTL)
		if err != nil {
			return fmt.Errorf("任务入队失败: %w", err)
		}

		switch result.(int64) {
		case 0:
			fmt.Printf("任务入队失败，任务已存在: %s\n", task.ID)
			return nil
		case -1:
			fmt.Printf("任务入队失败，相同任务正在处理中（去重）: %s\n", task.ID)
			return nil
		case 1:
			fmt.Printf("任务成功入队（已设置去重键）: %s\n", task.ID)
			return nil
		}
	} else {
		result, err := q.redisEngine.RunScript(ctx, q.enqueueScript, []string{q.redisEngine.GetName(), queueKey}, taskKey, taskData, task.ID)
		if err != nil {
			return fmt.Errorf("任务入队失败: %w", err)
		}

		if result.(int64) == 0 {
			fmt.Printf("任务入队失败，任务已存在: %s\n", task.ID)
			return nil
		}
	}

	return nil
}

// EnqueueGroupedTask 入队支持聚合的任务
func (q *Queue) EnqueueGroupedTask(ctx context.Context, task *taskstruct.Task) error {
	if task.GroupKey == "" {
		return q.EnqueueTask(ctx, task)
	}

	aggregatedTask, err := q.aggregator.AddTaskToGroup(ctx, task)
	if err != nil {
		return fmt.Errorf("聚合任务失败: %w", err)
	}

	// 如果返回了聚合任务，说明触发了批处理
	if aggregatedTask != nil {
		return q.EnqueueTask(ctx, aggregatedTask)
	}

	return nil
}

func (q *Queue) DequeueTask(ctx context.Context) (*taskstruct.Task, error) {
	taskID, err := q.redisEngine.RPop(ctx, q.GetQueueKey())
	if err != nil {
		if err == redis.Nil {
			fmt.Printf("%s 没有就绪任务\n", q.GetQueueKey())
			return nil, nil
		}
		return nil, err
	}
	task := taskstruct.Task{ID: taskID}

	taskKey := task.GetTaskKey()

	taskData, err := q.redisEngine.RunScript(ctx, q.dequeueScript, []string{q.redisEngine.GetName(), q.GetQueueKey()}, taskKey)
	if err != nil {
		if err == redis.Nil {
			fmt.Printf("%s 没有找到任务\n", taskKey)
			return nil, nil
		}
		return nil, err
	}

	if err := json.Unmarshal([]byte(taskData.(string)), &task); err != nil {
		return nil, err
	}
	return &task, nil
}
