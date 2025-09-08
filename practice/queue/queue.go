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
	name          string
	redisEngine   *redisengine.RedisEngine
	queue_type    string
	enqueueScript *redis.Script
	dequeueScript *redis.Script
}

func NewQueue(name string, redisEngine *redisengine.RedisEngine) *Queue {
	return &Queue{
		name:          name,
		redisEngine:   redisEngine,
		queue_type:    "queue",
		enqueueScript: enqueueScript,
		dequeueScript: dequeueScript,
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

	result, err := q.redisEngine.RunScript(ctx, q.enqueueScript, []string{q.redisEngine.GetName(), queueKey}, taskKey, taskData, task.ID)
	if err != nil {
		return fmt.Errorf("任务入队失败: %w", err)
	}

	if result.(int64) == 0 {
		fmt.Println("任务入队失败，任务已存在: ", task.ID)
		return nil
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
