package queue

import (
	"fmt"
	"time"
)

type Task struct {
	ID       string                 `json:"id"`        // 任务ID
	Type     string                 `json:"type"`      // 任务类型
	Payload  map[string]interface{} `json:"payload"`   // 任务负载
	MaxRetry int                    `json:"max_retry"` // 最大重试次数
	Created  time.Time              `json:"created"`   // 创建时间
}

func (t *Task) getTaskKey() string {
	return fmt.Sprintf("task:%s", t.ID)
}

func ProgressTask(task *Task) {
	fmt.Printf("任务%s 处理中 \n", task.ID)
}

func CreateTask(mark string, count int) []Task {
	tasks := []Task{}
	for i := 0; i < count; i++ {
		tasks = append(tasks, Task{
			ID:       fmt.Sprintf("task_%s_%d", mark, i),
			Type:     "test",
			Payload:  map[string]interface{}{"test01": "test01", "test02": "test02"},
			MaxRetry: 3,
			Created:  time.Now(),
		})
	}
	return tasks
}
