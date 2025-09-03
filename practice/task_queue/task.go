package task_queue

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
