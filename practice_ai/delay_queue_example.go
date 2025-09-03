package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	// 创建 Redis 客户端
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	
	// 创建延迟队列
	dq := NewDelayQueue(client)
	ctx := context.Background()
	
	fmt.Println("=== 延迟队列示例 ===")
	
	// 添加延迟任务
	addDelayTasks(ctx, dq)
	
	// 模拟处理延迟任务
	processDelayTasks(ctx, dq)
}

// addDelayTasks 添加延迟任务
func addDelayTasks(ctx context.Context, dq *DelayQueue) {
	now := time.Now()
	
	// 创建不同延迟时间的任务
	tasks := []*DelayTask{
		{
			QueueName: "email",
			TaskName:  "welcome_email",
			Payload:   map[string]interface{}{"user_id": 1001, "email": "user1@example.com"},
			DelayTime: now.Add(5 * time.Second),
			Created:   now,
		},
		{
			QueueName: "sms",
			TaskName:  "verification_code",
			Payload:   map[string]interface{}{"phone": "13800138000", "code": "123456"},
			DelayTime: now.Add(10 * time.Second),
			Created:   now,
		},
		{
			QueueName: "email",
			TaskName:  "reminder_email",
			Payload:   map[string]interface{}{"user_id": 1002, "type": "payment_due"},
			DelayTime: now.Add(15 * time.Second),
			Created:   now,
		},
		{
			QueueName: "notification",
			TaskName:  "push_notification",
			Payload:   map[string]interface{}{"user_id": 1003, "message": "您有新消息"},
			DelayTime: now.Add(3 * time.Second),
			Created:   now,
		},
	}
	
	// 添加任务到延迟队列
	for _, task := range tasks {
		err := dq.AddDelayTask(ctx, task)
		if err != nil {
			fmt.Printf("添加任务失败: %v\n", err)
			continue
		}
		
		taskKey := dq.generateTaskKey(task.QueueName, task.TaskName, task.DelayTime)
		fmt.Printf("✓ 添加延迟任务: %s (延迟 %.0f 秒)\n", 
			taskKey, task.DelayTime.Sub(time.Now()).Seconds())
	}
	
	// 显示队列大小
	size, _ := dq.GetQueueSize(ctx)
	fmt.Printf("\n当前队列大小: %d\n\n", size)
}

// processDelayTasks 处理延迟任务
func processDelayTasks(ctx context.Context, dq *DelayQueue) {
	fmt.Println("开始监听延迟任务...")
	
	// 模拟任务处理循环
	for i := 0; i < 20; i++ {
		fmt.Printf("\n--- 第 %d 次检查 (时间: %s) ---\n", 
			i+1, time.Now().Format("15:04:05"))
		
		// 获取就绪的任务
		readyTasks, err := dq.GetReadyTasks(ctx, 10)
		if err != nil {
			fmt.Printf("获取就绪任务失败: %v\n", err)
			time.Sleep(1 * time.Second)
			continue
		}
		
		if len(readyTasks) == 0 {
			fmt.Println("暂无就绪任务")
		} else {
			fmt.Printf("发现 %d 个就绪任务:\n", len(readyTasks))
			
			// 处理每个就绪任务
			for _, task := range readyTasks {
				processTask(task)
				
				// 移除已处理的任务
				err := dq.RemoveTask(ctx, task)
				if err != nil {
					fmt.Printf("移除任务失败: %v\n", err)
				} else {
					taskKey := dq.generateTaskKey(task.QueueName, task.TaskName, task.DelayTime)
					fmt.Printf("✓ 任务已移除: %s\n", taskKey)
				}
			}
		}
		
		// 显示当前队列大小
		size, _ := dq.GetQueueSize(ctx)
		fmt.Printf("剩余任务数: %d\n", size)
		
		// 如果队列为空，结束循环
		if size == 0 {
			fmt.Println("\n所有任务处理完成！")
			break
		}
		
		time.Sleep(1 * time.Second)
	}
	
	// 演示按队列查询
	fmt.Println("\n=== 按队列查询示例 ===")
	emailTasks, _ := dq.GetTasksByQueue(ctx, "email")
	fmt.Printf("email 队列剩余任务数: %d\n", len(emailTasks))
}

// processTask 模拟处理任务
func processTask(task *DelayTask) {
	fmt.Printf("🔄 处理任务: %s:%s\n", task.QueueName, task.TaskName)
	fmt.Printf("   数据: %+v\n", task.Payload)
	fmt.Printf("   预定时间: %s\n", task.DelayTime.Format("15:04:05"))
	fmt.Printf("   实际处理时间: %s\n", time.Now().Format("15:04:05"))
	
	// 模拟任务处理时间
	time.Sleep(100 * time.Millisecond)
}
