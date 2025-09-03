package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func testDelayQueue() {
	// 创建 Redis 客户端
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	
	// 测试连接
	ctx := context.Background()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		fmt.Printf("Redis 连接失败: %v\n", err)
		return
	}
	
	// 创建延迟队列
	dq := NewDelayQueue(client)
	
	fmt.Println("=== 延迟队列测试 ===")
	
	// 创建测试任务
	now := time.Now()
	task := &DelayTask{
		QueueName: "test",
		TaskName:  "hello",
		Payload:   map[string]interface{}{"message": "Hello World"},
		DelayTime: now.Add(3 * time.Second),
		Created:   now,
	}
	
	// 添加任务
	err = dq.AddDelayTask(ctx, task)
	if err != nil {
		fmt.Printf("添加任务失败: %v\n", err)
		return
	}
	
	taskKey := dq.generateTaskKey(task.QueueName, task.TaskName, task.DelayTime)
	fmt.Printf("✓ 任务已添加: %s\n", taskKey)
	
	// 等待任务就绪
	fmt.Println("等待任务就绪...")
	for i := 0; i < 5; i++ {
		time.Sleep(1 * time.Second)
		
		readyTasks, err := dq.GetReadyTasks(ctx, 1)
		if err != nil {
			fmt.Printf("获取任务失败: %v\n", err)
			continue
		}
		
		if len(readyTasks) > 0 {
			fmt.Printf("✓ 任务就绪: %+v\n", readyTasks[0])
			
			// 移除任务
			err = dq.RemoveTask(ctx, readyTasks[0])
			if err != nil {
				fmt.Printf("移除任务失败: %v\n", err)
			} else {
				fmt.Println("✓ 任务已移除")
			}
			break
		} else {
			fmt.Printf("第 %d 秒: 任务未就绪\n", i+1)
		}
	}
	
	fmt.Println("测试完成")
}

func main() {
	testDelayQueue()
}
