# 🎯 Redis任务存储编码练习指南

## 📋 你需要编码的9个TODO任务

### 🔥 核心功能区域（需要你编码）

#### TODO 1: 任务序列化
**位置**: `EnqueueTask` 方法
**任务**: 将Task结构体序列化为JSON格式
```go
// 提示代码：
taskData, err := json.Marshal(task)
if err != nil {
    return fmt.Errorf("序列化任务失败: %w", err)
}
```

#### TODO 2: 存储任务到Redis Hash
**位置**: `EnqueueTask` 方法  
**任务**: 使用HSET命令存储任务数据
```go
// 提示代码：
taskKey := fmt.Sprintf("task:%s", task.ID)
if err := ts.client.HSet(ctx, taskKey, "data", taskData).Err(); err != nil {
    return fmt.Errorf("存储任务数据失败: %w", err)
}
```

#### TODO 3: 任务ID入队
**位置**: `EnqueueTask` 方法
**任务**: 使用LPUSH将任务ID推入队列
```go
// 提示代码：
queueKey := fmt.Sprintf("queue:%s", task.Queue)
if err := ts.client.LPush(ctx, queueKey, task.ID).Err(); err != nil {
    return fmt.Errorf("任务入队失败: %w", err)
}
```

#### TODO 4: 从队列弹出任务ID
**位置**: `DequeueTask` 方法
**任务**: 使用RPOP从队列右端弹出任务ID
```go
// 提示代码：
taskID, err := ts.client.RPop(ctx, queueKey).Result()
if err == redis.Nil {
    log.Printf("📭 队列为空: %s", queueName)
    return nil, nil // 队列为空
}
if err != nil {
    return nil, fmt.Errorf("任务出队失败: %w", err)
}
```

#### TODO 5: 获取任务数据
**位置**: `DequeueTask` 方法
**任务**: 从Redis Hash获取任务数据
```go
// 提示代码：
taskKey := fmt.Sprintf("task:%s", taskID)
taskData, err := ts.client.HGet(ctx, taskKey, "data").Result()
if err != nil {
    return nil, fmt.Errorf("获取任务数据失败: %w", err)
}
```

#### TODO 6: 反序列化任务
**位置**: `DequeueTask` 方法
**任务**: 将JSON数据反序列化为Task结构体
```go
// 提示代码：
var task Task
if err := json.Unmarshal([]byte(taskData), &task); err != nil {
    return nil, fmt.Errorf("反序列化任务失败: %w", err)
}
```

#### TODO 7: 获取队列长度
**位置**: `GetQueueLength` 方法
**任务**: 使用LLEN获取队列长度
```go
// 提示代码：
queueKey := fmt.Sprintf("queue:%s", queueName)
return ts.client.LLen(ctx, queueKey).Result()
```

#### TODO 8: 获取所有队列键
**位置**: `ListQueues` 方法
**任务**: 使用KEYS命令获取所有队列
```go
// 提示代码：
keys, err := ts.client.Keys(ctx, "queue:*").Result()
if err != nil {
    return nil, err
}
```

#### TODO 9: 移除队列前缀
**位置**: `ListQueues` 方法
**任务**: 从键名中提取队列名
```go
// 提示代码：
queues[i] = key[6:] // 移除 "queue:" 前缀
```

## 🚀 编码步骤建议

### 第一阶段：基础存储功能
1. 完成 TODO 1-3（EnqueueTask方法）
2. 测试任务入队功能

### 第二阶段：读取功能
3. 完成 TODO 4-6（DequeueTask方法）
4. 测试任务出队功能

### 第三阶段：辅助功能
5. 完成 TODO 7-9（队列管理功能）
6. 测试完整流程

## 🧪 测试方法

### 运行练习版本
```bash
# 设置Redis密码
set REDIS_PASSWORD=123456

# 运行练习版本
go run practice_main.go
```

### 验证功能
程序会自动验证你编码的功能：
- ✅ 任务序列化和存储
- ✅ 队列操作正确性
- ✅ 数据完整性检查
- ✅ 任务出队和反序列化

## 📚 Redis命令参考

| 功能 | Redis命令 | Go方法 | 说明 |
|------|-----------|--------|------|
| 存储Hash | HSET | `HSet(ctx, key, field, value)` | 存储任务数据 |
| 获取Hash | HGET | `HGet(ctx, key, field)` | 获取任务数据 |
| 左推入队列 | LPUSH | `LPush(ctx, key, value)` | 任务入队 |
| 右弹出队列 | RPOP | `RPop(ctx, key)` | 任务出队 |
| 队列长度 | LLEN | `LLen(ctx, key)` | 获取队列长度 |
| 匹配键 | KEYS | `Keys(ctx, pattern)` | 查找匹配的键 |

## 🎯 学习目标检查

完成编码后，你应该能够：
- [ ] 理解Redis Hash和List数据结构的使用
- [ ] 掌握JSON序列化/反序列化
- [ ] 熟悉Redis Go客户端的基本操作
- [ ] 理解任务队列的FIFO特性
- [ ] 掌握错误处理的最佳实践

## 💡 进阶思考

编码完成后，思考这些问题：
1. 如果HSET成功但LPUSH失败，会发生什么？
2. 如何保证任务入队的原子性？
3. 多个worker同时出队时会有什么问题？
4. 如何优化Redis内存使用？

## 🔗 参考资源

- **asynq源码**: `asynq/internal/rdb/rdb.go`
- **Redis文档**: https://redis.io/commands
- **Go Redis客户端**: https://github.com/redis/go-redis

---

**记住**: 这是一个渐进式学习过程，先让代码跑起来，再逐步优化和深入理解！
