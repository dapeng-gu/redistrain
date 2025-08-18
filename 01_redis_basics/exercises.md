# Redis 基础数据结构练习题

## 练习说明
完成以下练习来巩固对 Redis 数据结构的理解。每个练习都有明确的目标和验收标准。

## 练习 1: Redis 命令行操作

### 目标
熟练掌握 Redis 命令行操作，理解各种数据结构的特点。

### 任务
1. 启动 Redis 服务器和客户端
2. 完成以下命令操作：

```bash
# String 操作
SET task:counter 0
INCR task:counter
INCRBY task:counter 10
GET task:counter
EXPIRE task:counter 3600
TTL task:counter

# List 操作 - 模拟任务队列
LPUSH queue:emails "task1" "task2" "task3"
LLEN queue:emails
LRANGE queue:emails 0 -1
RPOP queue:emails
BRPOP queue:emails 5

# Hash 操作 - 存储任务详情
HMSET task:123 id "123" type "email" status "pending" created_at 1640995200
HGET task:123 status
HGETALL task:123
HINCRBY task:123 retry 1
HSET task:123 status "completed"

# Sorted Set 操作 - 延时队列
ZADD queue:scheduled 1640995800 "task1" 1640996400 "task2"
ZRANGE queue:scheduled 0 -1 WITHSCORES
ZRANGEBYSCORE queue:scheduled -inf 1640995900
ZREM queue:scheduled "task1"

# Set 操作 - 任务去重
SADD processed:tasks "task1" "task2" "task1"
SCARD processed:tasks
SISMEMBER processed:tasks "task1"
```

### 验收标准
- [ ] 能够熟练使用所有基本命令
- [ ] 理解每种数据结构的使用场景
- [ ] 能够解释为什么选择特定的数据结构

## 练习 2: 分析 asynq 的键设计

### 目标
深入理解 asynq 的 Redis 键设计模式和数据结构选择。

### 任务
1. 分析以下 asynq 键的作用：
   - `asynq:{queue}:pending`
   - `asynq:{queue}:active` 
   - `asynq:{queue}:scheduled`
   - `asynq:{queue}:retry`
   - `asynq:{queue}:archived`
   - `asynq:{queue}:t:{task_id}`

2. 回答以下问题：
   - 为什么 pending 和 active 使用 List 而不是 Set？
   - 为什么 scheduled 和 retry 使用 Sorted Set？
   - Hash 存储任务详情有什么优势？
   - 如何保证任务不丢失？

### 验收标准
- [ ] 能够准确解释每个键的作用
- [ ] 理解数据结构选择的原因
- [ ] 能够设计类似的键结构

## 练习 3: 实现简单的任务队列

### 目标
使用 Redis 基础数据结构实现一个简单的任务队列系统。

### 任务
实现以下功能：
1. 任务入队（支持立即执行和延时执行）
2. 任务出队（支持阻塞和非阻塞）
3. 任务状态管理（pending, active, completed, failed）
4. 任务重试机制
5. 任务优先级支持

### 代码框架
```go
type SimpleTaskQueue struct {
    client *redis.Client
}

func (q *SimpleTaskQueue) Enqueue(task Task) error {
    // TODO: 实现任务入队
}

func (q *SimpleTaskQueue) EnqueueDelayed(task Task, delay time.Duration) error {
    // TODO: 实现延时任务入队
}

func (q *SimpleTaskQueue) Dequeue(queue string) (*Task, error) {
    // TODO: 实现任务出队
}

func (q *SimpleTaskQueue) CompleteTask(taskID string) error {
    // TODO: 实现任务完成
}

func (q *SimpleTaskQueue) RetryTask(taskID string) error {
    // TODO: 实现任务重试
}
```

### 验收标准
- [ ] 所有功能正常工作
- [ ] 任务不会丢失
- [ ] 支持并发操作
- [ ] 代码结构清晰

## 练习 4: 性能测试

### 目标
测试不同数据结构操作的性能特点。

### 任务
1. 测试 List 的 LPUSH/RPOP 性能
2. 测试 Sorted Set 的 ZADD/ZRANGE 性能
3. 测试 Hash 的 HSET/HGET 性能
4. 比较不同数据结构在大量数据下的表现

### 测试代码示例
```go
func BenchmarkListOperations(b *testing.B) {
    // TODO: 实现 List 性能测试
}

func BenchmarkSortedSetOperations(b *testing.B) {
    // TODO: 实现 Sorted Set 性能测试
}

func BenchmarkHashOperations(b *testing.B) {
    // TODO: 实现 Hash 性能测试
}
```

### 验收标准
- [ ] 完成所有性能测试
- [ ] 分析性能差异的原因
- [ ] 给出使用建议

## 练习 5: 错误处理和边界情况

### 目标
处理各种错误情况和边界条件。

### 任务
测试并处理以下情况：
1. Redis 连接断开
2. 队列为空时的出队操作
3. 重复的任务 ID
4. 内存不足的情况
5. 大量并发操作

### 验收标准
- [ ] 所有错误情况都有适当的处理
- [ ] 系统在异常情况下保持稳定
- [ ] 有完善的日志记录

## 总结问题

完成所有练习后，请回答：

1. **数据结构选择**：在什么情况下选择 List、Hash、Sorted Set？
2. **性能考虑**：哪些操作是 O(1)，哪些是 O(log N)？
3. **内存使用**：不同数据结构的内存效率如何？
4. **并发安全**：Redis 的哪些操作是原子的？
5. **扩展性**：如何设计可扩展的键结构？

## 下一步

完成这些练习后，你将具备：
- 深入理解 Redis 数据结构
- 能够分析和设计键结构
- 掌握任务队列的基本实现
- 为学习高级特性做好准备

准备好进入下一阶段：**Redis 高级特性学习**（Lua 脚本、事务、发布订阅等）。
