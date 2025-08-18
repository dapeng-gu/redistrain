# Redis 基础数据结构深入学习

## 学习目标
通过分析 asynq 的实现，深入理解 Redis 五种基本数据结构在任务队列中的应用。

## 核心概念

### 1. asynq 的 Redis 键设计模式

基于 asynq 源码分析，它使用以下键命名规范：

```
asynq:{queue_name}:pending     # List - 待处理任务队列
asynq:{queue_name}:active      # List - 正在处理的任务
asynq:{queue_name}:scheduled   # Sorted Set - 延时任务（按执行时间排序）
asynq:{queue_name}:retry       # Sorted Set - 重试任务（按重试时间排序）
asynq:{queue_name}:archived    # Sorted Set - 已归档任务
asynq:{queue_name}:t:{task_id} # Hash - 任务详细信息
asynq:{queue_name}:deadlines   # Sorted Set - 任务超时管理
```

### 2. 数据结构选择的原因

**为什么用 List 存储 pending 任务？**
- FIFO 特性，天然支持队列语义
- `LPUSH` + `BRPOP` 实现高效的生产者-消费者模式
- 原子操作，保证任务不丢失

**为什么用 Sorted Set 存储 scheduled 任务？**
- 按时间戳排序，方便查找到期任务
- `ZRANGEBYSCORE` 可以高效查询指定时间范围的任务
- 支持分数（时间戳）的范围操作

**为什么用 Hash 存储任务详情？**
- 结构化存储任务的多个字段
- 支持部分字段更新，避免序列化整个对象
- 内存效率高

## 实践练习

### 练习 1：基础数据结构操作
### 练习 2：模拟 asynq 的任务存储
### 练习 3：实现简单的任务队列

## 学习检查点

- [ ] 理解每种数据结构的特点和适用场景
- [ ] 掌握 asynq 的键设计模式
- [ ] 完成所有实践练习
- [ ] 能够解释数据结构选择的原因
