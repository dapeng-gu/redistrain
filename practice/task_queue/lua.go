package task_queue

import "github.com/redis/go-redis/v9"

// enqueueScript 原子性入队Lua脚本
var enqueueScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
    return 0  -- 任务已存在，返回0
end
redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
redis.call("LPUSH", KEYS[2], ARGV[3])

return 1  -- 成功入队，返回1
`)
