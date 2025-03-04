# settings.py

# Redis 配置
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB = 0

# MongoDB 配置
MONGODB_HOST = "127.0.0.1"
MONGODB_PORT = 27017
MONGODB_DB_NAME = "vally"
MONGODB_COLL_NAME="vally1"

# 爬虫配置
SEED_URLS = [
    "https://stardewvalleywiki.com/Stardew_Valley_Wiki",
    "https://stardewvalleywiki.com/Modding_talk:Mod_compatibility",
    "https://stardewvalleywiki.com/User_talk:Margotbean/Archive_2024",
    "https://stardewvalleywiki.com/Emily"
]

# 爬虫任务调度
CRAWLER_SLEEP_TIME = 1  # 每个任务之间的休眠时间（秒）
CRAWLER_RETRY_DELAY = 5  # 任务失败后的重试延迟（秒）


# 分布式 URL 分发策略
# "fifo" (先进先出), "priority" (优先级队列), "round_robin" (轮询分配)
URL_DISTRIBUTION_STRATEGY = "round_robin"
MACHINE_NUM=1
PRIORITY_THRESHOLD=4
# URL 优先级规则
PRIORITY_RULES = {
    1: [r"/User_talk"],  # 最高优先级
    2: [r"/Abigail", r"/Emily",r"/Penny"],  # 高优先级
    3: [r"/Modding_tal"],  # 中等优先级
}

# 配置分布式节点数 (仅对轮询分配策略生效)
DISTRIBUTED_NODE_COUNT = 2