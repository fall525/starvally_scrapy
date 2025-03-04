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
    "https://zh.stardewvalleywiki.com/Stardew_Valley_Wiki"
]

# 爬虫任务调度
CRAWLER_SLEEP_TIME = 1  # 每个任务之间的休眠时间（秒）
CRAWLER_RETRY_DELAY = 5  # 任务失败后的重试延迟（秒）

# 日志配置
LOG_FILE = "crawler.log"
LOG_LEVEL = "INFO"  # 可选: DEBUG, INFO, WARNING, ERROR, CRITICAL
