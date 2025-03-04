import redis

class RedisManager:
    def __init__(self, host="127.0.0.1", port=6379, db=0):
        self.redis_client = redis.StrictRedis(host=host, port=port, db=db, decode_responses=True)

    ### 📌 1️⃣ 任务队列 ###
    def push_url(self, url):
        """将新 URL 添加到任务队列"""
        if not self.is_visited(url):
            self.redis_client.lpush("url_queue", url)

    def pop_url(self):
        """获取任务队列中的 URL"""
        return self.redis_client.rpop("url_queue")

    def queue_size(self):
        """获取任务队列大小"""
        return self.redis_client.llen("url_queue")

    ### 📌 2️⃣ 去重机制 ###
    def is_visited(self, url):
        """判断 URL 是否已经爬取"""
        return self.redis_client.sismember("visited_urls", url)

    def mark_visited(self, url):
        """记录 URL 为已爬取"""
        self.redis_client.sadd("visited_urls", url)

    ### 📌 3️⃣ 任务状态管理 ###
    def set_status(self, url, status):
        """设置 URL 的爬取状态 (crawling, done, failed)"""
        self.redis_client.hset("url_status", url, status)

    def get_status(self, url):
        """获取 URL 当前的爬取状态"""
        return self.redis_client.hget("url_status", url)

    ### 📌 4️⃣ 失败重试机制 ###
    def push_failed_url(self, url):
        """将爬取失败的 URL 存入失败队列"""
        self.redis_client.lpush("failed_urls", url)

    def get_failed_urls(self):
        """获取所有爬取失败的 URL"""
        return self.redis_client.lrange("failed_urls", 0, -1)
