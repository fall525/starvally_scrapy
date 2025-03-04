import redis
import time
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
    def failed_queue_size(self):
        """获取任务队列大小"""
        return self.redis_client.llen("failed_urls")
    
    ### 📌 5️⃣ 爬虫进程状态管理 ###
    def add_active_crawler(self, pid):
        """将爬虫进程 ID 添加到活跃进程集合中"""
        self.redis_client.sadd("active_crawlers", pid)

    def remove_active_crawler(self, pid):
        """从活跃进程集合中移除指定爬虫进程 ID"""
        self.redis_client.srem("active_crawlers", pid)

    def get_active_crawlers(self):
        """获取所有活跃的爬虫进程 ID"""
        return self.redis_client.smembers("active_crawlers")

    
    def set_crawler_status(self, pid, status_info):
        """设置指定爬虫进程的状态信息（兼容旧版本 Redis）"""
        if isinstance(status_info, dict):
            for key, value in status_info.items():
                self.redis_client.hset(f"crawler:status:{pid}", key, value)
        else:
            print(f"❌ 状态信息格式错误，期望 dict，得到: {type(status_info)}")


    def get_crawler_status(self, pid):
        """获取指定爬虫进程的状态信息"""
        return self.redis_client.hgetall(f"crawler:status:{pid}")

    def clear_crawler_status(self, pid):
        """清除指定爬虫进程的状态信息"""
        self.redis_client.delete(f"crawler:status:{pid}")

    def send_heartbeat(self, pid, expire_time=30):
        """发送爬虫进程的心跳信号到 Redis"""
        self.redis_client.set(f"crawler:heartbeat:{pid}", time.time(), ex=expire_time)
