
from redis_manager import RedisManager
from settings import URL_DISTRIBUTION_STRATEGY, DISTRIBUTED_NODE_COUNT

class DistributedDistributionManager(RedisManager):
    """分布式 URL 分发管理器，先存入总任务队列，再分配"""

    def push_url_to_master(self, url):
        """所有 URL 统一先存入 `url_queue`"""
        if not self.is_visited(url):
            self.redis_client.lpush("url_queue", url)

    def distribute_url(self):
        """从 `url_queue` 取出 URL，并根据策略分配"""
        url = self.redis_client.rpop("url_queue")
        if not url:
            return None
        
        if URL_DISTRIBUTION_STRATEGY == "priority":
            self.redis_client.zadd("url_queue_priority", {url: 10})  # 默认优先级 10
        elif URL_DISTRIBUTION_STRATEGY == "round_robin":
            node_id = hash(url) % DISTRIBUTED_NODE_COUNT
            self.redis_client.lpush(f"url_queue_round_robin:{node_id}", url)
        else:
            self.redis_client.lpush("url_queue", url)

        return url  # 只是为了打印调试

    def pop_url(self, node_id=0):
        """从最终任务队列获取 URL"""
        if URL_DISTRIBUTION_STRATEGY == "priority":
            urls = self.redis_client.zrange("url_queue_priority", 0, 0)
            if urls:
                url = urls[0]
                self.redis_client.zrem("url_queue_priority", url)
                return url
        elif URL_DISTRIBUTION_STRATEGY == "round_robin":
            return self.redis_client.rpop(f"url_queue_round_robin:{node_id % DISTRIBUTED_NODE_COUNT}")
        else:
            return self.redis_client.rpop("url_queue")

        return None
