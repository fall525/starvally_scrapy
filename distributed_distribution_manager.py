from redis_manager import RedisManager
from settings import URL_DISTRIBUTION_STRATEGY, DISTRIBUTED_NODE_COUNT,MACHINE_NUM,PRIORITY_THRESHOLD
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
            priority = self.get_url_priority(url)  # 让 URL 优先级获取方法更独立
            self.redis_client.zadd("url_queue_priority", {url: priority})
        elif URL_DISTRIBUTION_STRATEGY == "round_robin":
            node_id = hash(url) % DISTRIBUTED_NODE_COUNT
            self.redis_client.lpush(f"url_queue_round_robin:{node_id}", url)
        else:
            self.redis_client.lpush("url_queue", url)

        return url  # 仅用于调试输出

    def pop_url(self, node_id=0):
        """从最终任务队列获取 URL，按优先级分配"""

        if URL_DISTRIBUTION_STRATEGY == "priority":
            if MACHINE_NUM == 1:
                # 机器 1 处理优先级 >= 阈值的 URL
                urls = self.redis_client.zrangebyscore("url_queue_priority", PRIORITY_THRESHOLD , "+inf", start=0, num=1)
            else:
                # 其他机器处理优先级 < 阈值的 URL
                urls = self.redis_client.zrangebyscore("url_queue_priority", "-inf", PRIORITY_THRESHOLD , start=0, num=1)

            if urls:
                url = urls[0]
                self.redis_client.zrem("url_queue_priority", url)
                return url

        elif URL_DISTRIBUTION_STRATEGY == "round_robin":
            return self.redis_client.rpop(f"url_queue_round_robin:{node_id % DISTRIBUTED_NODE_COUNT}")
        else:
            return self.redis_client.rpop("url_queue")

        return None
