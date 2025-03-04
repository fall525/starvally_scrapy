import time
from redis_manager import RedisManager
from db_manager import DBManager

# 解决一下心动缺失的问题
class MonitorManager:
    def __init__(self, host="127.0.0.1", port=6379, db=0):
        self.redis_client = RedisManager(host, port, db).redis_client
    
    ### 1. 获取爬虫进程状态 ###
    def get_active_pids(self):
        """获取所有活跃的爬虫进程ID"""
        return self.redis_client.smembers("active_crawlers")

    def get_crawler_status(self, pid):
        """获取指定爬虫进程的状态信息"""
        return self.redis_client.hgetall(f"crawler:status:{pid}")

    ### 2. 获取任务队列状态 ###
    def get_queue_size(self):
        """获取待爬取的URL数量"""
        return self.redis_client.llen("url_queue")

    def get_failed_queue_size(self):
        """获取失败的URL数量"""
        return self.redis_client.llen("failed_urls")
    
    def get_crawler_heartbeat(self, pid):
        """获取指定爬虫进程的最后心跳时间"""
        return self.redis_client.get(f"crawler:heartbeat:{pid}")

    ### 3. 任务状态统计 ###
    def get_all_status_count(self):
        """统计不同爬取状态的URL数量 (crawling, done, failed)"""
        status_count = {
            "crawling": 0,
            "done": 0,
            "failed": 0
        }
        all_status = self.redis_client.hgetall("url_status")
        for status in all_status.values():
            if status in status_count:
                status_count[status] += 1
        return status_count

monitor_manager = MonitorManager()
db_manager = DBManager()
HEARTBEAT_TIMEOUT = 20  # 心跳超时时间（秒）

def show_queue_status():
    """显示任务队列状态"""
    url_count = monitor_manager.get_queue_size()
    failed_count = monitor_manager.get_failed_queue_size()
    print(f"📥 待爬取 URL 数量: {url_count}, 失败 URL 数量: {failed_count}")

def show_db_status():
    """显示 MongoDB 数据库状态"""
    page_count = db_manager.count_pages()
    print(f"📚 已存储页面数量: {page_count}")

def show_url_status_count():
    """显示不同状态的URL数量"""
    status_count = monitor_manager.get_all_status_count()
    print(f"📊 URL 状态统计: 爬取中: {status_count['crawling']} | 已完成: {status_count['done']} | 失败: {status_count['failed']}")

def show_crawler_status():
    """显示所有爬虫进程的状态信息"""
    active_pids = monitor_manager.get_active_pids()
    print(f"\n🕵️‍♂️ 当前活跃爬虫进程: {len(active_pids)} 个")
    
    for pid in active_pids:
        status_info = monitor_manager.get_crawler_status(pid)
        last_heartbeat = monitor_manager.get_crawler_heartbeat(pid)
        if status_info:
            status = status_info.get('status')
            current_url = status_info.get('current_url')
            last_active_time = status_info.get('last_active_time')
            
            # 检测心跳超时
            if last_heartbeat:
                last_heartbeat = float(last_heartbeat)
                time_since_last_heartbeat = time.time() - last_heartbeat
                if time_since_last_heartbeat > HEARTBEAT_TIMEOUT:
                    status = "⚠️ 无响应"
            else:
                status = "❌ 心跳丢失"

            print(f"🐍 进程 {pid} | 状态: {status} | 当前URL: {current_url} | 上次活动: {last_active_time}")
        else:
            print(f"❓ 未找到进程 {pid} 的状态信息")

def monitor_status(interval=5):
    """主监控循环，每隔 interval 秒刷新一次状态信息"""
    try:
        while True:
            print("\n📊 正在刷新爬虫状态...")
            show_queue_status()
            show_db_status()
            show_url_status_count()
            show_crawler_status()
            time.sleep(interval)
    except KeyboardInterrupt:
        print("🛑 监控程序已退出")

if __name__ == "__main__":
    print("🚀 启动爬虫监控程序...")
    monitor_status()