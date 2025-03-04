import os
import time
from distributed_distribution_manager import DistributedDistributionManager
from db_manager import DBManager
import settings


db_manager = DBManager(
    host=settings.MONGODB_HOST,
    port=settings.MONGODB_PORT,
    db_name=settings.MONGODB_DB_NAME,
    col_name=settings.MONGODB_COLL_NAME
)

url_manager = DistributedDistributionManager(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB
)

# 推送种子 URL 到 Redis
def push_seed_urls(seed_urls):
    if url_manager.queue_size() == 0:
        for url in seed_urls:
            if not url_manager.is_visited(url):
                url_manager.push_url(url)
                print(f"🌱 推送种子 URL: {url}")
            else:
                print(f"🚫 URL 已经存在: {url}")
    else:
        print("📥 Redis 队列中已有任务，无需推送种子 URL")

# 启动爬虫程序
def start_crawler():
    # 通过命令行启动爬虫程序，这里假设爬虫程序是 crawler.py
    try:
        print("🚀 启动爬虫程序...")
        os.system("python crawler.py")
    except Exception as e:
        print(f"❌ 启动爬虫失败: {e}")


def main():
    print("🚀 启动分布式爬虫系统")
    push_seed_urls(settings.SEED_URLS)
    start_crawler()

    # 简单监控
    while True:
        time.sleep(10)
        url_count = url_manager.queue_size()
        page_count = db_manager.count_pages()
        print(f"📊 当前待爬取URL数量: {url_count}, 已存储页面数量: {page_count}")

if __name__ == "__main__":
    main()
