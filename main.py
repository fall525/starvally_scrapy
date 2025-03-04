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


# 启动爬虫程序
def start_crawler():
    try:
        print("🚀 启动爬虫程序...")
        os.system("python crawler_dis.py")
    except Exception as e:
        print(f"❌ 启动爬虫失败: {e}")


def main():
    print("🚀 启动分布式爬虫系统")
    # push_seed_urls(settings.SEED_URLS)
    start_crawler()

    # 简单监控
    while True:
        time.sleep(10)
        url_count = url_manager.queue_size()
        page_count = db_manager.count_pages()
        print(f"📊 当前待爬取URL数量: {url_count}, 已存储页面数量: {page_count}")

if __name__ == "__main__":
    main()
