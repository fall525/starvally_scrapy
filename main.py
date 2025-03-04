import os
import time
import re
import requests
from bs4 import BeautifulSoup
import settings
from distributed_distribution_manager import DistributedDistributionManager
from db_manager import DBManager

# 获取当前爬虫进程的 ID 和节点 ID (用于轮询分配策略)
pid = os.getpid()

# 设置 User-Agent，防止封禁
HEADERS = {"User-Agent": "Mozilla/5.0"}

# 初始化数据库和分布式管理器
db_manager = DBManager(
    host=settings.MONGODB_HOST,
    port=settings.MONGODB_PORT,
    db_name=settings.MONGODB_DB_NAME,
    col_name=settings.MONGODB_COLL_NAME
)

distribution_manager = DistributedDistributionManager(
    host=settings.REDIS_HOST,
    port=settings.REDIS_PORT,
    db=settings.REDIS_DB
)

# 推送种子 URL 到 Redis
def push_seed_urls():
    if distribution_manager.queue_size() == 0:
        for url in settings.SEED_URLS:
            if not distribution_manager.is_visited(url):
                distribution_manager.push_url(url)
                print(f"🌱 推送种子 URL: {url}")
            else:
                print(f"🚫 URL 已存在: {url}")
    else:
        print("📥 Redis 队列已有任务，无需推送种子 URL")

# 更新爬虫状态
def update_crawler_status(status, current_url=None):
    status_info = {
        "current_url": current_url if current_url else "idle",
        "status": status,
        "last_active_time": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    distribution_manager.set_crawler_status(pid, status_info)
    distribution_manager.add_active_crawler(pid)

# 发送心跳信号
def send_heartbeat():
    distribution_manager.send_heartbeat(pid)

# 获取网页文本
def fetch_text(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            content_div = soup.find("div", {"id": "mw-content-text"})
            if not content_div:
                return None, None

            paragraphs = content_div.find_all(["p", "li"])
            cleaned_text = "\n".join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))

            return cleaned_text, soup
        else:
            print(f"❌ 失败 {url} 状态码: {response.status_code}")
            return None, None
    except Exception as e:
        print(f"⚠️ 爬取 {url} 失败，错误: {e}")
        return None, None

# 解析内部链接
def parse_internal_links(soup, base_url):
    links = set()
    for a_tag in soup.find_all("a", href=True):
        link = a_tag["href"]
        if link.startswith("/") and not link.startswith("//"):
            link = base_url + link
        elif not link.startswith(base_url):
            continue
        links.add(link)
    return links

# 处理 URL 逻辑
def process_url():
    push_seed_urls()  # 先推送种子 URL
    try:
        while True:
            send_heartbeat()

            while distribution_manager.distribute_url():
                pass  # 处理所有未分发的 URL

            url = distribution_manager.pop_url()
            if not url:
                print("🌟 URL 队列为空，等待新任务...")
                update_crawler_status("idle")
                time.sleep(5)
                continue

            if distribution_manager.is_visited(url):
                print(f"🔄 {url} 已爬取，跳过")
                continue

            update_crawler_status("crawling", url)
            print(f"🌍 正在爬取: {url}")
            distribution_manager.set_status(url, "crawling")

            text, soup = fetch_text(url)
            if text:
                links = parse_internal_links(soup, "https://stardewvalleywiki.com")
                db_manager.save_page(url, text)

                distribution_manager.mark_visited(url)
                distribution_manager.set_status(url, "done")
                update_crawler_status("done", url)

                for link in links:
                    if not distribution_manager.is_visited(link):
                        distribution_manager.push_url_to_master(link)

                print(f"✅ 爬取成功: {url} (提取 {len(links)} 个站内链接)")
            else:
                print(f"❌ 爬取失败: {url}, 加入失败队列")
                distribution_manager.push_failed_url(url)
                distribution_manager.set_status(url, "failed")
                update_crawler_status("failed", url)

            time.sleep(1)
    finally:
        distribution_manager.clear_crawler_status(pid)
        distribution_manager.remove_active_crawler(pid)

# 启动爬虫
def start_crawler():
    update_crawler_status("idle")  # 启动时注册进程状态
    process_url()

if __name__ == "__main__":
    print("🚀 启动分布式爬虫系统")
    start_crawler()
