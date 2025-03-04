import requests
from bs4 import BeautifulSoup
import time
from redis_manager import RedisManager
from db_manager import DBManager
import os
# 初始化 Redis 和 MongoDB 连接
redis_manager = RedisManager()
db_manager = DBManager()
pid = os.getpid()
# 设置 User-Agent，防止封禁
HEADERS = {"User-Agent": "Mozilla/5.0"}


def update_crawler_status(status, current_url=None):
    """更新当前爬虫进程的状态到 Redis"""
    status_info = {
        "current_url": current_url if current_url else "idle",
        "status": status,
        "last_active_time": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    #print(f"🛠️ 正在更新状态: {status_info}")
    redis_manager.set_crawler_status(pid, status_info)
    redis_manager.add_active_crawler(pid)

def clear_crawler_status():
    """清理当前爬虫进程的状态"""
    redis_manager.clear_crawler_status(pid)
    redis_manager.remove_active_crawler(pid)

def send_heartbeat():
    """定期发送心跳信号到 Redis"""
    heartbeat_key = f"crawler:heartbeat:{pid}"
    redis_manager.redis_client.set(heartbeat_key, time.time(), ex=30) 

def fetch_text(url):
    """请求网页并提取纯文本内容（优化版）"""
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            # 获取正文区域
            content_div = soup.find("div", {"id": "mw-content-text"})
            if not content_div:
                return None, None
            
            # 解析所有 <p> 段落，并拼接成文本
            paragraphs = content_div.find_all(["p", "li"])
            cleaned_text = "\n\n".join(p.get_text(strip=True) for p in paragraphs)

            return cleaned_text, soup
        else:
            print(f"❌ 失败 {url} 状态码: {response.status_code}")
            return None, None
    except Exception as e:
        print(f"⚠️ 爬取 {url} 失败，错误: {e}")
        return None, None


# 解析网页，提取站内链接
def parse_internal_links(soup, base_url):
    """解析 HTML，提取站内链接"""
    links = set()

    for a_tag in soup.find_all("a", href=True):
        link = a_tag["href"]

        # 只保留站内链接（以 / 开头或包含 base_url）
        if link.startswith("/"):
            link = base_url + link
        elif not link.startswith(base_url):
            continue  # 跳过外部链接

        links.add(link)

    return links

# 处理 URL 任务
def process_url():
    """主爬取逻辑，从 Redis 队列获取 URL，爬取并存储"""
    try:
        while True:
            url = redis_manager.pop_url()  # 获取待爬取 URL
            send_heartbeat() 
            if url is None:
                print("🌟 URL 队列为空，等待新任务...")
                update_crawler_status("idle")
                time.sleep(5)
                continue

            if redis_manager.is_visited(url):
                print(f"🔄 {url} 已爬取，跳过")
                continue

            update_crawler_status("crawling", url)

            print(f"🌍 正在爬取: {url}")
            redis_manager.set_status(url, "crawling")  # 设置状态

            text, soup = fetch_text(url)  # 下载页面并提取文本
            if text:
                links = parse_internal_links(soup, "https://zh.stardewvalleywiki.com")  # 提取站内链接
                db_manager.save_page(url, text)  # 存入 MongoDB
                # print(text[:10])

                redis_manager.mark_visited(url)  # 标记已爬取
                redis_manager.set_status(url, "done")  # 更新状态
                update_crawler_status("done", url)

                # 添加新链接到任务队列
                for link in links:
                    if not redis_manager.is_visited(link):
                        redis_manager.push_url(link)

                print(f"✅ 爬取成功: {url} (提取 {len(links)} 个站内链接)")
            else:
                print(f"❌ 爬取失败: {url}, 加入失败队列")
                redis_manager.push_failed_url(url)
                redis_manager.set_status(url, "failed")
                update_crawler_status("failed", url)

            time.sleep(1)  # 控制爬取间隔，避免过快
    finally:
        clear_crawler_status()

if __name__ == "__main__":
    update_crawler_status("idle")  # 启动时注册进程状态
    process_url()