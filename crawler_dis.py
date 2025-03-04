import time
import os
import re
import requests
from bs4 import BeautifulSoup
from distributed_distribution_manager import DistributedDistributionManager
from db_manager import DBManager
from settings import DISTRIBUTED_NODE_COUNT, URL_DISTRIBUTION_STRATEGY,PRIORITY_RULES

# 初始化分发管理器和数据库连接
distribution_manager = DistributedDistributionManager()
db_manager = DBManager()

# 获取当前爬虫进程的 ID 和节点 ID (用于轮询分配策略)
pid = os.getpid()
node_id = pid % DISTRIBUTED_NODE_COUNT

# 设置 User-Agent，防止封禁
HEADERS = {"User-Agent": "Mozilla/5.0"}

def update_crawler_status(status, current_url=None):
    """更新当前爬虫进程的状态到 Redis"""
    status_info = {
        "current_url": current_url if current_url else "idle",
        "status": status,
        "last_active_time": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    distribution_manager.set_crawler_status(pid, status_info)
    distribution_manager.add_active_crawler(pid)

def clear_crawler_status():
    """清理当前爬虫进程的状态"""
    distribution_manager.clear_crawler_status(pid)
    distribution_manager.remove_active_crawler(pid)

def send_heartbeat():
    """定期发送心跳信号到 Redis"""
    distribution_manager.send_heartbeat(pid)

def fetch_text(url):
    """请求网页并提取纯文本内容（优化版）"""
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

def parse_internal_links(soup, base_url):
    """解析 HTML，提取站内链接"""
    links = set()
    for a_tag in soup.find_all("a", href=True):
        link = a_tag["href"]
        if link.startswith("/") and not link.startswith("//"):
            link = base_url + link
        elif not link.startswith(base_url):
            continue
        links.add(link)
    return links

def determine_url_priority(url):
    """ 根据 URL 模式确定优先级 (分数越低优先级越高) """
    for priority, patterns in PRIORITY_RULES.items():
        if any(re.search(pattern, url) for pattern in patterns):
            return priority
    return 10  # 默认最低优先级

def process_url():
    """主爬取逻辑，从任务队列获取 URL，爬取并存储"""
    try:
        while True:
            send_heartbeat()  # 发送心跳信号

            # 先执行 URL 分发（如果 `url_master_queue` 里有未分配的 URL）
            while distribution_manager.distribute_url():
                pass  # 处理所有未分发的 URL，直到队列为空

            # 从任务队列获取 URL
            url = distribution_manager.pop_url()  # ✅ 确保调用与类方法匹配
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
                        priority = determine_url_priority(link)
                        distribution_manager.push_url_to_master(link)  # ✅ 只推送到 `url_master_queue`

                print(f"✅ 爬取成功: {url} (提取 {len(links)} 个站内链接)")
            else:
                print(f"❌ 爬取失败: {url}, 加入失败队列")
                distribution_manager.push_failed_url(url)
                distribution_manager.set_status(url, "failed")
                update_crawler_status("failed", url)

            time.sleep(1)
    finally:
        clear_crawler_status()



if __name__ == "__main__":
    update_crawler_status("idle")  # 启动时注册进程状态
    process_url()
