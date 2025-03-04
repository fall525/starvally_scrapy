import requests
from bs4 import BeautifulSoup
import time
from redis_manager import RedisManager
from db_manager import DBManager

# 初始化 Redis 和 MongoDB 连接
redis_manager = RedisManager()
db_manager = DBManager()

# 设置 User-Agent，防止封禁
HEADERS = {"User-Agent": "Mozilla/5.0"}

# 爬取网页文本内容
# def fetch_text(url):
#     """请求网页并提取纯文本内容"""
#     try:
#         response = requests.get(url, headers=HEADERS, timeout=10)
#         if response.status_code == 200:
#             soup = BeautifulSoup(response.text, "html.parser")

#             # 获取主要内容区域（去掉导航、脚注、广告）
#             content_div = soup.find("div", {"id": "mw-content-text"})
#             if content_div:
#                 text = content_div.get_text(separator="\n", strip=True)
#             else:
#                 text = soup.get_text(separator="\n", strip=True)

#             return text, soup
#         else:
#             print(f"❌ 失败 {url} 状态码: {response.status_code}")
#             return None, None
#     except Exception as e:
#         print(f"⚠️ 爬取 {url} 失败，错误: {e}")
#         return None, None
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
    while True:
        url = redis_manager.pop_url()  # 获取待爬取 URL
        if url is None:
            print("🌟 URL 队列为空，等待新任务...")
            time.sleep(5)
            continue

        if redis_manager.is_visited(url):
            print(f"🔄 {url} 已爬取，跳过")
            continue

        print(f"🌍 正在爬取: {url}")
        redis_manager.set_status(url, "crawling")  # 设置状态

        text, soup = fetch_text(url)  # 下载页面并提取文本
        if text:
            links = parse_internal_links(soup, "https://zh.stardewvalleywiki.com")  # 提取站内链接
            db_manager.save_page(url, text)  # 存入 MongoDB
            # print(text[:10])

            redis_manager.mark_visited(url)  # 标记已爬取
            redis_manager.set_status(url, "done")  # 更新状态

            # 添加新链接到任务队列
            for link in links:
                if not redis_manager.is_visited(link):
                    redis_manager.push_url(link)

            print(f"✅ 爬取成功: {url} (提取 {len(links)} 个站内链接)")
        else:
            print(f"❌ 爬取失败: {url}, 加入失败队列")
            redis_manager.push_failed_url(url)
            redis_manager.set_status(url, "failed")

        time.sleep(1)  # 控制爬取间隔，避免过快

# if __name__ == "__main__":
#     process_url()