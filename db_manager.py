import pymongo

class DBManager:
    def __init__(self, host="127.0.0.1", port=27017, db_name="vally",col_name='vally1'):
        self.client = pymongo.MongoClient(host, port)
        self.db = self.client[db_name]
        self.collection = self.db[col_name]  # 存储网页数据的集合

    def save_page(self, url, text):
        """存储爬取的页面文本"""
        if not text.strip():  # 避免存入空数据
            print(f"⚠️ 跳过空白页面: {url}")
            return
        
        self.collection.update_one(
            {"url": url}, 
            {"$set": {"url": url, "text": text}}, 
            upsert=True  # 如果已存在该 URL，则更新，否则插入新数据
        )
        print(f"✅ 页面已存入 MongoDB: {url}")

    def get_page(self, url):
        """获取已存储页面的文本内容"""
        page = self.collection.find_one({"url": url})
        return page["text"] if page else None

    def count_pages(self):
        """返回当前存储的页面数量"""
        return self.collection.count_documents({})

