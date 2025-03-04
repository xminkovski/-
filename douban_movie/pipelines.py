# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymysql

class DoubanMoviePipeline:  #爬取网页

    
    #创建数据库
    def open_spider(self, spider):
        self.conn = pymysql.connect(
            host='localhost', 
            user='root',
            password='12345678',
            database='douban',
            charset='utf8mb4'
        )
        self.cursor = self.conn.cursor()
        
        # 更新建表语句，添加二级索引
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS comments (
                id INT AUTO_INCREMENT PRIMARY KEY,
                movie_name VARCHAR(255) NOT NULL,
                comment TEXT,
                rating VARCHAR(50),
                username VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_movie_name (movie_name),
                INDEX idx_username (username)
            )
        ''')
        self.conn.commit()

    def process_item(self, item, spider):
        try:
            sql = '''
                INSERT INTO comments (movie_name, comment, rating, username)
                VALUES (%s, %s, %s, %s)
            '''
            self.cursor.execute(sql, (
                item['movie_name'], 
                item['comment'], 
                item['rating'], 
                item['username']
            ))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e
        return item

    def close_spider(self, spider):
        self.conn.close()

class DoubanMovieMysqlPipeline:
    def __init__(self, host, user, password, database):
        self.connection = pymysql.connect(host=host, user=user, password=password, database=database)
        self.cursor = self.connection.cursor()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            host=crawler.settings.get('MYSQL_HOST'),
            user=crawler.settings.get('MYSQL_USER'),
            password=crawler.settings.get('MYSQL_PASSWORD'),
            database=crawler.settings.get('MYSQL_DATABASE')
        )

    def process_item(self, item, spider):
        sql = "INSERT INTO comments (movie_name, comment, rating, username) VALUES (%s, %s, %s, %s)"
        self.cursor.execute(sql, (item['movie_name'], item['comment'], item['rating'], item['username']))
        self.connection.commit()
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()