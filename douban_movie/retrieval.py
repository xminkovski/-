from pymongo import MongoClient
import pymysql
class DoubanDataRetriever:
    def __init__(self, host, user, password, database):
        self.connection = pymysql.connect(host=host, user=user, password=password, database=database)
        self.cursor = self.connection.cursor()

    def get_comments_by_movie(self, movie_name):
        sql = "SELECT * FROM comments WHERE movie_name = %s"
        self.cursor.execute(sql, (movie_name,))
        return self.cursor.fetchall()

    def get_comments_by_user(self, username):
        sql = "SELECT * FROM comments WHERE username = %s"
        self.cursor.execute(sql, (username,))
        return self.cursor.fetchall()

# 使用示例
if __name__ == "__main__":
    retriever = DoubanDataRetriever()
    
    # 查询《肖申克的救赎》所有评论
    print("《肖申克的救赎》评论：")
    for comment in retriever.get_comments_by_movie('肖申克的救赎'):
        print(comment)
    
    # 查询用户"影迷小明"的所有评论
    print("\n用户 影迷小明 的评论：")
    for comment in retriever.get_comments_by_user('影迷小明'):
        print(comment)