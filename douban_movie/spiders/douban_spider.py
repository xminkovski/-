import scrapy
import logging
import threading
import re
import redis
from douban_movie.items import DoubanMovieItem
from urllib.parse import urlparse, urlunparse
from scrapy_redis.spiders import RedisSpider
from jieba import cut
from kafka import KafkaConsumer
from pybloom_live import ScalableBloomFilter
from scrapy import signals
from kafka import KafkaProducer
from collections import defaultdict

# 禁用Twisted日志
logging.getLogger('twisted').setLevel(logging.CRITICAL)

class DoubanSpider(RedisSpider):
    name = "douban"
    redis_key = 'douban:start_urls'           # 统一入口队列
    redis_key_seeds = "douban:seeds"          # 种子URL队列
    redis_key_pending = "douban:pending"      # 待爬队列
    redis_key_visited = "douban:visited"      # 已爬集合
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092')
        # 初始化Redis连接
        self.redis_conn = redis.Redis(
            host='localhost',
            port=6379,
            decode_responses=True
        )
        
        # 初始化种子URL
        if self.redis_conn.scard(self.redis_key_seeds) == 0:
            initial_seed = 'https://movie.douban.com/subject/1292052/reviews?sort=hotest&start=0'
            self.redis_conn.sadd(self.redis_key_seeds, initial_seed)
        self.bloom_filter = ScalableBloomFilter()
        
        # 初始化域名请求计数器
        self.domain_counter = defaultdict(int)
        self.max_shards = 8  # 最大分片数

    def load_existing_urls(self):
        """初始化时加载已存在的 URL 到布隆过滤器"""
        visited_urls = self.redis_conn.smembers(self.redis_key_visited)
        for url in visited_urls:
            self.bloom_filter.add(url)

    def init_pubsub(self):
        """Redis 订阅监听外部手动推送的种子"""
        self.pubsub = self.redis_conn.pubsub()
        self.pubsub.subscribe('douban:seed_updates')
        self.listener_thread = threading.Thread(target=self.listen_for_seeds)
        self.listener_thread.daemon = True
        self.listener_thread.start()

    def start_requests(self):
        # 从种子队列获取初始URL
        while True:
            seed_url = self.redis_conn.spop(self.redis_key_seeds)
            if not seed_url:
                break
            if not self.redis_conn.sismember(self.redis_key_visited, self.normalize_url(seed_url)):
                yield scrapy.Request(seed_url, callback=self.parse)

    def init_pubsub(self):
        """Redis 订阅监听外部手动推送的种子"""
        self.pubsub = self.redis_conn.pubsub()
        self.pubsub.subscribe('douban:seed_updates')
        self.listener_thread = threading.Thread(target=self.listen_for_seeds)
        self.listener_thread.daemon = True
        self.listener_thread.start()

    def add_seed_url(self, url, priority=1):
        """动态添加种子 URL（支持优先级和去重）"""
        normalized = self.normalize_url(url)
        if normalized not in self.bloom_filter:
            self.redis_conn.zadd(self.redis_key_seeds, {normalized: priority})
            self.bloom_filter.add(normalized)

    def parse(self, response):
        self.logger.info(f"Current proxy: {response.request.meta.get('proxy')}")
        # 处理当前页面
        for review in response.xpath('//div[@class="main review-item"]'):
            item = DoubanMovieItem()
            item['movie_name'] = response.xpath('//h1/text()').get().strip()
            item['comment'] = review.xpath('.//div[@class="short-content"]/text()').get(default='').strip()
            item['rating'] = review.xpath('.//span[contains(@class, "allstar")]/@title').get(default='').strip()
            item['username'] = review.xpath('.//a[@class="name"]/text()').get(default='').strip()
            yield item

        # 处理分页
        next_page = response.xpath('//a[@class="next"]/@href').get()
        if next_page:
            next_page = response.urljoin(next_page)
            self.add_url_to_pending(next_page)

        # 动态更新种子URL途径1：从当前页面提取新种子（例如发现其他电影的评论页）
        new_seeds = response.xpath('//a[contains(@href, "/subject/")]/@href').getall()
        for url in new_seeds:
            full_url = response.urljoin(url)
            self.add_seed_url(full_url)  # 调用新增的种子管理方法

    def process_comment(self, text):
        #数据清洗与分词
        cleaned = re.sub(r'\s+', ' ', text)  # 去空格
        keywords = ' '.join(cut(cleaned))  # 中文分词
        return keywords

    def add_url_to_pending(self, url):
        """将URL加入待爬队列，并按哈希分片策略分发URL到不同Kafka分区"""
        normalized = self.normalize_url(url)
        shard_id = hash(normalized) % 4
        self.kafka_producer.send('douban:pending', key=str(shard_id).encode(), value=url.encode())
        
        """动态权重分片"""
        self.dynamic_weight_sharding(url)
        parsed = urlparse(url)
        domain = parsed.netloc
        
        # 更新域名请求计数
        self.domain_counter[domain] += 1
        
        # 根据请求频率计算分片数（频率越高，分片越多）
        base_shards = 2
        dynamic_shards = min(
            self.max_shards, 
            base_shards + (self.domain_counter[domain] // 100)  # 每100次请求增加1分片
        )
        # 计算分片ID
        shard_id = hash(domain) % dynamic_shards
        # 发送到Kafka指定分区
        self.kafka_producer.send(
            'douban:pending', 
            key=str(shard_id).encode(), 
            value=url.encode()
        )
        # 记录日志
        self.logger.debug(f"Domain: {domain} | Shards: {dynamic_shards} | Shard: {shard_id}")
    
    def normalize_url(self, url):
        """URL标准化处理"""
        parsed = urlparse(url)
        # 去除追踪参数
        clean_query = '&'.join(sorted([q for q in parsed.query.split('&') if not q.startswith('_')]))
        return urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            clean_query,
            ''  # 去除片段
        ))

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        self.scheduler.shutdown()


