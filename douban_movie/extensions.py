from prometheus_client import start_http_server, Counter, Gauge
from scrapy import signals

class MonitorExtension:
    def __init__(self):
        self.crawled_pages = Counter('crawled_pages', 'Total crawled pages')
        self.failed_requests = Counter('failed_requests', 'Total failed requests')
        self.proxy_usage = Gauge('proxy_usage', 'Current proxy IP usage ratio')
    @classmethod
    def from_crawler(cls, crawler):
        ext = cls()
        crawler.signals.connect(ext.page_crawled, signal=signals.item_scraped)
        start_http_server(8000)  # 指标暴露端口
        return ext
    
    def page_crawled(self, item, response, spider):
        self.crawled_pages.inc()