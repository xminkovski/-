# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
import requests
from scrapy.exceptions import IgnoreRequest

class ProxyPoolMiddleware:
    def __init__(self):
        self.proxy_pool_api = "http://127.0.0.1:5010"

    def get_proxy(self):
        """从 ProxyPool 获取代理"""
        try:
            response = requests.get(f"{self.proxy_pool_api}/get/").json()
            return response.get("proxy")
        except Exception as e:
            return None

    def delete_proxy(self, proxy):
        """删除无效代理"""
        requests.get(f"{self.proxy_pool_api}/delete/?proxy={proxy}")

    def process_request(self, request, spider):
        """为请求设置代理"""
        # 如果请求已设置代理，则跳过（例如重试时）
        if "proxy" in request.meta:
            return

        # 从 ProxyPool 获取代理
        proxy = self.get_proxy()
        if not proxy:
            spider.logger.error("No available proxy in pool!")
            raise IgnoreRequest("Proxy pool exhausted")

        # 设置代理（注意协议前缀 http:// 或 https://）
        request.meta["proxy"] = f"http://{proxy}"
        spider.logger.debug(f"Using proxy: {proxy}")

    def process_exception(self, request, exception, spider):
        """处理代理异常（如连接失败）"""
        proxy = request.meta.get("proxy", "").split("//")[-1]
        if proxy:
            spider.logger.error(f"Proxy {proxy} failed, deleting from pool")
            self.delete_proxy(proxy)
            # 标记此代理已失效，后续请求会重新获取新代理
            del request.meta["proxy"]
        # 触发 Scrapy 的重试机制
        return request

class DoubanMovieSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class DoubanMovieDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Log the response for debugging
        spider.logger.debug(f"Processing response for URL: {response.url}")

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Log the exception for debugging
        spider.logger.error(f"Exception occurred: {exception}")

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        return None

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)

class CustomDNSResolverMiddleware:
    def process_request(self, request, spider):
        # 自定义 DNS 解析逻辑（例如缓存或负载均衡）
        request.meta['dns_cache'] = {'your_domain': 'resolved_ip'}