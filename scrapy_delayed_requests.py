from weakref import WeakKeyDictionary

from scrapy import signals, Request
from scrapy.exceptions import DontCloseSpider
from twisted.internet import reactor

class DelayedRequestsMiddleware(object):
    requests = WeakKeyDictionary()

    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        crawler.signals.connect(s.spider_idle, signal=signals.spider_idle)
        return s

    @classmethod
    def spider_idle(cls, spider):
        if cls.requests.get(spider):
            spider.log('delayed requests pending, not closing spider')
            raise DontCloseSpider()

    def schedule_request(self, request, spider):
        spider.crawler.engine.schedule(request, spider)
        self.requests[spider] -= 1

    def process_spider_output(self, response, result, spider):
        for i in result:
            if isinstance(i, Request):
                delay = i.meta.pop('delay_request', None)
                if delay:
                    self.requests.setdefault(spider, 0)
                    self.requests[spider] += 1
                    reactor.callLater(delay, self.schedule_request, i.copy(), spider)
                    continue
            
            yield i
