class CrawlOrchestrator:
    def __init__(self, crawl_service):
        self.crawl_service = crawl_service


    async def run(self, request):
        response = await self.crawl_service.collect(request.targets)
        return {
            "news_list": response,
        }