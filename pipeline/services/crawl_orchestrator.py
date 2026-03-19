class CrawlOrchestrator:
    def __init__(self, crawl_service, csv_service):
        self.crawl_service = crawl_service
        self.csv_service = csv_service

    async def run(self, request):
        response = await self.crawl_service.collect(request.targets)
        file_path = self.csv_service.write_stock_csv(response.stocks)
        return {
            "count": response.count,
            "stocks": response.stocks,
            "file_path": file_path,
        }