class CrawlOrchestrator:
    def __init__(self, request_generator, crawl_service, csv_service, tag_service, transfer_service):
        self.request_generator = request_generator
        self.crawl_service = crawl_service
        self.csv_service = csv_service
        self.tag_service = tag_service
        self.transfer_service = transfer_service

    async def run(self):
        request = self.request_generator.generate()
        print("Generated request:", request)
        response = await self.crawl_service.collect(request.targets)
        crawled_file_paths = self.csv_service.write_stock_csv(response.stocks)
        tagged_file_paths = self.tag_service.tag_csv(crawled_file_paths)
        self.transfer_service.transfer_all(tagged_file_paths)

        return {
            "count": response.count,
            "stocks": response.stocks,
            "file_path": crawled_file_paths,
            "tagged_file_paths": tagged_file_paths,
        }