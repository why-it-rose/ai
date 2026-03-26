import logging
import time

logger = logging.getLogger(__name__)
class CrawlOrchestrator:
    def __init__(self, request_generator, crawl_service, csv_service, tag_service, transfer_service):
        self.request_generator = request_generator
        self.crawl_service = crawl_service
        self.csv_service = csv_service
        self.tag_service = tag_service
        self.transfer_service = transfer_service

    async def run_today(self):
        request = self.request_generator.generate_today()
        print(request)
        response = await self.crawl_service.collect(request.targets)
        crawled_file_paths = self.csv_service.write_stock_csv(response.stocks)
        tagged_file_paths = self.tag_service.tag_csv(crawled_file_paths)
        print(tagged_file_paths)
        # self.transfer_service.transfer_all(tagged_file_paths)

        return {
            # "count": response.count,
            # "stocks": response.stocks,
            # "file_path": crawled_file_paths,
            # "tagged_file_paths": tagged_file_paths,
        }

    async def run_entire(self):
        total_start = time.perf_counter()

        t = time.perf_counter()
        request = self.request_generator.generate_entire()
        logger.info("request 생성 elapsed=%.3fs", time.perf_counter() - t)

        t = time.perf_counter()
        response = await self.crawl_service.collect(request.targets)
        logger.info("crawl elapsed=%.3fs", time.perf_counter() - t)

        t = time.perf_counter()
        crawled_file_paths = self.csv_service.write_stock_csv(response.stocks)
        logger.info("csv 저장 elapsed=%.3fs", time.perf_counter() - t)

        t = time.perf_counter()
        tagged_file_paths = self.tag_service.tag_csv(crawled_file_paths)
        logger.info("tag elapsed=%.3fs", time.perf_counter() - t)

        t = time.perf_counter()
        self.transfer_service.transfer_all(tagged_file_paths)
        logger.info("db 적재 elapsed=%.3fs", time.perf_counter() - t)

        logger.info("pipeline total elapsed=%.3fs", time.perf_counter() - total_start)
        return {
            "count": response.count,
            "stocks": response.stocks,
            "file_path": crawled_file_paths,
            "tagged_file_paths": tagged_file_paths,
        }
