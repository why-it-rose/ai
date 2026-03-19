from pipeline.services.crawl_orchestrator import CrawlOrchestrator
from pipeline.services.crawl_service import CrawlService
from pipeline.services.csv_service import CsvService

crawl_service = CrawlService()
csv_service = CsvService()

crawl_orchestrator = CrawlOrchestrator(
    crawl_service=crawl_service,
    csv_service = csv_service,
)