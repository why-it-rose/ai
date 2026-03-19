from pipeline.services.crawl_orchestrator import CrawlOrchestrator
from pipeline.services.crawl_service import CrawlService
from pipeline.services.csv_service import CsvService
from pipeline.services.tag_service import TagService

crawl_service = CrawlService()
csv_service = CsvService()
tag_service = TagService()

crawl_orchestrator = CrawlOrchestrator(
    crawl_service=crawl_service,
    csv_service = csv_service,
    tag_service = tag_service,
)