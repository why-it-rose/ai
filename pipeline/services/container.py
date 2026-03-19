from pipeline.services.crawl_orchestrator import CrawlOrchestrator
from pipeline.services.crawl_service import CrawlService

crawl_service = CrawlService()

crawl_orchestrator = CrawlOrchestrator(
    crawl_service=crawl_service,
)