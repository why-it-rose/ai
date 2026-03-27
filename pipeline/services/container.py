from pipeline.services.sentiment_predictor import NewsSentimentService
from pipeline.services.crawl_orchestrator import CrawlOrchestrator
from pipeline.services.crawl_service import CrawlService
from pipeline.services.csv_service import CsvService
from pipeline.services.request_generator import RequestGenerator
from pipeline.services.tag_service import TagService
from pipeline.services.transfer_service import TransferService
from dotenv import dotenv_values

DB_CONFIG = dotenv_values(".env").get("DB_CONFIG")

request_generator = RequestGenerator()
crawl_service = CrawlService()
csv_service = CsvService()
tag_service = TagService()
transfer_service = TransferService(batch_size=500)
sentiment_predictor = NewsSentimentService(
        batch_size=64,
        model_name="snunlp/KR-FinBert-SC",
        max_length=256,
        body_max_chars=500,
        use_body=True,
        title_weight=0.6,
        body_weight=0.4,
    )
crawl_orchestrator = CrawlOrchestrator(
    request_generator=request_generator,
    crawl_service=crawl_service,
    csv_service=csv_service,
    tag_service=tag_service,
    transfer_service=transfer_service,
    sentiment_predictor=sentiment_predictor,
)
