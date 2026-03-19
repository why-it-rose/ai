from fastapi import APIRouter

from pipeline.schemas.crawl_request import CrawlJobRequest
from pipeline.services.container import crawl_orchestrator

router = APIRouter(prefix="/news", tags=["news"])

@router.post("/crawl")
async def crawl_news(request: CrawlJobRequest):
    result = await crawl_orchestrator.run(request)
    return result
# 여기에 트리거만 호출만 하고 바로 반환되도록 구성
