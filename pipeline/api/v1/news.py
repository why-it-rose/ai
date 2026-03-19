from fastapi import APIRouter

from pipeline.schemas.crawl_request import CrawlJobRequest
from pipeline.schemas.news_request import NewsCrawlRequest
from pipeline.schemas.news_response import NewsCrawlResponse
from pipeline.services.container import crawl_orchestrator

router = APIRouter(prefix="/news", tags=["news"])

@router.post("/crawl-response", response_model=NewsCrawlResponse)
async def crawl_news_endpoint(request: NewsCrawlRequest):
    return await crawl_news(request)

@router.post("/crawl")
async def crawl_news(request: CrawlJobRequest):
    result = await crawl_orchestrator.run(request)
    return result
# 여기에 트리거만 호출만 하고 바로 반환되도록 구성

@router.get("/test", response_model=dict)
async def test_endpoint():
    return {"message": "API is working!!!"}

# http://127.0.0.1:8000/api/v1/news/test

## 데이터 정제해기, DATETIME으로 날짜 변환, 본문 쓸모없는 부분 제거 등
## DB에 저장 후 태깅