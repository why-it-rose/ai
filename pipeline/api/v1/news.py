from fastapi import APIRouter

from pipeline.schemas.crawl_request import CrawlJobRequest
from pipeline.services.container import crawl_orchestrator

router = APIRouter(prefix="/news", tags=["news"])

@router.post("/crawl")
async def crawl_news(request: CrawlJobRequest):
    # 1. DB에 현재 작업에 대해서 PENDING 상태로 저장

    result = await crawl_orchestrator.run(request)
    # 2. 실행 후 DB에 현재 상태 RUNNING으로 업데이트


    # 3. crawl_orchestrator.run(request) 실행 (비동기) 완료 후, 해당 로직 내에 DB에 현재 상태 SUCCESS 또는 FAILURE로 업데이트


    # return 에는 작업에 대한 id 및 현재 상태 반환
    return result
# 여기에 트리거만 호출만 하고 바로 반환되도록 구성
