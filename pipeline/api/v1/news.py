from fastapi import APIRouter, BackgroundTasks

from pipeline.schemas.crawl_request import CrawlJobRequest
from pipeline.services.container import crawl_orchestrator

import logging

router = APIRouter(prefix="/news", tags=["news"])
logger = logging.getLogger(__name__)
@router.post("/crawl")
async def crawl_news(request: CrawlJobRequest, backgroundTask: BackgroundTasks):
    backgroundTask.add_task(run_crawl_job, request)
    return {"message": "작업이 등록됐습니다."}

async def run_crawl_job(request: CrawlJobRequest):
    try:
        result = await crawl_orchestrator.run(request)
        logger.info("크롤링 작업 완료: %s", result)
    except Exception as e:
        logger.exception("크롤링 작업 실패: %s", e)