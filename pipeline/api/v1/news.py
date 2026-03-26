from fastapi import APIRouter, BackgroundTasks
from pipeline.services.container import crawl_orchestrator

import logging

router = APIRouter(prefix="/news", tags=["news"])
logger = logging.getLogger(__name__)


@router.get("/crawl")
async def crawl_news(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_crawl_job)
    return {"message": "작업이 등록됐습니다."}

@router.get("/crawl-today")
async def crawl_news(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_crawl_job, True)
    return {"message": "오늘의 뉴스 작업이 등록됐습니다."}

async def run_crawl_job(is_today: bool = False):
    try:
        if(is_today):
            logger.info("오늘의 뉴스 크롤링 작업 시작")
            await crawl_orchestrator.run_today()
        else:
            logger.info("전체 크롤링 작업 시작")
            await crawl_orchestrator.run_entire()
        logger.info("크롤링 작업 완료")
    except Exception as e:
        logger.exception("크롤링 작업 실패: %s", e)
