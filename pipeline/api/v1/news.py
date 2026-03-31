from fastapi import APIRouter, BackgroundTasks
from pipeline.services.container import crawl_orchestrator, service

import logging

from pipeline.services.summarize_service import SummaryService

router = APIRouter(prefix="/news", tags=["news"])
logger = logging.getLogger(__name__)


@router.get("/crawl")
async def crawl_news(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_crawl_job)
    return {"message": "작업이 등록됐습니다."}

@router.get("/crawl-today")
async def crawl_news_today(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_crawl_job, True)
    return {"message": "오늘의 뉴스 작업이 등록됐습니다."}

@router.get("/summary")
async def summary(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_summary_job_all)
    return {'message': '이벤트 요약 작업 시작'}

@router.get("/summary/event/{eventId}")
async def summary(background_tasks: BackgroundTasks, eventId: int):
    background_tasks.add_task(run_summary_job_by_id, eventId)
    return {'message': '이벤트 요약 작업 시작'}


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

async def run_summary_job_all():
    service.summarize_events(
        event_ids=None,
        only_empty_summary=True,
        only_active_crawl=True,
        overwrite_existing_summary=False,
        update_relevance_scores=True,
    )

async def run_summary_job_by_id(eventId: int):
    # 특정 이벤트만 강제 재요약하고 싶으면 예시:
    service.summarize_events(
        event_ids=[eventId],
        only_empty_summary=False,
        only_active_crawl=False,
        overwrite_existing_summary=True,
        update_relevance_scores=True,
    )