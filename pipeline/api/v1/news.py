from fastapi import APIRouter, BackgroundTasks
from pipeline.services.container import crawl_orchestrator, service

import logging


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
async def summary_all(background_tasks: BackgroundTasks):
    background_tasks.add_task(run_summary_job_all)
    return {'message': '이벤트 요약 작업 시작'}


@router.get("/summary/stock/{stockId}")
async def summary_by_stock(background_tasks: BackgroundTasks, stockId: int):
    background_tasks.add_task(run_summary_job_by_stock, stockId)
    return {'message': f'종목(stock_id={stockId}) 이벤트 요약 작업 시작'}

@router.get("/summary/event/{eventId}")
async def summary_by_event(background_tasks: BackgroundTasks, eventId: int):
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
    try:
        logger.info("이벤트 요약 작업 시작 (모든 이벤트)")
        service.summarize_events(
            event_ids=None,
            stock_ids=None,
            only_empty_summary=True,
            only_active_crawl=True,
            overwrite_existing_summary=False,
            update_relevance_scores=True,
        )
        logger.info("이벤트 요약 작업 완료")
    except Exception as e:
        logger.exception("이벤트 요약 작업 실패: %s", e)


async def run_summary_job_by_stock(stock_id: int):
    try:
        logger.info("이벤트 요약 작업 시작 (stock_id=%d)", stock_id)
        service.summarize_events(
            event_ids=None,
            stock_ids=[stock_id],
            only_empty_summary=True,
            only_active_crawl=True,
            overwrite_existing_summary=False,
            update_relevance_scores=True,
        )
        logger.info("이벤트 요약 작업 완료 (stock_id=%d)", stock_id)
    except Exception as e:
        logger.exception("이벤트 요약 작업 실패: %s", e)

async def run_summary_job_by_id(eventId: int):
    try:
        logger.info("이벤트 요약 작업 시작 (event_id=%d)", eventId)
        # 특정 이벤트만 강제 재요약하고 싶으면 예시:
        service.summarize_events(
            event_ids=[eventId],
            stock_ids=None,
            only_empty_summary=False,
            only_active_crawl=False,
            overwrite_existing_summary=True,
            update_relevance_scores=True,
        )
        logger.info("이벤트 요약 작업 완료 (event_id=%d)", eventId)
    except Exception as e:
        logger.exception("이벤트 요약 작업 실패: %s", e)
