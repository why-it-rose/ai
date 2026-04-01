import logging
import os
import time
from pathlib import Path

logger = logging.getLogger(__name__)


class CrawlOrchestrator:
    def __init__(
        self,
        request_generator,
        crawl_service,
        csv_service,
        tag_service,
        transfer_service,
        sentiment_predictor,
    ):
        self.request_generator = request_generator
        self.crawl_service = crawl_service
        self.csv_service = csv_service
        self.tag_service = tag_service
        self.transfer_service = transfer_service
        self.sentiment_predictor = sentiment_predictor

    async def run_today(self):
        total_start = time.perf_counter()

        t = time.perf_counter()
        request = self.request_generator.generate_today()
        logger.info("request 생성 elapsed=%.3fs", time.perf_counter() - t)

        processed_files = []

        for target in request.targets:
            for period in target.periods:
                result = await self._run_single_unit(
                    stock_name=target.stock,
                    period=period,
                    link_events=False,
                )
                processed_files.append(result)

        logger.info("pipeline total elapsed=%.3fs", time.perf_counter() - total_start)
        return {
            "count": len(processed_files),
            "processed": processed_files,
        }

    async def run_entire(self):
        total_start = time.perf_counter()


        t = time.perf_counter()
        request = self.request_generator.generate_entire()
        logger.info("request 생성 elapsed=%.3fs", time.perf_counter() - t)

        processed_files = []
        skipped_units = []
        failed_units = []

        for target in request.targets:
            for period in target.periods:
                event_id = getattr(period, "event_id", None)

                acquired = self.request_generator.try_mark_event_pending(event_id)
                if not acquired:
                    skipped_units.append(
                        {
                            "stock": target.stock,
                            "event_id": event_id,
                            "reason": "already_processing_or_not_inactive",
                        }
                    )
                    logger.info(
                        "unit skip: stock=%s event_id=%s reason=already_processing_or_not_inactive",
                        target.stock,
                        event_id,
                    )
                    continue

                try:
                    result = await self._run_single_unit(
                        stock_name=target.stock,
                        period=period,
                        link_events=True,
                    )
                    processed_files.append(result)
                except Exception as e:
                    failed_units.append(
                        {
                            "stock": target.stock,
                            "event_id": event_id,
                            "error": str(e),
                        }
                    )
                    logger.exception(
                        "unit 처리 실패: stock=%s event_id=%s",
                        target.stock,
                        event_id,
                    )

        logger.info("pipeline total elapsed=%.3fs", time.perf_counter() - total_start)
        return {
            "count": len(processed_files),
            "processed": processed_files,
            "skipped_count": len(skipped_units),
            "skipped": skipped_units,
            "failed_count": len(failed_units),
            "failed": failed_units,
        }

    async def _run_single_unit(self, stock_name: str, period, link_events: bool) -> dict:
        event_id = getattr(period, "event_id", None)

        raw_path = self.csv_service.build_unit_csv_path(
            stock_name=stock_name,
            from_date=period.fromDate,
            to_date=period.toDate,
            event_id=event_id,
        )

        tagged_path = self.tag_service._build_output_path(raw_path)
        senti_path = str(
            self.sentiment_predictor._build_output_path(
                Path(os.path.abspath(tagged_path))
            )
        )

        try:
            t = time.perf_counter()

            if os.path.exists(senti_path):
                logger.info(
                    "최종 senti csv 재사용: stock=%s event_id=%s path=%s",
                    stock_name,
                    event_id,
                    senti_path,
                )
                ready_path = senti_path

            elif os.path.exists(tagged_path):
                logger.info(
                    "tagged csv 재사용 후 sentiment 진행: stock=%s event_id=%s path=%s",
                    stock_name,
                    event_id,
                    tagged_path,
                )
                ready_path = self.sentiment_predictor.fill_missing_sentiment_scores(tagged_path)[0]

            elif os.path.exists(raw_path):
                logger.info(
                    "raw csv 재사용 후 tag/sentiment 진행: stock=%s event_id=%s path=%s",
                    stock_name,
                    event_id,
                    raw_path,
                )
                tagged_file = self.tag_service.tag_csv([raw_path])[0]
                ready_path = self.sentiment_predictor.fill_missing_sentiment_scores(tagged_file)[0]

            else:
                logger.info("crawl 시작: stock=%s event_id=%s", stock_name, event_id)
                unit = await self.crawl_service.collect_unit(stock_name, period)

                raw_file = self.csv_service.write_unit_csv(
                    stock_name=stock_name,
                    from_date=period.fromDate,
                    to_date=period.toDate,
                    news=unit["news"],
                    event_id=event_id,
                )
                logger.info("raw csv 저장 완료: %s", raw_file)

                tagged_file = self.tag_service.tag_csv([raw_file])[0]
                ready_path = self.sentiment_predictor.fill_missing_sentiment_scores(tagged_file)[0]

            logger.info(
                "전처리 완료 elapsed=%.3fs stock=%s event_id=%s",
                time.perf_counter() - t,
                stock_name,
                event_id,
            )

            t = time.perf_counter()
            self.transfer_service.transfer([ready_path], link_events=link_events)

            if link_events and event_id is not None:
                self.request_generator.mark_event_active(event_id)

            logger.info(
                "db 적재 완료 elapsed=%.3fs stock=%s event_id=%s",
                time.perf_counter() - t,
                stock_name,
                event_id,
            )

            return {
                "stock": stock_name,
                "event_id": event_id,
                "raw_path": raw_path,
                "tagged_path": tagged_path,
                "ready_path": ready_path,
            }

        except Exception:
            if link_events and event_id is not None:
                self.request_generator.recover_pending_event(event_id)
                logger.warning(
                    "실패 event 상태 보정 완료: event_id=%s",
                    event_id,
                )
            raise