import csv
import logging
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Iterator

import pymysql
import pymysql.cursors
from dotenv import dotenv_values

from pipeline.services.event_news_relevance_service import EventNewsRelevanceService

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

STATUS_ACTIVE = "ACTIVE"
NOW_FMT = "%Y-%m-%d %H:%M:%S.%f"


def _now() -> str:
    return datetime.now().strftime(NOW_FMT)


def _parse_dt(value: str) -> str:
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            dt = datetime.strptime(value.strip(), fmt)
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt.strftime(NOW_FMT)
        except ValueError:
            continue
    raise ValueError(f"날짜 파싱 실패: {value!r}")


class TransferService:
    def __init__(self, batch_size: int = 500):
        env_path = Path(__file__).resolve().parents[2] / ".env"
        env = dotenv_values(env_path)

        self.db_config = {
            "host": env.get("DB_HOST"),
            "port": int(env.get("DB_PORT", 3306)),
            "user": env.get("DB_USER"),
            "password": env.get("DB_PASSWORD"),
            "database": env.get("DB_NAME"),
            "charset": env.get("DB_CHARSET", "utf8mb4"),
        }
        self.batch_size = batch_size
        self.relevance_service = EventNewsRelevanceService()

    def transfer(self, tagged_file_paths: list[str] | str, link_events: bool = True) -> None:
        if isinstance(tagged_file_paths, str):
            tagged_file_paths = [tagged_file_paths]

        csv_files = [Path(p) for p in tagged_file_paths]
        logger.info("csv_files=%s", csv_files)

        if not csv_files:
            logger.warning("CSV 파일 경로가 없습니다.")
            return

        exists_files = [p for p in csv_files if p.exists()]
        missing_files = [p for p in csv_files if not p.exists()]

        for p in missing_files:
            logger.warning("파일을 찾을 수 없습니다 → 건너뜀: %s", p)

        if not exists_files:
            logger.warning("처리할 수 있는 CSV 파일이 없습니다.")
            return

        with self._connect() as conn:
            for csv_path in exists_files:
                logger.info("처리 시작: %s (link_events=%s)", csv_path.name, link_events)
                self._process_csv(conn, csv_path, link_events=link_events)
                logger.info("처리 완료: %s", csv_path.name)

    def _process_csv(self, conn, csv_path: Path, *, link_events: bool = False) -> None:
        total_news_new = 0
        total_news_link = 0
        total_tag_link = 0
        total_event_link = 0
        chunk_count = 0

        event_map_cache: dict[int, dict] = {}
        expected_event_urls: dict[int, set[str]] = {}

        for rows in self._iter_csv_chunks(csv_path):
            if not rows:
                continue

            chunk_count += 1
            now = _now()

            stock_name = rows[0]["_stock_name"]
            stock_id = self._resolve_stock_id(conn, stock_name)
            if stock_id is None:
                logger.warning("stocks 테이블에 없는 종목입니다: %s", stock_name)
                return

            if link_events:
                event_ids = {row["_event_id"] for row in rows if row["_event_id"] is not None}
                for event_id in event_ids:
                    if event_id not in event_map_cache:
                        event = self._load_pending_event_by_id(conn, event_id, stock_id)
                        if event:
                            event_map_cache[event_id] = event
                            expected_event_urls.setdefault(event_id, set())

                for row in rows:
                    event_id = row["_event_id"]
                    if event_id is None:
                        continue
                    event = event_map_cache.get(event_id)
                    if not event:
                        continue

                    window_start = event["start_date"] - timedelta(days=1)
                    window_end = event["end_date"] + timedelta(days=1)

                    if window_start <= row["_published_date"] <= window_end:
                        expected_event_urls[event_id].add(row["_url"])

            tag_names = set()
            for row in rows:
                for pred_col in ("pred_major", "pred_sub"):
                    tag_name = (row.get(pred_col) or "").strip()
                    if tag_name:
                        tag_names.add(tag_name)

            tag_map = self._load_tag_ids_for_names(conn, tag_names)

            news_rows: list[tuple] = []
            seen_chunk_urls: set[str] = set()

            for row in rows:
                url = row["_url"]
                if not url or url in seen_chunk_urls:
                    continue

                seen_chunk_urls.add(url)
                news_rows.append(
                    (
                        now,
                        now,
                        row.get("content", ""),
                        row["_published_at"],
                        row.get("source", ""),
                        STATUS_ACTIVE,
                        row.get("thumbnailUrl") or None,
                        row.get("title", ""),
                        url,
                        row.get("sentiment_score") or None,
                    )
                )

            inserted_count = self._bulk_insert_news_ignore(conn, news_rows)
            total_news_new += inserted_count

            url_to_id = self._load_url_to_id_by_urls(conn, list(seen_chunk_urls))

            if not url_to_id:
                conn.commit()
                logger.info("  → chunk=%d 매핑 가능한 news_id 없음", chunk_count)
                continue

            news_stocks_rows: list[tuple] = []
            news_tags_rows: list[tuple] = []
            event_news_rows: list[tuple] = []

            seen_news_stock_pairs: set[tuple[int, int]] = set()
            seen_news_tag_pairs: set[tuple[int, int]] = set()
            seen_event_news_pairs: set[tuple[int, int]] = set()

            for row in rows:
                news_id = url_to_id.get(row["_url"])
                if not news_id:
                    continue

                news_stock_pair = (news_id, stock_id)
                if news_stock_pair not in seen_news_stock_pairs:
                    news_stocks_rows.append((now, now, STATUS_ACTIVE, news_id, stock_id))
                    seen_news_stock_pairs.add(news_stock_pair)

                for pred_col in ("pred_major", "pred_sub"):
                    tag_name = (row.get(pred_col) or "").strip()
                    if not tag_name:
                        continue

                    tag_id = tag_map.get(tag_name)
                    if not tag_id:
                        continue

                    news_tag_pair = (news_id, tag_id)
                    if news_tag_pair not in seen_news_tag_pairs:
                        news_tags_rows.append((now, now, STATUS_ACTIVE, news_id, tag_id))
                        seen_news_tag_pairs.add(news_tag_pair)

                if link_events and row["_event_id"] is not None:
                    event = event_map_cache.get(row["_event_id"])
                    if not event:
                        continue

                    window_start = event["start_date"] - timedelta(days=1)
                    window_end = event["end_date"] + timedelta(days=1)

                    if not (window_start <= row["_published_date"] <= window_end):
                        continue

                    relevance_score = self.relevance_service.calculate(row, event)

                    event_news_pair = (event["id"], news_id)
                    if event_news_pair not in seen_event_news_pairs:
                        event_news_rows.append(
                            (now, now, relevance_score, STATUS_ACTIVE, event["id"], news_id)
                        )
                        seen_event_news_pairs.add(event_news_pair)

            inserted_ns, inserted_nt, inserted_en = self._bulk_insert_junction(
                conn,
                news_stocks_rows,
                news_tags_rows,
                event_news_rows,
            )

            total_news_link += inserted_ns
            total_tag_link += inserted_nt
            total_event_link += inserted_en
            conn.commit()

        if link_events and expected_event_urls:
            activatable_event_ids = self._find_activatable_event_ids(conn, expected_event_urls)
            if activatable_event_ids:
                self._mark_events_active(conn, activatable_event_ids)
                conn.commit()

        logger.info(
            "csv=%s, new_news=%d, stock_links=%d, tag_links=%d, event_links=%d",
            csv_path.name,
            total_news_new,
            total_news_link,
            total_tag_link,
            total_event_link,
        )

    def _connect(self) -> pymysql.connections.Connection:
        return pymysql.connect(
            **self.db_config,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )

    @staticmethod
    def _resolve_stock_id(conn, company_name: str) -> int | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM stocks
                WHERE name = %s
                  AND status = 'ACTIVE'
                LIMIT 1
                """,
                (company_name,),
            )
            row = cur.fetchone()
            return row["id"] if row else None

    @staticmethod
    def _load_pending_event_by_id(conn, event_id: int, stock_id: int) -> dict | None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, event_type, start_date, end_date
                FROM events
                WHERE id = %s
                  AND stock_id = %s
                  AND crawl_status = 'PENDING'
                LIMIT 1
                """,
                (event_id, stock_id),
            )
            return cur.fetchone()

    @staticmethod
    def _load_tag_ids_for_names(conn, tag_names: set[str]) -> dict[str, int]:
        if not tag_names:
            return {}

        placeholders = ",".join(["%s"] * len(tag_names))
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, name
                FROM tags
                WHERE status = 'ACTIVE'
                  AND name IN ({placeholders})
                """,
                list(tag_names),
            )
            return {row["name"]: row["id"] for row in cur.fetchall()}

    def _iter_csv_chunks(self, csv_path: Path) -> Iterator[list[dict]]:
        chunk: list[dict] = []

        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for raw in reader:
                normalized = self._normalize_csv_row(raw)
                chunk.append(normalized)

                if len(chunk) >= self.batch_size:
                    yield chunk
                    chunk = []

        if chunk:
            yield chunk

    @staticmethod
    def _normalize_csv_row(row: dict) -> dict:
        url = (row.get("url") or "").strip()
        stock_name = (row.get("stock_name") or "").strip()

        published_at_raw = (row.get("publishedAt") or row.get("published_at") or "").strip()
        published_at = _parse_dt(published_at_raw)
        published_date = datetime.strptime(published_at, NOW_FMT).date()

        event_id_raw = row.get("event_id")
        try:
            event_id = int(event_id_raw) if event_id_raw not in (None, "", "nan") else None
        except (TypeError, ValueError):
            event_id = None

        sentiment_raw = row.get("sentiment_score")
        try:
            sentiment_score = float(sentiment_raw) if sentiment_raw not in (None, "") else 0.0
        except (TypeError, ValueError):
            sentiment_score = 0.0

        return {
            **row,
            "sentiment_score": sentiment_score,
            "_url": url,
            "_stock_name": stock_name,
            "_event_id": event_id,
            "_published_at": published_at,
            "_published_date": published_date,
        }

    def _bulk_insert_news_ignore(self, conn, news_rows: list[tuple]) -> int:
        if not news_rows:
            return 0

        insert_sql = """
            INSERT IGNORE INTO news
                (created_at, updated_at, content, published_at,
                 source, status, thumbnail_url, title, url, sentiment_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        inserted_count = 0

        with conn.cursor() as cur:
            for i in range(0, len(news_rows), self.batch_size):
                batch = news_rows[i: i + self.batch_size]
                affected = cur.executemany(insert_sql, batch)
                inserted_count += affected

        return inserted_count

    def _load_url_to_id_by_urls(self, conn, urls: list[str]) -> dict[str, int]:
        if not urls:
            return {}

        unique_urls = list(dict.fromkeys(urls))
        result: dict[str, int] = {}

        with conn.cursor() as cur:
            for i in range(0, len(unique_urls), self.batch_size):
                chunk = unique_urls[i: i + self.batch_size]
                placeholders = ",".join(["%s"] * len(chunk))
                cur.execute(
                    f"""
                    SELECT id, url
                    FROM news
                    WHERE url IN ({placeholders})
                    """,
                    chunk,
                )
                for row in cur.fetchall():
                    result[row["url"]] = row["id"]

        return result

    def _bulk_insert_junction(
        self,
        conn,
        news_stocks_rows: list[tuple],
        news_tags_rows: list[tuple],
        event_news_rows: list[tuple],
    ) -> tuple[int, int, int]:
        inserted_ns = 0
        inserted_nt = 0
        inserted_en = 0

        with conn.cursor() as cur:
            if news_stocks_rows:
                inserted_ns = cur.executemany(
                    """
                    INSERT IGNORE INTO news_stocks
                        (created_at, updated_at, status, news_id, stock_id)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    news_stocks_rows,
                )

            if news_tags_rows:
                inserted_nt = cur.executemany(
                    """
                    INSERT IGNORE INTO news_tags
                        (created_at, updated_at, status, news_id, tag_id)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    news_tags_rows,
                )

            if event_news_rows:
                inserted_en = cur.executemany(
                    """
                    INSERT IGNORE INTO event_news
                        (created_at, updated_at, relevance_score, status, event_id, news_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    event_news_rows,
                )

        return inserted_ns, inserted_nt, inserted_en

    def _find_activatable_event_ids(self, conn, expected_event_urls: dict[int, set[str]]) -> list[int]:
        event_ids = list(expected_event_urls.keys())
        actual_counts = self._load_event_news_counts(conn, event_ids)

        result = []
        for event_id, urls in expected_event_urls.items():
            expected_count = len(urls)
            actual_count = actual_counts.get(event_id, 0)
            logger.info("이벤트 %d: 예상 뉴스=%d, 실제 뉴스=%d", event_id, expected_count, actual_count)
            if actual_count >= expected_count:
                result.append(event_id)
        return result

    def _load_event_news_counts(self, conn, event_ids: list[int]) -> dict[int, int]:
        if not event_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(event_ids))
        actual_counts = {event_id: 0 for event_id in event_ids}

        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT event_id, COUNT(DISTINCT news_id) AS count
                FROM event_news
                WHERE event_id IN ({placeholders})
                GROUP BY event_id
                """,
                event_ids,
            )
            for row in cur.fetchall():
                actual_counts[row["event_id"]] = row["count"]

        return actual_counts

    @staticmethod
    def _mark_events_active(conn, event_ids: list[int]) -> None:
        if not event_ids:
            return

        placeholders = ",".join(["%s"] * len(event_ids))
        with conn.cursor() as cur:
            cur.execute(
                f"""
                    UPDATE events
                    SET crawl_status = 'ACTIVE',
                        updated_at = %s
                    WHERE id IN ({placeholders})
                      AND crawl_status = 'PENDING'
                    """,
                [_now(), *event_ids],
            )