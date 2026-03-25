import csv
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import pymysql
import pymysql.cursors
from dotenv import dotenv_values

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

    def transfer_all(self, tagged_file_paths: list[str] | str) -> None:
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
                company_name = csv_path.stem
                stock_id = self._resolve_stock_id(conn, company_name)

                if stock_id is None:
                    logger.warning("stocks 테이블에 없는 종목입니다: %s", company_name)
                    continue

                logger.info("처리 시작: %s (stock_id=%s)", csv_path.name, stock_id)
                self._process_csv(conn, csv_path, stock_id)
                logger.info("처리 완료: %s", csv_path.name)

    def _process_csv(self, conn, csv_path: Path, stock_id: int) -> None:
        stock_events = self._load_active_events_for_stock(conn, stock_id)

        total_news_new = 0
        total_news_link = 0
        total_tag_link = 0
        total_event_link = 0
        chunk_count = 0

        for rows in self._iter_csv_chunks(csv_path):
            chunk_count += 1
            now = _now()

            tag_names = set()
            for row in rows:
                for pred_col in ("pred_major", "pred_sub"):
                    tag_name = (row.get(pred_col) or "").strip()
                    if tag_name:
                        tag_names.add(tag_name)

            tag_map = self._load_tag_ids_for_names(conn, tag_names)

            # 1) news 본테이블: 청크 내부 URL 중복만 제거해서 INSERT IGNORE
            news_rows: list[tuple] = []
            seen_chunk_urls: set[str] = set()

            for row in rows:
                url = row["_url"]
                if url in seen_chunk_urls:
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
                    )
                )

            inserted_count = self._bulk_insert_news_ignore(conn, news_rows)
            total_news_new += inserted_count

            # 2) 방금 청크의 URL들만 다시 조회해서 url -> news_id 확보
            url_to_id = self._load_url_to_id_by_urls(conn, list(seen_chunk_urls))

            if not url_to_id:
                conn.commit()
                logger.info("  → chunk=%d 매핑 가능한 news_id 없음", chunk_count)
                continue

            # 3) junction row 생성
            news_stocks_rows: list[tuple] = []
            news_tags_rows: list[tuple] = []
            event_news_rows: list[tuple] = []

            # 같은 청크 내부 중복만 얕게 제거
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
                        logger.debug("태그 미존재: '%s'", tag_name)
                        continue

                    news_tag_pair = (news_id, tag_id)
                    if news_tag_pair not in seen_news_tag_pairs:
                        news_tags_rows.append((now, now, STATUS_ACTIVE, news_id, tag_id))
                        seen_news_tag_pairs.add(news_tag_pair)

                published_date = row["_published_date"]
                relevance_score = self._calc_relevance(row)

                for event in stock_events:
                    if event["start_date"] <= published_date <= event["end_date"]:
                        event_news_pair = (event["id"], news_id)
                        if event_news_pair not in seen_event_news_pairs:
                            event_news_rows.append(
                                (
                                    now,
                                    now,
                                    relevance_score,
                                    STATUS_ACTIVE,
                                    event["id"],
                                    news_id,
                                )
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

            logger.info(
                "  → chunk=%d news_new=%d, news_stocks=%d, news_tags=%d, event_news=%d",
                chunk_count,
                inserted_count,
                inserted_ns,
                inserted_nt,
                inserted_en,
            )

        logger.info(
            "  → total news_new=%d, news_stocks=%d, news_tags=%d, event_news=%d",
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
    def _load_active_events_for_stock(conn, stock_id: int) -> list[dict]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, start_date, end_date
                FROM events
                WHERE stock_id = %s
                  AND status = 'ACTIVE'
                ORDER BY start_date
                """,
                (stock_id,),
            )
            return list(cur.fetchall())

    @staticmethod
    def _load_tag_ids_for_names(conn, tag_names: set[str]) -> dict[str, int]:
        if not tag_names:
            return {}

        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(tag_names))
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

        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            for row in reader:
                url = (row.get("url") or "").strip()
                if not url:
                    continue

                published_at = _parse_dt(row["publishedAt"])
                published_date = datetime.strptime(published_at, NOW_FMT).date()

                normalized = dict(row)
                normalized["_url"] = url
                normalized["_published_at"] = published_at
                normalized["_published_date"] = published_date
                chunk.append(normalized)

                if len(chunk) >= self.batch_size:
                    yield chunk
                    chunk = []

        if chunk:
            yield chunk

    def _bulk_insert_news_ignore(self, conn, news_rows: list[tuple]) -> int:
        if not news_rows:
            return 0

        insert_sql = """
            INSERT IGNORE INTO news
                (created_at, updated_at, content, published_at,
                 source, status, thumbnail_url, title, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        inserted_count = 0

        with conn.cursor() as cur:
            for i in range(0, len(news_rows), self.batch_size):
                batch = news_rows[i : i + self.batch_size]
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
                chunk = unique_urls[i : i + self.batch_size]
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

    @staticmethod
    def _calc_relevance(row: dict) -> float:
        try:
            major = float(row.get("pred_major_prob") or 0)
            sub = float(row.get("pred_sub_prob") or 0)
            return round((major + sub) / 2, 4)
        except (ValueError, TypeError):
            return 0.0