"""
transfer_service.py

CSV → DB 전송 서비스
- {기업명}.csv 파일을 읽어 news / news_stocks / news_tags / event_news 테이블에 적재
- tags / stocks / events 테이블은 이미 채워져 있다고 가정
"""

import csv
import os
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pymysql
import pymysql.cursors
from dotenv import dotenv_values
import json

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# ---------------------------------------------------------------------------
# 상수
# ---------------------------------------------------------------------------
STATUS_ACTIVE = "ACTIVE"
NOW_FMT = "%Y-%m-%d %H:%M:%S.%f"      # datetime(6) 포맷


def _now() -> str:
    return datetime.now().strftime(NOW_FMT)


def _parse_dt(value: str) -> str:
    """publishedAt 문자열을 DB datetime(6) 포맷으로 변환"""
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            dt = datetime.strptime(value.strip(), fmt)
            # timezone 제거 후 naive datetime으로 저장
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt.strftime(NOW_FMT)
        except ValueError:
            continue
    raise ValueError(f"날짜 파싱 실패: {value!r}")


# ---------------------------------------------------------------------------
# TransferService
# ---------------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def transfer_all(self, tagged_file_paths: list[str] | str) -> None:
        if isinstance(tagged_file_paths, str):
            tagged_file_paths = [tagged_file_paths]

        self.csv_files = [Path(p) for p in tagged_file_paths]

        logger.info("csv_files=%s", self.csv_files)

        if not self.csv_files:
            logger.warning("CSV 파일 경로가 없습니다.")
            return

        # 존재하지 않는 파일 사전 경고
        missing = [p for p in self.csv_files if not p.exists()]
        for p in missing:
            logger.warning("파일을 찾을 수 없습니다 → 건너뜀: %s", p)
            # 부모 디렉토리가 존재하면 실제 파일 목록을 출력해 비교
            if p.parent.exists():
                actual_files = [repr(f.name) for f in p.parent.iterdir()]
                logger.warning("  ↳ 디렉토리 내 실제 파일 목록: %s", actual_files)
                logger.warning("  ↳ 요청한 파일명 repr: %s", repr(p.name))
            else:
                logger.warning("  ↳ 디렉토리 자체가 없음: %s", p.parent)

        csv_files = [p for p in self.csv_files if p.exists()]
        if not csv_files:
            logger.warning("처리할 수 있는 CSV 파일이 없습니다.")
            return

        with self._connect() as conn:
            # ── 1) 공통 조회 (태그 / 종목 / 이벤트) ─────────────────────
            tag_map   = self._load_tag_map(conn)        # {tag_name: tag_id}
            stock_map = self._load_stock_map(conn)      # {stock_name: stock_id}
            event_map = self._load_event_map(conn)      # {stock_id: [event_row, ...]}

            # ── 2) CSV 파일별 처리 ────────────────────────────────────────
            for csv_path in csv_files:
                company_name = csv_path.stem           # 파일명에서 확장자 제거
                stock_id = stock_map.get(company_name)
                if stock_id is None:
                    logger.warning("stocks 테이블에 '%s' 없음 → 건너뜀", company_name)
                    continue

                logger.info("처리 시작: %s (stock_id=%s)", csv_path.name, stock_id)
                self._process_csv(conn, csv_path, stock_id, tag_map, event_map)
                logger.info("처리 완료: %s", csv_path.name)

    # ------------------------------------------------------------------
    # 내부 – DB 연결
    # ------------------------------------------------------------------
    def _connect(self) -> pymysql.connections.Connection:
        conn = pymysql.connect(
            **self.db_config,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )
        return conn

    # ------------------------------------------------------------------
    # 내부 – 사전 조회 (메모리 캐시)
    # ------------------------------------------------------------------
    @staticmethod
    def _load_tag_map(conn) -> dict[str, int]:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM tags WHERE status = 'ACTIVE'")
            return {row["name"]: row["id"] for row in cur.fetchall()}

    @staticmethod
    def _load_stock_map(conn) -> dict[str, int]:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM stocks WHERE status = 'ACTIVE'")
            return {row["name"]: row["id"] for row in cur.fetchall()}

    @staticmethod
    def _load_event_map(conn) -> dict[int, list[dict]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, stock_id, start_date, end_date
                FROM events
                WHERE status = 'ACTIVE'
                ORDER BY stock_id, start_date
                """
            )
            result: dict[int, list[dict]] = {}
            for row in cur.fetchall():
                result.setdefault(row["stock_id"], []).append(row)
            return result

    # ------------------------------------------------------------------
    # 내부 – CSV 한 파일 처리
    # ------------------------------------------------------------------
    def _process_csv(
        self,
        conn,
        csv_path: Path,
        stock_id: int,
        tag_map: dict[str, int],
        event_map: dict[int, list[dict]],
    ) -> None:

        rows = self._read_csv(csv_path)
        if not rows:
            return

        stock_events = event_map.get(stock_id, [])

        # 배치 버퍼
        news_rows:        list[tuple] = []
        news_stocks_rows: list[tuple] = []
        news_tags_rows:   list[tuple] = []
        event_news_rows:  list[tuple] = []

        all_urls = [r.get("url", "").strip() for r in rows]

        # 이미 DB에 있는 url → news_id (뉴스 중복 삽입 방지 + 매핑 재사용)
        existing_url_ids = self._load_existing_url_ids(conn, [u for u in all_urls if u])

        now = _now()

        # ── 신규 뉴스만 news 삽입 버퍼에 추가 ────────────────────────
        for row in rows:
            url = row.get("url", "").strip()
            if not url or url in existing_url_ids:
                continue

            published_at = _parse_dt(row["publishedAt"])
            news_rows.append((
                now, now,
                row.get("content", ""),
                published_at,
                row.get("source", ""),
                STATUS_ACTIVE,
                row.get("thumbnailUrl") or None,
                row.get("title", ""),
                url,
            ))

        # ── news 벌크 삽입 (신규만) ───────────────────────────────────
        if news_rows:
            inserted_news = self._bulk_insert_news(conn, news_rows)
            url_to_id = {**existing_url_ids, **{u: nid for u, nid in inserted_news}}
        else:
            logger.info("  → 신규 뉴스 없음 (기존 뉴스 매핑만 진행)")
            url_to_id = existing_url_ids

        # ── 이미 태그가 매핑된 news_id 조회 (1회만 매핑 보장) ────────
        all_news_ids = list(url_to_id.values())
        tagged_news_ids = self._load_tagged_news_ids(conn, all_news_ids)

        # ── 중계 테이블 데이터 구성 + batch_size마다 flush ───────────
        total_ns, total_nt, total_en = 0, 0, 0

        for row in rows:
            url     = row.get("url", "").strip()
            news_id = url_to_id.get(url)
            if not url or news_id is None:
                continue

            published_at_str = _parse_dt(row["publishedAt"])
            published_date   = datetime.strptime(published_at_str, NOW_FMT).date()

            # news_stocks
            news_stocks_rows.append((now, now, STATUS_ACTIVE, news_id, stock_id))

            # news_tags: 이미 태그가 매핑된 뉴스는 건너뜀 (1회만 매핑)
            if news_id not in tagged_news_ids:
                for pred_col in ("pred_major", "pred_sub"):
                    tag_name = row.get(pred_col, "").strip()
                    tag_id   = tag_map.get(tag_name)
                    if tag_id:
                        news_tags_rows.append((now, now, STATUS_ACTIVE, news_id, tag_id))
                    else:
                        logger.debug("태그 미존재: '%s'", tag_name)
                # 이번 배치에서 태그가 추가됐으면 중복 방지를 위해 바로 set에 등록
                if news_tags_rows:
                    tagged_news_ids.add(news_id)

            # event_news – publishedAt 이 event 기간에 속하는 event 매핑
            for event in stock_events:
                if event["start_date"] <= published_date <= event["end_date"]:
                    relevance_score = self._calc_relevance(row)
                    event_news_rows.append((
                        now, now,
                        relevance_score,
                        STATUS_ACTIVE,
                        event["id"],
                        news_id,
                    ))

            # 버퍼가 batch_size를 넘으면 즉시 flush (메모리 절약)
            if len(news_stocks_rows) >= self.batch_size:
                self._bulk_insert_junction(conn, news_stocks_rows, news_tags_rows, event_news_rows)
                total_ns += len(news_stocks_rows)
                total_nt += len(news_tags_rows)
                total_en += len(event_news_rows)
                news_stocks_rows.clear()
                news_tags_rows.clear()
                event_news_rows.clear()

        # ── 남은 버퍼 최종 flush ──────────────────────────────────────
        if news_stocks_rows or news_tags_rows or event_news_rows:
            self._bulk_insert_junction(conn, news_stocks_rows, news_tags_rows, event_news_rows)
            total_ns += len(news_stocks_rows)
            total_nt += len(news_tags_rows)
            total_en += len(event_news_rows)

        conn.commit()

        logger.info(
            "  → news_new=%d, news_existing=%d, news_stocks=%d, news_tags=%d, event_news=%d",
            len(news_rows), len(existing_url_ids), total_ns, total_nt, total_en,
        )

    # ------------------------------------------------------------------
    # 내부 – CSV 읽기
    # ------------------------------------------------------------------
    @staticmethod
    def _read_csv(csv_path: Path) -> list[dict]:
        rows = []
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        return rows

    # ------------------------------------------------------------------
    # 내부 – 기존 URL 조회 → {url: news_id} 반환, CHUNK_SIZE 단위로 분할 조회
    # ------------------------------------------------------------------
    def _load_existing_url_ids(self, conn, urls: list[str]) -> dict[str, int]:
        if not urls:
            return {}
        result: dict[str, int] = {}
        chunk_size = self.batch_size  # 기본 500
        with conn.cursor() as cur:
            for i in range(0, len(urls), chunk_size):
                chunk = urls[i : i + chunk_size]
                placeholders = ",".join(["%s"] * len(chunk))
                cur.execute(
                    f"SELECT id, url FROM news WHERE url IN ({placeholders})", chunk
                )
                for row in cur.fetchall():
                    result[row["url"]] = row["id"]
        return result

    # ------------------------------------------------------------------
    # 내부 – 매핑된 news_id set 반환
    # ------------------------------------------------------------------
    def _load_tagged_news_ids(self, conn, news_ids: list[int]) -> set[int]:
        """news_tags 에 1건이라도 존재하는 news_id를 반환 (태그 중복 매핑 방지)"""
        if not news_ids:
            return set()
        result: set[int] = set()
        with conn.cursor() as cur:
            for i in range(0, len(news_ids), self.batch_size):
                chunk = news_ids[i : i + self.batch_size]
                placeholders = ",".join(["%s"] * len(chunk))
                cur.execute(
                    f"""
                    SELECT DISTINCT news_id
                    FROM news_tags
                    WHERE news_id IN ({placeholders})
                    """,
                    chunk,
                )
                for row in cur.fetchall():
                    result.add(row["news_id"])
        return result

    # ------------------------------------------------------------------
    # 내부 – news 벌크 삽입 → (url, news_id) 리스트 반환
    #         INSERT 후 삽입된 url로 SELECT해 정확한 ID를 확보
    # ------------------------------------------------------------------
    def _bulk_insert_news(
        self, conn, news_rows: list[tuple]
    ) -> list[tuple[str, int]]:
        insert_sql = """
            INSERT INTO news
                (created_at, updated_at, content, published_at,
                 source, status, thumbnail_url, title, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        inserted: list[tuple[str, int]] = []
        with conn.cursor() as cur:
            for i in range(0, len(news_rows), self.batch_size):
                batch = news_rows[i : i + self.batch_size]
                cur.executemany(insert_sql, batch)

                # 삽입된 url 목록으로 정확한 id 조회
                batch_urls = [row[8] for row in batch]   # url은 9번째 컬럼
                placeholders = ",".join(["%s"] * len(batch_urls))
                cur.execute(
                    f"SELECT id, url FROM news WHERE url IN ({placeholders})",
                    batch_urls,
                )
                for row in cur.fetchall():
                    inserted.append((row["url"], row["id"]))
        return inserted

    # ------------------------------------------------------------------
    # 내부 – 중계 테이블 벌크 삽입 (IGNORE = 중복 무시)
    # ------------------------------------------------------------------
    def _bulk_insert_junction(
        self,
        conn,
        news_stocks_rows: list[tuple],
        news_tags_rows:   list[tuple],
        event_news_rows:  list[tuple],
    ) -> None:
        with conn.cursor() as cur:
            # news_stocks
            if news_stocks_rows:
                cur.executemany(
                    """
                    INSERT IGNORE INTO news_stocks
                        (created_at, updated_at, status, news_id, stock_id)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    news_stocks_rows,
                )

            # news_tags
            if news_tags_rows:
                cur.executemany(
                    """
                    INSERT IGNORE INTO news_tags
                        (created_at, updated_at, status, news_id, tag_id)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    news_tags_rows,
                )

            # event_news
            if event_news_rows:
                cur.executemany(
                    """
                    INSERT IGNORE INTO event_news
                        (created_at, updated_at, relevance_score, status, event_id, news_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    event_news_rows,
                )

    # ------------------------------------------------------------------
    # 내부 – relevance_score 계산
    # ------------------------------------------------------------------
    @staticmethod
    def _calc_relevance(row: dict) -> float:
        """pred_major_prob 과 pred_sub_prob 의 평균을 relevance_score 로 사용"""
        try:
            major = float(row.get("pred_major_prob") or 0)
            sub   = float(row.get("pred_sub_prob")   or 0)
            return round((major + sub) / 2, 4)
        except (ValueError, TypeError):
            return 0.0