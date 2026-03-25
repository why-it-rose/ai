import csv
import logging
from datetime import datetime, timezone
from pathlib import Path

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

        # DB연결 및 사전 로딩
        with self._connect() as conn:
            tag_map = self._load_tag_map(conn)
            stock_map = self._load_stock_map(conn)
            event_map = self._load_event_map(conn)

            for csv_path in exists_files:
                company_name = csv_path.stem
                stock_id = stock_map.get(company_name)

                if stock_id is None:
                    logger.warning("잘못된 종목이 입력되었습니다.", company_name)
                    continue

                logger.info("처리 시작: %s (stock_id=%s)", csv_path.name, stock_id)
                self._process_csv(conn, csv_path, stock_id, tag_map, event_map)
                logger.info("처리 완료: %s", csv_path.name)

    def _connect(self) -> pymysql.connections.Connection:
        return pymysql.connect(
            **self.db_config,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )

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
            logger.info("빈 CSV입니다.: %s", csv_path.name)
            return

        # stock_id에 해당하는 event들 가져오기
        stock_events = event_map.get(stock_id, [])
        now = _now()

        # url컬럼 추출
        urls = [row["_url"] for row in rows]
        existing_url_ids = self._load_existing_url_ids(conn, urls)

        news_rows: list[tuple] = []
        seen_new_urls: set[str] = set()

        for row in rows:
            url = row["_url"]
            # 잘못된 url, 이미 DB에 있는 url, 이번 for문에 나왔던 url은 생략
            if not url or url in existing_url_ids or url in seen_new_urls:
                continue

            seen_new_urls.add(url)
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

        if news_rows:
            inserted_news = self._bulk_insert_news(conn, news_rows)
            url_to_id = {**existing_url_ids, **{url: news_id for url, news_id in inserted_news}}
        else:
            logger.info("  → 신규 뉴스 없음")
            url_to_id = existing_url_ids

        all_news_ids = list(url_to_id.values())
        if not all_news_ids:
            logger.info("  → 매핑 가능한 news_id 없음")
            conn.commit()
            return

        existing_news_stock_pairs = self._load_existing_news_stock_pairs(conn, all_news_ids, stock_id)
        existing_news_tag_pairs = self._load_existing_news_tag_pairs(conn, all_news_ids)

        target_event_ids = [event["id"] for event in stock_events]
        existing_event_news_pairs = self._load_existing_event_news_pairs(conn, all_news_ids, target_event_ids)

        news_stocks_rows: list[tuple] = []
        news_tags_rows: list[tuple] = []
        event_news_rows: list[tuple] = []

        total_ns, total_nt, total_en = 0, 0, 0

        for row in rows:
            url = row["_url"]
            news_id = url_to_id.get(url)
            if not news_id:
                continue

            # news - stock 쌍이 존재하지 않으면 추가
            news_stock_pair = (news_id, stock_id)
            if news_stock_pair not in existing_news_stock_pairs:
                news_stocks_rows.append((now, now, STATUS_ACTIVE, news_id, stock_id))
                existing_news_stock_pairs.add(news_stock_pair)

            # news - tag 쌍이 존재하지 않으면 추가
            for pred_col in ("pred_major", "pred_sub"):
                tag_name = (row.get(pred_col) or "").strip()
                if not tag_name:
                    continue

                tag_id = tag_map.get(tag_name)
                if not tag_id:
                    logger.debug("태그 미존재: '%s'", tag_name)
                    continue

                news_tag_pair = (news_id, tag_id)
                if news_tag_pair not in existing_news_tag_pairs:
                    news_tags_rows.append((now, now, STATUS_ACTIVE, news_id, tag_id))
                    existing_news_tag_pairs.add(news_tag_pair)

            published_date = row["_published_date"]

            # 연관도 점수 추가
            relevance_score = self._calc_relevance(row)

            # 이벤트 기간에 해당하고, event-news 쌍이 존재하지 않으면 추가
            for event in stock_events:
                if event["start_date"] <= published_date <= event["end_date"]:
                    event_news_pair = (event["id"], news_id)
                    if event_news_pair not in existing_event_news_pairs:
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
                        existing_event_news_pairs.add(event_news_pair)

            # 개수 쌓이면 insert
            if (
                len(news_stocks_rows) >= self.batch_size
                or len(news_tags_rows) >= self.batch_size
                or len(event_news_rows) >= self.batch_size
            ):
                self._bulk_insert_junction(conn, news_stocks_rows, news_tags_rows, event_news_rows)
                total_ns += len(news_stocks_rows)
                total_nt += len(news_tags_rows)
                total_en += len(event_news_rows)
                news_stocks_rows.clear()
                news_tags_rows.clear()
                event_news_rows.clear()

        # 끝나고 남은 것들 insert
        if news_stocks_rows or news_tags_rows or event_news_rows:
            self._bulk_insert_junction(conn, news_stocks_rows, news_tags_rows, event_news_rows)
            total_ns += len(news_stocks_rows)
            total_nt += len(news_tags_rows)
            total_en += len(event_news_rows)

        conn.commit()

        logger.info(
            "  → news_new=%d, news_existing=%d, news_stocks=%d, news_tags=%d, event_news=%d",
            len(news_rows),
            len(existing_url_ids),
            total_ns,
            total_nt,
            total_en,
        )

    @staticmethod
    def _read_csv(csv_path: Path) -> list[dict]:
        rows: list[dict] = []
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
                rows.append(normalized)
        return rows

    def _load_existing_url_ids(self, conn, urls: list[str]) -> dict[str, int]:
        if not urls:
            return {}

        result: dict[str, int] = {}
        unique_urls = list(dict.fromkeys(urls))

        with conn.cursor() as cur:
            # 배치 사이즈 단위로 DB 적재
            for i in range(0, len(unique_urls), self.batch_size):
                chunk = unique_urls[i : i + self.batch_size]
                placeholders = ",".join(["%s"] * len(chunk))
                cur.execute(
                    f"SELECT id, url FROM news WHERE url IN ({placeholders})",
                    chunk,
                )
                for row in cur.fetchall():
                    result[row["url"]] = row["id"]
        return result

    def _load_existing_news_stock_pairs(
        self,
        conn,
        news_ids: list[int],
        stock_id: int,
    ) -> set[tuple[int, int]]:
        if not news_ids:
            return set()

        result: set[tuple[int, int]] = set()
        with conn.cursor() as cur:
            for i in range(0, len(news_ids), self.batch_size):
                chunk = news_ids[i : i + self.batch_size]
                placeholders = ",".join(["%s"] * len(chunk))
                cur.execute(
                    f"""
                    SELECT news_id, stock_id
                    FROM news_stocks
                    WHERE stock_id = %s
                      AND news_id IN ({placeholders})
                    """,
                    [stock_id, *chunk],
                )
                for row in cur.fetchall():
                    result.add((row["news_id"], row["stock_id"]))
        return result

    def _load_existing_news_tag_pairs(
        self,
        conn,
        news_ids: list[int],
    ) -> set[tuple[int, int]]:
        if not news_ids:
            return set()

        result: set[tuple[int, int]] = set()
        with conn.cursor() as cur:
            for i in range(0, len(news_ids), self.batch_size):
                chunk = news_ids[i : i + self.batch_size]
                placeholders = ",".join(["%s"] * len(chunk))
                cur.execute(
                    f"""
                    SELECT news_id, tag_id
                    FROM news_tags
                    WHERE news_id IN ({placeholders})
                    """,
                    chunk,
                )
                for row in cur.fetchall():
                    result.add((row["news_id"], row["tag_id"]))
        return result

    def _load_existing_event_news_pairs(
        self,
        conn,
        news_ids: list[int],
        event_ids: list[int],
    ) -> set[tuple[int, int]]:
        if not news_ids or not event_ids:
            return set()

        result: set[tuple[int, int]] = set()
        with conn.cursor() as cur:
            for ni in range(0, len(news_ids), self.batch_size):
                news_chunk = news_ids[ni : ni + self.batch_size]
                news_placeholders = ",".join(["%s"] * len(news_chunk))

                for ei in range(0, len(event_ids), self.batch_size):
                    event_chunk = event_ids[ei : ei + self.batch_size]
                    event_placeholders = ",".join(["%s"] * len(event_chunk))

                    # 이벤트-뉴스 쌍이 존재하면 가져오기
                    cur.execute(
                        f"""
                        SELECT event_id, news_id
                        FROM event_news
                        WHERE news_id IN ({news_placeholders})
                          AND event_id IN ({event_placeholders})
                        """,
                        [*news_chunk, *event_chunk],
                    )
                    for row in cur.fetchall():
                        result.add((row["event_id"], row["news_id"]))
        return result

    def _bulk_insert_news(self, conn, news_rows: list[tuple]) -> list[tuple[str, int]]:
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

                #new테이블에 넣었던 url들 다시 조회해서 inserted 리스트에 (url, id) 형태로 저장
                batch_urls = [row[8] for row in batch]
                placeholders = ",".join(["%s"] * len(batch_urls))
                cur.execute(
                    f"SELECT id, url FROM news WHERE url IN ({placeholders})",
                    batch_urls,
                )
                for row in cur.fetchall():
                    inserted.append((row["url"], row["id"]))

        return inserted

    def _bulk_insert_junction(
        self,
        conn,
        news_stocks_rows: list[tuple],
        news_tags_rows: list[tuple],
        event_news_rows: list[tuple],
    ) -> None:
        with conn.cursor() as cur:
            if news_stocks_rows:
                cur.executemany(
                    """
                    INSERT IGNORE INTO news_stocks
                        (created_at, updated_at, status, news_id, stock_id)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    news_stocks_rows,
                )

            if news_tags_rows:
                cur.executemany(
                    """
                    INSERT IGNORE INTO news_tags
                        (created_at, updated_at, status, news_id, tag_id)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    news_tags_rows,
                )

            if event_news_rows:
                cur.executemany(
                    """
                    INSERT IGNORE INTO event_news
                        (created_at, updated_at, relevance_score, status, event_id, news_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    event_news_rows,
                )

    @staticmethod
    def _calc_relevance(row: dict) -> float:
        # TODO: 연관도 점수 기준 정한 후 개선 필요
        try:
            major = float(row.get("pred_major_prob") or 0)
            sub = float(row.get("pred_sub_prob") or 0)
            return round((major + sub) / 2, 4)
        except (ValueError, TypeError):
            return 0.0