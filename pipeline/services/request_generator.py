from collections import defaultdict
from pathlib import Path

import pymysql
from dotenv import dotenv_values

from pipeline.schemas.crawl_request import CrawlJobRequest, CrawlTarget, CrawlPeriod


class RequestGenerator:
    def __init__(self):
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

    def _connect(self) -> pymysql.connections.Connection:
        return pymysql.connect(
            **self.db_config,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )

    def generate(self) -> CrawlJobRequest:
        with self._connect() as conn:
            events = self._load_crawl_target_events(conn)
            event_ids = [row["event_id"] for row in events]

            if event_ids:
                self._mark_events_pending(conn, event_ids)
                conn.commit()

            return self.build_targets(events)

    @staticmethod
    def _load_crawl_target_events(conn) -> list[dict]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    e.id AS event_id,
                    s.name AS stock_name,
                    e.start_date,
                    e.end_date
                FROM events e
                JOIN stocks s ON s.id = e.stock_id
                WHERE e.crawl_status = 'INACTIVE';
                """
            )
            return list(cur.fetchall())
    def build_targets(self, rows: list[dict]) -> CrawlJobRequest:
        grouped = defaultdict(list)

        for row in rows:
            grouped[row["stock_name"]].append(
                CrawlPeriod(
                    event_id=row["event_id"],
                    fromDate=row["start_date"].strftime("%Y.%m.%d"),
                    toDate=row["end_date"].strftime("%Y.%m.%d"),
                )
            )

        return CrawlJobRequest(
            targets=[
                CrawlTarget(
                    stock=stock,
                    periods=periods,
                )
                for stock, periods in grouped.items()
            ]
        )

    @staticmethod
    def _mark_events_pending(conn, event_ids: list[int]) -> None:
        if not event_ids:
            return

        placeholders = ", ".join(["%s"] * len(event_ids))
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE events
                SET crawl_status = 'PENDING'
                WHERE id IN ({placeholders});
                """,
                event_ids,
            )