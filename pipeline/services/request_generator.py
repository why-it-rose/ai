from collections import defaultdict
from datetime import date, timedelta
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

    def connect(self) -> pymysql.connections.Connection:
        return self._connect()

    def generate_entire(self, conn=None) -> CrawlJobRequest:
        if conn is not None:
            events = self._load_crawl_target_events(conn)
            conn.commit()
            return self.build_targets(events)

        with self._connect() as own_conn:
            events = self._load_crawl_target_events(own_conn)
            own_conn.commit()
            return self.build_targets(events)

    def build_targets(self, rows: list[dict]) -> CrawlJobRequest:
        grouped = defaultdict(list)

        for row in rows:
            grouped[row["stock_name"]].append(
                CrawlPeriod(
                    event_id=row["event_id"],
                    fromDate=(row["start_date"] - timedelta(days=1)).strftime("%Y.%m.%d"),
                    toDate=(row["end_date"] + timedelta(days=1)).strftime("%Y.%m.%d"),
                )
            )

        return CrawlJobRequest(
            targets=[
                CrawlTarget(
                    stock=stock,
                    periods=sorted(periods, key=lambda x: (x.fromDate, x.toDate, x.event_id or 0)),
                )
                for stock, periods in sorted(grouped.items(), key=lambda x: x[0])
            ]
        )

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
                WHERE e.crawl_status = 'INACTIVE'
                  AND e.status = 'ACTIVE';
                """
            )
            return list(cur.fetchall())

    def try_mark_event_pending(self, event_id: int, conn=None) -> bool:
        if event_id is None:
            return True

        if conn is not None:
            return self._try_mark_event_pending(conn, event_id)

        with self._connect() as own_conn:
            return self._try_mark_event_pending(own_conn, event_id)

    @staticmethod
    def _try_mark_event_pending(conn, event_id: int) -> bool:
        with conn.cursor() as cur:
            affected = cur.execute(
                """
                UPDATE events
                SET crawl_status = 'PENDING'
                WHERE id = %s
                  AND crawl_status = 'INACTIVE'
                  AND status = 'ACTIVE'
                """,
                (event_id,),
            )
        conn.commit()
        return affected > 0

    def rollback_event_to_inactive(self, event_id: int, conn=None) -> None:
        if event_id is None:
            return

        if conn is not None:
            self._rollback_event_to_inactive(conn, event_id)
            return

        with self._connect() as own_conn:
            self._rollback_event_to_inactive(own_conn, event_id)

    @staticmethod
    def _rollback_event_to_inactive(conn, event_id: int) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE events
                SET crawl_status = 'INACTIVE'
                WHERE id = %s
                  AND crawl_status = 'PENDING'
                """,
                (event_id,),
            )
        conn.commit()

    def mark_event_active(self, event_id: int, conn=None) -> None:
        if event_id is None:
            return

        if conn is not None:
            self._mark_event_active(conn, event_id)
            return

        with self._connect() as own_conn:
            self._mark_event_active(own_conn, event_id)

    @staticmethod
    def _mark_event_active(conn, event_id: int) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE events
                SET crawl_status = 'ACTIVE'
                WHERE id = %s
                  AND crawl_status = 'PENDING'
                """,
                (event_id,),
            )
        conn.commit()

    def recover_pending_event(self, event_id: int, conn=None) -> None:
        if event_id is None:
            return

        if conn is not None:
            self._recover_pending_event(conn, event_id)
            return

        with self._connect() as own_conn:
            self._recover_pending_event(own_conn, event_id)

    @staticmethod
    def _recover_pending_event(conn, event_id: int) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS(
                    SELECT 1
                    FROM event_news
                    WHERE event_id = %s
                ) AS has_event_news
                """,
                (event_id,),
            )
            row = cur.fetchone()
            has_event_news = bool(row and row["has_event_news"])

            if has_event_news:
                cur.execute(
                    """
                    UPDATE events
                    SET crawl_status = 'ACTIVE'
                    WHERE id = %s
                      AND crawl_status = 'PENDING'
                    """,
                    (event_id,),
                )
            else:
                cur.execute(
                    """
                    UPDATE events
                    SET crawl_status = 'INACTIVE'
                    WHERE id = %s
                      AND crawl_status = 'PENDING'
                    """,
                    (event_id,),
                )
        conn.commit()

    def repair_pending_events(self) -> int:
        repaired = 0

        with self._connect() as own_conn:
            with own_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id
                    FROM events
                    WHERE crawl_status = 'PENDING'
                      AND status = 'ACTIVE'
                    """
                )
                pending_ids = [row["id"] for row in cur.fetchall()]

            own_conn.commit()

            for event_id in pending_ids:
                self._recover_pending_event(own_conn, event_id)
                repaired += 1

        return repaired

    def generate_today(self) -> CrawlJobRequest:
        today = date.today() - timedelta(days=1)

        with self._connect() as conn:
            stocks = self._load_today_target_stocks(conn)

        print(f"오늘의 타겟 주식 수: {len(stocks)}")
        return self._build_today_targets(stocks, today)

    def _build_today_targets(
        self,
        stocks: list[dict],
        target_date: date,
    ) -> CrawlJobRequest:
        date_str = target_date.strftime("%Y.%m.%d")

        return CrawlJobRequest(
            targets=[
                CrawlTarget(
                    stock=row["stock_name"],
                    periods=[
                        CrawlPeriod(
                            fromDate=date_str,
                            toDate=date_str,
                        )
                    ],
                )
                for row in stocks
            ]
        )

    @staticmethod
    def _load_today_target_stocks(conn) -> list[dict]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.name AS stock_name
                FROM stocks s
                WHERE s.status = 'ACTIVE';
                """
            )
            return list(cur.fetchall())