import logging
from pathlib import Path

import pymysql
import pymysql.cursors
import torch
from dotenv import dotenv_values
from transformers import AutoModelForSequenceClassification, AutoTokenizer

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

STATUS_ACTIVE = "ACTIVE"


class NewsSentimentService:
    def __init__(
        self,
        batch_size: int = 64,
        model_name: str = "snunlp/KR-FinBert-SC",
        max_length: int = 256,
        body_max_chars: int = 2000,
        use_body: bool = True,
        title_weight: float = 0.6,
        body_weight: float = 0.4,
    ):
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
        self._validate_db_config()

        self.batch_size = batch_size
        self.model_name = model_name
        self.max_length = max_length
        self.body_max_chars = body_max_chars
        self.use_body = use_body

        weight_sum = title_weight + body_weight
        if weight_sum <= 0:
            raise ValueError("title_weight + body_weight must be > 0")

        self.title_weight = title_weight / weight_sum
        self.body_weight = body_weight / weight_sum

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        logger.info("device=%s", self.device)

        logger.info("loading model: %s", self.model_name)
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
        self.model.to(self.device)
        self.model.eval()

        self.id2label = getattr(self.model.config, "id2label", None)
        logger.info("id2label=%s", self.id2label)

    def fill_missing_sentiment_scores(self, limit: int | None = None) -> int:
        total_updated = 0

        with self._connect() as conn:
            while True:
                fetch_size = self.batch_size if limit is None else min(self.batch_size, limit)
                if fetch_size <= 0:
                    break

                rows = self._load_news_without_sentiment(conn, fetch_size)
                if not rows:
                    logger.info("sentiment_score가 NULL인 뉴스가 없습니다.")
                    break

                titles = [self._normalize_text(row.get("title")) for row in rows]
                bodies = [self._normalize_text(row.get("content"), self.body_max_chars) for row in rows]

                logger.info("title inference start: rows=%d", len(rows))
                title_results = self._predict_sentiment_batch(titles)

                if self.use_body:
                    logger.info("body inference start: rows=%d", len(rows))
                    body_results = self._predict_sentiment_batch(bodies)
                else:
                    body_results = [self._empty_sentiment() for _ in rows]

                update_rows: list[tuple[float, int]] = []
                for idx, row in enumerate(rows):
                    final_score = self._merge_scores(
                        title_results[idx]["sentiment_score"],
                        body_results[idx]["sentiment_score"],
                    )
                    update_rows.append((final_score, row["id"]))

                updated_count = self._bulk_update_sentiment_scores(conn, update_rows)
                conn.commit()

                total_updated += updated_count
                logger.info(
                    "batch done: fetched=%d, updated=%d, total_updated=%d",
                    len(rows),
                    updated_count,
                    total_updated,
                )

                if limit is not None:
                    limit -= len(rows)
                    if limit <= 0:
                        break

        logger.info("sentiment update finished: total_updated=%d", total_updated)
        return total_updated

    def _connect(self) -> pymysql.connections.Connection:
        return pymysql.connect(
            **self.db_config,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )

    def _validate_db_config(self) -> None:
        if not self.db_config["user"]:
            raise ValueError("DB_USER가 비어 있습니다.")
        if self.db_config["password"] is None or self.db_config["password"] == "":
            raise ValueError("DB_PASSWORD가 비어 있습니다.")
        if not self.db_config["database"]:
            raise ValueError("DB_NAME이 비어 있습니다.")

    def _load_news_without_sentiment(self, conn, limit: int) -> list[dict]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, title, content
                FROM news
                WHERE status = %s
                  AND sentiment_score IS NULL
                ORDER BY id
                LIMIT %s
                """,
                (STATUS_ACTIVE, limit),
            )
            return list(cur.fetchall())

    def _predict_sentiment_batch(self, texts: list[str]) -> list[dict]:
        texts = [text if isinstance(text, str) and text.strip() else "" for text in texts]
        results: list[dict] = []

        for start in range(0, len(texts), self.batch_size):
            batch = texts[start:start + self.batch_size]

            inputs = self.tokenizer(
                batch,
                return_tensors="pt",
                truncation=True,
                padding=True,
                max_length=self.max_length,
            )
            inputs = {k: v.to(self.device) for k, v in inputs.items()}

            with torch.no_grad():
                outputs = self.model(**inputs)
                probs = torch.softmax(outputs.logits, dim=-1).cpu().numpy()

            for row_probs in probs:
                negative = float(row_probs[0])
                neutral = float(row_probs[1])
                positive = float(row_probs[2])

                label = max(
                    [("negative", negative), ("neutral", neutral), ("positive", positive)],
                    key=lambda x: x[1],
                )[0]

                sentiment_score = self._clamp_score(positive - negative)

                results.append(
                    {
                        "label": label,
                        "negative": negative,
                        "neutral": neutral,
                        "positive": positive,
                        "sentiment_score": sentiment_score,
                    }
                )

        return results

    def _bulk_update_sentiment_scores(self, conn, update_rows: list[tuple[float, int]]) -> int:
        if not update_rows:
            return 0

        updated_count = 0

        with conn.cursor() as cur:
            for i in range(0, len(update_rows), self.batch_size):
                batch = update_rows[i:i + self.batch_size]
                affected = cur.executemany(
                    """
                    UPDATE news
                    SET sentiment_score = %s,
                        updated_at = NOW(6)
                    WHERE id = %s
                      AND sentiment_score IS NULL
                    """,
                    batch,
                )
                updated_count += affected

        return updated_count

    def _merge_scores(self, title_score: float, body_score: float) -> float:
        if not self.use_body:
            return self._clamp_score(title_score)

        merged = (title_score * self.title_weight) + (body_score * self.body_weight)
        return self._clamp_score(merged)

    @staticmethod
    def _normalize_text(value: str | None, max_chars: int | None = None) -> str:
        text = (value or "").strip()
        if max_chars is not None:
            text = text[:max_chars]
        return text

    @staticmethod
    def _empty_sentiment() -> dict:
        return {
            "label": "neutral",
            "negative": 0.0,
            "neutral": 1.0,
            "positive": 0.0,
            "sentiment_score": 0.0,
        }

    @staticmethod
    def _clamp_score(score: float) -> float:
        if score > 1.0:
            score = 1.0
        elif score < -1.0:
            score = -1.0
        return round(float(score), 5)


if __name__ == "__main__":
    service = NewsSentimentService(
        batch_size=64,
        model_name="snunlp/KR-FinBert-SC",
        max_length=256,
        body_max_chars=500,
        use_body=True,
        title_weight=0.6,
        body_weight=0.4,
    )
    service.fill_missing_sentiment_scores()