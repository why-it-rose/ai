import csv
import logging
from pathlib import Path

import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer
import os

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUTPUT_DIR = os.path.abspath(
    os.path.join(BASE_DIR, "../../files/senti")
)

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
        output_dir: str | Path | None = DEFAULT_OUTPUT_DIR,
    ):
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

        self.output_dir = Path(output_dir) if output_dir else None

    def fill_missing_sentiment_scores(self, tagged_file_paths: list[str] | str) -> list[str]:
        if isinstance(tagged_file_paths, str):
            tagged_file_paths = [tagged_file_paths]

        csv_paths = [Path(path) for path in tagged_file_paths]
        ready_paths: list[str] = []

        for csv_path in csv_paths:
            if not csv_path.exists():
                logger.warning("파일을 찾을 수 없어 건너뜁니다: %s", csv_path)
                continue

            ready_path = self._build_output_path(csv_path)
            logger.info("sentiment csv processing start: input=%s output=%s", csv_path, ready_path)

            rows = self._read_csv_rows(csv_path)
            if not rows:
                logger.info("빈 CSV 입니다: %s", csv_path)
                self._write_csv_rows(ready_path, [], fieldnames=[])
                ready_paths.append(str(ready_path))
                continue

            self._fill_rows_with_sentiment(rows)
            fieldnames = self._build_fieldnames(rows, original_fieldnames=list(rows[0].keys()))
            self._write_csv_rows(ready_path, rows, fieldnames)

            ready_paths.append(str(ready_path))
            logger.info("sentiment csv processing done: rows=%d path=%s", len(rows), ready_path)

        return ready_paths

    def _fill_rows_with_sentiment(self, rows: list[dict]) -> None:
        titles = [self._normalize_text(row.get("title")) for row in rows]
        bodies = [
            self._normalize_text(
                row.get("content") or row.get("body") or row.get("article") or "",
                self.body_max_chars,
            )
            for row in rows
        ]

        logger.info("title inference start: rows=%d", len(rows))
        title_results = self._predict_sentiment_batch(titles)

        if self.use_body:
            logger.info("body inference start: rows=%d", len(rows))
            body_results = self._predict_sentiment_batch(bodies)
        else:
            body_results = [self._empty_sentiment() for _ in rows]

        for idx, row in enumerate(rows):
            title_result = title_results[idx]
            body_result = body_results[idx]

            final_score = self._merge_scores(
                title_result["sentiment_score"],
                body_result["sentiment_score"],
            )

            final_label = self._resolve_final_label(final_score)

            row["sentiment_score"] = self._format_score(final_score)
            row["sentiment_label"] = final_label
            row["title_sentiment_score"] = self._format_score(title_result["sentiment_score"])
            row["body_sentiment_score"] = self._format_score(body_result["sentiment_score"])

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

    def _read_csv_rows(self, csv_path: Path) -> list[dict]:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            return [dict(row) for row in reader]

    def _write_csv_rows(self, output_path: Path, rows: list[dict], fieldnames: list[str]) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with output_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for row in rows:
                safe_row = {field: row.get(field, "") for field in fieldnames}
                writer.writerow(safe_row)

    def _build_output_path(self, input_path: Path) -> Path:
        if self.output_dir:
            return self.output_dir / input_path.name
        return input_path

    @staticmethod
    def _build_fieldnames(rows: list[dict], original_fieldnames: list[str]) -> list[str]:
        extra_fields = [
            "sentiment_score",
            "sentiment_label",
            "title_sentiment_score",
            "body_sentiment_score",
        ]

        fieldnames = list(original_fieldnames)
        for field in extra_fields:
            if field not in fieldnames:
                fieldnames.append(field)
        return fieldnames

    def _merge_scores(self, title_score: float, body_score: float) -> float:
        if not self.use_body:
            return self._clamp_score(title_score)

        merged = (title_score * self.title_weight) + (body_score * self.body_weight)
        return self._clamp_score(merged)

    @staticmethod
    def _resolve_final_label(score: float) -> str:
        if score >= 0.15:
            return "positive"
        if score <= -0.15:
            return "negative"
        return "neutral"

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

    @staticmethod
    def _format_score(score: float) -> str:
        return f"{float(score):.5f}"


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

    paths = service.fill_missing_sentiment_scores(
        [
            "files/tagged/삼성전자.csv",
            "files/tagged/SK하이닉스.csv",
        ]
    )
    print(paths)