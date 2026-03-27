import csv
import os
import re
from typing import Optional


class CsvService:
    def __init__(self, output_dir: str = "../files/crawled"):
        self.output_dir = os.path.abspath(output_dir)
        os.makedirs(self.output_dir, exist_ok=True)

    def build_unit_csv_path(
        self,
        stock_name: str,
        from_date: str,
        to_date: str,
        event_id: Optional[int] = None,
    ) -> str:
        safe_stock = self._sanitize_filename(stock_name)
        safe_from = from_date.replace(".", "")
        safe_to = to_date.replace(".", "")

        if event_id is not None:
            file_name = f"{safe_stock}_event_{event_id}_{safe_from}_{safe_to}.csv"
        else:
            file_name = f"{safe_stock}_{safe_from}_{safe_to}.csv"

        return os.path.join(self.output_dir, file_name)

    def write_unit_csv(
        self,
        stock_name: str,
        from_date: str,
        to_date: str,
        news: list[dict],
        event_id: Optional[int] = None,
        overwrite: bool = False,
    ) -> str:
        file_path = self.build_unit_csv_path(
            stock_name=stock_name,
            from_date=from_date,
            to_date=to_date,
            event_id=event_id,
        )

        if os.path.exists(file_path) and not overwrite:
            return file_path

        with open(file_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "event_id",
                    "stock_name",
                    "fromDate",
                    "toDate",
                    "title",
                    "content",
                    "url",
                    "publishedAt",
                    "source",
                    "thumbnailUrl",
                ],
            )
            writer.writeheader()

            for row in news:
                writer.writerow(
                    {
                        "event_id": row.get("event_id"),
                        "stock_name": row.get("stock_name"),
                        "fromDate": row.get("fromDate"),
                        "toDate": row.get("toDate"),
                        "title": row.get("title", ""),
                        "content": row.get("content", ""),
                        "url": row.get("url", ""),
                        "publishedAt": row.get("publishedAt"),
                        "source": row.get("source"),
                        "thumbnailUrl": row.get("thumbnailUrl"),
                    }
                )

        return file_path

    @staticmethod
    def _sanitize_filename(value: str) -> str:
        value = value.strip()
        return re.sub(r'[\\/:*?"<>|]+', "_", value)