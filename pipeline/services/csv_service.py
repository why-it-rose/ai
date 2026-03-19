import csv
import os
from typing import List

from pipeline.schemas.news_response import NewsItemResponse


class CsvService:
    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def write_stock_csv(
        self,
        news_list: List[NewsItemResponse],
        file_name: str = "news.csv",
    ) -> str:
        file_path = os.path.join(self.output_dir, file_name)

        with open(file_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "title",
                    "content",
                    "url",
                    "publishedAt",
                    "source",
                    "thumbnailUrl",
                ],
            )
            writer.writeheader()

            for news in news_list:
                writer.writerow(
                    {
                        "title": news.title,
                        "content": news.content,
                        "url": news.url,
                        "publishedAt": news.publishedAt,
                        "source": news.source,
                        "thumbnailUrl": news.thumbnailUrl,
                    }
                )

        return file_path