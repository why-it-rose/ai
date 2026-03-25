import csv
import os
from typing import List
from pipeline.schemas.news_response import StockNewsResponse


class CsvService:
    def __init__(self, output_dir: str = "../files/crawled"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def write_stock_csv(
            self,
            stocks: List[StockNewsResponse],
    ) -> List[str]:

        file_paths = []
        for stock in stocks:
            file_path = os.path.join(self.output_dir, stock.stock_name + ".csv")
            file_paths.append(file_path)

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

                for news in stock.news:
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

        return file_paths
