from pipeline.crawlers.yonhap_crawler import YonhapCrawler


class CrawlService:
    def __init__(self):
        self.crawler = YonhapCrawler()

    async def collect_unit(self, stock: str, period) -> dict:
        articles = await self.crawler.crawl(
            stock=stock,
            fromDate=period.fromDate,
            toDate=period.toDate,
        )

        news = []
        for article in articles:
            news.append(
                {
                    "event_id": getattr(period, "event_id", None),
                    "stock_name": stock,
                    "fromDate": period.fromDate,
                    "toDate": period.toDate,
                    "title": article.get("title", ""),
                    "content": article.get("content", ""),
                    "url": article.get("url", ""),
                    "publishedAt": article.get("published_at"),
                    "source": article.get("source"),
                    "thumbnailUrl": article.get("thumbnail_url"),
                }
            )

        return {
            "event_id": getattr(period, "event_id", None),
            "stock_name": stock,
            "fromDate": period.fromDate,
            "toDate": period.toDate,
            "news": news,
        }