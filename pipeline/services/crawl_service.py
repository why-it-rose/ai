from pipeline.crawlers.yonhap_crawler import YonhapCrawler
from pipeline.schemas.news_response import NewsCrawlResponse, NewsItemResponse


class CrawlService:
    def __init__(self):
        self.crawler = YonhapCrawler()

    async def collect(self, targets) -> NewsCrawlResponse:
        stocks = []
        for target in targets:
            news = []
            for period in target.periods:
                # 이벤트 1개별
                articles = await self.crawler.crawl(
                    stock=target.stock,
                    fromDate=period.fromDate,
                    toDate=period.toDate,
                )

                items = [
                    NewsItemResponse(
                        title=article["title"],
                        content=article["content"],
                        url=article["url"],
                        publishedAt=article.get("published_at"),
                        source=article.get("source"),
                        thumbnailUrl=article.get("thumbnail_url"),
                    )
                    for article in articles
                ]
                news.extend(items)
            stocks.append({
                "stock_name": target.stock,
                "news": news,
            })

        return NewsCrawlResponse(
            count=len(stocks),
            stocks=stocks,
        )
