from pipeline.crawlers.yonhap_crawler import YonhapCrawler
from pipeline.schemas.news_response import NewsCrawlResponse, NewsItemResponse


class CrawlService:
    def __init__(self):
        self.crawler = YonhapCrawler()

    async def collect(self, targets) -> NewsCrawlResponse:
        all_news = []

        for target in targets:
            for period in target.periods:
                articles = await self.crawler.crawl(
                    keyword=target.keyword,
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
                all_news.extend(items)

        return NewsCrawlResponse(
            count=len(all_news),
            items=all_news,
        )
