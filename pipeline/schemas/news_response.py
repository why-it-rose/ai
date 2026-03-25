from pydantic import BaseModel
from typing import List, Optional


class NewsItemResponse(BaseModel):
    title: str
    content: str
    url: str
    publishedAt: Optional[str] = None
    source: Optional[str] = None
    thumbnailUrl: Optional[str] = None


class StockNewsResponse(BaseModel):
    stock_name: str
    news: List[NewsItemResponse]


class NewsCrawlResponse(BaseModel):
    count: int
    stocks: List[StockNewsResponse]
