from pydantic import BaseModel
from typing import List, Optional

class NewsItemResponse(BaseModel):
    title: str
    content: str
    url: str
    publishedAt: Optional[str] = None
    source: Optional[str] = None
    thumbnailUrl: Optional[str] = None

class NewsCrawlResponse(BaseModel):
    count: int
    items: List[NewsItemResponse]