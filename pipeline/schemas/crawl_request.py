from pydantic import BaseModel
from typing import List, Optional


class CrawlPeriod(BaseModel):
    event_id: Optional[int] = None
    fromDate: str
    toDate: str


class CrawlTarget(BaseModel):
    stock: str
    periods: List[CrawlPeriod]


class CrawlJobRequest(BaseModel):
    targets: List[CrawlTarget]
