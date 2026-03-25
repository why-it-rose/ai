from pydantic import BaseModel
from typing import List


class CrawlPeriod(BaseModel):
    event_id: int
    fromDate: str
    toDate: str


class CrawlTarget(BaseModel):
    stock: str
    periods: List[CrawlPeriod]


class CrawlJobRequest(BaseModel):
    targets: List[CrawlTarget]
