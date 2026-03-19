from pydantic import BaseModel
from typing import List


class CrawlPeriod(BaseModel):
    fromDate: str
    toDate: str

class CrawlTarget(BaseModel):
    keyword: str
    periods: List[CrawlPeriod]

class CrawlJobRequest(BaseModel):
    targets: List[CrawlTarget]