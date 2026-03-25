from pydantic import BaseModel
from typing import Optional


class NewsCrawlRequest(BaseModel):
    source: str
    stock: Optional[str] = None
    fromDate: Optional[str] = None
    toDate: Optional[str] = None
    limit: int = 50
