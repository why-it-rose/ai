from abc import ABC, abstractmethod


class BaseCrawler(ABC):
    @abstractmethod
    def crawl(
        self,
        keyword: str | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
        limit: int = 10,
    ) -> list[dict]:
        raise NotImplementedError