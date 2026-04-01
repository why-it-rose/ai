import asyncio
import logging
import os
import re
from datetime import datetime
from typing import Optional, Set
from urllib.parse import quote_plus

import httpx
from bs4 import BeautifulSoup

from pipeline.crawlers.base import BaseCrawler


class YonhapCrawler(BaseCrawler):
    USER_AGENT = os.getenv(
        "USER_AGENT",
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    )
    MAX_NO_NEW_PAGES = 5

    def __init__(
            self,
            office_id: str = "1001",
            delay: float = 0.15,
            max_pages: int = 0,
            empty_result_delay: float = 1.0,
    ):
        self.office_id = office_id
        self.delay = max(delay, 0.0)
        self.empty_result_delay = max(empty_result_delay, 0.1)
        self.max_pages = max(max_pages, 0)
        self.logger = logging.getLogger(__name__)

    async def crawl(
            self,
            stock: Optional[str] = None,
            fromDate: Optional[str] = None,
            toDate: Optional[str] = None,
            limit: int = 1000,
    ) -> list[dict]:
        if not stock:
            raise ValueError("stock값은 필수입니다.")
        if not fromDate or not toDate:
            raise ValueError("fromDate와 toDate는 필수입니다.")

        self._validate_date(fromDate)
        self._validate_date(toDate)

        if self._parse_dot_date(toDate) < self._parse_dot_date(fromDate):
            raise ValueError("toDate는 fromDate보다 같거나 이후여야 합니다.")

        if limit <= 0:
            raise ValueError("limit는 1 이상이어야 합니다.")

        async with httpx.AsyncClient(
                headers={"User-Agent": self.USER_AGENT},
                timeout=20.0,
                follow_redirects=True,
        ) as client:
            seen_urls: Set[str] = set()
            raw_items: list[dict] = []

            start = 1
            no_new_pages = 0
            pages = 0

            while no_new_pages < self.MAX_NO_NEW_PAGES:
                if self.max_pages > 0 and pages >= self.max_pages:
                    break
                if len(raw_items) >= limit:
                    break

                search_url = self._build_search_url(
                    query=stock,
                    start_date_dot=fromDate,
                    end_date_dot=toDate,
                    start=start,
                    office_id=self.office_id,
                )

                try:
                    resp = await client.get(search_url)
                    resp.raise_for_status()
                except httpx.HTTPError as e:
                    self.logger.error("검색 페이지 요청 실패: %s", e)
                    break

                urls = self._extract_yonhap_urls(resp.text)
                new_urls = [u for u in urls if u not in seen_urls]

                if not new_urls:
                    no_new_pages += 1
                    empty_wait = self.empty_result_delay * (2 ** (no_new_pages - 1))
                    self.logger.info(
                        "빈 결과 받음: stock=%s no_new_pages=%d wait=%.2fs",
                        stock,
                        no_new_pages,
                        empty_wait,
                    )
                    if no_new_pages < self.MAX_NO_NEW_PAGES:
                        await asyncio.sleep(empty_wait)
                    start += 10
                    pages += 1
                    continue

                no_new_pages = 0

                for url in new_urls:
                    if len(raw_items) >= limit:
                        break

                    seen_urls.add(url)

                    try:
                        article = await self._parse_article(url, client)
                        article["stock"] = stock
                        raw_items.append(article)
                    except httpx.HTTPError as e:
                        self.logger.warning("기사 요청 실패 (재시도 대기 후 계속): %s / %s", url, e)
                        if self.delay > 0:
                            await asyncio.sleep(self.delay * 2)
                    except Exception as e:
                        self.logger.warning("기사 파싱 실패: %s / %s", url, e)

                    if self.delay > 0:
                        await asyncio.sleep(self.delay)

                start += 10
                pages += 1

                if self.delay > 0:
                    await asyncio.sleep(self.delay)

            deduped = self._dedup_items(raw_items)
            return deduped[:limit]

    def _build_search_url(
            self,
            query: str,
            start_date_dot: str,
            end_date_dot: str,
            start: int,
            office_id: str = "1001",
    ) -> str:
        compact_start = self._to_compact_date(start_date_dot)
        compact_end = self._to_compact_date(end_date_dot)

        return (
            "https://search.naver.com/search.naver"
            f"?ssc=tab.news.all&query={quote_plus(query)}&sm=tab_opt&sort=0&photo=0"
            "&field=0&pd=3"
            f"&ds={start_date_dot}&de={end_date_dot}"
            "&docid=&related=0&mynews=1&office_type=2&office_section_code=2"
            f"&news_office_checked={office_id}"
            f"&nso=so:r,p:from{compact_start}to{compact_end}"
            "&is_sug_officeid=0&office_category=0&service_area=0"
            f"&start={start}"
        )

    def _extract_yonhap_urls(self, search_html: str) -> list[str]:
        urls = re.findall(
            r"https://n\.news\.naver\.com/mnews/article/001/\d+",
            search_html,
        )

        seen: Set[str] = set()
        deduped: list[str] = []

        for url in urls:
            if url in seen:
                continue
            seen.add(url)
            deduped.append(url)

        return deduped

    async def _parse_article(self, url: str, client: httpx.AsyncClient) -> dict:
        resp = await client.get(url)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        title = self._extract_title(soup)
        published_at = self._extract_published_at(soup)
        content = self._extract_content(soup)
        thumbnail_url = self._extract_thumbnail_url(soup)

        return {
            "title": title or "(제목 없음)",
            "content": content or "(본문 없음)",
            "url": url,
            "published_at": published_at or "(날짜 없음)",
            "source": "연합뉴스",
            "thumbnail_url": thumbnail_url or None,
        }

    def _extract_title(self, soup: BeautifulSoup) -> str:
        for selector in [
            "h2#title_area span",
            "h2.media_end_head_headline",
            "meta[property='og:title']",
        ]:
            node = soup.select_one(selector)
            if not node:
                continue

            if node.name == "meta":
                title = self._clean_text(node.get("content", ""))
            else:
                title = self._clean_text(node.get_text(" ", strip=True))

            if title:
                return title

        return ""

    def _extract_published_at(self, soup: BeautifulSoup) -> str:
        for selector in [
            "span._ARTICLE_DATE_TIME",
            "em.media_end_head_info_datestamp_time",
        ]:
            node = soup.select_one(selector)
            if not node:
                continue

            published_at = self._clean_text(
                node.get("data-date-time", "") or node.get_text(" ", strip=True)
            )
            if published_at:
                return published_at

        return ""

    def _extract_content(self, soup: BeautifulSoup) -> str:
        for selector in [
            "#dic_area",
            "article#dic_area",
            "div#newsct_article",
        ]:
            node = soup.select_one(selector)
            if not node:
                continue

            for bad in node.select("script, style, iframe"):
                bad.extract()

            content = self._clean_text(node.get_text(" ", strip=True))
            if content:
                return content

        return ""

    def _extract_thumbnail_url(self, soup: BeautifulSoup) -> str:
        article = soup.select_one("#dic_area, article#dic_area, div#newsct_article")

        if article:
            for selector in [
                "span.end_photo_org img",
                ".nbd_im_w img",
                "img#img1",
                "img",
            ]:
                node = article.select_one(selector)
                if not node:
                    continue

                thumbnail_url = (
                        node.get("data-src")
                        or node.get("src")
                        or node.get("data-lazy-src")
                        or ""
                ).strip()

                if thumbnail_url:
                    return thumbnail_url

        og_image = soup.select_one("meta[property='og:image']")
        if og_image:
            thumbnail_url = og_image.get("content", "").strip()
            if thumbnail_url:
                return thumbnail_url

        return ""

    def _dedup_items(self, items: list[dict]) -> list[dict]:
        seen: Set[tuple[str, str, str]] = set()
        result: list[dict] = []

        for item in items:
            key = (
                self._clean_text(item.get("title", "")).lower(),
                self._date_day_key(item.get("published_at", "")),
                self._clean_text(item.get("content", "")).lower(),
            )

            if key in seen:
                continue

            seen.add(key)
            result.append(item)

        return result

    def _date_day_key(self, date_str: str) -> str:
        return self._clean_text(date_str).split(" ")[0].lower()

    def _clean_text(self, text: str) -> str:
        return re.sub(r"\s+", " ", text).strip()

    def _to_compact_date(self, date_yyyymmdd_dot: str) -> str:
        dt = datetime.strptime(date_yyyymmdd_dot, "%Y.%m.%d")
        return dt.strftime("%Y%m%d")

    def _parse_dot_date(self, value: str) -> datetime:
        return datetime.strptime(value, "%Y.%m.%d")

    def _validate_date(self, value: str) -> None:
        try:
            self._parse_dot_date(value)
        except ValueError as e:
            raise ValueError("날짜 형식은 YYYY.MM.DD 이어야 합니다.") from e
