import json
import logging
import re
from pathlib import Path
from typing import Any

import pymysql
import pymysql.cursors
from dotenv import dotenv_values
from openai import OpenAI

logger = logging.getLogger(__name__)

STATUS_ACTIVE = "ACTIVE"


class SummaryService:
    """
    이벤트별 관련 뉴스들을 재평가하고,
    - event_news.relevance_score 보정
    - events.summary 저장
    """

    def __init__(
        self,
        eval_candidate_limit: int = 30,     # 이벤트당 점수 재평가 후보 수
        eval_chunk_size: int = 10,          # LLM 한 번에 평가할 뉴스 수
        summary_news_limit: int = 12,       # 최종 요약에 사용할 뉴스 수
        content_char_limit: int = 2200,     # 기사당 본문 최대 길이
        min_score_threshold_for_summary: float = 0.15,  # 최종 요약에 쓸 최소 점수
    ):
        env_path = Path(__file__).resolve().parents[2] / ".env"
        env = dotenv_values(env_path)

        self.db_config = {
            "host": env.get("DB_HOST"),
            "port": int(env.get("DB_PORT", 3306)),
            "user": env.get("DB_USER"),
            "password": env.get("DB_PASSWORD"),
            "database": env.get("DB_NAME"),
            "charset": env.get("DB_CHARSET", "utf8mb4"),
        }

        self.client = OpenAI(api_key=env.get("OPENAI_API_KEY"))
        self.model = env.get("OPENAI_MODEL", "gpt-5.4-mini")

        self.eval_candidate_limit = eval_candidate_limit
        self.eval_chunk_size = eval_chunk_size
        self.summary_news_limit = summary_news_limit
        self.content_char_limit = content_char_limit
        self.min_score_threshold_for_summary = min_score_threshold_for_summary

    # -----------------------------
    # public
    # -----------------------------

    def summarize_events(
        self,
        event_ids: list[int] | None = None,
        only_empty_summary: bool = True,
        only_active_crawl: bool = True,
        overwrite_existing_summary: bool = False,
        update_relevance_scores: bool = True,
    ) -> None:
        """
        기본 동작:
        - summary 비어있는 ACTIVE 이벤트만 처리
        - 각 이벤트의 event_news 상위 후보를 재평가
        - relevance_score 보정
        - 관련 기사만 요약해 events.summary 저장
        """
        with self._connect() as conn:
            events = self._load_target_events(
                conn=conn,
                event_ids=event_ids,
                only_empty_summary=only_empty_summary if not overwrite_existing_summary else False,
                only_active_crawl=only_active_crawl,
            )

            if not events:
                logger.info("요약 대상 이벤트가 없습니다.")
                return

            logger.info("요약 대상 이벤트 수=%d", len(events))

            for event in events:
                event_id = event["id"]
                logger.info("이벤트 처리 시작 event_id=%s", event_id)

                candidate_news = self._load_candidate_news_for_event(
                    conn=conn,
                    event_id=event_id,
                    limit=self.eval_candidate_limit,
                )

                if not candidate_news:
                    logger.info("event_id=%s 연결된 뉴스가 없습니다.", event_id)

                    if overwrite_existing_summary or only_empty_summary:
                        self._update_event_summary(
                            conn,
                            event_id,
                            "주가 변동의 직접 원인으로 볼 만한 핵심 뉴스가 확인되지 않았다."
                        )
                        conn.commit()
                    continue

                # 1) 뉴스별 LLM 재평가
                evaluations = self._evaluate_news_candidates(event, candidate_news)

                # 2) relevance_score 보정 반영
                if update_relevance_scores and evaluations:
                    self._apply_relevance_updates(conn, event_id, candidate_news, evaluations)

                # 3) 보정된 기준으로 최종 요약용 뉴스 선별
                summary_news = self._select_news_for_summary(candidate_news, evaluations)

                # 4) 이벤트 summary 생성
                summary_text = self._generate_event_summary(event, summary_news)

                # 5) summary 저장
                self._update_event_summary(conn, event_id, summary_text)

                conn.commit()
                logger.info("이벤트 처리 완료 event_id=%s", event_id)

    # -----------------------------
    # DB
    # -----------------------------

    def _connect(self) -> pymysql.connections.Connection:
        return pymysql.connect(
            **self.db_config,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )

    @staticmethod
    def _load_target_events(
        conn,
        event_ids: list[int] | None = None,
        only_empty_summary: bool = True,
        only_active_crawl: bool = True,
    ) -> list[dict]:
        conditions = ["status = 'ACTIVE'"]
        params: list[Any] = []

        if only_active_crawl:
            conditions.append("crawl_status = 'ACTIVE'")

        if only_empty_summary:
            conditions.append("(summary IS NULL OR TRIM(summary) = '')")

        if event_ids:
            placeholders = ",".join(["%s"] * len(event_ids))
            conditions.append(f"id IN ({placeholders})")
            params.extend(event_ids)

        where_sql = " AND ".join(conditions)

        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, stock_id, event_type, start_date, end_date, change_pct, summary
                FROM events
                WHERE {where_sql}
                ORDER BY id
                """,
                params,
            )
            return list(cur.fetchall())

    @staticmethod
    def _load_candidate_news_for_event(conn, event_id: int, limit: int) -> list[dict]:
        """
        event_news 상위 후보들만 가져온다.
        100개 이상이어도 처음부터 전부 모델에 넣지 않기 위함.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    n.id AS news_id,
                    n.title,
                    n.content,
                    n.source,
                    n.url,
                    n.published_at,
                    en.relevance_score
                FROM event_news en
                JOIN news n ON n.id = en.news_id
                WHERE en.event_id = %s
                  AND en.status = 'ACTIVE'
                  AND n.status = 'ACTIVE'
                ORDER BY en.relevance_score DESC, n.published_at DESC
                LIMIT %s
                """,
                (event_id, limit),
            )
            return list(cur.fetchall())

    @staticmethod
    def _update_event_summary(conn, event_id: int, summary: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE events
                SET summary = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (summary, event_id),
            )

    @staticmethod
    def _update_event_news_scores(conn, rows: list[tuple]) -> None:
        if not rows:
            return

        with conn.cursor() as cur:
            cur.executemany(
                """
                UPDATE event_news
                SET relevance_score = %s,
                    updated_at = NOW()
                WHERE event_id = %s
                  AND news_id = %s
                """,
                rows,
            )

    # -----------------------------
    # LLM evaluation
    # -----------------------------

    def _evaluate_news_candidates(self, event: dict, candidate_news: list[dict]) -> dict[int, dict]:
        """
        상위 후보 뉴스들을 청크로 나눠서 평가.
        반환:
        {
            news_id: {
                "is_relevant": bool,
                "reason": str,
                "adjustment": float
            }
        }
        """
        result: dict[int, dict] = {}

        chunks = self._chunked(candidate_news, self.eval_chunk_size)
        for chunk_idx, chunk in enumerate(chunks, start=1):
            logger.info(
                "LLM 뉴스 평가 event_id=%s chunk=%d size=%d",
                event["id"], chunk_idx, len(chunk)
            )

            prompt = self._build_evaluation_prompt(event, chunk)
            raw_text = self._call_model(prompt)
            parsed = self._parse_json_response(raw_text)

            items = parsed.get("news_evaluations", [])
            if not isinstance(items, list):
                logger.warning("event_id=%s chunk=%d 평가 결과 형식 이상", event["id"], chunk_idx)
                continue

            for item in items:
                try:
                    news_id = int(item["news_id"])
                    is_relevant = bool(item["is_relevant"])
                    reason = str(item.get("reason", "")).strip()
                    adjustment = float(item.get("adjustment", 0.0))

                    # 보정치는 안전 범위로 clamp
                    adjustment = max(-0.50, min(0.20, adjustment))

                    result[news_id] = {
                        "is_relevant": is_relevant,
                        "reason": reason,
                        "adjustment": adjustment,
                    }
                except (KeyError, TypeError, ValueError):
                    continue

        return result

    def _build_evaluation_prompt(self, event: dict, news_rows: list[dict]) -> str:
        news_blocks = []
        for idx, row in enumerate(news_rows, start=1):
            content = self._normalize_content(row.get("content", ""))
            if len(content) > self.content_char_limit:
                content = content[:self.content_char_limit]

            news_blocks.append(
                f"""[뉴스 {idx}]
news_id: {row["news_id"]}
기존 relevance_score: {row["relevance_score"]}
제목: {row.get("title", "")}
출처: {row.get("source", "")}
발행시각: {row.get("published_at", "")}
본문:
{content}
"""
            )

        joined_news = "\n\n".join(news_blocks)

        return f"""
너는 주가 이벤트 원인 분석 전문가다.

아래 뉴스들을 보고 각 뉴스가 실제 주가 변동의 원인/배경/재료와 관련 있는지 평가하라.
반드시 뉴스별 평가만 수행하고, 최종 출력은 JSON만 반환하라.

[이벤트 정보]
event_id: {event["id"]}
event_type: {event["event_type"]}
start_date: {event["start_date"]}
end_date: {event["end_date"]}
change_pct: {event["change_pct"]}

[낮은 relevance로 판단해야 하는 기사]
- 단순 사후 시황 기사
- "주가가 올랐다/떨어졌다", "급등/급락", "강세/약세"처럼 결과만 반복하는 기사
- 장중 흐름, 거래량, 수급, 차트 위주 기사
- 관련주/테마주 단순 나열 기사
- 기업명은 등장하지만 투자 재료와 무관한 일반 기사
- 주식 가격 움직임을 뒤늦게 설명하는 기사

[높은 relevance로 판단해야 하는 기사]
- 실적, 수주, 계약, 투자, 인수합병, 지분 변화
- 신제품, 기술, 특허, 승인, 정책, 규제, 공시, 소송
- 업황 변화, 비용 변화, 수요 변화
- 회사 가치나 미래 실적 기대에 직접 영향을 줄 수 있는 내용

[adjustment 규칙]
- 강한 관련성: 0.10 ~ 0.20
- 어느 정도 관련: 0.00 ~ 0.05
- 애매함: -0.10
- 단순 사후 시황 기사: -0.30
- 주식과 무관한 기사: -0.50

[판단 규칙]
- is_relevant는 요약에 사용할 만하면 true, 아니면 false
- reason은 한 줄로 간단히 작성
- 반드시 주어진 news_id를 그대로 반환
- 최종 출력은 JSON만 반환
- 마크다운 코드블록 금지
- 평가하지 않은 뉴스가 없도록 모든 news_id를 반환

[출력 JSON 형식]
{{
  "news_evaluations": [
    {{
      "news_id": 123,
      "is_relevant": true,
      "reason": "실적 개선 기대가 직접 언급됨",
      "adjustment": 0.12
    }}
  ]
}}

[뉴스 목록]
{joined_news}
""".strip()

    # -----------------------------
    # relevance update
    # -----------------------------

    def _apply_relevance_updates(
        self,
        conn,
        event_id: int,
        candidate_news: list[dict],
        evaluations: dict[int, dict],
    ) -> None:
        updates: list[tuple] = []

        for row in candidate_news:
            news_id = row["news_id"]
            old_score = float(row.get("relevance_score") or 0.0)

            evaluation = evaluations.get(news_id)
            if not evaluation:
                continue

            adjustment = float(evaluation.get("adjustment", 0.0))
            new_score = self._clamp_score(old_score + adjustment)

            row["adjusted_relevance_score"] = new_score
            row["llm_is_relevant"] = bool(evaluation.get("is_relevant", False))
            row["llm_reason"] = evaluation.get("reason", "")

            updates.append((new_score, event_id, news_id))

        self._update_event_news_scores(conn, updates)

    # -----------------------------
    # summary selection / generation
    # -----------------------------

    def _select_news_for_summary(
        self,
        candidate_news: list[dict],
        evaluations: dict[int, dict],
    ) -> list[dict]:
        """
        LLM이 relevant라고 판단한 뉴스들 중
        보정 점수 기준 상위 N개만 summary에 사용.
        """
        selected = []

        for row in candidate_news:
            news_id = row["news_id"]
            evaluation = evaluations.get(news_id)
            if not evaluation:
                continue

            is_relevant = bool(evaluation.get("is_relevant", False))
            if not is_relevant:
                continue

            adjusted = row.get("adjusted_relevance_score")
            if adjusted is None:
                old_score = float(row.get("relevance_score") or 0.0)
                adjusted = self._clamp_score(old_score + float(evaluation.get("adjustment", 0.0)))
                row["adjusted_relevance_score"] = adjusted

            if adjusted < self.min_score_threshold_for_summary:
                continue

            selected.append(row)

        selected.sort(
            key=lambda x: (
                float(x.get("adjusted_relevance_score") or 0.0),
                x.get("published_at"),
            ),
            reverse=True,
        )

        # 중복성 완화: 제목 기준 간단 dedupe
        deduped = []
        seen_title_keys = set()
        for row in selected:
            title_key = self._title_key(row.get("title", ""))
            if title_key in seen_title_keys:
                continue
            seen_title_keys.add(title_key)
            deduped.append(row)

        return deduped[:self.summary_news_limit]

    def _generate_event_summary(self, event: dict, summary_news: list[dict]) -> str:
        if not summary_news:
            return "주가 변동의 직접 원인으로 볼 만한 핵심 뉴스가 확인되지 않았다."

        news_blocks = []
        for idx, row in enumerate(summary_news, start=1):
            content = self._normalize_content(row.get("content", ""))
            if len(content) > self.content_char_limit:
                content = content[:self.content_char_limit]

            news_blocks.append(
                f"""[요약대상 뉴스 {idx}]
news_id: {row["news_id"]}
보정 relevance_score: {row.get("adjusted_relevance_score", row.get("relevance_score", 0))}
제목: {row.get("title", "")}
출처: {row.get("source", "")}
발행시각: {row.get("published_at", "")}
본문:
{content}
"""
            )

        joined_news = "\n\n".join(news_blocks)

        prompt = f"""
너는 주가 이벤트 요약 전문가다.

아래 뉴스들은 이미 1차로 선별된 '주가 변동 원인/배경과 관련 있는 기사'들이다.
이 기사들만 이용해서 이벤트 전체를 간결하게 요약하라.

[이벤트 정보]
event_id: {event["id"]}
event_type: {event["event_type"]}
start_date: {event["start_date"]}
end_date: {event["end_date"]}
change_pct: {event["change_pct"]}

[요약 규칙]
- 3~5문장
- 첫 문장은 이벤트 핵심 원인을 한 줄로 정리
- 중복 표현 제거
- 기사에 없는 내용 추측 금지
- "주가가 올랐다/떨어졌다" 같은 결과 반복 대신 원인 중심 서술
- DB 저장용 문체로 담백하게 작성
- 최종 출력은 요약문만 반환
- 마크다운 금지
- 기사들만으로 원인을 특정하기 어렵다면:
  "주가 변동의 직접 원인으로 볼 만한 핵심 뉴스가 확인되지 않았다."
  라고 출력

[뉴스 목록]
{joined_news}
""".strip()

        text = self._call_model(prompt).strip()
        text = self._strip_markdown_fence(text)

        if not text:
            return "주가 변동의 직접 원인으로 볼 만한 핵심 뉴스가 확인되지 않았다."

        return text

    # -----------------------------
    # OpenAI call
    # -----------------------------

    def _call_model(self, prompt: str) -> str:
        response = self.client.responses.create(
            model=self.model,
            input=prompt,
        )
        return (response.output_text or "").strip()

    # -----------------------------
    # utils
    # -----------------------------

    @staticmethod
    def _chunked(items: list[dict], size: int) -> list[list[dict]]:
        return [items[i:i + size] for i in range(0, len(items), size)]

    @staticmethod
    def _clamp_score(score: float) -> float:
        return round(max(0.0, min(1.0, score)), 4)

    @staticmethod
    def _normalize_content(text: str) -> str:
        if not text:
            return ""
        text = text.replace("\x00", " ")
        text = re.sub(r"\s+", " ", text).strip()
        return text

    @staticmethod
    def _title_key(title: str) -> str:
        key = title.lower().strip()
        key = re.sub(r"\s+", " ", key)
        key = re.sub(r"[^0-9a-z가-힣 ]+", "", key)
        return key[:80]

    @staticmethod
    def _strip_markdown_fence(text: str) -> str:
        text = text.strip()
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        return text.strip()

    def _parse_json_response(self, text: str) -> dict:
        text = self._strip_markdown_fence(text)

        # 1차: 그대로 파싱
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # 2차: JSON 객체 부분만 추출
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass

        logger.warning("JSON 파싱 실패, 원문 일부=%s", text[:300])
        return {}