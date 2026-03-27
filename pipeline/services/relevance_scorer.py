from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any


@dataclass(frozen=True)
class RelevanceConfig:
    # 1) 날짜 가중치
    SURGE_IN_EVENT_WEIGHT: float = 1.00
    SURGE_PRE1_WEIGHT: float = 0.70
    SURGE_POST1_WEIGHT: float = 0.45

    DROP_IN_EVENT_WEIGHT: float = 1.00
    DROP_PRE1_WEIGHT: float = 0.70
    DROP_POST1_WEIGHT: float = 1.00

    # 2) 태그 가중치
    DEFAULT_TAG_WEIGHT: float = 0.75

    # 3) 감성 보정치
    SENTIMENT_ADJUST_BASE: float = 1.00
    SENTIMENT_ADJUST_MULTIPLIER: float = 0.40
    SENTIMENT_ADJUST_MIN: float = 0.70
    SENTIMENT_ADJUST_MAX: float = 1.30

    # 4) 최종 점수 범위
    MIN_SCORE: float = 0.0
    MAX_SCORE: float = 1.5


class RelevanceScorer:
    CONFIG = RelevanceConfig()

    CATEGORY_WEIGHTS = {
        ("기업", "실적"): 1.35,
        ("기업", "재편"): 1.25,
        ("기업", "투자"): 1.20,
        ("기업", "주주"): 1.15,
        ("기업", "제품"): 1.00,
        ("시장", "수요"): 1.15,
        ("시장", "공급"): 1.10,
        ("시장", "비용"): 0.90,
        ("시장", "경쟁"): 0.85,
    }

    @classmethod
    def calc_relevance(
        cls,
        *,
        event_type: str,
        start_date: Any,
        end_date: Any,
        published_at: Any,
        sentiment_score: Any,
        pred_major: str | None = None,
        pred_sub: str | None = None,
    ) -> float:
        event_type = str(event_type or "").upper().strip()
        event_start_date = cls._to_date(start_date)
        event_end_date = cls._to_date(end_date)
        published_date = cls._to_date(published_at)
        sentiment_score = cls._to_float(sentiment_score, default=0.0)

        pred_major = (pred_major or "").strip()
        pred_sub = (pred_sub or "").strip()

        date_weight = cls._calc_date_weight(
            event_type=event_type,
            event_start_date=event_start_date,
            event_end_date=event_end_date,
            published_date=published_date,
        )
        if date_weight <= 0.0:
            return 0.0

        tag_weight = cls.CATEGORY_WEIGHTS.get(
            (pred_major, pred_sub),
            cls.CONFIG.DEFAULT_TAG_WEIGHT,
        )

        sentiment_adjust = cls._calc_sentiment_adjust(
            event_type=event_type,
            sentiment_score=sentiment_score,
        )

        score = date_weight * tag_weight * sentiment_adjust
        return cls._clamp(score, cls.CONFIG.MIN_SCORE, cls.CONFIG.MAX_SCORE)

    @classmethod
    def _calc_sentiment_adjust(cls, event_type: str, sentiment_score: float) -> float:
        direction = 1.0 if event_type == "SURGE" else -1.0
        directional_sentiment = sentiment_score * direction

        adjust = (
                cls.CONFIG.SENTIMENT_ADJUST_BASE
                + (directional_sentiment * cls.CONFIG.SENTIMENT_ADJUST_MULTIPLIER)
        )
        return cls._clamp(
            adjust,
            cls.CONFIG.SENTIMENT_ADJUST_MIN,
            cls.CONFIG.SENTIMENT_ADJUST_MAX,
        )

    @classmethod
    def _calc_date_weight(
            cls,
            *,
            event_type: str,
            event_start_date: date,
            event_end_date: date,
            published_date: date,
    ) -> float:
        if event_start_date <= published_date <= event_end_date:
            return (
                cls.CONFIG.SURGE_IN_EVENT_WEIGHT
                if event_type == "SURGE"
                else cls.CONFIG.DROP_IN_EVENT_WEIGHT
            )

        if published_date == event_start_date - timedelta(days=1):
            return (
                cls.CONFIG.SURGE_PRE1_WEIGHT
                if event_type == "SURGE"
                else cls.CONFIG.DROP_PRE1_WEIGHT
            )

        if published_date == event_end_date + timedelta(days=1):
            return (
                cls.CONFIG.SURGE_POST1_WEIGHT
                if event_type == "SURGE"
                else cls.CONFIG.DROP_POST1_WEIGHT
            )

        return 0.0

    @staticmethod
    def _to_date(value: Any) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            return datetime.fromisoformat(value).date()
        raise ValueError(f"date 변환 불가: {value!r}")

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            if value is None or value == "":
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _clamp(value: float, min_value: float, max_value: float) -> float:
        return round(max(min_value, min(max_value, float(value))), 5)