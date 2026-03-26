class RelevanceScorer:
    CATEGORY_WEIGHTS = {
        ("기업", "실적"): 1.30,
        ("기업", "제품"): 1.00,
        ("기업", "투자"): 1.15,
        ("기업", "재편"): 1.20,
        ("기업", "주주"): 1.15,

        ("시장", "수요"): 1.15,
        ("시장", "공급"): 1.10,
        ("시장", "비용"): 0.95,
        ("시장", "경쟁"): 0.90,

        ("정책", "규제"): 1.20,
        ("정책", "지원"): 1.00,
        ("정책", "승인"): 1.25,

        ("거시", "금리"): 1.25,
        ("거시", "환율"): 1.15,
        ("거시", "물가"): 0.95,
        ("거시", "경기"): 1.00,

        ("자금", "수급"): 1.25,
        ("자금", "투자"): 1.15,
        ("자금", "신용"): 1.00,

        ("리스크", "사고"): 1.25,
        ("리스크", "분쟁"): 1.10,
        ("리스크", "불법"): 1.30,
    }

    @staticmethod
    def _to_float(value, default=0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @classmethod
    def _get_tag_weight(cls, major_tag: str, sub_tag: str) -> float:
        return cls.CATEGORY_WEIGHTS.get((major_tag, sub_tag), 1.0)

    @classmethod
    def calc_relevance(cls, row: dict) -> float:
        major_prob = cls._to_float(row.get("pred_major_prob"))
        sub_prob = cls._to_float(row.get("pred_sub_prob"))

        major_prob = min(max(major_prob, 0.0), 1.0)
        sub_prob = min(max(sub_prob, 0.0), 1.0)

        # 경제/종목 뉴스 자체가 아니면 거의 0점 처리
        if major_prob < 0.25:
            return 0.0

        base_relevance = (major_prob * 0.8) + (sub_prob * 0.2)

        major_tag = str(row.get("pred_major") or "").strip()
        sub_tag = str(row.get("pred_sub") or "").strip()
        tag_weight = cls._get_tag_weight(major_tag, sub_tag)

        final_score = base_relevance * tag_weight

        # DECIMAL(5,4) 대응 + 확률형 점수 유지
        final_score = min(max(final_score, 0.0), 9.9999)

        return round(final_score, 4)