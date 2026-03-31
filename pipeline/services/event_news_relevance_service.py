from pipeline.services.relevance_scorer import RelevanceScorer


class EventNewsRelevanceService:
    @staticmethod
    def calculate(row: dict, event: dict) -> float:
        return RelevanceScorer.calc_relevance(
            event_type=event["event_type"],
            start_date=event["start_date"],
            end_date=event["end_date"],
            published_at=row["_published_date"],
            sentiment_score=row.get("sentiment_score"),
            pred_major=row.get("pred_major"),
            pred_sub=row.get("pred_sub"),
        )