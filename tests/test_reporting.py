from datetime import datetime, timedelta

from utils.discovery import ObjectRecord
from utils.reporting import (
    DecisionThresholds,
    compute_recommendation,
    Recommendation,
    render_summary_markdown,
)


def make_classified(name, classification, created_at=None):
    return (
        ObjectRecord(
            catalog="c", schema="s", name=name,
            object_type="TABLE", table_type="MANAGED",
            data_source_format="DELTA",
            storage_path=None, parent_managed_location=None,
            owner="u", created_at=created_at, last_altered=None,
        ),
        classification,
    )


class TestComputeRecommendation:
    def test_zero_new_objects_rollback_feasible(self):
        records = [
            make_classified("t1", "consistent_old"),
            make_classified("t2", "drift_managed_on_old"),
        ]
        rec = compute_recommendation(records, thresholds=DecisionThresholds(), bytes_on_new=0)
        assert rec.verdict == "ROLLBACK_FEASIBLE"

    def test_many_new_objects_forward(self):
        records = [
            make_classified(f"t{i}", "consistent_new") for i in range(100)
        ]
        rec = compute_recommendation(records, thresholds=DecisionThresholds(), bytes_on_new=0)
        assert rec.verdict == "FORWARD_MIGRATE_REQUIRED"

    def test_old_new_object_forces_forward(self):
        old_ts = datetime.utcnow() - timedelta(days=60)
        records = [
            make_classified("t1", "consistent_new", created_at=old_ts),
        ]
        rec = compute_recommendation(records, thresholds=DecisionThresholds(), bytes_on_new=0)
        # Object older than max_age_days_on_new → not safe to roll back
        assert rec.verdict == "FORWARD_MIGRATE_REQUIRED"

    def test_few_recent_new_objects_requires_signoff(self):
        recent = datetime.utcnow() - timedelta(days=2)
        records = [
            make_classified(f"t{i}", "consistent_new", created_at=recent)
            for i in range(5)
        ]
        rec = compute_recommendation(records, thresholds=DecisionThresholds(), bytes_on_new=1)
        assert rec.verdict == "ROLLBACK_REQUIRES_SIGNOFF"


class TestRenderSummaryMarkdown:
    def test_markdown_includes_counts_per_classification(self):
        records = [
            make_classified("t1", "consistent_old"),
            make_classified("t2", "drift_managed_on_old"),
            make_classified("t3", "drift_managed_on_old"),
        ]
        rec = compute_recommendation(records, thresholds=DecisionThresholds(), bytes_on_new=0)
        md = render_summary_markdown(records=records, recommendation=rec)
        assert "consistent_old" in md
        assert "drift_managed_on_old" in md
        assert "ROLLBACK_FEASIBLE" in md
