import json
import unittest
from pathlib import Path

from src.advisor_marketing_engine import OUTPUT_DIR, main


class TestAdvisorMarketingEngine(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.summary = main()

    def test_total_verified_record_scale(self) -> None:
        self.assertEqual(self.summary["total_verified_records"], 122294)

    def test_zero_join_fanout_and_dedup_integrity(self) -> None:
        wh = self.summary["warehouse_audit"]
        self.assertTrue(wh["sqlite_integrity_ok"])
        self.assertEqual(wh["deduplicated_advisor_rows"], wh["distinct_mart_advisors"])
        self.assertGreater(wh["duplicate_crm_rows_removed"], 0)
        self.assertGreater(wh["utc_to_est_boundary_rollovers_fixed"], 0)
        self.assertEqual(wh["fanout_drift_usd"], 0.0)
        self.assertGreater(wh["naive_fanout_inflation_multiplier"], 2.0)

    def test_propensity_and_segmentation_quality(self) -> None:
        prop = self.summary["propensity_model"]
        seg = self.summary["segmentation_and_campaign_lift"]
        self.assertGreaterEqual(prop["holdout_roc_auc"], 0.72)
        self.assertGreaterEqual(prop["top_decile_lift"], 2.0)
        self.assertGreater(seg["visit_relative_lift_pct"], 40.0)
        self.assertGreater(seg["conversion_relative_lift_pct"], 50.0)
        self.assertGreater(seg["kmeans_variance_explained_pct"], 40.0)
        self.assertEqual(len(seg["clusters"]), 4)

    def test_nlp_intent_and_llm_json_schema_compliance(self) -> None:
        nlp = self.summary["nlp_and_llm_enrichment"]
        self.assertEqual(nlp["num_intents"], 77)
        self.assertGreaterEqual(nlp["top1_accuracy_pct"], 80.0)
        self.assertGreaterEqual(nlp["top3_accuracy_pct"], 92.0)
        self.assertEqual(nlp["llm_json_schema_compliance_pct"], 100.0)

    def test_outputs_materialized(self) -> None:
        for fname in ("validation_summary.json", "advisor_segments.csv", "campaign_attribution.csv"):
            p = OUTPUT_DIR / fname
            self.assertTrue(p.exists(), f"Missing output {fname}")
        loaded = json.loads((OUTPUT_DIR / "validation_summary.json").read_text(encoding="utf-8"))
        self.assertEqual(loaded["total_verified_records"], 122294)


if __name__ == "__main__":
    unittest.main()
