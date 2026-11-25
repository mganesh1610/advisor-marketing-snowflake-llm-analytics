"""Self-Serve Advisor Marketing Analytics & Applied AI Dashboard (Streamlit)."""

import json
from pathlib import Path

import pandas as pd
import streamlit as st

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "outputs"

st.set_page_config(page_title="Advisor Marketing & Applied AI Analytics", layout="wide")
st.title("Advisor Marketing Attribution, Snowflake 360 & Applied AI Suite")

summary = json.loads((OUTPUT_DIR / "validation_summary.json").read_text(encoding="utf-8"))
wh = summary["warehouse_audit"]
prop = summary["propensity_model"]
seg = summary["segmentation_and_campaign_lift"]
nlp = summary["nlp_and_llm_enrichment"]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Verified Records", f"{summary['total_verified_records']:,}")
c2.metric("Join Fan-Out Drift", f"${wh['fanout_drift_usd']:.2f}")
c3.metric("Propensity ROC-AUC", f"{prop['holdout_roc_auc']:.3f}", f"{prop['top_decile_lift']}x top-decile lift")
c4.metric("LLM JSON Compliance", f"{nlp['llm_json_schema_compliance_pct']:.1f}%", f"{nlp['top3_accuracy_pct']}% Top-3 intent")

st.subheader("Multi-Channel Campaign & Email Attribution")
st.dataframe(pd.read_csv(OUTPUT_DIR / "campaign_attribution.csv"), use_container_width=True)

st.subheader("K-Means++ Behavioral Advisor Segments")
st.dataframe(pd.read_csv(OUTPUT_DIR / "advisor_segments.csv"), use_container_width=True)
