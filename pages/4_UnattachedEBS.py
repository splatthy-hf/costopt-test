import pandas as pd
import streamlit as st
import CostOptimizationDataPull as codp

UEBS_SUMMARY_FILE = "/tmp/uebs_sum.parquet"
UEBS_DETAILED_FILE = "/tmp/uebs_det.parquet"

st.set_page_config(page_title="Unattached EBS Findings", layout="wide")
st.markdown("# Unattached EBS Findings")
st.sidebar.header("Unattached EBS Findings")

summary_df, detailed_df = codp.get_finding(
    UEBS_SUMMARY_FILE, UEBS_DETAILED_FILE, "uebs"
)
summary_df["MoveToTracker"] = pd.Series([False for x in range(len(summary_df.index))])

# Output Findings Summary Table & Button to move findings to tracker
st.markdown("### Findings Summary")
esummary_df = st.data_editor(
    summary_df,
    key="summary_uebs",
    column_config={"MoveToTracker": st.column_config.CheckboxColumn()},
)
mover = st.button(
    "Move Selected Items to Tracker",
    type="primary",
    on_click=codp.add_to_tracker,
    args=(esummary_df, UEBS_SUMMARY_FILE, "summary_uebs"),
)

# Output Findings Detailed Table
st.markdown("### Findings Detailed")
st.write(detailed_df)

# Button logic to move findings from the summary table into the tracker

st.sidebar.success("Load Complete")
