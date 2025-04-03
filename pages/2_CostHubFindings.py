import pandas as pd
import streamlit as st
import CostOptimizationDataPull as codp

CHUB_SUMMARY_FILE = "/tmp/chub_sum.parquet"
CHUB_DETAILED_FILE = "/tmp/chub_det.parquet"

st.set_page_config(page_title="Cost Hub Findings", layout="wide")
st.markdown("# Cost Hub Findings")
st.sidebar.header("Cost Hub Findings")

summary_df, detailed_df = codp.get_finding(
    CHUB_SUMMARY_FILE, CHUB_DETAILED_FILE, "chub"
)
summary_df["MoveToTracker"] = pd.Series([False for x in range(len(summary_df.index))])

# Output Findings Summary Table & Button to move findings to tracker
st.markdown("### Findings Summary")
esummary_df = st.data_editor(
    summary_df,
    key="summary_chub",
    column_config={"MoveToTracker": st.column_config.CheckboxColumn()},
)
mover = st.button(
    "Move Selected Items to Tracker",
    type="primary",
    on_click=codp.add_to_tracker,
    args=(esummary_df, CHUB_SUMMARY_FILE, "summary_chub"),
)

# Output Findings Detailed Table
st.markdown("### Findings Detailed")
st.write(detailed_df)

st.sidebar.success("Load Complete")
