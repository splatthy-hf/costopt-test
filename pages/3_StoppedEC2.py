import pandas as pd
import streamlit as st
import CostOptimizationDataPull as codp

SEC2_SUMMARY_FILE = "/tmp/sec2_sum.parquet"
SEC2_DETAILED_FILE = "/tmp/sec2_det.parquet"

st.set_page_config(page_title="Stopped EC2 Findings", layout="wide")
st.markdown("# Stopped EC2 Findings")
st.sidebar.header("Stopped EC2 Findings")

summary_df, detailed_df = codp.get_finding(
    SEC2_SUMMARY_FILE, SEC2_DETAILED_FILE, "sec2"
)
summary_df["MoveToTracker"] = pd.Series([False for x in range(len(summary_df.index))])

# Output Findings Summary Table & Button to move findings to tracker
st.markdown("### Findings Summary")
esummary_df = st.data_editor(
    summary_df,
    key="summary_sec2",
    column_config={"MoveToTracker": st.column_config.CheckboxColumn()},
)
mover = st.button(
    "Move Selected Items to Tracker",
    type="primary",
    on_click=codp.add_to_tracker,
    args=(esummary_df, SEC2_SUMMARY_FILE, "summary_sec2"),
)

# Output Findings Detailed Table
st.markdown("### Findings Detailed")
st.write(detailed_df)

st.sidebar.success("Load Complete")
