"""
Streamlit page for tracking and managing cost optimization findings.

This module provides a user interface for viewing and editing cost optimization
tracking data, including functionality to:
- Display and edit existing tracking records
- Add new tracking records
- Filter records by status and savings type
- Calculate and display total savings
- Manage data persistence to S3

The page uses streamlit components for all UI elements and maintains state
through streamlit's session state functionality.
"""

import pandas as pd
import streamlit as st
from streamlit import session_state as ss
from datetime import date
import CostOptimizationDataPull as codp
import uuid

COL_CONFIG = {
    "DateOfSavings": st.column_config.DateColumn(
        "DateOfSavings", min_value=date(2022, 1, 1), format="MM-DD-YYYY", step=1
    ),
    "FinOpsLastModified": st.column_config.DateColumn(
        "FinOpsLastModified", min_value=date(2022, 1, 1), format="MM-DD-YYYY", step=1
    ),
}

st.set_page_config(page_title="Active Findings Cost Tracker", layout="wide")

if "unwritten_indexes" not in ss:
    ss.unwritten_indexes = []

blank_df = pd.DataFrame(
    columns=[
        "ResourceId",
        "RecommendationId",
        "DateOfSavings",
        "FinOpsStatus",
        "FinOpsLastModified",
        "Comments",
        "Account",
        "estimatedMonthlySavings",
        "Savings Type",
        "Cost Center",
        "Service Group",
        "Optimization Exemption",
        "Resource ID + Type",
    ]
)

df_type_dict = {
    "ResourceId": str,
    "RecommendationId": str,
    "DateOfSavings": "datetime64[ns]",
    "FinOpsStatus": str,
    "FinOpsLastModified": "datetime64[ns]",
    "Comments": str,
    "Account": str,
    "estimatedMonthlySavings": float,
    "Savings Type": str,
    "Cost Center": str,
    "Service Group": str,
    "Optimization Exemption": str,
    "Resource ID + Type": str,
}

blank_df = blank_df.astype(df_type_dict)


def clear_filter():
    for key, value in ss.items():
        if "filter_" in key and not value:
            ss[key] = True


st.markdown("# Active Findings Cost Tracker")
st.sidebar.header("Active Findings Cost Tracker")

ss.tracker_df = codp.ingest_tracker(tracker_type="inprogress", s3=True)

if "DateOfSavings" not in ss.tracker_df.columns:
    st.write("Initializing Date of Savings Row")
    st.write(ss.tracker_df.columns)
    ss.tracker_df = codp.build_savings_date_column(ss.tracker_df)

if len(ss.unwritten_indexes) > 0:
    unwritten_df = blank_df.copy()
    for index in ss.unwritten_indexes:
        unwritten_df.loc[len(unwritten_df)] = ss.tracker_df.loc[index]
    st.markdown(":red[Elements failed to write to S3.  S3 has newer records]")
    st.write(unwritten_df)

with st.form("Tracker_Form"):
    etracker_df = st.data_editor(
        ss.tracker_df,
        num_rows="fixed",
        hide_index=False,
        column_config=COL_CONFIG,
        key="ietracker",
    )
    ss.unwritten_undexes = st.form_submit_button(
        "Save", on_click=codp.modify_inprogress_tracker
    )

st.markdown("# Add Rows To Tracker")

if not "dek" in ss:
    ss.dek = str(uuid.uuid4())

with st.form("Row_Add_Form"):
    add_df = st.data_editor(
        blank_df,
        num_rows="dynamic",
        hide_index=True,
        column_config=COL_CONFIG,
        key=ss.dek,
    )
    st.form_submit_button("Save", on_click=codp.add_self_identified_to_tracker)

# ADD ROPPING OF ROWS AND RENDER

st.write(
    "Current Total of Tracker: $"
    + "{:.2f}".format(etracker_df["estimatedMonthlySavings"].sum())
)
st.sidebar.success("Load Complete")


def filter_process():
    statuses = [i for i in ss.tracker_df["FinOpsStatus"].unique() if i is not None]
    stypes = [i for i in ss.tracker_df["Savings Type"].unique() if i is not None]

    stypes = [x for x in stypes if type(x) == str]

    cfilter = st.sidebar.button(
        "Clear Filters", key="clear_filters", on_click=clear_filter
    )

    st.sidebar.markdown("Status Filter")
    st.write(statuses)
    for item in statuses:
        boxkey = "filter_status_" + item
        st.sidebar.checkbox(item, key=boxkey, value=True)

    st.sidebar.markdown("Savings Type Filter")
    for item in stypes:
        boxkey = "filter_stype_" + item
        st.sidebar.checkbox(item, key=boxkey, value=True)

    checked_statuses = []
    checked_stypes = []

    for box in statuses:
        if ss["filter_status_" + box]:
            checked_statuses.append(box)

    for box in stypes:
        if ss["filter_stype_" + box]:
            checked_stypes.append(box)

    ss.tracker_df = ss.tracker_df[
        ss.tracker_df["FinOpsStatus"].isin(checked_statuses)
        & ss.tracker_df["Savings Type"].isin(checked_stypes)
    ]
