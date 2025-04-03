import pandas as pd
import streamlit as st
from streamlit import session_state as ss
import CostOptimizationDataPull as codp
from datetime import date
from dateutil.relativedelta import relativedelta
import altair as alt

IDLE_STYPES = [
    "unattached ebs",
    "idle",
    "idle rds",
    "idle ec2",
    "idle dms",
    "idle lambda",
    "stop",
    "stopped ec2 instance",
    "s3 lifecycle policy",
]
RESERVATION_STYPES = ["purchasereservedinstances", "purchasesavingsplans"]
RIGHTSIZE_STYPES = ["rightsize", "upgrade", "rightsize ebs", "rightsizing"]

st.set_page_config(page_title="Cost Exemptions", layout="wide")

COL_CONFIG = {
    "DateOfSavings": st.column_config.DateColumn(
        "DateOfSavings", min_value=date(2022, 1, 1), format="MM-DD-YYYY", step=1
    ),
    "FinOpsLastModified": st.column_config.DateColumn(
        "FinOpsLastModified", min_value=date(2022, 1, 1), format="MM-DD-YYYY", step=1
    ),
}

st.markdown("# Cost Exemption Dashboards")
st.sidebar.header("Cost Exemption Dashboards")

ss.extracker_df = codp.ingest_tracker(tracker_type="exempt", s3=True)

with st.form("Tracker_Form"):
    eextracker_df = st.data_editor(
        ss.extracker_df,
        column_order=[
            "ResourceId",
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
            "RecommendationId",
            "DateOfSavings" 
            ],
        num_rows="fixed",
        hide_index=False,
        column_config=COL_CONFIG,
        key="eextracker",
    )
    ss.unwritten_undexes = st.form_submit_button(
        "Save", on_click=codp.modify_exempt_tracker
    )

rollup_df = pd.DataFrame(
    columns=["Cost Center", "Exemption Total"]
)
rollup_df.set_index("Cost Center")

for index, row in ss.extracker_df.iterrows():
    cc = eextracker_df.loc[index]['Cost Center']
    if cc is None:
        cc = 'NA'
    if cc not in rollup_df.index:
        rollup_df.loc[cc] = [cc, 0]
    rollup_df.loc[cc, 'Exemption Total'] += ss.extracker_df.loc[index]['estimatedMonthlySavings']
    
st.dataframe(rollup_df, hide_index=True)

c = (
    alt.Chart(ss.extracker_df)
    .mark_bar()
    .encode(
        alt.X("Cost Center:O"),
        alt.Y("estimatedMonthlySavings:Q").axis().title("Savings Amount $"),
        alt.Color("Savings Type"),
    )
)
st.altair_chart(c)

text_input = st.text_input("Enter Cost Center to Filter: ")

if text_input is not None:
    sgrollup_df = pd.DataFrame(
        columns=["Service Group", "Exemption Total"]
    )
    sgrollup_df.set_index("Service Group")
    for index, row in ss.extracker_df.iterrows():
        if row['Cost Center'] == text_input:
            sg = row['Service Group']
            if sg is None:
                sg = "No Tag Set"
            if sg not in sgrollup_df.index:
                sgrollup_df.loc[sg] = [sg, 0]
            sgrollup_df.loc[sg, 'Exemption Total'] += ss.extracker_df.loc[index]['estimatedMonthlySavings']

    c2 = (
        alt.Chart(sgrollup_df)
        .mark_bar()
        .encode(
            alt.X("Service Group:O"),
            alt.Y("Exemption Total:Q").axis().title("Savings Amount $"),
#            alt.Color("Savings Type"),
        )
    )
    st.altair_chart(c2)


st.sidebar.success("Load Complete")
