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
RESERVATION_STYPES = ["purchasereservedinstances", "purchasesavingsplans", "savinsplanspurchase"]
RIGHTSIZE_STYPES = ["rightsize", "upgrade", "rightsize ebs", "rightsizing"]

st.set_page_config(page_title="Cost Savings Dashboards", layout="wide")

ROLL_COL_CONFIG = {
    "Date": st.column_config.DateColumn(
        "Date", min_value=date(2022, 1, 1), format="MM-YYYY", step=1
    ),
    "Idle": st.column_config.NumberColumn(
        "Idle Resources",
        help="Price (in USD)",
        min_value=0,
        step=1,
        format="$%.2f",
    ),
    "Reservations": st.column_config.NumberColumn(
        "Reservations",
        help="Price (in USD)",
        min_value=0,
        step=1,
        format="$%.2f",
    ),
    "Rightsize": st.column_config.NumberColumn(
        "Rightsizing",
        help="Price (in USD)",
        min_value=0,
        step=1,
        format="$%.2f",
    ),
    "Strategic Initiative": st.column_config.NumberColumn(
        "Strategic Initiatives",
        help="Price (in USD)",
        min_value=0,
        step=1,
        format="$%.2f",
    ),
}

COL_CONFIG = {
    "DateOfSavings": st.column_config.DateColumn(
        "DateOfSavings", min_value=date(2022, 1, 1), format="MM-DD-YYYY", step=1
    ),
    "FinOpsLastModified": st.column_config.DateColumn(
        "FinOpsLastModified", min_value=date(2022, 1, 1), format="MM-DD-YYYY", step=1
    ),
}


st.markdown("# Cost Savings Dashboards")
st.sidebar.header("Cost Savings Dashboards")

ss.ctracker_df = codp.ingest_tracker(tracker_type="complete", s3=True)

with st.form("Tracker_Form"):
    cetracker_df = st.data_editor(
        ss.ctracker_df,
        num_rows="fixed",
        hide_index=False,
        column_config=COL_CONFIG,
        key="cetracker",
    )
    ss.unwritten_undexes = st.form_submit_button(
        "Save", on_click=codp.modify_complete_tracker
    )


rollup_df = pd.DataFrame(
    columns=["Date", "Idle", "Reservations",
             "Rightsize", "Strategic Initiative"]
)
rollup_df.set_index("Date")
now = date.today() - relativedelta(months=+1)
dates = []
for i in range(0, 12):
    dates.append(now - relativedelta(months=+i))

rollup_df["Date"] = pd.to_datetime(rollup_df["Date"]).dt.strftime("%Y-%m")

for d in dates:
    idle = 0
    rightsize = 0
    reservation = 0
    strat_init = 0
    for i in range(0, 12):
        roll_d = d - relativedelta(months=+i)
        for index, row in ss.ctracker_df.iterrows():
            if row["FinOpsStatus"].lower() == "complete":
                if (
                    roll_d.year == row["DateOfSavings"].year
                    and roll_d.month == row["DateOfSavings"].month
                ):
                    if row["Savings Type"].lower() in IDLE_STYPES:
                        idle += row["estimatedMonthlySavings"]
                    elif row["Savings Type"].lower() in RESERVATION_STYPES:
                        reservation += row["estimatedMonthlySavings"]
                    elif row["Savings Type"].lower() in RIGHTSIZE_STYPES:
                        rightsize += row["estimatedMonthlySavings"]
                    else:
                        strat_init += row["estimatedMonthlySavings"]

    rollup_df.loc[d] = [d, idle, reservation, rightsize, strat_init]

st.dataframe(rollup_df, column_config=ROLL_COL_CONFIG, hide_index=True)

melt_df = pd.melt(
    rollup_df, id_vars=["Date"], var_name="Savings Type", value_name="value"
)

#c = (
#    alt.Chart(melt_df)
#    .mark_area()
#    .encode(
#        alt.X("Date:T", axis=alt.Axis(format="%B-%Y", tickSize=0)),
#        alt.Y("value:Q").axis().title("Savings Amount $"),
#        alt.Color("Savings Type:N"),
#    )
#)

c = (
    alt.Chart(melt_df)
    .mark_area()
    .encode(
        alt.X("yearmonth(Date):T"),
        alt.Y("value:Q").axis().title("Savings Amount $"),
        alt.Color("Savings Type:N"),
    )

)

st.altair_chart(c)

rollup_sum = (
    rollup_df["Idle"].sum()
    + rollup_df["Reservations"].sum()
    + rollup_df["Rightsize"].sum()
    + rollup_df["Strategic Initiative"].sum()
)

# st.write("Current Total of Tracker: $" + '{:.2f}'.format(rollup_sum))
st.write(f"Current Total of Tracker: ${rollup_sum:,.2f}")

st.sidebar.button(
    "Archive 2+ Year Old Findings",
    key="button_archive_complete_findings",
    on_click=codp.archive_findings,
)

st.sidebar.success("Load Complete")
