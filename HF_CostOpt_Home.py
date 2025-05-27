import streamlit as st
from SSOGetCredentials import sso_login
import CostOptimizationDataPull as codp


CHUB_SUMMARY_FILE = "/tmp/chub_sum.parquet"
CHUB_DETAILED_FILE = "/tmp/chub_det.parquet"
SEC2_SUMMARY_FILE = "/tmp/sec2_sum.parquet"
SEC2_DETAILED_FILE = "/tmp/sec2_det.parquet"
UEBS_SUMMARY_FILE = "/tmp/uebs_sum.parquet"
UEBS_DETAILED_FILE = "/tmp/uebs_det.parquet"


st.set_page_config(page_title="Cost Management Working Portal", layout="wide")

"""
### Healthfirst Cost Management Dashboards/Tooling
"""
sso_login(webui=True)

st.write(codp.S3_TRACKER_INPROGRESS)
# Will need to write custom code to ingest S3 parquet, and write back edited data frames

# conn = st.connection('s3', type=FilesConnection,)
# df = conn.read("hf-dev-jmtest/10-CostTracker.parquet", input_format="parquet", ttl=600)

# st.write(df)

st.markdown("## Active SSO Session Established")

st.markdown("## Loading Tracker File")
if "tracker_df" not in st.session_state:
    st.session_state.tracker_df = codp.ingest_tracker(
        tracker_type="inprogress", s3=True
    )

if "ctracker_df" not in st.session_state:
    st.session_state.ctracker_df = codp.ingest_tracker(
        tracker_type="complete", s3=True)

if "extracker_df" not in st.session_state:
    st.session_state.ctracker_df = codp.ingest_tracker(
        tracker_type="exempt", s3=True)

chub_button = st.button(
    "Load Cost Optimization Hub Findings",
    key="load_chub",
    on_click=codp.get_finding,
    args=(CHUB_SUMMARY_FILE, CHUB_DETAILED_FILE, "chub", True),
)
sec2_button = st.button(
    "Load Stopped EC2 Findings",
    key="load_sec2",
    on_click=codp.get_finding,
    args=(SEC2_SUMMARY_FILE, SEC2_DETAILED_FILE, "sec2", True),
)
uebs_button = st.button(
    "Load Unattached EBS Findings",
    key="load_uebs",
    on_click=codp.get_finding,
    args=(UEBS_SUMMARY_FILE, UEBS_DETAILED_FILE, "uebs", True),
)

st.sidebar.success("Load Complete")
# xdf = pd.read_excel("./CostExplorer_merged.xlsx", index_col = 0)

# xdf = xdf.drop(columns="Cost Center total")

# tdf = xdf.T

# st.bar_chart(data=tdf)
