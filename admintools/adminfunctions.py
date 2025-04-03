import pandas as pd
import sys
sys.path.append('/workspaces/costopt-streamlit')
import streamlit as st
from streamlit import session_state as ss
from datetime import date
import uuid
import CostOptimizationDataPull as codp

DF_TYPE_DICT = {
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

def split_exempt():
    """
    Moves all Exempt records from Inprogress and Completed data frames

    Args:
        N/A

    Returns:
        N/A

    Note:
        Does a write to S3 of the appropriate files
    """

    tracker_df = codp.ingest_tracker(
            tracker_type="inprogress", s3=True
        )

    ctracker_df = codp.ingest_tracker(
            tracker_type="complete", s3=True)
        
    exdf = pd.DataFrame(
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

    exdf = exdf.astype(DF_TYPE_DICT)

    for index, row in tracker_df.iterrows():
        if (
            tracker_df.loc[index, "FinOpsStatus"].lower() == "exempt"
        ):
            exdf.loc[len(exdf)] = tracker_df.loc[index].copy()
            tracker_df = tracker_df.drop([index])
    for index, row in ctracker_df.iterrows():
        if (
            ctracker_df.loc[index, "FinOpsStatus"].lower() == "exempt"
        ):
            exdf.loc[len(exdf)] = ctracker_df.loc[index].copy()
            ctracker_df = ctracker_df.drop([index])

    codp.write_tracker(tracker_df, tracker_type="inprogress")
    codp.write_tracker(ctracker_df, tracker_type="complete")
    codp.write_tracker(exdf, tracker_type="exempt")