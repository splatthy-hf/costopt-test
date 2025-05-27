"""
Module for retrieving and processing AWS cost optimization data.

This module provides functions to:
- Pull data from various AWS cost optimization sources
- Process and aggregate finding data
- Manage data persistence
- Handle AWS authentication and session management

The module supports multiple types of cost findings including:
- Cost Optimization Hub recommendations
- Stopped EC2 instances
- Unattached EBS volumes
"""

import hashlib
import multiprocessing
import os
import time
import uuid
from datetime import date, datetime

import boto3
import pandas as pd
import streamlit as st
from streamlit import session_state as ss

from dateutil.relativedelta import relativedelta

from SSOGetCredentials import (
    get_account_names,
    get_accounts,
    map_accountid_to_name,
    check_token_time,
)
if os.environ.get('ENVIRONMENT') == 'DEV':
    S3_TRACKER_COMPLETE = "DevTracking/CostTracker_Complete.parquet"
    S3_TRACKER_EXEMPT = "DevTracking/CostTracker_Exempt.parquet"
    S3_TRACKER_INPROGRESS = "DevTracking/CostTracker_InProgress.parquet"
else:
    S3_TRACKER_COMPLETE = "Tracking/CostTracker_Complete.parquet"
    S3_TRACKER_EXEMPT = "Tracking/CostTracker_Exempt.parquet"
    S3_TRACKER_INPROGRESS = "Tracking/CostTracker_InProgress.parquet"

ACCOUNT = "HF-Payer"
S3_ACCOUNT = "hf-development"
S3_BUCKET = "hf-io-dev-costtracking"
TMP_TRACKER_INPROGRESS = "/tmp/CostTrackerInProgress.parquet"
TMP_TRACKER_COMPLETE = "/tmp/CostTracker_Complete.parquet"
TMP_TRACKER_EXEMPT = "/tmp/CostTracker_Exempt.parquet"
STATUS_IMPORT = True
TRACKER_FILE = "/mnt/local/10-CostTracker.xlsx"
OUTPUT_FILE = "/tmp/Scratch-CostRecommendationFindings.xlsx"
SHEET_NAME = "Cost Tracker Summary"

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


def parse_findings(aggs, itemlist):
    """
    Parses and processes a list of findings, enriching them with additional information and status.

    Args:
        aggs (list): List to store aggregated findings
        itemlist (list): List of items/findings to be processed, containing resource information
            and recommendations

    Returns:
        list: List of processed and enriched findings with the following key information:
            - ResourceId: Identifier for the resource
            - RecommendationId: Unique identifier for the recommendation
            - FinOpsStatus: Current status of the finding (defaults to "Needs Research")
            - FinOpsLastModified: Last modification date
            - Comments: Additional context and details about the finding
            - Account: AWS account identifier
            - Name: Resource name
            - estimatedMonthlySavings: Projected monthly cost savings
            - Savings Type: Type of cost optimization action
            - Cost Center: Associated cost center
            - Service Group: Associated service group
            - Optimization Exemption: Any exemption status

    Notes:
        - For rightsizing recommendations, the function enriches the comments with instance
          type details including CPU cores and RAM for EC2 instances
        - For EBS volume recommendations, the function includes volume type transition details
        - The function sanitizes the output by handling None values and nested dictionaries
    """

    session = boto3.Session(profile_name=ACCOUNT)
    ec2_client = session.client("ec2", "us-east-1")
    # Order of Operations
    # For each current finding in aggs
    # Reconcile if the finding is New, or existing
    # First check if recommendation ID exists
    record = {}
    record["FinOpsStatus"] = "Needs Research"
    record["FinOpsLastModified"] = date.today()
    record["Comments"] = ""

    for item in itemlist:
        agg = {}
        if item.get("resourceId") is None:
            if item.get("ResourceId") is None:
                # Only way this is true is if it's coming from Cost Opt Hub -
                # i.e Savings Plans/Reservations
                resid = item["RecommendationId"]
            else:
                resid = item["ResourceId"]
        else:
            resid = item["resourceId"]

        agg["ResourceId"] = resid
        agg["RecommendationId"] = item["RecommendationId"]
        # st.write('Lookup on Recommendation Id ' + agg['RecommendationId'])
        # record = find_record(ef, agg['RecommendationId'], agg['ResourceId'], agg['Savings Type'])
        agg["FinOpsStatus"] = record["FinOpsStatus"]

        agg["FinOpsLastModified"] = record["FinOpsLastModified"]
        agg["Comments"] = record["Comments"]

        if item.get("accountId") is not None:
            agg["Account"] = item.get("accountId")
        else:
            agg["Account"] = item.get("Account")

        agg["Name"] = item.get("Name")
        agg["estimatedMonthlySavings"] = float(
            item.get("estimatedMonthlySavings"))
        agg["Savings Type"] = item.get("actionType")
        if (agg["FinOpsStatus"] == "Needs Research") and (
            agg["Savings Type"] == "Rightsize"
        ):
            if item["currentResourceType"] == "Ec2Instance":
                r = ec2_client.describe_instance_types(
                    InstanceTypes=[item.get("currentResourceSummary")]
                )
                fromstring = (
                    item.get("currentResourceSummary")
                    + "("
                    + str(r["InstanceTypes"][0]["VCpuInfo"]["DefaultVCpus"])
                    + "Cores / RAM:"
                    + str(r["InstanceTypes"][0]
                          ["MemoryInfo"]["SizeInMiB"] / 1024)
                    + "GB)"
                )
                r = ec2_client.describe_instance_types(
                    InstanceTypes=[item.get("recommendedResourceSummary")]
                )
                tostring = (
                    item.get("recommendedResourceSummary")
                    + "("
                    + str(r["InstanceTypes"][0]["VCpuInfo"]["DefaultVCpus"])
                    + " Cores / RAM: "
                    + str(r["InstanceTypes"][0]
                          ["MemoryInfo"]["SizeInMiB"] / 1024)
                    + "GB)"
                )
                agg["Comments"] = fromstring + \
                    "->" + tostring + agg["Comments"]
            elif item["currentResourceType"] == "EbsVolume":
                agg["Comments"] = (
                    item.get("currentResourceSummary")
                    + "->"
                    + item.get("recommendedResourceSummary")
                    + agg["Comments"]
                )
        agg["Cost Center"] = item.get("Cost Center")
        agg["Service Group"] = item.get("Service Group")
        agg["Optimization Exemption"] = item.get("Optimization Exemption")
        for key, value in agg.items():
            if isinstance(value, dict):
                agg[key] = value["S"]
            elif value is None:
                agg[key] = ""

        aggs.append(agg.copy())
    return aggs


def parse_findings_chunk(args):
    """'
    Perform Parse Findings per chunk of recommendations
    """
    aggregation, chunk = args
    return parse_findings(aggregation, chunk)


def build_savings_date_column(tracker_df):
    """
    Creates a DateOfSavings column in the DataFrame based on specific conditions.

    Args:
        df (pandas.DataFrame): Input DataFrame requiring a savings date column

    Returns:
        pandas.DataFrame: DataFrame with new DateOfSavings column populated
    """
    tracker_df.insert(2, "DateOfSavings", None, allow_duplicates=False)
    for index, row in tracker_df.iterrows():
        if type(tracker_df.loc[index, "FinOpsStatus"]) == str:
            if tracker_df.loc[index, "FinOpsStatus"].lower() == "complete":
                tracker_df.loc[index, "DateOfSavings"] = tracker_df.loc[
                    index, "FinOpsLastModified"
                ]
    return tracker_df


def file_modified_recently(file_path, minutes=1440):
    """
    Checks if a file has been modified within the specified time period.

    Args:
        file_path (str): Path to the file to check
        minutes (int, optional): Number of minutes to check against. Defaults to 1440 (24 hours)

    Returns:
        bool: True if file was modified within specified minutes, False otherwise

    Note:
        Returns False if file doesn't exist or is inaccessible
    """
    # Get the current time
    current_time = time.time()

    # Get the last modification time of the file
    try:
        file_mod_time = os.path.getmtime(file_path)
    except OSError:
        # File doesn't exist or is inaccessible
        return False

    # Calculate the time difference in minutes
    time_difference = (current_time - file_mod_time) / 60

    # Check if the file was modified within the last 15 minutes
    return time_difference <= minutes


def split_inprogress_complete(df):
    """
    Splits a dataframe into two separate dataframes based on completion status.

    Args:
        df (pandas.DataFrame): Input dataframe containing cost optimization records

    Returns:
        tuple: Contains two pandas DataFrames:
            - idf: DataFrame containing in-progress records
            - cdf: DataFrame containing completed records

    Note:
        Records are split based on FinOpsStatus field, with 'complete' and 'exempt'
        status going to the completed DataFrame
    """

    idf = pd.DataFrame(
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
    cdf = pd.DataFrame(
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
    df = df.replace("nan", None)
    df = df.replace("NaN", None)
    idf = idf.astype(DF_TYPE_DICT)
    cdf = cdf.astype(DF_TYPE_DICT)

    for index, row in df.iterrows():
        if (
            df.loc[index, "FinOpsStatus"].lower() == "complete"
            or df.loc[index, "FinOpsStatus"].lower() == "exempt"
        ):
            cdf.loc[len(cdf)] = df.loc[index].copy()
        else:
            idf.loc[len(idf)] = df.loc[index].copy()
    #    idf = idf.replace({np.nan: None})
    #    cdf = cdf.replace({np.nan: None})
    return idf, cdf


def ingest_tracker(tracker_type="inprogress", s3=False):
    """
    Reads and processes the Cost Tracker data from either local Excel file or S3.

    Args:
        tracker_type (str, optional): Type of tracker to ingest ('inprogress' or 'complete').
            Defaults to "inprogress"
        s3 (bool, optional): Whether to read from S3 (True) or local file (False).
            Defaults to False

    Returns:
        pandas.DataFrame: Processed tracker data

    Note:
        When reading from local file, function splits data into inprogress and complete
        trackers and writes both to storage
    """

    if not s3:
        df = pd.read_excel(TRACKER_FILE)
        if "DateOfSavings" not in df.columns:
            df = build_savings_date_column(df)
        df = convert_excel(df)
        idf, cdf = split_inprogress_complete(df)
        write_tracker(idf, tracker_type="inprogress")
        write_tracker(cdf, tracker_type="complete")
    else:
        session = boto3.Session(profile_name=S3_ACCOUNT)
        s3_client = session.client("s3", "us-east-1")
        if tracker_type == "inprogress":
            s3_client.download_file(
                S3_BUCKET, S3_TRACKER_INPROGRESS, TMP_TRACKER_INPROGRESS
            )
            df = pd.read_parquet(TMP_TRACKER_INPROGRESS)
        elif tracker_type == "complete":
            s3_client.download_file(
                S3_BUCKET, S3_TRACKER_COMPLETE, TMP_TRACKER_COMPLETE
            )
            df = pd.read_parquet(TMP_TRACKER_COMPLETE)
        elif tracker_type == "exempt":
            s3_client.download_file(
                S3_BUCKET, S3_TRACKER_EXEMPT, TMP_TRACKER_EXEMPT
            )
            df = pd.read_parquet(TMP_TRACKER_EXEMPT)
    return df


# I think I need to split modify_tracker for the inprogress tracker, and the completed tracker.
# This is getting too complicated


def modify_inprogress_tracker():
    """
    Modifies the in-progress tracker based on edited rows and handles status transitions.

    Updates the in-progress tracker with edited values, manages completion status changes,
    and handles record deletion. When items are marked as complete or exempt, they are
    moved to the completed tracker.

    Returns:
        list: List of indexes that could not be written due to modification conflicts
    """

    s3_i_df = ingest_tracker(tracker_type="inprogress", s3=True)
    s3_c_df = ingest_tracker(tracker_type="complete", s3=True)
    s3_ex_df = ingest_tracker(tracker_type="exempt", s3=True)
    st.markdown("### Detected the following changes:")
    st.write(ss.ietracker["edited_rows"])
    modified_indexes = ss.ietracker["edited_rows"].keys()
    for index in modified_indexes:
        columns = ss.ietracker["edited_rows"][index].keys()
        for column in columns:
            ss.tracker_df.loc[index, column] = ss.ietracker["edited_rows"][index][
                column
            ]
        ss.tracker_df.loc[index, "FinOpsLastModified"] = date.today()

    for index in modified_indexes:
        s3_i_df.loc[index] = ss.tracker_df.loc[index]
        if ss.tracker_df.loc[index, "FinOpsStatus"].lower() == "complete":
            ss.tracker_df.loc[index, "DateOfSavings"] = date.today()
            s3_c_df.loc[len(s3_c_df)] = ss.tracker_df.loc[index]
            st.write(f"Dropping the following Row: {s3_i_df.loc[index]}")
            s3_i_df = s3_i_df.drop([index])
        elif ss.tracker_df.loc[index, "FinOpsStatus"].lower() == "exempt":
            s3_ex_df.loc[len(s3_ex_df)] = ss.tracker_df.loc[index]
            st.write(f"Dropping the following Row: {s3_i_df.loc[index]}")
            s3_i_df = s3_i_df.drop([index])
        elif ss.tracker_df.loc[index, "FinOpsStatus"] == "DeleteMe":
            st.write(f"Dropping the following Row: {s3_i_df.loc[index]}")
            s3_i_df = s3_i_df.drop([index])

    write_tracker(s3_i_df, tracker_type="inprogress", s3=True)
    write_tracker(s3_c_df, tracker_type="complete", s3=True)
    write_tracker(s3_ex_df, tracker_type="exempt", s3=True)


def modify_complete_tracker():
    """
    Modifies the completed tracker based on edited rows.

    Updates the completed tracker with edited values, manages date conversions,
    and handles record deletion requests.

    Returns:
        list: List of indexes that could not be written due to modification conflicts
    """
    s3_c_df = ingest_tracker(tracker_type="complete", s3=True)
    modified_indexes = ss.cetracker["edited_rows"].keys()
    st.markdown("### Detected the following changes:")
    st.write(ss.cetracker["edited_rows"])
    for index in modified_indexes:
        columns = ss.cetracker["edited_rows"][index].keys()
        for column in columns:
            ss.ctracker_df.loc[index, column] = ss.cetracker["edited_rows"][index][
                column
            ]
    unwritten_indexes = []
    for index in modified_indexes:
        if type(ss.ctracker_df.loc[index, "DateOfSavings"]) == str:
            ss.ctracker_df.loc[index, "DateOfSavings"] = datetime.strptime(
                ss.ctracker_df.loc[index, "DateOfSavings"], "%Y-%m-%d"
            ).date()
        ss.ctracker_df.loc[index, "FinOpsLastModified"] = date.today()
        if s3_c_df.loc[index, "FinOpsLastModified"] is None:
            s3_c_df.loc[index] = ss.ctracker_df.loc[index]
        elif (
            ss.ctracker_df.loc[index, "FinOpsLastModified"]
            >= s3_c_df.loc[index, "FinOpsLastModified"]
        ):
            s3_c_df.loc[index] = ss.ctracker_df.loc[index]
        else:
            unwritten_indexes.append(index)
        if ss.ctracker_df.loc[index, "FinOpsStatus"] == "DeleteMe":
            s3_c_df = s3_c_df.drop([index])
    write_tracker(s3_c_df, tracker_type="complete", s3=True)
    return unwritten_indexes


def modify_exempt_tracker():
    """
    Modifies the exempt tracker based on edited rows.

    Updates the exempt tracker with edited values, manages date conversions,
    and handles record deletion requests.

    Returns:
        list: List of indexes that could not be written due to modification conflicts
    """
    s3_e_df = ingest_tracker(tracker_type="exempt", s3=True)
    modified_indexes = ss.eextracker["edited_rows"].keys()
    st.markdown("### Detected the following changes:")
    st.write(ss.eextracker["edited_rows"])
    for index in modified_indexes:
        columns = ss.eextracker["edited_rows"][index].keys()
        for column in columns:
            ss.extracker_df.loc[index, column] = ss.eextracker["edited_rows"][index][
                column
            ]
    unwritten_indexes = []
    for index in modified_indexes:
        if type(ss.extracker_df.loc[index, "DateOfSavings"]) == str:
            ss.extracker_df.loc[index, "DateOfSavings"] = datetime.strptime(
                ss.extracker_df.loc[index, "DateOfSavings"], "%Y-%m-%d"
            ).date()
        ss.extracker_df.loc[index, "FinOpsLastModified"] = date.today()
        if s3_e_df.loc[index, "FinOpsLastModified"] is None:
            s3_e_df.loc[index] = ss.extracker_df.loc[index]
        elif (
            ss.extracker_df.loc[index, "FinOpsLastModified"]
            >= s3_e_df.loc[index, "FinOpsLastModified"]
        ):
            s3_e_df.loc[index] = ss.extracker_df.loc[index]
        else:
            unwritten_indexes.append(index)
        if ss.extracker_df.loc[index, "FinOpsStatus"] == "DeleteMe":
            s3_e_df = s3_e_df.drop([index])
    write_tracker(s3_e_df, tracker_type="exempt", s3=True)
    return unwritten_indexes


def convert_dates(df):
    """
    Converts datetime columns in a DataFrame to date objects.

    Args:
        df (pandas.DataFrame): DataFrame containing datetime columns to be converted

    Returns:
        pandas.DataFrame: DataFrame with datetime columns converted to date objects
    """
    for col in df.columns:
        if df[col].dtype == "datetime64[ns]":
            st.write(col)
            df[col] = df[col].dt.date
    return df


def write_tracker(df, tracker_type="inprogress", s3=True):
    """
    Writes the tracker DataFrame to storage (S3 or local).

    Args:
        df (pandas.DataFrame): DataFrame to be written
        tracker_type (str, optional): Type of tracker ('inprogress' or 'complete').
            Defaults to "inprogress"
        s3 (bool, optional): Whether to write to S3 (True) or local storage (False).
            Defaults to True

    Returns:
        None

    Note:
        When writing to S3, the function also updates the session state with the new DataFrame
    """

    # s3_client.download_file()
    if not s3:
        df.to_excel(TRACKER_FILE, index=False)
    else:
        session = boto3.Session(profile_name=S3_ACCOUNT)
        df = convert_dates(df)
        df = df.reset_index(drop=True)
        s3_client = session.client("s3", "us-east-1")
        if tracker_type == "inprogress":
            df.to_parquet(TMP_TRACKER_INPROGRESS)
            s3_client.upload_file(
                TMP_TRACKER_INPROGRESS, S3_BUCKET, S3_TRACKER_INPROGRESS
            )
            ss.tracker_df = df.copy()
        elif tracker_type == "complete":
            df.to_parquet(TMP_TRACKER_COMPLETE)
            s3_client.upload_file(TMP_TRACKER_COMPLETE,
                                  S3_BUCKET, S3_TRACKER_COMPLETE)
            ss.ctracker_df = df.copy()
        elif tracker_type == "exempt":
            df.to_parquet(TMP_TRACKER_EXEMPT)
            s3_client.upload_file(TMP_TRACKER_EXEMPT,
                                  S3_BUCKET, S3_TRACKER_EXEMPT)
            ss.extracker_df = df.copy()


def convert_excel(df):
    """
    Converts DataFrame columns to specified types and formats dates.

    Args:
        df (pandas.DataFrame): DataFrame to be converted

    Returns:
        pandas.DataFrame: Converted DataFrame with proper column types and date formats
    """

    df = df.astype(DF_TYPE_DICT)
    df["FinOpsLastModified"] = df["FinOpsLastModified"].dt.date
    df["DateOfSavings"] = df["DateOfSavings"].dt.date
    return df


def archive_findings():
    """
    Archives completed findings that are older than 2 years.

    Removes entries from the completed tracker where the DateOfSavings is more than
    2 years old from the current date. Updates the S3 stored tracker after removal.

    Returns:
        None
    """
    curd = date.today()
    s3_c_df = ingest_tracker(tracker_type="complete", s3=True)
    for index, row in s3_c_df.iterrows():
        if s3_c_df.loc[index, "DateOfSavings"] is not None:
            if s3_c_df.loc[index, "DateOfSavings"] < (curd - relativedelta(years=+2)):
                st.write(f"Dropping row {row}")
                s3_c_df = s3_c_df.drop(index)
    write_tracker(s3_c_df, tracker_type="complete", s3=True)


def add_to_tracker(df, s_file=None, state=None):
    """
    Adds selected rows from a source DataFrame to the main tracker.

    Args:
        df (pandas.DataFrame): Source DataFrame containing rows to potentially add
        s_file (str, optional): File path to save the updated source DataFrame
        state (str, optional): Session state key containing edited row information

    Returns:
        pandas.DataFrame: Updated source DataFrame with selected rows removed

    Note:
        Function requires 'tracker_df' to exist in session state
    """
    if "tracker_df" not in ss:
        ss.tracker_df = ingest_tracker(s3=True)
    if state is not None and s_file is not None:
        for index in ss[state]["edited_rows"].keys():
            if ss[state]["edited_rows"][index]["MoveToTracker"]:
                ss.tracker_df.loc[len(ss.tracker_df)] = df.loc[index]
                df.drop(index, inplace=True)
        if s_file is not None:
            df = df.reset_index(drop=True)
            df.to_parquet(s_file)
    else:
        st.markdown(
            ":red[Unexpected Condition, items were NOT added to tracker]")
    st.write(df)
    write_tracker(ss.tracker_df, s3=True)
    return df


def add_self_identified_to_tracker():
    """
    Adds manually entered (self-identified) items to the tracker.

    Processes newly added rows from the session state and adds them to the main tracker.
    Generates a new unique identifier for the data entry key after processing.

    Returns:
        None
    """
    if "tracker_df" not in ss:
        ss.tracker_df = ingest_tracker(s3=True)
    for row in ss[ss.dek]["added_rows"]:
        next_index = len(ss.tracker_df)
        for key in row.keys():
            ss.tracker_df.loc[next_index, key] = row[key]
        st.write(ss.tracker_df.loc[next_index])
    write_tracker(ss.tracker_df, s3=True)
    ss.dek = str(uuid.uuid4())


def tagmapper(items):
    """
    Extracts relevant tags from AWS resources and prepares them for sheet columns.

    Args:
        items (list): List of dictionaries containing AWS resource information and tags

    Returns:
        list: Modified items list with relevant tags extracted to top-level keys

    Note:
        Relevant tags include: 'Name', 'Cost Center', 'Service Group', 'Optimization Exemption'
    """

    relevant_tags = ["Name", "Cost Center",
                     "Service Group", "Optimization Exemption"]
    for item in items:
        if len(item["tags"]) > 0:
            if item["tags"][0].get("Key") is not None:
                key = "Key"
                val = "Value"
            else:
                key = "key"
                val = "value"
            for tag in item["tags"]:
                if tag[key] in relevant_tags:
                    item[tag[key]] = tag[val]
    return items


def calcost_ebs(vtype, iops, throughput, size):
    """
    Calculates the cost of an EBS volume based on its configuration.

    Args:
        vtype (str): Volume type ('gp2', 'gp3', 'st1', 'sc1', 'io1', 'io2')
        iops (int): IOPS value for the volume
        throughput (int): Throughput value in MB/s
        size (int): Size of the volume in GB

    Returns:
        float: Monthly cost of the EBS volume in USD

    Raises:
        TypeError: If size is not an integer
        ValueError: If size is not positive
    """

    # Validate the size parameter
    if not isinstance(size, int):
        raise TypeError("Size must be an integer.")
    if size <= 0:
        raise ValueError("Size must be a positive integer.")

    if vtype == "gp2":
        cost = 0.10 * size
    elif vtype == "gp3":
        cost = 0.08 * size
        if iops > 3000:
            cost += (iops - 3000) * 0.005
        if throughput > 125:
            cost += (throughput - 125) * 0.04
    elif vtype == "st1":
        cost = 0.045 * size
    elif vtype == "sc1":
        cost = 0.015 * size
    elif vtype == "io1":
        cost = (0.125 * size) + (0.065 * iops)
    elif vtype == "io2":
        cost = 0.125 * size
        if iops > 64000:
            cost += (32000 * 0.065) + (32000 * 0.046) + \
                ((iops - 64000) * 0.032)
        elif iops > 32000:
            cost += (32000 * 0.065) + ((iops - 32000) * 0.046)
        else:
            cost += iops * 0.065
    elif vtype == "standard":
        cost = 0.05 * size
    else:
        cost = 1000000
    return cost


def find_unattached_ebs():
    """
    Iterates across all known AWS accounts looking for EBS volumes that are not attached.

    Returns:
        List of dictionaries containing information about unattached EBS volumes
            with estimated monthly savings and other relevant details

    """
    volumes = []
    ANAMES = get_account_names()
    for account in ANAMES:
        session = boto3.Session(profile_name=account)
        ec2_client = session.client("ec2", "us-east-1")
        paginator = ec2_client.get_paginator("describe_volumes")
        rit = paginator.paginate(
            Filters=[{"Name": "status", "Values": ["available"]}])

        for page in rit:
            for volume in page["Volumes"]:
                vol = {}
                cost = calcost_ebs(
                    volume["VolumeType"],
                    volume.get("Iops"),
                    volume.get("Throughput"),
                    volume["Size"],
                )
                vol["resourceId"] = volume["VolumeId"]
                vol["Account"] = account
                vol["estimatedMonthlySavings"] = cost
                vol["actionType"] = "Unattached EBS"
                vol["Type"] = volume["VolumeType"]
                vol["Size"] = volume["Size"]
                vol["IOPS"] = volume.get("Iops")
                vol["Throughput"] = volume.get("Throughput")
                if volume.get("Tags") is not None:
                    vol["tags"] = volume["Tags"]
                else:
                    vol["tags"] = []
                vol["RecommendationId"] = recid_hasher(
                    vol["Account"], vol["resourceId"], vol["estimatedMonthlySavings"]
                )
                volumes.append(vol.copy())
    volumes = tagmapper(volumes)

    return volumes


def recid_hasher(value1, value2, value3):
    """
    Take a recommendation that does not have an ID and generate
    a recommendation ID based on a hash of critical values for that
    item
    """
    hashme = str(value1) + str(value2) + str(value3)
    return hashlib.sha256(hashme.encode("utf-8")).hexdigest()


def find_stopped_ec2():
    """Locate EC2 instances that are stopped and identify their EBS cost"""
    instances = []
    ANAMES = get_account_names()
    for account in ANAMES:
        session = boto3.Session(profile_name=account)
        ec2_client = session.client("ec2", "us-east-1")
        paginator = ec2_client.get_paginator("describe_instances")
        rit = paginator.paginate(
            Filters=[{"Name": "instance-state-name", "Values": ["stopped"]}]
        )

        for page in rit:
            st.write(
                "Found "
                + str(len(page["Reservations"]))
                + " Reservations to calculate for EC2 Savings in "
                + str(account)
            )
            for reservation in page["Reservations"]:
                for instance in reservation["Instances"]:
                    inst = {}
                    cost = 0
                    inst["resourceId"] = instance["InstanceId"]
                    inst["Account"] = account
                    inst["state"] = instance["State"]
                    inst["type"] = instance["InstanceType"]
                    inst["platform"] = instance.get("Platform")
                    inst["volumes"] = []
                    inst["actionType"] = "Stopped EC2 Instance"
                    for dev in instance["BlockDeviceMappings"]:
                        vid = dev["Ebs"]["VolumeId"]
                        inst["volumes"].append(vid)
                        r = ec2_client.describe_volumes(VolumeIds=[vid])
                        volume = r["Volumes"][0]
                        cost += calcost_ebs(
                            volume["VolumeType"],
                            volume.get("Iops"),
                            volume.get("Throughput"),
                            volume["Size"],
                        )
                    inst["estimatedMonthlySavings"] = cost
                    if instance.get("Tags") is None:
                        inst["tags"] = []
                    else:
                        inst["tags"] = instance["Tags"]
                    inst["RecommendationId"] = recid_hasher(
                        inst["Account"],
                        inst["resourceId"],
                        inst["estimatedMonthlySavings"],
                    )
                    instances.append(inst.copy())

    instances = tagmapper(instances)
    return instances


def aggregate_summary_parallel(findings, status_import, status_file=None):
    """
    Create an aggregate view that removes the details and creates common
    elements to enable status tracking.  This view will also be what is
    consumed to persist status tracking
    """
    aggregation = []

    # Split the recommendations list into chunks of 250
    chunk_size = 250
    findings_chunks = [
        findings[i: i + chunk_size] for i in range(0, len(findings), chunk_size)
    ]
    print("Chunking/Parsing CHub Recommendations and sanitizing")
    # Create a pool of worker processes
    with multiprocessing.Pool() as pool:
        # Process each chunk in parallel
        results = pool.map(
            parse_findings_chunk, [([], chunk) for chunk in findings_chunks]
        )

        # Combine the results from all chunks
        aggregation = [item for result in results for item in result]

    print("Importing status from Tracker File " + status_file)
    if status_import:
        aggregation = import_status(aggregation)

    return aggregation


def import_status(aggregation):
    """Function is designed to parse a Cost Tracker file, and find in-progress or
    exempt findings, and load their status"""
    if "tracker_df" not in ss:
        ss.tracker_df = ingest_tracker(tracker_type="inprogress", s3=True)
    if "extracker_df" not in ss:
        ss.extracker_df = ingest_tracker(tracker_type="exempt", s3=True)

    #   merged_df = ss.tracker_df.append(ss.ctracker_df, ignore_index=True)

    merged_df = pd.concat([ss.tracker_df, ss.extracker_df], ignore_index=True)

    print("Comparing Source File to Generated Output")
    for index, row in merged_df.iterrows():
        if merged_df.loc[index, "ResourceId"] is not None:
            if merged_df.loc[index, "FinOpsStatus"] is None:
                merged_df.loc[index, "FinOpsStatus"] = "Needs Research"
            status = merged_df.loc[index, "FinOpsStatus"]
            if status.lower() not in [
                "needs research",
                "complete",
                "completed",
                "archived",
                "archive",
            ]:
                resid = merged_df.loc[index, "ResourceId"]
                stype = merged_df.loc[index, "Savings Type"]
                for agg in aggregation:
                    if agg["ResourceId"] == resid and agg["Savings Type"] == stype:
                        agg["FinOpsStatus"] = status
    return aggregation


def get_finding(sfile, dfile, func, force_update=False):
    """
    Retrieves or generates finding data based on file freshness and force update flag.

    Args:
        sfile (str): Path to the summary parquet file
        dfile (str): Path to the detailed parquet file
        func (str): Function identifier to use for data generation ('chub', 'sec2', or 'uebs')
        force_update (bool, optional): Force regeneration of data from AWS. Defaults to False.

    Returns:
        tuple: A tuple containing two pandas DataFrames:
            - summary_df: Summarized findings data
            - detailed_df: Detailed findings data

    Raises:
        SystemExit: If AWS access token is expired or data refresh is needed
    """

    switcher = {"chub": gen_chub, "sec2": gen_sec2, "uebs": gen_uebs}

    if (
        file_modified_recently(sfile)
        and file_modified_recently(dfile)
        and not force_update
    ):
        summary_df = pd.read_parquet(sfile)
        detailed_df = pd.read_parquet(dfile)
    else:
        st.write("## Findings are Stale need to regenerate them from AWS")
        if check_token_time() == "":
            st.markdown(
                ":red[AWS Access Token Expired - Please return to Homepage to reload]"
            )
            st.stop()
        elif not force_update:
            st.markdown(
                ":red[Please return to the home screen and refresh findings data data]"
            )
            st.stop()
        summary_df, detailed_df = switcher[func]()
        summary_df.to_parquet(sfile)
        detailed_df.to_parquet(dfile)

    return summary_df, detailed_df


def gen_uebs():
    """
    Generates summary and detailed dataframes for unattached EBS volumes.

    Returns:
        tuple: A tuple containing two pandas DataFrames:
            - summary_df: Summarized unattached EBS volume data
            - detailed_df: Detailed unattached EBS volume data
    """
    uebs = find_unattached_ebs()
    detailed_df = pd.DataFrame(uebs)
    summary_df = pd.DataFrame(
        aggregate_summary_parallel(uebs, STATUS_IMPORT, TRACKER_FILE)
    )

    return summary_df, detailed_df


def gen_sec2():
    """
    Generates summary and detailed dataframes for stopped EC2 instances.

    Returns:
        tuple: A tuple containing two pandas DataFrames:
            - summary_df: Summarized stopped EC2 instance data
            - detailed_df: Detailed stopped EC2 instance data
    """

    stopped_ec2s = find_stopped_ec2()
    detailed_df = pd.DataFrame(stopped_ec2s)
    summary_df = pd.DataFrame(
        aggregate_summary_parallel(stopped_ec2s, STATUS_IMPORT, TRACKER_FILE)
    )

    return summary_df, detailed_df


def gen_chub():
    """
    Generates cost optimization recommendations from AWS Cost Optimization Hub.

    Retrieves recommendations filtered by implementation effort (Very Low, Low, Medium)
    for the current account.

    Returns:
        tuple: A tuple containing two pandas DataFrames:
            - summary_df: Summarized cost optimization recommendations
            - detailed_df: Detailed cost optimization recommendations
    """

    st.write("Working in account number " + ACCOUNT + " currently")

    request_filter = {
        "implementationEfforts": ["VeryLow", "Low", "Medium"],
    }
    session = boto3.Session(profile_name=ACCOUNT)
    chub_client = session.client("cost-optimization-hub", "us-east-1")
    paginator = chub_client.get_paginator("list_recommendations")
    recommendations = []
    pruned_recommendations = []

    for page in paginator.paginate(filter=request_filter):
        recommendations.extend(page["items"])
    accounts = get_accounts()
    for recommendation in recommendations:
        if recommendation.get("currentResourceType") != "RdsReservedInstances":
            recommendation["RecommendationId"] = recommendation["recommendationId"]
            recommendation["accountId"] = map_accountid_to_name(
                accounts, recommendation["accountId"]
            )
            pruned_recommendations.append(recommendation.copy())

    st.write("Generating Cost Optimization Hub Findings")
    pruned_recommendations = tagmapper(pruned_recommendations)
    st.write("Number of Cost Hub Findings: " +
             str(len(pruned_recommendations)))

    summary_df = pd.DataFrame(
        aggregate_summary_parallel(
            pruned_recommendations, STATUS_IMPORT, TRACKER_FILE)
    )
    detailed_df = pd.DataFrame(pruned_recommendations)

    return summary_df, detailed_df
