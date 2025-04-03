from datetime import datetime, timedelta
import boto3


def get_cost_and_usage(sso_profile_name):
    # Configure boto3 session with SSO profile
    session = boto3.Session(profile_name=sso_profile_name)

    # Create Cost Explorer client
    ce_client = session.client("ce", "us-east-1")

    # Set the time range for the last 30 days
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=30)

    try:
        # Call the AWS Cost Explorer API to get cost and usage data
        response = ce_client.get_cost_and_usage(
            TimePeriod={"Start": start_date.isoformat(), "End": end_date.isoformat()},
            Granularity="DAILY",
            Metrics=["UnblendedCost", "UsageQuantity"],
            GroupBy=[
                {"Type": "DIMENSION", "Key": "SERVICE"},
                {"Type": "DIMENSION", "Key": "USAGE_TYPE"},
            ],
        )

        return response["ResultsByTime"]

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None
