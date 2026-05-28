import boto3
import json
import os
from datetime import datetime, date, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

AWS_ACCESS_KEY_ID     = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION            = os.environ.get("AWS_REGION", "us-east-1")

GCP_SERVICE_ACCOUNT_JSON = os.environ["GCP_SERVICE_ACCOUNT_JSON"]
GCP_BILLING_PROJECT      = os.environ["GCP_BILLING_PROJECT"]
GCP_BQ_DATASET           = os.environ["GCP_BQ_DATASET"]
GCP_BQ_TABLE             = os.environ["GCP_BQ_TABLE"]

AWS_MONTHLY_BUDGET  = float(os.environ.get("AWS_BUDGET",  "3000"))
GCP_MONTHLY_BUDGET  = float(os.environ.get("GCP_BUDGET",  "2000"))

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data.json")

def month_range():
    today = date.today()
    start = today.replace(day=1)
    if today.month == 12:
        end = date(today.year + 1, 1, 1)
    else:
        end = date(today.year, today.month + 1, 1)
    print(f"DEBUG: start={start}, end={end}", flush=True)
    return start.isoformat(), end.isoformat()

def prev_14_days():
    today = date.today()
    return [(today - timedelta(days=i)).isoformat() for i in range(13, -1, -1)]

def fetch_aws():
    sts = boto3.client(
        "sts",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    identity = sts.get_caller_identity()
    account_id = identity['Account']
    print(f"  AWS account ID in use: {account_id}")

    # check budgets for real billing-system total
    try:
        budgets_client = boto3.client("budgets", region_name="us-east-1",
            aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        b_resp = budgets_client.describe_budgets(AccountId=account_id)
        for b in b_resp.get("Budgets", []):
            actual = b.get("CalculatedSpend", {}).get("ActualSpend", {})
            print(f"  DEBUG budget '{b['BudgetName']}': actual={actual.get('Amount')} {actual.get('Unit')}")
    except Exception as e:
        print(f"  DEBUG budgets error: {e}")

    # check for alternate billing views
    try:
        billing_client = boto3.client("billing", region_name="us-east-1",
            aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        views = billing_client.list_billing_views(activeTimeRange={
            "activeAfterInclusive": f"{date.today().replace(day=1).isoformat()}T00:00:00Z",
            "activeBeforeInclusive": f"{date.today().isoformat()}T23:59:59Z",
        })
        print(f"  DEBUG billing views: {[v.get('arn') for v in views.get('billingViews', [])]}")
    except Exception as e:
        print(f"  DEBUG billing views error: {e}")

    ce = boto3.client(
        "ce",
        region_name="us-east-1",  # Cost Explorer is only available in us-east-1
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    start, end = month_range()

    billing_view_arn = "arn:aws:billing::679617709218:billingview/primary"

    # total across all linked accounts using primary billing view
    total_resp = ce.get_cost_and_usage(
        TimePeriod={"Start": start, "End": end},
        Granularity="MONTHLY",
        Metrics=["AmortizedCost"],
        GroupBy=[{"Type": "DIMENSION", "Key": "LINKED_ACCOUNT"}],
        BillingViewArn=billing_view_arn,
    )
    total = 0.0
    for g in total_resp["ResultsByTime"][0]["Groups"]:
        amt = float(g["Metrics"]["AmortizedCost"]["Amount"])
        print(f"  DEBUG account {g['Keys'][0]}: ${amt}")
        total += amt

    # per-service breakdown using primary billing view
    resp = ce.get_cost_and_usage(
        TimePeriod={"Start": start, "End": end},
        Granularity="MONTHLY",
        Metrics=["AmortizedCost"],
        GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
        BillingViewArn=billing_view_arn,
    )
    services = []
    for group in resp["ResultsByTime"][0]["Groups"]:
        amount = float(group["Metrics"]["AmortizedCost"]["Amount"])
        if amount < 0.01:
            continue
        services.append({"name": group["Keys"][0], "amount": round(amount, 2)})
    services.sort(key=lambda x: x["amount"], reverse=True)
    days = prev_14_days()
    daily_end = date.today() + timedelta(days=1)
    daily_resp = ce.get_cost_and_usage(
        TimePeriod={"Start": days[0], "End": daily_end.isoformat()},
        Granularity="DAILY",
        Metrics=["AmortizedCost"],
        BillingViewArn=billing_view_arn,
    )
    daily = []
    for r in daily_resp["ResultsByTime"]:
        daily.append({
            "date":   r["TimePeriod"]["Start"],
            "amount": round(float(r["Total"]["AmortizedCost"]["Amount"]), 2),
        })
    return {"total": round(max(total, 0.0), 2), "services": services[:8], "daily": daily}

def fetch_gcp():
    creds_info = json.loads(GCP_SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(project=GCP_BILLING_PROJECT, credentials=creds)
    today = date.today()
    month_start = today.replace(day=1).isoformat()
    table = f"`{GCP_BILLING_PROJECT}.{GCP_BQ_DATASET}.{GCP_BQ_TABLE}`"
    service_query = f"""
        SELECT
            service.description AS service_name,
            ROUND(SUM(cost), 2)  AS total_cost
        FROM {table}
        WHERE DATE(usage_start_time) >= '{month_start}'
          AND DATE(usage_start_time) <= '{today.isoformat()}'
          AND cost > 0
        GROUP BY service_name
        ORDER BY total_cost DESC
        LIMIT 8
    """
    services = []
    total = 0.0
    for row in client.query(service_query).result():
        services.append({"name": row.service_name, "amount": float(row.total_cost)})
        total += float(row.total_cost)
    cutoff = (today - timedelta(days=13)).isoformat()
    daily_query = f"""
        SELECT
            DATE(usage_start_time)  AS day,
            ROUND(SUM(cost), 2)     AS total_cost
        FROM {table}
        WHERE DATE(usage_start_time) >= '{cutoff}'
          AND DATE(usage_start_time) <= '{today.isoformat()}'
        GROUP BY day
        ORDER BY day ASC
    """
    daily = []
    for row in client.query(daily_query).result():
        daily.append({"date": str(row.day), "amount": float(row.total_cost)})
    return {"total": round(total, 2), "services": services, "daily": daily}

def main():
    print("fetching AWS costs...", flush=True)
    aws = fetch_aws()
    print(f"  AWS total: ${aws['total']}", flush=True)
    print("fetching GCP costs...", flush=True)
    gcp = fetch_gcp()
    print(f"  GCP total: ${gcp['total']}", flush=True)
    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "month": date.today().strftime("%Y-%m"),
        "budget": {
            "aws":   AWS_MONTHLY_BUDGET,
            "gcp":   GCP_MONTHLY_BUDGET,
            "total": AWS_MONTHLY_BUDGET + GCP_MONTHLY_BUDGET,
        },
        "aws": aws,
        "gcp": gcp,
    }
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)
    print(f"wrote {OUTPUT_PATH}", flush=True)

if __name__ == "__main__":
    main()
