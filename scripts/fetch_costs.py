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
    ce = boto3.client(
        "ce",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    start, end = month_range()
    resp = ce.get_cost_and_usage(
        TimePeriod={"Start": start, "End": end},
        Granularity="MONTHLY",
        Metrics=["UnblendedCost"],
        GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
    )
    services = []
    total = 0.0
    for group in resp["ResultsByTime"][0]["Groups"]:
        amount = float(group["Metrics"]["UnblendedCost"]["Amount"])
        if amount < 0.01:
            continue
        services.append({"name": group["Keys"][0], "amount": round(amount, 2)})
        total += amount
    services.sort(key=lambda x: x["amount"], reverse=True)
    days = prev_14_days()
    daily_end = date.today() + timedelta(days=1)
    daily_resp = ce.get_cost_and_usage(
        TimePeriod={"Start": days[0], "End": daily_end.isoformat()},
        Granularity="DAILY",
        Metrics=["UnblendedCost"],
    )
    daily = []
    for r in daily_resp["ResultsByTime"]:
        daily.append({
            "date":   r["TimePeriod"]["Start"],
            "amount": round(float(r["Total"]["UnblendedCost"]["Amount"]), 2),
        })
    return {"total": round(total, 2), "services": services[:8], "daily": daily}

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
