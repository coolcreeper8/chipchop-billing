import boto3
import json
import os
from datetime import datetime, date, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

# ─── config ────────────────────────────────────────────────────────────────────
# set these as GitHub Secrets, they land as env vars in the Action
AWS_ACCESS_KEY_ID     = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION            = os.environ.get("AWS_REGION", "us-east-1")

GCP_SERVICE_ACCOUNT_JSON = os.environ["GCP_SERVICE_ACCOUNT_JSON"]   # full JSON string
GCP_BILLING_PROJECT      = os.environ["GCP_BILLING_PROJECT"]         # project that owns the BQ dataset
GCP_BQ_DATASET           = os.environ["GCP_BQ_DATASET"]              # e.g. billing_export
GCP_BQ_TABLE             = os.environ["GCP_BQ_TABLE"]                # e.g. gcp_billing_export_v1_XXXXXX

AWS_MONTHLY_BUDGET  = float(os.environ.get("AWS_BUDGET",  "3000"))
GCP_MONTHLY_BUDGET  = float(os.environ.get("GCP_BUDGET",  "2000"))

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data.json")

# ─── helpers ───────────────────────────────────────────────────────────────────
def month_range():
    today = date.today()
    start = today.replace(day=1)
    end   = today + timedelta(days=1)  # exclusive end, always after start
    return start.isoformat(), end.isoformat()

def prev_14_days():
    today = date.today()
    return [(today - timedelta(days=i)).isoformat() for i in range(13, -1, -1)]

# ─── AWS ───────────────────────────────────────────────────────────────────────
def fetch_aws():
    ce = boto3.client(
        "ce",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    start, end = month_range()

    # total + per-service breakdown
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

    # daily trend for last 14 days
    days = prev_14_days()
    daily_resp = ce.get_cost_and_usage(
        TimePeriod={"Start": days[0], "End": end},
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

# ─── GCP ───────────────────────────────────────────────────────────────────────
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

    # total + per-service breakdown
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

    # daily trend for last 14 days
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

# ─── main ──────────────────────────────────────────────────────────────────────
def main():
    print("fetching AWS costs...")
    aws = fetch_aws()
    print(f"  AWS total: ${aws['total']}")

    print("fetching GCP costs...")
    gcp = fetch_gcp()
    print(f"  GCP total: ${gcp['total']}")

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
    print(f"wrote {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
