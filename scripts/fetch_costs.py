import boto3
import csv
import gzip
import io
import zipfile
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
    return start.isoformat(), end.isoformat()

def prev_14_days():
    today = date.today()
    return [(today - timedelta(days=i)).isoformat() for i in range(13, -1, -1)]

def _read_cur_s3(report, kwargs):
    """Read CUR CSV.gz files from S3 and return (service_list, daily_amounts_by_date)."""
    if report.get("Format", "textCSV") not in ("textCSV", "textORcsv"):
        print(f"  CUR report '{report['ReportName']}' uses {report.get('Format')} format; only textCSV/textORcsv is supported")
        return None

    bucket = report["S3Bucket"]
    raw_prefix = report.get("S3Prefix", "").strip("/")
    report_name = report["ReportName"]
    s3_prefix = f"{raw_prefix}/{report_name}/" if raw_prefix else f"{report_name}/"

    s3 = boto3.client("s3", region_name="us-east-1", **kwargs)
    today = date.today()
    month_tag = today.strftime("%Y%m")

    paginator = s3.get_paginator("list_objects_v2")
    csv_keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=s3_prefix)
        for obj in page.get("Contents", [])
        if month_tag in obj["Key"] and obj["Key"].endswith((".csv.gz", ".csv.zip"))
    ]

    if not csv_keys:
        print(f"  No CUR CSV files found under s3://{bucket}/{s3_prefix} for {month_tag}")
        return None

    services: dict[str, float] = {}
    daily: dict[str, float] = {}
    month_start = today.replace(day=1).isoformat()

    for key in csv_keys:
        print(f"  Reading CUR: s3://{bucket}/{key}", flush=True)
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if key.endswith(".csv.zip"):
            zf = zipfile.ZipFile(io.BytesIO(body))
            text = io.TextIOWrapper(zf.open(zf.namelist()[0]), encoding="utf-8")
        else:
            text = io.TextIOWrapper(gzip.GzipFile(fileobj=io.BytesIO(body)), encoding="utf-8")
        reader = csv.DictReader(text)
        for row in reader:
                try:
                    cost = float(row.get("lineItem/UnblendedCost") or 0)
                except ValueError:
                    continue
                if cost < 0.0001:
                    continue
                service = row.get("product/ProductName") or row.get("lineItem/ProductCode", "")
                usage_date = (row.get("lineItem/UsageStartDate") or "")[:10]
                if service:
                    services[service] = services.get(service, 0.0) + cost
                if usage_date >= month_start:
                    daily[usage_date] = daily.get(usage_date, 0.0) + cost

    service_list = sorted(
        [{"name": k, "amount": round(v, 2)} for k, v in services.items() if v >= 0.01],
        key=lambda x: x["amount"], reverse=True,
    )
    return service_list[:8], {k: round(v, 2) for k, v in daily.items()}


def fetch_aws():
    kwargs = {"aws_access_key_id": AWS_ACCESS_KEY_ID, "aws_secret_access_key": AWS_SECRET_ACCESS_KEY}

    # Real total from Budgets API — CE data warehouse is unavailable for this account
    account_id = boto3.client("sts", region_name="us-east-1", **kwargs).get_caller_identity()["Account"]
    b_resp = boto3.client("budgets", region_name="us-east-1", **kwargs).describe_budgets(AccountId=account_id)
    total = 0.0
    for b in b_resp.get("Budgets", []):
        actual = b.get("CalculatedSpend", {}).get("ActualSpend", {})
        if actual.get("Amount"):
            total = float(actual["Amount"])
            break

    # Try CUR first for service breakdown and daily costs
    services = []
    cur_daily: dict[str, float] = {}
    try:
        cur_client = boto3.client("cur", region_name="us-east-1", **kwargs)
        reports = cur_client.describe_report_definitions().get("ReportDefinitions", [])
        print(f"  Found {len(reports)} CUR report(s)", flush=True)
        for report in reports:
            result = _read_cur_s3(report, kwargs)
            if result:
                services, cur_daily = result
                print(f"  CUR gave {len(services)} services, {len(cur_daily)} daily entries", flush=True)
                break
    except Exception as e:
        print(f"  CUR unavailable: {e}", flush=True)

    # Fall back to CE for service breakdown if CUR yielded nothing
    ce = boto3.client("ce", region_name="us-east-1", **kwargs)
    if not services:
        start, end = month_range()
        try:
            resp = ce.get_cost_and_usage(
                TimePeriod={"Start": start, "End": end},
                Granularity="MONTHLY",
                Metrics=["UnblendedCost"],
                GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
            )
            for group in resp["ResultsByTime"][0]["Groups"]:
                amount = float(group["Metrics"]["UnblendedCost"]["Amount"])
                if amount < 0.01:
                    continue
                services.append({"name": group["Keys"][0], "amount": round(amount, 2)})
            services.sort(key=lambda x: x["amount"], reverse=True)
            services = services[:8]
        except Exception as e:
            print(f"  CE service breakdown failed: {e}", flush=True)

    # Build daily list for last 14 days, merging CUR data where available
    days = prev_14_days()
    if cur_daily:
        daily = [{"date": d, "amount": cur_daily.get(d, 0.0)} for d in days]
    else:
        try:
            daily_resp = ce.get_cost_and_usage(
                TimePeriod={"Start": days[0], "End": (date.today() + timedelta(days=1)).isoformat()},
                Granularity="DAILY",
                Metrics=["UnblendedCost"],
            )
            daily = [
                {"date": r["TimePeriod"]["Start"],
                 "amount": round(max(float(r["Total"]["UnblendedCost"]["Amount"]), 0.0), 2)}
                for r in daily_resp["ResultsByTime"]
            ]
        except Exception as e:
            print(f"  CE daily trend failed: {e}", flush=True)
            daily = [{"date": d, "amount": 0.0} for d in days]

    return {"total": round(total, 2), "services": services, "daily": daily}

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
