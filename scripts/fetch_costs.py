import boto3
import csv
import gzip
import io
import zipfile
import json
import os
import requests
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

ANTHROPIC_API_KEY        = os.environ.get("ANTHROPIC_API_KEY", "")
OPENAI_API_KEY           = os.environ.get("OPENAI_API_KEY", "")
AZURE_TENANT_ID          = os.environ.get("AZURE_TENANT_ID", "")
AZURE_CLIENT_ID          = os.environ.get("AZURE_CLIENT_ID", "")
AZURE_CLIENT_SECRET      = os.environ.get("AZURE_CLIENT_SECRET", "")
AZURE_SUBSCRIPTION_ID    = os.environ.get("AZURE_SUBSCRIPTION_ID", "")

AWS_MONTHLY_BUDGET       = float(os.environ.get("AWS_BUDGET",       "3000"))
GCP_MONTHLY_BUDGET       = float(os.environ.get("GCP_BUDGET",       "2000"))
ANTHROPIC_MONTHLY_BUDGET = float(os.environ.get("ANTHROPIC_BUDGET", "0"))
OPENAI_MONTHLY_BUDGET    = float(os.environ.get("OPENAI_BUDGET",    "0"))
AZURE_MONTHLY_BUDGET     = float(os.environ.get("AZURE_BUDGET",     "0"))

OUTPUT_PATH  = os.path.join(os.path.dirname(__file__), "..", "data.json")
HISTORY_PATH = os.path.join(os.path.dirname(__file__), "..", "history.json")

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

def fetch_anthropic():
    """Fetch Claude API spending via Anthropic Usage API."""
    if not ANTHROPIC_API_KEY:
        return None
    today = date.today()
    month_start = today.replace(day=1).isoformat()
    days = prev_14_days()
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
    }
    # Monthly usage grouped by model
    resp = requests.get(
        "https://api.anthropic.com/v1/usage",
        headers=headers,
        params={"start_date": month_start, "end_date": today.isoformat()},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    # Each entry has model, input_tokens, output_tokens, and cost fields.
    # Exact field names — verify against console.anthropic.com/docs/api/usage.
    services = []
    total = 0.0
    for entry in data.get("data", []):
        cost = float(entry.get("cost_usd") or entry.get("total_cost") or 0)
        model = entry.get("model", "unknown")
        if cost >= 0.01:
            services.append({"name": model, "amount": round(cost, 2)})
            total += cost
    services.sort(key=lambda x: x["amount"], reverse=True)
    # Daily trend
    daily_resp = requests.get(
        "https://api.anthropic.com/v1/usage",
        headers=headers,
        params={"start_date": days[0], "end_date": today.isoformat(), "granularity": "day"},
        timeout=30,
    )
    daily_resp.raise_for_status()
    daily_map = {}
    for entry in daily_resp.json().get("data", []):
        d = (entry.get("date") or entry.get("start_date") or "")[:10]
        cost = float(entry.get("cost_usd") or entry.get("total_cost") or 0)
        if d:
            daily_map[d] = daily_map.get(d, 0.0) + cost
    daily = [{"date": d, "amount": round(daily_map.get(d, 0.0), 2)} for d in days]
    return {"total": round(total, 2), "services": services[:8], "daily": daily}


def fetch_openai():
    """Fetch OpenAI API spending via OpenAI Billing Usage API."""
    if not OPENAI_API_KEY:
        return None
    today = date.today()
    month_start = today.replace(day=1).isoformat()
    days = prev_14_days()
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    resp = requests.get(
        "https://api.openai.com/v1/dashboard/billing/usage",
        headers=headers,
        params={
            "start_date": month_start,
            "end_date": (today + timedelta(days=1)).isoformat(),
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    # total_usage is in cents
    total = round(data.get("total_usage", 0) / 100, 2)
    # Service breakdown and daily costs from daily_costs entries
    model_costs: dict[str, float] = {}
    daily_map: dict[str, float] = {}
    resp14 = requests.get(
        "https://api.openai.com/v1/dashboard/billing/usage",
        headers=headers,
        params={
            "start_date": days[0],
            "end_date": (today + timedelta(days=1)).isoformat(),
        },
        timeout=30,
    )
    resp14.raise_for_status()
    for day_entry in resp14.json().get("daily_costs", []):
        day_str = date.fromtimestamp(day_entry["timestamp"]).isoformat()
        for item in day_entry.get("line_items", []):
            cost = item.get("cost", 0) / 100
            model_costs[item.get("name", "unknown")] = model_costs.get(item.get("name", "unknown"), 0.0) + cost
            daily_map[day_str] = daily_map.get(day_str, 0.0) + cost
    services = sorted(
        [{"name": k, "amount": round(v, 2)} for k, v in model_costs.items() if v >= 0.01],
        key=lambda x: x["amount"], reverse=True,
    )[:8]
    daily = [{"date": d, "amount": round(daily_map.get(d, 0.0), 2)} for d in days]
    return {"total": total, "services": services, "daily": daily}


def _azure_token():
    resp = requests.post(
        f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/oauth2/v2.0/token",
        data={
            "grant_type":    "client_credentials",
            "client_id":     AZURE_CLIENT_ID,
            "client_secret": AZURE_CLIENT_SECRET,
            "scope":         "https://management.azure.com/.default",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def fetch_azure():
    """Fetch Azure spending via Cost Management Query API."""
    if not all([AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_SUBSCRIPTION_ID]):
        return None
    today = date.today()
    month_start = today.replace(day=1).isoformat()
    days = prev_14_days()
    token = _azure_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = (
        f"https://management.azure.com/subscriptions/{AZURE_SUBSCRIPTION_ID}"
        f"/providers/Microsoft.CostManagement/query?api-version=2023-11-01"
    )
    # Monthly by service
    resp = requests.post(url, headers=headers, timeout=30, json={
        "type": "ActualCost",
        "timeframe": "Custom",
        "timePeriod": {"from": month_start + "T00:00:00Z", "to": today.isoformat() + "T23:59:59Z"},
        "dataset": {
            "granularity": "None",
            "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}},
            "grouping": [{"type": "Dimension", "name": "ServiceName"}],
        },
    })
    resp.raise_for_status()
    props = resp.json()["properties"]
    cols = [c["name"] for c in props["columns"]]
    cost_idx, svc_idx = cols.index("Cost"), cols.index("ServiceName")
    services, total = [], 0.0
    for row in props["rows"]:
        cost = float(row[cost_idx])
        if cost >= 0.01:
            services.append({"name": row[svc_idx], "amount": round(cost, 2)})
            total += cost
    services.sort(key=lambda x: x["amount"], reverse=True)
    # Daily trend
    daily_resp = requests.post(url, headers=headers, timeout=30, json={
        "type": "ActualCost",
        "timeframe": "Custom",
        "timePeriod": {"from": days[0] + "T00:00:00Z", "to": today.isoformat() + "T23:59:59Z"},
        "dataset": {
            "granularity": "Daily",
            "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}},
        },
    })
    daily_resp.raise_for_status()
    dprops = daily_resp.json()["properties"]
    dcols = [c["name"] for c in dprops["columns"]]
    dcost_idx, ddate_idx = dcols.index("Cost"), dcols.index("UsageDate")
    daily_map = {}
    for row in dprops["rows"]:
        raw = str(row[ddate_idx])  # YYYYMMDD integer
        d = f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
        daily_map[d] = round(float(row[dcost_idx]), 2)
    daily = [{"date": d, "amount": daily_map.get(d, 0.0)} for d in days]
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

def update_history(output):
    try:
        with open(HISTORY_PATH) as f:
            history = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        history = {"months": []}
    month = output["month"]
    entry = {"month": month}
    for key in ("aws", "gcp", "anthropic", "openai", "azure"):
        if output.get(key):
            entry[key] = {"total": output[key]["total"], "services": output[key]["services"]}
    months = history["months"]
    for i, m in enumerate(months):
        if m["month"] == month:
            months[i] = entry
            break
    else:
        months.append(entry)
    months.sort(key=lambda x: x["month"])
    with open(HISTORY_PATH, "w") as f:
        json.dump(history, f, indent=2)
    print(f"wrote {HISTORY_PATH}", flush=True)


def main():
    print("fetching AWS costs...", flush=True)
    aws = fetch_aws()
    print(f"  AWS total: ${aws['total']}", flush=True)

    print("fetching GCP costs...", flush=True)
    gcp = fetch_gcp()
    print(f"  GCP total: ${gcp['total']}", flush=True)

    anthropic, openai, azure = None, None, None

    if ANTHROPIC_API_KEY:
        print("fetching Anthropic costs...", flush=True)
        try:
            anthropic = fetch_anthropic()
            print(f"  Anthropic total: ${anthropic['total']}", flush=True)
        except Exception as e:
            print(f"  Anthropic fetch failed: {e}", flush=True)

    if OPENAI_API_KEY:
        print("fetching OpenAI costs...", flush=True)
        try:
            openai = fetch_openai()
            print(f"  OpenAI total: ${openai['total']}", flush=True)
        except Exception as e:
            print(f"  OpenAI fetch failed: {e}", flush=True)

    if AZURE_TENANT_ID:
        print("fetching Azure costs...", flush=True)
        try:
            azure = fetch_azure()
            print(f"  Azure total: ${azure['total']}", flush=True)
        except Exception as e:
            print(f"  Azure fetch failed: {e}", flush=True)

    active_budgets = (
        AWS_MONTHLY_BUDGET + GCP_MONTHLY_BUDGET
        + (ANTHROPIC_MONTHLY_BUDGET if anthropic else 0)
        + (OPENAI_MONTHLY_BUDGET    if openai    else 0)
        + (AZURE_MONTHLY_BUDGET     if azure     else 0)
    )
    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "month": date.today().strftime("%Y-%m"),
        "budget": {
            "aws":       AWS_MONTHLY_BUDGET,
            "gcp":       GCP_MONTHLY_BUDGET,
            "anthropic": ANTHROPIC_MONTHLY_BUDGET,
            "openai":    OPENAI_MONTHLY_BUDGET,
            "azure":     AZURE_MONTHLY_BUDGET,
            "total":     active_budgets,
        },
        "aws":       aws,
        "gcp":       gcp,
        "anthropic": anthropic,
        "openai":    openai,
        "azure":     azure,
    }
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)
    print(f"wrote {OUTPUT_PATH}", flush=True)
    update_history(output)

if __name__ == "__main__":
    main()
