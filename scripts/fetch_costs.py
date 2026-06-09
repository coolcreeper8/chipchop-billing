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
import re as _re
def _clean_guid(v): return _re.sub(r'[^0-9a-fA-F\-]', '', v.strip())
AZURE_TENANT_ID          = _clean_guid(os.environ.get("AZURE_TENANT_ID", ""))
AZURE_CLIENT_ID          = _clean_guid(os.environ.get("AZURE_CLIENT_ID", ""))
AZURE_CLIENT_SECRET      = os.environ.get("AZURE_CLIENT_SECRET", "").strip()
AZURE_SUBSCRIPTION_ID    = _clean_guid(os.environ.get("AZURE_SUBSCRIPTION_ID", ""))

def _budget(key, default):
    return float(os.environ.get(key) or default)

AWS_MONTHLY_BUDGET       = _budget("AWS_BUDGET",       3000)
GCP_MONTHLY_BUDGET       = _budget("GCP_BUDGET",       2000)
ANTHROPIC_MONTHLY_BUDGET = _budget("ANTHROPIC_BUDGET", 0)
OPENAI_MONTHLY_BUDGET    = _budget("OPENAI_BUDGET",    0)
AZURE_MONTHLY_BUDGET     = _budget("AZURE_BUDGET",     0)

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

def past_months(n=6):
    """Returns list of (start, end, YYYY-MM) for last n completed months."""
    today = date.today()
    months = []
    for i in range(1, n + 1):
        month = today.month - i
        year  = today.year
        while month <= 0:
            month += 12
            year  -= 1
        first = date(year, month, 1)
        last  = date(first.year + 1, 1, 1) if first.month == 12 else date(first.year, first.month + 1, 1)
        months.append((first.isoformat(), last.isoformat(), first.strftime("%Y-%m")))
    return list(reversed(months))

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
    # Use the billing-period START date as the S3 prefix so we only match the
    # current month's directory (e.g. 20260601-...) and never accidentally pick
    # up last month's directory whose end date shares the same YYYYMM prefix.
    month_first = today.replace(day=1).strftime("%Y%m%d")
    paginator = s3.get_paginator("list_objects_v2")
    csv_keys = [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=s3_prefix + month_first)
        for obj in page.get("Contents", [])
        if obj["Key"].endswith((".csv.gz", ".csv.zip"))
    ]

    if not csv_keys:
        print(f"  No CUR CSV files found under s3://{bucket}/{s3_prefix}{month_first}*/")
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
            service    = row.get("product/ProductName") or row.get("lineItem/ProductCode", "")
            usage_date = (row.get("lineItem/UsageStartDate") or "")[:10]
            if service and usage_date >= month_start:
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
    """Fetch Claude API spending via Anthropic Cost Admin API.
    Requires an Admin API key (sk-ant-admin...) — NOT a regular API key.
    Get one at: console.anthropic.com → Settings → Admin Keys.
    """
    if not ANTHROPIC_API_KEY:
        return None
    today = date.today()
    month_start = today.replace(day=1)
    days = prev_14_days()
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
    }
    base_params = [
        ("starting_at",  month_start.strftime("%Y-%m-%dT00:00:00Z")),
        ("ending_at",    today.strftime("%Y-%m-%dT23:59:59Z")),
        ("bucket_width", "1d"),
        ("limit",        "31"),
    ]

    # Model-level costs
    resp = requests.get(
        "https://api.anthropic.com/v1/organizations/cost_report",
        headers=headers,
        params=base_params + [("group_by[]", "description")],
        timeout=30,
    )
    resp.raise_for_status()
    model_costs: dict[str, float] = {}
    daily_map:   dict[str, float] = {}
    for bucket in resp.json().get("data", []):
        day = (bucket.get("starting_at") or "")[:10]
        for result in bucket.get("results", []):
            cost_usd = float(result.get("amount", "0") or "0") / 100
            model = result.get("model") or result.get("description", "unknown")
            if cost_usd > 0:
                model_costs[model] = model_costs.get(model, 0.0) + cost_usd
                daily_map[day]     = daily_map.get(day, 0.0) + cost_usd
    services = sorted(
        [{"name": k, "amount": round(v, 2)} for k, v in model_costs.items() if v >= 0.01],
        key=lambda x: x["amount"], reverse=True,
    )[:8]
    daily = [{"date": d, "amount": round(daily_map.get(d, 0.0), 2)} for d in days]

    # Workspace (user/team) breakdown — resolve workspace IDs to names
    workspace_names: dict[str, str] = {}
    try:
        ws_list = requests.get(
            "https://api.anthropic.com/v1/organizations/workspaces",
            headers=headers, timeout=15,
        )
        if ws_list.ok:
            for ws in ws_list.json().get("data", []):
                workspace_names[ws["id"]] = ws.get("name", ws["id"])
    except Exception:
        pass

    ws_resp = requests.get(
        "https://api.anthropic.com/v1/organizations/cost_report",
        headers=headers,
        params=base_params + [("group_by[]", "workspace_id")],
        timeout=30,
    )
    ws_costs: dict[str, float] = {}
    if ws_resp.ok:
        for bucket in ws_resp.json().get("data", []):
            for result in bucket.get("results", []):
                cost_usd = float(result.get("amount", "0") or "0") / 100
                ws_id   = result.get("workspace_id") or "default"
                ws_name = workspace_names.get(ws_id, ws_id)
                if cost_usd > 0:
                    ws_costs[ws_name] = ws_costs.get(ws_name, 0.0) + cost_usd
    users = sorted(
        [{"name": k, "amount": round(v, 2)} for k, v in ws_costs.items() if v >= 0.01],
        key=lambda x: x["amount"], reverse=True,
    )[:8]

    return {
        "total": round(sum(model_costs.values()), 2),
        "services": services,
        "users": users,
        "daily": daily,
    }


def _openai_all_buckets(headers: dict, params: dict) -> list:
    """Fetch all pages from the OpenAI organization costs endpoint."""
    buckets = []
    p = dict(params, limit=100)
    while True:
        r = requests.get(
            "https://api.openai.com/v1/organization/costs",
            headers=headers, params=p, timeout=30,
        )
        r.raise_for_status()
        body = r.json()
        buckets.extend(body.get("data", []))
        if not body.get("has_more"):
            break
        next_page = body.get("next_page")
        if not next_page:
            break
        p = dict(p, page=next_page)
    return buckets


def fetch_openai():
    """Fetch OpenAI API spending via OpenAI Organization Costs API.
    Requires an Admin API key — NOT a regular inference API key.
    Get one at: platform.openai.com → Settings → Organization → Admin API keys.
    """
    if not OPENAI_API_KEY:
        return None
    from datetime import timezone
    today = date.today()
    month_start = today.replace(day=1)
    days = prev_14_days()
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    start_ts = int(datetime(month_start.year, month_start.month, month_start.day, tzinfo=timezone.utc).timestamp())
    end_ts   = int(datetime(today.year, today.month, today.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())

    # Model-level costs — paginated
    model_costs: dict[str, float] = {}
    daily_map:   dict[str, float] = {}
    for bucket in _openai_all_buckets(headers, {"start_time": start_ts, "end_time": end_ts, "bucket_width": "1d"}):
        day = date.fromtimestamp(bucket.get("start_time", 0)).isoformat()
        for result in bucket.get("results", []):
            cost = float((result.get("amount") or {}).get("value", 0) or 0)
            model = result.get("line_item") or result.get("model_id") or "unknown"
            if cost > 0:
                model_costs[model] = model_costs.get(model, 0.0) + cost
                daily_map[day]     = daily_map.get(day, 0.0) + cost
    services = sorted(
        [{"name": k, "amount": round(v, 2)} for k, v in model_costs.items() if v >= 0.01],
        key=lambda x: x["amount"], reverse=True,
    )[:8]
    daily = [{"date": d, "amount": round(daily_map.get(d, 0.0), 2)} for d in days]

    # Project-level breakdown — resolve project IDs to names
    project_names: dict[str, str] = {}
    try:
        proj_list = requests.get(
            "https://api.openai.com/v1/organization/projects",
            headers=headers, timeout=15,
        )
        if proj_list.ok:
            for p in proj_list.json().get("data", []):
                project_names[p["id"]] = p.get("name", p["id"])
    except Exception:
        pass

    proj_costs: dict[str, float] = {}
    for bucket in _openai_all_buckets(headers, {"start_time": start_ts, "end_time": end_ts, "bucket_width": "1d", "group_by[]": "project_id"}):
        for result in bucket.get("results", []):
            cost = float((result.get("amount") or {}).get("value", 0) or 0)
            proj_id   = result.get("project_id") or "default"
            proj_name = project_names.get(proj_id, proj_id)
            if cost > 0:
                proj_costs[proj_name] = proj_costs.get(proj_name, 0.0) + cost
    users = sorted(
        [{"name": k, "amount": round(v, 2)} for k, v in proj_costs.items() if v >= 0.01],
        key=lambda x: x["amount"], reverse=True,
    )[:8]

    return {
        "total": round(sum(model_costs.values()), 2),
        "services": services,
        "users": users,
        "daily": daily,
    }


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
    if not resp.ok:
        print(f"  Azure token error body: {resp.text}", flush=True)
    resp.raise_for_status()
    return resp.json()["access_token"]


def fetch_azure():
    """Fetch Azure spending via Cost Management Query API."""
    missing = [k for k, v in [
        ("AZURE_TENANT_ID", AZURE_TENANT_ID), ("AZURE_CLIENT_ID", AZURE_CLIENT_ID),
        ("AZURE_CLIENT_SECRET", AZURE_CLIENT_SECRET), ("AZURE_SUBSCRIPTION_ID", AZURE_SUBSCRIPTION_ID),
    ] if not v]
    if missing:
        print(f"  Azure: skipping — missing secrets: {missing}", flush=True)
        return None
    today = date.today()
    month_start = today.replace(day=1).isoformat()
    days = prev_14_days()
    token = _azure_token()
    print(f"  Azure: token OK", flush=True)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = (
        f"https://management.azure.com/subscriptions/{AZURE_SUBSCRIPTION_ID}"
        f"/providers/Microsoft.CostManagement/query?api-version=2023-03-01"
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
    if not resp.ok:
        print(f"  Azure CM query error {resp.status_code}: {resp.text[:500]}", flush=True)
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
    # Resource-group breakdown
    rg_resp = requests.post(url, headers=headers, timeout=30, json={
        "type": "ActualCost",
        "timeframe": "Custom",
        "timePeriod": {"from": month_start + "T00:00:00Z", "to": today.isoformat() + "T23:59:59Z"},
        "dataset": {
            "granularity": "None",
            "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}},
            "grouping": [{"type": "Dimension", "name": "ResourceGroup"}],
        },
    })
    users = []
    if rg_resp.ok:
        rg_props = rg_resp.json()["properties"]
        rg_cols = [c["name"] for c in rg_props["columns"]]
        rg_cost_idx, rg_name_idx = rg_cols.index("Cost"), rg_cols.index("ResourceGroup")
        rg_costs = {}
        for row in rg_props["rows"]:
            cost = float(row[rg_cost_idx])
            name = row[rg_name_idx] or "unassigned"
            if cost >= 0.01:
                rg_costs[name] = rg_costs.get(name, 0.0) + cost
        users = sorted(
            [{"name": k, "amount": round(v, 2)} for k, v in rg_costs.items()],
            key=lambda x: x["amount"], reverse=True,
        )[:8]
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
    if not daily_resp.ok:
        print(f"  Azure daily query error {daily_resp.status_code}: {daily_resp.text[:500]}", flush=True)
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
    return {"total": round(total, 2), "services": services[:8], "users": users, "daily": daily}


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


def _aws_budget_history(kwargs):
    """Return {YYYY-MM: amount} for all past months from budget performance history."""
    try:
        account_id = boto3.client("sts", region_name="us-east-1", **kwargs).get_caller_identity()["Account"]
        budgets_client = boto3.client("budgets", region_name="us-east-1", **kwargs)
        budgets = budgets_client.describe_budgets(AccountId=account_id).get("Budgets", [])
        if not budgets:
            return {}
        budget_name = budgets[0]["BudgetName"]
        # fetch up to 12 months of performance history
        from datetime import timezone
        end_dt   = datetime.now(tz=timezone.utc)
        start_dt = end_dt.replace(month=1, day=1) if end_dt.month > 6 else \
                   datetime(end_dt.year - 1, end_dt.month + 6, 1, tzinfo=timezone.utc)
        resp = budgets_client.describe_budget_performance_history(
            AccountId=account_id,
            BudgetName=budget_name,
            TimePeriod={"Start": start_dt, "End": end_dt},
        )
        result = {}
        for entry in resp.get("BudgetPerformanceHistory", {}).get("BudgetedAndActualAmountsList", []):
            actual = float(entry.get("ActualAmount", {}).get("Amount", 0) or 0)
            period_start = entry.get("TimePeriod", {}).get("Start")
            if period_start and actual > 0:
                if isinstance(period_start, str):
                    label = period_start[:7]
                else:
                    label = period_start.strftime("%Y-%m")
                result[label] = round(actual, 2)
        print(f"  AWS budget history: {result}", flush=True)
        return result
    except Exception as e:
        print(f"  AWS budget history failed: {e}", flush=True)
        return {}


def backfill_history():
    """Query past 12 months for all providers and keep history.json up to date."""
    try:
        with open(HISTORY_PATH) as f:
            history = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        history = {"months": []}

    months_list = history.get("months", [])

    has_real_aws = {m["month"] for m in months_list if (m.get("aws") or {}).get("total", 0) > 0}
    has_real_ant = {m["month"] for m in months_list if (m.get("anthropic") or {}).get("total", 0) > 0}
    # Only trust OpenAI history if total > $1; values near $0 were likely from the 7-bucket pagination bug
    has_real_oai = {m["month"] for m in months_list if (m.get("openai") or {}).get("total", 0) > 1.0}
    has_real_az  = {m["month"] for m in months_list if (m.get("azure") or {}).get("total", 0) > 0}

    needs = set()
    for _, _, lbl in past_months(12):
        if lbl not in has_real_aws:
            needs.add(lbl)
        if ANTHROPIC_API_KEY and lbl not in has_real_ant:
            needs.add(lbl)
        if OPENAI_API_KEY and lbl not in has_real_oai:
            needs.add(lbl)
        if AZURE_TENANT_ID and lbl not in has_real_az:
            needs.add(lbl)

    to_fill = [(s, e, lbl) for s, e, lbl in past_months(12) if lbl in needs]
    if not to_fill:
        return

    print(f"  Backfilling {len(to_fill)} past months...", flush=True)
    kwargs = {"aws_access_key_id": AWS_ACCESS_KEY_ID, "aws_secret_access_key": AWS_SECRET_ACCESS_KEY}
    aws_history = _aws_budget_history(kwargs)

    for m_start, m_end, m_label in to_fill:
        print(f"    {m_label}...", flush=True)
        import time; time.sleep(2)  # avoid rate-limiting across providers

        # Load existing entry so we only overwrite what we're fixing
        existing = next((m for m in months_list if m["month"] == m_label), {"month": m_label})
        entry = dict(existing)

        # AWS
        if m_label not in has_real_aws:
            aws_total = aws_history.get(m_label, 0.0)
            entry["aws"] = {"total": aws_total, "services": []}
            if aws_total > 0:
                print(f"      AWS: ${aws_total}", flush=True)

        # GCP
        if m_label not in has_real_aws:  # re-fetch GCP alongside AWS for completeness
            try:
                creds_info = json.loads(GCP_SERVICE_ACCOUNT_JSON)
                creds = service_account.Credentials.from_service_account_info(
                    creds_info, scopes=["https://www.googleapis.com/auth/cloud-platform"])
                client = bigquery.Client(project=GCP_BILLING_PROJECT, credentials=creds)
                table = f"`{GCP_BILLING_PROJECT}.{GCP_BQ_DATASET}.{GCP_BQ_TABLE}`"
                total_row = list(client.query(f"""
                    SELECT ROUND(SUM(cost),2) AS total FROM {table}
                    WHERE DATE(usage_start_time) >= '{m_start}' AND DATE(usage_start_time) < '{m_end}'
                """).result())
                gcp_total = float(total_row[0].total or 0) if total_row else 0.0
                svc_rows = list(client.query(f"""
                    SELECT service.description AS svc, ROUND(SUM(cost),2) AS amt FROM {table}
                    WHERE DATE(usage_start_time) >= '{m_start}' AND DATE(usage_start_time) < '{m_end}'
                      AND cost > 0
                    GROUP BY svc ORDER BY amt DESC LIMIT 8
                """).result())
                entry["gcp"] = {
                    "total": gcp_total,
                    "services": [{"name": r.svc, "amount": float(r.amt)} for r in svc_rows if float(r.amt) >= 0.01],
                }
            except Exception as e:
                print(f"      GCP: {e}", flush=True)

        # OpenAI — paginated
        if OPENAI_API_KEY and m_label not in has_real_oai:
            try:
                from datetime import timezone
                oai_hdrs = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
                ts0 = int(datetime.strptime(m_start, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
                ts1 = int(datetime.strptime(m_end,   "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
                oai_costs: dict[str, float] = {}
                for b in _openai_all_buckets(oai_hdrs, {"start_time": ts0, "end_time": ts1, "bucket_width": "1d"}):
                    for res in b.get("results", []):
                        cost = float((res.get("amount") or {}).get("value", 0) or 0)
                        model = res.get("line_item") or res.get("model_id") or "unknown"
                        if cost > 0:
                            oai_costs[model] = oai_costs.get(model, 0.0) + cost
                oai_total = round(sum(oai_costs.values()), 2)
                entry["openai"] = {
                    "total": oai_total,
                    "services": sorted(
                        [{"name": k, "amount": round(v, 2)} for k, v in oai_costs.items() if v >= 0.01],
                        key=lambda x: x["amount"], reverse=True
                    )[:8],
                }
                print(f"      OpenAI: ${oai_total}", flush=True)
            except Exception as e:
                print(f"      OpenAI: {e}", flush=True)

        # Anthropic — always re-fetch if we have the key and month was missing
        if ANTHROPIC_API_KEY and m_label not in has_real_ant:
            try:
                m_last = (date.fromisoformat(m_end) - timedelta(days=1)).isoformat()
                r = requests.get(
                    "https://api.anthropic.com/v1/organizations/cost_report",
                    headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01"},
                    params=[
                        ("starting_at",  m_start + "T00:00:00Z"),
                        ("ending_at",    m_last  + "T23:59:59Z"),
                        ("bucket_width", "1d"),
                        ("group_by[]",   "description"),
                        ("limit",        "31"),
                    ],
                    timeout=30,
                )
                r.raise_for_status()
                ant_costs: dict[str, float] = {}
                for b in r.json().get("data", []):
                    for res in b.get("results", []):
                        # amount is in cents
                        cost_usd = float(res.get("amount", "0") or "0") / 100
                        model = res.get("model") or res.get("description", "unknown")
                        if cost_usd > 0:
                            ant_costs[model] = ant_costs.get(model, 0.0) + cost_usd
                ant_total = round(sum(ant_costs.values()), 2)
                entry["anthropic"] = {
                    "total": ant_total,
                    "services": sorted(
                        [{"name": k, "amount": round(v, 2)} for k, v in ant_costs.items() if v >= 0.01],
                        key=lambda x: x["amount"], reverse=True
                    )[:8],
                }
                print(f"      Anthropic: ${ant_total}", flush=True)
            except Exception as e:
                print(f"      Anthropic: {e}", flush=True)

        # Azure — query Cost Management for each past month
        if AZURE_TENANT_ID and m_label not in has_real_az:
            try:
                az_token = _azure_token()
                az_hdrs = {"Authorization": f"Bearer {az_token}", "Content-Type": "application/json"}
                az_url = (
                    f"https://management.azure.com/subscriptions/{AZURE_SUBSCRIPTION_ID}"
                    f"/providers/Microsoft.CostManagement/query?api-version=2023-03-01"
                )
                az_resp = requests.post(az_url, headers=az_hdrs, timeout=30, json={
                    "type": "ActualCost",
                    "timeframe": "Custom",
                    "timePeriod": {"from": m_start + "T00:00:00Z", "to": (date.fromisoformat(m_end) - timedelta(days=1)).isoformat() + "T23:59:59Z"},
                    "dataset": {
                        "granularity": "None",
                        "aggregation": {"totalCost": {"name": "Cost", "function": "Sum"}},
                        "grouping": [{"type": "Dimension", "name": "ServiceName"}],
                    },
                })
                az_resp.raise_for_status()
                az_props = az_resp.json()["properties"]
                az_cols  = [c["name"] for c in az_props["columns"]]
                ci, si   = az_cols.index("Cost"), az_cols.index("ServiceName")
                az_svcs, az_total = [], 0.0
                for row in az_props["rows"]:
                    cost = float(row[ci])
                    if cost >= 0.01:
                        az_svcs.append({"name": row[si], "amount": round(cost, 2)})
                        az_total += cost
                az_svcs.sort(key=lambda x: x["amount"], reverse=True)
                entry["azure"] = {"total": round(az_total, 2), "services": az_svcs[:8]}
                print(f"      Azure: ${round(az_total, 2)}", flush=True)
            except Exception as e:
                print(f"      Azure: {e}", flush=True)

        # Merge entry back into months list
        for i, m in enumerate(months_list):
            if m["month"] == m_label:
                months_list[i] = entry
                break
        else:
            months_list.append(entry)

    months_list.sort(key=lambda x: x["month"])
    with open(HISTORY_PATH, "w") as f:
        json.dump(history, f, indent=2)
    print(f"  Backfill wrote {HISTORY_PATH}", flush=True)


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
        print(f"  tenant={AZURE_TENANT_ID[:8]}...(len={len(AZURE_TENANT_ID)}) client={AZURE_CLIENT_ID[:8]}...(len={len(AZURE_CLIENT_ID)}) sub={AZURE_SUBSCRIPTION_ID[:8]}...{AZURE_SUBSCRIPTION_ID[-4:]}(len={len(AZURE_SUBSCRIPTION_ID)})", flush=True)
        try:
            azure = fetch_azure()
            if azure is None:
                print("  Azure: skipped — one or more secrets missing/empty", flush=True)
            else:
                print(f"  Azure total: ${azure['total']}", flush=True)
        except Exception as e:
            import traceback
            print(f"  Azure fetch failed: {e}", flush=True)
            traceback.print_exc()

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
    backfill_history()
    update_history(output)

if __name__ == "__main__":
    main()
