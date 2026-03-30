import math
import dataiku
import pandas as pd
import requests
from dataiku.customrecipe import get_input_names_for_role, get_output_names_for_role, get_recipe_config
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlparse, parse_qs, unquote_plus
from functools import lru_cache
from markdownify import markdownify as md_from_html

# ========= Read config (Settings tab) =========
cfg = get_recipe_config()

CONFLUENCE_BASE = cfg["CONFLUENCE_BASE"].rstrip("/")  # normalize
CONFLUENCE_USER = cfg["CONFLUENCE_USER"]
CONFLUENCE_TOKEN = cfg["CONFLUENCE_TOKEN"]

INPUT_COL = cfg["INPUT_COLUMN"]
OUTPUT_COL = cfg["OUTPUT_COLUMN_NAME"]

VERIFY_SSL = False  # keep as-is, or add a BOOLEAN param if you want

if not (CONFLUENCE_BASE and CONFLUENCE_USER and CONFLUENCE_TOKEN):
    raise RuntimeError("Missing Confluence configuration in recipe settings.")

# ========= Resolve datasets from roles =========
input_name = get_input_names_for_role("input_dataset")[0]
output_name = get_output_names_for_role("output_dataset")[0]

inp = dataiku.Dataset(input_name)
df = inp.get_dataframe()

if INPUT_COL not in df.columns:
    raise RuntimeError(f"Input column '{INPUT_COL}' not found. Available: {list(df.columns)}")

# ========= HTTP session =========
def make_session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter)
    s.mount("https://", adapter)

    s.headers.update({
        "Accept": "application/json",
        "Authorization": f"Bearer {CONFLUENCE_TOKEN}",
    })
    return s

SESSION = make_session()

def _get_json(url, params=None, timeout=20):
    r = SESSION.get(url, params=params, verify=VERIFY_SSL, timeout=timeout)
    r.raise_for_status()
    return r.json()

# ========= Confluence helpers =========
def get_by_id(page_id: str):
    url = f"{CONFLUENCE_BASE}/rest/api/content/{page_id}"
    params = {"expand": "body.view,body.storage,version,space"}
    return _get_json(url, params=params)

def get_by_space_title(space_key: str, title: str):
    url = f"{CONFLUENCE_BASE}/rest/api/content"
    params = {"spaceKey": space_key, "title": title, "expand": "body.view,body.storage"}
    data = _get_json(url, params=params)
    results = data.get("results") or []
    if not results:
        raise ValueError(f"No page found for space={space_key} title={title}")
    return results[0]

def fetch_html_from_url(raw_url: str) -> str:
    try:
        if not raw_url or (isinstance(raw_url, float) and math.isnan(raw_url)):
            return ""
        u = urlparse(str(raw_url))
        if not u.scheme.startswith("http"):
            return ""

        q = parse_qs(u.query)
        if "pageId" in q and q["pageId"]:
            data = get_by_id(q["pageId"][0])
            return (data.get("body", {}).get("view", {}) or {}).get("value", "") or ""

        parts = [p for p in u.path.split("/") if p]

        if len(parts) >= 4 and parts[0] == "spaces" and parts[2] == "pages":
            data = get_by_id(parts[3])
            return (data.get("body", {}).get("view", {}) or {}).get("value", "") or ""

        if len(parts) >= 3 and parts[0] == "display":
            space = parts[1]
            title = unquote_plus(parts[2])
            data = get_by_space_title(space, title)
            return (data.get("body", {}).get("view", {}) or {}).get("value", "") or ""

        return ""
    except Exception as e:
        print(f"[WARN] Failed to fetch HTML for URL='{raw_url}': {e}")
        return ""

def html_to_markdown(html: str) -> str:
    if not html:
        return ""
    return md_from_html(
        html,
        heading_style="ATX",
        bullets="*",
        autolinks=True,
        strip=["style"]
    ).strip()

@lru_cache(maxsize=4096)
def cached_markdown_from_url(url: str) -> str:
    return html_to_markdown(fetch_html_from_url(url))

def safe_str(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

# ========= Transform =========
df[OUTPUT_COL] = df[INPUT_COL].apply(lambda x: cached_markdown_from_url(safe_str(x)))

# ========= Write out =========
out = dataiku.Dataset(output_name)
out.write_with_schema(df)