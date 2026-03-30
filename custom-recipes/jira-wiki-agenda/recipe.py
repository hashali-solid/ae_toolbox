# Code for custom recipe jira-wiki-agenda
# Fetches Jira issues by filter, compares with latest Confluence work-week child page via LLM,
# creates a new child page with change report, outputs a copy of that page.

import html
import json
import re
import time
import dataiku
import pandas as pd
import requests
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dataiku.customrecipe import get_output_names_for_role, get_recipe_config

VERIFY_SSL = False

# #region agent log
DEBUG_LOG = r"c:\Users\ahashim\OneDrive - NANDPS\Code\Dataiku\gsg-toolbox\debug-66f5b9.log"
def _debug_log(msg, data, hypothesis_id, location="recipe.py"):
    try:
        with open(DEBUG_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps({"sessionId": "66f5b9", "timestamp": int(time.time() * 1000), "location": location, "message": msg, "data": data, "hypothesisId": hypothesis_id}) + "\n")
    except Exception:
        pass
# #endregion

# ---------- Config ----------
cfg = get_recipe_config()
JIRA_BASE = (cfg.get("jira_base_url") or "").rstrip("/")
JIRA_TOKEN = cfg.get("jira_token") or ""
JIRA_FILTER_ID = (cfg.get("jira_filter_id") or "").strip()
_raw_jira_fields = cfg.get("jira_fields") or []
JIRA_FIELDS = [
    {"field_title": (x.get("field_title") or "").strip(), "field_id": (x.get("field_id") or "").strip()}
    for x in _raw_jira_fields
    if (x.get("field_id") or "").strip()
]
if not JIRA_FIELDS:
    JIRA_FIELDS = [
        {"field_title": "Key", "field_id": "key"},
        {"field_title": "Summary", "field_id": "summary"},
        {"field_title": "Description", "field_id": "description"},
        {"field_title": "Comment", "field_id": "comment"},
    ]
JIRA_FIELDS_STR = ",".join(f["field_id"] for f in JIRA_FIELDS)
WIKI_BASE = (cfg.get("wiki_base_url") or "").rstrip("/")
WIKI_USERNAME = (cfg.get("wiki_username") or "").strip()
WIKI_TOKEN = cfg.get("wiki_token") or ""
WIKI_PARENT_ID = (cfg.get("wiki_parent_page_id") or "").strip()
llm_param = cfg.get("llm_selection")
LLM_SYSTEM_PROMPT = (cfg.get("llm_system_prompt") or "").strip()

if not JIRA_BASE or not JIRA_TOKEN or not JIRA_FILTER_ID:
    raise RuntimeError("Missing Jira configuration: jira_base_url, jira_token, jira_filter_id are required.")
if not WIKI_BASE or not WIKI_USERNAME or not WIKI_TOKEN or not WIKI_PARENT_ID:
    raise RuntimeError("Missing Wiki configuration: wiki_base_url, wiki_username, wiki_token, wiki_parent_page_id are required.")
if not llm_param:
    raise RuntimeError("LLM selection is required.")

# LLM id: param can be string (id) or dict with "id"
llm_id = llm_param if isinstance(llm_param, str) else (llm_param.get("id") or llm_param.get("name") or "")

# ---------- Output ----------
output_names = get_output_names_for_role("main_output")
if not output_names:
    raise RuntimeError("Main output dataset is required.")
output_dataset = dataiku.Dataset(output_names[0])


def _jira_session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({"Accept": "application/json", "Authorization": f"Bearer {JIRA_TOKEN}"})
    return s


def fetch_jira_issues():
    session = _jira_session()
    all_issues = []
    start_at = 0
    max_results = 50
    while True:
        url = f"{JIRA_BASE}/rest/api/2/search"
        params = {
            "jql": f"filter={JIRA_FILTER_ID}",
            "startAt": start_at,
            "maxResults": max_results,
            "fields": JIRA_FIELDS_STR,
        }
        r = session.get(url, params=params, verify=VERIFY_SSL, timeout=60)
        r.raise_for_status()
        data = r.json()
        issues = data.get("issues") or []
        all_issues.extend(issues)
        total = data.get("total", 0)
        if start_at + len(issues) >= total:
            break
        start_at += len(issues)
        if start_at >= total:
            break
    return all_issues


def _wiki_session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET", "POST", "PUT"])
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {WIKI_TOKEN}",
    })
    return s


def _parse_work_week_from_title(title):
    """Return (year, week) or None. Supports WWww - (MM/DD-YYYY), MM-DD (Www-YYYY), ISO YYYY-Www, Www, 'Week of YYYY-MM-DD'."""
    if not title:
        return None
    title = title.strip()
    # WWww - (MM/DD/YYYY) e.g. WW10 - (03/02/2026)
    m = re.search(r"WW(\d{1,2})\s*-\s*\((\d{1,2})/(\d{1,2})/(\d{4})\)", title, re.I)
    if m:
        return (int(m.group(4)), int(m.group(1)))
    # MM-DD (Www-YYYY) e.g. 03-09 (W10-2026)
    m = re.search(r"\(W(\d{1,2})-(\d{4})\)", title, re.I)
    if m:
        return (int(m.group(2)), int(m.group(1)))
    # ISO: 2025-W09
    m = re.match(r"(\d{4})-W(\d{1,2})\b", title, re.I)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    # WW24 or W24
    m = re.search(r"W{1,2}(\d{1,2})\b", title, re.I)
    if m:
        y = datetime.utcnow().year
        return (y, int(m.group(1)))
    # Week of 2025-02-24
    m = re.search(r"Week\s+of\s+(\d{4})-(\d{2})-(\d{2})", title, re.I)
    if m:
        from datetime import date
        try:
            d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            iso = d.isocalendar()
            return (iso[0], iso[1])
        except Exception:
            return None
    return None


def get_parent_and_children(wiki_session):
    parent_url = f"{WIKI_BASE}/rest/api/content/{WIKI_PARENT_ID}"
    r = wiki_session.get(parent_url, params={"expand": "space"}, verify=VERIFY_SSL, timeout=30)
    r.raise_for_status()
    parent = r.json()
    space_key = (parent.get("space") or {}).get("key")
    if not space_key:
        raise RuntimeError("Could not get space key from parent page.")

    child_url = f"{WIKI_BASE}/rest/api/content/{WIKI_PARENT_ID}/child/page"
    pages = []
    start = 0
    limit = 100
    expand = "body.view,body.storage,version"
    while True:
        r = wiki_session.get(child_url, params={"start": start, "limit": limit, "expand": expand}, verify=VERIFY_SSL, timeout=30)
        r.raise_for_status()
        data = r.json()
        # Confluence: some versions return page.results, others return top-level results
        chunk = (data.get("page") or {}).get("results") or data.get("results") or []
        pages.extend(chunk)
        if len(chunk) < limit:
            break
        start += limit
    return parent, space_key, pages


def select_latest_work_week_page(pages):
    """Return the page with latest work-week in title, or latest by version.when, or None."""
    with_week = []
    for p in pages:
        title = (p.get("title") or "").strip()
        parsed = _parse_work_week_from_title(title)
        when = (p.get("version") or {}).get("when")
        if parsed:
            with_week.append((parsed, when, p))
    if with_week:
        with_week.sort(key=lambda x: (x[0][0], x[0][1]), reverse=True)
        return with_week[0][2]
    if pages:
        with_when = [((p.get("version") or {}).get("when") or "", p) for p in pages]
        with_when.sort(key=lambda x: x[0], reverse=True)
        return with_when[0][1]
    return None


# Max length for long text fields (description, comment) to avoid blowing LLM context
MAX_FIELD_TEXT_LEN = 8000


def _format_field_value(val, field_id=""):
    """Format a Jira field value for LLM-readable text. Handles dict, list, string."""
    if val is None:
        return ""
    if isinstance(val, dict):
        # Jira user/assignee: prefer name derived from email (e.g. Yujiao.Hou@... -> Yujiao Hou)
        email = val.get("emailAddress") or ""
        if isinstance(email, str) and "@" in email:
            local = email.split("@")[0].strip()
            if local:
                name_from_email = local.replace(".", " ").title()
                return name_from_email
        # Common Jira objects: status, assignee, priority, etc.
        name = val.get("name") or val.get("displayName") or val.get("value")
        if name is not None:
            return str(name).strip()
        if "comments" in val:
            return _format_field_value(val["comments"], field_id)
        # Fallback: join non-nested values
        parts = [str(v) for k, v in val.items() if v is not None and not isinstance(v, (dict, list))]
        return " | ".join(parts) if parts else ""
    if isinstance(val, list):
        if not val:
            return ""
        # Comment list: each item often has body, author, created
        out = []
        for i, item in enumerate(val):
            if isinstance(item, dict):
                body = (item.get("body") or item.get("content") or "")
                if isinstance(body, dict):
                    body = body.get("plain", body.get("storage", "")) or ""
                text = (body or "").strip()
                if text:
                    author = (item.get("author") or {})
                    if isinstance(author, dict):
                        author = author.get("displayName") or author.get("name") or ""
                    created = (item.get("created") or item.get("updated") or "")[:10]
                    out.append(f"[{created} {author}]: {text}")
            else:
                out.append(str(item))
        return "\n".join(out[-20:]) if out else ""  # last 20 comments
    s = str(val).strip()
    if len(s) > MAX_FIELD_TEXT_LEN:
        s = s[:MAX_FIELD_TEXT_LEN] + "... [truncated]"
    return s


def build_issues_payload_for_llm(issues):
    """Build full per-issue blocks (field_title: value) for the LLM."""
    blocks = []
    for i in issues:
        key = (i.get("key") or "").strip()
        fields = i.get("fields") or {}
        lines = [f"---", f"Issue: {key}", ""]
        for entry in JIRA_FIELDS:
            title, fid = entry.get("field_title") or "", entry.get("field_id") or ""
            if not fid:
                continue
            if fid == "key":
                val = key
            else:
                val = fields.get(fid)
            text = _format_field_value(val, fid)
            if text:
                lines.append(f"{title}:")
                lines.append(text)
                lines.append("")
        blocks.append("\n".join(lines))
    return "\n".join(blocks) if blocks else "(no issues)"


def extract_body_text(page):
    for rep in ("view", "storage"):
        body = (page.get("body") or {}).get(rep) or {}
        val = body.get("value") or ""
        if val:
            # crude strip of HTML tags for plain text
            text = re.sub(r"<[^>]+>", " ", val)
            text = re.sub(r"\s+", " ", text).strip()
            return text[:50000]
    return ""


def current_work_week_title():
    """Return page title in format WWww - (MM/DD/YYYY) using Monday of current ISO week for the date."""
    from datetime import date
    d = date.today()
    iso = d.isocalendar()
    year, week, _ = iso
    monday = date.fromisocalendar(year, week, 1)
    mm_dd_yyyy = monday.strftime("%m/%d/%Y")
    return f"WW{week} - ({mm_dd_yyyy})"


def current_work_week_iso():
    """Return (year, week) for today (ISO)."""
    from datetime import date
    d = date.today()
    iso = d.isocalendar()
    return (iso[0], iso[1])


def find_page_for_current_work_week(pages):
    """Return the child page whose title parses to the current work week, or None."""
    current = current_work_week_iso()
    # #region agent log
    _debug_log("find_page current work week", {"current_iso": current, "current_type": str(type(current))}, "C", "recipe.py:291")
    # #endregion
    for p in pages:
        title = (p.get("title") or "").strip()
        parsed = _parse_work_week_from_title(title)
        # #region agent log
        _debug_log("page title check", {"title": title, "title_repr": repr(title), "parsed": parsed, "parsed_type": str(type(parsed)) if parsed else None, "equals_current": parsed == current}, "B,C,E", "recipe.py:298")
        # #endregion
        if parsed == current:
            return p
    # #region agent log
    _debug_log("find_page no match", {"returning": None}, "D", "recipe.py:303")
    # #endregion
    return None


def call_llm(previous_state_text, current_issues_data, first_page=False):
    """current_issues_data: full per-issue blocks with all requested fields (field_title labels)."""
    project = dataiku.api_client().get_default_project()
    llm = project.get_llm(llm_id)
    completion = llm.new_completion()

    table_instruction = """
Use this exact top-level section order (do not change the order):
## Executive Summary
## Change Summary
## Issue Status Summaries

You MUST include a **Change Summary** section with a markdown table. The table is required. Use exactly this header row:
| Issue | Assignee | Status | Change | Comments/Updates |
Then add one row per issue from the issue data above (one row per Jira issue). For **Assignee**, use the ticket assignee from the data.
For **Change**, use only one of these emojis per row (include a short legend above or below the table):
- 📈 increasing/high escalation, still progressing but not near closure
- 📉 decreasing or low priority, progressing towards closure
- ✅ closed/resolved
- ⚠️ stalled, no updates, customer concerned about pace
- 🚨 blocker, escalation, or major issue caught/sighted
In **Comments/Updates**, write a short summary for technical and non-technical readers: what is happening, why, what is next, and if anything is blocking; use plain language.

For **Issue Status Summaries**, determine and include the most significant issues from the full Jira data, including all high priority/exposure tickets (P1/P2 where present), with one line per issue key.
Do not wrap your response in a code block (no ```markdown); output raw markdown.
Use markdown headings for section titles: ## for main sections and ### for sub-sections; do not use **bold** for section titles—use # hashes so they render as proper headings."""
    if first_page:
        user_content = f"""Current state (full Jira issue data from filter):

{current_issues_data}

---

Compose a status report for the wiki page: read and use the full data for each issue above. Keep concise narrative coverage in Executive Summary, then Change Summary table, then line-by-line Issue Status Summaries for the most significant and high priority/exposure issues. Do not compare to a previous report (this is the first page).{table_instruction}"""
    else:
        user_content = f"""Previous week's status report (from last work-week Confluence page):

{previous_state_text or '(none)'}

---

Current state (full Jira issue data from filter):

{current_issues_data}

---

Compose the new wiki page as a status report. Read and use the full current Jira data for each issue. Compare your new report to the previous week's report above and explain how the situation evolved. Keep clear markdown sections in this exact order: Executive Summary, Change Summary, Issue Status Summaries. In Issue Status Summaries, provide one line per issue for the most significant and high priority/exposure tickets (P1/P2 where present). Output in markdown.{table_instruction}"""

    if LLM_SYSTEM_PROMPT:
        full_message = f"System instructions:\n{LLM_SYSTEM_PROMPT}\n\n---\n\nUser task:\n{user_content}"
    else:
        full_message = user_content

    completion.with_message(full_message)
    resp = completion.execute()
    if not getattr(resp, "success", True):
        raise RuntimeError(f"LLM completion failed: {getattr(resp, 'message', resp)}")
    text = (resp.text or "").strip()
    # If LLM wrapped output in ```markdown ... ```, strip so we parse inner content as markdown
    if text.startswith("```"):
        first_nl = text.find("\n")
        if first_nl != -1 and text.rstrip().endswith("```"):
            text = text[first_nl + 1 : text.rstrip().rfind("```")].strip()
    return text


def _parse_table_row(line):
    """Split a markdown table line into cell strings (strip; drop empty first/last from leading/trailing |)."""
    raw = [c.strip() for c in line.strip().split("|")]
    if not raw:
        return []
    if raw[0] == "":
        raw = raw[1:]
    if raw and raw[-1] == "":
        raw = raw[:-1]
    return raw


def _is_table_separator_row(cells):
    """True if every cell looks like :---: or --- (markdown table alignment row)."""
    if not cells:
        return False
    return all(re.match(r"^:?-+:?$", c.strip()) for c in cells)


def _table_block_to_html(table_lines):
    """Convert consecutive markdown table lines to Confluence <table> HTML."""
    rows = [_parse_table_row(ln) for ln in table_lines if ln.strip()]
    if not rows:
        return ""
    ncol = len(rows[0])
    out = ["<table>", "<thead>", "<tr>"]
    for cell in rows[0]:
        out.append("<th>" + html.escape(cell) + "</th>")
    out.append("</tr>")
    out.append("</thead>")
    start = 1
    if len(rows) > 1 and _is_table_separator_row(rows[1]):
        start = 2
    out.append("<tbody>")
    for r in rows[start:]:
        if len(r) != ncol:
            continue
        out.append("<tr>")
        for cell in r:
            out.append("<td>" + html.escape(cell) + "</td>")
        out.append("</tr>")
    out.append("</tbody>")
    out.append("</table>")
    return "\n".join(out)


def markdown_to_simple_html(text):
    """Convert markdown-ish text to minimal Confluence storage HTML. Escapes LLM text so <, >, & are valid XHTML."""
    if not text:
        return "<p></p>"
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    out = []
    in_list = False
    i = 0
    while i < len(lines):
        line = lines[i]
        s = line.strip()
        if not s:
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append("<p></p>")
            i += 1
            continue
        if "|" in s and s.count("|") >= 2:
            table_lines = []
            while i < len(lines) and lines[i].strip() and "|" in lines[i] and lines[i].strip().count("|") >= 2:
                table_lines.append(lines[i])
                i += 1
            if in_list:
                out.append("</ul>")
                in_list = False
            tbl = _table_block_to_html(table_lines)
            if tbl:
                out.append(tbl)
            continue
        esc = html.escape(s)
        if re.match(r"^####\s+", s):
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append("<h4>" + html.escape(re.sub(r"^####\s+", "", s)) + "</h4>")
        elif re.match(r"^###\s+", s):
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append("<h3>" + html.escape(re.sub(r"^###\s+", "", s)) + "</h3>")
        elif re.match(r"^##\s+", s):
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append("<h2>" + html.escape(re.sub(r"^##\s+", "", s)) + "</h2>")
        elif re.match(r"^#\s+", s):
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append("<h1>" + html.escape(re.sub(r"^#\s+", "", s)) + "</h1>")
        elif re.match(r"^[-*]\s+", s) or re.match(r"^\d+\.\s+", s):
            if not in_list:
                out.append("<ul>")
                in_list = True
            li_text = re.sub(r"^[-*]\s+", "", re.sub(r"^\d+\.\s+", "", s))
            out.append("<li>" + html.escape(li_text) + "</li>")
        else:
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append("<p>" + esc + "</p>")
        i += 1
    if in_list:
        out.append("</ul>")
    return "\n".join(out) if out else "<p></p>"


def create_confluence_child(wiki_session, space_key, title, body_html):
    url = f"{WIKI_BASE}/rest/api/content"
    params = {"expand": "body.storage,version,space"}
    try:
        parent_id = int(WIKI_PARENT_ID)
    except (TypeError, ValueError):
        parent_id = WIKI_PARENT_ID
    payload = {
        "type": "page",
        "title": title,
        "space": {"key": space_key},
        "ancestors": [{"id": parent_id}],
        "body": {"storage": {"value": body_html, "representation": "storage"}},
    }
    r = wiki_session.post(url, params=params, json=payload, verify=VERIFY_SSL, timeout=60)
    if not r.ok:
        err_msg = r.text
        try:
            err_body = r.json()
            err_msg = err_body.get("message") or err_body.get("reason") or str(err_body) or r.text
        except Exception:
            pass
        raise RuntimeError(f"Confluence create content failed {r.status_code}: {err_msg}")
    return r.json()


def update_confluence_page(wiki_session, page_id, version_number, title, body_html):
    """PUT update existing page; version_number is current, we send version_number + 1."""
    url = f"{WIKI_BASE}/rest/api/content/{page_id}"
    params = {"expand": "body.storage,version,space"}
    payload = {
        "type": "page",
        "title": title,
        "version": {"number": int(version_number) + 1},
        "body": {"storage": {"value": body_html, "representation": "storage"}},
    }
    r = wiki_session.put(url, params=params, json=payload, verify=VERIFY_SSL, timeout=60)
    r.raise_for_status()
    return r.json()


def build_output_row(created_page, body_storage_fallback=None):
    """One row: page_id, title, body_storage, url, space_key, version_number, created_at, parent_page_id."""
    version = created_page.get("version") or {}
    body = (created_page.get("body") or {}).get("storage") or {}
    body_val = body.get("value") or body_storage_fallback or ""
    links = created_page.get("_links") or {}
    base = links.get("base") or WIKI_BASE
    ctx = links.get("context") or ""
    web_link = (base + ctx + "/pages/viewpage.action?pageId=" + created_page.get("id", "")) if created_page.get("id") else ""
    space = created_page.get("space") or {}
    space_key = space.get("key") or ""

    return {
        "page_id": created_page.get("id"),
        "title": created_page.get("title") or "",
        "body_storage": body_val,
        "url": web_link,
        "space_key": space_key,
        "version_number": version.get("number"),
        "created_at": version.get("when"),
        "parent_page_id": WIKI_PARENT_ID,
    }


# ---------- Run ----------
wiki_session = _wiki_session()

# 1) Jira issues
issues = fetch_jira_issues()
current_issues_data = build_issues_payload_for_llm(issues)

# 2) Confluence parent + children, latest work-week page
parent, space_key, child_pages = get_parent_and_children(wiki_session)
# #region agent log
_debug_log("child_pages after get_parent_and_children", {"count": len(child_pages), "titles": [(p.get("title"), repr((p.get("title") or ""))) for p in child_pages]}, "A", "recipe.py:465")
# #endregion
latest_page = select_latest_work_week_page(child_pages)
previous_state_text = extract_body_text(latest_page) if latest_page else ""
is_first_page = not child_pages

# 3) LLM (status report from full issue data; when previous page exists, compare to it)
report_text = call_llm(
    previous_state_text,
    current_issues_data,
    first_page=is_first_page,
)
body_html = markdown_to_simple_html(report_text)

# 4) Same day / same work week: update existing page; otherwise create new child page
# When the script is rerun on the same day (or any day in the same work week), we only alter
# the existing wiki page for that week instead of creating a duplicate.
new_title = current_work_week_title()
existing_current_week_page = find_page_for_current_work_week(child_pages)
# #region agent log
_debug_log("create vs update branch", {"has_existing_page": existing_current_week_page is not None, "page_id": existing_current_week_page.get("id") if existing_current_week_page else None, "new_title": new_title}, "D", "recipe.py:480")
# #endregion
if existing_current_week_page is not None:
    page_id = existing_current_week_page["id"]
    version = (existing_current_week_page.get("version") or {}).get("number")
    version = int(version) if version is not None else 1
    created = update_confluence_page(wiki_session, page_id, version, new_title, body_html)
else:
    created = create_confluence_child(wiki_session, space_key, new_title, body_html)

# 5) Output dataset (one row = copy of new child page)
row = build_output_row(created, body_storage_fallback=body_html)
df = pd.DataFrame([row])
output_dataset.write_with_schema(df)
