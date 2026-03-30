# Jira Wiki Agenda (Dataiku Custom Recipe)

This custom recipe generates a weekly Confluence status page from Jira issues and writes a one-row output dataset containing the created or updated page details.

## What This Tool Does

- Fetches Jira issues using a configured Jira filter (`filter=<jira_filter_id>`).
- Pulls child pages under a configured Confluence parent page.
- Finds the latest work-week page (based on title patterns like `WW13 - (03/23/2026)` or fallback by page version date).
- Uses a selected Dataiku LLM to generate a markdown status report:
  - If no prior child page exists, it generates a first-week report.
  - If a prior page exists, it compares current Jira state with previous report text.
  - It enforces section order: `Executive Summary` -> `Change Summary` -> `Issue Status Summaries`.
- Converts generated markdown into simple Confluence storage HTML.
- Updates the current work-week page if it already exists; otherwise creates a new child page.
- Writes a one-row dataset copy of the resulting Confluence page.

## End-to-End Flow

1. Read recipe configuration from `recipe.json` parameters.
2. Validate required Jira, Confluence, and LLM settings.
3. Fetch Jira issues with pagination (`/rest/api/2/search`).
4. Build LLM input from selected Jira fields (formatted per issue).
5. Fetch parent Confluence page and all child pages.
6. Determine previous state from the latest work-week page.
7. Generate report text through the configured Dataiku LLM.
8. Enforce section order and issue-summary coverage requirements through prompt constraints.
9. Let the LLM determine the most significant issues from the Jira data (including all `P1/P2` where present).
10. Convert markdown-like output to Confluence storage HTML.
11. Determine page title for current ISO week (`WWww - (MM/DD/YYYY)`).
12. Update existing current-week page or create a new child page.
13. Write output dataset with one row for the resulting page.

## Recipe Parameters

Configured in `recipe.json`:

- `jira_base_url` (required): Jira base URL (no trailing slash).
- `jira_token` (required): Jira API token used in `Authorization: Bearer`.
- `jira_filter_id` (required): Filter ID used as JQL `filter=<id>`.
- `jira_fields` (optional/object list): Jira fields requested from search API.
  - Each row includes:
    - `field_title`: display name shown to the LLM.
    - `field_id`: Jira field ID (for example: `key`, `summary`, `assignee`, `customfield_12345`).
  - If empty, code defaults to:
    - Key (`key`)
    - Summary (`summary`)
    - Description (`description`)
    - Comment (`comment`)
- `wiki_base_url` (required): Confluence base URL (no trailing slash).
- `wiki_username` (required): Username parameter (validated as required).
- `wiki_token` (required): Confluence bearer token.
- `wiki_parent_page_id` (required): Parent page content ID where child pages are managed.
- `llm_selection` (required): Dataiku LLM used for report generation.
- `llm_system_prompt` (optional): Additional prompt instructions; a default prompt is provided in `recipe.json`.

## LLM Output Expectations

The recipe enforces instructions to include a markdown change table with this exact header:

`| Issue | Assignee | Status | Change | Comments/Updates |`

Required section order:

- `## Executive Summary`
- `## Change Summary`
- `## Issue Status Summaries`

Change emojis expected in the `Change` column:

- `✅` closed/resolved
- `📈` increasing or high escalation, still progressing but not near closure
- `📉` decreasing or low priority, progressing toward closure
- `⚠️` stalled, no updates, customer concerned about pace
- `🚨` blocker, escalation, or major issue caught/sighted

Issue status summary coverage:

- `Issue Status Summaries` must include line-by-line entries for the most significant issues determined by the LLM from Jira data.
- The prompt explicitly requires inclusion of all high priority/exposure tickets (`P1/P2` where present).

The recipe strips code fences if the LLM wraps output in triple backticks.

## Confluence Page Behavior

- Current title format is generated as `WWww - (MM/DD/YYYY)` where the date is the Monday of the current ISO week.
- If a child page for the current ISO week already exists, it is updated (version incremented).
- Otherwise, a new child page is created under `wiki_parent_page_id`.

## Output Dataset

The output role `main_output` contains a single-row dataset with:

- `page_id`
- `title`
- `body_storage`
- `url`
- `space_key`
- `version_number`
- `created_at`
- `parent_page_id`

This row is a copy of the Confluence page returned by create/update API calls.

## Field Formatting Notes

To make LLM input readable, the recipe:

- Converts object fields (status, assignee, etc.) into simple display text.
- Derives user-like names from email local-part where available.
- Flattens list fields such as comments and keeps recent entries.
- Truncates very long text values to reduce LLM context pressure.

## Operational Caveats

- SSL verification is disabled in code (`VERIFY_SSL = False`).
- A hardcoded local debug log path is used in `recipe.py` (`DEBUG_LOG`).
- `wiki_username` is required by config validation but Confluence calls use bearer token auth header.
- The markdown-to-HTML converter is intentionally simple; complex markdown constructs may not render exactly as in full markdown engines.

## Troubleshooting

- Missing configuration errors:
  - Ensure all required Jira, Confluence, and LLM parameters are set.
- Jira API failures:
  - Verify `jira_base_url`, token permissions, and `jira_filter_id`.
- Confluence fetch/create/update failures:
  - Verify `wiki_base_url`, token permissions, and `wiki_parent_page_id`.
  - Confirm the parent page exists and is accessible.
- Empty or weak report quality:
  - Review `jira_fields` to ensure key business context is included.
  - Refine `llm_system_prompt` for preferred tone and structure.

## Files

- Recipe code: `custom-recipes/jira-wiki-agenda/recipe.py`
- Recipe definition: `custom-recipes/jira-wiki-agenda/recipe.json`
