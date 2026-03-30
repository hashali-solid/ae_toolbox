# Jira Weekly Report - User Guide (Non-Technical)

This guide explains how to use the **Jira Wiki Agenda** tool in simple terms.

## What this tool does

This tool creates (or updates) your weekly Confluence report automatically by:

- reading Jira tickets from a saved Jira filter
- summarizing progress with AI
- creating a weekly Confluence page in the right parent location
- adding a clear change table with status emojis

You do not need to write the weekly report manually.

## What you need before running it

Please make sure you have:

- a Jira filter ID that contains the tickets you want in the report
- Jira URL and Jira API token
- Confluence URL, parent page ID, and Confluence token
- an LLM selected in the recipe settings

If any of these are missing, the tool will stop and show an error.

## How to run it (step by step)

1. Open the **Jira wiki agenda** recipe in Dataiku.
2. Fill in the required settings:
   - Jira base URL
   - Jira token
   - Jira filter ID
   - Wiki base URL
   - Wiki username
   - Wiki token
   - Wiki parent page ID
   - LLM selection
3. (Optional) Adjust Jira fields if you want more/less ticket detail.
4. Run the recipe.
5. Open the output dataset (`main_output`) and use the `url` column to open the Confluence page.

## What the generated report looks like

The report is produced in this order:

1. **Executive Summary**
2. **Change Summary**
3. **Issue Status Summaries**

In **Change Summary**, each ticket row includes a `Change` emoji:

- `✅` closed/resolved
- `📈` increasing or high escalation (still progressing, not near closure)
- `📉` decreasing or lower priority (progressing toward closure)
- `⚠️` stalled/no updates/customer concerned about pace
- `🚨` blocker/escalation/major issue

## How ticket summaries are chosen

The AI reviews all Jira ticket data and decides which issues are most significant.

It is instructed to include all high priority/exposure tickets (P1/P2 where available), and to present issue summaries line-by-line for the important items.

## Create vs update behavior

The tool uses weekly titles like `WW13 - (03/23/2026)`.

- If this week already has a page under the parent, it updates that page.
- If not, it creates a new child page for this week.

So rerunning during the same week updates the same page instead of creating duplicates.

## Output you can use

The output dataset contains one row with useful details:

- page ID
- page title
- page body (storage format)
- page URL
- space key
- version number
- created/updated timestamp

Use the `url` field to open and share the page.

## Common issues and quick fixes

- **"Missing Jira configuration"**
  - Check Jira URL, token, and filter ID.

- **"Missing Wiki configuration"**
  - Check Confluence URL, username, token, and parent page ID.

- **No page created / update failed**
  - Confirm your token has permission to read and create/update pages in that Confluence space.

- **Report quality is not as expected**
  - Include better Jira fields (for example: status, assignee, priority, summary, comments).
  - Refine the optional LLM system prompt.

## Best practices

- Keep your Jira filter focused on current, relevant work.
- Include fields that give clear context (status, owner, priority, summary, comments).
- Run once per week at a consistent time (for example, before weekly sync).
- Review the generated page quickly before sharing broadly.
