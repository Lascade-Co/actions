"""Fetch crash reports from Firebase Crashlytics via BigQuery.

Queries the Crashlytics BigQuery export for the latest app release,
excluding native (NDK) crashes, and outputs JSON with each crash
issue's title, subtitle, stack frames, affected session count, and
a direct link to the Crashlytics console.

Usage:
    python scripts/crashlytics_report.py --project travelanimator-c8542 --app com.travelanimator.routemap
    python scripts/crashlytics_report.py --project travelanimator-c8542 --app com.travelanimator.routemap --limit 10
"""

import argparse
import json
import shutil
import subprocess
import sys

CRASHLYTICS_URL = (
    "https://console.firebase.google.com/u/0/project/{project}"
    "/crashlytics/app/android:{package}/issues/{issue_id}"
)

QUERY_TEMPLATE = """\
WITH latest_version AS (
  SELECT
    application.display_version AS display_ver,
    application.build_version AS build_ver
  FROM `{project}`.`firebase_crashlytics`.`{table}`
  WHERE event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  ORDER BY SAFE_CAST(application.build_version AS INT64) DESC
  LIMIT 1
),
crashes AS (
  SELECT *
  FROM `{project}`.`firebase_crashlytics`.`{table}`
  WHERE application.display_version = (SELECT display_ver FROM latest_version)
    AND ARRAY_LENGTH(exceptions) > 0
),
counts AS (
  SELECT
    issue_id,
    issue_title,
    issue_subtitle,
    error_type,
    COUNT(DISTINCT firebase_session_id) AS affected_sessions
  FROM crashes
  GROUP BY issue_id, issue_title, issue_subtitle, error_type
),
representative AS (
  SELECT
    issue_id,
    (SELECT ARRAY_AGG(STRUCT(f.file AS file, f.line AS line) ORDER BY f_idx LIMIT 10)
     FROM UNNEST(exceptions) e WITH OFFSET e_idx,
          UNNEST(e.frames) f WITH OFFSET f_idx
     WHERE e_idx = 0
    ) AS frames,
    ROW_NUMBER() OVER (PARTITION BY issue_id ORDER BY event_timestamp DESC) AS rn
  FROM crashes
)
SELECT
  (SELECT display_ver FROM latest_version) AS version,
  (SELECT build_ver FROM latest_version) AS build,
  c.issue_id,
  c.error_type,
  c.issue_title,
  c.issue_subtitle,
  c.affected_sessions,
  r.frames
FROM counts c
JOIN representative r ON c.issue_id = r.issue_id AND r.rn = 1
ORDER BY c.affected_sessions DESC
LIMIT {limit}
"""


def run_query(query, project):
    result = subprocess.run(
        [
            "bq", "query",
            "--project_id", project,
            "--format=json",
            "--use_legacy_sql=false",
            "--max_rows", "1000",
            query,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        sys.exit(1)

    stdout = result.stdout.strip()
    if not stdout or stdout == "[]":
        return []
    return json.loads(stdout)


def enrich_rows(rows, project, package):
    """Add crashlytics_url and normalize frames from BQ JSON."""
    for row in rows:
        issue_id = row.get("issue_id", "")
        if issue_id:
            row["crashlytics_url"] = CRASHLYTICS_URL.format(
                project=project, package=package, issue_id=issue_id,
            )

        frames_raw = row.get("frames")
        if isinstance(frames_raw, str):
            try:
                row["frames"] = json.loads(frames_raw)
            except (json.JSONDecodeError, TypeError):
                row["frames"] = []
        elif not isinstance(frames_raw, list):
            row["frames"] = []

    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Fetch Crashlytics crash reports from BigQuery"
    )
    parser.add_argument(
        "--project", required=True,
        help="GCP / Firebase project ID (e.g. travelanimator-c8542)",
    )
    parser.add_argument(
        "--app", required=True,
        help="Android package name (e.g. com.travelanimator.routemap)",
    )
    parser.add_argument(
        "--platform", default="ANDROID", choices=["ANDROID", "IOS"],
        help="Platform suffix for the BQ table (default: ANDROID)",
    )
    parser.add_argument(
        "--limit", type=int, default=25,
        help="Max number of crash issues to return (default: 25)",
    )
    args = parser.parse_args()

    if not shutil.which("bq"):
        print("Error: 'bq' CLI not found. Install the Google Cloud SDK.", file=sys.stderr)
        sys.exit(1)

    table = args.app.replace(".", "_") + "_" + args.platform
    query = QUERY_TEMPLATE.format(
        project=args.project, table=table, limit=args.limit,
    )
    rows = run_query(query, args.project)
    rows = enrich_rows(rows, args.project, args.app)
    print(json.dumps(rows, indent=2))


if __name__ == "__main__":
    main()