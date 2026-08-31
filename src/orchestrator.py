"""
orchestrator.py

Reads today's pipeline queue from data_source_registry_tb in Snowflake
and dispatches each pipeline's GitHub Actions workflow via the gh CLI.

Fire-and-forget: the orchestrator only records that a dispatch was attempted.
Pipeline outcome tracking (success/failure) is the responsibility of each
pipeline repo.

Expected environment variables (set as GitHub Actions secrets):
    SNOWFLAKE_ACCOUNT   - e.g. xy12345.us-east-1
    SNOWFLAKE_USER      - service account username
    SNOWFLAKE_PASSWORD  - service account password
    SNOWFLAKE_WAREHOUSE - compute warehouse name
    SNOWFLAKE_DATABASE  - governance database name
    SNOWFLAKE_SCHEMA    - schema containing the registry table
    ORGANIZATION          - GitHub org or user that owns the pipeline repos
    GH_TOKEN            - Fine-grained PAT with Actions:write on pipeline repos
"""

import os
import subprocess
import logging
import sys
from datetime import date, datetime, timezone
from dataclasses import dataclass

import snowflake.connector

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SNOWFLAKE_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER      = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
SNOWFLAKE_DATABASE  = os.environ["SNOWFLAKE_GOVERNANCE_DATABASE"]
SNOWFLAKE_SCHEMA    = os.environ["SNOWFLAKE_GOVERNANCE_SCHEMA"]
REGISTRY_TABLE      = os.environ["SNOWFLAKE_GOVERNANCE_REGISTRY_TABLE"]
GITHUB_ORG          = os.environ["GH_ORG"]

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class DataSource:
    source_id:     str
    snowflake_table:   str
    repo_name:          str
    schedule_frequency: str

# ---------------------------------------------------------------------------
# Snowflake helpers
# ---------------------------------------------------------------------------

def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    log.info("Connecting to Snowflake...")
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


def fetch_todays_queue(conn: snowflake.connector.SnowflakeConnection) -> list[DataSource]:
    """
    Returns active sources scheduled to run today, ordered by SCHEDULE_PRIORITY.
    """
    query = f"""
        SELECT
            SOURCE_ID,
            SNOWFLAKE_TABLE,
            REPO_NAME,
            SCHEDULE_FREQUENCY
        FROM {REGISTRY_TABLE}
        WHERE IS_ACTIVE = TRUE
          AND REPO_NAME IS NOT NULL
          AND SCHEDULE_FREQUENCY = 'daily'
    """

    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()

    sources = [
        DataSource(
            source_id     = row[0],
            snowflake_table   = row[1],
            repo_name          = row[2],
            schedule_frequency = row[3],
        )
        for row in rows
    ]

    log.info(f"Queue built: {len(sources)} source(s) to process today ({date.today()})")
    return sources


def record_dispatched(
    conn:       snowflake.connector.SnowflakeConnection,
    source_ids: list[str],
) -> None:
    """
    Writes a LAST_DISPATCHED_AT timestamp for all successfully dispatched sources
    in a single batch update.
    """
    if not source_ids:
        return

    placeholders = ", ".join(["%s"] * len(source_ids))
    cursor = conn.cursor()
    cursor.execute(
        f"""
        UPDATE {REGISTRY_TABLE}
        SET
            LAST_DISPATCHED_AT = CURRENT_TIMESTAMP(),
            UPDATED_AT         = CURRENT_TIMESTAMP()
        WHERE DATA_SOURCE_ID IN ({placeholders})
        """,
        source_ids,
    )
    cursor.close()
    conn.commit()

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

def dispatch(source: DataSource, run_date: str) -> bool:
    """
    Fires a workflow_dispatch event on the pipeline repo via the gh CLI.
    Returns True on success, False on failure.
    The gh CLI uses GH_TOKEN from the environment automatically.
    """
    log.info(
        f"Dispatching  [{source.source_id}] {source.snowflake_table}  "
        f"repo={source.repo_name}"
    )

    result = subprocess.run(
        [
            "gh", "workflow", "run", "main.yml",
            "--repo",  f"{GITHUB_ORG}/{source.repo_name}",
            "--ref",   "main",
        ],
        capture_output=True,
        text=True,
        env={**os.environ, "GH_TOKEN": os.environ["GH_TOKEN"]},
    )
        
    if result.returncode == 0:
        log.info(f"  OK    [{source.source_id}] {source.snowflake_table}")
        return True

    log.error(
        f"  FAIL  [{source.source_id}] {source.snowflake_table}  "
        f"stderr={result.stderr.strip()}"
    )
    return False

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    run_date = str(date.today())

    log.info("=" * 60)
    log.info(f"Orchestrator starting  run_date={run_date}")
    log.info("=" * 60)

    conn = get_snowflake_connection()

    try:
        queue = fetch_todays_queue(conn)

        if not queue:
            log.info("Nothing scheduled for today. Exiting.")
            return

        dispatched_ids = []
        failed         = []

        for source in queue:
            if dispatch(source, run_date):
                dispatched_ids.append(source.source_id)
            else:
                failed.append(source)

        #record_dispatched(conn, dispatched_ids)

        # Summary
        log.info("=" * 60)
        log.info(f"Run complete  dispatched={len(dispatched_ids)}  failed={len(failed)}")

        if failed:
            log.error("Dispatch failures — manual intervention or re-run required:")
            for s in failed:
                log.error(f"  [{s.source_id}] {s.snowflake_table}")

        log.info("=" * 60)

        # Exit non-zero so GitHub Actions marks the run as failed and triggers
        # failure notifications if any dispatches did not go out.
        if failed:
            sys.exit(1)

    finally:
        try:
            conn.close()
        except Exception:
            pass
        log.info("Snowflake connection closed.")


if __name__ == "__main__":
    main()