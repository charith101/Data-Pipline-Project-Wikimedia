import os
import sys
import logging
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

WIKI_PROJECT = "en.wikipedia"
ACCESS = "all-access"
HEADERS = {"User-Agent": "data-pipeline-project/1.0 (contact: wijesinghecharith33@gmail.com)"}


def fetch_top_pageviews() -> tuple[list[dict], datetime]:
    """Fetch top pageviews, falling back up to 7 days if data isn't ready yet."""
    for days_back in range(2, 8):
        date = datetime.now(timezone.utc) - timedelta(days=days_back)
        year, month, day = date.strftime("%Y"), date.strftime("%m"), date.strftime("%d")
        url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/{WIKI_PROJECT}/{ACCESS}/{year}/{month}/{day}"

        response = requests.get(url, headers=HEADERS, timeout=30)
        if response.status_code == 200:
            logger.info(f"Found data for {date.strftime('%Y-%m-%d')} (tried {days_back} days back)")
            return response.json()["items"][0]["articles"], date

        logger.warning(f"No data yet for {date.strftime('%Y-%m-%d')} (status {response.status_code}), trying earlier date...")

    raise RuntimeError("No pageview data available for the past 7 days — check Wikimedia API status")


def transform(articles: list[dict], date: datetime) -> list[dict]:
    """Reshape raw API records into the rows we want in BigQuery."""
    loaded_at = datetime.now(timezone.utc).isoformat()
    rows = []

    for article in articles:
        row = {
            "article": article["article"],
            "views": article["views"],
            "rank": article["rank"],
            "pageview_date": date.strftime("%Y-%m-%d"),
            "loaded_at": loaded_at,
        }
        rows.append(row)

    return rows


def load_to_bigquery(rows: list[dict], project_id: str) -> None:
    """Batch-load rows into raw_pipeline.wiki_top_pageviews (sandbox-safe — no streaming)."""
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.raw_pipeline.wiki_top_pageviews"

    schema = [
        bigquery.SchemaField("article", "STRING"),
        bigquery.SchemaField("views", "INTEGER"),
        bigquery.SchemaField("rank", "INTEGER"),
        bigquery.SchemaField("pageview_date", "DATE"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()
    logger.info(f"Loaded {len(rows)} rows into {table_id}")


def main():
    project_id = os.environ["GCP_PROJECT_ID"]
    articles, run_date = fetch_top_pageviews()
    logger.info(f"Processing {len(articles)} articles for {run_date.strftime('%Y-%m-%d')}")
    rows = transform(articles, run_date)
    load_to_bigquery(rows, project_id)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Pipeline run failed")
        sys.exit(1)