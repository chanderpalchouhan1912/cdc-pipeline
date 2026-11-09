"""
cdc_listener.py

Reads row changes from PostgreSQL through logical replication (wal2json)
and publishes each change as a JSON event to Azure Event Hubs.

Delivery guarantee: AT-LEAST-ONCE.
The slot position (LSN) is acknowledged only AFTER Event Hubs has accepted
the event. If the process crashes before that, the change is read again on
restart. Duplicates are possible; losses are not.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import LogicalReplicationConnection
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration: read from the .env file or environment variables.
# Secrets are never written in this file.
# ---------------------------------------------------------------------------
load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def require_env(name):
    """Stop immediately with a clear message if a required setting is missing."""
    value = os.getenv(name)
    if not value:
        sys.exit(f"Missing required environment variable: {name}")
    return value


PG_CONFIG = {
    "host": require_env("PGHOST"),
    "port": os.getenv("PGPORT", "5432"),
    "dbname": require_env("PGDATABASE"),
    "user": require_env("PGUSER"),
    "password": require_env("PGPASSWORD"),
    "sslmode": os.getenv("PGSSLMODE", "require"),
}
SLOT_NAME = os.getenv("CDC_SLOT_NAME", "customers_cdc_slot")
TABLE_NAME = os.getenv("CDC_TABLE_NAME", "customers")
EVENTHUB_CONN_STR = require_env("EVENTHUB_CONNECTION_STRING")
EVENTHUB_NAME = require_env("EVENTHUB_NAME")

MAX_SEND_ATTEMPTS = 5          # retries per event before giving up
MAX_BACKOFF_SECONDS = 60       # upper limit for reconnect/retry waits
KEEPALIVE_SECONDS = 10         # how often we tell PostgreSQL we are alive

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("cdc")

# wal2json action codes -> readable operation names
OP_MAP = {"I": "INSERT", "U": "UPDATE", "D": "DELETE"}


# ---------------------------------------------------------------------------
# Event building: turn one wal2json change into one JSON event.
# ---------------------------------------------------------------------------
def columns_to_dict(columns):
    """
    wal2json format-version 2 returns columns as a list of
    {"name": ..., "type": ..., "value": ...}. Convert it to a plain dict.
    """
    return {c["name"]: c.get("value") for c in (columns or [])}


def build_event(change, lsn):
    """
    Build the event published to Event Hubs.

    - INSERT: 'after' holds the new row.
    - UPDATE: 'before' holds the old row (needs REPLICA IDENTITY FULL), 'after' the new row.
    - DELETE: 'before' holds the deleted row. 'after' is None.
    """
    action = change["action"]
    before = columns_to_dict(change.get("identity"))
    after = None if action == "D" else columns_to_dict(change.get("columns"))

    row = after if after is not None else before
    return {
        "op": action,
        "operation": OP_MAP[action],
        "table": change.get("table", TABLE_NAME),
        "lsn": str(lsn),                      # position in the WAL, useful for de-duplication
        "customer_id": row.get("customer_id"),
        "before": before if action in ("U", "D") else None,
        "after": after,
        "published_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Publisher: sends events to Event Hubs with retries.
# ---------------------------------------------------------------------------
class EventHubPublisher:
    def __init__(self):
        self.producer = EventHubProducerClient.from_connection_string(
            EVENTHUB_CONN_STR, eventhub_name=EVENTHUB_NAME
        )

    def publish(self, event):
        """
        Send one event. Returns only when Event Hubs has accepted it.
        Raises RuntimeError after all retries fail, so the caller does NOT
        acknowledge the LSN.
        """
        payload = json.dumps(event, default=str)
        for attempt in range(1, MAX_SEND_ATTEMPTS + 1):
            try:
                # Same customer_id -> same partition -> events stay in order.
                batch = self.producer.create_batch(partition_key=str(event["customer_id"]))
                batch.add(EventData(payload))
                self.producer.send_batch(batch)
                return
            except Exception as exc:
                wait = min(2 ** attempt, MAX_BACKOFF_SECONDS)
                log.warning("Publish attempt %d failed: %s. Retrying in %ss", attempt, exc, wait)
                time.sleep(wait)
        raise RuntimeError("Publish failed after all retries; LSN will not be acknowledged")

    def close(self):
        self.producer.close()


# ---------------------------------------------------------------------------
# Replication consumer.
# ---------------------------------------------------------------------------
def run_stream(publisher):
    """
    Opens a replication connection and processes changes until an error occurs.
    On error, the caller reconnects. PostgreSQL resumes from the last
    acknowledged LSN, so no change is lost.
    """
    conn = psycopg2.connect(connection_factory=LogicalReplicationConnection, **PG_CONFIG)
    try:
        cur = conn.cursor()
        cur.start_replication(
            slot_name=SLOT_NAME,
            decode=True,
            options={
                "format-version": "2",          # one JSON object per change
                "include-transaction": "0",     # skip BEGIN/COMMIT messages
                "add-tables": f"public.{TABLE_NAME}",
            },
        )
        log.info("Streaming changes for table '%s' from slot '%s'", TABLE_NAME, SLOT_NAME)

        def handle(msg):
            change = json.loads(msg.payload)

            # Ignore anything that is not a row change (for example, truncate).
            # Still acknowledge it, so the slot can move past it.
            if change.get("action") not in OP_MAP:
                msg.cursor.send_feedback(flush_lsn=msg.data_start)
                return

            event = build_event(change, msg.data_start)
            publisher.publish(event)                     # 1. deliver first
            msg.cursor.send_feedback(flush_lsn=msg.data_start)  # 2. only then acknowledge

            log.info(
                "%s customer_id=%s lsn=%s published",
                event["operation"], event["customer_id"], event["lsn"],
            )

        # keepalive_interval keeps the connection open while no changes arrive.
        cur.consume_stream(handle, keepalive_interval=KEEPALIVE_SECONDS)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main loop: reconnect on failure, stop cleanly on Ctrl+C.
# ---------------------------------------------------------------------------
def main():
    publisher = EventHubPublisher()
    backoff = 5
    try:
        while True:
            try:
                run_stream(publisher)
                backoff = 5                       # a clean run resets the backoff
            except psycopg2.Error as exc:
                log.error("Database replication error: %s. Reconnecting in %ss", exc, backoff)
            except RuntimeError as exc:
                log.error("%s. Restarting from last acknowledged position in %ss", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
    except KeyboardInterrupt:
        log.info("Stopped by user. Last acknowledged position is kept in the slot.")
    finally:
        publisher.close()


if __name__ == "__main__":
    main()