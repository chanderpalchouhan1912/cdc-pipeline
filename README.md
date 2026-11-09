# PostgreSQL CDC to Azure Event Hubs

Captures every INSERT, UPDATE, and DELETE on a PostgreSQL `customers` table and publishes each change as a JSON event to Azure Event Hubs, in commit order, with at-least-once delivery.

## 1\. Architecture

```
Azure Database for PostgreSQL (Flexible Server, wal\_level=logical)
   │  WAL records every committed change
   ▼
Logical replication slot "customers\_cdc\_slot" (wal2json plugin)
   │  Changes decoded to JSON (format-version 2, one object per change)
   ▼
Python listener (listener/cdc\_listener.py, psycopg2)
   │  Builds an event → publishes → only then acknowledges the LSN
   ▼
Azure Event Hubs, Basic tier, hub "customer-changes", partition key = customer\_id
   │
   ▼
verification\_notebook.ipynb (reads events, rebuilds final state, compares with the table)
```

## 2\. Architectural evaluation

Four approaches were considered for capturing row-level changes from PostgreSQL.

|Approach|Latency|Operational complexity|Cost|DELETE handling|Decision|
|-|-|-|-|-|-|
|**Custom Python listener** (psycopg2 logical replication + wal2json)|Seconds|Medium: we own offsets, retries, reconnects|Lowest: one small process, no extra services|Captured from the WAL with the old row (REPLICA IDENTITY FULL)|**Selected**|
|**Debezium + Kafka Connect**|Seconds|High: JVM, Kafka Connect, offset topics, connector management|Medium to high: needs a Kafka cluster or managed equivalent|Captured; emits a delete event and optionally a tombstone for compacted topics|Rejected for this assignment: needs Docker or a managed Kafka, which is not practical to set up and debug in the time available. **Recommended for production.**|
|**Azure Data Factory CDC**|Minutes|Low: managed, configured in the UI|Pay per pipeline run and activity|Limited; depends on the connector's change tracking|Rejected: hides offset and checkpoint handling, so the mechanism cannot be shown or explained.|
|**Spark Structured Streaming with JDBC polling**|Minutes|Medium: needs a Spark cluster and a watermark column|Medium: Spark compute runs continuously|Hard deletes are missed, because polling only sees rows that still exist|Rejected: fails the "no data loss on deletes" requirement.|

### Why the custom listener

* **Correctness:** reads the WAL, so every committed INSERT, UPDATE, and DELETE is seen in order. Polling cannot see hard deletes.
* **Transparency:** offsets, acknowledgement, retries, and reconnects are visible in code, which is what the assessment asks for.
* **Cost:** no extra compute or Kafka cluster. The only paid resources are a Burstable PostgreSQL server and an Event Hubs Basic namespace.
* **Scope:** one table and one consumer, where a full connector framework adds more operational weight than value.

## 3\. Delivery and correctness

### Offsets and checkpointing

* The replication slot stores PostgreSQL's position for our consumer (confirmed flush LSN). PostgreSQL keeps WAL until that position advances.
* The listener calls `send\_feedback(flush\_lsn=...)` **only after** Event Hubs accepts the event. This is the checkpoint.
* A crash between publishing and acknowledging causes the change to be read again on restart. This gives **at-least-once** delivery: duplicates are possible, losses are not.

### Duplicates

* Each event carries its `lsn`, so consumers can de-duplicate on `(customer\_id, lsn)`.

### Ordering

* The partition key is `customer\_id`. All changes for one customer go to the same partition, in commit order.

### Retries and recovery

* Publishing retries up to 5 times with exponential backoff. If all retries fail, the LSN is not acknowledged, and the listener restarts from the last acknowledged position.
* A lost database connection is handled by reconnecting with backoff (5 s, doubling, capped at 60 s). This was observed during testing, when the server closed the connection and the listener reconnected on its own.

### Serialization

Each event is UTF-8 JSON:

```json
{
  "op": "U",
  "operation": "UPDATE",
  "table": "customers",
  "lsn": "922758056",
  "customer\_id": 1,
  "before": {"customer\_id": 1, "full\_name": "Asha Rao", "email": "asha.rao@example.com", "status": "ACTIVE", "updated\_at": "..."},
  "after":  {"customer\_id": 1, "full\_name": "Asha Rao", "email": "asha.rao@example.com", "status": "INACTIVE", "updated\_at": "..."},
  "published\_at": "2026-11-08T13:30:00+00:00"
}
```

* `op` is `I`, `U`, or `D`. `before` is set for UPDATE and DELETE. `after` is `null` for DELETE.

## 4\. DELETE handling and tombstones

* A **hard delete** is published as an event with `"op": "D"`. Its `before` field contains the deleted row, so the consumer knows which customer was removed.
* A **soft delete** (setting `status` to a deleted value) arrives as an UPDATE event like any other change.
* We do **not** publish Kafka-style null tombstones. Tombstones exist so that a compacted topic can drop a key. Event Hubs does not compact topics, so a delete is expressed as an explicit `D` event instead. A consumer that keeps the latest state per key removes the row when it sees `D`.
* `REPLICA IDENTITY FULL` on the table is what makes the old row available for UPDATE and DELETE events.

## 5\. Repository structure

```
cdc-pipeline/
├── README.md                          this file
├── requirements.txt                   Python dependencies
├── .env.example                       settings template (no secrets)
├── .gitignore                         excludes .env, .venv, caches
├── config/
│   ├── check\_setup.py                 verifies installed packages
│   └── run\_sql.py                     runs a SQL file using .env settings
├── sql/
│   ├── 00\_grant\_replication.sql       grants the admin role replication rights
│   ├── 01\_setup.sql                   table, REPLICA IDENTITY FULL, updated\_at trigger, publication
│   ├── 03\_create\_slot.sql             creates the wal2json replication slot
│   ├── 04\_demo\_changes.sql            sample INSERT, UPDATE, DELETE
│   └── 99\_teardown.sql                removes everything created by the setup
├── listener/
│   └── cdc\_listener.py                CDC listener: decode, publish, acknowledge
├── notebooks/
│   └── verification\_notebook.ipynb    baseline, changes, event read, final-state comparison
└── docs/
    ├── eventhub\_data\_explorer\_customers.png   evidence of published events
    └── ...
```

## 6\. Setup and run order

### Prerequisites

* Python 3.10 or newer, and Git
* An Azure subscription with rights to create resources
* Windows PowerShell commands are shown; the same steps work on macOS or Linux with equivalent commands

### Step 1: Install Python packages

```
python -m venv .venv
.\\.venv\\Scripts\\Activate.ps1
pip install -r requirements.txt
```

### Step 2: Create Azure resources (same region for both)

1. **PostgreSQL Flexible Server:** Burstable B1ms, PostgreSQL 16, 32 GB storage, public access with a firewall rule for your IP.
2. **Server parameters** (equivalent of `postgresql.conf`), then restart the server:

|Parameter|Value|Purpose|
|-|-|-|
|`wal\_level`|`logical`|Writes row-level changes to the WAL|
|`max\_replication\_slots`|`10`|Allows replication slots|
|`max\_wal\_senders`|`10`|Allows replication connections|
|`azure.extensions`|`wal2json`|Makes the wal2json decoder available|

3. **Event Hubs namespace:** Basic tier, with a hub named `customer-changes`.

### Step 3: Configure settings

Copy `.env.example` to `.env` and fill in the PostgreSQL host, user, password, and the Event Hubs connection string. Never commit `.env`.

### Step 4: Create the database objects

```
python config\\run\_sql.py sql\\00\_grant\_replication.sql   # only if the admin role lacks REPLICATION
python config\\run\_sql.py sql\\01\_setup.sql
python config\\run\_sql.py sql\\03\_create\_slot.sql
```

`03\_create\_slot.sql` runs on its own because PostgreSQL cannot create a replication slot inside a transaction that has written data.

### Step 5: Start the listener

```
python listener\\cdc\_listener.py
```

Run only one listener at a time. A second listener cannot use the slot, and will retry until the first one stops.

### Step 6: Generate changes and verify

```
python config\\run\_sql.py sql\\04\_demo\_changes.sql
```

The listener prints one line per change, ending in `published`. Open the notebook for the full verification.

### Step 7: Clean up

Stop the listener, then run:

```
python config\\run\_sql.py sql\\99\_teardown.sql
```

Delete the resource group in Azure to stop all billing.

## 7\. Verification results

* **Listener logs:** INSERT, UPDATE, and DELETE events for each test customer are logged as `published`, with their LSNs.
* **Azure metrics:** the namespace's Incoming Messages chart shows the published events.
* **Data Explorer:** docs/eventhub\_data\_explorer\_customers.png` shows the same events in the Event Hubs portal, as an independent check.
* **Notebook (Azure Databricks):** notebooks/verification\_notebook.ipynb` runs on Databricks serverless compute. It makes five changes in PostgreSQL, reads the events back from Event Hubs (5 of 5 found), rebuilds the final state from them, and compares it with the table. The result is MATCH. The outputs are saved in the notebook, and the screenshot is `docs/databricks\_verification\_match.png`.

## 8\. Known limitations and production improvements

* **Single listener and single table:** one process handles one slot. Scaling would mean one slot per table group, or a managed tool such as Debezium.
* **Batching:** each event is sent in its own Event Hubs batch. Batching would improve throughput.
* **Schema changes:** events carry no schema version. Production would add a version field and a schema registry.
* **Replication slot risk:** if the listener stops for a long time, WAL accumulates on the server. Monitor slot lag from `pg\_replication\_slots` and alert on it.
* **Secrets:** credentials come from `.env` for this project. Production would use Azure Key Vault and managed identity.
* **Delta Lake sink:** not implemented. Spark Structured Streaming into Delta on ADLS Gen2 is the production target for the downstream store.
* **Notebook dependency:** the notebook needs network access to Event Hubs over WebSocket on port 443 for its read step.
* **Notebook read timeout:** the Event Hubs receiver runs for a fixed 60-second window, then stops. This is intentional, so the notebook always finishes. A production consumer would run continuously and checkpoint its position.

