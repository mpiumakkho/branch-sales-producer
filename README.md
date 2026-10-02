# branch-sales-producer

Branch side of Branch Daily Sales Sync. One instance runs at every branch. It reads the days the branch manager has confirmed in the branch back-office database and sends them to the HQ Kafka topic `branch-sales.daily-summary`.

The message format is the contract in the HQ consumer repo ([branch-sales-consumer/contract](https://github.com/mpiumakkho/branch-sales-consumer/tree/main/contract)). The two sides share no code; [`contract/`](contract/) holds a copy of the schema that the tests check every message against.

| Folder | Content |
|---|---|
| `src/` | The producer (Java 25, Spring Boot 4) |
| [`backoffice/`](backoffice/) | Simulated back-office tables, for tests and the demo |
| [`contract/`](contract/) | Copy of the contract schema, test only |

## Branch database

| Table | Owner | Producer access |
|---|---|---|
| `daily_sales` | back-office system | read: `sale_date`, `branch_code`, `status` (`DRAFT`/`CONFIRMED`), `revision`, `confirmed_at` |
| `daily_sales_line` | back-office system | read: `sale_date`, local `category_code`, `amount`, `quantity` |
| `sync_log` | producer (Flyway) | read and write: send status per `(sale_date, revision)` |

In a real branch the back-office tables already exist. Here they come from [`backoffice/postgresql/schema.sql`](backoffice/postgresql/schema.sql), and confirmation is simulated with SQL:

```sql
update daily_sales set status = 'CONFIRMED', revision = revision + 1, confirmed_at = now() where sale_date = '2026-10-01';
```

Flyway only creates `sync_log`, with its own history table `branch_sales_flyway_history`, and baselines the existing schema at version 0. Migrations are per database vendor (`db/migration/{vendor}`); MySQL and SQL Server come in step 7. The SQL in the code uses no vendor-specific statements for the same reason.

## Send rounds

```
cron (every hour, Asia/Bangkok) ──► random delay 0–30 min ──► round:
   read pending days (CONFIRMED, no SENT row in sync_log for that revision), oldest first
   for each day: map categories → write message → send, wait for ack (acks=all) → sync_log SENT
```

| Situation | Result |
|---|---|
| Broker acknowledged | `SENT`, `attempts + 1`, `event_id`, `sent_at` |
| Data cannot form a valid message (unmapped category, negative amount, no lines, ...) | `FAILED` with `last_error`; next day in the round continues; tried again every round |
| Kafka not reachable | `FAILED` with `last_error`; round stops; remaining days stay pending for the next round |
| Producer stops after the ack, before writing `SENT` | revision is sent again next round; HQ skips it as a duplicate (rule R5) |
| Manager edits and re-confirms | new revision is pending and sent; the old revision's `SENT` row stays |

Days and lines are read in one repeatable-read transaction, so the lines always belong to the revision that was read. Every day has key `branchCode`, so all messages of a branch go to one partition and HQ reads them in order.

## Category mapping

Each back-office uses its own category codes. They are mapped to the HQ codes ([categories.md](https://github.com/mpiumakkho/branch-sales-consumer/blob/main/contract/categories.md)) in configuration. Several local codes may map to one HQ code; their amounts and quantities are added up into one line.

```yaml
branch-sales:
  category-mapping:
    BEV: BEVERAGE
    DRINK_HOT: BEVERAGE
    SNK: SNACK
```

## Configuration

| Variable | Default | |
|---|---|---|
| `BRANCH_DB_URL` | `jdbc:postgresql://localhost:5434/branch` | branch back-office database |
| `BRANCH_DB_USER` | `branch_app` | |
| `BRANCH_DB_PASSWORD` | none | required |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | HQ Kafka; from the `branch-sales-wan` network use `kafka.hq.example:9094` |
| `SEND_CRON` | `0 0 * * * *` | round start times (Spring cron, Asia/Bangkok); `-` disables rounds |
| `SEND_MAX_JITTER` | `30m` | upper bound of the random delay before each round |

Kafka producer: `acks=all`, idempotence on, a send fails after about 30 s if HQ is not reachable.

## Test

```bash
./mvnw test
```

Needs JDK 25 and Docker. Tests start their own PostgreSQL (with the back-office tables) and Kafka containers.

| Test | Checks |
|---|---|
| `SummaryMessageWriterTest` | messages are valid against the contract schema; money format, Bangkok time with seconds, category mapping and merging, data rejected before sending |
| `SendRoundTest` | only confirmed revisions are sent, once; re-confirmation sends the new revision; `sync_log` contents; a bad day does not block other days; Kafka failure (simulated and with the broker paused) stops the round and the next round sends everything in date order |
| `SendSchedulerTest` | random delay stays between 0 and the maximum |
