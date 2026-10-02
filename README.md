# branch-sales-producer

Branch side of Branch Daily Sales Sync. One instance runs at every branch. It reads the days the branch manager has confirmed in the branch back-office database and sends them to its own HQ Kafka topic `branch-sales.daily-summary.<branchCode>`, logged in with its own Kafka user over TLS.

The message format is the contract in the HQ consumer repo ([branch-sales-consumer/contract](https://github.com/mpiumakkho/branch-sales-consumer/tree/main/contract)). The two sides share no code; [`contract/`](contract/) holds a copy of the schema that the tests check every message against.

| Folder | Content |
|---|---|
| `src/` | The producer (Java 25, Spring Boot 4) |
| [`backoffice/`](backoffice/) | Simulated back-office tables, for tests and the demo |
| [`contract/`](contract/) | Copy of the contract schema, test only |
| [`demo/`](demo/) | Two demo branches (configuration, simulated manager actions) for the end-to-end demo |

## Branch database

| Table | Owner | Producer access |
|---|---|---|
| `daily_sales` | back-office system | read: `sale_date`, `branch_code`, `status` (`DRAFT`/`CONFIRMED`), `revision`, `confirmed_at` |
| `daily_sales_line` | back-office system | read: `sale_date`, local `category_code`, `amount`, `quantity` |
| `sync_log` | producer (Flyway) | read and write: current send status per `(sale_date, revision)` |
| `sync_attempt` | producer (Flyway) | insert only: one row per send attempt (time, result, error, `event_id`) |

In a real branch the back-office tables already exist. Here they come from [`backoffice/postgresql/schema.sql`](backoffice/postgresql/schema.sql), and confirmation is simulated with SQL:

```sql
update daily_sales set status = 'CONFIRMED', revision = revision + 1, confirmed_at = now() where sale_date = '2026-10-01';
```

Flyway only creates `sync_log` and `sync_attempt`, with its own history table `branch_sales_flyway_history`, and baselines the existing schema at version 0. Migrations are per database vendor (`db/migration/{vendor}`); MySQL and SQL Server come in step 7. The SQL in the code uses no vendor-specific statements for the same reason.

## Send rounds

```
cron (every hour, Asia/Bangkok) ──► random delay 0–30 min ──► round:
   read pending days (CONFIRMED, no SENT row in sync_log for that revision), oldest first
   for each day: map categories → write message → send, wait for ack (acks=all) → sync_log SENT
```

| Situation | Result |
|---|---|
| Broker acknowledged | `SENT`, `attempts + 1`, `event_id`, `sent_at` |
| Data cannot form a valid message (unmapped category, negative amount, no lines, `branch_code` not the configured branch, ...) | `FAILED` with `last_error`; next day in the round continues; tried again every round |
| Kafka not reachable | `FAILED` with `last_error`; round stops; remaining days stay pending for the next round |
| Producer stops after the ack, before writing `SENT` | revision is sent again next round; HQ skips it as a duplicate (rule R5) |
| Manager edits and re-confirms | new revision is pending and sent; the old revision's `SENT` row stays |

Every attempt also adds a row to `sync_attempt`, in the same transaction as the `sync_log` update. `sync_log` keeps only the latest error (cleared on success); `sync_attempt` keeps all of them. An attempt that failed at Kafka keeps the `event_id` of its message, because such a message can still reach HQ (see the duplicate case above). There is no automatic clean-up yet: while HQ is unreachable, each pending day adds one row per round.

```bash
demo/sql.sh BR0001 demo/sync-attempts.sql    # attempt history of a demo branch
```

Days and lines are read in one repeatable-read transaction, so the lines always belong to the revision that was read. Every day has key `branchCode`, so all messages of a branch go to one partition and HQ reads them in order.

## Branch identity

The branch code comes from configuration (`branch-sales.branch-code`), not from the back-office data. It is the record key and the `branchCode` of every message. A back-office day whose `daily_sales.branch_code` is different (a re-coded branch, a database copied from another branch) is not sent: it is logged as `FAILED`, because HQ would otherwise store it as a second row under the other code.

At startup the producer checks the branch code and every mapped HQ category code against the contract patterns, and does not start if one is wrong.

## Category mapping

Each back-office uses its own category codes. They are mapped to the HQ codes ([categories.md](https://github.com/mpiumakkho/branch-sales-consumer/blob/main/contract/categories.md)) in configuration. Several local codes may map to one HQ code; their amounts and quantities are added up into one line.

```yaml
branch-sales:
  branch-code: BR0001
  category-mapping:
    BEV: BEVERAGE
    DRINK_HOT: BEVERAGE
    SNK: SNACK
```

## Run with Docker

[`docker-compose.yml`](docker-compose.yml) runs one branch: a back-office PostgreSQL with the simulated tables, and the producer. The producer joins the HQ network `branch-sales-wan` (created by the HQ infra in the consumer repo) and reaches Kafka at `kafka.hq.example:9094` with profile `sasl` (TLS + SCRAM). HQ must have onboarded the branch first (`infra/onboard-branch.sh` in the consumer repo) with the same password. Each branch is started with its own override file, which sets the project name, the database port and the branch configuration:

```bash
cp .env.example .env    # set BRANCH_DB_PASSWORD, BR0001_KAFKA_PASSWORD, HQ_CA_CERT
docker compose -f docker-compose.yml -f demo/BR0001.compose.yaml up -d --build
demo/sql.sh BR0001 demo/BR0001/01-enter-and-confirm.sql    # the manager enters and confirms a day
demo/sql.sh BR0001 demo/branch-status.sql                  # days and their sync_log status
```

The full walkthrough with HQ and two branches, including the failure cases, is in the consumer repo: [demo/README.md](https://github.com/mpiumakkho/branch-sales-consumer/blob/main/demo/README.md).

## Configuration

| Variable | Default | |
|---|---|---|
| `BRANCH_CODE` | none | required: this branch's code in the HQ branch registry (`branch-sales.branch-code`). Only back-office days with this `branch_code` are sent |
| `BRANCH_DB_URL` | `jdbc:postgresql://localhost:5434/branch` | branch back-office database |
| `BRANCH_DB_USER` | `branch_app` | |
| `BRANCH_DB_PASSWORD` | none | required |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | HQ Kafka; from the `branch-sales-wan` network use `kafka.hq.example:9094` with profile `sasl` |
| `SPRING_PROFILES_ACTIVE` | none | `sasl` for the HQ external listener: TLS + SCRAM-SHA-512, user = branch code ([application-sasl.yaml](src/main/resources/application-sasl.yaml)). Without it the producer uses PLAINTEXT (HQ host listener from an IDE, tests) |
| `KAFKA_PASSWORD` | none | profile `sasl`: the branch's SCRAM password, from HQ |
| `KAFKA_TRUSTSTORE` | `/certs/ca.crt` | profile `sasl`: CA certificate from HQ (PEM) |
| `SEND_CRON` | `0 0 * * * *` | round start times (Spring cron, Asia/Bangkok); `-` disables rounds |
| `SEND_MAX_JITTER` | `30m` | upper bound of the random delay before each round |
| `SPRING_CONFIG_ADDITIONAL_LOCATION` | none | extra configuration file, e.g. the branch's category mapping (`file:/config/branch.yaml` in Docker) |

Kafka producer: `acks=all`, idempotence on, a send fails after about 30 s if HQ is not reachable. The topic is `branch-sales.daily-summary.<branch-code>`; the branch's Kafka user may write no other topic, and HQ rejects a message whose `branchCode` is not the topic's branch.

If HQ revokes the branch (`offboard-branch.sh`), sends fail with `TopicAuthorizationException` and the days stay pending (`FAILED`). After HQ onboards it again with a new password, update `KAFKA_PASSWORD` and restart the producer; the next round sends them.

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
