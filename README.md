# branch-sales-producer

[![ci](https://github.com/mpiumakkho/branch-sales-producer/actions/workflows/ci.yml/badge.svg)](https://github.com/mpiumakkho/branch-sales-producer/actions/workflows/ci.yml)

Branch side of Branch Daily Sales Sync. Every branch runs its own Kafka broker and MongoDB next to the producer. The producer reads the days the branch manager has confirmed in the branch back-office database and publishes them to the branch's Kafka topic `branch-sales.daily-summary`; HQ connects to the branch, reads them, and writes a receipt per record to `branch-sales.receipt`, which the producer records. The branch therefore knows for every day whether HQ stored it or why it was rejected.

The message formats are the contract in the HQ consumer repo ([branch-sales-consumer/contract](https://github.com/mpiumakkho/branch-sales-consumer/tree/main/contract)). The two sides share no code; [`contract/`](contract/) holds copies of the schemas that the tests check messages against.

| Folder | Content |
|---|---|
| `src/` | The producer (Java 25, Spring Boot 4) |
| [`backoffice/`](backoffice/) | Simulated back-office tables, for tests and the demo |
| [`contract/`](contract/) | Copies of the contract schemas, test only |
| [`edge/`](edge/), [`mongo/`](mongo/) | Branch edge (HAProxy) and MongoDB user script for `docker-compose.yml` |
| [`demo-branches/`](demo-branches/) | Two demo branches (configuration, simulated manager actions) for the end-to-end demo |

## Branch database

| Table | Owner | Producer access |
|---|---|---|
| `daily_sales` | back-office system | read: `sale_date`, `branch_code`, `status` (`DRAFT`/`CONFIRMED`), `revision`, `confirmed_at` |
| `daily_sales_line` | back-office system | read: `sale_date`, local `category_code`, `amount`, `quantity` |

The producer writes nothing to the branch database: a login with `SELECT` on these two tables is enough (the demo creates `branch_sales_reader`, [backoffice/postgresql/reader-user.sh](backoffice/postgresql/reader-user.sh)). Its own state is in MongoDB (below). In a real branch the back-office tables already exist. Here they come from `backoffice/<database>/schema.sql`, and confirmation is simulated with SQL:

```sql
update daily_sales set status = 'CONFIRMED', revision = revision + 1, confirmed_at = now() where sale_date = '2026-10-01';
```

### Supported databases

One producer build for every branch; the database is chosen by `BRANCH_DB_URL`.

| Database | Tested with | `BRANCH_DB_URL` example | Back-office schema | Branch requirement |
|---|---|---|---|---|
| PostgreSQL | 18.6 | `jdbc:postgresql://host:5432/branch` | [backoffice/postgresql](backoffice/postgresql/schema.sql) | — |
| MySQL | 8.4 | `jdbc:mysql://host:3306/branch?connectionTimeZone=Asia/Bangkok` | [backoffice/mysql](backoffice/mysql/schema.sql) | `connectionTimeZone=Asia/Bangkok` in the URL: the back-office stores local time without a zone (`DATETIME`), which must be read as Bangkok time (rule R7) |
| SQL Server | 2022 | `jdbc:sqlserver://host:1433;databaseName=branch;encrypt=true;trustServerCertificate=false` | [backoffice/sqlserver](backoffice/sqlserver/schema.sql) | `ALLOW_SNAPSHOT_ISOLATION ON` for the database (one-time DBA setting, see below) |

Days and lines are read in one read-only transaction that sees one snapshot, so the lines always belong to the revision that was read and the producer never blocks the back-office's writes:

- PostgreSQL and MySQL (InnoDB): `REPEATABLE READ`, which reads from a snapshot.
- SQL Server: `SNAPSHOT`. Its `REPEATABLE READ` takes shared locks instead, which would make back-office writes wait and could deadlock with them. `SNAPSHOT` needs `ALTER DATABASE <db> SET ALLOW_SNAPSHOT_ISOLATION ON`; it adds row versions in `tempdb` and does not change how other transactions behave.

`readingIsNotBlockedByAnOpenBackOfficeTransaction` checks this on all three: with the day locked by an open back-office transaction, the producer still reads the last committed state within 5 seconds. With SQL Server's default isolation, that test times out.

## Send state (MongoDB)

Collection `sync_state`, one document per `(saleDate, revision)`, id `2026-10-01#1`:

| Status | Meaning | Sent again? |
|---|---|---|
| (no document) | never sent | yes |
| `FAILED` | no message could be written from the data, or the branch broker did not acknowledge it; `lastError` says why | yes, every round |
| `SENT` | the branch broker acknowledged the message; `sentOffsets` holds its offset(s); waiting for HQ | after `SEND_RESEND_AFTER` without a receipt (the branch broker may have lost it); HQ then answers `DUPLICATE` if it had stored it |
| `HQ_ACCEPTED` | HQ receipt `INSERTED`, `UPDATED`, `DUPLICATE` or `STALE`: HQ holds this revision or a higher one. Final | no |
| `HQ_REJECTED` | HQ receipt `REJECTED`: `hqRejectReason`, `hqDetail`. HQ keeps the record and can replay it; a later `INSERTED` receipt for the same offset makes it `HQ_ACCEPTED` | no (fix the data and confirm again, or wait for HQ's replay) |

Every status change is one update of one document, applied atomically by MongoDB, so no transaction or replica set is needed, and nothing is buffered: a producer that stops right after a broker ack has lost only the `SENT` write, and sends the revision again (HQ answers `DUPLICATE`). The document keeps `attempts` and the last 50 events (`history`): send attempts with their offset or error, and HQ receipts. Receipts are matched to the revision by the offset the message was sent at.

Once a day (`CLEANUP_CRON`, 03:30 Asia/Bangkok) documents of days HQ accepted whose last change is older than `SEND_STATE_RETENTION` (90 days) are deleted. Days still waiting (`SENT`, `FAILED`) or rejected by HQ are kept whatever their age, so nothing pending disappears from view. The retention must be longer than the lookback, otherwise an accepted day still within the lookback would be sent again.

```bash
demo-branches/sync-state.sh BR0001             # a demo branch's send state
demo-branches/sync-state.sh BR0001 --history   # with every attempt and receipt
```

## Send rounds

```
cron (every hour, Asia/Bangkok) ──► random delay 0–30 min ──► round:
   read CONFIRMED days of the last SEND_LOOKBACK days, oldest first; keep those whose revision needs sending (table above)
   for each day: map categories → write message → send to the branch broker, wait for ack (acks=all) → SENT
```

| Situation | Result |
|---|---|
| Broker acknowledged | `SENT`, `attempts + 1`, offset added to `sentOffsets` |
| Data cannot form a valid message (unmapped category, negative amount, no lines, `branch_code` not the configured branch, ...) | `FAILED` with `lastError`; the round continues with the next day; tried again every round |
| Branch broker not reachable | `FAILED` with `lastError`; the round stops; remaining days stay pending for the next round |
| Manager edits and re-confirms | the new revision is sent; the old revision's document stays |
| HQ's receipt arrives before `SENT` was written (HQ can answer within milliseconds) | the receipt listener retries for about 10 s, then applies it; after that it is logged and skipped, and the revision is sent again after `SEND_RESEND_AFTER` |

Every day has key `branchCode` and the topic has one partition, so HQ reads a branch's days in order.

## Branch identity

The branch code comes from configuration (`branch-sales.branch-code`), not from the back-office data. It is the record key and the `branchCode` of every message, and HQ checks it against the branch whose broker it read the record from. A back-office day whose `daily_sales.branch_code` is different (a re-coded branch, a database copied from another branch) is not sent: it is recorded as `FAILED`, because HQ would otherwise store it under the other code or reject it. A receipt for another branch code is logged and ignored.

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

[`docker-compose.yml`](docker-compose.yml) runs one branch: the back-office PostgreSQL with the simulated tables, MongoDB, the branch's Kafka broker, `kafka-init`, the edge and the producer. HQ must have onboarded the branch first (`infra/onboard-branch.sh` in the consumer repo), which produces the broker certificate for `kafka.<branch>.example` and HQ's password; both go into the branch's `.env`. Each branch is started with its own override file, which sets the project name, the branch host name, the certificate, the password and the branch configuration:

```bash
cp .env.example .env    # set the passwords, BR0001_HQ_KAFKA_PASSWORD and BR0001_KAFKA_PEM from HQ
docker compose -f docker-compose.yml -f demo-branches/BR0001.compose.yaml up -d --build
HQ_KAFKA_PASSWORD=... ./smoke-test.sh BR0001 ../branch-sales-consumer/infra/tls/out/ca.crt
demo-branches/sql.sh BR0001 demo-branches/BR0001/01-enter-and-confirm.sql    # the manager enters and confirms a day
demo-branches/sync-state.sh BR0001                                  # its send state, then HQ's receipt
```

| Service | Image | Purpose |
|---|---|---|
| `branch-db` | `postgres:18.6-alpine` | Simulated back-office; the producer logs in as `branch_sales_reader` (SELECT only) |
| `mongodb` | `mongo:8.0.16` | `sync_state`; the producer logs in as `branch_sales` (readWrite on `branch_sales`) |
| `kafka` | `apache/kafka:4.3.1` | Single KRaft node. Listeners: `LOCAL` `kafka:19092` PLAINTEXT (branch network only), `EXTERNAL` `:9094` SASL_SSL (for HQ, through the edge), `CONTROLLER` `:9093`. ACLs on; `User:ANONYMOUS` on `LOCAL` is a super user |
| `kafka-init` | `apache/kafka:4.3.1` | Creates `branch-sales.daily-summary` and `branch-sales.receipt` (1 partition, 30 days), user `hq` with HQ's password, and its ACLs (read summaries, write receipts, group `hq-branch-sales-consumer`), then exits. Run again after a password change |
| `edge` | `haproxy:3.2.25-alpine` | Stands in for the branch firewall: the only branch container on `branch-sales-wan`, under the alias `kafka.<branch>.example`, forwarding TCP 9094 to the broker's `EXTERNAL` listener. TLS passes through ([edge/haproxy.cfg](edge/haproxy.cfg)) |
| `producer` | built from this repo | |

`smoke-test.sh` runs from `wan`, as HQ connects: HQ's user can log in over TLS (host name checked against the HQ CA) and sees the two topics, cannot write the summary topic, a wrong password and a plaintext client are refused, and nothing but the edge's port 9094 is reachable from `wan` (not Kafka's other ports, MongoDB, the database or the producer).

The full walkthrough with HQ and two branches, including the failure cases, is in the consumer repo: [demo-branches/README.md](https://github.com/mpiumakkho/branch-sales-consumer/blob/main/demo/README.md).

## Configuration

| Variable | Default | |
|---|---|---|
| `BRANCH_CODE` | none | required: this branch's code in the HQ branch registry (`branch-sales.branch-code`). Only back-office days with this `branch_code` are sent |
| `BRANCH_DB_URL` | `jdbc:postgresql://localhost:5434/branch` | branch back-office database: PostgreSQL, MySQL or SQL Server ([examples](#supported-databases)) |
| `BRANCH_DB_USER` | `branch_app` | a login with `SELECT` on `daily_sales` and `daily_sales_line` |
| `BRANCH_DB_PASSWORD` | none | required |
| `MONGODB_URI` | `mongodb://localhost:27017/branch_sales` | the branch's MongoDB, database for `sync_state` |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | the branch's own broker; in Docker `kafka:19092` |
| `SEND_CRON` | `0 0 * * * *` | round start times (Spring cron, Asia/Bangkok); `-` disables rounds |
| `SEND_MAX_JITTER` | `30m` | upper bound of the random delay before each round |
| `SEND_LOOKBACK` | `60d` | confirmed days older than this are not read |
| `SEND_RESEND_AFTER` | `24h` | a `SENT` day without an HQ receipt after this long is sent again |
| `SEND_STATE_RETENTION` | `90d` | send state of accepted days is deleted this long after its last change; must exceed `SEND_LOOKBACK` |
| `CLEANUP_CRON` | `0 30 3 * * *` | when the clean-up runs (Spring cron, Asia/Bangkok); `-` disables it |
| `SPRING_CONFIG_ADDITIONAL_LOCATION` | none | extra configuration file, e.g. the branch's category mapping (`file:/config/branch.yaml` in Docker) |

Kafka producer: `acks=all`, idempotence on, a send fails after about 30 s if the branch broker is not reachable. The topics and HQ's consumer group are fixed by the contract.

## Test

```bash
./mvnw test
```

Needs JDK 25 and Docker. Tests start their own Kafka, MongoDB and branch database containers (PostgreSQL, MySQL and SQL Server, each with the back-office tables); HQ is simulated by writing receipts to the receipt topic. The SQL Server image is about 1.5 GB and needs 2 GB of memory.

| Test | Checks |
|---|---|
| `SummaryMessageWriterTest` | messages are valid against the contract schema; money format, Bangkok time with seconds, category mapping and merging, data rejected before sending |
| `PostgresSendRoundTest`, `MySqlSendRoundTest`, `SqlServerSendRoundTest` | the same tests ([AbstractSendRoundTest](src/test/java/io/github/mpiumakkho/branchsales/producer/service/AbstractSendRoundTest.java)) on each database: only confirmed revisions are sent, once; `SENT` waits for HQ and becomes `HQ_ACCEPTED` or `HQ_REJECTED` from the receipt; a rejected day is not sent again until re-confirmed, and a later `INSERTED` receipt (HQ replay) accepts it; a `SENT` day without a receipt is sent again after `resend-after`, and either copy's receipt completes it; a receipt that arrives before `SENT` is applied once the offset is known; days outside the lookback are not read; a bad day does not block other days; reading is not blocked by an open back-office transaction; Kafka failure (simulated and with the broker paused) stops the round and the next round sends everything in date order, with the history showing both attempts |
| `ProducerPropertiesTest` | branch code, category mapping and durations are checked at startup, including retention longer than lookback |
| `SyncStateCleanupTest` | only accepted days older than the retention are deleted; waiting and rejected days stay |
| `SendSchedulerTest` | random delay stays between 0 and the maximum |
