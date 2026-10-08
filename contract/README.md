# Contract copy

The schemas here are copies of the message contract owned by the HQ consumer repo:
[branch-sales-consumer/contract](https://github.com/mpiumakkho/branch-sales-consumer/tree/main/contract) (topics, record key, validation layers, examples).

The producer and the consumer share no code. These copies are used only by the tests: every message the producer writes must be valid against the schema of its type (summary or return), and the receipts the tests send to the producer are checked against the receipt schema.

When the contract changes, copy the new schema files here in the same commit as the producer change that uses it.

| File | Copied from consumer commit |
|---|---|
| `daily-sales-summary.v1.schema.json` | `53881e6` (contract v1) |
| `daily-return.v1.schema.json` | `7f36f1b` (daily returns) |
| `daily-sales-receipt.v1.schema.json` | `7f36f1b` (`type`, `PARENT_MISSING`) |
