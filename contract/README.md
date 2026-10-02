# Contract copy

`daily-sales-summary.v1.schema.json` is a copy of the message contract owned by the HQ consumer repo:
[branch-sales-consumer/contract](https://github.com/mpiumakkho/branch-sales-consumer/tree/main/contract) (topics, record key, validation layers, examples).

The producer and the consumer share no code. This copy is used only by the tests, to check that every message the producer writes is valid against the schema.

When the contract changes, copy the new schema file here in the same commit as the producer change that uses it.

| File | Copied from consumer commit |
|---|---|
| `daily-sales-summary.v1.schema.json` | `53881e6` (contract v1) |
