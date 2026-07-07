# Implementation Plans

Plans are numbered in suggested implementation order. Each plan's "Relationship to other plans"
section states what it depends on and what it is independent of (independent plans can be built
in parallel).

| # | Plan | Depends on | Independent of |
|---|------|------------|----------------|
| 01 | [Tables](01-tables.md) | — | — |
| 07 | [Public client package](07-public-client.md) | 01 | 03, 05 — best done right before 02 |
| 02 | [Protocol framing & argument encoding](02-protocol-framing.md) | — | 01 |
| 03 | [WAL persistence & snapshots](03-wal-persistence.md) | 01, 02 | — |
| 04 | [Replication (master/standby)](04-replication.md) | 02, 03 | 01 (beyond 03) |
| 05 | [Memory/disk tiering with LRU](05-lru-tiering.md) | 03 | 04 |
| 06 | [Typed values & operations](06-typed-values.md) | 02, 03 | 04, 05 |

Dependency graph:

```
01 tables ──┬──────┐
            │      ├──> 03 WAL ──┬──> 04 replication
07 client ──┤      │             ├──> 05 LRU tiering
            │      │             └──> 06 typed values
02 protocol ┴──────┘
```

Plan 07 was added after 01 was implemented; its number reflects addition order, but its
recommended slot is between 01 and 02 (the public API absorbs 02's breaking protocol change).
After plan 03 lands, plans 04, 05, and 06 are mutually independent and can proceed in parallel.
