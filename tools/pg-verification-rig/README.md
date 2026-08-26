# PostgreSQL verification rig

A realistic self-hosted PostgreSQL **target**, plus a store, so the Darling service can be pointed at
something that behaves like a customer's server.

```bash
docker compose up -d --build
docker compose exec -T target psql -U postgres -d appdb -v ON_ERROR_STOP=1 < seed.sql
# -> tables 40 | indexes 160 | tables_over_the_size_floor 40
```

Target on **55432** (`postgres` / `targetpw`, database `appdb`), store on **55441**
(`darling` / `storepw`). Point a `darling.json` at both and run `--validate-config`; it should report
**23 of 24 PostgreSQL collectors apply**, skipping only `pg_wait_stats` — Aurora's own wait
instrumentation, which has no vanilla equivalent.

## Why this exists

Eight defects were found in a single day by running the **real service** against this and reading what it
stored. Not one was reachable from CI, from a small container, or from the Aurora fleet:

| | |
|---|---|
| #2622 | three collectors wrote fewer payload values than they declared — every row rejected, for three schema versions |
| #2623 | a per-database collector failing in *some* databases reported SUCCESS with no note, which is what hid #2622 |
| #2617 | `pg_index_bloat` had never returned a row |
| #2625 | self-hosted PostgreSQL had no top-queries answer at all |
| #2629 | nine collectors were readable only from the Windows Viewer |
| #2630 | `pg_wait_sampling` was 100% `ClientRead` — idle clients drowning every real event |

## The two things that make it work

**Scale.** Several collectors have size floors — `pg_column_stats` needs `relpages >= 128`, `pg_index_bloat`
measures only indexes worth measuring — so below them a healthy collector and a broken one both return
nothing. `seed.sql` exists to clear them. A two-table container finds nothing, which is exactly why those
defects survived long enough to be found here.

**Every instrument loaded.** `pg_stat_statements`, `pg_wait_sampling`, `pg_stat_kcache`, `pg_qualstats`,
`pgstattuple`, `pg_buffercache`, `hypopg`, and `auto_explain` configured the way plan capture requires.

## Things that cost real time to learn

- **`shared_preload_libraries` must be set on the command line**, not with `ALTER SYSTEM`. It is a
  list-valued GUC and `ALTER SYSTEM` quotes the whole list as a single name, after which the server will
  not start.
- **`%Q` in `log_line_prefix` is the one people miss.** Without the query id in the prefix, captured plans
  are orphans that join to nothing — `pg_plan_capture_readiness` checks for exactly this.
- **`pg_qualstats.sample_rate` defaults to 1%**, so a short verification run records almost nothing. Set
  to 1 here.
- **Extensions come from PGDG packages, not from source.** Building them turns a two-minute image into a
  twenty-minute one, against a compiler configuration you guessed.
- **`pg_read_file`'s ACL is `postgres=X/postgres`.** The `pg_read_server_files` role does *not* carry
  EXECUTE; a superuser must grant it explicitly. That grant also exposes `pg_hba.conf`, which is why the
  plan-capture collector reports its absence as a grant the operator must choose to give.

## Two majors at once, for version drift

```bash
docker compose --profile multiversion up -d    # adds a plain PostgreSQL 18 target on 55418
```

Some of what this product reads is not stable across majors, and every one of those differences is silent:
17 gutted `pg_stat_bgwriter`, moving five columns to `pg_stat_checkpointer` and deleting `buffers_backend`
outright; 18 removed `pg_stat_io`'s `op_bytes` and replaced it with measured `read_bytes` / `write_bytes` /
`extend_bytes`. A collector that guards the difference correctly and a read that quietly returns NULL look
identical from one server.

Register **both** targets in `darling.json` and the difference becomes an observable, which is how #2653 and
#2655 were found and verified:

- the registry stamps `postgres_major_version` 17 and 18 side by side
- `pg_io_stats` splits cleanly — 17's rows carry `op_bytes` and no `read_bytes`, 18's the reverse
- `get_pg_io_stats` answers `estimated_from_block_size` for one and `measured` for the other

It also quantifies things reasoning gets wrong. 18's vectored reads mean one entry in `reads` can cover
several blocks, so the pre-18 `reads x block_size` estimate undercounts — measured here, by **10× to 16×**,
not the few percent it sounds like.

The 18 target is the **plain image**, not the extension build. The PGDG extension packages do not track a
new major immediately, and pinning to them would make the rig fail to build on the day a major ships —
exactly when this target is most useful. It exercises the core catalog views, which is where version drift
lives; the extension-backed collectors belong on the 17 target.

## Reproducing the Aurora-only paths

You cannot, from here, and that is fine. `pg_wait_stats` and the RDS log API need a managed target. Their
parsing is covered by tests against real captured output instead — see
`Darling.Tests/Fixtures/auto_explain_real_block.txt`, which was captured from this rig.
