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

## Reproducing the Aurora-only paths

You cannot, from here, and that is fine. `pg_wait_stats` and the RDS log API need a managed target. Their
parsing is covered by tests against real captured output instead — see
`Darling.Tests/Fixtures/auto_explain_real_block.txt`, which was captured from this rig.
