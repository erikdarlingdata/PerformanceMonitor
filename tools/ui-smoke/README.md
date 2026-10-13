# Web UI smoke check (#5701)

Loads every Darling web page and Web Admin tab in a real Chrome against a service running on a throwaway, seeded
PostgreSQL store. It fails the job on a console error, a page error, a failed request, an answer of 400 or more, an
error strip, or a panel or grid that the seed says must have data and does not. No screenshots are compared: a screenshot
and a trace are kept only for a page that failed.

It runs in `.github/workflows/darling-ui-smoke.yml` ("Darling web UI smoke") on a pull request that touches
`Darling/PerformanceMonitor.Darling.Service/wwwroot/**`, `tools/ui-smoke/**` or the workflow (draft pull requests are
skipped until ready), by manual dispatch, and in the nightly (`nightly.yml`, outside `publish`'s `needs`, so a red
reports and does not stop publishing). It is not a required check: a path-filtered workflow never reports when skipped.

## Files

| File | What it is |
| --- | --- |
| `run-ui-smoke.ps1` | The entry, in CI and locally: builds the service, starts it on the store, waits for `/api/ping` and the config seed, applies `seed.sql`, runs `smoke.mjs`, stops the service. |
| `darling.ui-smoke.json` | The service config. Two servers, `ui-sql` (SQL Server) and `ui-pg` (PostgreSQL), both at `127.0.0.1` port 1, so they fail to connect at once and the service never collects over the seeded rows. (`servers: []` is refused by config validation.) A loopback web host with no `web.network` block needs no sign-in. |
| `seed.sql` | The rows. Every time is relative to the store's own clock and every count is fixed, so the assertions are exact. |
| `pages.json` | The pages and what each must show. |
| `smoke.mjs` | The browser walk (`playwright-core`, the installed Chrome or Edge). |

## What it checks, per page

1. No browser console error, page error, failed request, or response of 400 or more (`/api/*` or an asset; the URL is
   printed with the problem).
2. `#main` fills; the sidebar lists both servers; the route's nav link (or the server) is active.
3. No `.strip.error` or `[role=alert]` in `#main`, no panel still loading or in error.
4. Panels and grids named in `pages.json` have the state and rows the seed guarantees.
5. Panels the seed leaves empty show their documented empty text (`state: empty`).
6. Waits are for state (no loading strip, network idle, two identical panel reads in a row), never fixed sleeps.

A page that fails is retried once; a retry that passes is printed as a `FLAKE` line and never hidden.

## Run it locally

You need Node, Chrome or Edge, the .NET SDK, and the PostgreSQL runtime (`Darling/artifacts/pg-runtime.zip`, built by
`Darling/tools/fetch-pg-runtime.ps1`, extracted to a folder).

```powershell
$bin = '<extracted pg-runtime>\pgsql\bin'
& $bin\initdb.exe -D C:\scratch\ui-pg -U darling -A trust --encoding=UTF8
# postgresql.conf: shared_preload_libraries = 'timescaledb,pg_stat_statements', port = 55401, listen_addresses = '127.0.0.1',
#                  timezone = 'UTC', log_timezone = 'UTC'   (CI's workflow lists the worker numbers)
& $bin\pg_ctl.exe -D C:\scratch\ui-pg -l C:\scratch\ui-pg.log -w start
& $bin\createdb.exe -h 127.0.0.1 -p 55401 -U darling darling
tools/ui-smoke/run-ui-smoke.ps1 -PgBin $bin -PgPort 55401 -Channel msedge     # chrome is the default
& $bin\pg_ctl.exe -D C:\scratch\ui-pg -m fast stop
```

Logs, `report.json`, failure screenshots and traces land in `-WorkDir` (default `%TEMP%\darling-ui-smoke`). Open a
trace with `npx playwright show-trace <page>.trace.zip`. `UI_SMOKE_PAGES=ui-pg-waits,alerts` runs only those pages when
you call `node smoke.mjs` yourself.

## Add a page or a seeded table

- A page: one entry in `pages.json` (`name`, `hash`, and `nav` for a shell route or `server` for a server tab). Add
  `expect.panels` (by `title` or by `panel`, the read name) with `state` and `minRows`, or `expect.grids` for a page
  with no panel cards. Take the panel names from a run's log: it prints every panel with its state and row count.
  A `panel` key is unambiguous only for a read that feeds one card; key a fanout panel (several cards sharing one read) by `title`.
- A table: one more section in `seed.sql`, in the same shape, with its own `collection_id` range (the ranges are in
  each section's comment). `UiSmokeSeedAppliesLiveTests` (Darling.Tests) applies the seed to a freshly migrated store
  and asserts its row counts, so a schema change that breaks the seed fails the PG shards on that pull request; add the
  new table's count there too.

## The allow-list

`pages.json` `allowList` starts empty and stays that way unless a message is known and harmless and cannot be fixed in
the product. Each entry needs a `match` substring and the `issue` number that tracks it; `smoke.mjs` refuses an entry
without both. Fix the product first: the first-load 404 this check found (the page had no icon link, so the browser
asked for `/favicon.ico`) was fixed in `index.html`, not allow-listed.
