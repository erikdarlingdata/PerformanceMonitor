# Web UI smoke check (#5701)

Loads every Darling web page and Web Admin tab in a real Chrome against a service running on a throwaway, seeded
PostgreSQL store. It fails the job on a console error, a page error, a failed request, an answer of 400 or more, an
error strip, or a panel or grid that the seed says must have data and does not. A screenshot and a trace are kept for a
page that failed. Four pages are also compared with screenshot baselines (see "Screenshot baselines" below).

It runs in `.github/workflows/darling-ui-smoke.yml` ("Darling web UI smoke") by manual dispatch and in the nightly
(`nightly.yml`, outside `publish`'s `needs`, so a red reports and does not stop publishing). It no longer runs on pull
requests (#6656), and it is not a required check.

## Files

| File | What it is |
| --- | --- |
| `run-ui-smoke.ps1` | The entry, in CI and locally: builds the service, starts it on the store, waits for `/api/ping` and the config seed, applies `seed.sql`, runs `smoke.mjs`, stops the service. |
| `darling.ui-smoke.json` | The service config. Two servers, `ui-sql` (SQL Server) and `ui-pg` (PostgreSQL), both at `127.0.0.1` port 1, so they fail to connect at once and the service never collects over the seeded rows. (`servers: []` is refused by config validation.) The web host asks for its token on loopback too, so `web.network` holds only a token. `run-ui-smoke.ps1` makes a fresh one per run, signs in once, and passes the session cookie to `smoke.mjs` and `shots.mjs`, so the pages render as a signed-in editor sees them. |
| `seed.sql` | The rows. Every time is relative to the store's own clock and every count is fixed, so the assertions are exact. |
| `pages.json` | The pages and what each must show. |
| `smoke.mjs` | The browser walk (`playwright-core`, the installed Chrome or Edge). |
| `shots.mjs` | The screenshot step (`playwright-core`'s own Chromium, `pixelmatch`, `pngjs`). |
| `baselines/*.png` | The baseline screenshots, one per page marked `screenshot` in `pages.json`. |

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

## Screenshot baselines

Four pages have a baseline PNG in `baselines/`: `fleet`, `ui-sql-overview`, `ui-sql-queries` and `admin-servers`. After the
smoke walk, `shots.mjs` renders each one at 1280 by 800 and compares it with its baseline. A pixel counts as different when
it differs by more than a small color threshold. The renders are the same from run to run, so there is no allowance.
A page is marked by a `screenshot` key in `pages.json`. The key lists CSS selectors to paint over, for text that changes
from run to run.

The compare is advisory (#5931). A page that differs from its baseline prints one `NOTICE screenshot <page>: <n> pixels differ`
line and writes its baseline, actual and diff images. It does not fail the job. The last lines of the run say how many
pages matched, how many differ and how many had problems. The workflow also writes a table of the per-page pixel counts to
the job summary.

What still fails the job:

- A page that does not load, or a console or page error.
- A panel or grid that the page's `expect` entry requires and that is missing or empty.
- A step that fails (the stage that shoots the pages throws, the seed cannot be applied, the service does not start).
- A screenshot problem that is not a pixel difference: no baseline for a page, a size that differs from the baseline, a
  page that never settles, or a fleet that never turns collection stale.

How the page is kept the same on every run:

- The browser is Playwright's own Chromium, pinned by the `playwright-core` version. It is not the runner's Chrome, which
  updates with the runner image.
- The viewport, scale (1), color scheme, locale (en-US), time zone (UTC) and reduced motion are fixed. Animations are off and
  the caret is hidden.
- The seed dates every row from one anchor, the minute `run-ui-smoke.ps1` ran it. The service must keep its real clock, so the
  browser is moved instead. Every `/api` answer has its timestamps shifted to a fixed date. Every windowed read gets an
  `as_of` of anchor plus one minute. The page clock is pinned to the same fixed date plus one minute.
- A screenshot is taken again until two in a row are identical. If that does not happen in 8 tries, the page fails.
- The service marks the two unreachable servers "collection stale" a couple of minutes after the seed anchor, and "offline"
  after 30. The shots wait until `/api/fleet` shows every card stale. They give up after 4 minutes with one message if the
  cards never got there, and with another if the cards are already past stale.
- The database size in the status bar is not the same on every run. Its width changes when it gains a digit (99 MB to 100 MB).
  In one run the item was missing, because the read of the size had not finished. A mask hides the text, not the box. For the
  shots, `shots.mjs` adds a style that hides the item and the separator before it, so the bar is the same either way.
- `report.json` is `{ pages, screenshotProblems, screenshots }`. `screenshots` has the name, pixel count and percentage of
  each compared page. The page walk is written first, then rewritten with the screenshot results, so a failure in the
  screenshot step cannot lose the walk.
- Playwright's Chromium lives in the folder named by `PLAYWRIGHT_BROWSERS_PATH`. The workflow sets it and caches that folder.

To refresh the baselines (after a change to the page that you want to keep):

1. Start the workflow "Darling UI smoke" by hand on your branch with `update_baselines=true`. For example:
   `gh workflow run darling-ui-smoke.yml --ref <branch> -f update_baselines=true`.
2. Download the artifact `darling-ui-smoke-new-baselines` from that run.
3. Copy the PNGs into `tools/ui-smoke/baselines/` and commit them on the pull request.
4. Review GitHub's image diff of each PNG. Never commit a PNG that you rendered on your own machine. Fonts and the browser
   differ, so most pixels differ in CI.

Every run, pass or fail, uploads the artifact `darling-ui-smoke`. For each page that differs, its `ui-smoke/out/shot-failures` folder
holds `<page>.baseline.png`, `<page>.actual.png` and `<page>.diff.png` (the changed pixels). If the change is wanted, refresh
the baselines as above. If a runner image update moved the pixels, refresh them the same way. A drift that nobody refreshes
does not fail anything, so look at the NOTICE lines on a pull request that changes a page.

## Add a page or a seeded table

- A page: one entry in `pages.json` (`name`, `hash`, and `nav` for a shell route or `server` for a server tab). Add
  `expect.panels` (by `title` or by `panel`, the read name) with `state` and `minRows`, or `expect.grids` for a page
  with no panel cards, or `expect.strips` (`key`, `state`) for a strip inside the Overview timeline panel's one SVG. Take the panel names from a run's log: it prints every panel with its state and row count.
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
