---
name: lane-orders
description: Standing orders for a lane agent working PerformanceMonitor issues in a parallel wave, plus the coordinator's dispatch and verification checklist. Use when dispatching lane agents or acting as one. Machine-neutral; the coordinator fills the placeholders per wave.
argument-hint: [lane]
disable-model-invocation: false
---

# Lane orders

One lane works one issue, or a few small related ones, in its own worktree. The coordinator reviews each PR, arms
auto-merge and splices the CHANGELOG. These orders apply alongside the user's token guardrails, which win on
conflict.

**The coordinator fills these per wave, in every brief:**
- `<LANE>`, `<ISSUES>`, `<DEADLINE>`;
- `<RIG_PORT>`, `<RIG_DIR>`, `<PG_RUNTIME_ZIP>`;
- `<REPORT_FILE>`, `<CHANGELOG_BUFFER_DIR>`. Under worktree isolation, `<REPORT_FILE>` must be inside the lane's
  own worktree (for example `.lane-report.md`, untracked), because the harness blocks writes to the coordinator's
  scratchpad. Isolated worktrees live under `.claude/worktrees/` inside the repo, so a repo-scanning test must not
  exclude that path;
- `<CO_AUTHOR_TRAILER>`, `<SESSION_URL>`.

## For the coordinator

Before dispatch:
- Rank the queue and get the OK the guardrails require.
- Post every ruling on its issue AND put it in the first prompt.
- Pre-approve any store migration by name. Every other lane stops and reports instead.

At dispatch:
- Run code-editing lanes with worktree isolation (Claude Code: `isolation: "worktree"`).
- Give each lane its own rig port.
- Set a wall-clock check at each lane's deadline; don't wait on notifications.

At each report:
1. Stop the agent.
2. Check that the shared checkout is clean.
3. Read the PR's core product diff against the checklist below.
4. Arm auto-merge only after that, with CI green.

**Verification checklist** (the invariants agents most often miss):
- **Storage precision:** PostgreSQL and DuckDB keep microseconds; a .NET tick is not stored.
- **NULL and tie ordering:** a descending sort puts NULLs first in PostgreSQL.
- **Plans that need an index some stores won't have yet:** fall back to the old shape when the index is missing.
- **Translated or locale-specific text:** `lc_messages` isn't always English.
- **Per-row versus per-snapshot semantics:** "newest row per key" is not "the newest snapshot".
- **Lite parity**, claimed as checked. Verify the claim, not just its presence.

## Hard rules for the lane

- **Never edit, build in, or check out branches in the shared main checkout.** Your first action is creating your
  own worktree.
- Never paste full test output. Grep for `[FAIL]|Total:`.
- Read files by offset/limit. Never read a whole large file.
- Run targeted test classes while iterating. Run the FULL suite ONCE, at the end.
- Cap command output: pipe through `head`, `tail -n` or `grep`. Never dump full CI logs, full JSON, or whole
  issue/PR bodies.
- Stop and write a handoff note when your context passes about 150k.
- Never end your turn to wait for a background notification. Run the full suite in the foreground (about 10
  minutes), or poll your own output file.
- Write your full final report to `<REPORT_FILE>`. Your last message is a summary of 300 words or fewer that points
  to it.
- Finish, or stop at a committed and pushed checkpoint with a handoff, by `<DEADLINE>`.
- **Never stop to ask.** If an item needs a decision nobody has made, park it with the question in your report and
  move on.
- Never edit `CHANGELOG.md`. Never run `Installer.Tests`. Never touch a monitored or production server. Never kill
  processes by image name; kill only PIDs you started. Use absolute paths, never shell env-var paths.

## Worktree and branch

- `git -C <repo> fetch origin`, then `git -C <repo> worktree add -b fix/<issue>-<slug> <worktrees>/<name> origin/dev`.
  Use one branch per issue.
- Before the final full run, `git merge origin/dev`. Never rebase. Resolve by keeping both sides.

## PostgreSQL rig

1. Use a current runtime zip at `<PG_RUNTIME_ZIP>`. If it is missing or stale, run `Darling/tools/fetch-pg-runtime.ps1`,
   which is what CI runs. A stale zip fails the runtime-version pin tests.
2. Extract it to `<RIG_DIR>`, then run `initdb -D <RIG_DIR>/data -U darling -A trust --encoding=UTF8`.
3. Append to `postgresql.conf`:
   - `shared_preload_libraries = 'timescaledb'`
   - `port = <RIG_PORT>`
   - `listen_addresses = '127.0.0.1'`
   - `timescaledb.max_background_workers` and `max_worker_processes`, as CI's `darling-pg` job sets them.
4. Check the port is free. Start the server in the background, because `pg_ctl -w start` hangs a tool call, and
   confirm "ready to accept connections" in the log.
5. Create `darlingtest` for the suite, and a separate `probe` database for hand-run SQL. A suite database reused
   across several full runs produces false failures; drop and recreate it before a final run.
6. Set `DARLING_TEST_PG` to `Host=127.0.0.1;Port=<RIG_PORT>;Username=darling;Database=darlingtest`. Set
   `DARLING_TEST_PGRUNTIME=<RIG_DIR>` only when you touch the managed runtime or role provisioning.
7. Stop the server when you finish.

## Build and test

- Build `Darling/Darling.Tests/Darling.Tests.csproj`, and `Lite.Tests/Lite.Tests.csproj` if you touch Lite. Require
  `0 Warning(s)`; a piped build reports the pipe's exit code.
- The tests are MTP executables: run `Darling.Tests.exe` / `Lite.Tests.exe` from `bin/Debug/<tfm>/`, filtered with
  `-class "*Name*"` or `-method "*Name*"`. `dotnet test` does not work.
- For a lone failure in a class you didn't touch, re-run that class alone before calling it yours.
- **Filter by every class in every test file you touch**, not by the file's name. One file can hold several classes
  (`PgPlanCaptureCollectorDefinitionTests.cs` also holds `PgPlanLogParserTests`), and filtering on one class skips
  the others, including tests you just added. That let a forged-header regression reach CI (#4016).
- Census, meta and pin tests are real gates:
  - `McpPayloadContractCensusTests` and the MCP inventory pins;
  - `StartupCommandTimeoutTests` and `StorageCommandTimeoutTests`;
  - `AlertReadFailureSurfaceTests` and `DocCommentHygiene`;
  - `LivePostgresCollectionHygieneTests` and `LiveCleanupConversionRatchetTests`.

  Update a pin deliberately, with the reason. Never weaken one.
- **Live classes:**
  - Clean up through `LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, ...)`, with `bodySucceeded` set
    as the body's last statement.
  - A class that uses `ScratchPostgres` carries the `#1776 own-store` comment.
  - A fixture on its own database that must not race TimescaleDB's scheduler calls
    `_timescaledb_functions.stop_background_workers()`.

## How to work

1. Read the issue and every comment. "Ruled" comments win over the body.
2. Confirm the diagnosis on your rig before building: EXPLAIN (ANALYZE, BUFFERS) after seeding. If it's wrong, say
   so with numbers.
3. Fix it in the surrounding style. Comments say why and cite the issue. Results stay identical unless the issue
   asks for a behavior change.
4. **Lite parity is mandatory** wherever the same read or pattern exists in Lite.
5. Pin it with a regression test that fails on the old shape; prove that once by reverting. Add live tests wherever
   the relations or rows read change.
6. Fix small in-lane defects in the same PR. File an issue only for large or design-level work, and list it.

## Store migrations

Only a lane pre-approved by name may add one. Versions stay dense: take the next free number when you push, and
renumber if dev moved. A version bump also obliges:
- the sentinel in `ViewerDataService.StoreSchemaProbeSql`, plus its `MapProbedSchemaVersion` arm;
- the `GetBoolean` list in `GetStoreSchemaVersionAsync`, plus `ViewerSchemaVersionGateTests`;
- "newest migration" tests written as `Scripts[^1]`.

Never edit an existing migration.

## Finish

- End each commit with `<CO_AUTHOR_TRAILER>` and `Claude-Session: <SESSION_URL>`.
- Open the PR against `dev`. Title: the outcome in plain language, plus `(#<issue>)`. The body starts with
  `Closes #<issue>.`, then `## Why`, `## What changes` and `## Test plan` (checkboxes, measured numbers).
- **Don't merge and don't enable auto-merge.** The coordinator does both.
- A user-visible change gets a CHANGELOG entry in `<CHANGELOG_BUFFER_DIR>/<PR>-reported.txt`, in this form:

      SECTION: Fixed
      ENTRY:
      - **<bold one-line outcome>** ([#<PR>]) - <what was wrong, the user impact, what changed>
      REF:
      [#<PR>]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/<PR>

  Plain punctuation, no em dashes. A test-only PR gets none.

## Final report (in `<REPORT_FILE>`)

- the PR URL, or why the issue was parked;
- the diagnosis, confirmed or corrected, with before and after numbers;
- test totals, with each failure and why;
- issues filed;
- what the coordinator should double-check.
