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
- `<CHANGELOG_BUFFER_DIR>`;
- `<CO_AUTHOR_TRAILER>`, `<SESSION_URL>`.

If your brief leaves one unfilled, don't invent a value. Skip the step that needs it and say so in the PR body.
In wave E (2026-09-24), a lane whose brief left `<CHANGELOG_BUFFER_DIR>` unfilled wrote its changelog entry to a
made-up path inside the repo.

Isolated worktrees live under `.claude/worktrees/` inside the repo, so a repo-scanning test must not exclude that
path.

## For the coordinator

Before dispatch:
- Rank the queue and get the OK the guardrails require.
- Post every ruling on its issue AND put it in the first prompt.
- Pre-approve any store migration by name. Every other lane stops and reports instead.
- Fast-forward the main checkout's local `dev`. A `lane` agent loads this skill from that checkout, and a stale
  checkout can lack it.

At dispatch:
- Dispatch each lane as a `lane` agent (Claude Code: `subagent_type: "lane"`) where the harness defines that
  type. It runs on the default tier (Sonnet) in its own worktree, with this skill loaded and a context watchdog
  attached. Where the type does not exist, use a general agent on the default tier with worktree isolation
  (Claude Code: `isolation: "worktree"`). Put these orders in its brief.
- A lane that fixes review findings also runs on the default tier. The top tier reviews, and the default tier
  fixes what the review found.
- Give each lane its own rig port.
- Set a wall-clock check at each lane's deadline; don't wait on notifications.
- Size each lane to finish under 200k. In wave B (2026-09-23), a lane's context grew about 1.9k tokens per call
  from a 40k base, so 200k is about 80 calls. A #3898 lane that converted three tools used 100-117 calls.
  In wave C (2026-09-24), the base fell to 24k, but a lane with two twin pairs (two tools on both products) still
  used 103-167 calls and peaked at 214-242k. Give a #3898 lane ONE twin pair, or two tools on one product.
- Check that the effort level took effect: grep a lane transcript for `"effort":`. In wave C, `effort: high` in the
  agent file did not apply, and every lane turn logged the seat's `max`.
- Every head clause about a status must keep its condition's scope. In wave C, each head-fact fix the coordinator
  made was a scope error: "nothing collected yet" for "none collected in the window", or one state left out.
- Paste into the brief the few lines a lane needs (an example, a ruling) instead of pointing it at a PR or file.
  In wave B, every lane read an 8-10k-character PR section to see one example.
- Label each issue the lane takes `in-progress` (`gh issue edit <n> --add-label in-progress`), so the board shows
  it as taken; remove the label when the issue closes or the work stops.

At each report:
1. Stop the agent.
2. Check that the shared checkout is clean.
3. Read the PR's core product diff against the checklist below. Lane PRs arrive as drafts, which can't be merged
   from the UI. On 2026-09-23, three PRs opened ready were merged before this step finished, and one of them
   put a regression on dev.
4. A security PR, or an irreversible data rewrite, gets a review round before it leaves draft.
5. Mark it ready (`gh pr ready <n>`) and arm auto-merge only after that. Auto-merge waits for CI.

At each wave boundary, backtrack every issue number the wave filed or touched:
- close what is done;
- give every open issue a disposition comment and a work-order rank;
- fix the `in-progress` labels.
A wave that files follow-ups faster than it closes them hides its own backlog. On 2026-09-23 a backtrack of
#3898-#4043 found 22 stale `in-progress` labels and three worked issues missing one.

After each wave, measure it with `lane-audit.py`, as the guardrails' "Measure every wave" section says. Record the
WAVE line, and the defects you fixed in each PR, in the handoff.

When a PR's required check fails on a test it doesn't touch, don't just re-run it. Take a census of the last two
days' failed first attempts: `gh run list --workflow Build`, each run's attempts through the REST
`actions/runs/<id>/attempts/<n>/jobs` endpoint, and a `[FAIL]` grep of each failed job's log. Fix every flake it
finds, in one PR. A flake that stalls every lane outranks the rest of the queue (2026-09-23: four flakes in one
census, one of them introduced by that night's own wave).

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
- Never paste full test output. Grep for `[FAIL]|Total:`. A test log saved to a file is still test output: grep
  it, never Read it.
- Read files by offset/limit. Never read a whole large file.
- grep prints whole lines. Pipe it through `cut -c1-200` in files with long lines: each MCP `Description(...)` is
  one line of 1-3k characters.
- Run targeted test classes while iterating. Run the FULL suite ONCE, at the end.
- Cap command output: pipe through `head`, `tail -n` or `grep`. Never dump full CI logs, full JSON, or whole
  issue/PR bodies.
- Plan to finish before your watchdog's first warning, at 200k. Past about 150k, start no new item: finish the
  one in hand, then the finish phase. On 2026-09-23, 40% of lane cost came after that warning.
- Never end your turn to wait for a background notification. Run the full suite in the foreground (about 10
  minutes), or poll your own output file.
- Put your full report in the PR body (see "Final report"). Your last message is 400 words or fewer and links the
  PR. Don't write report files: the harness can block a subagent's file writes outside its worktree, or all of
  them, and a PR body outlives any scratch file.
- Before you call a failure pre-existing, re-run it alone on a freshly created database, after merging `origin/dev`.
  If it passes there, the cause was your database or your stale base, not the code; don't file it. If it still
  fails, check that dev's own latest CI run passes it. If dev passes, the failure is yours, even in a file you
  never opened: pin and census tests read the source files you changed. (#4025's lane called
  `CollectionLogDrainForensicsStoreTests` pre-existing, but it counts `DarlingWorker`'s fault arms, and the lane
  had added one.)
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
   - `timezone = 'UTC'` and `log_timezone = 'UTC'`. initdb takes the machine's time zone, but CI's runners run
     in UTC. On 2026-09-25, three live tests failed on every America/New_York rig and passed in CI:
     `ServerListAndSummaryPlanShapeTests`, `CaptureDownChunkOrderTests` and `ViewerW2aLivePostgresTests`.
     Lanes spent turns chasing them.
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
- Open the PR against `dev` **as a draft** (`gh pr create --draft`). Title: the outcome in plain language, plus
  `(#<issue>)`. The body starts with `Closes #<issue>.`, then `## Why`, `## What changes` and `## Test plan`
  (checkboxes, measured numbers). List anything you did not run (a live test, the full suite) as an unchecked box.
- Write the PR body in one Write. Run the plain-English checker on it once, fix the real hits in one pass, and
  stop. The coordinator polishes the prose; a lane that re-runs the checker does it at its largest context.
- **Don't mark it ready, don't merge, and don't enable auto-merge.** The coordinator does all three after
  verifying.
- A user-visible change gets a CHANGELOG entry in `<CHANGELOG_BUFFER_DIR>/<PR>-reported.txt`, in this form:

      SECTION: Fixed
      ENTRY:
      - **<bold one-line outcome>** ([#<PR>]) - <what was wrong, the user impact, what changed>
      REF:
      [#<PR>]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/<PR>

  Plain punctuation, no em dashes. A test-only PR gets none.
- Check every claim in the entry against your diff, the negative ones too. A removed issue reference is
  deleted text, so that entry can't say nothing was deleted. A change to a description must not read as a
  change to behavior. Name every user-visible change the PR makes. Lane entries were wrong in 8 of 42 at
  the #4080 splice and 6 of 41 at #4143.

## Final report (the PR body)

The PR body carries it under its sections. A parked item with no PR gets it as a comment on the issue instead.

- the PR URL, or why the issue was parked;
- the diagnosis, confirmed or corrected, with before and after numbers;
- test totals, with each failure and why;
- issues filed;
- what the coordinator should double-check.
