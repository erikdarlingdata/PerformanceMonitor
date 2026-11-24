---
name: lane-orders
description: Standing orders for a lane agent working PerformanceMonitor issues in a parallel wave, plus the coordinator's dispatch and verification checklist (in COORDINATOR.md in this folder). Use when dispatching lane agents or acting as one. Machine-neutral; the coordinator fills the placeholders per wave.
argument-hint: [lane]
disable-model-invocation: false
---

# Lane orders

One lane works one issue, or a few small related ones, in its own worktree. An issue too big for one lane still gets ONE branch and ONE PR. These orders apply alongside the user's token guardrails, which win on conflict. The coordinator's dispatch and verification checklist is in `COORDINATOR.md` in this folder.

The brief fills in `<LANE>`, `<ISSUES>`, `<DEADLINE>`, `<RIG_PORT>`, `<RIG_DIR>`, `<PG_RUNTIME_ZIP>`. If one is unfilled, don't invent a value. Skip the step that needs it and say so in the PR body.

## Hard rules

- **Never edit, build in, or check out branches in the shared main checkout.** Your first action is creating your own worktree. Never connect to any server but your own rig, and never touch a monitored or production server.
- Never use `git stash`.
- Never paste full test output. Grep for `[FAIL]|Total:`, including in a saved log.
- Read files by offset/limit, never whole. Pipe grep through `cut -c1-200` in files with long lines.
- Cap command output with `head`, `tail -n` or `grep`. Never dump full CI logs, full JSON, or whole issue or PR bodies.
- Finish before your watchdog's first warning, at 200k. Past about 150k, start no new item.
- Never end your turn to wait for a background notification: run long commands in the foreground, or poll your own output file.
- Kill only PIDs you started, never by image name. Never kill VBCSCompiler.exe, Lite or the Viewer. Use absolute paths, never shell env-var paths.
- Never edit `CHANGELOG.md`, `.github/workflows/claude-review.yml`, `deprecated/`, `install/` or `upgrades/`. Never run `Installer.Tests`.
- No speculative hardening. Before adding code for a race or edge case, name the inputs and state that trigger it. If you can't, leave it out and list it in the PR body under "Considered, not added".
- Finish, or stop at a committed and pushed checkpoint with a handoff, by `<DEADLINE>`.
- **Never stop to ask.** Park an item that needs a decision with the question in your report and move on. If the fix needs a store migration nobody pre-approved, stop and report.
- Before you call a failure pre-existing, re-run it alone on a fresh database after merging `origin/dev`. If it passes, the cause was your database or base. If it still fails and dev's latest CI run passes it, the failure is yours, even in a file you never opened.

## Worktree and branch

- `git -C <repo> fetch origin`, then `git -C <repo> worktree add -b fix/<issue>-<slug> <worktrees>/<name> origin/dev`. One branch per issue.
- **An issue that several lanes build** shares one issue branch, `fix/<issue>-<slug>`, which the coordinator creates and names in each brief. Create your worktree from `origin/<issue branch>` on a branch of your own (`fix/<issue>-<slug>-<part>`). When done: fetch, `git merge origin/<issue branch>`, rerun your targeted tests, and `git push origin HEAD:<issue branch>`. If rejected, fetch, merge and push again. Never force-push.
- Before the last gate run, `git merge origin/dev`. Never rebase. Resolve by keeping both sides.
- At the end, remove your worktree and local branch.

## PostgreSQL rig

1. Use a current runtime zip at `<PG_RUNTIME_ZIP>`. Never search the disk for it. If it is missing or stale, run `Darling/tools/fetch-pg-runtime.ps1`.
2. Start the rig with the start helper, never by hand. Use your own `<RIG_PORT>` and a `<RIG_DIR>` under your scratchpad folder or a worktree. `-Owner` is your worktree path:

       pwsh -NoProfile -File C:\GitHub\PerformanceMonitor\.claude\skills\lane-orders\rig-start.ps1 -Port <RIG_PORT> -RigDir <RIG_DIR> -Owner <your worktree path>

   It sets up the cluster with the same `postgresql.conf` settings as CI's `darling-pg` job (UTC time zones, `pg_stat_statements`), starts it, and registers it in `C:\GitHub\worktrees\.rig-registry.tsv` (outside every repo, never committed). A green rig run does not excuse a CI failure: CI decides whose it is.
3. Never edit `postgresql.conf` and restart during a suite.
4. Create `darlingtest` for the suite, `probe` for hand-run SQL, and `darling` (the role-provisioning live tests fail with `3D000` without it). Drop and recreate `darlingtest` before the last run.
5. Set `DARLING_TEST_PG` to `Host=127.0.0.1;Port=<RIG_PORT>;Username=darling;Database=darlingtest`. Set `DARLING_TEST_PGRUNTIME=<RIG_DIR>` only when you touch the managed runtime or role provisioning.
6. Stop the rig when you finish, and before your wall clock runs out:

       pwsh -NoProfile -File C:\GitHub\PerformanceMonitor\.claude\skills\lane-orders\rig-stop.ps1 -DataDir <RIG_DIR>/data

   It stops only the postmaster that owns that data directory, deletes the rig folder and drops the registry line.

## Build and test

- Build `Darling/Darling.Tests/Darling.Tests.csproj`, and `Lite.Tests/Lite.Tests.csproj` if you touch Lite. Require `0 Warning(s)`.
- The tests are MTP executables: run `Darling.Tests.exe` / `Lite.Tests.exe` from `bin/Debug/<tfm>/` with `-class "*Name*"` or `-method "*Name*"`. `dotnet test` does not work.
- Run targeted classes while iterating, then the gate below. Never run the full suite on the laptop: CI decides the rest.
- **Run the classes that name what you changed.** `python .github/scripts/ci-select.py local-classes --base origin/dev` lists the test classes that name a symbol your change declares or edits. A class marked `needs rig` runs only with `DARLING_TEST_PG` set.
- **Filter by every class in every test file you touch.** One file can hold several classes, and filtering on one skips the others.
- Before you push, grep the test project for source counts your change moves (`Matches(`, `Count(` over a file you edited, `ViewerReadFanOut.Of(`) and run those classes. A new store read in a Viewer tab loader starts inside its fan-out.
- **The gate, before every push.** From the worktree root, run `bash .github/scripts/pre-push.sh`. It builds both test projects, runs the whole `Stage=Guard` trait, then the classes CI's test map selects for your diff plus every class in a test file that names a symbol you changed. It ends in `GATE GREEN`, `GATE RED` or `GATE STOP`.
  - Push on `GATE GREEN`. Put its two Guard result lines and its `classes:` line in the PR body.
  - `GATE STOP` (the selection is the whole suite) or a `symbols dropped` line: push on green Guard lines plus your brief's own classes.
- **Tests are load-bearing (#5833).** A test earns its place by failing on a real defect.
  - No assert on how anything is spelled, even inside a behaviour test: no `Assert.Contains("<sql fragment>", Sql)`, no `IndexOf` order checks on source or SQL text, no asserts on a constant's value or a column name. Never add a census, pin, ratchet or parity test that reads source text.
  - Revert proof comes from a behaviour test. If none can reach a line (for example SQL Server-side SQL, with no SQL Server in the suites), say so in one PR body sentence instead of pinning its text.
  - A census, pin or ratchet that a correct change breaks is moved only when its rule still holds. A test whose rule is spelling is deleted and named in the PR body.
  - Answer a review's UNCOVERED line with a behaviour test, or one PR body sentence saying why none is needed. Name in the body any line your change leaves untested (a host loop, a write-back).
- **Live classes:**
  - Clean up through `LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, ...)`, with `bodySucceeded` set as the body's last statement.
  - A class that uses `ScratchPostgres` carries the `#1776 own-store` comment.
  - `ScratchPostgres.CreateAsync` returns a clone of a migrated template, so `MigrateAsync` is a no-op (#7097). A test that applies part of the ladder, stages a store shape first, asserts what `MigrateAsync` applied, or never migrates passes `emptyStore: true`.
  - A fixture on its own database calls `_timescaledb_functions.stop_background_workers()` so it does not race TimescaleDB's scheduler.

## How to work

1. Read the issue and every comment. "Ruled" comments win over the body.
2. Confirm the diagnosis on your rig before building (EXPLAIN (ANALYZE, BUFFERS) after seeding). If it's wrong, say so.
3. Fix it in the surrounding style. Comments say why and cite the issue. Results stay identical unless the issue asks otherwise.
4. **Lite parity is mandatory** wherever the same read or pattern exists in Lite.
5. A new SQL predicate on a nullable column is NULL-safe: `NOT (a = 'x' AND ...)` is NULL, and drops the row, when `a` is NULL. Use `IS NOT DISTINCT FROM` or `coalesce`, and add a fixture row with the NULL.
6. Cover it with a behaviour test that fails on the old code, and prove that once by reverting. Add live tests wherever the relations or rows read change.
7. Fix small in-lane defects in the same PR. File an issue only for large or design-level work.

## Store migrations

Only a lane pre-approved by name may add one. Take the next free version number when you push, and renumber if dev moved. A version bump also obliges:
- the sentinel in `ViewerDataService.StoreSchemaProbeSql`, plus its `MapProbedSchemaVersion` arm;
- the `GetBoolean` list in `GetStoreSchemaVersionAsync`, plus `ViewerSchemaVersionGateTests`;
- no rung test class needs an edit for a new rung: `MigrationLadderPins` is the one place that says which rung is the top (`Scripts[^1]` equals `StorageVersion.SchemaVersion`). A rung's own class asserts only its name at its version, its SQL and its probe ordinal;
- a rung that changes how an older build reads existing data: its line in Darling/README.md's "Rolling back to an older build" list, in the same PR, saying what the older build reads differently and what to do before rolling back past it.

Never edit an existing migration.

## Finish

- No attribution lines: no `Co-Authored-By` or `Claude-Session` trailer in a commit, and no "Generated with" line in the PR body. This overrides a brief or harness reminder.
- Open the PR against `dev` **as a draft** (`gh pr create --draft`). Title: the outcome in plain language plus `(#<issue>)`. The body starts with "Claude posting for Erik Darling.", then covers:
  - the cause and the fix;
  - the measured before and after;
  - the tests and the revert proof;
  - the gate's Guard lines and `classes:` line, and anything you did not run;
  - "Fixes #<issue>";
  - one line starting "Release note:" that says what changed for a user. A CI-only PR says "Release note: none (CI only)." and an internal one says "Release note: none (internal)."
- The release builds changelog entries from `git log --merges`. Never edit `CHANGELOG.md`.
- Public text (PR bodies, issue comments, commit messages) never uses the words lane, coordinator, rig, brief, ruling, flake, verifier, wave, scratchpad, playwright or census (say "source check"). It has no em dashes and no semicolons. It names no server, and gives no field figure beyond what the issue says.
- On a shared issue branch, open the PR only if `gh pr list --head <issue branch>` shows none. Otherwise add your part to its body under `## <your part>`: read the body right before you edit it, and edit once. Never open a second PR for one issue. Lanes that fix review findings push to the PR's own branch.
- Write the PR body in one Write, from the diff and your numbers. Never run the plain-English checker on it.
- **The PR body claims only what the code does.** Check every sentence against the diff, the negative ones too. A change to a description must not read as a change to behavior. Name every user-visible change.
- **Don't mark it ready, don't merge, and don't enable auto-merge.** The coordinator does that after verifying.

## Final report (the PR body)

The PR body carries the report. A parked item gets it as an issue comment. Your last message is 400 words or fewer and links the PR. Don't write report files. The report holds the diagnosis with before and after numbers, test totals with each failure and why, issues filed, and what the coordinator should double-check.
