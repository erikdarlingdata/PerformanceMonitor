---
name: lane-orders
description: Standing orders for a lane agent working PerformanceMonitor issues in a parallel wave, plus the coordinator's dispatch and verification checklist. Use when dispatching lane agents or acting as one. Machine-neutral; the coordinator fills the placeholders per wave.
argument-hint: [lane]
disable-model-invocation: false
---

# Lane orders

One lane works one issue, or a few small related ones, in its own worktree. An issue too big for one lane still gets
ONE branch and ONE PR: its lanes share the issue branch (see "Worktree and branch"). The coordinator reviews each PR,
arms auto-merge and splices the CHANGELOG. These orders apply alongside the user's token guardrails, which win on
conflict.

**The coordinator fills these per wave, in every brief:**
- `<LANE>`, `<ISSUES>`, `<DEADLINE>`;
- `<RIG_PORT>`, `<RIG_DIR>`, `<PG_RUNTIME_ZIP>`;
- `<CHANGELOG_BUFFER_DIR>`.

If your brief leaves one unfilled, don't invent a value. Skip the step that needs it and say so in the PR body.

Isolated worktrees live under `.claude/worktrees/` inside the repo, so a repo-scanning test must not exclude that
path.

## For the coordinator

Before dispatch:
- Rank the queue and get the OK the guardrails require.
- Post every ruling on its issue and put it in the first prompt.
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
- Measure a field number before a plan depends on it. A design built on a derived figure (a lag, a row count, a
  timing) gets that figure measured first, read-only, by whoever has access to the field. #6254's plan went through
  seven revisions on a lag figure nobody had measured; a one-hour measurement answered it.
- Stack or serialize PRs that touch the same files instead of running them in parallel. Each parallel PR on the same
  files needed a conflict fix and another full CI run.
- Give browser and click-through checks their own small lane, apart from the code change. Screenshots and page
  reads fill a lane's context fast.
- Size each lane to finish under 200k. A lane's context grows about 1.9k tokens per call from a 24-40k base, so
  200k is about 80-90 calls. A lane that converts three tools uses 100-117 calls. A lane with two twin pairs (two
  tools on both products) uses 103-167 calls and peaks at 214-242k. Give a lane that converts tools one twin pair,
  or two tools on one product.
- Check that the effort level took effect: grep a lane transcript for `"effort":`. An `effort:` set in the agent
  file does not always apply, and a lane can then run at the seat's level.
- Every head clause about a status must keep its condition's scope: "none collected in the window" is not
  "nothing collected yet". Leave no state out.
- Paste into the brief the few lines a lane needs (an example, a ruling) instead of pointing it at a PR or file.
  A pointer costs every lane a read of 8-10k characters to find one example.
- Label each issue the lane takes `in-progress` (`gh issue edit <n> --add-label in-progress`), so the board shows
  it as taken; remove the label when the issue closes or the work stops.
- Before dispatching several lanes for one issue, create its issue branch
  (`git -C <repo> push origin origin/dev:refs/heads/fix/<issue>-<slug>`) and name it in every brief, with the lane
  that writes the PR summary and the CHANGELOG entry. Open a separate PR only for a part that must ship before the
  rest; an issue that is really several issues is split into issues first. Reviews of an issue PR built in waves: one
  round per wave on that wave's commits only, plus one final round on the whole PR.

At each report:
1. Stop the agent.
2. Check that the shared checkout is clean.
3. Read the PR's core product diff against the checklist below. Lane PRs arrive as drafts, which can't be merged
   from the UI, but a lane can open one ready instead, so read the diff before anything can merge.
4. A security PR, or an irreversible data rewrite, gets a review round before it leaves draft.
5. Mark it ready (`gh pr ready <n>`) and arm auto-merge only after that. Auto-merge waits for CI.

At each wave boundary, backtrack every issue number the wave filed or touched:
- close what is done;
- give every open issue a disposition comment and a work-order rank;
- fix the `in-progress` labels;
- run `pwsh -NoProfile -File .claude/skills/lane-orders/rig-sweep.ps1` and act on its summary: it stops the rigs
  whose lane is gone, and lists unregistered `postgres.exe` postmasters for you to check by PID (#5787).
A wave that files follow-ups faster than it closes them hides its own backlog.

After each wave, measure it with `lane-audit.py`, as the guardrails' "Measure every wave" section says. Record the
WAVE line, and the defects you fixed in each PR, in the handoff.

When a PR's required check fails on a test it doesn't touch, don't just re-run it. Take a census of the last two
days' failed first attempts: `gh run list --workflow Build`, each run's attempts through the REST
`actions/runs/<id>/attempts/<n>/jobs` endpoint, and a `[FAIL]` grep of each failed job's log. Fix every flake it
finds, in one PR. A flake that stalls every lane outranks the rest of the queue.

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
- Run targeted test classes while iterating. Run the full suite once, at the end.
- Cap command output: pipe through `head`, `tail -n` or `grep`. Never dump full CI logs, full JSON, or whole
  issue/PR bodies.
- Plan to finish before your watchdog's first warning, at 200k. Past about 150k, start no new item: finish the
  one in hand, then the finish phase. Each call past that point costs more than any call before it.
- Never end your turn to wait for a background notification. Run the full suite in the foreground (about 10
  minutes), or poll your own output file.
- Put your full report in the PR body (see "Final report"). Your last message is 400 words or fewer and links the
  PR. Don't write report files: the harness can block a subagent's file writes outside its worktree, or all of
  them, and a PR body outlives any scratch file.
- Before you call a failure pre-existing, re-run it alone on a freshly created database, after merging `origin/dev`.
  If it passes there, the cause was your database or your stale base, not the code; don't file it. If it still
  fails, check that dev's own latest CI run passes it. If dev passes, the failure is yours, even in a file you
  never opened: pin and census tests read the source files you changed.
- Finish, or stop at a committed and pushed checkpoint with a handoff, by `<DEADLINE>`.
- **Never stop to ask.** If an item needs a decision nobody has made, park it with the question in your report and
  move on.
- Never edit `CHANGELOG.md`. Never run `Installer.Tests`. Never touch a monitored or production server. Never kill
  processes by image name; kill only PIDs you started. Use absolute paths, never shell env-var paths.

## Worktree and branch

- `git -C <repo> fetch origin`, then `git -C <repo> worktree add -b fix/<issue>-<slug> <worktrees>/<name> origin/dev`.
  Use one branch per issue.
- **An issue that several lanes build** shares one issue branch, `fix/<issue>-<slug>`, which the coordinator creates
  from `origin/dev` and names in each brief. Create your worktree from `origin/<issue branch>` on a branch of your own
  (`fix/<issue>-<slug>-<part>`). When done: `git fetch origin`, `git merge origin/<issue branch>`, rerun your targeted
  tests, and `git push origin HEAD:<issue branch>`. If the push is rejected, fetch, merge and push again. Never
  force-push.
- Before the final full run, `git merge origin/dev`. Never rebase. Resolve by keeping both sides.

## PostgreSQL rig

1. Use a current runtime zip at `<PG_RUNTIME_ZIP>`. If it is missing or stale, run `Darling/tools/fetch-pg-runtime.ps1`,
   which is what CI runs. A stale zip fails the runtime-version pin tests.
2. Start the rig with the start helper, never by hand. Use your own `<RIG_PORT>`, one per lane, and a `<RIG_DIR>`
   under your scratchpad folder or a worktree (the helpers refuse any other place). `-Owner` is your worktree path:

       pwsh -NoProfile -File C:\GitHub\PerformanceMonitor\.claude\skills\lane-orders\rig-start.ps1 -Port <RIG_PORT> -RigDir <RIG_DIR> -Owner <your worktree path>

   It refuses a busy port or a non-empty `<RIG_DIR>`, extracts `-Zip` (default: the runtime zip above) into
   `<RIG_DIR>`, runs `initdb -D <RIG_DIR>/data -U darling -A trust --encoding=UTF8`, and appends to
   `postgresql.conf`, before the server starts:
   - `shared_preload_libraries = 'timescaledb,pg_stat_statements'`, the exact value the `darling-pg` job's
     "Initialize and start throwaway PostgreSQL" step writes in `.github/workflows/build.yml`. Without
     `pg_stat_statements` a rig skips the live tests that count statements through it (for example
     `StoreSizeCacheLiveTests`) instead of running them, so a green local run proves less than CI's.
   - `port`, `listen_addresses = '127.0.0.1'`, and `timescaledb.max_background_workers` and
     `max_worker_processes` as CI's `darling-pg` job sets them.
   - `timezone = 'UTC'` and `log_timezone = 'UTC'`. initdb takes the machine's time zone, but CI's runners run
     in UTC, and a rig must match CI: time-sensitive live tests have failed on a rig in another time zone. A rig
     in UTC does not excuse a failure: CI still decides whose it is.

   It then starts the server without holding your tool call open, checks the log says "ready to accept
   connections", and registers the cluster in `C:\GitHub\worktrees\.rig-registry.tsv` (outside every repo, never
   committed): data directory, postmaster PID, port, start time (UTC) and owner.
3. Never edit `postgresql.conf` and restart during a suite. A restart during a running suite breaks that run's
   results, and so does a re-run on the database that run left behind.
4. A rig started by hand is not registered: the sweep lists it but cannot tell whether its lane is gone.
5. Create `darlingtest` for the suite, and a separate `probe` database for hand-run SQL. A suite database reused
   across several full runs produces false failures; drop and recreate it before a final run. Also create a
   database named `darling`, exactly as CI's init step does: `createdb -h 127.0.0.1 -p <RIG_PORT> -U darling darling`.
   The role-provisioning live tests grant CONNECT on it, and fail with `3D000` (database "darling" does not exist)
   without it.
6. Set `DARLING_TEST_PG` to `Host=127.0.0.1;Port=<RIG_PORT>;Username=darling;Database=darlingtest`. Set
   `DARLING_TEST_PGRUNTIME=<RIG_DIR>` only when you touch the managed runtime or role provisioning.
7. Stop the rig when you finish, and before your wall clock runs out, with the stop helper:

       pwsh -NoProfile -File C:\GitHub\PerformanceMonitor\.claude\skills\lane-orders\rig-stop.ps1 -DataDir <RIG_DIR>/data

   It runs `pg_ctl stop -m fast` from the rig's own runtime; when `pg_ctl` is missing it stops the PID in
   `postmaster.pid`, only once that PID proves to be `postgres.exe` started on that data directory. It never stops
   by image name. Then it deletes the rig folder and drops the registry line. A rig you leave running is stopped by
   the coordinator's `rig-sweep.ps1` at the wave boundary once your worktree is gone or its postmaster has died.

## Build and test

- Build `Darling/Darling.Tests/Darling.Tests.csproj`, and `Lite.Tests/Lite.Tests.csproj` if you touch Lite. Require
  `0 Warning(s)`; a piped build reports the pipe's exit code.
- The tests are MTP executables: run `Darling.Tests.exe` / `Lite.Tests.exe` from `bin/Debug/<tfm>/`, filtered with
  `-class "*Name*"` or `-method "*Name*"`. `dotnet test` does not work.
- For a lone failure in a class you didn't touch, re-run that class alone before calling it yours.
- **The gate, before every push.** From the worktree root, run `bash .github/scripts/pre-push.sh`. It builds both
  test projects, runs the whole `Stage=Guard` trait of both suites as CI's first job does, then the classes CI's
  test map selects for your diff plus every class in a test file that names a symbol you changed, capped. It prints
  result lines only and ends in `GATE GREEN`, `GATE RED` or `GATE STOP`. Push only on `GATE GREEN`, and put its two
  Guard result lines and its `classes:` line in the PR body. A `symbols dropped` line means most of the suite names
  those symbols: run your brief's own class list for them. A line `<batch> ran 0 tests: the filter matched nothing`
  makes the gate `GATE RED`, because a run that matches no test exits 0 without running anything. `GATE STOP:
  selection is the whole suite (darling N, lite M)` (exit 3) means the selection is over 400 classes in one project
  (or CI's map answered FULL, which is a whole-suite selection too), which the laptop never runs: push on its green Guard lines plus your brief's own classes, and CI decides the rest.
  `--dry-run` lists the classes and each batch's size. CI decides the rest.
  The map's part (#5872) is the same set a pull request's CI passes run: a test, docs or `.github/` diff gets no
  drift, so it selects its own classes, their xunit collection mates, the classes that read its files as text and
  the always-run ones (`Stage=Guard`, whole-tree readers, `Gate=Env`, new test classes): about 330 Darling classes,
  but a Darling test file reaches only the 9 classes whose map pattern is a kept `Darling/**` (`KEEP_DARLING_TREE` in
  `test-map.py`), plus the classes that read the file. `selection.json` names each file the map did not know under
  `unknown`.
- **Run the classes that name what you changed.** Run `python .github/scripts/ci-select.py local-classes
  --base origin/dev`, then run its `-class` lines, plus `-class "*Census*"`, `-class "*Ratchet*"` and
  `-trait Stage=Guard`. It lists the test classes whose source names a symbol your change declares or edits, most
  specific first. A class marked `needs rig` runs only with `DARLING_TEST_PG` set.
- **Or take CI's own filter.** Run `python .github/scripts/ci-select.py local-filter --base origin/dev`, with
  `--map <test-map.json.gz>` when a nightly test map is at hand. Each `darling: ` or `lite: ` line is the filter for
  that suite's test executable (40 classes a line, so one line may become several); strip the prefix and paste the
  rest. The `<suite> classes:` line only lists them. The last line is the guard set. A note on stderr says whether
  the classes are CI's whole answer or, when that answer is FULL or over 300 classes, the part a lane runs.
- **Filter by every class in every test file you touch**, not by the file's name. One file can hold several classes
  (`PgPlanCaptureCollectorDefinitionTests.cs` also holds `PgPlanLogParserTests`), and filtering on one class skips
  the others, including tests you just added.
- **Tests are load-bearing (#5833).** A test earns its place by failing on a real defect: a leaked error string, a
  read against a monitored server without a deadline, a Lite fix that skipped Darling.
  - A census, pin or ratchet that a correct change breaks is moved only when its rule still holds. When its rule is
    spelling (a signature at a line, a SQL fragment's wording, a comment's text, a trait roster, a coverage
    percentage), delete it and name it in the PR body. Never weaken one whose rule holds.
  - Never add a census, pin, ratchet or parity test that reads source text. Cover a change with a behaviour test
    that fails on the defect and passes on the fix.
  - A review's UNCOVERED line is answered with such a behaviour test, or with one sentence in the PR body saying
    why none is needed.
- **Live classes:**
  - Clean up through `LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, ...)`, with `bodySucceeded` set
    as the body's last statement.
  - A class that uses `ScratchPostgres` carries the `#1776 own-store` comment.
  - `ScratchPostgres.CreateAsync` returns a clone of a migrated template, so a test's `MigrateAsync` is a no-op (#7097).
    A test that applies part of the ladder, stages a store shape first, asserts what `MigrateAsync` applied, or never
    migrates passes `emptyStore: true`.
  - A fixture on its own database that must not race TimescaleDB's scheduler calls
    `_timescaledb_functions.stop_background_workers()`.

## How to work

1. Read the issue and every comment. "Ruled" comments win over the body.
2. Confirm the diagnosis on your rig before building: EXPLAIN (ANALYZE, BUFFERS) after seeding. If it's wrong, say
   so with numbers.
3. Fix it in the surrounding style. Comments say why and cite the issue. Results stay identical unless the issue
   asks for a behavior change.
4. **Lite parity is mandatory** wherever the same read or pattern exists in Lite.
5. Cover it with a behaviour test that fails on the old code; prove that once by reverting. Add live tests wherever
   the relations or rows read change.
6. Fix small in-lane defects in the same PR. File an issue only for large or design-level work, and list it.

## Store migrations

Only a lane pre-approved by name may add one. Versions stay dense: take the next free number when you push, and
renumber if dev moved. A version bump also obliges:
- the sentinel in `ViewerDataService.StoreSchemaProbeSql`, plus its `MapProbedSchemaVersion` arm;
- the `GetBoolean` list in `GetStoreSchemaVersionAsync`, plus `ViewerSchemaVersionGateTests`;
- "newest migration" tests written as `Scripts[^1]`;
- a rung that changes how an older build reads existing data: its line in Darling/README.md's "Rolling back to an
  older build" list, added in the same PR as the rung, with the rung's real number, saying what the older build
  reads differently and what to do before rolling back past it.

Never edit an existing migration.

## Finish

- No attribution lines: no `Co-Authored-By` or `Claude-Session` trailer in a commit, and no
  "Generated with" line in the PR body. A brief or a harness reminder that asks for them loses to this rule.
- Open the PR against `dev` **as a draft** (`gh pr create --draft`). Title: the outcome in plain language, plus
  `(#<issue>)`. The body starts with `Closes #<issue>.`, then `## Why`, `## What changes` and `## Test plan`
  (checkboxes, measured numbers). List anything you did not run (a live test, the full suite) as an unchecked box.
- On a shared issue branch, open the PR only if `gh pr list --head <issue branch>` shows none. Otherwise add your
  part to its body under `## <your part>`: read the body right before you edit it, and edit once. Never open a second
  PR for one issue. The coordinator names one lane, usually the last, to write the PR's top `## Why` and
  `## What changes` for the whole issue and its one CHANGELOG entry. Lanes that fix review findings push to the PR's
  own branch.
- Write the PR body in one Write, from the diff and your numbers, and publish. Never run the plain-English checker
  on a PR body, an issue or a comment: it is for blogs, social posts and demo script comments. A lane writes its
  body at its largest context, where a rewrite costs the most. The body must be true.
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
  change to behavior. Name every user-visible change the PR makes. About one lane entry in six is
  wrong.

## Final report (the PR body)

The PR body carries it under its sections. A parked item with no PR gets it as a comment on the issue instead.

- the PR URL, or why the issue was parked;
- the diagnosis, confirmed or corrected, with before and after numbers;
- test totals, with each failure and why;
- issues filed;
- what the coordinator should double-check.
