# Review traps

The defect shapes this repository keeps producing, one line each, with the pull request or issue that
bit. The Claude review (`.github/workflows/claude-review.yml`, #3710) reads this file before it reads
the PR body and must say, for every trap the diff is near, whether it applies here and why — a trap
that does not apply is stated as not applying, with the line that proves it. Humans reviewing by hand
should do the same.

Every line names the number that verified it; a line without a number is a claim, not a trap, and
does not belong here. The bar for adding a line: the shape has bitten at least once in a merged PR or
a filed issue, and a diff reader who knew the shape would have caught it. Group by the layer the
reviewer is reading. Keep each line one line — the review quotes them.

## SQL (T-SQL and PostgreSQL)

- `ORDER BY 3 + 4 DESC` is not "ordinal 3 plus ordinal 4": a bare integer in `ORDER BY` is an output-column ordinal, but `3 + 4` is an expression and folds to the constant 7, which sorts nothing — `DarlingPgKernelStatsReader` returned the alphabetically-first series as "hottest" through a dozen green string assertions (#3613; `EXPLAIN` showed the tiebreak as the only sort key). Sort by named columns.
- `MAX(max_dop)` grouped by `query_hash` is the highest DOP ANY plan for that statement text ever ran at since it was cached — `sys.dm_exec_query_stats.max_dop` is a per-plan cumulative maximum, so a MAXDOP-1 instance reported 16 from a plan compiled before the pin (#3648, fixed in #3651). An aggregate across plans needs provenance (which plan, when) or the newest plan's reading.
- Integer division in a rate column: `cntr_value_per_second` as `delta / seconds` over two integer columns truncates every rate under 1/s to 0 — the frozen Dashboard's read (#3658) and the Full-edition install scripts (`install/02`, `06`, `47`; #3653 Q9, approved, not yet landed). Any `a / b` over integer columns is suspect until a `1.0 *` or a CAST is found.
- A percent whose denominator is the PAGE: `share = row / SUM(rows on this page)` sums to 100% of whatever `LIMIT` admitted, not of the window — #3613 moved every denominator to a window aggregate (`SUM(SUM(x)) OVER ()`) on the same statement. Ask what a share is "of".
- `CASE WHEN interval > 0 THEN delta / interval ELSE 0 END` spells "unknowable" as 0: a restart's fabricated zero or a missing interval becomes a measured 0.00 rate — fifteen arms across both SKUs (#3707). End at `END` (NULL) and carry the NULL to the wire.
- Truncation inferred from a full page: `truncated = rows.Count >= limit` says "more" when the page is exactly `limit` long and "that was all" is equally true — #3594 named it, #3679 fixed four PostgreSQL pages, #3699 three more plus a `Take(limit)` over a read already cut at `limit`, binding them through `McpHelpers.BoundPage`. Fetch `limit + 1`; the flag is `fetched.Count > limit`.
- A count taken off the page and named as the population's: `non_default_count = rows.Count(!IsDefault)` over the rows fetched reported the `limit` as a fact about the snapshot (#3679). The population's count belongs on the statement above the `LIMIT`; the page's count is `*_returned`.
- `pg_relation_size` is the heap alone; TOAST and indexes live elsewhere, and a dimension's heap is a few pages while its bytes ARE its TOAST (#3610's inventory says so at the table arm). Size a relation with `pg_total_relation_size`, or a reconciliation against `pg_database_size` will not close.
- `timescaledb_information.hypertables` never lists a continuous aggregate's materialization (`… AND ca.mat_hypertable_id IS NULL` on 2.28.1): an inventory walking that view was blind to 57% of a 415 GiB store (#3582 → #3610). Enumerate `continuous_aggregates` and size each through `hypertable_detailed_size` on its materialization.
- `chunk_compression_stats` returns ZERO rows for a hypertable whose compression is off, so a chunk count over it reads 0 (#3610, measured on a two-chunk materialization). Count chunks from `timescaledb_information.chunks`.
- `timescaledb_information.job_history` is ownership-filtered (`pg_has_role(current_user, owner, 'MEMBER')`) while `jobs` and `job_stats` have no `WHERE` at all — a role that sees all 110 jobs and zero history rows concludes recording is broken (#3585; a real postmortem declared rows unknowable while they sat in the table). A read of that view must state whose eyes it has.
- A persistent `next_start = -infinity` on TimescaleDB 2.26.4+ is a CRASH BACKOFF the scheduler clears itself (`consecutive_crashes > 0`), not "will never run again"; `alter_job(next_start => now())` on such a row RESETS the backoff, pushes the retry out by up to an hour and re-times every sibling job (#3629, measured on 2.28.1). Below 2.26.4 the old reading is correct. Version-gate the sentence and the re-arm.
- A first-level continuous aggregate's materialization is chunked at ten times the raw interval unless `set_chunk_time_interval` says otherwise, so retention and compression stop ten days short of the design (#3620 → #3621).
- `timescaledb_information.jobs` reports a policy on an aggregate under the aggregate's VIEW name (`COALESCE(ca.user_view_schema, ht.schema_name)`), not the materialization's (#3610). Join policies to aggregates on the view name.

## Collectors and deltas

- A delta family that stores no interval cannot tell a restart's zero from a quiet interval: `0.00 ms/sec` was the fabricated shape on every restart until the four naked families stored `sample_interval_seconds` (#3595) and the baselines stopped counting a restart's zero as a quiet sample (#3698). A NEW delta column needs an interval beside it and `IS DISTINCT FROM 0` in the aggregates that roll it up.
- A counter differenced against the OLD instance: after a restart, failover or re-point, the first pass subtracts the new instance's counters from the dead one's — five families did it once each (#3705; identity epochs in #3694). A delta calculator needs an identity (`sqlserver_start_time` + `@@SERVERNAME`, or `pg_postmaster_start_time()`) observed BEFORE its first subtraction.
- A gauge differenced like a counter: `Total Server Memory (KB)` falling read as a counter reset (the `(0, 0)` marker) because `cntr_type` was never selected (#3708, Darling V132 / Lite v62). Classify by the stored type; the `/sec` name proxy is only the pre-rung fallback.
- The first point of a LAG-differenced trend is unrated, not 0: a fabricated 0.0 at the window's head reads as a cliff (#3642). Likewise `IsDBNull ? 0`: an unmeasured pass is null with a `*_measured` flag beside it, not 0 MB (#3682).
- A rate divided by a NOMINAL span when the collector observed less of it is wrong in the direction of "quieter": a quiet hour halved the next hour's Query Store rate (#3695); analysis facts divide by observed time, and a window with a hole says so (#3592).
- A coverage gate written before any collector stamped coverage: `facts.Count == 0 || ObservedDurationMs <= 0` made an observed-and-quiet window wear the dead-collector envelope (#3687). Gate on the witness, not the fact count.
- An empty analysis window is "unavailable", not an all-clear (#3553); a server nothing has banded yet is Unknown, not Healthy (#3635).

## Analysis (facts, scorers, graph, stories)

- `FactScorer.ScoreAll` skips amplifiers when `BaseSeverity <= 0` (`FactScorer.cs:100`): "base 0, let the co-fire lift it" is mechanically impossible, and two `pg_config` facts were inert by construction for exactly this (#3688). A fact that should fire only on evidence needs the COLLECTOR to stamp the evidence onto it and the scorer arm to return the base off that stamp.
- A non-virtual method called through a base reference cannot be overridden: `InferenceEngine` holds a `RelationshipGraph` and calls `GetActiveEdges` on it, so a subclass's `new` method never dispatches — the one-word `virtual` seam was the fix (#3688). Check the static type at the call site, not the subclass.
- `InferenceEngine.BuildStories` consumes facts at or above 0.5 as their own roots BEFORE lower-severity roots walk — a test of an edge into a leaf must keep the leaf below 0.5 or the traversal drops it as consumed (existing SQL Server semantics, relearned on #3688).
- `FactAdvice.PopulateStoryText` runs BEFORE `ClusterIntoIncidents` / `StampClusters` / `Reconcile` (steps 3.5 → 3.6 → 3.7 in both SKUs' services), so a sentence added after folding must re-read the frozen `StoryText` blob through `TryReadStoryText` / `SerializeForStoryText` (#3709).
- The notification filters `Severity >= threshold` BEFORE grouping by `IncidentId`: a 0.5 story sharing an incident with a 1.0 story never reaches the e-mail, so its words must ride the leading story's own text (#3709).
- Wait facts are keyed on the raw `wait_type`, and `FactCollectorHelpers.IsGeneralLockWait` folds `LCK_M_SCH_M` into `LCK` — no collector produces a fact keyed `SCH_M`, so every SCH_M arm (scorer ramp, the #3632 edge, advice, the #3709 fold set) is dormant against a real store (#3709, reported and not fixed there). A key in the graph is not a key in the data; grep the collector that emits it.
- The engine emits ONE `ANOMALY_WAIT_PROFILE` per run — a fixture with four maintenance-family anomalies is a unit-level shape; a real run has at most two (the profile and `ANOMALY_WRITE_LATENCY`) (#3709).
- `confidence = (path.Count - 1) / path.Count` measured path LENGTH, so a lone uncorroborated fact carried the highest confidence the engine could express (#3632). Confidence is corroboration; severity is impact; they are not multiplied.
- A threshold that means different units at different surfaces: the File Growth bar meant "per five minutes" on one store and "per day" on another until it meant megabytes per hour everywhere (#3631). State the unit at every surface that shows the number.

## Alerting and notifications

- An alert evaluated every ~30 s against an observation that changes once per COLLECTION re-fires on every cooldown expiry against the same rows: Forced Plan Failing fired six times on one 04:02→04:18 rise (#3579 → #3628), Database File Growth up to twelve cards per hourly observation (#3636). The engine must remember the observation stamp (`collection_time`) it last fired on, per key, beside — never folded into — its last-seen stamp.
- An alert that fires on ONE breaching sample and resolves on the next: tempdb Space paged 22 times about a tempdb never in trouble for longer than 206 s (#3692). Presence-shaped conditions sit behind `AlertPersistenceGate` at K consecutive collections.
- "Did I send" is not "was it delivered": a once-a-day gate in process memory re-announces on every restart and never retries a failed delivery (#3633). Gate on a delivery stamp in the store.
- Slack: the payload is ONE attachment, Slack caps it at 50 blocks, and the Details path spends about two blocks per detail (divider + section), so 19 details breach regardless of text length — three five-fact pages died `invalid_attachments` while a 2,835-character body delivered (#3612 → #3618). Budget blocks, not characters, and state the omission.
- Slack `fields` render two-across in submission order: seven fields per query interleave the queries (#3644). One section block per record.
- A text cut at a UTF-16 index can land between the halves of a surrogate pair; `System.Text.Json` then substitutes U+FFFD (it does NOT reject the payload, contrary to the filed hypothesis) and the reader sees a replacement glyph where an emoji was (#3625). Cut on `StringInfo` text elements and count omitted characters in the same unit.
- A default interface member compiles a parity gap away: `DeliverAndReportAsync` with a default body left Lite's deliverer and fourteen fakes inheriting an answer nobody wrote (#3640). CONTRIBUTING's two-store parity rule names `IAlertDeliverer`; its members are required.
- A history row wearing the colour its alert's NAME implies rather than the tier it fired at (#3635; the PostgreSQL half in #3680).

## Storage and MCP payloads

- A JSON number above 2^53 is rounded by every JavaScript client: tick-scale sweep ids emitted as numbers reached the web drill-down mangled and found nothing (#3487). Emit 64-bit ids as strings at the presentation layer.
- A `total_*` assigned from a `.Count` is a count of the page unless the set is provably whole (#3699 rosters the ten sites that prove it). Otherwise it is `*_returned`.
- A "latest snapshot" read anchored on `= (SELECT MAX(collection_time) …)` must project the anchor so the payload can say WHEN, and a tool that accepts `hours_back` must read it (#3637).
- A retention-tiered store answers a 7-day question from a 4-day raw tier and labels the missing days quiet unless the read routes by tier and says which tier served it (#3590; the viewer's charts in #3684).
- Two copies of one definition pinned EQUAL to each other fail only after BOTH have drifted (#3684). Alias, do not copy.
- Every MCP filter is part of the query, not a post-filter on the page: `parallel_only` / `min_dop` / `blocking_only` cut before the `LIMIT` (#3641).
- `hours_back`, `limit`, `top`, `days_back` are refused when out of range, never clamped — a clamp is a silent lie about the window (#3699's census pins zero clamps across 156 methods; a negative window is refused, #3641).

## Tests and censuses

- A census fires on any NEW string constant in `PgTargetFactKeys`: `PgTargetMcpSurfaceTests.EveryPgRecommendation_NamesARegisteredTool_…` requires a `PgTargetToolRecommendations.ByFactKey` row for every key, and `PgTargetKnobsTests`' "other lanes' keys fall to 0" pin grades every key (#3688). A non-key alias is excluded by predicate, with the reason inline.
- `InternalsVisibleTo` on PerformanceMonitor.Analysis names Lite.Tests only — a helper Darling.Tests must call has to be public (#3688).
- A pin that encodes the lie: `ACappedResult_WithholdsTheUnsatisfiedSummary_…` handed the builder two facets at `limit = 2` and asserted `truncated` — the `>= limit` inference as its own proof (#3699). Ask what a pin would fail ON.
- A roster-based census does not see the file it does not roster: `EveryPagedTool_ObservesTruncation_…` walked ten tools and missed three in another file (#3699). An anti-pattern census sweeps the class (every tool body); rosters are for the positive contract only.
- A test that reads the clock twice measures the CI scheduler: the delta-seeder tests failed at 123 and 243 on unrelated PRs (#3676). One anchor owns both ends of an interval.
- An expression-bodied member holding a `{ }` property pattern is read SHORT by the T-SQL convention guard's member walk (#3667; the #3607 precedent) — a member the walk cannot read whole is one no census can see into.
- A test that constructs a "four maintenance anomalies in one run" fixture is testing a shape no live run produces (#3709, the one-profile-per-run line above). Fixtures should carry the population the engine actually emits, or say they do not.

## Workflows and CI

- dorny/paths-filter v4 evaluates each pattern as an INDEPENDENT predicate under `predicate-quantifier: some`: a bare `!**/*.md` line is not a subtraction, it is its own rule ("any non-markdown file anywhere") and made every area filter true for every change (#1714, run 30219202642; #3116). Keep the carve-out INSIDE the include: `dir/**/!(*.md)`.
- Under `set -euo pipefail`, a bare `$(gh api …)` assignment that fails kills the step with one context-free stderr line — three transient 404/503s read as deterministic hard reds in one afternoon (#2309). Every lookup goes through a wrapper that names the endpoint and decides whether a failure is a verdict.
- `gh api --paginate --jq` runs the filter once PER PAGE: a `max_by` or `last` is per-page, not global; the newest row is the last LINE of the emitted stream (claude-review-guard.yml's verdict scan).
- A summary helper reading `"$1"` truncates every message whose `echo` wraps with a trailing backslash — the wrapped pieces are separate arguments; read `"$*"` (claude-review-guard.yml).
- `claude-code-action` refuses to run when `.github/workflows/claude-review.yml` differs from the DEFAULT branch's copy, exits SUCCESS in seconds and sets no output (#2229 cause 1); a PR that edits that file cannot be Claude-reviewed until it merges, and every dev PR after it goes unreviewed until the next release sync (#3657's pending-release arm).

## Repository mechanics

- `.gitattributes` says `text=auto eol=crlf`: a Python `open().read()` / `write()` round-trip converts the WHOLE file's endings, and the working-copy diff shows every line changed while git's index diff is clean. A diff that touches every line of a file is a line-ending conversion until proven otherwise; fix the working copy with `perl -pi -e 's/\r?\n/\r\n/'`.
- Lite and Darling ship in parity (CONTRIBUTING's two-store rule): a fix to one SKU's reader, tool, alert or description without the counterpart's is a finding, not a nit — the census in `McpPageContractTests` compares the two SKUs' payload keys by tool name for exactly this (#3594).
