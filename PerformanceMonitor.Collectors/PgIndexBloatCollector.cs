/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// B-tree index bloat, MEASURED rather than estimated — <c>pgstatindex</c> from pgstattuple (#2561).
///
/// <para><b>Why exact and not an estimator.</b> #2561 proposed porting the ioguix btree estimator, which
/// derives an expected page count from <c>pg_stats</c> column widths. Measured, that route does not work for
/// the role this product runs as: <c>pg_stats</c> returns ZERO rows to a <c>pg_monitor</c>-only login,
/// because the view filters on <c>has_column_privilege</c> — the same trap that produced #2542's 88.59%
/// reported against a true 0.50%. Meanwhile <c>pgstatindex</c> DOES run for <c>pg_monitor</c> (verified),
/// because pgstattuple grants EXECUTE to <c>pg_stat_scan_tables</c>, which <c>pg_monitor</c> includes. So
/// under exactly the permissions we have, the exact function works and the estimator is blind.</para>
///
/// <para><b>The density is stored raw and NEVER converted to a bloat percentage.</b>
/// <c>100 - avg_leaf_density</c> is about 10% on a PERFECT index: measured across seven freshly-built
/// indexes, density landed between 89.98 and 91.48, and after <c>REINDEX</c> between 87.07 and 90.81. There
/// is no constant to subtract, so any stored "bloat %" would bake in a false floor that varies per index.
/// The server's own numbers are kept and the interpretation is left to the read, which is the same rule the
/// plan-readiness collector follows for GUC text.</para>
///
/// <para><b>Non-btree indexes must be excluded before the function is called, not filtered afterwards.</b>
/// <c>pgstatindex</c> RAISES on anything else — verified on GIN, BRIN and hash, all
/// <c>relation "x" is not a btree index</c> — so a single GIN index would take the whole collection down
/// every cycle. The btree filter sits behind an <c>OFFSET 0</c> optimisation fence: in testing the planner
/// applied a plain <c>WHERE</c> first and the naive form worked, but correctness here should not depend on
/// plan shape when the failure is total.</para>
///
/// <para><b>Every bound on work selects what the function is applied TO.</b> Same category as the btree
/// filter above, and the reason the whole query is shaped the way it is. A bound written as a qual on a
/// <c>LEFT JOIN LATERAL</c> to the function does not bound anything: the planner cannot skip an inner side
/// it has not evaluated, so the function runs once per candidate row and the qual only chooses whether to
/// keep the answer. The result is a statement that reads the entire instance while every
/// <c>skipped_reason</c> in its output is correct, which is indistinguishable from a working collector in
/// anything except whether it ever returns. So the size ceiling and both work budgets select the
/// <c>in_budget</c> relation, and the measurements are joined back afterwards.</para>
///
/// <para><b>Nothing is silently skipped.</b> The cost of this function is a full read of the index —
/// measured at exactly <c>relpages</c> blocks, 62,840 for a 491 MB index — so very large indexes are passed
/// over rather than read daily. They still get a ROW, with the measurements NULL and
/// <c>skipped_reason</c> populated, because a size cap that made indexes disappear would read as "no bloat
/// here" on precisely the biggest ones.</para>
///
/// <para><b>The measured set ROTATES, and that is what makes the deferral in a
/// <c>skipped_reason</c> true.</b> The cycle budget admits roughly one near-ceiling index, and largest-first
/// ordering picks the same one every time — so a stateless version of this collector measured index #1
/// forever while stamping the other 2,456 with "not measured this cycle (work budget)", a claim of DEFERRAL
/// that no mechanism could honour (#3153). A per-database cursor
/// (<see cref="RotationCursorKeyPrefix"/>) records where the last cycle stopped; the next cycle resumes
/// strictly below it and wraps to the largest index once nothing measurable is left beneath it. Largest-first
/// is kept, because it is the right PRIOR for one cycle — the median index on the first production target is
/// 216 kB and 70% of the census is under 1 MB, so smallest-first would maximise a row count over objects
/// whose bloat is worth kilobytes. What was wrong was repeating a prior with no memory, which turns it into a
/// permanent selection of one index.</para>
///
/// <para><b>This collector's census is the COMPLETE btree population — no size floor</b> — which is why it
/// returns MORE rows than <see cref="PgIndexUsageStatsCollector"/> for the same target on the same day
/// (2,500 against 1,517 measured on the first production target, a 65% difference; #3158). That collector
/// applies a deliberate 64 kB floor and is the REPORTABLE SUBSET; this one applies none, because a bloat
/// census has to be complete to be a census. Both are correct for their own purpose and the difference is
/// entirely that one floor: every row in the gap was under 64 kB. The counter-intuitive direction — the
/// btree-only collector returning more rows than the all-access-method one — is a red herring: every index
/// on that target is a btree, so the access-method filter costs this collector nothing there.</para>
///
/// <para>Primaries only. A standby's index files are byte-identical to the primary's by replication, so
/// measuring both spends the same full-index read twice for one answer.</para>
/// </summary>
public sealed class PgIndexBloatCollector : PostgresCollectorDefinitionBase<PgIndexBloatCollector.Row>
{
    public static PgIndexBloatCollector Instance { get; } = new();

    private PgIndexBloatCollector()
    {
    }

    /// <summary>
    /// Indexes at or above this many bytes are recorded but NEVER measured — permanently, not this cycle.
    /// The read is proportional to index size, so this bounds what any single index can cost.
    ///
    /// <para><b>It is the DEADLINE that sets this figure, and that only became visible once the block rate
    /// was measured (#3164).</b> One index just under this ceiling is the largest amount of work a single
    /// statement can be asked to do, so the ceiling has to fit the deadline on its own — at
    /// <see cref="MeasuredBlocksPerSecond"/>, 1 GiB is 131,072 blocks and about 129 s, inside the 150 s
    /// that leaves half of <see cref="CommandTimeoutSecondsOverride"/> for everything else.
    /// <c>ThePerIndexCeiling_FitsTheDeadline_OnItsOwn</c> asserts that directly rather than leaving it to
    /// be inherited from the lockstep below, because the lockstep is exactly what a future decoupling
    /// would remove. At the measured rate a 2 GiB ceiling is 259 s in one statement — past the whole
    /// allowance on a single index — which is why this ceiling could not stay where it was whatever the
    /// cycle budget did.</para>
    ///
    /// <para>It moves in lockstep with <see cref="CycleMeasureBudgetBytes"/>, because the cycle budget may
    /// never sit below it: the band between the two would be indexes that are legitimate candidates,
    /// earn no "too large" reason, and yet exceed the whole cycle on their own first row every run.
    /// Lowering one without the other manufactures exactly that band — and since #3153 that band would
    /// stall the ROTATION CURSOR rather than merely mislabel one index, which is a strictly worse
    /// failure. See <see cref="CycleMeasureBudgetBytes"/> for the argument in full.</para>
    ///
    /// <para><b>Over-ceiling indexes are a terminal state, and the row says so.</b> Measured on the first
    /// production target AT A 2 GiB CEILING, 43 indexes sat above it holding 311 GB — 68% of that
    /// instance's index footprint, mean 7.4 GB, largest 27 GB. That census belongs to the ceiling it was
    /// taken at: this ceiling is lower, so the terminal set is LARGER by however many indexes fall between
    /// the two, and that count has not been measured — the store holding that target was not reachable for
    /// #3164. What is measured is the direction and the mechanism, not a new count. At
    /// <see cref="MeasuredBlocksPerSecond"/> the 311 GB set is about 11 hours of reading and its largest
    /// member alone is roughly an hour in one statement, so no per-statement deadline this product could
    /// plausibly set reaches them: <c>pgstatindex</c> is the wrong instrument for them rather than a
    /// mis-tuned one. Raising the ceiling does not help, and an estimator is not available —
    /// <c>pg_stats</c> returns zero rows to a <c>pg_monitor</c>-only login (see the type header). So their
    /// reason states PERMANENCE instead of implying a deferral, and points at what does cover them:
    /// <see cref="PgIndexUsageStatsCollector"/> runs on the same target at the same daily cadence with the
    /// same retention, needs no extension, records <c>index_bytes</c> and <c>table_bytes</c> per index, and
    /// covers every one of them whatever this ceiling is — an index growing while its table's row count
    /// does not is itself a bloat signal, and it is already collected.</para>
    /// </summary>
    public const long MeasureCeilingBytes = 1L * 1024 * 1024 * 1024;

    /// <summary>
    /// PostgreSQL's block size. <c>pgstatindex</c> is charged per BLOCK rather than per byte, so this is
    /// the unit the budget's cost argument is actually stated in.
    ///
    /// <para>Compile-time on the server (<c>BLCKSZ</c>) and 8 KB on every build this runs against,
    /// including Aurora. A server built with a different value would make the arithmetic below optimistic
    /// by that ratio, which is one more reason the rate it is multiplied by is a pessimistic one.</para>
    /// </summary>
    public const int BlockSizeBytes = 8192;

    /// <summary>
    /// The block rate <see cref="CycleMeasureBudgetBytes"/> and <see cref="MeasureCeilingBytes"/> are both
    /// sized against, and the reason each is a small number rather than a large one.
    ///
    /// <para><b>Why a block rate and not a byte throughput.</b> <c>pgstatindex</c> walks the index one
    /// block at a time through the buffer manager with no prefetch, so its cost is a count of
    /// potentially-synchronous single-block reads rather than a bulk transfer. On network-attached storage
    /// each miss is a round trip, so per-block LATENCY sets the rate and a sequential-throughput figure
    /// overstates it by orders of magnitude.</para>
    ///
    /// <para><b>This is now a MEASUREMENT, and it came in at roughly HALF the figure that preceded it
    /// (#3164).</b> The figure here used to be an assumption of 2,000 blocks/s, named pessimistic and
    /// carrying no measurement, because none existed. One does now: the first SUCCESS row this collector
    /// has ever produced recorded <c>collection_log.sql_duration_ms</c> of <b>252,940 ms</b>, which is the
    /// instrument <see cref="CycleMeasureBudgetBytes"/> pre-registered for exactly this decision. That run
    /// executed under a 2 GiB cycle budget, and the gate admits an index only when the running total
    /// THROUGH that index is still within budget, so the bytes it read cannot have exceeded the budget —
    /// which bounds the rate from above at 262,144 blocks / 252.94 s = <b>1,036 blocks/s</b> using nothing
    /// but that row's duration and the shipped gate. The rate derived from the bytes that run actually
    /// measured is ~1,013 blocks/s, and it is the figure kept here.</para>
    ///
    /// <para><b>What the denominator actually contains, because the name says "blocks per second" and
    /// this is not a pure I/O rate.</b> <c>sql_duration_ms</c> on a <see cref="RunsPerDatabase"/> collector
    /// is the SUM across databases of connect + execute + drain, so the figure below is blocks divided by
    /// everything the statement spent, not by <c>pgstatindex</c> time alone — and the run it comes from
    /// swept two databases, one of which failed fast on a missing extension. Every one of those terms
    /// inflates the denominator, so the true per-block read rate is FASTER than this. That is the right
    /// direction and the right quantity: what this constant feeds is a comparison against a command
    /// deadline, and the deadline covers connect and drain too. A pure I/O rate would understate the
    /// budget's real cost by exactly the terms it left out.</para>
    ///
    /// <para><b>What this figure does NOT rest on.</b> n = 1 — one run, one target, one night. Two numbers
    /// elsewhere in this type read like corroboration and are not: <see cref="MeasureCeilingBytes"/>' "about
    /// 11 hours" for the over-ceiling set and "roughly an hour" for its largest member are this same rate
    /// restated at coarse precision, not independent derivations. So the only independent check on the
    /// figure below is the 1,036 blocks/s upper bound above, which agrees on the order and on the
    /// direction. The three-significant-figure value is kept rather than rounded because rounding it would
    /// invent a margin, and the margin belongs in ONE place — see the next paragraph.</para>
    ///
    /// <para><b>The safety margin is the half-deadline allowance, and it must not be double-counted here.</b>
    /// <c>TheCycleBudget_FitsTheDeadline_AtTheMeasuredBlockRate</c> compares the budget against HALF of
    /// <see cref="CommandTimeoutSecondsOverride"/>, so a run whose rate comes in materially below this
    /// figure still has the other half of the deadline to finish in. Shading this constant below the
    /// measurement as well would spend that headroom twice and make neither figure mean anything. One
    /// number, one meaning: this one is what was measured, and the allowance is what pays for being wrong
    /// about it.</para>
    ///
    /// <para><b>What would justify moving it.</b> A DISTRIBUTION rather than another single row — several
    /// SUCCESS rows across cache states and instance load, because per-block latency on network-attached
    /// storage is the rate-setter and a single sample of it is not a rate. A measurement LOWER than this
    /// reds the deadline pins and brings the budget and the ceiling down again; that is the mechanism
    /// working rather than a problem, and it is the same trigger that produced this change.</para>
    /// </summary>
    public const int MeasuredBlocksPerSecond = 1_013;

    /// <summary>
    /// How many bytes of index ONE ATTEMPT will measure in total, largest first. Independent of
    /// <see cref="MeasureCeilingBytes"/> in what it measures: that one bounds a single index, this one
    /// bounds the statement.
    ///
    /// <para><b>Per ATTEMPT, which on this collector is per DATABASE.</b>
    /// <see cref="RunsPerDatabase"/> is true and each database gets its own statement under its own
    /// command deadline, so this is the bound the deadline is actually compared against. A sweep over N
    /// databases can therefore measure up to N times this figure in total, spread across N statements.
    /// The deadline is per statement, so that is the right granularity for the bound, but it is not a
    /// bound on a sweep.</para>
    ///
    /// <para><b>Why 1 GiB, and why it used to be 2.</b> Derived from the cost model in
    /// <see cref="MeasuredBlocksPerSecond"/>: 1 GiB is 131,072 blocks, which at 1,013 blocks/s is about
    /// 129 seconds, inside half of <see cref="CommandTimeoutSecondsOverride"/>. The remaining headroom
    /// pays for the catalog scan, connection setup, and being wrong about the rate. The figure was 2 GiB
    /// while the rate was an ASSUMED 2,000 blocks/s; the same arithmetic at the measured 1,013 puts 2 GiB
    /// at 259 seconds, past the whole allowance, so the budget came down rather than the allowance going
    /// up (#3164).</para>
    ///
    /// <para><b>The measurement that decided it is the one this comment used to ask for.</b> The line
    /// here previously read "until a SUCCESS row exists, every value here is an argument rather than a
    /// measurement, and the small end of the argument is the one that produces the row" — and that is
    /// what happened: the small end produced a row, the row supplied a rate, and the rate argued the
    /// budget DOWN. So the trigger fired as pre-registered rather than being overruled.</para>
    ///
    /// <para><b>What would justify raising it now.</b> Not another single SUCCESS row — a distribution of
    /// them, for the reason given in <see cref="MeasuredBlocksPerSecond"/>: one sample of a
    /// latency-bound rate is not a rate. Raising the command deadline is the other arithmetic route and
    /// is deliberately not taken here; see <see cref="CommandTimeoutSecondsOverride"/> for what it
    /// would and would not cost.</para>
    ///
    /// <para><b>It must never drop BELOW <see cref="MeasureCeilingBytes"/>, and that it equals it is a
    /// floor rather than a coincidence.</b> A cycle budget under the per-index ceiling opens a band
    /// between them in which an index can never be measured at all: it is under the ceiling, so it is a
    /// legitimate candidate and gets no "too large" reason, yet it alone exceeds the whole cycle, so it
    /// is over budget on its own first row every single run. It would be labelled
    /// <c>not measured this cycle</c> forever, which is precisely the "deferred" reading that this
    /// collector refuses to let stand for "never". Equal is the tightest value with no such band, and
    /// <c>TheCycleBudget_IsNeverBelowThePerIndexCeiling</c> pins it.</para>
    ///
    /// <para><b>Rotation did not weaken that floor — it made it LOAD-BEARING for liveness (#3153).</b>
    /// Because <c>budget &gt;= ceiling</c>, the first sub-ceiling candidate at or below the rotation cursor
    /// is admitted unconditionally: its own size is under the ceiling, hence under the budget, so the
    /// running total cannot already have excluded it. That single guarantee is what makes the cursor
    /// advance by at least one index EVERY cycle, which is in turn what makes "a later run in this pass
    /// reaches this index" a fact rather than a hope. Drop the budget below the ceiling and a band index
    /// sitting at the cursor is admitted by neither gate, so the cycle measures NOTHING, records the pass
    /// as complete, wraps to the largest index, and arrives back at the same band index — the collector
    /// stops measuring anything at all, on every index, permanently. The band used to mislabel one index;
    /// it now stops the whole mechanism.</para>
    ///
    /// <para><b>Decoupling the two would not have helped here, and the arithmetic is what says so
    /// (#3164).</b> The route #3153 left open was an unconditional first-admission rule plus the deadline
    /// constraint restated as a SUM (<c>ceiling + budget</c>) rather than an ordering, whose whole point
    /// was to let the budget fall to fit a slower rate while the ceiling stayed at 2 GiB. At the measured
    /// rate that goal is unreachable by any budget: 2 GiB is 262,144 blocks and about 259 s in ONE
    /// statement, so the ceiling alone is already past the 150 s allowance before a budget is chosen, and
    /// <c>ceiling + budget &lt;= allowance</c> has no solution at all. So the binding constraint was never
    /// the ordering between these two figures — it was the ceiling's own deadline cost, which the lockstep
    /// had been hiding by making the two numbers equal. Once the ceiling comes down to fit the deadline,
    /// setting the budget equal to it satisfies everything with no new mechanism, and decoupling would
    /// only permit the one direction (<c>budget &lt; ceiling</c>) that reopens the band. The transitive
    /// coverage is also the reason <c>ThePerIndexCeiling_FitsTheDeadline_OnItsOwn</c> now exists: while
    /// <c>budget &gt;= ceiling</c> holds, a deadline pin on the budget covers the ceiling for free, so a
    /// decoupling would silently remove the only assertion the ceiling's cost had.</para>
    ///
    /// <para><b>Accepted consequence.</b> An index at or above the ceiling is reported at its size with
    /// the ceiling's own reason and is never measured — on a large target that includes the biggest and
    /// most reclaimable indexes on the instance. That is a real gap, and it is stated in the row rather
    /// than hidden: the alternative on offer is not "measure them" but "measure nothing", which is what
    /// an unbounded statement delivers. See <see cref="MeasureCeilingBytes"/> for why that set is terminal
    /// at this geometry and which collector carries its size trend instead.</para>
    ///
    /// <para><b>What this bound does NOT buy.</b> Coverage is not budget-limited; before #3153 it was
    /// memory-limited, and now it is CADENCE-limited. A complete pass over the first production target's
    /// 2,457 measurable indexes was a fixed ~19.6M blocks however it was spread, so at one statement per
    /// day a full pass takes months. Rotation converts "never" into "eventually"; how long "eventually"
    /// is, is set by how much <c>pgstatindex</c> time per day is acceptable on a production instance and
    /// by nothing else. That is a scheduling decision, not a number this constant can express.</para>
    ///
    /// <para><b>What halving it cost, stated in the unit it is paid in (#3164).</b> Pass length is
    /// measurable blocks divided by this budget, so halving the budget at most DOUBLES the pass: ~75
    /// cycles became at most ~150, and at the 1,440-minute cadence in <c>CollectorScheduleDefaults</c>
    /// that is at most ~150 days rather than ~75. "At most", because the same change lowered
    /// <see cref="MeasureCeilingBytes"/>, which removes the indexes between the old and new ceilings from
    /// the measurable set entirely — a smaller numerator against the halved denominator, so the true pass
    /// length lands somewhere below the doubling. How far below depends on how much of that target's
    /// ~150 GB of measurable index sits between 1 and 2 GiB, and that has not been measured. The cadence
    /// is deliberately NOT touched in the same change: lowering the budget lengthens the pass, so moving
    /// both at once would conflate two effects and leave neither figure meaning anything. It is re-derived
    /// from whatever this budget is, separately.</para>
    ///
    /// <para><b>Why slower coverage is the right thing to pay.</b> A budget that overruns its deadline
    /// produces NO ROW AT ALL — strictly worse than a smaller budget that completes, and it is also the
    /// state that produces no <c>sql_duration_ms</c> to measure the next decision from. The one SUCCESS
    /// row this collector has ever produced spent 84% of its deadline, so the margin being defended here
    /// is real rather than theoretical. Before #3153 a smaller budget cost REACHABILITY — the same
    /// largest indexes were re-selected every cycle and anything below the cut was never measured — and
    /// rotation is what converted that cost into a rate. Paying in coverage rate is only an option at all
    /// because rotation exists.</para>
    /// </summary>
    public const long CycleMeasureBudgetBytes = 1L * 1024 * 1024 * 1024;

    /// <summary>
    /// Prefix of the per-database rotation cursor key in <c>collector_state</c> (V44, primary key
    /// <c>(server_id, collector_name, state_key)</c>) — the concrete key is
    /// <c>RotationCursorKeyPrefix || database_name</c>, the same shape <c>query_store</c>'s per-database
    /// keys use.
    ///
    /// <para><b>Why the key is per DATABASE while <see cref="CollectorContext.State"/> is per server.</b>
    /// <see cref="RunsPerDatabase"/> is true, and a cursor shared across databases would advance one
    /// database past indexes another database never measured — which is the false claim this whole change
    /// exists to remove, reintroduced one level up. So each database carries its own, and
    /// <see cref="BuildQuery"/> splices every loaded cursor into the statement with
    /// <c>current_database()</c> choosing the right one, because the host builds this collector's query
    /// ONCE for the whole per-database sweep.</para>
    ///
    /// <para><b>Why declaring the PREFIX in <see cref="StateKeys"/> is enough.</b> Both hosts use the
    /// declared list only as a gate — <c>StateKeys.Count == 0</c> decides whether a state query runs at
    /// all — and the query itself is <c>WHERE server_id = $1 AND collector_name = $2</c>, so it returns
    /// every key stored under this collector's name, not only the declared ones.
    /// <c>TheStateLoadIsByCollectorName_NotByDeclaredKey</c> pins that, because a future change that
    /// filtered the load down to the declared keys would silently return nothing here: rotation would stop
    /// and the collector would look exactly like it does today, measuring one index forever.</para>
    ///
    /// <para><b>Orphans are PRUNED, and "a few dozen bytes" was the wrong unit.</b> A dropped database's
    /// cursor matches no <c>current_database()</c> again, so it changes no behaviour on its own — but
    /// <see cref="BuildQuery"/> splices EVERY loaded cursor and the host reuses that one statement for each
    /// live database, so the cost is
    /// <c>O(databases ever seen) x O(live databases this cycle)</c> in query text and bound parameters,
    /// every cycle, permanently. At three parameters per cursor that reaches PostgreSQL's 65,535-parameter
    /// statement limit at 21,845 names ever having existed — verified against PostgreSQL 17 with this
    /// query's own <c>resume</c> shape: 21,845 cursors execute (1.5 MB of SQL), and 21,846 throw
    /// <c>A statement cannot have more than 65535 parameters</c> before the statement is sent, which fails
    /// the cycle for EVERY database rather than degrading. So the host prunes them against the database
    /// list its own sweep enumerated — see <see cref="PgPerDatabaseCollectorState"/>, and note that the
    /// query_store prune cannot serve this prefix because its live-database source is a SQL Server
    /// snapshot.</para>
    ///
    /// <para><b>A database that disappears and comes back starts at the LARGEST index, deliberately.</b>
    /// The prune deletes its cursor, so it is indistinguishable from a database seen for the first time.
    /// Resuming mid-pass instead would be actively wrong rather than merely wasteful: a recreated database
    /// has a new catalog, so the stored <c>(bytes, oid)</c> coordinate names an oid that no longer exists,
    /// and every index sorting above that coordinate in the NEW catalog would be stamped
    /// <c>above the rotation cursor</c> — asserting that this pass already advanced past an index it has
    /// never measured. That is the same false claim #3153 exists to remove, so the honest cost is at most
    /// one pass of re-measurement.</para>
    /// </summary>
    public const string RotationCursorKeyPrefix = "rotate:";

    /// <summary>
    /// Stored in place of a cursor when a cycle measured nothing, which — given
    /// <see cref="CycleMeasureBudgetBytes"/> is never below <see cref="MeasureCeilingBytes"/> — can only
    /// mean no measurable candidate is left below the cursor: the pass is done and the next one starts at
    /// the largest index again.
    ///
    /// <para>A sentinel rather than a deletion, because <see cref="CollectorContext.PendingState"/> is
    /// upserted and never deletes: leaving the old value in place would park the cursor at the end of the
    /// pass forever. It parses as "no cursor", so it lands on the same conservative path as an absent or
    /// unreadable value.</para>
    /// </summary>
    public const string RotationPassCompleteMarker = "wrap";

    /// <summary>
    /// The most cursors <see cref="BuildQuery"/> will splice into one statement. Above this it splices
    /// NONE, and every database starts its pass at the largest index.
    ///
    /// <para><b>A backstop, not the bound.</b> What bounds the cursor list is the host's prune
    /// (<see cref="PgPerDatabaseCollectorState"/>), which holds it at the count of LIVE databases. This
    /// exists because that prune has a realistic silent-no-op path — an enumeration that keeps coming back
    /// empty is a permissions failure the prune must not act on, and a store written by a build that
    /// predates the prune carries whatever it accumulated. Without a cap the accumulation ends at
    /// PostgreSQL's 65,535-parameter limit, where the statement throws before being sent and the cycle
    /// fails for every database; with one it degrades to the pre-#3153 selection instead.</para>
    ///
    /// <para><b>Why 1,024.</b> Twenty-one times below the parameter ceiling (1,024 x 3 = 3,072 of 65,535)
    /// and about nine times the largest per-database fan-out this product has measured, so no real target
    /// reaches it — measured on PostgreSQL 17, a 1,024-cursor <c>resume</c> is 69 KB of SQL and executes in
    /// single-digit milliseconds, while 21,845 is 1.5 MB. A target that does reach it has already
    /// accumulated a thousand orphans, which is the broken state this makes graceful.</para>
    ///
    /// <para><b>No row lies when it engages.</b> Splicing no cursor makes every candidate in-window, so the
    /// reasons that remain are the ceiling and work-budget ones, which are true; nothing claims to be above
    /// a cursor. Degraded and honest, which is the direction to fail in.</para>
    /// </summary>
    public const int MaxSplicedCursors = 1_024;

    /// <summary>
    /// Where one database's last cycle stopped: the coordinate, in this collector's own measurement order,
    /// of the last index it measured.
    /// </summary>
    /// <param name="DatabaseName">The database the cursor belongs to, from the state key's suffix.</param>
    /// <param name="IndexBytes">The measurement order's leading column, descending.</param>
    /// <param name="IndexOid">The tiebreaker, ascending — unique per database, which
    /// <c>index_name</c> is not.</param>
    public readonly record struct RotationCursor(string DatabaseName, long IndexBytes, long IndexOid);

    /// <param name="AvgLeafDensity">The server's own figure, 0–100. NOT a bloat percentage — a healthy
    /// index sits near 90, so subtracting from 100 invents roughly 10 points of bloat that is not there.</param>
    /// <param name="LeafFragmentation">Share of leaf pages out of physical order. Zero on a freshly built
    /// index; near 50 on the churned one measured while designing this.</param>
    /// <param name="EmptyPages">Pages holding nothing. Directly reclaimable by <c>REINDEX</c> and the most
    /// concrete number here.</param>
    /// <param name="DeletedPages">Pages marked deleted and awaiting reuse.</param>
    /// <param name="SkippedReason">Null when measured. Populated when the index was not measured — too
    /// large for the ceiling, already passed by the rotation cursor, or past this cycle's work budget — so
    /// a bound can never masquerade as an absence of bloat.</param>
    /// <param name="IndexOid">NOT STORED, and deliberately absent from <see cref="PayloadColumns"/>: the
    /// rotation cursor needs a coordinate that is unique per database and <c>index_name</c> is not, so the
    /// oid is projected for <see cref="ReadAsync"/> to compute the next cursor from and then discarded.
    /// Adding it to the payload would be a store rung for a value no read wants.</param>
    public readonly record struct Row(
        string? DatabaseName,
        string? SchemaName,
        string? TableName,
        string? IndexName,
        long IndexBytes,
        int? TreeLevel,
        long? InternalPages,
        long? LeafPages,
        long? EmptyPages,
        long? DeletedPages,
        double? AvgLeafDensity,
        double? LeafFragmentation,
        string? SkippedReason,
        long IndexOid);

    /* The candidate set is fenced with OFFSET 0 so the btree filter is applied BEFORE pgstatindex is
       called on anything. See the type header for why that matters more than usual here.

       Every gate that bounds WORK selects the function's INPUT relation (in_budget), and the function's
       output is joined back onto the full candidate set afterwards. A gate written as a qual on a LEFT
       JOIN LATERAL instead reads identically and bounds nothing: the planner has no way to skip an inner
       side it has not evaluated, so the function runs for every candidate row and the qual only decides
       whether to keep the answer. Measured on PostgreSQL 17.11 with a call-counting stand-in and ten
       candidates of which one was in budget: the ON-clause form reported `Rows Removed by Join Filter: 9`
       over a `Function Scan ... loops=10`, the fenced form `loops=1`, and the two returned byte-identical
       rows. Correct labels over an unbounded read is the shape that produces them.

       So skipped indexes reappear through the outer LEFT JOIN to `measured` rather than through a LEFT
       JOIN LATERAL on the function itself. An index that is not measured must still be RETURNED carrying
       its skipped_reason - dropping it would read as an index that does not exist - and this way it is
       returned without being read.

       System schemas are excluded - their indexes are not something an operator reindexes on our advice.

       pgstatindex is qualified public., NOT pg_catalog. - it is an EXTENSION function and lives wherever
       pgstattuple was created, which is public by convention. Qualifying it pg_catalog. simply does not
       resolve (verified: "function pg_catalog.pgstatindex(oid) does not exist"), and leaving it unqualified
       would let an object in an earlier search_path schema shadow it. An install that put pgstattuple
       somewhere else fails into ObjectMissing, which is the honest outcome and is exactly what
       pg_extension_availability (#2545) reports on. Same convention as PgBufferUsageCollector's
       public.pg_buffercache.

       The oid is cast to regclass because that is the parameter type; an unqualified oid does not match. */
    private static string BuildQueryText(string resumeBody) => @"
WITH candidates AS (
    SELECT
        n.nspname                       AS schema_name,
        t.relname                       AS table_name,
        c.relname                       AS index_name,
        c.oid                           AS index_oid,
        pg_catalog.pg_relation_size(c.oid) AS index_bytes
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_am AS am
      ON am.oid = c.relam
    JOIN pg_catalog.pg_index AS x
      ON x.indexrelid = c.oid
    JOIN pg_catalog.pg_class AS t
      ON t.oid = x.indrelid
    JOIN pg_catalog.pg_namespace AS n
      ON n.oid = c.relnamespace
    WHERE c.relkind = 'i'
    AND   am.amname = 'btree'
    AND   x.indisvalid
    AND   x.indisready
    AND   n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
    OFFSET 0
),
/* WHERE THE LAST CYCLE STOPPED (#3153), for THIS database, and the only thing in this statement that is
   not a pure function of the target catalog.

   Exactly one row, always: an aggregate with no GROUP BY returns one row even over an empty input, so the
   cross join below can never drop a candidate and an absent cursor arrives as NULL rather than as a
   missing row. At most one VALUES row can match - the state dictionary is keyed by database name - so the
   max() is picking a value, not combining several.

   Every cursor the host loaded is spliced, not just this database's, because BuildQuery is called ONCE per
   cycle for the whole per-database sweep: the collector declares no watermark, so the host builds the plan
   before it opens the first database. current_database() is what selects this database's row, which is
   also why the cursor coordinate is a SIZE and an OID rather than a row offset. */
resume AS (
" + resumeBody + @"
),
/* THE ROTATION WINDOW: the candidates at or below the cursor, in the statement's own measurement order.

   Written once, here, because it is needed three times below - by both window aggregates FILTER clauses
   and by the gate - and a predicate this shape retyped three times is three chances to disagree.

   The comparison is the measurement order run backwards, and it has to match that order exactly or the
   cursor is not a resume point: index_bytes DESCENDING, then index_oid ASCENDING. index_oid is the
   tiebreaker precisely because it is UNIQUE per database, which index_name is not - pg_class is unique on
   (relname, relnamespace), so two schemas can hold equally-sized indexes of the same name, and a cursor
   parked on a non-unique coordinate would exclude a sibling it never measured while labelling it as
   already passed. Measured on PostgreSQL 17.11: exactly that pair at 1,138,688 bytes each.

   STRICTLY greater, never >=. Re-admitting the index the cursor names would look conservative and is the
   one shape that can stall: an index just under the ceiling would be re-measured, exhaust the budget on
   its own, leave the cursor unchanged, and be re-measured forever.

   An index that GREW past the cursor, or was created above it, sits outside the window without having
   been measured this pass - which is why the reason for those rows claims the cursor position and the
   wrap, not that the index was measured. Bounded by one pass either way. */
windowed AS (
    SELECT
        k.*,
        (r.cursor_bytes IS NULL
         OR k.index_bytes < r.cursor_bytes
         OR (k.index_bytes = r.cursor_bytes
             AND k.index_oid::bigint > r.cursor_oid)) AS in_rotation_window
    FROM candidates AS k
    CROSS JOIN resume AS r
),
/* THE ACCOUNTING for the work budget (#2617), and only the accounting - in_budget below is what
   enforces it. pgstatindex reads every page it is pointed at, so an unbounded statement reads the
   whole instance: measured on a live Aurora target, 1,517 indexes totalling 461 GB in a single
   statement, which never finished and dropped the connection mid-read.

   Ranked by size and measured largest-first WITHIN THE ROTATION WINDOW, because bloat that matters is
   concentrated in big indexes - a small index at 40% density is worth kilobytes. Everything past the
   budget is still RETURNED, with a reason, so the read never mistakes unmeasured for healthy.

   TWO figures, and the BYTE one is what bounds the work (#2997). A count bounds pages only where
   count correlates with bytes; on the first production target it did not - at a 20 GB per-index
   ceiling the 200 largest sub-ceiling indexes there admitted 286 GB, a figure that carries the
   ceiling it was measured at because the ceiling is what decides which indexes are sub-ceiling. The
   count survives because it is legible - an operator can predict the biggest N in a way they cannot
   predict a byte figure - not because it bounds anything: see CycleMeasureBudgetBytes for which of
   the two is load-bearing.

   BOTH are now counted over the rows this cycle can actually measure, which is what makes them
   compatible with rotation. A rank over the whole census would count the rows ALREADY passed and the
   over-ceiling ones that are never handed to the function, so the count bound would stop admitting
   anything at all once the cursor moved past its Nth row - a hard stop dressed as a budget. It also
   fixes a latent miscount the count bound had while it was unreachable: size_rank counted over-ceiling
   indexes, so up to 43 of its 200 slots went to indexes nothing ever measures. */
ranked AS (
    SELECT
        k.*,
        /* 1-based position among the indexes this cycle will measure. count(*) FILTER rather than
           row_number(), because row_number() takes no FILTER and the rank has to skip the rows the
           gate skips; count over an empty frame is 0, never NULL. */
        count(*) FILTER (WHERE k.index_bytes < " + CeilingLiteral + @" AND k.in_rotation_window)
            OVER (ORDER BY k.index_bytes DESC, k.index_oid
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS measure_rank,
        /* The running total of what will actually be READ, so the cycle can stop at a byte figure
           rather than a row count. FILTERed to sub-ceiling indexes IN THE WINDOW because those are
           the only ones handed to pgstatindex and therefore the only ones that cost pages; charging
           an over-ceiling or already-passed index to the budget would spend the whole allowance on
           indexes nobody reads. Since the over-ceiling indexes sort first and can total more than the
           budget between them, the unfiltered form would exhaust the allowance before the first
           measurable index and measure nothing at all.

           coalesce because a FILTERed window sum is NULL until its frame contains a matching row,
           and the over-ceiling and already-passed indexes both sort FIRST under this ordering.
           NULL <= budget is NULL rather than false, so the raw form would leave the gate neither
           open nor closed. */
        coalesce(
            sum(k.index_bytes) FILTER (WHERE k.index_bytes < " + CeilingLiteral + @" AND k.in_rotation_window)
                OVER (ORDER BY k.index_bytes DESC, k.index_oid
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
            0)::bigint                      AS measured_bytes_through_here
    FROM windowed AS k
),
/* THE GATE, and the only place the four bounds are enforced. Selecting the relation pgstatindex is
   applied to is what makes them bounds; a qual on the function's join makes them labels. Fenced with
   OFFSET 0 on the same argument as the candidate set - when the failure mode is that the collector
   returns nothing at all, the bound should not rest on the planner choosing to push a filter down.

   The rotation window is enforced HERE and not in candidates, because a candidate outside the window
   must still be RETURNED with its size and a reason. Filtering the census would make an index the
   cursor has passed indistinguishable from one that does not exist. */
in_budget AS (
    SELECT
        k.index_oid                     AS index_oid
    FROM ranked AS k
    WHERE k.index_bytes < " + CeilingLiteral + @"
    AND   k.in_rotation_window
    AND   k.measure_rank <= " + BudgetLiteral + @"
    AND   k.measured_bytes_through_here <= " + CycleByteBudgetLiteral + @"
    OFFSET 0
),
/* CROSS JOIN LATERAL here, because every row of in_budget is by construction one this statement has
   decided to read. Nothing is dropped by the cross join: an index absent from in_budget still reaches
   the result through the outer LEFT JOIN below, with its measurements NULL and a reason. */
measured AS (
    SELECT
        b.index_oid                     AS index_oid,
        s.tree_level                    AS tree_level,
        s.internal_pages                AS internal_pages,
        s.leaf_pages                    AS leaf_pages,
        s.empty_pages                   AS empty_pages,
        s.deleted_pages                 AS deleted_pages,
        s.avg_leaf_density              AS avg_leaf_density,
        s.leaf_fragmentation            AS leaf_fragmentation
    FROM in_budget AS b
    CROSS JOIN LATERAL public.pgstatindex(b.index_oid::regclass) AS s
)
SELECT
    current_database()::text            AS database_name,
    k.schema_name::text                 AS schema_name,
    k.table_name::text                  AS table_name,
    k.index_name::text                  AS index_name,
    k.index_bytes::bigint               AS index_bytes,
    m.tree_level                        AS tree_level,
    m.internal_pages::bigint            AS internal_pages,
    m.leaf_pages::bigint                AS leaf_pages,
    m.empty_pages::bigint               AS empty_pages,
    m.deleted_pages::bigint             AS deleted_pages,
    m.avg_leaf_density::double precision   AS avg_leaf_density,
    m.leaf_fragmentation::double precision AS leaf_fragmentation,
    /* Four arms, and the split between them is the whole point of #3153: exactly one of them describes a
       PERMANENT outcome, and it is the one that says so. The other three are deferrals that a later cycle
       in this pass really does honour, because the cursor advances by at least one index every cycle. */
    CASE
        WHEN k.index_bytes >= " + CeilingLiteral + @"
            THEN 'larger than the measurement ceiling: pgstatindex reads every page, and no per-statement '
                 || 'deadline this collector can set reaches an index this size. Recorded but NEVER '
                 || 'measured - this is not a deferral. Its size trend is collected by pg_index_usage_stats.'
        /* Already passed by the cursor. This arm exists because without it these rows would arrive with
           every measurement NULL and NO reason - the one output shape this collector must never produce,
           since a blank measurement with no reason is indistinguishable from a measured emptiness. It
           claims the CURSOR POSITION and the wrap rather than claiming the index was measured, because an
           index that grew into this range, or was created in it, was not. */
        WHEN NOT k.in_rotation_window
            THEN 'above the rotation cursor: this pass has already advanced past this position, and the '
                 || 'cursor comes back to it when the pass wraps to the largest index. Recorded at its '
                 || 'size so it is never mistaken for healthy.'
        /* The BYTE budget is reported ahead of the count one because it is the bound that actually
           binds on a large-index target, and an index past both is past this one first. */
        WHEN k.measured_bytes_through_here > " + CycleByteBudgetLiteral + @"
            THEN 'not measured this cycle (work budget): pgstatindex reads every page, so a run measures '
                 || 'at most ' || pg_catalog.pg_size_pretty(" + CycleByteBudgetLiteral + @"::bigint)
                 || ' of index from the rotation cursor down. The cursor advances past what it measured, '
                 || 'so a later run in this pass reaches this index - deferred, not skipped.'
        WHEN k.measure_rank > " + BudgetLiteral + @"
            THEN 'not measured this cycle (work budget): pgstatindex reads every page, so a run measures '
                 || 'at most ' || " + BudgetLiteral + @" || ' indexes from the rotation cursor down. The '
                 || 'cursor advances past what it measured, so a later run in this pass reaches this '
                 || 'index - deferred, not skipped.'
    END::text                           AS skipped_reason,
    /* NOT a payload column - see Row.IndexOid. Projected last so every stored column keeps its ordinal,
       and projected at all because the rotation cursor needs a coordinate that is unique per database. */
    k.index_oid::bigint                 AS index_oid
FROM ranked AS k
/* Plain LEFT JOIN on the already-computed measurements, NOT a join to the function. Every candidate
   appears exactly once; the ones in_budget excluded arrive here with their measurement columns NULL,
   which is what the skipped_reason arms above describe. */
LEFT JOIN measured AS m
  ON m.index_oid = k.index_oid
ORDER BY k.index_bytes DESC";

    /// <summary>
    /// The <c>resume</c> body for a cycle with no usable cursor for any database — a first run, a store
    /// that has never been written, a state read that failed, a completed pass, or a value this build
    /// cannot parse. All five collapse to the same conservative path deliberately: start at the LARGEST
    /// index, which is byte-for-byte what this collector did before it had a cursor at all. Absent is what
    /// a first run, a restarted host and a broken store all look like, so absent must not mean "skip".
    /// </summary>
    private const string NoCursorResumeBody = @"    SELECT
        NULL::bigint                    AS cursor_bytes,
        NULL::bigint                    AS cursor_oid";

    private const string CeilingLiteral = "1073741824";

    /* The upper bound on how many indexes one attempt will MEASURE, largest first: 200 indexes OR
       CycleMeasureBudgetBytes, whichever comes first. The byte figure is the one that binds on any
       target large enough to matter; see the ranked CTE for why the count is kept anyway. */
    private const string BudgetLiteral = "200";

    /* Kept in the C# type system as well as in the SQL literal so a reader has one authoritative
       figure and TheBudgetLiterals_AgreeWithTheirConstants can pin that the two agree. */
    private const string CycleByteBudgetLiteral = "1073741824";

    /// <summary>
    /// Five minutes, because even a bounded cycle of pages is real work and a slow single index
    /// should yield a CLASSIFIED timeout rather than a dropped connection. index_object_stats takes
    /// the same override for the same reason (#1135).
    ///
    /// <para>This is a BACKSTOP, not the bound. The deadline cannot make an over-large statement
    /// finish - it only decides how long the sweep waits before giving up, and a statement that does
    /// not fit spends the whole five minutes and then reports nothing. What bounds the work is
    /// <see cref="CycleMeasureBudgetBytes"/>, which is sized to fit inside HALF of this figure; a run
    /// that needs the rest of the deadline has already been mis-budgeted.</para>
    ///
    /// <para>Npgsql's CommandTimeout is a socket READ timeout that every backend message restarts,
    /// so it bounds backend SILENCE rather than total elapsed time. <c>pgstatindex</c> sends nothing
    /// while it scans, so it gets no reprieve from that and the deadline does fire - which is why
    /// the failures arrive as <c>Exception while reading from stream</c>, the transport's own words
    /// for a read that ran out of time.</para>
    ///
    /// <para><b>Raising it is the other way to make the arithmetic work, and it is NOTHING to do with a
    /// 120-second wall clock (#3164).</b> That objection was checked and does not apply: 120 s is not a
    /// product-wide budget but the value three SQL Server collectors chose for
    /// <see cref="PerItemWallClockBudget"/>, the opt-in per-item budget #2673 added, whose base default is
    /// <c>null</c> and which <c>query_store</c> already sets to 600 s — longer than this deadline. This
    /// collector does not override it, so no wall clock bounds it at all and this figure is its only
    /// deadline. A per-collector budget above 300 s is therefore established rather than novel.</para>
    ///
    /// <para><b>It is still not the move, for reasons that are about cost rather than mechanism.</b> The
    /// figure that would fit a 2 GiB ceiling at the measured rate is ~518 s, so this would sit at over
    /// eight minutes of <c>pgstatindex</c> in one statement against a production instance — and because
    /// this is a backstop rather than a bound, it is also eight minutes that a MIS-BUDGETED run spends
    /// before reporting nothing. It would be sized to make one n=1 measurement fit rather than derived
    /// from anything, which is the move the budget's own pin exists to prevent. And it would leave
    /// <see cref="MeasuredBlocksPerSecond"/> wrong, which is the actual defect. Raising it needs its own
    /// argument about acceptable occupancy of a production instance, made on its own terms.</para>
    /// </summary>
    public override int? CommandTimeoutSecondsOverride => 300;

    public override string Name => "pg_index_bloat";

    public override string TargetTable => "pg_index_bloat";

    /// <summary>
    /// Primaries only. A standby's index files are byte-identical to the primary's, so the answer is the
    /// same and the cost — a full read of every index — would be paid twice. Matches
    /// <see cref="PgIndexUsageStatsCollector"/>'s gate, which the two share a cadence with.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => !target.IsInRecovery;

    /// <summary>Per-database: indexes and their catalogs are per-database.</summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => true;

    /// <summary><c>pgstatindex</c> is the function this MEASURES with, and it is the extension's. A plain
    /// <c>CREATE EXTENSION</c> with no restart, but in every database wanted — which
    /// <see cref="RunsPerDatabase"/> above already implies rather than restating here.</summary>
    public override IReadOnlyList<PgExtensionDependency> RequiredPgExtensions { get; } = new[]
    {
        new PgExtensionDependency("pgstattuple", PgExtensionInstallKind.CreateExtension),
    };

    /// <summary>
    /// The one piece of per-server state this collector declares: the rotation cursor's key prefix.
    ///
    /// <para>The concrete keys are per DATABASE and therefore not knowable here — the database set is
    /// discovered per cycle by the host. What this list has to be right about is only whether a state
    /// query runs at all, which is all either host reads it for; see
    /// <see cref="RotationCursorKeyPrefix"/> for why loading by collector name makes the prefix
    /// sufficient, and for the pin that keeps it sufficient.</para>
    /// </summary>
    public override IReadOnlyList<string> StateKeys { get; } = new[] { RotationCursorKeyPrefix };

    /// <summary>
    /// Splices this server's rotation cursors into the statement (#3153). The returned text is a function
    /// of <see cref="CollectorContext.State"/>, which is the whole difference between a collector that
    /// rotates and one that re-measures its largest index forever.
    ///
    /// <para>The cursors are BOUND, not interpolated. Two of the three fields are integers and would be
    /// safe either way, but the database name is a catalog identifier that may contain anything a quoted
    /// identifier may contain, and <c>DatabaseExclusionFilter</c> already established parameter binding as
    /// this repo's answer for names reaching a PostgreSQL statement.</para>
    ///
    /// <para>No cursors — a first run, an empty store, a failed state read, or every database's pass
    /// complete — yields <see cref="NoCursorResumeBody"/> and NO parameters, so the emitted statement is
    /// byte-identical to the pre-rotation one. That is the conservative direction: start at the largest
    /// index.</para>
    /// </summary>
    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        var cursors = ReadCursors(context.State);

        /* No cursors, or so many that the statement itself is at risk: both take the conservative path of
           starting every database at the largest index. See MaxSplicedCursors for why the cap is a backstop
           behind the host's prune rather than the bound. */
        if (cursors.Count == 0 || cursors.Count > MaxSplicedCursors)
        {
            return new CollectorQuery(BuildQueryText(NoCursorResumeBody));
        }

        var values = new StringBuilder();
        var parameters = new List<CollectorParameter>(cursors.Count * 3);

        for (var i = 0; i < cursors.Count; i++)
        {
            var cursor = cursors[i];
            var index = i.ToString(CultureInfo.InvariantCulture);

            if (i > 0)
            {
                values.Append(",\n        ");
            }

            /* Explicit casts on the two integers so PostgreSQL types the VALUES columns from the SQL
               rather than from parameter inference, which is what lets max() and the comparisons below
               resolve on a first execution. */
            values.Append("(@rot_db_").Append(index)
                  .Append(", @rot_bytes_").Append(index).Append("::bigint")
                  .Append(", @rot_oid_").Append(index).Append("::bigint)");

            parameters.Add(new CollectorParameter(
                "@rot_db_" + index, cursor.DatabaseName, CollectorParameterType.NVarChar128));
            parameters.Add(new CollectorParameter(
                "@rot_bytes_" + index, cursor.IndexBytes, CollectorParameterType.BigInt));
            parameters.Add(new CollectorParameter(
                "@rot_oid_" + index, cursor.IndexOid, CollectorParameterType.BigInt));
        }

        var resumeBody =
            "    SELECT\n"
            + "        max(c.cursor_bytes)             AS cursor_bytes,\n"
            + "        max(c.cursor_oid)               AS cursor_oid\n"
            + "    FROM (VALUES\n        " + values + "\n"
            + "    ) AS c(database_name, cursor_bytes, cursor_oid)\n"
            + "    WHERE c.database_name = pg_catalog.current_database()";

        return new CollectorQuery(BuildQueryText(resumeBody), parameters);
    }

    /// <summary>
    /// The usable cursors in a loaded state dictionary, oldest-format-tolerant and ordered by database
    /// name so the emitted text is a function of the STATE rather than of dictionary iteration order —
    /// otherwise two cycles with identical state could emit different strings, and nothing about the
    /// statement would be reproducible.
    ///
    /// <para>Anything unparseable is DROPPED rather than repaired: an unreadable cursor and an absent one
    /// have the same right answer, which is to start this database's pass at the largest index.</para>
    /// </summary>
    public static List<RotationCursor> ReadCursors(IReadOnlyDictionary<string, string>? state)
    {
        var cursors = new List<RotationCursor>();

        if (state is null)
        {
            return cursors;
        }

        foreach (var entry in state)
        {
            if (!entry.Key.StartsWith(RotationCursorKeyPrefix, StringComparison.Ordinal))
            {
                continue;
            }

            var databaseName = entry.Key[RotationCursorKeyPrefix.Length..];

            if (databaseName.Length == 0 || !TryParseCursor(entry.Value, out var bytes, out var oid))
            {
                continue;
            }

            cursors.Add(new RotationCursor(databaseName, bytes, oid));
        }

        cursors.Sort(static (left, right) => string.CompareOrdinal(left.DatabaseName, right.DatabaseName));

        return cursors;
    }

    /// <summary>
    /// Parses a stored cursor value, <c>index_bytes|index_oid</c>. False for
    /// <see cref="RotationPassCompleteMarker"/> and for anything else this build does not recognise, which
    /// is what makes "pass complete", "never written" and "corrupt" one code path.
    /// </summary>
    public static bool TryParseCursor(string? value, out long indexBytes, out long indexOid)
    {
        indexBytes = 0;
        indexOid = 0;

        if (string.IsNullOrEmpty(value))
        {
            return false;
        }

        var separator = value.IndexOf('|', StringComparison.Ordinal);

        if (separator <= 0 || separator == value.Length - 1)
        {
            return false;
        }

        if (!long.TryParse(
                value.AsSpan(0, separator), NumberStyles.Integer, CultureInfo.InvariantCulture, out var bytes)
            || !long.TryParse(
                value.AsSpan(separator + 1), NumberStyles.Integer, CultureInfo.InvariantCulture, out var oid)
            || bytes < 0
            || oid < 0)
        {
            return false;
        }

        indexBytes = bytes;
        indexOid = oid;
        return true;
    }

    /// <summary>Formats one database's cursor for storage.</summary>
    public static string FormatCursor(long indexBytes, long indexOid) =>
        indexBytes.ToString(CultureInfo.InvariantCulture)
        + "|"
        + indexOid.ToString(CultureInfo.InvariantCulture);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        /* This collector runs once per database and pgstatindex measures the connected database only, so
           without this the same index name in two databases is one indistinguishable row (#2599). */
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("schema_name", CollectorColumnType.Varchar),
        new CollectorColumn("table_name", CollectorColumnType.Varchar),
        new CollectorColumn("index_name", CollectorColumnType.Varchar),
        new CollectorColumn("index_bytes", CollectorColumnType.BigInt),
        /* Tree depth. A level that climbs on an index whose row count did not is a shape worth seeing, and
           it costs nothing to keep since the function already returned it. */
        new CollectorColumn("tree_level", CollectorColumnType.Integer),
        new CollectorColumn("internal_pages", CollectorColumnType.BigInt),
        new CollectorColumn("leaf_pages", CollectorColumnType.BigInt),
        /* The two concrete reclaimable numbers, and the only ones here that need no interpretation. */
        new CollectorColumn("empty_pages", CollectorColumnType.BigInt),
        new CollectorColumn("deleted_pages", CollectorColumnType.BigInt),
        /* Stored RAW. See the type header: a healthy index reads near 90, so this is not 100-minus-bloat. */
        new CollectorColumn("avg_leaf_density", CollectorColumnType.Double),
        new CollectorColumn("leaf_fragmentation", CollectorColumnType.Double),
        new CollectorColumn("skipped_reason", CollectorColumnType.Varchar),
    };

    /// <summary>
    /// Drains the rows and, from the rows themselves, works out where the NEXT cycle should resume
    /// (#3153).
    ///
    /// <para><b>Derived from the output rather than returned as a column</b>, because the cursor is not a
    /// fact about any one index — it is the position of the LAST index this statement measured, in the
    /// statement's own measurement order. Reading it off the rows keeps the SQL free of a second copy of
    /// the ordering rule.</para>
    ///
    /// <para><b>Nothing measured means the pass is over</b>, not that the cycle failed. Because
    /// <see cref="CycleMeasureBudgetBytes"/> is never below <see cref="MeasureCeilingBytes"/>, the first
    /// sub-ceiling candidate at or below the cursor is always admitted — so an empty measured set can only
    /// mean there was no such candidate, and the cursor wraps. A cycle that FAILS never reaches here at
    /// all, leaves no pending key, and the older cursor is re-read next cycle: the conservative
    /// direction.</para>
    ///
    /// <para><b>Accepted, and bounded:</b> the cursor is staged from the DRAIN, and the host lands
    /// <see cref="CollectorContext.PendingState"/> after the cycle rather than after each database's
    /// flush. A database whose rows are read and then fail to STORE therefore advances its cursor over
    /// measurements nobody kept, and those indexes wait for the wrap instead of the next cycle — one pass,
    /// on a cycle that already stored nothing. Landing it post-flush needs the per-item completion seam
    /// (#2312) in both hosts, which this change deliberately leaves alone.</para>
    /// </summary>
    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        /* The running "last measured" coordinate: smallest index_bytes, and among ties the largest
           index_oid, which is the tail of ORDER BY index_bytes DESC, index_oid. Tracked as a scan rather
           than taken from the last row, because the output ordering carries no tiebreaker and a row that
           was SKIPPED can legitimately sort after one that was measured. */
        var measuredAny = false;
        var cursorBytes = 0L;
        var cursorOid = 0L;

        /* current_database() from the rows, which is what the payload stores, falling back to the name the
           host is iterating for the case where a database has no btree indexes at all and therefore no
           rows to read it from. */
        string? databaseName = null;

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                DatabaseName: reader.IsDBNull(0) ? null : reader.GetString(0),
                SchemaName: reader.IsDBNull(1) ? null : reader.GetString(1),
                TableName: reader.IsDBNull(2) ? null : reader.GetString(2),
                IndexName: reader.IsDBNull(3) ? null : reader.GetString(3),
                IndexBytes: reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                /* Every measurement is nullable because a skipped index has none of them, and NULL is the
                   honest representation of "not measured" where 0 would read as a measured emptiness. */
                TreeLevel: reader.IsDBNull(5) ? null : reader.GetInt32(5),
                InternalPages: reader.IsDBNull(6) ? null : reader.GetInt64(6),
                LeafPages: reader.IsDBNull(7) ? null : reader.GetInt64(7),
                EmptyPages: reader.IsDBNull(8) ? null : reader.GetInt64(8),
                DeletedPages: reader.IsDBNull(9) ? null : reader.GetInt64(9),
                AvgLeafDensity: reader.IsDBNull(10) ? null : reader.GetDouble(10),
                LeafFragmentation: reader.IsDBNull(11) ? null : reader.GetDouble(11),
                SkippedReason: reader.IsDBNull(12) ? null : reader.GetString(12),
                IndexOid: reader.IsDBNull(13) ? 0 : reader.GetInt64(13)));

            var row = rows[^1];

            databaseName ??= row.DatabaseName;

            /* A reason IS the complement of the gate, so a null reason means this index was measured -
               and it stays right however pgstatindex renders an odd index (an empty one reports
               avg_leaf_density as NaN, and #3121 nulls it on the way out of the READ, so a measurement
               column is not a reliable "was it measured"). */
            if (row.SkippedReason is not null)
            {
                continue;
            }

            if (!measuredAny
                || row.IndexBytes < cursorBytes
                || (row.IndexBytes == cursorBytes && row.IndexOid > cursorOid))
            {
                cursorBytes = row.IndexBytes;
                cursorOid = row.IndexOid;
                measuredAny = true;
            }
        }

        databaseName ??= context.CurrentDatabaseName;

        if (!string.IsNullOrEmpty(databaseName))
        {
            context.PendingState[RotationCursorKeyPrefix + databaseName] = measuredAny
                ? FormatCursor(cursorBytes, cursorOid)
                : RotationPassCompleteMarker;
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas. Bloat is a level, and the history is what distinguishes an index that has been bloated
           since it was built from one that got that way this month. */
        writer
            .Value(row.DatabaseName)
            .Value(row.SchemaName)
            .Value(row.TableName)
            .Value(row.IndexName)
            .Value(row.IndexBytes)
            .Value(row.TreeLevel)
            .Value(row.InternalPages)
            .Value(row.LeafPages)
            .Value(row.EmptyPages)
            .Value(row.DeletedPages)
            .Value(row.AvgLeafDensity)
            .Value(row.LeafFragmentation)
            .Value(row.SkippedReason);
    }
}
