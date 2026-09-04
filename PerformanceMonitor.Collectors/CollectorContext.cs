/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Host-supplied context a collector definition runs under: the server identity and collection
/// timestamp the host stamps on every row, the delta calculator, and host-configured filters.
/// </summary>
public sealed class CollectorContext
{
    private static readonly IReadOnlySet<string> s_emptySet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// The empty <see cref="State"/> — what a host passes for a definition that declares no state keys
    /// (so it ran no state query), and what a definition sees when nothing has been stored yet.
    /// </summary>
    public static readonly IReadOnlyDictionary<string, string> NoState =
        new Dictionary<string, string>(StringComparer.Ordinal);

    public required int ServerId { get; init; }

    public required string ServerName { get; init; }

    /// <summary>UTC timestamp the host stamps as collection_time; definitions pass it to delta calls.</summary>
    public required DateTime CollectionTime { get; init; }

    public required ICollectorDeltaCalculator Deltas { get; init; }

    /// <summary>Target-server facts the definition may branch on (engine edition, etc.).</summary>
    public CollectorTargetInfo Target { get; init; } = new();

    /// <summary>
    /// The most recent already-collected value of the definition's <c>WatermarkColumn</c>, fetched
    /// by the host from ITS store (Lite: DuckDB; Darling: Postgres) before the query is built.
    /// Null when the definition declares no watermark or nothing was collected yet.
    /// Settable (not init-only) for one host mutation: definitions with a
    /// <c>PerDatabaseWatermarkColumn</c> get this re-fetched per database inside the Azure
    /// per-database loop, right before each per-database <c>BuildQuery</c> (see
    /// <see cref="CurrentDatabaseName"/> and <see cref="EnumerationProbeResult"/> for the other
    /// mid-cycle mutations).
    /// </summary>
    public DateTime? Watermark { get; set; }

    /// <summary>
    /// The database the host's Azure per-database loop is currently reading, set per iteration
    /// (null outside that loop — single-connection runs and non-Azure targets). The XE collectors
    /// use it as the authoritative <c>database_name</c> for rows read on this path: a
    /// database-scoped session only ever captures its own database, so the connection's database IS
    /// the ring buffer's database — while the value parsed from the event XML can be missing
    /// (older graph shapes), and a row with a null database_name could never advance its
    /// database's watermark, re-inserting every cycle until it aged out of the ring buffer.
    /// </summary>
    public string? CurrentDatabaseName { get; set; }

    /// <summary>
    /// The most recent already-collected value of the definition's <c>NumericWatermarkColumn</c>,
    /// fetched by the host from ITS store (Lite: DuckDB; Darling: Postgres) before the query is
    /// built — the numeric (bigint) twin of <see cref="Watermark"/>, for collectors that dedup on a
    /// monotonic identity/sequence column rather than a timestamp (job_history's
    /// <c>sysjobhistory.instance_id</c>, a unique monotonic IDENTITY bigint that survives server-side
    /// purges). Null when the definition declares no numeric watermark or nothing was collected yet.
    /// Purely additive: the timestamp <see cref="Watermark"/> path is unchanged and every existing
    /// collector leaves this null.
    /// </summary>
    public long? NumericWatermark { get; init; }

    /// <summary>
    /// Set by the host ONLY when the definition's <c>WatermarkColumn</c> came back null (the hot store is
    /// empty): true when a prior SUCCESS row for this collector+server exists in the host's collection_log.
    /// Lets a definition whose all-history first-run fallback would re-scan source data already aged out of
    /// the hot store (default_trace_events re-reading .trc files whose events are in Lite's parquet archive,
    /// which <c>v_default_trace_events</c> UNIONs without dedup) tell a TRUE first run (no prior success →
    /// collect all history) from a hot store merely emptied by retention/archival (prior success → a
    /// BOUNDED recent window, never all-history). False in the common case — the host computes it only when
    /// the watermark is null, and only default_trace_events and job_history consult it.
    /// </summary>
    public bool HasCollectedBefore { get; init; }

    /// <summary>
    /// The per-server collector state the host loaded from ITS store (Lite: DuckDB; Darling: Postgres)
    /// for the keys this definition declared in <c>StateKeys</c> — the sibling of <see cref="Watermark"/>
    /// for state no MAX() over the collected rows can produce. Empty when the definition declares no
    /// keys (every collector but default_trace_events), when nothing has been stored yet, or when the
    /// state read failed: a definition MUST treat "absent" as its documented conservative path, because
    /// absent is what a first run, a restarted host, and a broken store all look like.
    /// </summary>
    public IReadOnlyDictionary<string, string> State { get; init; } = NoState;

    /// <summary>
    /// State the definition wants persisted for the NEXT cycle, written during this cycle (typically in
    /// <c>ReadAsync</c>, from a value the query itself returned) and upserted by the host once the cycle
    /// completes — so a cycle that collected zero rows still records what it observed. Nothing is written
    /// for a cycle that threw: the next run then re-reads the older state and takes its conservative
    /// path, which is the safe direction. One of the context members the definition writes back to the
    /// host (the others are <see cref="PerItemTextBudgetExceeded"/> with its
    /// <see cref="PerItemTextBytesShipped"/>/<see cref="PerItemShippedBoundary"/> companions, and
    /// <see cref="CatchupClampApplied"/>).
    /// </summary>
    public Dictionary<string, string> PendingState { get; } = new(StringComparer.Ordinal);

    /// <summary>Wait types excluded from collection (Lite: ignored_wait_types.json — #1240).</summary>
    public IReadOnlySet<string> IgnoredWaitTypes { get; init; } = s_emptySet;

    /// <summary>Per-server excluded database names (spliced via <see cref="DatabaseExclusionFilter"/>).</summary>
    public IReadOnlyList<string> ExcludedDatabases { get; init; } = System.Array.Empty<string>();

    /// <summary>
    /// When true, the query_stats and query_store collectors capture the execution plan text into
    /// their plan column (query_stats.query_plan_xml / query_store_stats.query_plan_text); when
    /// false they leave it NULL and the generated SQL is byte-identical to the no-plan form.
    /// Default false: Lite deliberately never captures plans (they blew out DuckDB/parquet) and
    /// never sets this flag. Darling sets it true — PostgreSQL TOAST compresses the plan text
    /// transparently. This is the shared-collector equivalent of the full Dashboard's per-collector
    /// <c>config.collection_schedule.collect_plan</c> flag (install/08_collect_query_stats.sql,
    /// install/09_collect_query_store.sql).
    /// </summary>
    public bool CapturePlanXml { get; init; }

    /// <summary>
    /// When true, the query_store payload leaves <c>query_sql_text</c> NULL and the host is responsible
    /// for resolving statement text through <see cref="QueryStoreCollector.BuildTextFetchByIdsQuery"/> instead
    /// (#2150). Default false, which keeps the text inline exactly as it ships today.
    ///
    /// <para><b>Why the text has to leave that projection.</b> The payload selects it inside a
    /// <c>TOP ... WITH TIES ... ORDER BY last_execution_time</c>, and a Top-N Sort carries every output
    /// column through the sort while reading ALL of its input before emitting a row — so choosing the rows
    /// to ship materialized <c>nvarchar(max)</c> text for the entire qualifying set. Measured on a
    /// purpose-built Azure SQL DB store with #2210's plan XML already gone, the ONE column being the only
    /// difference: time-to-first-row 4.67s against 0.45s at 1,505 rows, 5.02s against 0.57s at 4,037.
    /// Neither knob bounds it — <c>TOP (500)</c> measured the same as <c>TOP (50000)</c>, and wall time was
    /// flat from a 4 MB to a 256 MB client budget, because the server finishes before the client sees a
    /// byte.</para>
    ///
    /// <para><b>Why this is a flag and not simply removed</b>, which is the part worth being careful about:
    /// Lite stores that text inline in DuckDB and its grid reads it from there, so nulling the column
    /// unconditionally would blind Lite. Gated, the emitted column keeps its ORDINAL either way — same
    /// shape, one value — which is the pattern the version-gated columns in this collector already use, and
    /// it means a host that has not built text storage keeps working unchanged.</para>
    /// </summary>
    public bool FetchQueryTextSeparately { get; init; }

    /// <summary>
    /// Whether this cycle's query_store payload includes the OPEN Query Store interval (#2312). Default
    /// true — today's behavior for every caller that never touches it. When false, the interval
    /// pre-filter adds <c>i.end_time &lt;= SYSUTCDATETIME()</c>, shipping only CLOSED intervals — which
    /// are immutable and therefore final on first collection, while the open interval's cumulative
    /// snapshot is the whole re-read bill on a big primary (40–110 s per run measured). Set PER ITEM by
    /// the hosts' per-database watermark delegates from
    /// <see cref="QueryStoreOpenIntervalState.ShouldIncludeOpenInterval"/>, like <see cref="Watermark"/>;
    /// mutable (not init) for exactly that reason. Only the query_store payload reads it.
    /// </summary>
    public bool IncludeOpenInterval { get; set; } = true;

    /// <summary>
    /// When true (the default — today's behavior), the default_trace_events collector records
    /// Object:Created/Altered/Deleted schema-change (DDL) events; when false its
    /// <c>@include_object_ddl</c> gate drops that entire slice while leaving every other curated
    /// category (file auto-grow/shrink stalls, ErrorLog, security audits, Server Memory Change)
    /// untouched. The shared-collector equivalent of the full Dashboard proc's
    /// <c>@include_object_events</c> (install/29_collect_default_trace.sql). Default true so Lite is
    /// unchanged — it never sets this flag; Darling sets it from darling.json's
    /// <c>collectSchemaChangeEvents</c> to silence the flood a create/drop-happy workload produces
    /// (e.g. HammerDB's TPC-H Query 15 creates and drops a <c>revenue</c> view thousands of times,
    /// and the collector faithfully records every Object:Created/Object:Deleted).
    /// </summary>
    public bool CollectSchemaChangeEvents { get; init; } = true;

    /// <summary>
    /// Host-configured perfmon counter override (Lite: perfmon_counters.json). Null means the
    /// definition's curated default list applies.
    /// </summary>
    public IReadOnlyList<string>? PerfmonCounterOverride { get; init; }

    /// <summary>
    /// Host override for the per-item text byte budget (#2164), in BYTES. Null keeps the definition's
    /// own <see cref="ICollectorDefinition{TRow}.PerItemTextByteBudget"/> — which is what Lite passes,
    /// so its behavior is unchanged. Darling supplies this from the store's operator knob.
    ///
    /// <para>Why a knob at all: the budget's job is bounding memory, and the compile-time 64 MB was
    /// sized for a same-region client. Over a cross-region link the same 64 MB is a MINUTE of the
    /// monitored server holding one query open draining to the client (ASYNC_NETWORK_IO), which is
    /// tenant-visible on small hardware — the 2026-08-10 field case. A smaller budget costs catch-up
    /// latency, never data, because #1960's boundary-group completion makes every cut resumable.</para>
    ///
    /// <para>Composes multiplicatively with the host's fleet sweep width — peak transient is
    /// approximately (concurrent sweeps) × (this budget) — which is why both are operator knobs on
    /// the same store rung and why their UI hints name each other (#2170).</para>
    /// </summary>
    public int? TextByteBudgetOverride { get; init; }

    /// <summary>
    /// Result of the definition's enumeration probe (see
    /// <c>ICollectorDefinition.BuildEnumerationProbe</c>), set by the host between enumeration
    /// and the per-item loop. Null when the definition declares no probe, the probe failed, or
    /// it returned SQL NULL — the definition falls back to its documented default (query_store:
    /// PRODUCTVERSION 13). One of the context members the host mutates mid-cycle (the others are
    /// <see cref="Watermark"/> and <see cref="CurrentDatabaseName"/>, set per database inside the
    /// Azure per-database loop).
    /// </summary>
    public object? EnumerationProbeResult { get; set; }

    /// <summary>
    /// Truncation signal for the per-item text-byte budget (#1556), set by a definition's read method
    /// when it stops reading a database's rows because its
    /// <see cref="ICollectorDefinition{TRow}.PerItemTextByteBudget"/> was reached, and read back by the
    /// host to surface the collection WARNING. Written per database (each definition that enforces a
    /// budget resets it at the top of its read), so the host reads it immediately after the read
    /// returns — on the enumerated path from <c>ReadItemAsync</c>, and on the Azure per-database path
    /// from <c>ReadAsync</c> (#1836). False in the common case — only budgeted collectors touch it.
    /// </summary>
    public bool PerItemTextBudgetExceeded { get; set; }

    /// <summary>
    /// Milliseconds spent waiting for <c>ExecuteReaderAsync</c> to return for the item just read (#2164),
    /// set by the host around the open. Splits a batch's server time into the part the client cannot
    /// influence and the part it can:
    ///
    /// <para>For a multi-statement batch like query_store's staged shape, ADO.NET returns the reader only
    /// when the first ROWSET is available — so this number spans every preceding non-rowset statement (the
    /// <c>SELECT … INTO #pm_qs_slice</c> aggregate) plus the final select's time-to-first-row. The
    /// remaining time, drain, is row streaming the client's byte budget and read loop actually govern.</para>
    ///
    /// <para>Why it exists: cutting the byte budget 64 MB → 12 MB on a production server moved bytes 5x and
    /// the batch clock ~0%, which said the dominant term is upstream of shipping — but the single blended
    /// <c>sql:</c> number could not prove WHICH statement, so any next fix would have been a guess. Zero
    /// when the host does not measure it (Lite today), so a zero must never be read as "instant".</para>
    /// </summary>
    public long PerItemOpenMs { get; set; }

    /// <summary>
    /// True once the host has measured this item's phases (#2854) — the enumerated twin of the
    /// server-scoped <c>ServerPhasesMeasured</c> flag, and set from the same <c>finally</c> that stamps
    /// <see cref="PerItemOpenMs"/> so a faulting open still declares itself measured.
    ///
    /// <para>Exists because the log site previously gated on <c>PerItemOpenMs &gt; 0</c>, which conflates
    /// two opposite states: an open that was genuinely instant, and one that never ran. Both read as zero,
    /// so the split line — the only per-database attribution there is — was suppressed in exactly the case
    /// worth reading. A host that measures nothing leaves this false and prints nothing, which is the
    /// honest answer; a host that measures an instant open prints <c>open:0ms</c>, which is a measurement.</para>
    /// </summary>
    public bool PerItemPhasesMeasured { get; set; }

    /// <summary>
    /// Milliseconds the SERVER-SCOPED read's <c>ExecuteReaderAsync</c> took (#2851) — the twin of
    /// <see cref="PerItemOpenMs"/> for the path that collects a whole server in one query rather than
    /// enumerating databases. Separate from <see cref="PerItemOpenMs"/> rather than reusing it because the
    /// enumerated log site gates on <c>PerItemOpenMs &gt; 0</c>: sharing the field would make a server-scoped
    /// run start printing a per-database line it has no database name for.
    ///
    /// <para>Exists because the largest collector on the fleet could not be attributed at all. procedure_stats
    /// runs 4,900ms p50 on use1 while the same query, run from the same box against the same target, takes
    /// 247ms — an 18.8x gap with the box at 4% CPU, and no way to tell whether it was the execute, the drain,
    /// or neither. Zero when the host does not measure it, so a zero must never be read as "instant" — see
    /// the ServerPhasesMeasured flag that gates the log line.</para>
    /// </summary>
    public long ServerScopeOpenMs { get; set; }

    /// <summary>
    /// Milliseconds the server-scoped <c>ReadAsync</c> took (#2851) — row streaming. Measured on its own
    /// stopwatch rather than inferred as the residual, so that the residual means something: on the
    /// enumerated path drain IS the residual (see <see cref="DrainMsFrom"/>) and therefore silently absorbs
    /// query building, command construction and the supplemental query. Here those are separated out, so a
    /// large residual is a finding about our own code rather than an artifact of the definition.
    /// </summary>
    public long ServerScopeDrainMs { get; set; }

    /// <summary>
    /// Rows the server-scoped drain actually delivered (#2864), counted by
    /// <see cref="DrainCountingDataReader"/> rather than by the collector's own loop.
    ///
    /// <para>Distinct from the run's stored row count, and that distinction is the point: an abandoned cycle
    /// ships nothing, so <c>rows_collected</c> is 0 whether the target sent no rows at all or sent 149 and
    /// then went quiet. Those two are a stalled target and a stalled stream, and they want different fixes.
    /// -1 means the host did not wrap the reader — a value no real count can take, so "not measured" cannot
    /// be misread as "delivered nothing".</para>
    /// </summary>
    public long ServerScopeRowsRead { get; set; } = -1;

    /// <summary>
    /// UTF-16 payload bytes the server-scoped drain delivered (#2864) — the string and binary getters only,
    /// NOT the wire size, which no <c>DbDataReader</c> exposes. Honest scope and the useful one: the
    /// collectors this exists for are dominated by a single large text column, so string bytes are the
    /// payload to within a rounding error. -1 when unmeasured, for the reason above.
    /// </summary>
    public long ServerScopeBytesRead { get; set; } = -1;

    /// <summary>
    /// The drain stopwatch's reading at the LAST successful read (#2864). Subtract it from
    /// <see cref="ServerScopeDrainMs"/> and you have how long the reader sat with nothing arriving, which is
    /// the figure that separates a slow stream from a stalled one — both of which otherwise end at the
    /// wall-clock budget with a positive row count. -1 when no row ever arrived OR when unmeasured; the row
    /// count above disambiguates those two, and 0 is left free because "row 1 arrived instantly" is a real
    /// answer that must not collide with a sentinel.
    /// </summary>
    public long ServerScopeLastReadMs { get; set; } = -1;

    /// <summary>
    /// The monitored target's own session identifier for this run (#2864) — SQL Server's SPID, PostgreSQL's
    /// backend pid. Null when the provider does not expose one.
    ///
    /// <para>Read from the open connection as a property, NOT by issuing <c>SELECT @@SPID</c>: a round trip
    /// per collector per server per cycle is ~25,000 extra queries an hour against the fleet to record a
    /// number the client already has. The value is what makes a stalled run joinable to everything else the
    /// tool collects — <c>waiting_tasks</c>, <c>dmv_blocking_snapshot</c> and <c>query_snapshots</c> all
    /// carry a session id, so without it a retrospective "what was OUR session waiting on" cannot be asked
    /// even where the snapshot that would answer it was captured.</para>
    /// </summary>
    public int? TargetSessionId { get; set; }

    /// <summary>
    /// Milliseconds the item's watermark refresh took (#2164), set by the host when it runs one. This is NOT
    /// server think-time or streaming — for query_store it is a STORE read (and on the catch-up/adaptive
    /// path a store write too), yet the driver's <c>sql:</c> stopwatch starts before it. Measured so it can
    /// be subtracted rather than silently inflating drain, which would corrupt the one number this
    /// instrumentation exists to make trustworthy. Zero when the host runs no per-item watermark.
    /// </summary>
    public long PerItemWatermarkMs { get; set; }

    /// <summary>
    /// Milliseconds the item's separate plan-XML fetch took (#2312 investigation), set by the host that
    /// runs one (Darling; Lite has no separate fetch and leaves it zero). The fetch is INSIDE the driver's
    /// per-item <c>sql:</c> stopwatch but is neither open nor drain — it is its own query against
    /// <c>sys.query_store_plan</c>, and on a database with a huge Query Store catalog it can dominate the
    /// whole item (ayr-01: a 0-row closed-only cycle still cost 298s, and the blended number could not say
    /// where). Measured so drain stops absorbing it, exactly the #2164 argument one seam further down.
    /// </summary>
    public long PerItemPlanFetchMs { get; set; }

    /// <summary>
    /// Milliseconds the item's separate statement-text fetch took (#2150's fetch, split out for the #2312
    /// investigation) — same contract as <see cref="PerItemPlanFetchMs"/>: inside <c>sql:</c>, not drain,
    /// zero when the host runs no separate fetch.
    /// </summary>
    public long PerItemTextFetchMs { get; set; }

    /* #2811: the fetch phases above are each a whole METHOD, not a query. FetchAndStorePlansAsync
       round-trips the store to learn what is already held, issues at most two target statements, and writes
       the results back — two of those three steps are Postgres. A 189,562ms plan_fetch on a production
       database was read as SQL Server query time for a full day and tuned on that premise; the tuning was
       measured at 0.508s in isolation and moved production nothing, because nobody could see which step held
       the time. These sub-phases exist so that question is answered by the log rather than by argument. Same
       zero-means-unmeasured contract as their parent.

       #2819 removed a fourth step this comment used to open with. The method acquired its own store
       connection, and that acquisition — not the query — was the phase: the probe SQL measured 0.39ms while
       the phase measured 673-6,663ms, and a zero-id cycle issuing no SQL at all still cost 673ms. Both
       fetches now borrow the connection the collector body already holds, so no acquisition happens here and
       probe: below is the round trip alone. Kept in the record rather than edited away, because the reason
       these fields exist is that a phase whose documentation outlived what it measures is how the cost was
       misread in the first place. */

    /// <summary>
    /// Milliseconds of <see cref="PerItemPlanFetchMs"/> spent on the touch/probe round trip that decides
    /// which plans are still owed — the STORE half that runs before any target query is issued.
    /// <para>Round trip ONLY since #2819: the fetch borrows the collector body's connection rather than
    /// opening one, so this no longer carries a connection acquisition. It used to, and that acquisition was
    /// the overwhelming majority of it — treat any pre-#2819 reading of this field as mostly pool wait.</para>
    /// </summary>
    public long PerItemPlanProbeMs { get; set; }

    /// <summary>
    /// Milliseconds of <see cref="PerItemPlanFetchMs"/> spent in the TARGET statements only, summed across
    /// chunks: <c>ExecuteReaderAsync</c> plus the row-read loop against <c>sys.query_store_plan</c>. This is
    /// the number a hint on that query can move, and the only one comparable to a benchmark of the statement.
    /// </summary>
    public long PerItemPlanTargetMs { get; set; }

    /// <summary>
    /// Milliseconds of <see cref="PerItemPlanFetchMs"/> spent writing the fetched plans back to the store.
    /// </summary>
    public long PerItemPlanWriteMs { get; set; }

    /// <summary>Target statements issued for the plan fetch (chunk count) — 0, 1 or 2 at today's cap.</summary>
    public int PerItemPlanChunks { get; set; }

    /// <summary>
    /// Plan ids the fetch attempted this pass. Paired with <see cref="PerItemPlanTargetMs"/> it gives a
    /// per-id cost, which is the only honest way to compare a cold production pass against a benchmark run
    /// over hot recently-executed plans — the two differ by orders of magnitude and the totals alone hide it.
    /// </summary>
    public int PerItemPlanIdsAttempted { get; set; }

    /// <summary>
    /// Plan references handed to the touch/probe round trip this pass — the probe's INPUT size, which is
    /// what <see cref="PerItemPlanProbeMs"/> actually scales with (measured ~0.61ms per reference, dead
    /// linear across 78/272/847-reference passes). Deliberately separate from
    /// <see cref="PerItemPlanIdsAttempted"/>, which counts only the ids that came back MISSING and were
    /// then fetched from the target: the two differ by orders of magnitude on a healthy database, and a
    /// pass that probes 847 references and finds nothing owed logs zero attempted ids while doing half a
    /// second of store work. #2819 and #2822 both divided probe cost by the attempted-id count and drew a
    /// phantom "140x gap" from it (#2823); this field is the honest denominator.
    /// </summary>
    public int PerItemPlanProbeIds { get; set; }

    /// <summary>Store-half of <see cref="PerItemTextFetchMs"/> — the touch/probe round trip. Same contract as
    /// <see cref="PerItemPlanProbeMs"/>, including borrowing the body's connection rather than opening one (#2819).</summary>
    public long PerItemTextProbeMs { get; set; }

    /// <summary>Target-half of <see cref="PerItemTextFetchMs"/>. Same contract as <see cref="PerItemPlanTargetMs"/>.</summary>
    public long PerItemTextTargetMs { get; set; }

    /// <summary>Store-write half of <see cref="PerItemTextFetchMs"/>. Same contract as <see cref="PerItemPlanWriteMs"/>.</summary>
    public long PerItemTextWriteMs { get; set; }

    /// <summary>Target statements issued for the text fetch (chunk count).</summary>
    public int PerItemTextChunks { get; set; }

    /// <summary>Query ids the text fetch attempted this pass. Same purpose as <see cref="PerItemPlanIdsAttempted"/>.</summary>
    public int PerItemTextIdsAttempted { get; set; }

    /// <summary>Query references handed to the text touch/probe round trip. Same purpose and same reason
    /// for existing separately as <see cref="PerItemPlanProbeIds"/>.</summary>
    public int PerItemTextProbeIds { get; set; }

    /// <summary>
    /// The part of <see cref="PerItemPlanFetchMs"/> that is neither probe, target, nor write — the method's
    /// own bookkeeping (candidate capping, the size estimator, the carry-over set maths). Exists so the
    /// sub-split SUMS to its parent by construction rather than approximately: a residual that has to be
    /// inferred by subtracting printed terms is exactly the ambiguity this whole change exists to remove, and
    /// a large value here is itself the finding (the cost would be in our own code, not in either database).
    /// Clamped at zero for the same stopwatch-skew reason as <see cref="DrainMsFrom"/>.
    /// </summary>
    public long PlanFetchOtherMs =>
        Math.Max(0, PerItemPlanFetchMs - PerItemPlanProbeMs - PerItemPlanTargetMs - PerItemPlanWriteMs);

    /// <summary>Text-fetch twin of <see cref="PlanFetchOtherMs"/>.</summary>
    public long TextFetchOtherMs =>
        Math.Max(0, PerItemTextFetchMs - PerItemTextProbeMs - PerItemTextTargetMs - PerItemTextWriteMs);

    /// <summary>
    /// The item's row-STREAMING time: the driver's blended per-item total minus the phases that are not
    /// streaming (<see cref="PerItemWatermarkMs"/>, <see cref="PerItemOpenMs"/>,
    /// <see cref="PerItemPlanFetchMs"/>, <see cref="PerItemTextFetchMs"/>). Lives here rather than at
    /// the log site so the subtraction has exactly one definition and a test can pin the shipped arithmetic
    /// instead of a copy of it. Clamped at zero: the phases are measured on separate stopwatches, so tiny
    /// skew must never surface as negative drain.
    /// </summary>
    public long DrainMsFrom(long itemSqlMs) =>
        Math.Max(0, itemSqlMs - PerItemOpenMs - PerItemWatermarkMs - PerItemPlanFetchMs - PerItemTextFetchMs);

    /// <summary>
    /// Cumulative text bytes the budgeted read actually materialized for the item just read (#1960),
    /// reset and written alongside <see cref="PerItemTextBudgetExceeded"/>. Read by the host purely
    /// for the bounded-cycle WARNING, so a long catch-up reports how much each cycle shipped rather
    /// than just that it was cut.
    /// </summary>
    public long PerItemTextBytesShipped { get; set; }

    /// <summary>
    /// The watermark-column value of the LAST row the budgeted read kept for the item just read
    /// (#1960) — under oldest-first shipping, the exact boundary the next cycle resumes from. Reset
    /// and written alongside <see cref="PerItemTextBudgetExceeded"/>; null when the read kept no
    /// rows or the boundary row carried no watermark value. Read by the host for the bounded-cycle
    /// WARNING.
    /// </summary>
    public DateTime? PerItemShippedBoundary { get; set; }

    /// <summary>
    /// Catch-up signal (#1836): set by a definition whose cutoff computation floored a stale watermark
    /// to the <see cref="WatermarkPolicy"/> horizon, so the host can log the bounded history hole the
    /// clamp deliberately creates. Only query_store clamps, and only the Azure per-database branch
    /// needs this: on the enumeration path the HOST clamps (and logs) before the definition ever sees
    /// the watermark, so the definition's own clamp is a no-op there and this stays false — the same
    /// signal cannot produce a duplicate warning. Written per database, at query build time, so the
    /// host reads it right after building that database's query.
    /// </summary>
    public bool CatchupClampApplied { get; set; }
}
