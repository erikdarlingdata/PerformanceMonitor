/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The Fleet-wide NOC roll-up read (#1562) — the enriched per-server Overview CARD and the cross-server rollup
/// lifted out of the WPF-only <c>ViewerDataService.Overview.cs</c> / <c>.Fleet.cs</c> into a service-side reader
/// so the SAME reads power the web dashboard's <c>/api/fleet</c>, the <c>get_fleet_overview</c> MCP tool, and
/// (via the shared <see cref="ServerHealthClassifier"/> banding) the WPF viewer. STORED reads only, no live
/// monitored-server hit.
///
/// <para><b>Scale lens (the plan's R6).</b> The WPF Overview fans out N servers x ~8 per-server reads on every
/// refresh; that does not scale to a 500-server central store. Because Darling keys every row by
/// <c>server_id</c> in ONE Postgres database, this reader instead runs a BOUNDED set of cross-server reads
/// (one per-server newest-row probe per metric, driven from the registry in one statement; one
/// <c>GROUP BY server_id</c> windowed count per incident source; one cross-server collection-health aggregate)
/// — a fixed ~9 round-trips regardless of fleet size — then assembles the per-server cards in C#.</para>
///
/// <para><b>Every read is bounded by the fleet, never by the retention (#3895).</b> The newest-row reads were
/// <c>DISTINCT ON (server_id)</c> over the whole table, which reads, decompresses and sorts every retained row
/// to keep one per server; the windowed counts were bounded on the EVENT's timestamp, which no chunk is
/// partitioned on. Both grew with servers x retained days, and a 43-server store measured 12.3 s cold for one
/// overview. Now each newest-row read is a per-server <c>LATERAL ... LIMIT 1</c> (one index descent in the
/// live chunk for a server that is collecting) and each windowed count also carries
/// <see cref="EventWindowFloor"/> on the partition column. <c>FleetReadsAreBoundedByTheFleetTests</c> holds
/// every statement here to one of those two shapes.</para>
///
/// <para><b>Banding lives once.</b> Every band (per-metric severity, the card's overall band, collection
/// freshness, the fleet band + worst-first score) comes from <see cref="ServerHealthClassifier"/> in
/// PerformanceMonitor.Common — the SAME classifier the WPF cards use — so the thresholds are defined in exactly
/// one place. The fleet blocking / deadlock totals are the SUM of the per-server card counts (each already
/// applying Lite's per-server XE-preferred / DMV-fallback rule), so the totals reconcile with the cards by
/// construction; no separate totals query is needed.</para>
/// </summary>
internal static class DarlingFleetReader
{
    /// <summary>Shared serializer options for the fleet DTOs — snake_case field names come from the DTOs'
    /// <c>[JsonPropertyName]</c> attributes, enum bands serialize as their string names, and the output is
    /// COMPACT (#2350, matching the MCP tool convention). ONE options object so the web endpoint and the MCP
    /// tool serialize the identical shape.</summary>
    public static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = false,
        Converters = { new JsonStringEnumConverter() },
    };

    /* ─────────────────────────── cross-server SQL (public for dialect pinning) ─────────────────────────── */

    /// <summary>The enabled fleet — the servers the roll-up cards cover, from the registry the worker upserts on
    /// each first connect (the same source <c>DarlingServerResolver</c> resolves against, so every card is
    /// drillable via <c>/api/read/{tool}?server=</c>). <c>sql_engine_edition</c> is the raw probed
    /// SERVERPROPERTY('EngineEdition') the worker stamped on connect (5 = Azure SQL DB, 8 = Azure MI, box
    /// editions otherwise), the reliable per-server platform signal the composer's D4 auto-greying keys on;
    /// nullable when a server has not yet connected.
    ///
    /// <para><c>engine_kind</c> (V82, #2530) is the other engine axis, and the one the edition cannot carry:
    /// a PostgreSQL target has no <c>SERVERPROPERTY</c>, so it lands at edition 0 exactly like a SQL Server
    /// that has never connected. Riding on the SAME registry row costs no extra round-trip, which is what
    /// keeps this reader's bounded fan-out bounded.</para>
    ///
    /// <para>The <c>is_silenced</c> column (#2031) is the SQL mirror of the Viewer's
    /// <c>ViewerDataService.IsWholeServerSilence</c> predicate — an enabled, unexpired mute rule scoped to the
    /// server (matched case-insensitively on the same COALESCE(display, storage) name the card shows, which is
    /// the name the Viewer's Silence writes) with NO narrowing pattern on any other field. Display-only: the
    /// web seat has no silence action; this exists so a dataless-quiet server and a silenced one stop looking
    /// identical on the fleet cards and to <c>get_fleet_overview</c>.</para> $ none.</summary>
    public const string FleetServersSql = @"
SELECT s.server_id, COALESCE(s.display_name, s.server_name) AS display_name, s.server_name, s.sql_engine_edition, s.engine_kind,
       EXISTS
       (
           SELECT 1
           FROM config_mute_rules m
           WHERE lower(m.server_name) = lower(COALESCE(s.display_name, s.server_name))
           AND   m.enabled
           AND   (m.expires_at_utc IS NULL OR m.expires_at_utc > (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'))
           AND   m.metric_name IS NULL
           AND   m.database_pattern IS NULL
           AND   m.query_text_pattern IS NULL
           AND   m.wait_type_pattern IS NULL
           AND   m.job_name_pattern IS NULL
       ) AS is_silenced
FROM servers s
WHERE s.is_enabled
ORDER BY s.server_name";

    /// <summary>Every (server, tag) assignment — one row per tag a server carries — for the fleet cards'
    /// read-only tag pills (#2020). Ordered by the tag's sort order then name so a card's pills are stable;
    /// <c>colour</c> is the stored <c>#RRGGBB</c> or NULL (an uncoloured tag renders as a neutral pill, the same
    /// as the desktop apps — no palette is resolved here). Bare table names resolve through the store's
    /// search_path to <c>config.server_tag_map</c> / <c>config.server_tags</c>. $ none.</summary>
    public const string FleetTagsSql = @"
SELECT m.server_id, t.id, t.name, t.colour
FROM server_tag_map m
JOIN server_tags t ON t.id = m.tag_id
ORDER BY m.server_id, t.sort_order, lower(t.name)";

    /// <summary>The full tag forest — every tag with its parent and colour — for the web fleet's read-only
    /// tree/group rendering (#2020). Ordered by sort order then name so siblings render stably. Separate from the
    /// per-server <see cref="FleetTagsSql"/> because the tree needs the WHOLE hierarchy: an organisational parent
    /// tag with no directly-assigned servers must still nest its children correctly, exactly as the desktop
    /// FleetView does (it is handed the whole tag list). $ none.</summary>
    public const string FleetTagForestSql = @"
SELECT id, name, parent_id, sort_order, colour
FROM server_tags
ORDER BY sort_order, lower(name)";

    /// <summary>The driver predicate of the four newest-row reads over SQL Server-only collector tables
    /// (<see cref="FleetCpuSql"/>, <see cref="FleetMemorySql"/>, <see cref="FleetMemoryPressureSql"/>,
    /// <see cref="FleetThreadsSql"/>): every registry row EXCEPT the ones the registry positively says are
    /// PostgreSQL (#3895).
    ///
    /// <para><b>Why exclude anyone.</b> A per-server probe for a server with no row in the table has to walk
    /// every retained chunk before it can say so, and on an uncompressed chunk the planner may take that walk
    /// through the time index, filtering the whole chunk to find nothing: on DARLING01 the two PostgreSQL
    /// targets were 16 of the 17 ms the memory probe cost. A PostgreSQL target never has a row here — its CPU
    /// is <see cref="FleetPgCpuSql"/>'s, and it has no ring buffer, grant semaphores or schedulers to collect —
    /// so skipping it changes no card, and a store monitoring a few SQL Servers beside fifty PostgreSQL
    /// clusters does not pay fifty walks per metric.</para>
    ///
    /// <para><b>Only on a positive claim.</b> A NULL kind (never stamped) and an unrecognised one are probed,
    /// the direction that cannot lose a SQL Server's reading. Normalised the way
    /// <see cref="MonitoredEngineKind.IsPostgres"/> normalises — trim, then lowercase — so the SQL and the C#
    /// give the same answer about the same row.</para></summary>
    internal const string SqlServerCollectedTargetSql =
        "(s.engine_kind IS NULL OR lower(btrim(s.engine_kind)) NOT IN ('"
        + MonitoredEngineKind.Postgres + "', '" + MonitoredEngineKind.AuroraPostgres + "'))";

    /// <summary>Latest SQL + other-process CPU per server (newest ring-buffer sample). $ none.
    ///
    /// <para>The <c>collection_time</c> key, matching <see cref="FleetMemorySql"/> below, is here for the
    /// FRAME as well as for the cost. <c>sample_time</c> is the monitored server's local wall clock, so
    /// leading on it invites the bound that looks like the obvious optimisation and returns zero rows for
    /// every server behind the store. Ordering carries no clock frame; see <c>DarlingWorker.LatestCpuSql</c>
    /// for the full reasoning and <c>LatestCpuReadShapeSqlTests</c> for the guard.</para>
    ///
    /// <para><b>One probe per server, not one sort of the table</b> (#3895). This was <c>DISTINCT ON
    /// (server_id)</c> over the whole view, which has no per-server <c>LIMIT</c> and so reads, decompresses and
    /// sorts every retained row whatever the time key is: 260,291 rows, 26 MB and 188 ms (plus 63 ms of
    /// planning) on DARLING01's nine servers, 1.2 s on a 43-server, thirty-day rig. Driven from the registry,
    /// each server gets its own <c>LIMIT 1</c>, so TimescaleDB's ordered ChunkAppend walks that server's
    /// chunks newest-first and stops at the first row: 28 buffers and 0.6 ms on DARLING01, 3 ms at 43
    /// servers, and no longer a function of how many days are retained. The rows are identical: the registry
    /// is where the cards come from, and <see cref="SqlServerCollectedTargetSql"/> skips only servers that
    /// never write this table.</para>
    /// </summary>
    public const string FleetCpuSql = $@"
SELECT
    s.server_id,
    latest.sqlserver_cpu_utilization,
    latest.other_process_cpu_utilization
FROM servers AS s
CROSS JOIN LATERAL
(
    SELECT
        sqlserver_cpu_utilization,
        other_process_cpu_utilization
    FROM v_cpu_utilization_stats
    WHERE server_id = s.server_id
    ORDER BY collection_time DESC, sample_time DESC
    LIMIT 1
) AS latest
WHERE s.is_enabled
AND   {SqlServerCollectedTargetSql}";

    /// <summary>Latest instance CPU per PostgreSQL/Aurora target — the newest Performance Insights sample
    /// per server, within the freshness bound. $1 the freshness cutoff (naive UTC).
    ///
    /// <para><b>Why a second CPU read rather than a wider first one</b> (#3267). <see cref="FleetCpuSql"/>
    /// reads <c>v_cpu_utilization_stats</c>, which the SQL Server ring-buffer collector writes and a
    /// PostgreSQL target never has a row in; instance CPU for those targets lands in
    /// <c>collect.pg_cpu_utilization</c> from the AWS API (#2719), a different table with different
    /// columns. Nothing joins them and nothing should — the two carry different detail (see
    /// <see cref="FleetCpuSource"/>) and the SQL Server path is deliberately untouched.</para>
    ///
    /// <para><b>Both predicates are load-bearing, and they are not the same predicate.</b>
    /// <c>collection_time</c> is the hypertable's partition dimension, so it is the one that lets
    /// TimescaleDB chunk-exclude — the same reasoning as <see cref="FleetLastCollectionSql"/>, and the
    /// mistake to avoid is a bare <c>DISTINCT ON</c> that reads this table's whole retained history on
    /// every fleet call. <c>sample_time</c> is what the reading is ABOUT, and it is the honest freshness
    /// test: a cycle backfills up to <c>RdsCpuIngestor.LookbackWindow</c> of points under one
    /// <c>collection_time</c>, so a row inside the collection bound can still carry a sample older than
    /// it. Unlike the SQL Server arm, comparing <c>sample_time</c> to a store-derived instant is
    /// frame-correct here: Performance Insights stamps its data points in UTC and the ingestor stores them
    /// naive-UTC unchanged, where the ring buffer's <c>sample_time</c> is the monitored server's LOCAL wall
    /// clock (see <c>LatestCpuReadShapeSqlTests</c> for what a bound on that one costs).</para>
    ///
    /// <para><b>Falling out of this read is the correct answer, unlike
    /// <see cref="FleetLastCollectionSql"/>'s 48 hours.</b> That read's value is "when did we last see this
    /// server", so a long-dark server that falls out of it must still read Offline rather than never
    /// collected (#3935, <see cref="ClassifyWindowedFreshness"/>). A CPU LEVEL is the opposite: a
    /// four-hour-old reading is not this server's CPU now, and the card says so by carrying
    /// null and banding Unknown. The bound is <c>DarlingPgCpuUtilizationReader.Freshness</c> — three times
    /// the collector's own 5-minute cadence, so two missed cycles are tolerated — and it is that constant
    /// rather than a new number so the card and the High CPU alert agree on what "current" means.</para>
    ///
    /// <para><c>cpu_percent IS NOT NULL</c> because Performance Insights returns a data point with a null
    /// value for a period it has no sample for, and the ingestor stores it; the newest row is not
    /// necessarily the newest MEASUREMENT.</para>
    ///
    /// <para><b>The capacity columns come off the SAME row, and are deliberately not filtered on</b>
    /// (#3281). <c>acu_utilization_percent</c> is what the CPU band actually reads, so a second
    /// <c>IS NOT NULL</c> on it would silently drop a whole card's CPU reading in order to find an older
    /// row that happened to have a capacity sample — trading a current measurement for a stale one. A NULL
    /// capacity on the newest CPU row is the honest answer and bands Unknown.</para></summary>
    public const string FleetPgCpuSql = @"
SELECT DISTINCT ON (server_id)
    server_id,
    cpu_percent,
    acu_utilization_percent,
    max_configured_acu
FROM pg_cpu_utilization
WHERE collection_time >= $1
AND   sample_time >= $1
AND   cpu_percent IS NOT NULL
ORDER BY server_id, collection_time DESC, sample_time DESC";

    /// <summary>Latest total server memory + buffer pool (MB) per server. $ none. The per-server probe
    /// <see cref="FleetCpuSql"/> describes (#3895): 48.5 ms plus 42.4 ms of planning on DARLING01 as a
    /// <c>DISTINCT ON</c> over every retained row, one index descent per collecting server now.</summary>
    public const string FleetMemorySql = $@"
SELECT
    s.server_id,
    latest.total_server_memory_mb,
    latest.buffer_pool_mb
FROM servers AS s
CROSS JOIN LATERAL
(
    SELECT
        CAST(total_server_memory_mb AS double precision) AS total_server_memory_mb,
        CAST(buffer_pool_mb AS double precision) AS buffer_pool_mb
    FROM v_memory_stats
    WHERE server_id = s.server_id
    ORDER BY collection_time DESC
    LIMIT 1
) AS latest
WHERE s.is_enabled
AND   {SqlServerCollectedTargetSql}";

    /// <summary>Latest resource-semaphore pressure per server — grant waiters / timeout + forced-grant deltas /
    /// granted MB, summed across every pool at each server's newest grant-snapshot instant. $ none.
    ///
    /// <para><b>Two probes per server, both bounded</b> (#3895): the newest instant, by the same per-server
    /// <c>LIMIT 1</c> <see cref="FleetCpuSql"/> uses (it needs only the key, so it is an index-only descent),
    /// then the pools AT that instant by equality, which TimescaleDB excludes to the one chunk holding it at
    /// run time. This was a <c>MAX(collection_time)</c> over every retained row joined back to the same
    /// table: 9,537 buffers and 256.6 ms on DARLING01, where the two probes take 44 buffers and 2.3 ms. The
    /// price is planning, because each probe plans the table's chunk list: 15.9 ms against the join's 8.0 ms
    /// warm. A server with no grant snapshot at all has no instant and so no row, exactly as the join had
    /// none.</para></summary>
    public const string FleetMemoryPressureSql = $@"
SELECT
    s.server_id,
    pressure.waiter_count,
    pressure.timeout_error_count,
    pressure.forced_grant_count,
    pressure.granted_memory_mb
FROM servers AS s
CROSS JOIN LATERAL
(
    SELECT collection_time
    FROM v_memory_grant_stats
    WHERE server_id = s.server_id
    ORDER BY collection_time DESC
    LIMIT 1
) AS latest
CROSS JOIN LATERAL
(
    SELECT
        CAST(COALESCE(SUM(m.waiter_count), 0) AS bigint) AS waiter_count,
        CAST(COALESCE(SUM(m.timeout_error_count_delta), 0) AS bigint) AS timeout_error_count,
        CAST(COALESCE(SUM(m.forced_grant_count_delta), 0) AS bigint) AS forced_grant_count,
        CAST(COALESCE(SUM(m.granted_memory_mb), 0) AS double precision) AS granted_memory_mb
    FROM v_memory_grant_stats AS m
    WHERE m.server_id = s.server_id
    AND   m.collection_time = latest.collection_time
) AS pressure
WHERE s.is_enabled
AND   {SqlServerCollectedTargetSql}";

    /// <summary>Latest worker-thread pressure per server (newest cpu_scheduler_stats snapshot). $ none. The
    /// per-server probe <see cref="FleetCpuSql"/> describes (#3895): 37.6 ms plus 46.0 ms of planning on
    /// DARLING01 as a <c>DISTINCT ON</c> over every retained row.
    ///
    /// <para>#3936: the tiebreak is <c>collection_id DESC</c>, not a second <c>collection_time</c>. A
    /// run-overlap or clock-resolution collision can store two DIFFERENT snapshots under one
    /// <c>collection_time</c> for one server — <c>collection_id</c> is the per-process monotonic counter
    /// every row already carries (<see cref="PerformanceMonitor.Collectors.CollectionIdGenerator"/>), so it
    /// is the one column guaranteed to order two same-instant rows the same way on every read, instead of a
    /// bare <c>LIMIT 1</c> returning either one depending on physical row order.</para></summary>
    public const string FleetThreadsSql = $@"
SELECT
    s.server_id,
    latest.max_workers_count,
    latest.total_current_workers_count,
    latest.total_runnable_tasks_count,
    latest.total_work_queue_count
FROM servers AS s
CROSS JOIN LATERAL
(
    SELECT
        max_workers_count,
        total_current_workers_count,
        total_runnable_tasks_count,
        total_work_queue_count
    FROM v_cpu_scheduler_stats
    WHERE server_id = s.server_id
    ORDER BY collection_time DESC, collection_id DESC
    LIMIT 1
) AS latest
WHERE s.is_enabled
AND   {SqlServerCollectedTargetSql}";

    /// <summary>Blocking in the window per server from BOTH sources — XE blocked-process reports and the always-on
    /// DMV snapshot, each counted per <c>server_id</c> with its worst wait, lined up by a FULL OUTER JOIN so a
    /// server present in only one source still appears. The caller applies Lite's XE-preferred / DMV-fallback per
    /// server. $1 window start, $2 window end, $3 the <see cref="EventWindowFloor"/> for $1 (all naive UTC).
    ///
    /// <para>$3 is the partition-column bound the event window cannot supply (#3895): without it both scans
    /// open every retained chunk to count the last hour. It cannot drop a row the window wants —
    /// <see cref="EventWindowFloor"/> says why.</para></summary>
    public const string FleetBlockingSql = @"
SELECT
    COALESCE(xe.server_id, dmv.server_id) AS server_id,
    COALESCE(xe.cnt, 0) AS xe_count,
    COALESCE(xe.max_wait, 0) AS xe_max_wait,
    COALESCE(dmv.cnt, 0) AS dmv_count,
    COALESCE(dmv.max_wait, 0) AS dmv_max_wait
FROM
(
    SELECT server_id, COUNT(*) AS cnt, MAX(wait_time_ms) AS max_wait
    FROM v_blocked_process_reports
    WHERE event_time >= $1
    AND   event_time <= $2
    AND   collection_time >= $3
    GROUP BY server_id
) AS xe
FULL OUTER JOIN
(
    SELECT server_id, COUNT(*) AS cnt, MAX(wait_time_ms) AS max_wait
    FROM v_dmv_blocking_snapshots
    WHERE event_time >= $1
    AND   event_time <= $2
    AND   collection_time >= $3
    GROUP BY server_id
) AS dmv ON xe.server_id = dmv.server_id";

    /// <summary>Deadlocks in the window per server — count and newest deadlock instant (for each card's
    /// "last seen" detail). $1 window start, $2 window end, $3 the <see cref="EventWindowFloor"/> for $1 (all
    /// naive UTC) — the partition-column bound <see cref="FleetBlockingSql"/> carries, for its reason (#3895):
    /// 131 ms over every retained chunk on DARLING01 without it.
    ///
    /// <para><b>This view is the SQL Server extended-event capture and nothing else</b> (#3017).
    /// <c>v_deadlocks</c> is <c>SELECT * FROM deadlocks</c>, and <c>deadlocks</c> is written by exactly one
    /// collector — <c>DeadlocksCollector</c>, whose <c>TargetTable</c> it is. A PostgreSQL target's deadlocks
    /// never reach it, so this count is structurally zero for a PostgreSQL server no matter how many
    /// deadlocks its clusters have; <see cref="FleetPgDeadlockSql"/> is that engine's read (#3539), and
    /// <see cref="BuildCard"/> takes one or the other by engine. Zero is also what a genuinely quiet SQL
    /// Server reports, which is why the total ships with <see cref="FleetDeadlockCoverage"/> beside it: the
    /// reading that needs no action and the reading whose collector is silent are otherwise the same
    /// character.</para></summary>
    public const string FleetDeadlockSql = @"
SELECT server_id, COUNT(*) AS cnt, MAX(deadlock_time) AS last_seen
FROM v_deadlocks
WHERE deadlock_time >= $1
AND   deadlock_time <= $2
AND   collection_time >= $3
GROUP BY server_id";

    /// <summary>Deadlocks in the window per PostgreSQL server (#3539) — the same two columns as
    /// <see cref="FleetDeadlockSql"/>, from the engine's own counter rather than a captured graph. $1 window
    /// start, $2 window end (both naive UTC).
    ///
    /// <para><b>A counter DIFFERENCE, never a sum of the column.</b> <c>pg_database_stats.deadlocks</c> is
    /// <c>pg_stat_database.deadlocks</c> stored raw: a lifetime counter per database, sampled every minute,
    /// so the same 122 sits in every one of a day's 1,440 rows and <c>SUM(deadlocks)</c> over a window is a
    /// number with no meaning (measured on one store: 8,006,912 across 50 servers with zero new deadlocks in
    /// the window). What happened IN the window is each consecutive pair's difference, taken per
    /// <c>(server_id, database_name)</c> series — the same <c>LAG</c> shape <c>DarlingPgDatabaseReader.PgDatabaseSql</c>
    /// uses for <c>get_pg_database_stats</c>, and the two must agree on a server or the fleet card would
    /// contradict the tool it sends a reader to.</para>
    ///
    /// <para><b>Clamped at zero across a reset.</b> <c>pg_stat_reset()</c> or a crash restart rewinds the
    /// counter, and a plain difference goes negative there; <c>GREATEST(…, 0)</c> drops that interval
    /// rather than subtracting a lifetime from the window. The interval's real deadlocks (those between the
    /// reset and the next sample) survive as the next difference. The tool reports how many intervals
    /// clamped; this card does not — a band is not the place for a reset count, and
    /// <c>get_pg_database_stats</c> is one call away.</para>
    ///
    /// <para><b>Summed across databases</b> because the card is per SERVER and the SQL Server count it
    /// sits beside is too: a deadlock graph names the databases involved, but <c>v_deadlocks</c> is counted
    /// per <c>server_id</c>, and the band's tiers were measured per server-hour (#3368). The NULL-named
    /// shared-relation row PostgreSQL emits is its own series under <c>PARTITION BY</c> (grouping
    /// semantics), so it differences correctly and is summed in.</para>
    ///
    /// <para><b><c>last_seen</c> is the sample that first showed the increase</b> — the deadlock happened
    /// somewhere in the preceding minute, which is the resolution a per-minute counter has. Bounded to the
    /// window like the count, where the SQL Server "last seen" is the newest graph in the window too; the
    /// unbounded "ever" the viewer's card shows for SQL Server has no cheap PostgreSQL twin (it would be a
    /// scan of the whole counter series for its last step), and "last within the window" is what the
    /// count-carrying chip actually renders.</para>
    ///
    /// <para><b><c>intervals</c> is how many differences were taken</b> — the count of consecutive-sample
    /// pairs across every series in the window — and it is what makes the count a MEASUREMENT. A difference
    /// needs two samples; a server with one row in the window, or none, has had no difference taken, and
    /// its <c>cnt</c> of zero is the arithmetic of an empty set, not an observation that nothing deadlocked.
    /// This is where the PostgreSQL arm parts from the SQL Server one on purpose: <c>COUNT(*)</c> over an
    /// event table with no events IS an observation (the collector looked and found none), where
    /// <c>LAG</c> over no samples is undefined. <see cref="BuildCard"/> bands only when this is positive,
    /// which is also what keeps #3539 A6 intact — an online PostgreSQL target nothing has collected from
    /// measures nothing and reads Warning, not "Healthy — 1 of 6 measured".</para>
    ///
    /// <para>Bounded on <c>collection_time</c>, the hypertable's partitioning column, so chunk exclusion
    /// keeps the read to the window's chunk(s): on the one-minute cadence the default hour is ~60 rows per
    /// database per server, and the fleet's whole hour is tens of thousands of rows behind the
    /// <c>(server_id, collection_time)</c> index — the same order as <see cref="FleetBlockingSql"/>'s
    /// two scans.</para></summary>
    public const string FleetPgDeadlockSql = @"
WITH sampled AS
(
    SELECT
        server_id,
        collection_time,
        deadlocks - LAG(deadlocks) OVER (PARTITION BY server_id, database_name ORDER BY collection_time) AS raw_delta
    FROM pg_database_stats
    WHERE collection_time >= $1
    AND   collection_time <= $2
)
SELECT
    server_id,
    CAST(coalesce(SUM(GREATEST(raw_delta, 0)), 0) AS bigint) AS cnt,
    MAX(collection_time) FILTER (WHERE raw_delta > 0) AS last_seen,
    CAST(count(raw_delta) AS bigint) AS intervals
FROM sampled
GROUP BY server_id";

    /// <summary>The deadlock health band's two tiers from the singleton settings row (#3368, V120).
    ///
    /// <para>Read through the same VIEWER-role pool every other fleet read uses, and no window parameter:
    /// this is the current configuration, not history. <c>config_alert_settings</c> is the store's one
    /// control-plane row, so the tiers sit beside every other operator-settable threshold rather than in a
    /// second settings table an operator would have to know to look in.</para></summary>
    public const string FleetDeadlockRateThresholdSql = @"
SELECT deadlock_warn_per_hour, deadlock_critical_per_hour
FROM config_alert_settings
WHERE id = 1";

    /// <summary>Newest collection time per server — drives each card's freshness status. $1 window start
    /// (<see cref="LastCollectionWindowStart"/>).
    /// Bounded (not a bare GROUP BY over the whole table) so TimescaleDB can chunk-exclude: this table only
    /// grows, and every collector run adds a row, so an unbounded MAX(collection_time) over ALL history was
    /// re-scanning the server's ENTIRE collection archive (millions of rows) on every fleet-overview call just
    /// to find a timestamp from the last few minutes — the exact "materialize a bound, don't scan the whole
    /// history" mistake fixed elsewhere today (pg_statement_stats #2691, pg_wait_stats #2695). The window is
    /// 48 hours, not the OfflineThreshold this feeds: a server genuinely offline for HOURS must still
    /// report its true last-seen time (age computed correctly, still bands Offline) rather than falling out of
    /// the result entirely and being treated as having no history at all.
    ///
    /// <para>Excludes <c>server_id = 0</c>, the fleet-maintenance run-record sentinel
    /// (<c>DarlingObservability.FleetServerId</c>) — the retention purge and the oversized-plan backlog sweep
    /// write their per-run records there because they are fleet-wide and have no one server to attribute a
    /// run to. It is not a real server, so it must not appear as a phantom group a key-iterating consumer
    /// could render as "server 0"; the Viewer's twin of this read
    /// (<c>ViewerDataService.ServerFreshnessSql</c>) already carries the same clause. The rollup happens to
    /// read this map only by <c>TryGetValue</c> on a registry id today, so the phantom is currently inert —
    /// which is exactly why the guard belongs in the SQL rather than in that reading habit.</para>
    ///
    /// <para><b>A bound is not a limit, and the 48 hours alone still read two days of the fleet's biggest
    /// table</b> (#3895). As a <c>GROUP BY</c> the window aggregated every collector run of every server in it
    /// — 490,940 rows and 103 MB on DARLING01's nine servers, for eleven timestamps — so it is now a
    /// per-server <c>LIMIT 1</c> driven from the enabled registry, an index-only descent per server (0.25 ms
    /// there; 160 ms to 1 ms on a 43-server rig). The window is KEPT, for the paragraph above, and the planner
    /// still excludes every chunk older than two days before it starts.</para>
    ///
    /// <para><b>Nothing in the window is not "never collected"</b> (#3935). A server dark for longer than the
    /// window reached the card as a null and read "Awaiting first collection", banded Warning, while the WPF
    /// viewer, whose per-server read has no window, called the same server Offline. Dropping the window would
    /// have fixed the word at a price paid on every call: on DARLING01's 19 chunks the probe with no window
    /// plans in 89–101 ms cold and 3.1–3.3 ms warm (8,937 and 273 planning buffers), against 9.8–13.1 ms and
    /// 0.6–0.8 ms here (894 and 43), and it grows with the 60-day retention. So the window stays and the row
    /// says what the window cannot: it is a LEFT JOIN, one row for EVERY enabled registry server, carrying
    /// the registry's <c>created_date</c> — the server's first successful connect, written once by the
    /// upsert — beside a newest collection that is null when the window holds none.
    /// <see cref="ClassifyWindowedFreshness"/> turns that pair into Offline or never-collected. A null the
    /// read positively reported is the only miss that can become Offline: a server absent from the result
    /// altogether (disabled between <see cref="FleetServersSql"/> and this read) keeps the no-claim reading,
    /// which is the lesson of the rotating false-Offlines this read's null fallback was written for.</para></summary>
    public const string FleetLastCollectionSql = @"
SELECT
    s.server_id,
    latest.collection_time AS last_collection_time,
    s.created_date
FROM servers AS s
LEFT JOIN LATERAL
(
    SELECT collection_time
    FROM v_collection_log
    WHERE server_id = s.server_id
    AND   collection_time >= $1
    ORDER BY collection_time DESC
    LIMIT 1
) AS latest ON TRUE
WHERE s.is_enabled
AND   s.server_id <> 0";

    /// <summary>How far back <see cref="FleetLastCollectionSql"/> looks for a server's newest collection: two
    /// days, deliberately far wider than <see cref="ServerHealthThresholds.OfflineThreshold"/> (the statement
    /// says why).</summary>
    internal static readonly TimeSpan LastCollectionWindow = TimeSpan.FromHours(48);

    /// <summary>The instant <see cref="FleetLastCollectionSql"/>'s $1 is bound to for a roll-up at
    /// <paramref name="now"/>, as naive UTC. ONE derivation for the bind and for
    /// <see cref="ClassifyWindowedFreshness"/>, so the window the read searched and the window the card reasons
    /// about cannot drift apart.</summary>
    internal static DateTime LastCollectionWindowStart(DateTime now) =>
        DateTime.SpecifyKind(now - LastCollectionWindow, DateTimeKind.Unspecified);

    /// <summary>
    /// A card's collection freshness from <see cref="FleetLastCollectionSql"/>'s row (#3935): the shared
    /// registration rule (<see cref="ServerHealthClassifier"/>'s <c>ClassifyFreshness</c> overload, #3967) with
    /// this read's window as what it could see. A newest collection inside the window bands on the ladder; with
    /// none, a server first connected BEFORE the window reads Offline and one first connected inside it reads
    /// awaiting its first collection, which is exact because the worker collects only after connecting (on
    /// DARLING01 every one of the 17 registry rows has its first retained collection after its
    /// <c>created_date</c>).
    ///
    /// <para><b>The window, not the retention, is what this read could see.</b> So a server that connected more
    /// than two days ago and has never written a <c>collection_log</c> row reads Offline here, while the
    /// viewer and <c>list_servers</c>, whose reads have no window, can still prove it never collected and say
    /// so. That case is rare: every collector run writes a row whatever its outcome, so it takes a connect no
    /// sweep followed. A server dark past the retention reads Offline on every surface.</para>
    ///
    /// <para><b>A null registration keeps the ladder's reading.</b> The service writes <c>created_date</c> on
    /// every registry insert; a row without one was inserted by hand. The same holds for a server the read did
    /// not report at all, which the caller passes as two nulls.</para>
    /// </summary>
    /// <param name="lastCollection">The newest collection inside the window, or null when the window holds
    /// none.</param>
    /// <param name="registeredAt">The registry's <c>created_date</c> from the same row, or null.</param>
    /// <param name="now">The roll-up's reference instant, the one the window was cut from.</param>
    internal static ServerFreshness ClassifyWindowedFreshness(DateTime? lastCollection, DateTime? registeredAt, DateTime now) =>
        ServerHealthClassifier.ClassifyFreshness(lastCollection, registeredAt, LastCollectionWindowStart(now), now);

    /// <summary>Cross-server per-collector 7-day health aggregate — one row per (server, collector) pair carrying
    /// the columns the shared <c>CollectorHealth.HealthStatus</c> banding needs, so the caller counts each
    /// server's FAILING collectors exactly as the per-server Collection Health tab does. $1 window start (the
    /// trailing 7 days, naive UTC). <c>last_run_time</c> (any status, not just success) feeds the STOPPED band
    /// — a collector that has gone dark entirely (its AppliesTo gate flipped off, say) must not read as
    /// FAILING just because its last SUCCESS is old; a collector still being invoked and erroring every cycle
    /// has a recent last_run_time and correctly stays FAILING.
    ///
    /// <para>Excludes the <c>server_id = 0</c> fleet-maintenance sentinel for the reason
    /// <see cref="FleetLastCollectionSql"/> gives, and one further one that is specific to this read: the
    /// rows there are NOT collector runs, so banding them through <c>CollectorHealth.HealthStatus</c> would
    /// apply a staleness ladder built for a per-server cadence to a fleet-wide maintenance pass that has
    /// none.</para></summary>
    public const string FleetCollectionHealthSql = $@"
SELECT
    server_id,
    collector_name,
    COUNT(*) AS total_runs,
    -- #2926: SUCCESS excludes an abandonment that predates #2803, so the Success column beside
    -- Abandoned cannot count the same run twice. Post-#2803 rows need no exclusion - ABANDONED
    -- is not SUCCESS - and an ordinary empty run stays counted, which is what the COALESCE in
    -- the shared predicate is for: NULL under this NOT would have dropped it.
    SUM(CASE WHEN status = 'SUCCESS'
              AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql}
             THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS error_count,
    MAX(CASE WHEN status IN ('SUCCESS', 'SKIPPED') THEN collection_time END) AS last_success_time,
    SUM(CASE WHEN status = 'PERMISSIONS' THEN 1 ELSE 0 END) AS permission_denied_count,
    MAX(collection_time) AS last_run_time,
    -- #2804: the fleet rollup bands through the SAME CollectorHealth.HealthStatus as the per-server
    -- grid, so it has to feed the classifier the same inputs. Left unselected, AbandonedCount would
    -- default to 0 here and this count alone would keep calling a partially-abandoning collector
    -- HEALTHY while every other surface called it WARNING -- and it would COMPILE, because the
    -- default is silent. That is the #2779/#2784 failure shape: one surface fixed, its sibling
    -- quietly left on the old reading.
    --
    -- #2926: keyed on the ROW, not on the status alone. collection_log is append-only, so a
    -- window can still hold cycles written before #2803 gave abandonment its own status:
    -- status = 'SUCCESS' beside rows_collected = 0 and the budget note. Counted by status
    -- alone this read 0 for them, and the collector banded HEALTHY while losing cycles - a
    -- filter correct against current writes and silently wrong against older ones, failing in
    -- the reassuring direction. The pattern is one LIKE because the budget is INTERPOLATED and
    -- the shipped values differ (120 s for procedure_stats/query_stats/plan_correction, 600 s
    -- for query_store), so equality against one rendered sentence matches one collector.
    SUM(CASE WHEN {EnumeratedCollectorDriver.AbandonedRunPredicateSql}
             THEN 1 ELSE 0 END) AS abandoned_count,
    -- #3240: same reasoning as abandoned_count above — this rollup bands through the SAME shared
    -- classifier as the per-server surfaces, and an unselected count defaults to 0, COMPILES, and
    -- would band an extension-missing collector FAILING here (no success, staleness path) while every
    -- other surface says EXTENSION_MISSING. APPENDED, read positionally.
    SUM(CASE WHEN status = 'EXTENSION_MISSING' THEN 1 ELSE 0 END) AS extension_missing_count,
    -- #3819: the two instants that, with last_run_time above, say whether this collector STOPPED
    -- producing rather than never having produced here. The fleet card counts the rows where they do
    -- (regressed_collector_count), so an install that flips a productive collector to a named skip is a
    -- NUMBER on the card and in the install countersign rather than a status word nobody re-reads.
    --
    -- Both are plain aggregates, which is the whole reason the fleet read can ask this at all: the
    -- per-server twin resolves the same predicate through its existing ranked subquery, but this
    -- statement has none, and adding one to hold a window function would put a sort of the fleet's whole
    -- 7-day collection_log in front of a GROUP BY that hashes today. #3735 memoized this read because
    -- three concurrent copies of it crossed the mcp role's 15 s statement_timeout; buying a boolean with
    -- a fleet-wide sort would spend that headroom again. Nothing here needs the streak's WIDTH, only
    -- whether the newest run is inside one, and two MAX()es answer that.
    --
    -- A NULL status counts as non-skip, like the per-server twin: it is not one of the declared skip
    -- words, and reading it as one would let an unwritten status manufacture a streak. APPENDED, read
    -- positionally.
    MAX(CASE WHEN status IS NULL
              OR status NOT IN ({CollectorRuntimePrecondition.NamedSkipStatusSqlList})
             THEN collection_time END) AS last_non_skip_time,
    MAX(CASE WHEN rows_collected > 0 THEN collection_time END) AS last_productive_time,
    -- #3885: the instant the current zero-row-SUCCESS streak began after -- the newest run that was NOT a
    -- success storing nothing. With last_run_time above and the collector's shipped cadence, this is what
    -- CollectorHealthClassifier.EstimateTrailingZeroRowSuccessRuns turns into the run count the
    -- produced-then-stopped predicate reads, so the fleet's regressed_collector_count includes a collector
    -- that stopped PRODUCING and not only one that started SKIPPING. The measured case is the reason the
    -- card needs it: job_history went dark on 41 of 43 servers, every run SUCCESS, and the number on the
    -- card an install countersign reads stayed 0 for a fortnight.
    --
    -- ONE plain aggregate, deliberately, and an ESTIMATE downstream rather than the per-server read's exact
    -- width: that one counts runs off a ranked subquery it already has, and this statement has none. Adding
    -- one to hold a window function would put a sort of the fleet's whole 7-day collection_log in front of
    -- a GROUP BY that hashes today -- and #3735 memoized this read precisely because three concurrent
    -- copies of it crossed the mcp role's 15 s statement_timeout. The estimate's error direction and its
    -- bound are documented on the classifier; it reads HIGH across a saturated sweep, never low, which is
    -- the only direction safe for a count whose job is to stop this class hiding under HEALTHY.
    --
    -- The abandonment exclusion is success_count's own (#2926): a pre-#2803 abandoned cycle is stored as
    -- SUCCESS with zero rows plus the budget note, and that is data loss rather than a quiet source.
    -- APPENDED, read positionally.
    MAX(CASE WHEN NOT (status = 'SUCCESS'
                       AND COALESCE(rows_collected, 0) = 0
                       AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql})
             THEN collection_time END) AS last_zero_row_streak_break_time
FROM v_collection_log
WHERE collection_time >= $1
AND   server_id <> 0
GROUP BY server_id, collector_name";

    /* ─────────── #3893 arm 2: the same read, composed from the hourly aggregate + a raw head slice ───────────
       Moved to CollectionHealthRollupSupport in Storage (#4226) so the viewer's ViewerDataService reads the
       SAME guard and composer instead of carrying its own raw-only fleet scan. The members below are thin
       wrappers — same names, same values, same behavior — so this file's own pins (FreshStoreWatermarkTests,
       CollectionHealthAggregateTests, FleetCollectionHealthMemoTests) needed no rewrite beyond the one
       source-text scan for the guard's catch clause, which moved to Storage with the method it guards. */

    /// <summary>See <see cref="CollectionHealthRollupSupport.RollupProbeSql"/>.</summary>
    internal const string CollectionHealthRollupProbeSql = CollectionHealthRollupSupport.RollupProbeSql;

    /// <summary>See <see cref="CollectionHealthRollupSupport.MaterializedWatermarkFloor"/>.</summary>
    internal static readonly DateTime MaterializedWatermarkFloor = CollectionHealthRollupSupport.MaterializedWatermarkFloor;

    /// <summary>See <see cref="CollectionHealthRollupSupport.WatermarkSql"/>.</summary>
    internal const string CollectionHealthWatermarkSql = CollectionHealthRollupSupport.WatermarkSql;

    /// <summary>See <see cref="CollectionHealthRollupSupport.ContinuitySql"/>.</summary>
    internal const string CollectionHealthContinuitySql = CollectionHealthRollupSupport.ContinuitySql;

    /// <summary>The raw read restricted to the partial head hour [$1, $2): <see cref="FleetCollectionHealthSql"/>
    /// itself with one predicate inserted (<see cref="CollectionHealthRollupSupport.InsertHeadBound"/>), DERIVED
    /// rather than restated, so the head slice can never drift from the raw read's eleven aggregates (a pin
    /// holds the insertion to exactly one place).</summary>
    internal static readonly string FleetCollectionHealthHeadSliceSql =
        CollectionHealthRollupSupport.InsertHeadBound(FleetCollectionHealthSql);

    /// <summary>
    /// <see cref="FleetCollectionHealthSql"/>'s result, served from <c>collect.collection_health_hourly</c>
    /// (<see cref="CollectionHealthRollupSupport.ComposeFleetSql"/>): every WHOLE hour bucket from $2 (the first
    /// hour boundary at or after the window start $1) UNION ALL the raw head slice [$1, $2), re-aggregated per
    /// (server, collector). Same thirteen ordinals, same names, same types (the SUMs cast back to bigint), so
    /// the reader below cannot tell which statement it ran. The aggregate is <c>materialized_only = false</c>:
    /// buckets above its watermark are computed real-time from raw, so the result is current to the second.
    /// </summary>
    internal static readonly string FleetCollectionHealthComposedSql =
        CollectionHealthRollupSupport.ComposeFleetSql(FleetCollectionHealthSql);

    /// <summary>The first hour boundary at or after <paramref name="windowStart"/> — where whole buckets begin.
    /// See <see cref="CollectionHealthRollupSupport.CeilingHour"/>.</summary>
    internal static DateTime CeilingHour(DateTime windowStart) => CollectionHealthRollupSupport.CeilingHour(windowStart);

    /// <summary>
    /// Chooses the statement for the seven-day collection-health read: the composed one only when both guards
    /// pass, else the exact raw scan. See <see cref="CollectionHealthRollupSupport.RollupUsableAsync"/> for (a)
    /// the ABSENT guard and (b) the CONTINUITY guard.
    /// </summary>
    internal static Task<bool> CollectionHealthRollupUsableAsync(
        NpgsqlDataSource postgres, DateTime headEnd, CancellationToken cancellationToken) =>
        CollectionHealthRollupSupport.RollupUsableAsync(postgres, headEnd, cancellationToken);

    /// <summary>The default depth of the worst-first "Needs attention" ranking.</summary>
    public const int DefaultWorstCount = 5;

    /// <summary>How long a completed 7-day collection-health scan is served from memory before the next
    /// caller re-reads it (#3735) — 60 seconds. The floor of what the rollup could report differently: it
    /// counts per-collector RUNS over seven days, and the fastest collector cadence is one minute, so between
    /// two reads a minute apart at most one run per collector has landed and no band the shared
    /// <see cref="CollectorHealth.HealthStatus"/> ladder computes over a week of them can have moved. Shorter
    /// would re-run a 7-day aggregate to learn nothing; longer would make the fleet's collector dots lag
    /// behind <c>get_collection_health</c>, which reads the same rows uncached. See
    /// <see cref="CollectionHealthMemo"/> for what the memo is protecting against.</summary>
    internal static readonly TimeSpan CollectionHealthMemoLifetime = TimeSpan.FromSeconds(60);

    /// <summary>One <see cref="CollectionHealthMemo"/> per <see cref="NpgsqlDataSource"/>, weakly keyed so a
    /// disposed data source takes its memo with it. Per data source rather than a bare static for two reasons:
    /// the MCP host and the web host each build their OWN data source (<c>DarlingMcpHostService</c> /
    /// <c>DarlingWebHostService</c>, different roles, different pools), so each host holds its own memo and
    /// one scan per minute per HOST is the bound — not one per process, which would hand the web host a rollup
    /// the mcp role read; and gated-live tests spin several stores in one process, and a static shared across
    /// them would bleed one store's collection health into another's (the same reason
    /// <c>ComposeStoreAvailability</c> keys its cache this way).</summary>
    private static readonly ConditionalWeakTable<NpgsqlDataSource, CollectionHealthMemo> s_collectionHealthMemos = new();

    /// <summary>The memo the 7-day collection-health rollup for <paramref name="postgres"/> is served through
    /// (#3735). Internal so a live test can ask the memo how many scans three racing overview calls actually
    /// cost the store.</summary>
    internal static CollectionHealthMemo CollectionHealthMemoFor(NpgsqlDataSource postgres) =>
        s_collectionHealthMemos.GetOrCreateValue(postgres);

    /* ─────────────────────────── the read ─────────────────────────── */

    /// <summary>
    /// Rolls the whole enabled fleet up in a bounded set of cross-server reads: the pre-banded per-server cards,
    /// the band counts, the cross-server blocking / deadlock totals (summed from the cards), and the worst-first
    /// ranking. <paramref name="windowStartUtc"/>..<paramref name="windowEndUtc"/> bound the blocking / deadlock
    /// counts (the caller passes the same window the cards imply); <paramref name="nowUtc"/> is the freshness
    /// reference (defaults to <see cref="DateTime.UtcNow"/>).
    /// </summary>
    public static async Task<FleetOverviewResult> GetFleetOverviewAsync(
        NpgsqlDataSource postgres,
        DateTime windowStartUtc,
        DateTime windowEndUtc,
        DateTime? nowUtc = null,
        int worstCount = DefaultWorstCount,
        CancellationToken cancellationToken = default)
    {
        var now = nowUtc ?? DateTime.UtcNow;

        var servers = await ReadServersAsync(postgres, cancellationToken);

        var cpu = await ReadCpuAsync(postgres, cancellationToken);
        var pgCpu = await ReadPgCpuAsync(postgres, now, cancellationToken);
        var memory = await ReadMemoryAsync(postgres, cancellationToken);
        var memoryPressure = await ReadMemoryPressureAsync(postgres, cancellationToken);
        var threads = await ReadThreadsAsync(postgres, cancellationToken);
        var blocking = await ReadBlockingAsync(postgres, windowStartUtc, windowEndUtc, cancellationToken);
        var deadlocks = await ReadDeadlocksAsync(postgres, windowStartUtc, windowEndUtc, cancellationToken);
        var pgDeadlocks = await ReadPgDeadlocksAsync(postgres, windowStartUtc, windowEndUtc, cancellationToken);
        var lastCollection = await ReadLastCollectionAsync(postgres, now, cancellationToken);
        /* #3735: the ONE read in this fan-out that does not depend on the caller's window — the 7-day
           collection-health aggregate is the same statement whatever hours_back was — and therefore the one
           that is single-flighted and memoized. Every other read above and below stays a per-call read: each
           either carries the window (and, #3895, a partition-column floor for it) or is a per-server
           newest-row probe that costs one index descent per collecting server. See CollectionHealthMemo for
           the production photo this answers. */
        var (failingCollectors, collectionHealthAgeSeconds) = await CollectionHealthMemoFor(postgres).GetAsync(
            now,
            (scanNow, scanToken) => ReadFailingCollectorCountsAsync(postgres, scanNow, scanToken),
            cancellationToken);
        var tags = await ReadTagsAsync(postgres, cancellationToken);
        var tagForest = await ReadTagForestAsync(postgres, cancellationToken);
        /* #3368: the deadlock band's tiers, read ONCE per roll-up rather than per card. A store reload can
           hot-swap the settings row mid-read, and re-reading per server would let one roll-up band some
           servers on the old pair and the rest on the new one — a mixed reading no configuration ever held,
           and the band counts and the worst-first ranking would be derived from it. */
        var deadlockTiers = await ReadDeadlockRateThresholdsAsync(postgres, cancellationToken);

        var cards = new List<FleetServerCard>(servers.Count);
        foreach (var server in servers)
        {
            cpu.TryGetValue(server.ServerId, out var c);
            /* A miss leaves default(PgCpuRow) — three nulls, which is the reading ("no current instance
               CPU, and no capacity sample") and not a measurement. TryGetValue into a double would have
               left 0.0, which is why the row is a struct of nullables rather than three plain doubles. */
            pgCpu.TryGetValue(server.ServerId, out var pg);
            memory.TryGetValue(server.ServerId, out var m);
            memoryPressure.TryGetValue(server.ServerId, out var mp);
            threads.TryGetValue(server.ServerId, out var t);
            blocking.TryGetValue(server.ServerId, out var b);
            deadlocks.TryGetValue(server.ServerId, out var deadlock);
            /* A miss leaves default(PgDeadlockRow) - zero count, no last-seen, ZERO intervals - which for
               a PostgreSQL target is "no difference was taken" (unmeasured, not quiet), and for a SQL
               Server is a row BuildCard never looks at. */
            pgDeadlocks.TryGetValue(server.ServerId, out var pgDeadlock);
            /* The row is a struct of NULLABLES, so a miss is two nulls rather than a value. This map used
               to hold a bare DateTime, and `TryGetValue(..., out var lastColl)` then left default(DateTime)
               (0001-01-01) on a miss — not null, so ClassifyFreshness skipped its NeverCollected branch,
               computed an age of ~2000 years and banded Offline: the rotating false-Offlines on
               actively-collecting servers the null fallback was written for. #3935 keeps that lesson: a
               server the read reported with an empty window reads Offline or never-collected by its
               registration (ClassifyWindowedFreshness); a server the read did not report at all keeps the
               no-claim reading. */
            lastCollection.TryGetValue(server.ServerId, out var collected);
            failingCollectors.TryGetValue(server.ServerId, out var collectors);
            tags.TryGetValue(server.ServerId, out var serverTags);

            cards.Add(BuildCard(
                server, c, pg, m, mp, t, b, deadlock, pgDeadlock, collected.LastCollection, collectors, serverTags, now,
                windowEndUtc - windowStartUtc, deadlockTiers, collected.RegisteredAt));
        }

        return BuildRollup(cards, now, windowStartUtc, windowEndUtc, worstCount, tagForest, collectionHealthAgeSeconds);
    }

    /// <summary>Builds one pre-banded card from a server's raw cross-server reads (pure — the reduction the WPF
    /// <c>ServerSummaryItem</c> does, minus the brushes, over the shared classifier).
    ///
    /// <para><b>Internal, not private</b>, for the reason <see cref="BuildRollup"/> is public: this is the
    /// step that decides what a card CLAIMS, so it is worth asserting without a store. #3267 was a defect in
    /// exactly this reduction — a whole engine's cards carrying null where a reading existed — and the only
    /// tests that could see it before were the live-Postgres ones. A caller with no interest in a given
    /// metric passes <c>default</c> for its row, which is why those types never have to be NAMED in a test
    /// — they are internal only because CS0051 requires every parameter type of an internal method to be
    /// at least as accessible as it.</para></summary>
    /// <param name="deadlock">The SQL Server extended-event count for the window (<see cref="FleetDeadlockSql"/>);
    /// structurally zero for a PostgreSQL target and ignored for one.</param>
    /// <param name="pgDeadlock">The PostgreSQL counter-difference count for the window
    /// (<see cref="FleetPgDeadlockSql"/>, #3539), with how many differences it was summed from;
    /// structurally empty for a SQL Server and ignored for one. Two parameters rather than one pre-merged
    /// row so this method — the step that decides what a card CLAIMS — is where the engine picks, and a
    /// test can hand a card BOTH rows and assert which one it believed.</param>
    /// <param name="lastCollection">The newest collection inside <see cref="FleetLastCollectionSql"/>'s
    /// window, or null when it holds none.</param>
    /// <param name="registeredAt">The registry's <c>created_date</c> off the same row (#3935), which tells a
    /// null <paramref name="lastCollection"/> apart: registered before the window is Offline, registered
    /// inside it is awaiting its first collection. Trailing and defaulted to null, which keeps the shared
    /// ladder's reading, so a caller that only exercises the metric rows need not invent a registration.</param>
    internal static FleetServerCard BuildCard(
        FleetServerRow server,
        CpuRow cpu,
        PgCpuRow pgCpu,
        MemoryRow memory,
        MemoryPressureRow pressure,
        ThreadsRow threads,
        BlockingRow blocking,
        DeadlockRow deadlock,
        PgDeadlockRow pgDeadlock,
        DateTime? lastCollection,
        CollectorCounts collectors,
        List<FleetTag>? tags,
        DateTime now,
        TimeSpan deadlockWindow,
        DeadlockRateThresholds deadlockTiers,
        DateTime? registeredAt = null)
    {

        /* Lite's XE-preferred / DMV-fallback, per server: XE when it has any row this window, else the DMV
           snapshot — both count and worst-wait come from whichever source wins. */
        var blockingCount = blocking.XeCount > 0 ? blocking.XeCount : blocking.DmvCount;
        var maxBlockingWaitMs = blocking.XeCount > 0 ? blocking.XeMaxWait : blocking.DmvMaxWait;

        var cpuPercent = cpu.SqlCpu;
        var otherCpu = cpu.OtherCpu;

        /* Per-server target ENGINE (#2530), the axis the platform flags below cannot express: they are all
           derived from a SQL Server SERVERPROPERTY, which a PostgreSQL target does not have. Read up here
           rather than beside ClassifyPlatform because the CPU source classification needs it. */
        var (isPostgres, isAurora) = ClassifyEngineKind(server.EngineKind);

        /* The engine's OWN deadlock reading (#3539): the extended-event graph count for a SQL Server, the
           pg_stat_database counter difference for a PostgreSQL target. Chosen by engine rather than summed,
           because the other engine's row is a structural zero and a sum would hide which instrument
           answered; the card says which through deadlock_source. The collector band travels with the
           count for the same reason - coverage asks about the collector that produced THIS number. */
        var deadlockCount = isPostgres ? pgDeadlock.Count : deadlock.Count;
        var deadlockLastSeen = isPostgres ? pgDeadlock.LastSeen : deadlock.LastSeen;
        var deadlockCollectorBand = isPostgres ? collectors.PgDeadlockBand : collectors.DeadlockBand;
        /* Whether the count is a MEASUREMENT. On SQL Server it always is: COUNT(*) over the graph table is
           an observation even at zero (#3272's engine-not-collector rule). On PostgreSQL the count is a
           counter DIFFERENCE, and a difference of fewer than two samples is not zero deadlocks, it is no
           reading - see FleetPgDeadlockSql's intervals paragraph. */
        var deadlockMeasured = !isPostgres || pgDeadlock.Intervals > 0;

        /* One expression for "total non-idle host CPU, from whichever collector has it" (#3267), shared with
           the viewer's card so the two cannot drift on the fallback. cpuPercent stays the SQL-Server-process
           share and is NOT filled from the PostgreSQL arm: Performance Insights publishes only the host
           total, so there is no per-process split to claim, and cpu_source says which arm answered rather
           than leaving a consumer to infer it from which fields are null. */
        var instanceCpuPercent = pgCpu.InstanceCpuPercent;
        var totalCpu = FleetCpuProvenance.TotalNonIdleCpuPercent(cpuPercent, otherCpu, instanceCpuPercent);
        var cpuSource = FleetCpuProvenance.ClassifyCpuSource(cpuPercent, instanceCpuPercent, isPostgres, isAurora);
        var cpuForAlert = totalCpu ?? cpuPercent;

        var availableThreads = threads.TotalThreads.HasValue
            ? threads.TotalThreads.Value - (threads.CurrentWorkers ?? 0)
            : (int?)null;

        var hasMemoryPressure = pressure.WaiterCount > 0 || pressure.TimeoutCount > 0 || pressure.ForcedCount > 0;
        var maxBlockedSeconds = maxBlockingWaitMs / 1000.0;

        /* The two DMV-sourced readings, with "not measured" expressed as null for an engine that has no
           row in the views behind them (#3272). The reads above produced zeros for such a target, and a
           zero here argued Healthy — a green dot for a metric nothing measured. The published COUNTS are
           left exactly as they are: the fleet coverage block explains a total built out of those zeros, and
           nulling them would make the total's own denominator unreadable. It is the BAND that stops
           claiming health.

           Deadlocks left this set in #3539: both engines now have a source behind the reading (the graph
           capture or the server counter). The SQL Server arm keeps the engine-not-collector rule
           ServerMetricSources states - a silent deadlocks collector reads zero and bands Healthy, with
           deadlock_source and the coverage block disclosing the gap. The PostgreSQL arm is gated on the
           instrument instead: its count is a difference, and a window with fewer than two samples per
           series has had no difference taken, so the band reads Unknown there rather than a Healthy
           computed from an empty set. That is also what keeps #3539 A6's card - an online PostgreSQL
           target nothing has collected from - measuring nothing. */
        var memoryPressureForBand = ServerMetricSources.DmvSourced(hasMemoryPressure, isPostgres);
        var blockingForBand = ServerMetricSources.DmvSourced(blockingCount, isPostgres);
        int? deadlocksForBand = deadlockMeasured ? deadlockCount : null;

        var metrics = new ServerHealthMetrics
        {
            CpuPercentForAlert = cpuForAlert,
            /* #3281: the band reads percent of the CONFIGURED ceiling on the Performance Insights arm,
               because cpuForAlert there is percent of an allocation that moves. The source is what decides
               which, so it travels with the two numbers rather than being re-derived. */
            CapacityUtilizationPercent = pgCpu.AcuUtilizationPercent,
            CpuSource = cpuSource,
            HasMemoryPressure = memoryPressureForBand,
            BlockingCount = blockingForBand,
            MaxBlockedSeconds = maxBlockedSeconds,
            /* #3539 A3: the blocking count's own denominator — the same card window the deadlock count was
               read over, because one pair of bounds windowed both reads. */
            BlockingWindow = deadlockWindow,
            DeadlockCount = deadlocksForBand,
            /* #3368: the count's own denominator and the store's tiers travel WITH it, because the band is
               a rate. Omitting either would leave the deadlock dot banded on one pair of numbers while the
               overall band and the fleet score used another — the contradiction #3281 fixed on the CPU
               arm, one metric over. */
            DeadlockWindow = deadlockWindow,
            DeadlockRateThresholds = deadlockTiers,
            TotalThreads = threads.TotalThreads,
            AvailableThreads = availableThreads,
            ThreadsWaitingForCpu = threads.RunnableTasks,
            RequestsWaitingForThreads = threads.WorkQueue,
            FailedCollectorCount = collectors.Failing,
            /* #3539 A8d: the denominator that grades the failing count into a share. */
            CollectorCount = collectors.Total,
        };

        /* Freshness -> the card's collection state, through the SAME mapping the WPF card and the sidebar
           row use (#2473). It was a hand-written copy of ApplyFreshness that happened to agree; the copy on
           the sidebar row happened not to, which is the argument for none of them writing it out. #3935: the
           band itself comes from the shared ladder too, with the one departure a windowed read needs — an
           empty window on a server registered before it is Offline, not "never collected". */
        var freshness = ClassifyWindowedFreshness(lastCollection, registeredAt, now);
        var flags = ServerCollectionStatusRules.FlagsFor(freshness);
        var isOnline = flags.IsOnline;
        var awaitingFirstCollection = flags.AwaitingFirstCollection;
        var collectionStale = flags.CollectionStale;

        var overall = ServerHealthClassifier.OverallMetricSeverity(metrics);
        var band = ServerHealthClassifier.ClassifyBand(isOnline, awaitingFirstCollection, collectionStale, overall);
        /* #3528: the fold above skips Unknown, so an online server with five of six metrics structurally
           Unknown still bands Healthy. The counts ride the card so every consumer of the band label can
           qualify it ("Healthy — 1 of 6 measured") instead of rendering an unqualified green. */
        var (measuredMetrics, totalMetrics) = ServerHealthClassifier.MeasuredMetricCounts(metrics);

        /* Per-server platform (design D4): the reliable signal the composer's measure auto-greying matches a
           measure's appliesTo against — see ClassifyPlatform for the edition mapping and why AWS RDS / msdb are
           deliberately not surfaced. */
        var (isAzureSqlDb, isAzureManagedInstance) = ClassifyPlatform(server.EngineEdition);

        return new FleetServerCard
        {
            ServerId = server.ServerId,
            DisplayName = server.DisplayName,
            ServerName = server.ServerName,
            EngineEdition = server.EngineEdition,
            EngineKind = server.EngineKind,
            IsPostgres = isPostgres,
            IsAurora = isAurora,
            IsAzureSqlDb = isAzureSqlDb,
            IsAzureManagedInstance = isAzureManagedInstance,
            IsSilenced = server.IsSilenced,
            Tags = tags ?? (IReadOnlyList<FleetTag>)Array.Empty<FleetTag>(),
            Band = band,
            Status = StatusLabel(isOnline, awaitingFirstCollection, collectionStale),
            IsOnline = isOnline,
            AwaitingFirstCollection = awaitingFirstCollection,
            CollectionStale = collectionStale,
            LastCollectionTime = lastCollection,
            CpuPercent = cpuPercent,
            OtherProcessCpuPercent = otherCpu,
            TotalCpuPercent = totalCpu,
            CpuSeverity = ServerHealthClassifier.CpuSeverity(
                cpuForAlert, pgCpu.AcuUtilizationPercent, cpuSource),
            InstanceCpuPercent = instanceCpuPercent,
            AcuUtilizationPercent = pgCpu.AcuUtilizationPercent,
            MaxConfiguredAcu = pgCpu.MaxConfiguredAcu,
            CpuSource = cpuSource,
            MemoryMb = memory.MemoryMb,
            BufferPoolMb = memory.BufferPoolMb,
            GrantedMemoryMb = pressure.GrantedMemoryMb,
            MemoryWaiterCount = pressure.WaiterCount,
            MemoryTimeoutCount = pressure.TimeoutCount,
            MemoryForcedCount = pressure.ForcedCount,
            HasMemoryPressure = hasMemoryPressure,
            MemorySeverity = ServerHealthClassifier.MemorySeverity(memoryPressureForBand),
            BlockingCount = blockingCount,
            MaxBlockingWaitMs = maxBlockingWaitMs,
            /* blockingForBand, not the raw count, for the reason DeadlockRatePerHour below gives: a
               PostgreSQL target's raw count is a structural zero, and a rate derived from it would publish
               0.0/hr against a severity that reads Unknown (#3539 A3). */
            BlockingRatePerHour = blockingForBand.HasValue
                ? ServerHealthClassifier.BlockingRatePerHour(blockingForBand.Value, deadlockWindow)
                : null,
            BlockingWindow = deadlockWindow,
            BlockingSeverity = ServerHealthClassifier.BlockingSeverity(blockingForBand, maxBlockedSeconds, deadlockWindow),
            DeadlockCount = deadlockCount,
            DeadlockLastSeen = deadlockLastSeen,
            DeadlockMeasured = deadlockMeasured,
            /* Through deadlocksForBand rather than the raw int so the rate and the severity below are
               derived from ONE value: the chip and the viewer detail line render this on non-null alone,
               and a rate published for a count the band did not see is the #3017 confusion one field
               over. Null exactly when the PostgreSQL arm took no difference (#3539). */
            DeadlockRatePerHour = deadlocksForBand.HasValue
                ? ServerHealthClassifier.DeadlockRatePerHour(deadlocksForBand.Value, deadlockWindow)
                : null,
            DeadlockWindow = deadlockWindow,
            DeadlockRateThresholds = deadlockTiers,
            DeadlockSeverity = ServerHealthClassifier.DeadlockSeverity(
                deadlocksForBand, deadlockWindow, deadlockTiers),
            DeadlockCollectorBand = deadlockCollectorBand,
            TotalThreads = threads.TotalThreads,
            CurrentWorkers = threads.CurrentWorkers,
            AvailableThreads = availableThreads,
            ThreadsWaitingForCpu = threads.RunnableTasks,
            RequestsWaitingForThreads = threads.WorkQueue,
            ThreadsSeverity = ServerHealthClassifier.ThreadsSeverity(threads.TotalThreads, availableThreads, threads.RunnableTasks, threads.WorkQueue),
            HealthyCollectorCount = collectors.Healthy,
            FailedCollectorCount = collectors.Failing,
            RegressedCollectorCount = collectors.Regressed,
            CollectorCount = collectors.Total,
            CollectorSeverity = ServerHealthClassifier.CollectorSeverity(collectors.Failing, collectors.Total),
            OverallMetricSeverity = overall,
            MeasuredMetricCount = measuredMetrics,
            MetricCount = totalMetrics,
        };
    }

    /// <summary>Reduces the pre-banded cards to the fleet rollup — band counts, cross-server totals (summed from
    /// the cards), and the worst-first ranking. Pure so the reduction is unit-testable without a store.</summary>
    /// <param name="collectionHealthAgeSeconds">How old the collection-health half of the cards is (#3735) —
    /// 0 when the 7-day scan ran for this roll-up, else the memo's age; published as
    /// <c>collection_health_age_seconds</c>. Trailing and defaulted so the pure-reduction tests that hand this
    /// method cards they built themselves keep reading as fresh.</param>
    public static FleetOverviewResult BuildRollup(
        IReadOnlyList<FleetServerCard> cards,
        DateTime now,
        DateTime windowStartUtc,
        DateTime windowEndUtc,
        int worstCount = DefaultWorstCount,
        IReadOnlyList<FleetTagNode>? tags = null,
        int collectionHealthAgeSeconds = 0)
    {
        var healthy = 0;
        var warning = 0;
        var critical = 0;
        var offline = 0;
        var failures = 0;
        long totalBlocking = 0;
        long totalDeadlocks = 0;
        var deadlockSourcesRead = 0;
        var deadlockPostgresTargets = 0;
        var deadlockCollectorsSilent = 0;
        var deadlockCollectorsDenied = 0;

        foreach (var card in cards)
        {
            switch (card.Band)
            {
                case FleetHealthBand.Offline: offline++; break;
                case FleetHealthBand.Critical: critical++; break;
                case FleetHealthBand.Warning: warning++; break;
                default: healthy++; break;
            }

            if (card.FailedCollectorCount > 0)
            {
                failures++;
            }

            /* #3017's denominator, reduced from the CARDS for the same reason the totals above are: the
               coverage figure and the total it qualifies then reconcile by construction rather than by two
               queries agreeing. Only the arms FleetDeadlockCoverage.IsCovered names count as read — every
               other arm, INCLUDING an enum value a later build adds and this switch has never heard of,
               lands in the silent bucket. A new source kind that inflated the read count would restore
               exactly the defect this exists to fix, where one that lands in an uncovered bucket merely
               attributes a real gap imprecisely. The PostgreSQL arm is covered AND tallied on its own
               (#3539): the sub-count names the instrument, the read count names the coverage. */
            if (FleetDeadlockCoverage.IsCovered(card.DeadlockSource))
            {
                deadlockSourcesRead++;
            }

            switch (card.DeadlockSource)
            {
                case FleetDeadlockSource.Read: break;
                case FleetDeadlockSource.PostgresTarget: deadlockPostgresTargets++; break;
                case FleetDeadlockSource.CollectorDenied: deadlockCollectorsDenied++; break;
                default: deadlockCollectorsSilent++; break;
            }

            totalBlocking += card.BlockingCount;
            totalDeadlocks += card.DeadlockCount;
        }

        var problems = cards
            .Where(c => c.Band != FleetHealthBand.Healthy)
            .OrderByDescending(c => ServerHealthClassifier.FleetHealthScore(c.Band, c.ToHealthMetrics()))
            .ThenBy(c => c.DisplayName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var worst = problems
            .Take(worstCount)
            .Select(c => new FleetRankedServer
            {
                ServerId = c.ServerId,
                DisplayName = c.DisplayName,
                Band = c.Band,
                Score = ServerHealthClassifier.FleetHealthScore(c.Band, c.ToHealthMetrics()),
                Reason = BuildReason(c),
            })
            .ToList();

        return new FleetOverviewResult
        {
            /* Emit every instant as naive UTC (Kind=Unspecified) so the JSON carries no zone suffix — the
               house rule is localize-only-in-the-browser (#1562 R5). */
            GeneratedAt = DateTime.SpecifyKind(now, DateTimeKind.Unspecified),
            WindowStart = DateTime.SpecifyKind(windowStartUtc, DateTimeKind.Unspecified),
            WindowEnd = DateTime.SpecifyKind(windowEndUtc, DateTimeKind.Unspecified),
            TotalServers = cards.Count,
            HealthyCount = healthy,
            WarningCount = warning,
            CriticalCount = critical,
            OfflineCount = offline,
            ServersWithCollectionFailures = failures,
            TotalBlockingEvents = totalBlocking,
            TotalDeadlocks = totalDeadlocks,
            DeadlockCoverage = new FleetDeadlockCoverage
            {
                ServersRead = deadlockSourcesRead,
                ServersTotal = cards.Count,
                PostgresServers = deadlockPostgresTargets,
                ServersCollectorSilent = deadlockCollectorsSilent,
                ServersCollectorDenied = deadlockCollectorsDenied,
            },
            WorstServers = worst,
            AdditionalProblemCount = Math.Max(0, problems.Count - worst.Count),
            Cards = cards,
            Tags = tags ?? Array.Empty<FleetTagNode>(),
            CollectionHealthAgeSeconds = collectionHealthAgeSeconds,
        };
    }

    /// <summary>A short "why it needs attention" line for a ranked server, from the card's own banded metrics —
    /// mirrors the WPF <c>FleetRollup.BuildReason</c> content over the pre-banded card.
    ///
    /// <para>Internal so the prose can be asserted against the card it describes. The clause a flag produces
    /// is the plainest statement of what that flag means, and #3098 is a field whose name and whose clause
    /// said different things for long enough that four readings of the name were wrong.</para></summary>
    internal static string BuildReason(FleetServerCard c)
    {
        if (c.IsOnline == false)
        {
            return "Offline - no recent collection";
        }

        if (c.AwaitingFirstCollection)
        {
            /* The word itself, not a copy of it — this was one of five spellings of the phrase across four
               files, which is the duplication #2473's pin now forbids. */
            return ServerCollectionStatus.AwaitingFirstCollection.Word();
        }

        var parts = new List<string>();

        /* The figure that DECIDED the band, not the one beside it (#3281). On a serverless PostgreSQL
           target the band comes from percent of the configured ACU ceiling, so naming the raw CPU here
           would put "CPU 100%" against a card banded off 96% capacity — two numbers, neither explaining
           the other. The capacity clause is shared with the viewer's own reason line so the two surfaces
           cannot say it differently. */
        if (c.CpuSeverity >= HealthSeverity.Warning)
        {
            var clause = FleetCpuProvenance.CapacityBandClause(c.AcuUtilizationPercent, c.CpuSource);

            if (clause is not null)
            {
                parts.Add(clause);
            }
            else if (c.TotalCpuPercent.HasValue)
            {
                parts.Add($"CPU {c.TotalCpuPercent.Value:F0}%");
            }
        }

        if (c.ThreadsSeverity >= HealthSeverity.Warning)
        {
            parts.Add(c.RequestsWaitingForThreads > 0
                ? $"Threads {c.RequestsWaitingForThreads} starved"
                : "Threads low");
        }

        if (c.MemorySeverity >= HealthSeverity.Warning && c.HasMemoryPressure)
        {
            parts.Add($"Memory {c.MemoryWaiterCount} grant waiter{(c.MemoryWaiterCount == 1 ? "" : "s")}");
        }

        if (c.BlockingSeverity >= HealthSeverity.Warning && c.BlockingCount > 0)
        {
            /* #3539 A3: the deadlock line's rule, one metric over — the count is the countable fact, the
               rate is the banded one, and an unrateable window prints the count alone. */
            parts.Add(c.BlockingRatePerHour.HasValue
                ? $"Blocking {c.BlockingCount} ({c.BlockingRatePerHour.Value.ToString("0.0", CultureInfo.InvariantCulture)}/hr)"
                : $"Blocking {c.BlockingCount}");
        }

        if (c.DeadlockSeverity >= HealthSeverity.Warning && c.DeadlockCount > 0)
        {
            /* #3368: the RATE is what banded, so the reason names it. "Deadlocks 1" against a Critical band
               was the reading #3368 was filed about, and the count alone cannot say which tier it crossed —
               1 in an hour and 1 in a day are the same string. The count stays because it is the countable
               fact; the rate is added because it is the banded one. An unrateable window prints the count
               alone, which is exactly what the band had to go on. */
            parts.Add(c.DeadlockRatePerHour.HasValue
                ? $"Deadlocks {c.DeadlockCount} ({c.DeadlockRatePerHour.Value.ToString("0.0", CultureInfo.InvariantCulture)}/hr)"
                : $"Deadlocks {c.DeadlockCount}");
        }

        if (c.CollectorSeverity >= HealthSeverity.Warning)
        {
            /* #3539 A8d: the share is what grades the band, so the denominator is named when there is one
               — "3 of 40 collectors failing" — and the bare count stands when none was declared. */
            parts.Add(c.CollectorCount > 0
                ? $"{c.FailedCollectorCount} of {c.CollectorCount} collectors failing"
                : $"{c.FailedCollectorCount} collector{(c.FailedCollectorCount == 1 ? "" : "s")} failing");
        }

        if (c.CollectionStale)
        {
            parts.Add("collection stale");
        }

        if (c.MetricCount > 0 && c.MeasuredMetricCount == 0)
        {
            /* #3539 A6: the card banded Warning because NOTHING on it was measured (OverallMetricSeverity's
               nothing-measured arm), and no per-metric clause above can fire for a card whose every band is
               Unknown — so without this the ranking would show "Needs attention" against a card that
               cannot say why. The same words the viewer's reason uses. */
            parts.Add(NoMetricMeasuredReason);
        }

        return parts.Count > 0 ? string.Join(", ", parts) : "Needs attention";
    }

    /// <summary>The reason clause for a card on which no metric was measured (#3539 A6) — the viewer's
    /// <c>FleetRollup.BuildReason</c> spells it identically, so the two surfaces read alike.</summary>
    internal const string NoMetricMeasuredReason = "no metric measured yet";

    /// <summary>The card's status word. Delegates to the one ladder every Darling surface renders (#2473):
    /// this file's own copy agreed with the WPF card, but the WPF sidebar row's copy did not, and three
    /// agreeing copies plus one that does not is still four places where the answer is decided.</summary>
    private static string StatusLabel(bool? isOnline, bool awaitingFirstCollection, bool collectionStale) =>
        ServerCollectionStatusRules.Classify(isOnline, collectionStale, awaitingFirstCollection).Word();

    /// <summary>
    /// Classifies a server's raw SERVERPROPERTY('EngineEdition') into the RELIABLE per-server platform flags the
    /// composer's D4 measure auto-greying keys on: <c>5</c> = Azure SQL Database, <c>8</c> = Azure Managed
    /// Instance (the same 5/8 classification <see cref="!:DarlingServerConnector"/> applies at connect). Any other
    /// edition — a box edition (2/3/4…), or <c>null</c> (a server the worker has not yet connected/probed) — is
    /// neither, so the composer keeps the measure badge (no signal rather than a wrong one).
    ///
    /// <para>AWS RDS and msdb access are deliberately NOT returned: unlike engine edition they are not persisted on
    /// the <c>servers</c> registry (RDS reports as an ordinary box edition, and <c>HAS_DBACCESS('msdb')</c> is
    /// probed but never stored), so there is no reliable stored signal to derive them from here.</para>
    /// </summary>
    internal static (bool IsAzureSqlDb, bool IsAzureManagedInstance) ClassifyPlatform(int? engineEdition) =>
        (engineEdition == 5, engineEdition == 8);

    /// <summary>
    /// Classifies the stored engine-kind token (V82, #2530) into the two booleans a browser actually branches
    /// on, so no consumer has to know the vocabulary's spelling. The raw token still rides on the card beside
    /// them — a UI that wants to LABEL the engine needs the word, and a UI that wants to choose a tab set
    /// needs the boolean.
    ///
    /// <para><c>null</c> — a pre-V82 row, or a server that has not connected since the rung landed — is
    /// neither, so a card with no signal renders exactly as it did before this column existed rather than
    /// claiming SQL Server on the strength of an absence. Same discipline as
    /// <see cref="ClassifyPlatform"/>'s null edition.</para>
    /// </summary>
    internal static (bool IsPostgres, bool IsAurora) ClassifyEngineKind(string? engineKind) =>
        (MonitoredEngineKind.IsPostgres(engineKind), MonitoredEngineKind.IsAurora(engineKind));

    /* ─────────────────────────── per-query readers ─────────────────────────── */

    private static async Task<List<FleetServerRow>> ReadServersAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var rows = new List<FleetServerRow>();
        await using var command = postgres.CreateCommand(FleetServersSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new FleetServerRow(
                reader.GetInt32(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetInt32(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                !reader.IsDBNull(5) && reader.GetBoolean(5)));
        }

        return rows;
    }

    private static async Task<Dictionary<int, List<FleetTag>>> ReadTagsAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var map = new Dictionary<int, List<FleetTag>>();
        await using var command = postgres.CreateCommand(FleetTagsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var serverId = reader.GetInt32(0);
            if (!map.TryGetValue(serverId, out var list))
            {
                list = new List<FleetTag>();
                map[serverId] = list;
            }

            list.Add(new FleetTag
            {
                Id = reader.GetInt32(1),
                Name = reader.IsDBNull(2) ? "" : reader.GetString(2),
                Colour = reader.IsDBNull(3) ? null : reader.GetString(3),
            });
        }

        return map;
    }

    private static async Task<List<FleetTagNode>> ReadTagForestAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var forest = new List<FleetTagNode>();
        await using var command = postgres.CreateCommand(FleetTagForestSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            forest.Add(new FleetTagNode
            {
                Id = reader.GetInt32(0),
                Name = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ParentId = reader.IsDBNull(2) ? null : reader.GetInt32(2),
                SortOrder = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                Colour = reader.IsDBNull(4) ? null : reader.GetString(4),
            });
        }

        return forest;
    }

    private static async Task<Dictionary<int, CpuRow>> ReadCpuAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var map = new Dictionary<int, CpuRow>();
        await using var command = postgres.CreateCommand(FleetCpuSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            map[reader.GetInt32(0)] = new CpuRow(
                reader.IsDBNull(1) ? null : Convert.ToDouble(reader.GetValue(1)),
                reader.IsDBNull(2) ? null : Convert.ToDouble(reader.GetValue(2)));
        }

        return map;
    }

    /// <summary>The newest current Performance Insights reading per PostgreSQL/Aurora target (#3267/#3281)
    /// — the raw CPU and, from the same row, the capacity headroom the band reads. No entry for a server
    /// without one, so the caller's miss is an absent key rather than a zero; within an entry each figure
    /// is independently nullable, so a card can have a current CPU reading and no capacity sample.</summary>
    private static async Task<Dictionary<int, PgCpuRow>> ReadPgCpuAsync(NpgsqlDataSource postgres, DateTime nowUtc, CancellationToken cancellationToken)
    {
        var map = new Dictionary<int, PgCpuRow>();
        await using var command = postgres.CreateCommand(FleetPgCpuSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        /* Naive UTC at the bind, matching every other comparison against the store's naive `timestamp`
           columns: Kind=Utc is not rejected, Npgsql infers `timestamptz` and PostgreSQL zone-shifts it. */
        command.Parameters.AddWithValue(
            DateTime.SpecifyKind(nowUtc - DarlingPgCpuUtilizationReader.Freshness, DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            map[reader.GetInt32(0)] = new PgCpuRow(
                reader.IsDBNull(1) ? null : Convert.ToDouble(reader.GetValue(1)),
                reader.IsDBNull(2) ? null : Convert.ToDouble(reader.GetValue(2)),
                reader.IsDBNull(3) ? null : Convert.ToDouble(reader.GetValue(3)));
        }

        return map;
    }

    private static async Task<Dictionary<int, MemoryRow>> ReadMemoryAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var map = new Dictionary<int, MemoryRow>();
        await using var command = postgres.CreateCommand(FleetMemorySql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            map[reader.GetInt32(0)] = new MemoryRow(
                reader.IsDBNull(1) ? null : Convert.ToDouble(reader.GetValue(1)),
                reader.IsDBNull(2) ? null : Convert.ToDouble(reader.GetValue(2)));
        }

        return map;
    }

    private static async Task<Dictionary<int, MemoryPressureRow>> ReadMemoryPressureAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var map = new Dictionary<int, MemoryPressureRow>();
        await using var command = postgres.CreateCommand(FleetMemoryPressureSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            map[reader.GetInt32(0)] = new MemoryPressureRow(
                reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3)),
                reader.IsDBNull(4) ? null : Convert.ToDouble(reader.GetValue(4)));
        }

        return map;
    }

    private static async Task<Dictionary<int, ThreadsRow>> ReadThreadsAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var map = new Dictionary<int, ThreadsRow>();
        await using var command = postgres.CreateCommand(FleetThreadsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            map[reader.GetInt32(0)] = new ThreadsRow(
                reader.IsDBNull(1) ? null : Convert.ToInt32(reader.GetValue(1)),
                reader.IsDBNull(2) ? null : Convert.ToInt32(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0 : Convert.ToInt32(reader.GetValue(3)),
                reader.IsDBNull(4) ? 0 : Convert.ToInt64(reader.GetValue(4)));
        }

        return map;
    }

    private static async Task<Dictionary<int, BlockingRow>> ReadBlockingAsync(
        NpgsqlDataSource postgres, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken)
    {
        var map = new Dictionary<int, BlockingRow>();
        await using var command = postgres.CreateCommand(FleetBlockingSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        AddTimestamp(command, EventWindowFloor.For(startUtc));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            map[reader.GetInt32(0)] = new BlockingRow(
                reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0 : Convert.ToInt32(reader.GetValue(3)),
                reader.IsDBNull(4) ? 0 : Convert.ToInt64(reader.GetValue(4)));
        }

        return map;
    }

    /// <summary>
    /// The store's deadlock-rate tiers (#3368, V120), or the shipped pair when the row is absent.
    ///
    /// <para><b>A missing row falls back rather than failing.</b> <c>config_alert_settings</c> is a
    /// singleton seeded on first worker start, so a store read before that has no row — and the fleet
    /// roll-up is a READ that must still answer. The shipped pair is what such a store would seed anyway,
    /// so the fallback is the store's own future value, not a guess.</para>
    ///
    /// <para>Values come back RAW; <see cref="DeadlockRateThresholds"/> clamps on read, so a hand-edited
    /// row cannot drive a nonsense threshold and the roll-up reports the tier it actually used.</para>
    /// </summary>
    private static async Task<DeadlockRateThresholds> ReadDeadlockRateThresholdsAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(FleetDeadlockRateThresholdSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return new DeadlockRateThresholds(reader.GetDouble(0), reader.GetDouble(1));
        }

        return DeadlockRateThresholds.Default;
    }

    private static async Task<Dictionary<int, DeadlockRow>> ReadDeadlocksAsync(
        NpgsqlDataSource postgres, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken)
    {
        var map = new Dictionary<int, DeadlockRow>();
        await using var command = postgres.CreateCommand(FleetDeadlockSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        AddTimestamp(command, EventWindowFloor.For(startUtc));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            map[reader.GetInt32(0)] = new DeadlockRow(
                reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1)),
                reader.IsDBNull(2) ? null : reader.GetDateTime(2));
        }

        return map;
    }

    /// <summary>The PostgreSQL twin of <see cref="ReadDeadlocksAsync"/> (#3539) — same carrier, same window,
    /// the engine's own counter behind it. Read for the whole fleet in one pass like every other per-metric
    /// read here; a SQL Server has no row in <c>pg_database_stats</c> and simply does not appear.</summary>
    private static async Task<Dictionary<int, PgDeadlockRow>> ReadPgDeadlocksAsync(
        NpgsqlDataSource postgres, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken)
    {
        var map = new Dictionary<int, PgDeadlockRow>();
        await using var command = postgres.CreateCommand(FleetPgDeadlockSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* The SUM comes back as bigint; the card's count is an int like the SQL Server arm's. A window
               whose clamped deadlock differences overflow int is not a reading this card can render either
               way, so saturate rather than wrap - a wrapped count could band a catastrophe Healthy. */
            var count = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
            map[reader.GetInt32(0)] = new PgDeadlockRow(
                (int)Math.Min(count, int.MaxValue),
                reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3)));
        }

        return map;
    }

    /// <summary>One <see cref="LastCollectionRow"/> per enabled registry server, INCLUDING the ones whose
    /// window holds nothing (#3935): their row is the read's positive report that it looked and found none,
    /// which is what <see cref="ClassifyWindowedFreshness"/> needs before it may call a server Offline.</summary>
    private static async Task<Dictionary<int, LastCollectionRow>> ReadLastCollectionAsync(NpgsqlDataSource postgres, DateTime now, CancellationToken cancellationToken)
    {
        var map = new Dictionary<int, LastCollectionRow>();
        await using var command = postgres.CreateCommand(FleetLastCollectionSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddTimestamp(command, LastCollectionWindowStart(now));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            map[reader.GetInt32(0)] = new LastCollectionRow(
                reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                reader.IsDBNull(2) ? null : reader.GetDateTime(2));
        }

        return map;
    }

    /// <summary>Reads the cross-server 7-day collector health and counts each server's HEALTHY / FAILING
    /// collectors through the shared <see cref="CollectorHealth.HealthStatus"/> banding, plus (#3819) the
    /// ones that stopped producing, off <see cref="CollectorHealth.RegressedFromProductive"/> rather than
    /// off a band.</summary>
    private static async Task<Dictionary<int, CollectorCounts>> ReadFailingCollectorCountsAsync(
        NpgsqlDataSource postgres, DateTime now, CancellationToken cancellationToken)
    {
        var counts = new Dictionary<int, CollectorCounts>();
        /* #3893 arm 2: the composed read (hourly aggregate + raw head slice) when both guards pass, else the
           raw scan. Same thirteen ordinals either way, so everything below is shared. */
        var windowStart = DateTime.SpecifyKind(now.AddDays(-7), DateTimeKind.Unspecified);
        var headEnd = CeilingHour(windowStart);
        var composed = await CollectionHealthRollupUsableAsync(postgres, headEnd, cancellationToken);
        await using var command = postgres.CreateCommand(composed ? FleetCollectionHealthComposedSql : FleetCollectionHealthSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddTimestamp(command, windowStart);
        if (composed)
        {
            AddTimestamp(command, headEnd);
        }
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var serverId = reader.GetInt32(0);
            var health = new CollectorHealth
            {
                CollectorName = reader.GetString(1),
                TotalRuns = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                SuccessCount = reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3)),
                ErrorCount = reader.IsDBNull(4) ? 0 : Convert.ToInt64(reader.GetValue(4)),
                LastSuccessTime = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                PermissionDeniedCount = reader.IsDBNull(6) ? 0 : Convert.ToInt64(reader.GetValue(6)),
                LastRunTime = reader.IsDBNull(7) ? null : reader.GetDateTime(7),
                AbandonedCount = reader.IsDBNull(8) ? 0 : Convert.ToInt64(reader.GetValue(8)),
                /* Appended (#3240) — the band this row computes must agree with the per-server reads. */
                ExtensionMissingCount = reader.IsDBNull(9) ? 0 : Convert.ToInt64(reader.GetValue(9)),
                /* Appended (#3819) — same reasoning as the two counts above. These feed
                   CollectorHealth.RegressedFromProductive, which HealthStatus reads as its floor, so
                   leaving them unset would band a regressed collector HEALTHY here while the per-server
                   grid called it WARNING. That is the #2779/#2784 failure shape: one surface fixed, its
                   sibling quietly left on the old reading, and it would COMPILE, because the default is
                   silent. CurrentStatus is deliberately NOT read: it composes display prose this rollup
                   never renders, and the predicate does not take it. */
                LastNonSkipTime = reader.IsDBNull(10) ? null : reader.GetDateTime(10),
                LastProductiveTime = reader.IsDBNull(11) ? null : reader.GetDateTime(11),
            };

            /* #3885: the produced-then-stopped arm's input, set AFTER construction because it is derived
               from two of this row's own members plus the cadence rather than read from a column. The
               per-server twin reads an exact count off its ranked subquery; this one estimates, for the
               statement-timeout reason FleetCollectionHealthSql gives. Left unset, the arm would read 0
               here and the card's regressed count would stay silent on a collector that stopped producing
               while get_collection_health called it WARNING -- the #2779/#2784 shape, and it would COMPILE,
               because the default is silent. */
            health.TrailingZeroRowSuccessRuns = CollectorHealthClassifier.EstimateTrailingZeroRowSuccessRuns(
                health.LastRunTime,
                reader.IsDBNull(12) ? null : reader.GetDateTime(12),
                health.TotalRuns,
                health.FrequencyMinutes);

            counts.TryGetValue(serverId, out var existing);
            var status = health.HealthStatus;
            counts[serverId] = new CollectorCounts(
                existing.Healthy + (status == "HEALTHY" ? 1 : 0),
                existing.Failing + (status == "FAILING" ? 1 : 0),
                /* #3539 A8d: every banded row, whatever its band — the share's denominator. */
                existing.Total + 1,
                /* #3819: counted off the PREDICATE rather than off the band, and the two are not the same
                   population. The band floor only moves a row that would have read HEALTHY; a collector
                   whose success clock has already run out reads FAILING and is regressed as well. Keyed
                   on the band, this count would go quiet exactly when the regression got worse.

                   #3885: the SAME count, now over both regression classes -- stopped SKIPPING and stopped
                   PRODUCING -- through the row's one AnyRegression member. One number, because an install
                   countersign reads it as "collectors that stopped doing what they used to do" and a second
                   parallel count would have to be learned, added, and never confused with the first. The
                   two predicates are disjoint by construction (one needs the newest run to be a skip, the
                   other needs it to be a success), so nothing is double-counted. */
                existing.Regressed + (health.AnyRegression ? 1 : 0),
                /* #3017: the ONE collector per engine whose band the deadlock total's coverage turns on,
                   kept alongside the Healthy/Failing tallies because it comes out of the same aggregate —
                   no extra round trip, which is what keeps this reader's fan-out bounded. Named from the
                   collector rather than as a literal so a rename cannot leave this silently matching
                   nothing and reporting every server uncovered. Both engines' bands are kept on every
                   server because this aggregate does not know the engine; BuildCard picks. */
                string.Equals(health.CollectorName, DeadlocksCollector.Instance.Name, StringComparison.Ordinal)
                    ? status
                    : existing.DeadlockBand,
                string.Equals(health.CollectorName, PgDatabaseStatsCollector.Instance.Name, StringComparison.Ordinal)
                    ? status
                    : existing.PgDeadlockBand);
        }

        return counts;
    }

    private static void AddTimestamp(NpgsqlCommand command, DateTime value) =>
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(value, DateTimeKind.Unspecified) });

    /* ─────────────────────────── raw-read carriers (internal) ─────────────────────────── */

    internal readonly record struct FleetServerRow(int ServerId, string DisplayName, string ServerName, int? EngineEdition, string? EngineKind, bool IsSilenced);
    internal readonly record struct CpuRow(double? SqlCpu, double? OtherCpu);

    /// <summary>One PostgreSQL/Aurora target's newest current Performance Insights row (#3281). One struct
    /// rather than three maps so a call site cannot pick up the CPU and drop the capacity that qualifies
    /// it, and its <c>default</c> is three nulls — which classifies as "nothing was read".</summary>
    /// <param name="InstanceCpuPercent"><c>os.cpuUtilization.total.avg</c>, percent of the capacity
    /// CURRENTLY ALLOCATED.</param>
    /// <param name="AcuUtilizationPercent">Percent of the CONFIGURED ceiling — the figure the band
    /// reads.</param>
    /// <param name="MaxConfiguredAcu">The configured ACU ceiling, for the card's detail line.</param>
    internal readonly record struct PgCpuRow(
        double? InstanceCpuPercent, double? AcuUtilizationPercent, double? MaxConfiguredAcu);
    internal readonly record struct MemoryRow(double? MemoryMb, double? BufferPoolMb);
    internal readonly record struct MemoryPressureRow(long WaiterCount, long TimeoutCount, long ForcedCount, double? GrantedMemoryMb);
    internal readonly record struct ThreadsRow(int? TotalThreads, int? CurrentWorkers, int RunnableTasks, long WorkQueue);
    internal readonly record struct BlockingRow(int XeCount, long XeMaxWait, int DmvCount, long DmvMaxWait);
    internal readonly record struct DeadlockRow(int Count, DateTime? LastSeen);

    /// <summary>One server's <see cref="FleetLastCollectionSql"/> row (#3935): its newest collection inside the
    /// window and the registry's <c>created_date</c>, either of which can be null. Its <c>default</c> — two
    /// nulls — is what a server the read did not report at all is handed, and it classifies exactly as a
    /// null did before #3935: never collected, the amber no-claim reading, never a red Offline built out of
    /// an absence.</summary>
    /// <param name="LastCollection">The newest collection at or after <see cref="LastCollectionWindowStart"/>,
    /// or null when the window holds none.</param>
    /// <param name="RegisteredAt">The server's first successful connect, as the registry recorded it.</param>
    internal readonly record struct LastCollectionRow(DateTime? LastCollection, DateTime? RegisteredAt);
    /// <summary>The PostgreSQL deadlock reading (#3539): the summed counter differences, the sample that
    /// showed the newest step, and <paramref name="Intervals"/> — how many differences the sum was taken
    /// over. Its <c>default</c> is zero intervals, which <see cref="BuildCard"/> reads as unmeasured: a
    /// PostgreSQL target that fell out of the read (no rows in the window) must band Unknown, and a
    /// struct whose default meant "measured zero" would make that the quiet outcome of a miss.</summary>
    internal readonly record struct PgDeadlockRow(int Count, DateTime? LastSeen, long Intervals);
    /// <param name="DeadlockBand">The <c>deadlocks</c> collector's own 7-day band for this server, or null
    /// when that collector left no row in the health window at all (#3017). Null and
    /// <see cref="CollectorHealthClassifier.NeverRun"/> mean the same thing to a reader and take the same
    /// action, but they arrive differently: null is the absent GROUP, NEVER_RUN would be a present group with
    /// no runs in it.</param>
    /// <param name="PgDeadlockBand">The <c>pg_database_stats</c> collector's band, same terms (#3539) — the
    /// deadlock-source collector on a PostgreSQL target, where <paramref name="DeadlockBand"/> is always
    /// null because that engine has no <c>deadlocks</c> collector.</param>
    /// <param name="Total">Every collector banded for this server in the health window, on any band (#3539
    /// A8d) — the denominator <see cref="ServerHealthClassifier.CollectorSeverity"/> grades the failing count
    /// against. Healthy + Failing is NOT it: STALE, WARNING, STOPPED, NO_PERMISSIONS and EXTENSION_MISSING rows
    /// are all banded collectors that are neither.</param>
    /// <param name="Regressed">Collectors on this server that WERE producing rows and now report a named
    /// skip every cycle (#3819) — <c>CollectorHealth.RegressedFromProductive</c>, counted off the
    /// predicate rather than off the band.
    ///
    /// <para><b>It is not disjoint from <paramref name="Failing"/> and the two must never be added.</b> A
    /// regression's first day reads HEALTHY on the staleness ladder (the last productive cycle is hours
    /// old), so the band floor moves it to WARNING; by the second day the success clock has run out and
    /// the same collector reads FAILING while still being regressed. Counting off the band would make
    /// this go quiet precisely as the regression got worse.</para></param>
    internal readonly record struct CollectorCounts(int Healthy, int Failing, int Total, int Regressed, string? DeadlockBand = null, string? PgDeadlockBand = null);

    /// <summary>One completed 7-day collection-health scan (#3735): the per-server counts
    /// <see cref="ReadFailingCollectorCountsAsync"/> produced and the <c>nowUtc</c> the scan was started with —
    /// which is both the instant its <c>$1 = now - 7 days</c> window was cut from and the instant
    /// <c>collection_health_age_seconds</c> is measured from. Read-only by type so a caller sharing the
    /// dictionary with every other caller of the next minute cannot mutate it under them; the reader only ever
    /// <c>TryGetValue</c>s it.</summary>
    internal sealed record CollectionHealthScan(IReadOnlyDictionary<int, CollectorCounts> Counts, DateTime ReadAtUtc);

    /// <summary>
    /// Single-flight plus a one-minute memo over the 7-day collection-health rollup (#3735). Concurrent callers
    /// share ONE in-flight scan; a completed scan younger than <see cref="CollectionHealthMemoLifetime"/> is
    /// served from memory. One instance per <see cref="NpgsqlDataSource"/>, through
    /// <see cref="CollectionHealthMemoFor"/>.
    ///
    /// <para><b>The production photo this answers.</b> 2026-09-19 13:36Z, the largest production store:
    /// <see cref="FleetCollectionHealthSql"/> was running THREE times concurrently — the identical statement,
    /// the same start, one per <c>get_fleet_overview</c> call a single caller had raced in parallel with three
    /// different <c>hours_back</c> values — each with two parallel workers, nine backends on
    /// <c>collection_log</c> in pure I/O waits while the collectors' flush band was mid-<c>COPY</c>. The slowest
    /// copy crossed the <c>mcp</c> role's server-side <c>statement_timeout</c> of 15 s; the caller's retry landed
    /// after the flush drained and succeeded. Solo, the same plan runs in 680 ms as a healthy parallel
    /// aggregate. The cap did its job — raising it would have hidden the fan-out — and the plan is not the
    /// problem, so neither is touched here. What was wrong is that the rollup is WINDOW-INDEPENDENT:
    /// <c>hours_back</c> bounds only the blocking and deadlock reads, and this read's <c>$1</c> is always
    /// <c>now - 7 days</c>, so three windows bought three copies of one answer. The web fleet page reaches the
    /// same read through <c>/api/fleet</c>, so a browser's auto-refresh and an agent's tick stack the same way.
    /// The fix is not to run the statement N times: one scan per minute per host, however many overview calls
    /// race.</para>
    ///
    /// <para><b>The caller's <c>nowUtc</c> is the memo's clock.</b> It is already the reader's freshness
    /// reference (the instant <c>generated_at</c> carries) and both production callers pass
    /// <c>DateTime.UtcNow</c>; using it rather than a private stopwatch means the age published beside the
    /// payload is measured from the instant the payload itself publishes, and a test can drive expiry without
    /// sleeping. A caller whose <c>nowUtc</c> is BEHIND the memo's is served the memo at age 0: the rollup is
    /// not older than that caller's reference and there is nothing newer to read. A hit's <c>$1</c> is up to a
    /// minute older than a fresh scan's would have been, which at a 7-day window is nothing.</para>
    ///
    /// <para><b>The shared scan runs on <see cref="CancellationToken.None"/>, bounded by the 20 s command
    /// deadline and the role's 15 s <c>statement_timeout</c>; a caller's own token releases only that
    /// caller.</b> Three reasons, in order of weight. The MCP surface never hands a token down at all —
    /// <see cref="McpCommandDeadlines"/> documents that every tool calls its reader without one — so on the
    /// surface the photo was taken the scan already ran to its deadline whatever the caller did. The web
    /// surface DOES thread <c>HttpContext.RequestAborted</c>, and a tab that navigates away mid-refresh must not
    /// discard a scan two other waiters, or the next tick, are about to use; the alternative — a linked token
    /// that cancels when the last waiter leaves — turns an auto-refreshing browser into cancel-and-restart
    /// churn, N sequential copies of the statement instead of N concurrent ones, the same pile-up in a
    /// different shape. And the worst case under <c>None</c> is bounded to ONE orphaned statement per host per
    /// minute, which is the bound this class promises anyway. So when the first caller cancels while two others
    /// wait: that caller's <see cref="Task.WaitAsync(CancellationToken)"/> throws to it at once, the statement
    /// keeps running for the two, and its result is memoized for everyone who arrives in the next minute.</para>
    ///
    /// <para><b>A failed scan is not memoized, and cannot become an unobserved exception.</b> The shared task
    /// carries its outcome rather than faulting: waiters present when a scan fails rethrow it with its original
    /// stack; the memo keeps whatever it last read successfully (still served while under a minute old), and
    /// the next caller past that starts a fresh scan. A scan every waiter had abandoned before it failed
    /// completes quietly — had the shared task faulted instead, that exception would have surfaced on the
    /// finalizer thread as <c>TaskScheduler.UnobservedTaskException</c>, from a read nobody was waiting on.</para>
    ///
    /// <para><b>What this does NOT do.</b> The other twelve reads in the overview are untouched: each carries the
    /// caller's window or is a latest-snapshot read that costs one index descent per server. The SQL text, the
    /// command deadline and the role's timeout are unchanged. And caller-side hygiene still applies: an agent
    /// that wants three windows should call once with the widest and derive, or sequence the three — the
    /// window-bound reads still cost what they cost, and the write band does not care about the caller's
    /// reasons.</para>
    /// </summary>
    internal sealed class CollectionHealthMemo
    {
        /// <summary>The scan's outcome as a VALUE — exactly one of the two is non-null — so the shared task
        /// completes successfully whether the statement did or not (see the class summary's last paragraph).
        /// The dispatch info, not the bare exception, so a waiter's rethrow keeps the statement's own stack
        /// rather than acquiring the waiter's.</summary>
        private sealed record ScanOutcome(CollectionHealthScan? Scan, ExceptionDispatchInfo? Failure);

        private readonly object _gate = new();

        /// <summary>The scan the current callers share. Non-null and incomplete while a statement is running;
        /// a COMPLETED task here is history (its success is in <c>_latest</c>, its failure is in
        /// nothing), and the next caller who finds the memo stale starts a new one over it.</summary>
        private Task<ScanOutcome>? _inFlight;

        /// <summary>The newest scan that SUCCEEDED, or null before the first one has. Served while younger than
        /// <see cref="CollectionHealthMemoLifetime"/>; a failed scan never replaces it.</summary>
        private CollectionHealthScan? _latest;

        private int _scansStarted;

        /// <summary>How many statements this memo has actually started, over its whole life — the figure a
        /// live test compares against the number of overview calls it raced. Diagnostic; nothing reads it in
        /// production.</summary>
        internal int ScansStarted
        {
            get
            {
                lock (_gate)
                {
                    return _scansStarted;
                }
            }
        }

        /// <summary>
        /// The 7-day collection-health counts as of <paramref name="nowUtc"/> — from memory when the memo is
        /// under a minute old, from the scan already in flight when there is one, else from a scan this call
        /// starts through <paramref name="scan"/> — with how many whole seconds older than
        /// <paramref name="nowUtc"/> the reading is (0 when this call's own scan produced it).
        /// </summary>
        /// <param name="scan">The statement, as a function of the instant to cut the window from and the token
        /// to run under. The memo, not the caller, decides the token (see the class summary), which is why the
        /// seam takes one rather than closing over the caller's. Substitutable so a test can count executions
        /// without a store.</param>
        /// <param name="cancellationToken">Releases THIS caller's wait. It does not reach the statement.</param>
        internal async Task<(IReadOnlyDictionary<int, CollectorCounts> Counts, int AgeSeconds)> GetAsync(
            DateTime nowUtc,
            Func<DateTime, CancellationToken, Task<Dictionary<int, CollectorCounts>>> scan,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(scan);

            Task<ScanOutcome> shared;
            TaskCompletionSource<ScanOutcome>? lead = null;
            lock (_gate)
            {
                if (_latest is { } latest && nowUtc - latest.ReadAtUtc < CollectionHealthMemoLifetime)
                {
                    return (latest.Counts, AgeSeconds(nowUtc, latest));
                }

                if (_inFlight is not { IsCompleted: false })
                {
                    /* RunContinuationsAsynchronously: the waiters' continuations — the rest of thirteen
                       overview reads each — must not run inline on whichever thread completes the statement,
                       or the leader's connection callback would carry every joiner's card assembly. */
                    lead = new TaskCompletionSource<ScanOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
                    _inFlight = lead.Task;
                    _scansStarted++;
                }

                shared = _inFlight;
            }

            if (lead is not null)
            {
                /* Started OUTSIDE the lock so the statement's synchronous prefix (command construction) never
                   runs under it, and deliberately not awaited here: this caller is one waiter among any
                   number, and its own cancellation below must not be the scan's. RunScanAsync completes the
                   source in both arms and never throws, so the discarded task cannot fault. */
                _ = RunScanAsync(lead, nowUtc, scan);
            }

            var outcome = await shared.WaitAsync(cancellationToken);
            outcome.Failure?.Throw();
            var read = outcome.Scan!;
            return (read.Counts, AgeSeconds(nowUtc, read));
        }

        private async Task RunScanAsync(
            TaskCompletionSource<ScanOutcome> lead,
            DateTime nowUtc,
            Func<DateTime, CancellationToken, Task<Dictionary<int, CollectorCounts>>> scan)
        {
            try
            {
                var counts = await scan(nowUtc, CancellationToken.None);
                var read = new CollectionHealthScan(counts, nowUtc);
                lock (_gate)
                {
                    _latest = read;
                }

                lead.SetResult(new ScanOutcome(read, null));
            }
            catch (Exception ex)
            {
                lead.SetResult(new ScanOutcome(null, ExceptionDispatchInfo.Capture(ex)));
            }
        }

        /// <summary>Whole seconds from the scan's reference instant to <paramref name="nowUtc"/>, floored at
        /// zero — a caller whose clock reads behind the scan's is not holding a reading from the future, it is
        /// holding the freshest one there is.</summary>
        private static int AgeSeconds(DateTime nowUtc, CollectionHealthScan read) =>
            (int)Math.Max(0, Math.Floor((nowUtc - read.ReadAtUtc).TotalSeconds));
    }
}

/// <summary>
/// One pre-banded Overview card in the fleet roll-up — every band already computed by the shared
/// <see cref="ServerHealthClassifier"/>, every instant a naive-UTC value the browser localizes. Serialized with
/// snake_case field names for both <c>/api/fleet</c> and the <c>get_fleet_overview</c> MCP tool.
/// </summary>
public sealed class FleetServerCard
{
    [JsonPropertyName("server_id")] public int ServerId { get; init; }
    [JsonPropertyName("display_name")] public string DisplayName { get; init; } = "";
    [JsonPropertyName("server_name")] public string ServerName { get; init; } = "";

    /// <summary>The raw probed SERVERPROPERTY('EngineEdition') (5 = Azure SQL DB, 8 = Azure Managed Instance, a
    /// box edition otherwise); null when the server has not yet connected. The reliable per-server platform
    /// signal the composer's D4 measure auto-greying matches a measure's <c>appliesTo</c> against.</summary>
    [JsonPropertyName("engine_edition")] public int? EngineEdition { get; init; }

    /// <summary>The target's engine KIND as the registry recorded it on its last connect (#2530) —
    /// <c>sqlserver</c>, <c>postgres</c>, or <c>aurora-postgres</c>; null when no connect has stamped it (a
    /// pre-V82 store, or a server that has never connected).
    ///
    /// <para>This is the discriminator <c>engine_edition</c> cannot supply and never could: a PostgreSQL
    /// target has no <c>SERVERPROPERTY</c>, so it lands with edition 0, which is also what a SQL Server that
    /// has never connected lands with. Every surface that wants to show PostgreSQL panels to PostgreSQL
    /// targets and SQL Server tabs to SQL Server targets branches on THIS.</para></summary>
    [JsonPropertyName("engine_kind")] public string? EngineKind { get; init; }

    /// <summary>How <see cref="EngineKind"/> reads in a sentence — "SQL Server", "PostgreSQL", "Aurora
    /// PostgreSQL" — or null when the store has made no claim.
    ///
    /// <para>On the card because <see cref="MonitoredEngineKind.DescribeEngineKind"/> is deliberately the
    /// single copy of those words, and a browser mapping three tokens to three strings itself would be a
    /// second table in a language the first cannot be shared with. The web server page had exactly that for
    /// the length of one review round.</para>
    ///
    /// <para>DERIVED rather than assigned, so it cannot be forgotten: every card is built by an object
    /// initializer, and a settable field would be null on any card whose builder did not think of it —
    /// including one added later on a path nobody re-reads.</para>
    ///
    /// <para><b>Three answers, and the third is the interesting one.</b> An ABSENT kind is null: a surface has
    /// nothing to say about a server whose engine was never stamped, and no badge is better than a badge
    /// describing the store's silence as a property of the server. A RECOGNISED token gets
    /// <see cref="MonitoredEngineKind.DescribeEngineKind"/>'s words. A token this build has never heard of —
    /// a store written by a NEWER build — gets the token back verbatim, NOT the describer's
    /// "an unrecognised engine": that phrase is worded to sit mid-sentence in the capability messages, and as
    /// a label beside "SQL Server" and "Aurora PostgreSQL" it reads as the wrong part of speech. The raw token
    /// is also the more useful of the two, being the string an operator would search their own store for. It
    /// is deliberately not mapped onto a default, which is the whole reason the describer refuses to guess in
    /// the first place.</para>
    /// </summary>
    [JsonPropertyName("engine_description")]
    public string? EngineDescription =>
        string.IsNullOrWhiteSpace(EngineKind) ? null
        : MonitoredEngineKind.IsKnown(EngineKind) ? MonitoredEngineKind.DescribeEngineKind(EngineKind)
        : EngineKind.Trim();

    /// <summary>True when this server is PostgreSQL (Aurora or stock) — derived from
    /// <see cref="EngineKind"/>, so a consumer never has to know the token vocabulary. False when the kind is
    /// unknown: absence of a claim, not a claim of SQL Server.</summary>
    [JsonPropertyName("is_postgres")] public bool IsPostgres { get; init; }

    /// <summary>True when this server is Amazon Aurora PostgreSQL specifically — derived from
    /// <see cref="EngineKind"/>. Separate from <see cref="IsPostgres"/> because a large proprietary surface
    /// (the <c>aurora_stat_*</c> functions) exists only there, so a panel fed by an Aurora-only collector has
    /// to be able to tell the two apart.</summary>
    [JsonPropertyName("is_aurora")] public bool IsAurora { get; init; }

    /// <summary>True when this server is Azure SQL Database (engine edition 5) — reliable, derived from
    /// <see cref="EngineEdition"/>.</summary>
    [JsonPropertyName("is_azure_sql_db")] public bool IsAzureSqlDb { get; init; }

    /// <summary>True when this server is Azure SQL Managed Instance (engine edition 8) — reliable, derived from
    /// <see cref="EngineEdition"/>.</summary>
    [JsonPropertyName("is_azure_mi")] public bool IsAzureManagedInstance { get; init; }

    /// <summary>True when a whole-server alert silence (an enabled, unexpired mute rule scoped to this server
    /// with no narrowing pattern) is active (#2031) — display-only, so a silenced server stops looking like a
    /// healthy-quiet one. The web seat has no silence action; silencing stays with the Viewer/MCP.</summary>
    [JsonPropertyName("is_silenced")] public bool IsSilenced { get; init; }

    /// <summary>The server's tags for the read-only fleet pills (#2020) — id, name, and stored <c>#RRGGBB</c>
    /// colour (null = neutral pill). Empty when the server has none. Tagging stays with the Viewer / Lite; the
    /// web seat only reads them.</summary>
    [JsonPropertyName("tags")] public IReadOnlyList<FleetTag> Tags { get; init; } = Array.Empty<FleetTag>();

    [JsonPropertyName("band")] public FleetHealthBand Band { get; init; }
    [JsonPropertyName("status")] public string Status { get; init; } = "";
    [JsonPropertyName("is_online")] public bool? IsOnline { get; init; }
    [JsonPropertyName("awaiting_first_collection")] public bool AwaitingFirstCollection { get; init; }

    /// <summary>
    /// The newest collection has lagged past <see cref="ServerHealthThresholds.StaleThreshold"/> without being
    /// old enough to call the server dark — <see cref="ServerFreshness.Stale"/>, from
    /// <see cref="ServerCollectionStatusRules.FlagsFor"/>. It is what bands an otherwise-calm card Warning.
    ///
    /// <para><b>Derivable from this same payload, which is the point.</b> The flag is a function of
    /// <c>last_collection</c> against the roll-up's <c>generated_at</c>, and <c>status</c> reads
    /// <c>"Warning"</c> whenever it is true on a reachable server. A reader who cannot check a field's name
    /// against its population has to trust the name, and #3098 measured what that costs on this one: two
    /// agents drew four wrong conclusions from it in a single day, one of them a retracted claim about WHEN a
    /// cluster of collector errors happened. Every field on this card that cannot be recomputed from the card
    /// is one more that has to be trusted.</para>
    ///
    /// <para><b>Not an error signal, and there are two that are.</b> <c>failed_collector_count</c> counts
    /// collectors currently failing and <c>collector_severity</c> bands it. Those and this one disagree
    /// routinely and correctly: a server can collect on time with a collector failing, and can go quiet with
    /// every collector's last run a success.</para>
    /// </summary>
    [JsonPropertyName("collection_stale")] public bool CollectionStale { get; init; }

    /// <summary>The newest collection inside the fleet read's two-day window
    /// (<see cref="DarlingFleetReader.LastCollectionWindow"/>); null when the window holds none. A null means
    /// one of two things and <c>status</c> says which (#3935): "Awaiting first collection" for a server
    /// registered inside the window that has not collected yet, "Offline" for one that went quiet before the
    /// window began. The WPF viewer and <c>list_servers</c> read this instant with no window, so for the
    /// second kind they show the older time this card leaves out; the band agrees.</summary>
    [JsonPropertyName("last_collection")] public DateTime? LastCollectionTime { get; init; }

    /// <summary>SQL Server's OWN share of host CPU, from the ring buffer. Null on Azure SQL DB and on every
    /// PostgreSQL target: Performance Insights reports the host total and no per-process breakdown, so there
    /// is no value to put here and filling it from the total would claim an attribution nothing measured.
    /// <see cref="CpuSource"/> says which arm answered — do not infer it from this field being null.</summary>
    [JsonPropertyName("cpu_percent")] public double? CpuPercent { get; init; }

    /// <summary>Non-database CPU on the host, from the ring buffer. Null wherever <see cref="CpuPercent"/>
    /// is, and additionally on SQL Server on Linux before 2025 CU1 (<c>SystemIdle</c> reports 0 there, so
    /// the host share is not derivable).</summary>
    [JsonPropertyName("other_process_cpu_percent")] public double? OtherProcessCpuPercent { get; init; }

    /// <summary>Total non-idle host CPU, the one field populated on BOTH engines (#3267). SQL Server's ring
    /// buffer reaches it as <c>sqlserver + other_process</c>; a PostgreSQL/Aurora target's is Performance
    /// Insights' <c>os.cpuUtilization.total.avg</c> verbatim.
    ///
    /// <para><b>It is what <see cref="CpuSeverity"/> bands only on the SQL Server arm</b> (#3281). On the
    /// Performance Insights arm this figure is percent of the capacity CURRENTLY ALLOCATED, which on
    /// Aurora Serverless v2 moves — 100% here is routinely a scale-up rather than saturation — so the band
    /// reads <see cref="AcuUtilizationPercent"/> instead. This value stays published unchanged because it
    /// answers a real question ("was a core pinned"); it is just not the saturation signal.</para></summary>
    [JsonPropertyName("total_cpu_percent")] public double? TotalCpuPercent { get; init; }

    /// <summary>The Performance Insights instance reading on its own (#2719/#3267) — the same number
    /// <see cref="TotalCpuPercent"/> carries on a PostgreSQL target, published separately so a consumer can
    /// see the raw per-source value without unpicking the fallback. Null on every SQL Server target.</summary>
    [JsonPropertyName("instance_cpu_percent")] public double? InstanceCpuPercent { get; init; }

    /// <summary>Percent of the CONFIGURED capacity ceiling in use — Aurora Serverless v2's
    /// <c>os.general.acuUtilization.avg</c> (#3281), and the figure <see cref="CpuSeverity"/> bands on the
    /// Performance Insights arm. Null on every SQL Server target, and null on a PostgreSQL target
    /// Performance Insights returned no capacity sample for, where the band reads Unknown rather than
    /// claiming health it never measured.</summary>
    [JsonPropertyName("acu_utilization_percent")] public double? AcuUtilizationPercent { get; init; }

    /// <summary>The cluster's configured ACU ceiling at this reading —
    /// <c>os.general.maxConfiguredAcu.avg</c> (#3281). Published so a consumer can state the headroom in
    /// ACUs rather than only as a percentage, and because it is the answer to "so raise what": a serverless
    /// instance genuinely at its ceiling is fixed by the ceiling, not by the workload. Null wherever
    /// <see cref="AcuUtilizationPercent"/> is.</summary>
    [JsonPropertyName("max_configured_acu")] public double? MaxConfiguredAcu { get; init; }

    /// <summary>Which collector produced this card's CPU number, and when there is none, which of the two
    /// reasons (#3267). The default arm is <see cref="FleetCpuSource.NotCollected"/>, so a card built
    /// without either reading claims nothing rather than sitting at an arm that means "measured".</summary>
    [JsonPropertyName("cpu_source")] public FleetCpuSource CpuSource { get; init; }

    [JsonPropertyName("cpu_severity")] public HealthSeverity CpuSeverity { get; init; }

    [JsonPropertyName("memory_mb")] public double? MemoryMb { get; init; }
    [JsonPropertyName("buffer_pool_mb")] public double? BufferPoolMb { get; init; }
    [JsonPropertyName("granted_memory_mb")] public double? GrantedMemoryMb { get; init; }
    [JsonPropertyName("memory_waiter_count")] public long MemoryWaiterCount { get; init; }
    [JsonPropertyName("memory_timeout_count")] public long MemoryTimeoutCount { get; init; }
    [JsonPropertyName("memory_forced_count")] public long MemoryForcedCount { get; init; }
    [JsonPropertyName("has_memory_pressure")] public bool HasMemoryPressure { get; init; }
    [JsonPropertyName("memory_severity")] public HealthSeverity MemorySeverity { get; init; }

    [JsonPropertyName("blocking_count")] public int BlockingCount { get; init; }
    [JsonPropertyName("max_blocking_wait_ms")] public long MaxBlockingWaitMs { get; init; }
    /// <summary>Blocking events per HOUR over the card's window — the figure the count arm of
    /// <c>blocking_severity</c> banded on (#3539 A3), or null when the window was too short to normalise.
    /// Published beside the raw count for <see cref="DeadlockRatePerHour"/>'s reason.</summary>
    [JsonPropertyName("blocking_rate_per_hour")] public double? BlockingRatePerHour { get; init; }
    [JsonPropertyName("blocking_severity")] public HealthSeverity BlockingSeverity { get; init; }

    /// <summary>The window <c>blocking_count</c> covers (#3539 A3) — carried, not serialized, for the reason
    /// <see cref="DeadlockWindow"/> is; the roll-up already publishes the bounds once.</summary>
    [JsonIgnore]
    public TimeSpan BlockingWindow { get; init; }

    /// <summary>Deadlocks in the card's window, from the engine's own instrument: captured deadlock graphs
    /// on SQL Server, the <c>pg_stat_database.deadlocks</c> counter differenced per database and summed on
    /// PostgreSQL (#3539). <see cref="DeadlockSource"/> says which, and whether the collector behind it was
    /// actually running.</summary>
    [JsonPropertyName("deadlock_count")] public int DeadlockCount { get; init; }

    /// <summary>The newest deadlock in the window — the graph's own timestamp on SQL Server; on PostgreSQL
    /// the sample that first showed the counter step, so "within the preceding minute".</summary>
    [JsonPropertyName("deadlock_last_seen")] public DateTime? DeadlockLastSeen { get; init; }

    /// <summary>Whether <see cref="DeadlockCount"/> is a measurement this card banded on (#3539) — always
    /// on SQL Server (a graph count is an observation even at zero), and on PostgreSQL only when at least
    /// one counter difference was taken in the window (a difference of fewer than two samples is no
    /// reading). Carried, not serialized: <c>deadlock_rate_per_hour</c> is null and
    /// <c>deadlock_severity</c> is Unknown exactly when this is false on a rateable window, so the wire
    /// already says it; this is for <see cref="ToHealthMetrics"/>, which must hand the re-band the same
    /// null the card banded on. Defaults to false so a card built by a path that did not decide reads
    /// unmeasured — the direction that cannot claim health.</summary>
    [JsonIgnore]
    public bool DeadlockMeasured { get; init; }
    /// <summary>Deadlocks per HOUR over the card's window — the figure <c>deadlock_severity</c> banded on
    /// (#3368), or null when the window was too short to normalise. Published beside the raw count because a
    /// card that bands on a number it does not show leaves a reader unable to tell which tier was
    /// crossed.</summary>
    [JsonPropertyName("deadlock_rate_per_hour")] public double? DeadlockRatePerHour { get; init; }

    [JsonPropertyName("deadlock_severity")] public HealthSeverity DeadlockSeverity { get; init; }

    /// <summary>The window <c>deadlock_count</c> covers (#3368) — carried, not serialized, so
    /// <see cref="ToHealthMetrics"/> can re-band on the same denominator the card banded on. The window is
    /// already published on the roll-up as <c>window_start</c> / <c>window_end</c>, so a second copy on
    /// every card would be 43 restatements of one fact.</summary>
    [JsonIgnore]
    public TimeSpan DeadlockWindow { get; init; }

    /// <summary>The tiers this card banded on (#3368) — carried, not serialized, for
    /// <see cref="ToHealthMetrics"/>. <c>get_alert_settings</c> is where a reader asks what they are, and
    /// putting them on every card would invite reading two cards' copies as two configurations.</summary>
    [JsonIgnore]
    public DeadlockRateThresholds DeadlockRateThresholds { get; init; }

    /// <summary>This server's deadlock-source collector band over the trailing seven days of collection
    /// health (#3017) — the fact that explains a <see cref="DeadlockCount"/> of zero. The collector is
    /// <c>deadlocks</c> on SQL Server and <c>pg_database_stats</c> on PostgreSQL (#3539). Null when that
    /// collector left no row in the health window, which is itself the answer rather than the absence of
    /// one: nothing was read for this server.</summary>
    [JsonPropertyName("deadlock_collector_band")] public string? DeadlockCollectorBand { get; init; }

    /// <summary>Whether <see cref="DeadlockCount"/> read a deadlock source for this server, and when it did
    /// not, which cause (#3017) — see <see cref="FleetDeadlockCoverage.ClassifyDeadlockSource"/> for what each
    /// value means and what it asks an operator to do.
    ///
    /// <para>DERIVED rather than assigned, for the reason <see cref="EngineDescription"/> is: every card is
    /// built by an object initializer, and a settable field would sit at its enum default on any card whose
    /// builder did not think of it — including one added later on a path nobody re-reads. Derived, the
    /// unset case is <see cref="FleetDeadlockSource.CollectorSilent"/>, which is the honest reading of a
    /// card carrying no band and the only default that cannot inflate the fleet's coverage.</para></summary>
    [JsonPropertyName("deadlock_source")]
    public FleetDeadlockSource DeadlockSource =>
        FleetDeadlockCoverage.ClassifyDeadlockSource(IsPostgres, DeadlockCollectorBand);

    [JsonPropertyName("total_threads")] public int? TotalThreads { get; init; }
    [JsonPropertyName("current_workers")] public int? CurrentWorkers { get; init; }
    [JsonPropertyName("available_threads")] public int? AvailableThreads { get; init; }
    [JsonPropertyName("threads_waiting_for_cpu")] public int ThreadsWaitingForCpu { get; init; }
    [JsonPropertyName("requests_waiting_for_threads")] public long RequestsWaitingForThreads { get; init; }
    [JsonPropertyName("threads_severity")] public HealthSeverity ThreadsSeverity { get; init; }

    [JsonPropertyName("healthy_collector_count")] public int HealthyCollectorCount { get; init; }
    [JsonPropertyName("failed_collector_count")] public int FailedCollectorCount { get; init; }

    /// <summary>
    /// Collectors on this server that WERE producing rows and now report a named skip every cycle
    /// (#3819) — the number an install countersign reads instead of a status word.
    ///
    /// <para><b>The reading it exists to end.</b> The <c>.453</c> install took <c>pg_statement_stats</c>
    /// from 85% productive to <c>EXTENSION_MISSING</c> every cycle on 23 of 50 PostgreSQL clusters. That
    /// status is a legitimate resting state for an optional module, so every surface read it as one — and
    /// for the first day the collectors' own band read HEALTHY besides, because their last productive
    /// cycle was hours old and the staleness ladder had nothing to fire on. The countersign accepted it
    /// for 24 hours. A named skip on a collector that had been producing is a different fact from the
    /// same status on one that never has, and this is the card's half of telling them apart.</para>
    ///
    /// <para><b>Deliberately NOT a component of <see cref="FailedCollectorCount"/>, and never to be added
    /// to it.</b> The two populations overlap: a regression reads HEALTHY-floored-to-WARNING on day one
    /// and FAILING on day two, regressed throughout. They are separate axes of the same rows, so a sum
    /// would double-count exactly the servers this is for. <see cref="CollectorSeverity"/> still grades
    /// the FAILING share alone, because widening a severity is a separate question with its own evidence
    /// bar.</para>
    /// </summary>
    [JsonPropertyName("regressed_collector_count")] public int RegressedCollectorCount { get; init; }

    /// <summary>Every collector banded for this server in the health window, on any band (#3539 A8d) — the
    /// denominator <c>collector_severity</c> grades <c>failed_collector_count</c> against. Not
    /// healthy + failed: STALE, WARNING, STOPPED and the permission bands are banded collectors that are
    /// neither. Zero with nothing failing bands <c>collector_severity</c> Unknown, not Healthy (#3539
    /// A6): no collector has been banded for this server, so there is no collection to call clean.</summary>
    [JsonPropertyName("collector_count")] public int CollectorCount { get; init; }
    [JsonPropertyName("collector_severity")] public HealthSeverity CollectorSeverity { get; init; }

    /// <summary>The worst per-metric band, or Unknown when NOT ONE metric on the card was measured (#3539
    /// A6) — which <c>band</c> then reads as Warning, the never-collected server's band, rather than
    /// Healthy.</summary>
    [JsonPropertyName("overall_metric_severity")] public HealthSeverity OverallMetricSeverity { get; init; }

    /// <summary>How many of the card's per-metric severities carried a real reading when it banded (#3528)
    /// — the band's fold skips Unknown, so a card can read Healthy off one measured metric of six. When
    /// this is below <see cref="MetricCount"/>, the band label deserves the qualifier ("Healthy — 1 of 6
    /// measured"); the web fleet page renders exactly that. Purely descriptive: it feeds neither the band
    /// nor the worst-first score, so rank-neutrality of Unknown is unchanged — with the one exception
    /// #3539 A6 draws at zero: a card measuring NOTHING is not Healthy (see <c>overall_metric_severity</c>),
    /// and its <c>reason</c> says so.</summary>
    [JsonPropertyName("measured_metric_count")] public int MeasuredMetricCount { get; init; }

    /// <summary>The denominator for <see cref="MeasuredMetricCount"/> — how many per-metric severities the
    /// card carries at all. Published rather than assumed at six so a consumer never hardcodes a figure the
    /// next metric row changes.</summary>
    [JsonPropertyName("metric_count")] public int MetricCount { get; init; }

    /// <summary>The card's raw per-metric inputs, for re-scoring in the rollup (not serialized).</summary>
    [JsonIgnore]
    public ServerHealthMetrics ToHealthMetricsValue => ToHealthMetrics();

    internal ServerHealthMetrics ToHealthMetrics() => new()
    {
        CpuPercentForAlert = TotalCpuPercent ?? CpuPercent,
        /* #3281: the three travel together, because the band reads the capacity figure on the Performance
           Insights arm and the source is what decides which. Omitting them here would leave the CPU DOT
           banded on the ceiling while the overall band and the fleet score fell back to
           percent-of-allocated — a card contradicting itself, and a routine scale-up ranked as maxed out. */
        CapacityUtilizationPercent = AcuUtilizationPercent,
        CpuSource = CpuSource,
        /* Re-derived from IsPostgres rather than read back off the published counts, because those are
           deliberately left as zeros (#3017) — reading them here would hand the ranking a measurement the
           card's own severity says it does not have. */
        HasMemoryPressure = ServerMetricSources.DmvSourced(HasMemoryPressure, IsPostgres),
        BlockingCount = ServerMetricSources.DmvSourced(BlockingCount, IsPostgres),
        MaxBlockedSeconds = MaxBlockingWaitMs / 1000.0,
        /* #3539 A3: the count's denominator, for the same reason the deadlock window travels below. */
        BlockingWindow = BlockingWindow,
        /* The engine's own instrument's count since #3539, handed to the re-band exactly as the card
           banded it: measured on every SQL Server card, and on a PostgreSQL card only when a difference
           was taken - see DeadlockMeasured. */
        DeadlockCount = DeadlockMeasured ? DeadlockCount : null,
        /* #3368: the three travel together for the reason the CPU trio above does. Without the window the
           re-band would have no denominator and the worst-first score would rank every deadlocking server
           at Warning; without the tiers it would rank them against the shipped pair while the card's own
           dot used the store's. */
        DeadlockWindow = DeadlockWindow,
        DeadlockRateThresholds = DeadlockRateThresholds,
        TotalThreads = TotalThreads,
        AvailableThreads = AvailableThreads,
        ThreadsWaitingForCpu = ThreadsWaitingForCpu,
        RequestsWaitingForThreads = RequestsWaitingForThreads,
        FailedCollectorCount = FailedCollectorCount,
        /* #3539 A8d: the share's denominator, or the re-band would grade presence-flat again. */
        CollectorCount = CollectorCount,
    };
}

/// <summary>One tag on a fleet card — read-only, for the web pills (#2020). Serialized snake_case like the card.
/// <c>colour</c> is the stored <c>#RRGGBB</c> or null (null renders as a neutral pill, matching the desktop
/// apps); tagging itself stays with the Viewer / Lite.</summary>
public sealed class FleetTag
{
    [JsonPropertyName("id")] public int Id { get; init; }
    [JsonPropertyName("name")] public string Name { get; init; } = "";
    [JsonPropertyName("colour")] public string? Colour { get; init; }
}

/// <summary>One node in the fleet's tag forest — read-only, for the web tree/group rendering (#2020). Carries the
/// hierarchy (<c>parent_id</c>, null at a root) and the ordering the desktop FleetView projects with; a
/// <c>colour</c> of null renders as a neutral header, matching the pills.</summary>
public sealed class FleetTagNode
{
    [JsonPropertyName("id")] public int Id { get; init; }
    [JsonPropertyName("name")] public string Name { get; init; } = "";
    [JsonPropertyName("parent_id")] public int? ParentId { get; init; }
    [JsonPropertyName("sort_order")] public int SortOrder { get; init; }
    [JsonPropertyName("colour")] public string? Colour { get; init; }
}

/// <summary>One entry in the fleet's worst-first "Needs attention" ranking.</summary>
public sealed class FleetRankedServer
{
    [JsonPropertyName("server_id")] public int ServerId { get; init; }
    [JsonPropertyName("display_name")] public string DisplayName { get; init; } = "";
    [JsonPropertyName("band")] public FleetHealthBand Band { get; init; }
    [JsonPropertyName("band_label")] public string BandLabel => ServerHealthClassifier.BandLabel(Band);
    [JsonPropertyName("score")] public long Score { get; init; }
    [JsonPropertyName("reason")] public string Reason { get; init; } = "";
}

/// <summary>The full fleet roll-up payload — the pre-banded per-server cards, the band counts, the cross-server
/// totals, and the worst-first ranking. The web dashboard's <c>/api/fleet</c> body and the
/// <c>get_fleet_overview</c> MCP tool's serialized result.</summary>
public sealed class FleetOverviewResult
{
    [JsonPropertyName("generated_at")] public DateTime GeneratedAt { get; init; }
    [JsonPropertyName("window_start")] public DateTime WindowStart { get; init; }
    [JsonPropertyName("window_end")] public DateTime WindowEnd { get; init; }
    [JsonPropertyName("total_servers")] public int TotalServers { get; init; }
    [JsonPropertyName("healthy_count")] public int HealthyCount { get; init; }
    [JsonPropertyName("warning_count")] public int WarningCount { get; init; }
    [JsonPropertyName("critical_count")] public int CriticalCount { get; init; }
    [JsonPropertyName("offline_count")] public int OfflineCount { get; init; }
    [JsonPropertyName("servers_with_collection_failures")] public int ServersWithCollectionFailures { get; init; }
    [JsonPropertyName("total_blocking_events")] public long TotalBlockingEvents { get; init; }
    [JsonPropertyName("total_deadlocks")] public long TotalDeadlocks { get; init; }

    /// <summary>How much of the fleet <see cref="TotalDeadlocks"/> actually read a deadlock source for, with
    /// the causes named (#3017). Never null — a total with no denominator beside it is the defect.</summary>
    [JsonPropertyName("deadlock_coverage")] public FleetDeadlockCoverage DeadlockCoverage { get; init; } = new();

    [JsonPropertyName("additional_problem_count")] public int AdditionalProblemCount { get; init; }
    [JsonPropertyName("worst_servers")] public IReadOnlyList<FleetRankedServer> WorstServers { get; init; } = Array.Empty<FleetRankedServer>();
    [JsonPropertyName("cards")] public IReadOnlyList<FleetServerCard> Cards { get; init; } = Array.Empty<FleetServerCard>();

    /// <summary>The full tag forest for the read-only web tree/group rendering (#2020) — every tag with its
    /// parent, ordering, and colour, so the fleet page can group cards under a nested tag tree even when a parent
    /// tag has no directly-assigned servers. Empty when no tags are defined. Assignment/editing stays desktop-only.</summary>
    [JsonPropertyName("tags")] public IReadOnlyList<FleetTagNode> Tags { get; init; } = Array.Empty<FleetTagNode>();

    /// <summary>How many whole seconds older than <see cref="GeneratedAt"/> the collection-health half of this
    /// roll-up is (#3735): 0 when the 7-day collector-health scan ran for this call, otherwise the age of the
    /// memoized scan it was served from — under 60 on a hit, because that is how long
    /// <c>DarlingFleetReader</c> serves one scan before re-reading. The half it qualifies is every card's
    /// <c>healthy_collector_count</c> / <c>failed_collector_count</c> / <c>collector_count</c> /
    /// <c>collector_severity</c> / <c>deadlock_collector_band</c> / <c>deadlock_source</c>, and the fleet's
    /// <c>servers_with_collection_failures</c> and <c>deadlock_coverage</c>; <c>generated_at</c> minus this is
    /// that half's own reference instant. Everything else on the payload was read for this call. Published
    /// so an agent that just watched a collector fail and re-reads the fleet a moment later knows why the dot
    /// has not moved yet, and because a roll-up that serves a memo without saying so is a roll-up whose
    /// freshness has to be trusted. Trailing and additive.</summary>
    [JsonPropertyName("collection_health_age_seconds")] public int CollectionHealthAgeSeconds { get; init; }
}
