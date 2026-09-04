/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

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
/// <c>server_id</c> in ONE Postgres database, this reader instead runs a BOUNDED set of cross-server aggregates
/// (one <c>DISTINCT ON (server_id)</c> latest-snapshot read per metric, one <c>GROUP BY server_id</c> windowed
/// count per incident source, one cross-server collection-health aggregate) — a fixed ~9 round-trips regardless
/// of fleet size — then assembles the per-server cards in C#.</para>
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

    /// <summary>Latest SQL + other-process CPU per server (newest ring-buffer sample). $ none.</summary>
    public const string FleetCpuSql = @"
SELECT DISTINCT ON (server_id)
    server_id,
    sqlserver_cpu_utilization,
    other_process_cpu_utilization
FROM v_cpu_utilization_stats
ORDER BY server_id, sample_time DESC";

    /// <summary>Latest total server memory + buffer pool (MB) per server. $ none.</summary>
    public const string FleetMemorySql = @"
SELECT DISTINCT ON (server_id)
    server_id,
    CAST(total_server_memory_mb AS double precision),
    CAST(buffer_pool_mb AS double precision)
FROM v_memory_stats
ORDER BY server_id, collection_time DESC";

    /// <summary>Latest resource-semaphore pressure per server — grant waiters / timeout + forced-grant deltas /
    /// granted MB, summed across every pool at each server's newest grant-snapshot instant. $ none.</summary>
    public const string FleetMemoryPressureSql = @"
SELECT
    m.server_id,
    CAST(COALESCE(SUM(m.waiter_count), 0) AS bigint),
    CAST(COALESCE(SUM(m.timeout_error_count_delta), 0) AS bigint),
    CAST(COALESCE(SUM(m.forced_grant_count_delta), 0) AS bigint),
    CAST(COALESCE(SUM(m.granted_memory_mb), 0) AS double precision)
FROM v_memory_grant_stats m
JOIN
(
    SELECT server_id, MAX(collection_time) AS max_collection_time
    FROM v_memory_grant_stats
    GROUP BY server_id
) latest
    ON m.server_id = latest.server_id
    AND m.collection_time = latest.max_collection_time
GROUP BY m.server_id";

    /// <summary>Latest worker-thread pressure per server (newest cpu_scheduler_stats snapshot). $ none.</summary>
    public const string FleetThreadsSql = @"
SELECT DISTINCT ON (server_id)
    server_id,
    max_workers_count,
    total_current_workers_count,
    total_runnable_tasks_count,
    total_work_queue_count
FROM v_cpu_scheduler_stats
ORDER BY server_id, collection_time DESC";

    /// <summary>Blocking in the window per server from BOTH sources — XE blocked-process reports and the always-on
    /// DMV snapshot, each counted per <c>server_id</c> with its worst wait, lined up by a FULL OUTER JOIN so a
    /// server present in only one source still appears. The caller applies Lite's XE-preferred / DMV-fallback per
    /// server. $1 window start, $2 window end (both naive UTC).</summary>
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
    GROUP BY server_id
) AS xe
FULL OUTER JOIN
(
    SELECT server_id, COUNT(*) AS cnt, MAX(wait_time_ms) AS max_wait
    FROM v_dmv_blocking_snapshots
    WHERE event_time >= $1
    AND   event_time <= $2
    GROUP BY server_id
) AS dmv ON xe.server_id = dmv.server_id";

    /// <summary>Deadlocks in the window per server — count and newest deadlock instant (for each card's
    /// "last seen" detail). $1 window start, $2 window end (both naive UTC).</summary>
    public const string FleetDeadlockSql = @"
SELECT server_id, COUNT(*) AS cnt, MAX(deadlock_time) AS last_seen
FROM v_deadlocks
WHERE deadlock_time >= $1
AND   deadlock_time <= $2
GROUP BY server_id";

    /// <summary>Newest collection time per server — drives each card's freshness status. $1 window start.
    /// Bounded (not a bare GROUP BY over the whole table) so TimescaleDB can chunk-exclude: this table only
    /// grows, and every collector run adds a row, so an unbounded MAX(collection_time) over ALL history was
    /// re-scanning the server's ENTIRE collection archive (millions of rows) on every fleet-overview call just
    /// to find a timestamp from the last few minutes — the exact "materialize a bound, don't scan the whole
    /// history" mistake fixed elsewhere today (pg_statement_stats #2691, pg_wait_stats #2695). The window is
    /// 48 hours, not the OfflineThreshold this feeds: a server genuinely offline for HOURS must still
    /// report its true last-seen time (age computed correctly, still bands Offline) rather than falling out of
    /// the result entirely and being treated as having no history at all.</summary>
    public const string FleetLastCollectionSql = @"
SELECT server_id, MAX(collection_time) AS last_collection_time
FROM v_collection_log
WHERE collection_time >= $1
GROUP BY server_id";

    /// <summary>Cross-server per-collector 7-day health aggregate — one row per (server, collector) pair carrying
    /// the columns the shared <c>CollectorHealth.HealthStatus</c> banding needs, so the caller counts each
    /// server's FAILING collectors exactly as the per-server Collection Health tab does. $1 window start (the
    /// trailing 7 days, naive UTC). <c>last_run_time</c> (any status, not just success) feeds the STOPPED band
    /// — a collector that has gone dark entirely (its AppliesTo gate flipped off, say) must not read as
    /// FAILING just because its last SUCCESS is old; a collector still being invoked and erroring every cycle
    /// has a recent last_run_time and correctly stays FAILING.</summary>
    public const string FleetCollectionHealthSql = @"
SELECT
    server_id,
    collector_name,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
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
    SUM(CASE WHEN status = 'ABANDONED' THEN 1 ELSE 0 END) AS abandoned_count
FROM v_collection_log
WHERE collection_time >= $1
GROUP BY server_id, collector_name";

    /// <summary>The default depth of the worst-first "Needs attention" ranking.</summary>
    public const int DefaultWorstCount = 5;

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
        var memory = await ReadMemoryAsync(postgres, cancellationToken);
        var memoryPressure = await ReadMemoryPressureAsync(postgres, cancellationToken);
        var threads = await ReadThreadsAsync(postgres, cancellationToken);
        var blocking = await ReadBlockingAsync(postgres, windowStartUtc, windowEndUtc, cancellationToken);
        var deadlocks = await ReadDeadlocksAsync(postgres, windowStartUtc, windowEndUtc, cancellationToken);
        var lastCollection = await ReadLastCollectionAsync(postgres, now, cancellationToken);
        var failingCollectors = await ReadFailingCollectorCountsAsync(postgres, now, cancellationToken);
        var tags = await ReadTagsAsync(postgres, cancellationToken);
        var tagForest = await ReadTagForestAsync(postgres, cancellationToken);

        var cards = new List<FleetServerCard>(servers.Count);
        foreach (var server in servers)
        {
            cpu.TryGetValue(server.ServerId, out var c);
            memory.TryGetValue(server.ServerId, out var m);
            memoryPressure.TryGetValue(server.ServerId, out var mp);
            threads.TryGetValue(server.ServerId, out var t);
            blocking.TryGetValue(server.ServerId, out var b);
            deadlocks.TryGetValue(server.ServerId, out var deadlock);
            /* Not `lastCollection.TryGetValue(..., out var lastColl)` — that leaves lastColl as
               default(DateTime) (0001-01-01) on a miss, and default(DateTime) is NOT null, so it
               does not hit ClassifyFreshness's NeverCollected branch: it falls through to the
               age-vs-OfflineThreshold check with an age of ~2000 years and always bands Offline.
               A server whose row fell outside the bounded window above (or, before that fix, was
               ever missing for any other reason) must read as "no recent history", not as a fake
               ancient timestamp. */
            DateTime? lastColl = lastCollection.TryGetValue(server.ServerId, out var lastCollValue)
                ? lastCollValue
                : null;
            failingCollectors.TryGetValue(server.ServerId, out var collectors);
            tags.TryGetValue(server.ServerId, out var serverTags);

            cards.Add(BuildCard(server, c, m, mp, t, b, deadlock, lastColl, collectors, serverTags, now));
        }

        return BuildRollup(cards, now, windowStartUtc, windowEndUtc, worstCount, tagForest);
    }

    /// <summary>Builds one pre-banded card from a server's raw cross-server reads (pure — the reduction the WPF
    /// <c>ServerSummaryItem</c> does, minus the brushes, over the shared classifier).</summary>
    private static FleetServerCard BuildCard(
        FleetServerRow server,
        CpuRow cpu,
        MemoryRow memory,
        MemoryPressureRow pressure,
        ThreadsRow threads,
        BlockingRow blocking,
        DeadlockRow deadlock,
        DateTime? lastCollection,
        CollectorCounts collectors,
        List<FleetTag>? tags,
        DateTime now)
    {
        var deadlockCount = deadlock.Count;

        /* Lite's XE-preferred / DMV-fallback, per server: XE when it has any row this window, else the DMV
           snapshot — both count and worst-wait come from whichever source wins. */
        var blockingCount = blocking.XeCount > 0 ? blocking.XeCount : blocking.DmvCount;
        var maxBlockingWaitMs = blocking.XeCount > 0 ? blocking.XeMaxWait : blocking.DmvMaxWait;

        var cpuPercent = cpu.SqlCpu;
        var otherCpu = cpu.OtherCpu;
        var totalCpu = cpuPercent.HasValue ? cpuPercent.Value + (otherCpu ?? 0) : (double?)null;
        var cpuForAlert = totalCpu ?? cpuPercent;

        var availableThreads = threads.TotalThreads.HasValue
            ? threads.TotalThreads.Value - (threads.CurrentWorkers ?? 0)
            : (int?)null;

        var hasMemoryPressure = pressure.WaiterCount > 0 || pressure.TimeoutCount > 0 || pressure.ForcedCount > 0;
        var maxBlockedSeconds = maxBlockingWaitMs / 1000.0;

        var metrics = new ServerHealthMetrics
        {
            CpuPercentForAlert = cpuForAlert,
            HasMemoryPressure = hasMemoryPressure,
            BlockingCount = blockingCount,
            MaxBlockedSeconds = maxBlockedSeconds,
            DeadlockCount = deadlockCount,
            TotalThreads = threads.TotalThreads,
            AvailableThreads = availableThreads,
            ThreadsWaitingForCpu = threads.RunnableTasks,
            RequestsWaitingForThreads = threads.WorkQueue,
            FailedCollectorCount = collectors.Failing,
        };

        /* Freshness -> the card's collection state, through the SAME mapping the WPF card and the sidebar
           row use (#2473). It was a hand-written copy of ApplyFreshness that happened to agree; the copy on
           the sidebar row happened not to, which is the argument for none of them writing it out. */
        var freshness = ServerHealthClassifier.ClassifyFreshness(lastCollection, now);
        var flags = ServerCollectionStatusRules.FlagsFor(freshness);
        var isOnline = flags.IsOnline;
        var awaitingFirstCollection = flags.AwaitingFirstCollection;
        var hasCollectorErrors = flags.HasCollectorErrors;

        var overall = ServerHealthClassifier.OverallMetricSeverity(metrics);
        var band = ServerHealthClassifier.ClassifyBand(isOnline, awaitingFirstCollection, hasCollectorErrors, overall);

        /* Per-server platform (design D4): the reliable signal the composer's measure auto-greying matches a
           measure's appliesTo against — see ClassifyPlatform for the edition mapping and why AWS RDS / msdb are
           deliberately not surfaced. */
        var (isAzureSqlDb, isAzureManagedInstance) = ClassifyPlatform(server.EngineEdition);

        /* Per-server target ENGINE (#2530), the axis the platform flags above cannot express: they are all
           derived from a SQL Server SERVERPROPERTY, which a PostgreSQL target does not have. */
        var (isPostgres, isAurora) = ClassifyEngineKind(server.EngineKind);

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
            Status = StatusLabel(isOnline, awaitingFirstCollection, hasCollectorErrors),
            IsOnline = isOnline,
            AwaitingFirstCollection = awaitingFirstCollection,
            HasCollectorErrors = hasCollectorErrors,
            LastCollectionTime = lastCollection,
            CpuPercent = cpuPercent,
            OtherProcessCpuPercent = otherCpu,
            TotalCpuPercent = totalCpu,
            CpuSeverity = ServerHealthClassifier.CpuSeverity(cpuForAlert),
            MemoryMb = memory.MemoryMb,
            BufferPoolMb = memory.BufferPoolMb,
            GrantedMemoryMb = pressure.GrantedMemoryMb,
            MemoryWaiterCount = pressure.WaiterCount,
            MemoryTimeoutCount = pressure.TimeoutCount,
            MemoryForcedCount = pressure.ForcedCount,
            HasMemoryPressure = hasMemoryPressure,
            MemorySeverity = ServerHealthClassifier.MemorySeverity(hasMemoryPressure),
            BlockingCount = blockingCount,
            MaxBlockingWaitMs = maxBlockingWaitMs,
            BlockingSeverity = ServerHealthClassifier.BlockingSeverity(blockingCount, maxBlockedSeconds),
            DeadlockCount = deadlockCount,
            DeadlockLastSeen = deadlock.LastSeen,
            DeadlockSeverity = ServerHealthClassifier.DeadlockSeverity(deadlockCount),
            TotalThreads = threads.TotalThreads,
            CurrentWorkers = threads.CurrentWorkers,
            AvailableThreads = availableThreads,
            ThreadsWaitingForCpu = threads.RunnableTasks,
            RequestsWaitingForThreads = threads.WorkQueue,
            ThreadsSeverity = ServerHealthClassifier.ThreadsSeverity(threads.TotalThreads, availableThreads, threads.RunnableTasks, threads.WorkQueue),
            HealthyCollectorCount = collectors.Healthy,
            FailedCollectorCount = collectors.Failing,
            CollectorSeverity = ServerHealthClassifier.CollectorSeverity(collectors.Failing),
            OverallMetricSeverity = overall,
        };
    }

    /// <summary>Reduces the pre-banded cards to the fleet rollup — band counts, cross-server totals (summed from
    /// the cards), and the worst-first ranking. Pure so the reduction is unit-testable without a store.</summary>
    public static FleetOverviewResult BuildRollup(
        IReadOnlyList<FleetServerCard> cards,
        DateTime now,
        DateTime windowStartUtc,
        DateTime windowEndUtc,
        int worstCount = DefaultWorstCount,
        IReadOnlyList<FleetTagNode>? tags = null)
    {
        var healthy = 0;
        var warning = 0;
        var critical = 0;
        var offline = 0;
        var failures = 0;
        long totalBlocking = 0;
        long totalDeadlocks = 0;

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
            WorstServers = worst,
            AdditionalProblemCount = Math.Max(0, problems.Count - worst.Count),
            Cards = cards,
            Tags = tags ?? Array.Empty<FleetTagNode>(),
        };
    }

    /// <summary>A short "why it needs attention" line for a ranked server, from the card's own banded metrics —
    /// mirrors the WPF <c>FleetRollup.BuildReason</c> content over the pre-banded card.</summary>
    private static string BuildReason(FleetServerCard c)
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

        if (c.CpuSeverity >= HealthSeverity.Warning && c.TotalCpuPercent.HasValue)
        {
            parts.Add($"CPU {c.TotalCpuPercent.Value:F0}%");
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
            parts.Add($"Blocking {c.BlockingCount}");
        }

        if (c.DeadlockSeverity >= HealthSeverity.Warning && c.DeadlockCount > 0)
        {
            parts.Add($"Deadlocks {c.DeadlockCount}");
        }

        if (c.CollectorSeverity >= HealthSeverity.Warning)
        {
            parts.Add($"{c.FailedCollectorCount} collector{(c.FailedCollectorCount == 1 ? "" : "s")} failing");
        }

        if (c.HasCollectorErrors)
        {
            parts.Add("collection stale");
        }

        return parts.Count > 0 ? string.Join(", ", parts) : "Needs attention";
    }

    /// <summary>The card's status word. Delegates to the one ladder every Darling surface renders (#2473):
    /// this file's own copy agreed with the WPF card, but the WPF sidebar row's copy did not, and three
    /// agreeing copies plus one that does not is still four places where the answer is decided.</summary>
    private static string StatusLabel(bool? isOnline, bool awaitingFirstCollection, bool hasCollectorErrors) =>
        ServerCollectionStatusRules.Classify(isOnline, hasCollectorErrors, awaitingFirstCollection).Word();

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
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            map[reader.GetInt32(0)] = new CpuRow(
                reader.IsDBNull(1) ? null : Convert.ToDouble(reader.GetValue(1)),
                reader.IsDBNull(2) ? null : Convert.ToDouble(reader.GetValue(2)));
        }

        return map;
    }

    private static async Task<Dictionary<int, MemoryRow>> ReadMemoryAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var map = new Dictionary<int, MemoryRow>();
        await using var command = postgres.CreateCommand(FleetMemorySql);
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
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
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

    private static async Task<Dictionary<int, DeadlockRow>> ReadDeadlocksAsync(
        NpgsqlDataSource postgres, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken)
    {
        var map = new Dictionary<int, DeadlockRow>();
        await using var command = postgres.CreateCommand(FleetDeadlockSql);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            map[reader.GetInt32(0)] = new DeadlockRow(
                reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1)),
                reader.IsDBNull(2) ? null : reader.GetDateTime(2));
        }

        return map;
    }

    private static async Task<Dictionary<int, DateTime>> ReadLastCollectionAsync(NpgsqlDataSource postgres, DateTime now, CancellationToken cancellationToken)
    {
        var map = new Dictionary<int, DateTime>();
        await using var command = postgres.CreateCommand(FleetLastCollectionSql);
        AddTimestamp(command, DateTime.SpecifyKind(now.AddHours(-48), DateTimeKind.Unspecified));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (!reader.IsDBNull(1))
            {
                map[reader.GetInt32(0)] = reader.GetDateTime(1);
            }
        }

        return map;
    }

    /// <summary>Reads the cross-server 7-day collector health and counts each server's HEALTHY / FAILING
    /// collectors through the shared <see cref="CollectorHealth.HealthStatus"/> banding.</summary>
    private static async Task<Dictionary<int, CollectorCounts>> ReadFailingCollectorCountsAsync(
        NpgsqlDataSource postgres, DateTime now, CancellationToken cancellationToken)
    {
        var counts = new Dictionary<int, CollectorCounts>();
        await using var command = postgres.CreateCommand(FleetCollectionHealthSql);
        AddTimestamp(command, DateTime.SpecifyKind(now.AddDays(-7), DateTimeKind.Unspecified));
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
            };

            counts.TryGetValue(serverId, out var existing);
            var status = health.HealthStatus;
            counts[serverId] = new CollectorCounts(
                existing.Healthy + (status == "HEALTHY" ? 1 : 0),
                existing.Failing + (status == "FAILING" ? 1 : 0));
        }

        return counts;
    }

    private static void AddTimestamp(NpgsqlCommand command, DateTime value) =>
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(value, DateTimeKind.Unspecified) });

    /* ─────────────────────────── raw-read carriers (internal) ─────────────────────────── */

    private readonly record struct FleetServerRow(int ServerId, string DisplayName, string ServerName, int? EngineEdition, string? EngineKind, bool IsSilenced);
    private readonly record struct CpuRow(double? SqlCpu, double? OtherCpu);
    private readonly record struct MemoryRow(double? MemoryMb, double? BufferPoolMb);
    private readonly record struct MemoryPressureRow(long WaiterCount, long TimeoutCount, long ForcedCount, double? GrantedMemoryMb);
    private readonly record struct ThreadsRow(int? TotalThreads, int? CurrentWorkers, int RunnableTasks, long WorkQueue);
    private readonly record struct BlockingRow(int XeCount, long XeMaxWait, int DmvCount, long DmvMaxWait);
    private readonly record struct DeadlockRow(int Count, DateTime? LastSeen);
    private readonly record struct CollectorCounts(int Healthy, int Failing);
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
    [JsonPropertyName("has_collector_errors")] public bool HasCollectorErrors { get; init; }
    [JsonPropertyName("last_collection")] public DateTime? LastCollectionTime { get; init; }

    [JsonPropertyName("cpu_percent")] public double? CpuPercent { get; init; }
    [JsonPropertyName("other_process_cpu_percent")] public double? OtherProcessCpuPercent { get; init; }
    [JsonPropertyName("total_cpu_percent")] public double? TotalCpuPercent { get; init; }
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
    [JsonPropertyName("blocking_severity")] public HealthSeverity BlockingSeverity { get; init; }

    [JsonPropertyName("deadlock_count")] public int DeadlockCount { get; init; }
    [JsonPropertyName("deadlock_last_seen")] public DateTime? DeadlockLastSeen { get; init; }
    [JsonPropertyName("deadlock_severity")] public HealthSeverity DeadlockSeverity { get; init; }

    [JsonPropertyName("total_threads")] public int? TotalThreads { get; init; }
    [JsonPropertyName("current_workers")] public int? CurrentWorkers { get; init; }
    [JsonPropertyName("available_threads")] public int? AvailableThreads { get; init; }
    [JsonPropertyName("threads_waiting_for_cpu")] public int ThreadsWaitingForCpu { get; init; }
    [JsonPropertyName("requests_waiting_for_threads")] public long RequestsWaitingForThreads { get; init; }
    [JsonPropertyName("threads_severity")] public HealthSeverity ThreadsSeverity { get; init; }

    [JsonPropertyName("healthy_collector_count")] public int HealthyCollectorCount { get; init; }
    [JsonPropertyName("failed_collector_count")] public int FailedCollectorCount { get; init; }
    [JsonPropertyName("collector_severity")] public HealthSeverity CollectorSeverity { get; init; }

    [JsonPropertyName("overall_metric_severity")] public HealthSeverity OverallMetricSeverity { get; init; }

    /// <summary>The card's raw per-metric inputs, for re-scoring in the rollup (not serialized).</summary>
    [JsonIgnore]
    public ServerHealthMetrics ToHealthMetricsValue => ToHealthMetrics();

    internal ServerHealthMetrics ToHealthMetrics() => new()
    {
        CpuPercentForAlert = TotalCpuPercent ?? CpuPercent,
        HasMemoryPressure = HasMemoryPressure,
        BlockingCount = BlockingCount,
        MaxBlockedSeconds = MaxBlockingWaitMs / 1000.0,
        DeadlockCount = DeadlockCount,
        TotalThreads = TotalThreads,
        AvailableThreads = AvailableThreads,
        ThreadsWaitingForCpu = ThreadsWaitingForCpu,
        RequestsWaitingForThreads = RequestsWaitingForThreads,
        FailedCollectorCount = FailedCollectorCount,
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
    [JsonPropertyName("additional_problem_count")] public int AdditionalProblemCount { get; init; }
    [JsonPropertyName("worst_servers")] public IReadOnlyList<FleetRankedServer> WorstServers { get; init; } = Array.Empty<FleetRankedServer>();
    [JsonPropertyName("cards")] public IReadOnlyList<FleetServerCard> Cards { get; init; } = Array.Empty<FleetServerCard>();

    /// <summary>The full tag forest for the read-only web tree/group rendering (#2020) — every tag with its
    /// parent, ordering, and colour, so the fleet page can group cards under a nested tag tree even when a parent
    /// tag has no directly-assigned servers. Empty when no tags are defined. Assignment/editing stays desktop-only.</summary>
    [JsonPropertyName("tags")] public IReadOnlyList<FleetTagNode> Tags { get; init; } = Array.Empty<FleetTagNode>();
}
