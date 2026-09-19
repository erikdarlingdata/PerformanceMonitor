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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Service-side reads for the health MCP tools (<see cref="DarlingMcpHealthTools"/>) — the one-shot per-server
/// summary (get_server_summary) and the daily health rollup (get_daily_summary). Both reproduce the viewer's
/// proven reads (<c>ViewerDataService.Overview.cs</c> / <c>.DailySummary.cs</c>, themselves Lite's
/// <c>GetServerSummaryAsync</c> / <c>GetDailySummaryAsync</c> ported to Postgres) rather than referencing them —
/// the MCP host is in the Service assembly and cannot reference the WPF Viewer (the viewer's
/// <c>ServerSummaryItem</c> carries WPF brushes; only the raw metric reads are lifted here). All STORED reads
/// (no live monitored-server hit).
///
/// <para>The server-summary read is the CPU + memory + blocking + deadlock + last-collection subset the
/// same-named Lite tool exposes: latest SQL CPU, latest total server memory, blocking count in the last hour
/// (XE blocked-process reports, falling back to the always-on DMV snapshot when the XE count is zero), deadlock
/// count in the last hour, and the newest collection time. The daily-summary read is the viewer's full
/// <c>DailySummaryRangeSql</c>; the composite health band is computed by the SHARED
/// <see cref="DailyHealthBandCalculator"/> so it bands identically to Lite and the Darling viewer.</para>
/// </summary>
internal static class DarlingHealthReader
{
    /* ═══════════════════════════ server summary ═══════════════════════════ */

    /// <summary>One server's one-shot health snapshot — the CPU/memory/blocking/deadlock/last-collection subset
    /// the same-named Lite tool exposes.</summary>
    public sealed record ServerSummaryReadResult(
        double? CpuPercent, double? MemoryMb, int BlockingCount, int DeadlockCount, DateTime? LastCollectionTime)
    {
        /// <summary>The <c>collection_time</c> of the CPU snapshot <see cref="CpuPercent"/> came from (#3541 A10)
        /// — its own clock, distinct from <see cref="LastCollectionTime"/>, which is the newest collection of
        /// ANY collector for the server. <c>init</c> rather than positional so the positional shape existing
        /// callers construct is unchanged. Null when there is no CPU row.</summary>
        public DateTime? CpuCapturedAt { get; init; }

        /// <summary>The <c>collection_time</c> of the memory snapshot <see cref="MemoryMb"/> came from (#3541
        /// A10). Null when there is no memory row.</summary>
        public DateTime? MemoryCapturedAt { get; init; }

        /// <summary>True when the server has no collected data at all (no CPU/memory snapshot and no collection
        /// log) — the tool surfaces the #1224 "unavailable" miss instead of an all-zero card.</summary>
        public bool HasNoData =>
            CpuPercent is null && MemoryMb is null && LastCollectionTime is null;
    }

    /// <summary>Latest SQL-process CPU for one server (newest ring-buffer sample). $1 server_id.
    /// Same shape and same reasoning as <c>DarlingWorker.LatestCpuSql</c>: ordered on the hypertable's
    /// <c>collection_time</c> partition column so ordered ChunkAppend stops at the newest chunk, with
    /// <c>sample_time</c> as the within-batch tiebreak, and no time predicate because <c>sample_time</c> is
    /// the monitored server's local wall clock.</summary>
    public const string ServerSummaryCpuSql = @"
SELECT sqlserver_cpu_utilization, collection_time
FROM v_cpu_utilization_stats
WHERE server_id = $1
ORDER BY collection_time DESC, sample_time DESC
LIMIT 1";

    /// <summary>Latest total server memory (MB) for one server, with the snapshot's own <c>collection_time</c>
    /// (#3541 A10). $1 server_id.</summary>
    public const string ServerSummaryMemorySql = @"
SELECT CAST(total_server_memory_mb AS double precision), collection_time
FROM v_memory_stats
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";

    /// <summary>Blocking counts in the window from both sources — XE blocked-process reports and the always-on
    /// DMV snapshot; the caller applies Lite's XE-preferred, DMV-fallback rule. $1 server_id, $2 window start.</summary>
    public const string ServerSummaryBlockingSql = @"
SELECT
    (SELECT COUNT(*) FROM v_blocked_process_reports WHERE server_id = $1 AND event_time >= $2),
    (SELECT COUNT(*) FROM v_dmv_blocking_snapshots  WHERE server_id = $1 AND event_time >= $2)";

    /// <summary>Deadlock count in the window. $1 server_id, $2 window start.</summary>
    public const string ServerSummaryDeadlockSql = @"
SELECT COUNT(*) FROM v_deadlocks WHERE server_id = $1 AND deadlock_time >= $2";

    /// <summary>Newest collection time across all collectors for one server. $1 server_id.</summary>
    public const string ServerSummaryLastCollectionSql = @"
SELECT MAX(collection_time) FROM v_collection_log WHERE server_id = $1";

    /// <summary>The span the blocking and deadlock counts cover, ending at the read's clock — Lite's window.
    /// Published by the tool so "recent" has a number.</summary>
    public const int ServerSummaryCountsWindowHours = 1;

    /// <summary>
    /// One server's one-shot health summary — the viewer's <c>GetServerSummaryAsync</c> reduced to the subset
    /// the same-named Lite tool serves. Blocking / deadlock counts use a one-hour window (Lite's window); CPU
    /// and memory take the newest snapshot, each carrying its own <c>collection_time</c> (#3541 A10) — the
    /// payload used to publish ONE clock (<c>last_collection</c>, the newest collection of ANY collector) beside
    /// two figures it did not stamp, so a CPU row from a collector that died yesterday read as current
    /// because the collection log was fresh from the collectors still running.
    /// </summary>
    public static async Task<ServerSummaryReadResult> GetServerSummaryAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var windowStart = DateTime.UtcNow.AddHours(-ServerSummaryCountsWindowHours);

        double? cpuPercent = null;
        DateTime? cpuCapturedAt = null;
        double? memoryMb = null;
        DateTime? memoryCapturedAt = null;
        var blockingCount = 0;
        var deadlockCount = 0;
        DateTime? lastCollection = null;

        await using (var command = postgres.CreateCommand(ServerSummaryCpuSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                cpuPercent = reader.IsDBNull(0) ? null : Convert.ToDouble(reader.GetValue(0));
                cpuCapturedAt = reader.GetDateTime(1);
            }
        }

        await using (var command = postgres.CreateCommand(ServerSummaryMemorySql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                memoryMb = reader.IsDBNull(0) ? null : Convert.ToDouble(reader.GetValue(0));
                memoryCapturedAt = reader.GetDateTime(1);
            }
        }

        await using (var command = postgres.CreateCommand(ServerSummaryBlockingSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            DarlingMcpReadParameters.AddTimestamp(command, windowStart);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                var xeCount = reader.IsDBNull(0) ? 0 : (int)reader.GetInt64(0);
                var dmvCount = reader.IsDBNull(1) ? 0 : (int)reader.GetInt64(1);
                /* Lite's fallback: use XE when it has any row this window, else the DMV snapshot. */
                blockingCount = xeCount > 0 ? xeCount : dmvCount;
            }
        }

        await using (var command = postgres.CreateCommand(ServerSummaryDeadlockSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            DarlingMcpReadParameters.AddTimestamp(command, windowStart);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                deadlockCount = reader.IsDBNull(0) ? 0 : (int)reader.GetInt64(0);
            }
        }

        await using (var command = postgres.CreateCommand(ServerSummaryLastCollectionSql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            var result = await command.ExecuteScalarAsync(cancellationToken);
            if (result is not null && result != DBNull.Value)
            {
                lastCollection = Convert.ToDateTime(result);
            }
        }

        return new ServerSummaryReadResult(cpuPercent, memoryMb, blockingCount, deadlockCount, lastCollection)
        {
            CpuCapturedAt = cpuCapturedAt,
            MemoryCapturedAt = memoryCapturedAt,
        };
    }

    /* ═══════════════════════════ daily summary ═══════════════════════════ */

    /// <summary>One day's rolled-up signals plus the shared composite health band. Structurally a subset of the
    /// viewer's <c>DailySummaryRow</c>; the band comes from the SHARED <see cref="DailyHealthBandCalculator"/>.</summary>
    public sealed record DailySummaryReadRow(
        DateTime SummaryDate, decimal TotalWaitTimeSec, string TopWaitType, long UniqueQueries, long DeadlockCount,
        long BlockingEvents, long HighCpuEvents, long CollectionErrors, long MemoryPressureEvents,
        long MemoryCriticalEvents, long AlertCount, long MaxBlockDurationMs, bool HasData)
    {
        /// <summary>The store's deadlock-rate tiers (#3368/#3525) — stamped by the calendar-day reads so
        /// <see cref="HealthBand"/> bands on the pair <c>get_alert_settings</c> reports rather than the
        /// shipped defaults. The default is the shipped pair, which is what a store at its V120 column
        /// defaults holds anyway.</summary>
        public DeadlockRateThresholds RateTiers { get; init; } = DeadlockRateThresholds.Default;

        /// <summary>The clock the still-forming day's window clamps against (#3525 review): anchored
        /// reads hand their resolved window end so a backdated as_of clamps against its own "now";
        /// unanchored reads (the explicit-date tool, the viewer path) band against the wall clock.</summary>
        public DateTime ReferenceUtc { get; init; } = DateTime.UtcNow;

        /// <summary>Collector runs of every status in the window (#3539 A2) — the denominator the
        /// collection-error share bands on. An <c>init</c> member rather than a positional parameter so the
        /// positional shape every existing constructor call uses is unchanged; the reader stamps it from the
        /// trailing <c>collection_runs</c> column.</summary>
        public long CollectionRuns { get; init; }

        /// <summary>
        /// Whether this row's counts are a measurement or the shape retention left behind (#3541 A9) — see
        /// <see cref="DailySummaryDataState"/>. Stamped by the range reader from the day, the run count and the
        /// store's retention horizon; the default is Collected so a row constructed without a reader (the
        /// tests' hand-built rows, the fleet sweep's) bands as it always did.
        /// </summary>
        public DailySummaryDataState DataState { get; init; } = DailySummaryDataState.Collected;

        /// <summary>The horizon <see cref="DataState"/> was judged against, carried so the single-day tool can
        /// publish it beside a purged verdict; <c>null</c> on a row nobody judged.</summary>
        public DateTime? RetentionHorizon { get; init; }

        /// <summary>How many of the seven per-signal sources hold at least one row for the day (#3541 A9) —
        /// the aggregate's trailing <c>signal_sources_present</c> column, the fact that tells a purged shell
        /// from a day the purge has not reached.</summary>
        public int SignalSourcesPresent { get; init; }

        public DailyHealthSignals ToSignals() => new()
        {
            /* #3541 A9: a purged or past-horizon day is a NoData day to the band, whatever the spine still
               holds for it — the COALESCEd zeros it carries may be absences, and measured-zero-Healthy was the
               lie. HasData alone said "a spine row exists", which the collection log's longer horizon made
               true for a whole second month of purged signals. Inside retention (Collected, NoRunRecord) a
               zero IS a measurement and the band stands. */
            HasData = HasData && DataState is not (DailySummaryDataState.Purged or DailySummaryDataState.PastHorizon),
            Deadlocks = DeadlockCount,
            CollectionErrors = CollectionErrors,
            CollectionRuns = CollectionRuns,
            HighCpuEvents = HighCpuEvents,
            BlockingEvents = BlockingEvents,
            /* #3539 A2: the day bands blocking through the card's BlockingSeverity, whose wait arm reads the
               longest block — so the peak travels in the signals rather than only into the reasons line. */
            PeakBlockWaitMs = MaxBlockDurationMs,
            MemoryPressureEvents = MemoryPressureEvents,
            MemoryCriticalEvents = MemoryCriticalEvents,
            AlertCount = AlertCount,
            /* #3525: a finished calendar day bands over its full 24 hours; the still-forming day clamps
               to its elapsed portion against ReferenceUtc, or an active storm dilutes against hours that
               have not happened yet (review finding on #3525). The fleet sweep does NOT read this
               projection: it sums this row type's raw counts into signals windowed to its own span. */
            Window = DailyHealthBandCalculator.CalendarDayWindow(SummaryDate, ReferenceUtc),
        };

        public DailyHealthBand HealthBand =>
            DailyHealthBandCalculator.Classify(ToSignals(), new DailyHealthThresholds { DeadlockRates = RateTiers });

        /// <summary>Human label for the band ("Healthy" / "Warning" / "Critical" / "No Data").</summary>
        public string OverallHealth => DailyHealthBandCalculator.Label(HealthBand);
    }

    /// <summary>
    /// The daily-summary aggregate SQL — single definition in <see cref="DailySummarySql"/>, shared with the
    /// viewer's Performance Calendar. This was a hand-copied literal described as "verbatim" with nothing
    /// enforcing it, so the calendar and this MCP tool could silently answer the same day differently (#1661).
    /// </summary>
    public const string DailySummaryRangeSql = DailySummarySql.RangeSql;

    /// <summary>The range read's rows plus the horizon they were judged against (#3541 A9).</summary>
    /// <param name="Rows">One row per day the spine holds, oldest first, each stamped with its <see cref="DailySummaryReadRow.DataState"/>.</param>
    /// <param name="RetentionHorizon">The oldest UTC day every signal source still holds — <see cref="DailySummaryRetention.HorizonFor"/>.</param>
    /// <param name="ShortestRetentionDays">The retention (days) the horizon was computed from: the shortest effective horizon among the sources.</param>
    public sealed record DailySummaryRangeReadResult(List<DailySummaryReadRow> Rows, DateTime RetentionHorizon, int ShortestRetentionDays);

    /// <summary>
    /// The collectors whose tables the daily aggregate reads as SIGNALS, by their schedule names — the
    /// sources whose retention decides the horizon (#3541 A9). The collection log and the alert log are the
    /// other two spine members; they are constants on <see cref="DarlingRetention"/> and are folded in by
    /// <see cref="ShortestSignalRetentionDays"/>. <c>query_stats</c> is included even though old windows route
    /// its CTE to a rollup with its own longer retention: the horizon is a floor over EVERY signal, and the
    /// rollup keeps only the query count, not the band's inputs.
    /// <para>#3653: the list, the fleet-override SQL and the shortest-retention arithmetic live on
    /// <see cref="DailySummaryHorizon"/> in Storage, where the viewer's Performance Calendar — which reads the
    /// same aggregate but cannot see this assembly — computes the SAME horizon. These members keep this reader's
    /// names as aliases so the tool and its tests read as before; the definitions are one.</para>
    /// </summary>
    internal static readonly string[] DailySummarySignalCollectors = DailySummaryHorizon.SignalCollectors;

    /// <summary>
    /// The FLEET-WIDE retention overrides (<c>server_id</c> NULL) for the signal collectors — the same rows
    /// <c>StoreConfigProvider.ResolveFleetRetentionDays</c> layers over <c>CollectorScheduleDefaults</c> for
    /// the purge itself, so the horizon this reader publishes is the horizon the purge actually enforces
    /// rather than the shipped default. A per-server override cannot apply to a shared-table purge, which is
    /// why only fleet rows are read. $1 the collector names. Single definition in
    /// <see cref="DailySummaryHorizon.FleetRetentionOverridesSql"/> (#3653).
    /// </summary>
    public const string FleetRetentionOverridesSql = DailySummaryHorizon.FleetRetentionOverridesSql;

    /// <summary>
    /// The shortest effective retention among the daily aggregate's sources, in days — the number the
    /// horizon is measured back from. Pure: <paramref name="fleetOverrideDays"/> is the collector →
    /// retention_days map the store holds (empty on an untouched store).
    ///
    /// <para>The signal collectors resolve through <see cref="StoreConfigProvider.ResolveFleetRetentionDays"/>
    /// so an operator-shortened or -lengthened retention moves the horizon with it, with the same floor the
    /// purge applies to the two baseline-serving raw tables (<see cref="BaselineMath.BaselineWindowDays"/> for
    /// <c>cpu_utilization</c>). The collection log and the alert log are folded in at their constants; on a
    /// default store they are the LONGER horizons (60 and 90 days), which is exactly why a spine row can
    /// outlive its signals and why the shortest one is the horizon.</para>
    /// <para>#3653: the arithmetic is <see cref="DailySummaryHorizon.ShortestSignalRetentionDays(Func{string, int}, int)"/>;
    /// this wrapper feeds it the purge's own resolver over the service's override rows and the baseline floor,
    /// which is what makes the horizon this tool publishes the horizon the purge enforces.</para>
    /// </summary>
    internal static int ShortestSignalRetentionDays(IReadOnlyList<ScheduleOverride> fleetOverrides)
        => DailySummaryHorizon.ShortestSignalRetentionDays(
            collector => StoreConfigProvider.ResolveFleetRetentionDays(collector, fleetOverrides),
            BaselineMath.BaselineWindowDays);

    private static async Task<IReadOnlyList<ScheduleOverride>> ReadFleetRetentionOverridesAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var overrides = new List<ScheduleOverride>();
        await using var command = postgres.CreateCommand(FleetRetentionOverridesSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = DailySummarySignalCollectors });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            overrides.Add(new ScheduleOverride(null, reader.GetString(0), null, reader.GetInt32(1), true, null));
        }

        return overrides;
    }

    /// <summary>One <see cref="DailySummaryReadRow"/> per collected day in the half-open [fromDate, toDate)
    /// window (the viewer's <c>GetDailySummaryRangeAsync</c>).
    ///
    /// <para>#1661: routes to the same retention tier the viewer's calendar does. This matters beyond
    /// correctness — the calendar and this MCP tool answer the same question, so if only one routed they would
    /// report different query counts for the same day and there would be no way to tell which was right.</para>
    ///
    /// <para>#3541 A9: returns the rows AND the retention horizon they were judged against — see
    /// <see cref="DailySummaryRangeReadResult"/> and the horizon note in the body.</para>
    /// </summary>
    public static async Task<DailySummaryRangeReadResult> GetDailySummaryRangeAsync(
        NpgsqlDataSource postgres, int serverId, DateTime fromDate, DateTime toDate,
        DateTime? referenceUtc = null, CancellationToken cancellationToken = default)
    {
        /* #1664: gate the age decision on the rollups actually existing — a plain-PostgreSQL store has none
           (and never drops raw, so raw is complete there). #1759: and on what they have MATERIALIZED, which is
           a separate question — a rollup created over pre-existing history answers old windows with silence.
           BOTH gates or neither: this tool and the viewer's calendar answer the same question off the same SQL,
           so routing them differently would have them report different query counts for the same day on exactly
           the affected stores, with no way to tell which was right. Probed per call, uncached: get_daily_health
           runs at human/model cadence and these are two small lookups. */
        var rollups = await TimescaleSupport.DetectRollupsAsync(postgres, cancellationToken);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(postgres, rollups, cancellationToken);
        var tier = RetentionTierRouter.Resolve(
            DateTime.UtcNow, fromDate, rollups.QueryGrainHourly, rollups.QueryGrainDaily,
            coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView));

        /* #3525: the deadlock-rate tiers the day band evaluates, read ONCE per range rather than per row —
           DarlingFleetReader's own hoist argument: a settings write mid-read must not band some days on the
           old pair and the rest on the new one. */
        var rateTiers = await ReadDeadlockRateThresholdsAsync(postgres, cancellationToken);

        /* #3541 A9: the retention horizon, from the store's effective retention and the READER's wall clock.
           The clock is deliberately NOT the caller's anchor — a purge is a wall-clock event and a backdated
           as_of cannot un-purge a table; anchoring the horizon to as_of would let "as_of 25 days ago,
           days_back 30" paint the purged stretch green again, which is the defect. The anchor still governs
           the WINDOW (fromDate/toDate above) and the still-forming day's clamp (ReferenceUtc below); the
           horizon is a property of the store. DailySummaryRetention.HorizonFor documents the date arithmetic. */
        var fleetOverrides = await ReadFleetRetentionOverridesAsync(postgres, cancellationToken);
        var shortestRetentionDays = ShortestSignalRetentionDays(fleetOverrides);
        var horizon = DailySummaryRetention.HorizonFor(DateTime.UtcNow, shortestRetentionDays);

        var results = new List<DailySummaryReadRow>();
        /* #3653 (Q12): tier over the legacy pair above; the hourly RELATION by the supply rule — the
           interval-honest successor where it reaches as far back as the legacy for this window. The viewer's
           calendar makes the identical call, so the two still count the same queries for the same day. */
        await using var command = postgres.CreateCommand(DailySummarySql.RangeSqlFor(tier, coverage.HourlyRelationFor(TimescaleSupport.QueryStatsHourlyView, fromDate)));
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddTimestamp(command, fromDate.Date);
        DarlingMcpReadParameters.AddTimestamp(command, toDate.Date);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var row = ReadDailySummaryRow(reader);
            results.Add(row with
            {
                RateTiers = rateTiers,
                ReferenceUtc = referenceUtc ?? DateTime.UtcNow,
                DataState = DailySummaryRetention.StateFor(row.SummaryDate, row.CollectionRuns, row.SignalSourcesPresent, horizon),
                RetentionHorizon = horizon,
            });
        }

        return new DailySummaryRangeReadResult(results, horizon, shortestRetentionDays);
    }

    /// <summary>The deadlock band's tiers from the store's singleton settings row (#3368, V120), or the
    /// shipped pair when the row is absent — <c>DarlingFleetReader.ReadDeadlockRateThresholdsAsync</c>'s
    /// read, off the same published SQL, for the DAY surfaces (#3525). Values come back RAW;
    /// <see cref="DeadlockRateThresholds"/> clamps on read.</summary>
    private static async Task<DeadlockRateThresholds> ReadDeadlockRateThresholdsAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(DarlingFleetReader.FleetDeadlockRateThresholdSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return new DeadlockRateThresholds(reader.GetDouble(0), reader.GetDouble(1));
        }

        return DeadlockRateThresholds.Default;
    }

    /// <summary>
    /// The daily-summary signals for one server over an EXACT half-open window — the fleet sweep's
    /// per-server read (#3466 lane 2), which is <see cref="GetDailySummaryRangeAsync"/> minus two
    /// choices that are the calendar's contract rather than the SQL's: the <c>.Date</c> truncation
    /// (a sweep span is sub-day and starts at the previous sweep's instant, not midnight) and the
    /// per-call rollup probe (a sweep span ends at "now" and is capped at one day by
    /// <c>FleetSweepCadence.IntervalMinutesCeiling</c>, so it always sits inside the 4-day raw window
    /// and the raw tier is correct by construction rather than by routing).
    ///
    /// <para>The statement is <see cref="DailySummarySql.RangeSql"/> itself — the ONE aggregate the
    /// calendar, <c>get_daily_summary</c> and now the sweep all band from, so the sweep's verdicts and
    /// the day surfaces cannot disagree about the same signals. The SQL buckets by UTC day, so a span
    /// crossing midnight returns one row per day touched; the caller sums the rows, which is exact
    /// because every COUNT is additive over the same half-open window — and takes the MAX of the one
    /// magnitude, the peak block wait (#3539 A2), which is exact for the same reason.</para>
    ///
    /// <para><b>Throws on a store fault, deliberately</b> — the engine-read posture
    /// (<c>FleetSweepStore.GetLatestSweepAsync</c>'s reasoning): the sweep's caller must render a
    /// failed read as a dead instrument, never as a quiet server, and only a raised fault lets it
    /// tell those apart.</para>
    /// </summary>
    public static async Task<List<DailySummaryReadRow>> GetWindowSignalsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime fromUtc, DateTime toUtc, CancellationToken cancellationToken = default)
    {
        var results = new List<DailySummaryReadRow>();
        await using var command = postgres.CreateCommand(DailySummarySql.RangeSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddTimestamp(command, fromUtc);
        DarlingMcpReadParameters.AddTimestamp(command, toUtc);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(ReadDailySummaryRow(reader));
        }

        return results;
    }

    /// <summary>Daily summary for one server on a specific date (or today, UTC, when <paramref name="summaryDate"/>
    /// is null) — the viewer's <c>GetDailySummaryAsync</c>. Returns a No-Data row when the day had no collection.</summary>
    public static async Task<DailySummaryReadRow> GetDailySummaryAsync(
        NpgsqlDataSource postgres, int serverId, DateTime? summaryDate = null, CancellationToken cancellationToken = default)
    {
        var targetDate = summaryDate?.Date ?? DateTime.UtcNow.Date;
        var range = await GetDailySummaryRangeAsync(postgres, serverId, targetDate, targetDate.AddDays(1), cancellationToken: cancellationToken);
        return range.Rows.Count > 0
            ? range.Rows[0]
            : new DailySummaryReadRow(targetDate, 0m, "", 0, 0, 0, 0, 0, 0, 0, 0, 0, HasData: false)
            {
                /* A day the spine does not hold at all is not "collected" either: before the horizon it is
                   purged like any other, inside it simply without a run record — so the single-day tool can say which. */
                DataState = DailySummaryRetention.StateFor(targetDate, 0, 0, range.RetentionHorizon),
                RetentionHorizon = range.RetentionHorizon,
            };
    }

    private static DailySummaryReadRow ReadDailySummaryRow(DbDataReader reader) => new(
        reader.IsDBNull(0) ? DateTime.MinValue : Convert.ToDateTime(reader.GetValue(0)),
        reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
        reader.IsDBNull(2) ? "" : reader.GetString(2),
        reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3)),
        reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
        reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
        reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6)),
        reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7)),
        reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
        reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)),
        reader.IsDBNull(10) ? 0L : Convert.ToInt64(reader.GetValue(10)),
        reader.IsDBNull(11) ? 0L : Convert.ToInt64(reader.GetValue(11)),
        HasData: true)
    {
        /* #3539 A2: the trailing collection_runs column, appended after peak_block_wait_ms so the eleven
           positional reads above stay where they were. */
        CollectionRuns = reader.IsDBNull(12) ? 0L : Convert.ToInt64(reader.GetValue(12)),
        /* #3541 A9: the signal-presence count, after collection_runs. */
        SignalSourcesPresent = reader.IsDBNull(13) ? 0 : Convert.ToInt32(reader.GetValue(13)),
    };
}
