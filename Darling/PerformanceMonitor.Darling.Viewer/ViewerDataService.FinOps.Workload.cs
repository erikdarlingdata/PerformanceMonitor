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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// FinOps Database Resources / Application Connections / Optimization (wait + expensive) / High Impact
/// reads — Lite's <c>LocalDataService.FinOps.Workload.cs</c> ported to Postgres. The DuckDB SQL ports
/// near-verbatim: positional params, <c>FULL JOIN</c>, <c>NULLIF</c>/<c>COALESCE</c>, <c>ILIKE</c>, the
/// repeated wait-category CASE, and the correlated sample-text subqueries all run identically on PG. The
/// high-impact query reads the <c>query_stats</c> base table (as Lite does) for its correlated subqueries;
/// every other read uses the <c>v_*</c> passthrough views. SQL kept in <c>public const</c> so tests pin it.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>Per-database resource usage from query_stats + file_io_stats deltas. $1 server_id, $2 cutoff. The SQL and the
    /// retention-tier routing live in <see cref="DarlingFinOpsDatabaseResourcesReader"/>.</summary>
    public const string DatabaseResourceUsageSql = DarlingFinOpsDatabaseResourcesReader.DatabaseResourceUsageSql;

    /// <inheritdoc cref="DarlingFinOpsDatabaseResourcesReader.DatabaseResourceUsageSqlFor(RetentionTier)"/>
    public static string DatabaseResourceUsageSqlFor(RetentionTier tier) =>
        DarlingFinOpsDatabaseResourcesReader.DatabaseResourceUsageSqlFor(tier);

    /// <inheritdoc cref="DarlingFinOpsDatabaseResourcesReader.DatabaseResourceUsageSqlFor(RetentionTier, RollupCoverage, DateTime)"/>
    public static string DatabaseResourceUsageSqlFor(RetentionTier tier, RollupCoverage coverage, DateTime windowStartUtc) =>
        DarlingFinOpsDatabaseResourcesReader.DatabaseResourceUsageSqlFor(tier, coverage, windowStartUtc);

    /// <inheritdoc cref="DarlingFinOpsDatabaseResourcesReader.TopResourceConsumersSqlFor(RetentionTier)"/>
    public static string TopResourceConsumersSqlFor(RetentionTier tier) =>
        DarlingFinOpsDatabaseResourcesReader.TopResourceConsumersSqlFor(tier);

    /// <inheritdoc cref="DarlingFinOpsDatabaseResourcesReader.TopResourceConsumersSqlFor(RetentionTier, RollupCoverage, DateTime)"/>
    public static string TopResourceConsumersSqlFor(RetentionTier tier, RollupCoverage coverage, DateTime windowStartUtc) =>
        DarlingFinOpsDatabaseResourcesReader.TopResourceConsumersSqlFor(tier, coverage, windowStartUtc);

    /* The window start is taken BEFORE the rollup probe; the reader resolves the tier AFTER it (its own clock read), as before the move. */
    public async Task<List<DatabaseResourceUsageRow>> GetDatabaseResourceUsageAsync(int serverId, int hoursBack = 24, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);
        var (rollups, coverage) = await GetRollupAvailabilityAsync(cancellationToken);
        var items = await DarlingFinOpsDatabaseResourcesReader.GetDatabaseResourceUsageAsync(
            _dataSource, serverId, rollups, coverage, cutoff, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        return items.ConvertAll(DatabaseResourceUsageRow.From);
    }

    /// <summary>
    /// Per-application connection counts plus the collected per-app resource + session-status metrics from
    /// session_stats (last 24h). AVG/MAX over the window for connection/running/sleeping/dormant counts and
    /// CPU/reads/writes/logical-reads (the resource columns are nullable, so AVG/MAX yield NULL until populated).
    /// The SQL lives in <see cref="DarlingFinOpsApplicationConnectionsReader"/>. $1 server_id, $2 cutoff.
    /// </summary>
    public const string ApplicationConnectionsSql = DarlingFinOpsApplicationConnectionsReader.ApplicationConnectionsSql;

    public async Task<List<ApplicationConnectionRow>> GetApplicationConnectionsAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddHours(-24);

        /* #4766: the rows read their times on THIS server's clock (its collected one, else the viewer machine's offset,
           the rule every list row uses), read once per load, and not on the active server tab's: the FinOps tab lists the
           server it was opened for. */
        var clock = ViewerTimeHelper.ClockForServerOrMachine(
            await GetServerClocksAsync(serverId, cancellationToken), serverId, TimeZoneInfo.Local, DateTime.UtcNow);

        var items = await DarlingFinOpsApplicationConnectionsReader.GetApplicationConnectionsAsync(
            _dataSource, serverId, cutoff, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        return items.ConvertAll(d => ApplicationConnectionRow.From(d, clock));
    }

    /// <summary>
    /// Top databases by total CPU AND by average CPU per execution for the Utilization summary — one pass feeding both
    /// grids (#4227). The SQL lives in <see cref="DarlingFinOpsDatabaseResourcesReader"/>. $1 server_id, $2 cutoff.
    /// </summary>
    public const string TopResourceConsumersSql = DarlingFinOpsDatabaseResourcesReader.TopResourceConsumersSql;

    /// <summary>
    /// Both top-consumer grids from the one <see cref="TopResourceConsumersSql"/> round trip, ranked and sliced to
    /// <paramref name="topN"/> by <see cref="DarlingFinOpsDatabaseResourcesReader.GetTopResourceConsumersAsync"/>.
    /// </summary>
    public async Task<(List<TopResourceConsumerRow> ByTotal, List<TopResourceConsumerRow> ByAvg)> GetTopResourceConsumersAsync(int serverId, int hoursBack = 24, int topN = 5, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);
        var (rollups, coverage) = await GetRollupAvailabilityAsync(cancellationToken);
        var (byTotal, byAvg) = await DarlingFinOpsDatabaseResourcesReader.GetTopResourceConsumersAsync(
            _dataSource, serverId, rollups, coverage, cutoff, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, topN, cancellationToken);
        return (byTotal.Select(TopResourceConsumerRow.From).ToList(), byAvg.Select(TopResourceConsumerRow.From).ToList());
    }

    /// <summary>The wait-category summary's SQL; lives in <see cref="DarlingFinOpsOptimizationReader"/>.</summary>
    public const string WaitCategorySummarySql = DarlingFinOpsOptimizationReader.WaitCategorySummarySql;

    public async Task<List<WaitCategorySummaryRow>> GetWaitCategorySummaryAsync(int serverId, int hoursBack = 24, CancellationToken cancellationToken = default)
    {
        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);

        var rows = await DarlingFinOpsOptimizationReader.GetWaitCategorySummaryAsync(
            _dataSource, serverId, cutoff, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        return rows.Select(WaitCategorySummaryRow.From).ToList();
    }

    /// <summary>The expensive-queries read's SQL; lives in <see cref="DarlingFinOpsOptimizationReader"/>.</summary>
    public const string ExpensiveQueriesSql = DarlingFinOpsOptimizationReader.ExpensiveQueriesSql;

    public async Task<List<ExpensiveQueryRow>> GetExpensiveQueriesAsync(int serverId, int hoursBack = 24, int topN = 20, CancellationToken cancellationToken = default)
    {
        /* #1661: this reader projects query_text and query_plan_xml, and NO rollup carries per-row text — the
           CAGGs group by identity and sum deltas. So unlike the aggregate FinOps queries this one cannot be
           routed; it is limited to whatever raw still retains, however wide a window the picker offers. Clamp to
           that horizon so the query states the range it can actually answer. The UI labels the clamp
           (FinOpsTab.Loaders) using the same router, rather than quietly showing a few days of a month. */
        var now = DateTime.UtcNow;
        var (cutoff, _) = RetentionTierRouter.ClampToTextHorizon(now, now.AddHours(-hoursBack));

        var rows = await DarlingFinOpsOptimizationReader.GetExpensiveQueriesAsync(
            _dataSource, serverId, cutoff, topN, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        return rows.Select(ExpensiveQueryRow.From).ToList();
    }

    /// <summary>The high-impact read's SQL; lives in <see cref="DarlingFinOpsHighImpactReader"/>.</summary>
    public const string HighImpactQueriesSql = DarlingFinOpsHighImpactReader.HighImpactQueriesSql;

    public async Task<List<HighImpactQueryRow>> GetHighImpactQueriesAsync(int serverId, int hoursBack = 24, CancellationToken cancellationToken = default)
    {
        var rows = await DarlingFinOpsHighImpactReader.ReadAsync(
            _dataSource, serverId, hoursBack, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        return rows.Select(HighImpactQueryRow.From).ToList();
    }
}
