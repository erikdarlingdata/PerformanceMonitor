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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;

namespace PerformanceMonitor.Darling.Service.Mcp;

public sealed partial class DarlingMcpFinOpsTools
{
    internal const string OptimizationView = "optimization";

    internal const string OptimizationViewLine =
        "optimization: idle databases, tempdb, waits, costly queries, grants.";

    internal const string OptimizationViewGuide =
        "optimization answers five sections, each with its own status (ok, empty or not_collected, with a message when not collected) and its own window, beside server, hours_back, monthly_cost_usd and cost_reason. monthly_cost_usd is the server's registered monthly cost, null with cost_reason 'monthly cost not set' when none is set or it is 0 or less; every est_cost_usd is then null. idle_databases: databases with no query executions in a fixed 7 days (window_days 7; hours_back does not move it), from the latest size snapshot, system databases left out; rows are database_name, total_size_mb, file_count and last_execution_server_local, ordered by total_size_mb descending then database_name; up to 500 rows with database_count and truncated. last_execution_server_local is the monitored server's own clock, not UTC, printed yyyy-MM-ddTHH:mm:ss with no Z, and null when none was recorded. tempdb_pressure: a fixed 24 hours (window_hours 24); rows are metric, current_mb (latest sample), peak_24h_mb and warning, the service's own band text ('' when none), in the fixed order User Objects, Internal Objects, Version Store, Total Reserved. wait_categories: wait time by category over hours_back; rows are category, total_wait_time_ms, waiting_tasks, pct_of_total (as stored, one decimal), top_wait_type, top_wait_time_ms and est_cost_usd, ordered by total_wait_time_ms descending then category. expensive_queries: the statements with the most CPU over hours_back, clamped to the 3 days of retained query text, with effective_start the UTC start actually read (Z); limit is the top-N here (1-50, default 10; the desktop shows 20) and the only section limit applies to. Rows are database_name, total_cpu_ms, avg_cpu_ms_per_exec, total_reads, avg_reads_per_exec, executions, query_preview (first 200 characters, no full text), has_plan (no plan XML; get_plan_xml returns a stored plan by query_hash) and est_cost_usd, ordered by total_cpu_ms descending then database_name then query_preview; which of several statements tied at the top-N cut is returned is the engine's choice, since the SQL orders by CPU only. memory_grant_efficiency: a fixed 24 hours (window_hours 24, the desktop's window); one row per UTC day (day yyyy-MM-dd) with avg_granted_mb, avg_used_mb, efficiency_pct, peak_granted_mb, wasted_mb (average granted minus average used, as the desktop), total_grantees, total_waiters, timeout_errors and forced_grants, ordered by day ascending. est_cost_usd is the section's share of the window's monthly budget (monthly cost x hours_back / 730) by wait time or CPU among the rows returned, so for expensive_queries it depends on limit; rounded to 2 places, null when the cost is unset or the rows total 0. The whole answer is status not_collected only when every section is gated for the server's engine, and status empty when no section has rows. Times are UTC and end in Z except last_execution_server_local.";

    /// <summary>The fixed ceiling on the idle-database list.</summary>
    internal const int MaxIdleDatabaseRows = 500;

    private const int IdleDatabaseWindowDays = 7;
    private const int TempdbWindowHours = 24;
    private const int MemoryGrantWindowHours = 24;

    /// <summary>The idle-database ordering: size descending, then name, so the order is total and a cut never depends on SQL order.</summary>
    internal static List<IdleDatabase> OrderIdleDatabases(IEnumerable<IdleDatabase> rows) =>
        rows.OrderByDescending(r => r.TotalSizeMb)
            .ThenBy(r => r.DatabaseName, StringComparer.Ordinal)
            .ToList();

    /// <summary>The wait-category ordering: total wait time descending, then category.</summary>
    internal static List<WaitCategorySummary> OrderWaitCategories(IEnumerable<WaitCategorySummary> rows) =>
        rows.OrderByDescending(r => r.TotalWaitTimeMs)
            .ThenBy(r => r.Category, StringComparer.Ordinal)
            .ToList();

    /// <summary>The expensive-query ordering: CPU descending, then database, then preview.</summary>
    internal static List<ExpensiveQuery> OrderExpensiveQueries(IEnumerable<ExpensiveQuery> rows) =>
        rows.OrderByDescending(r => r.TotalCpuMs)
            .ThenBy(r => r.DatabaseName, StringComparer.Ordinal)
            .ThenBy(r => r.QueryPreview, StringComparer.Ordinal)
            .ToList();

    /// <summary>
    /// One row's estimated cost, exactly the desktop's: its part of the rows' total times the window budget
    /// (<see cref="FinOpsCost.Share"/> of <see cref="FinOpsCost.WindowBudget"/>), to 2 places away from zero.
    /// Null when no monthly cost is set or the rows total 0.
    /// </summary>
    internal static decimal? OptimizationCostShare(long part, long total, decimal monthly, int hoursBack)
    {
        if (monthly <= 0m || total <= 0) return null;
        return Math.Round(FinOpsCost.Share(part, total, FinOpsCost.WindowBudget(monthly, hoursBack)), 2, MidpointRounding.AwayFromZero);
    }

    internal static object IdleDatabaseRow(IdleDatabase r) => new
    {
        database_name = r.DatabaseName,
        total_size_mb = r.TotalSizeMb,
        file_count = r.FileCount,
        /* The monitored server's own clock, read verbatim: no Z, no shifting. */
        last_execution_server_local = r.LastExecutionTime?.ToString("yyyy-MM-dd'T'HH:mm:ss", CultureInfo.InvariantCulture),
    };

    internal static object TempdbRow(TempdbSummaryMetric r) => new
    {
        metric = r.Metric,
        current_mb = r.CurrentMb,
        peak_24h_mb = r.Peak24hMb,
        warning = r.Warning,
    };

    internal static List<object> WaitCategoryRows(IEnumerable<WaitCategorySummary> rows, decimal monthly, int hoursBack)
    {
        var ordered = OrderWaitCategories(rows);
        var total = ordered.Sum(r => r.TotalWaitTimeMs);
        return ordered.Select(r => (object)new
        {
            category = r.Category,
            total_wait_time_ms = r.TotalWaitTimeMs,
            waiting_tasks = r.WaitingTasks,
            pct_of_total = r.PctOfTotal,
            top_wait_type = r.TopWaitType,
            top_wait_time_ms = r.TopWaitTimeMs,
            est_cost_usd = OptimizationCostShare(r.TotalWaitTimeMs, total, monthly, hoursBack),
        }).ToList();
    }

    internal static List<object> ExpensiveQueryRows(IEnumerable<ExpensiveQuery> rows, decimal monthly, int hoursBack)
    {
        var ordered = OrderExpensiveQueries(rows);
        var total = ordered.Sum(r => r.TotalCpuMs);
        return ordered.Select(r => (object)new
        {
            database_name = r.DatabaseName,
            total_cpu_ms = r.TotalCpuMs,
            avg_cpu_ms_per_exec = r.AvgCpuMsPerExec,
            total_reads = r.TotalReads,
            avg_reads_per_exec = r.AvgReadsPerExec,
            executions = r.Executions,
            query_preview = r.QueryPreview,
            has_plan = r.QueryPlanXml != null,
            est_cost_usd = OptimizationCostShare(r.TotalCpuMs, total, monthly, hoursBack),
        }).ToList();
    }

    internal static List<object> MemoryGrantRows(IEnumerable<MemoryGrantEfficiencyDto> rows) =>
        rows.OrderBy(r => r.Day).Select(r => (object)new
        {
            day = r.Day.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            avg_granted_mb = r.AvgGrantedMb,
            avg_used_mb = r.AvgUsedMb,
            efficiency_pct = r.EfficiencyPct,
            peak_granted_mb = r.PeakGrantedMb,
            wasted_mb = r.AvgGrantedMb - r.AvgUsedMb,
            total_grantees = r.TotalGrantees,
            total_waiters = r.TotalWaiters,
            timeout_errors = r.TimeoutErrors,
            forced_grants = r.ForcedGrants,
        }).ToList();

    /// <summary>The message inside a not-collected status envelope.</summary>
    private static string? NotCollectedMessage(string status)
    {
        using var doc = JsonDocument.Parse(status);
        return doc.RootElement.GetProperty("message").GetString();
    }

    private static async Task<string> ReadOptimizationAsync(
        NpgsqlDataSource postgres, (int ServerId, string ServerName) resolved, int hoursBack, int limit, CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        var (textStart, _) = RetentionTierRouter.ClampToTextHorizon(now, now.AddHours(-hoursBack));
        var timeout = McpCommandDeadlines.ReadSeconds;

        var idle = await DarlingFinOpsOptimizationReader.GetIdleDatabasesAsync(postgres, resolved.ServerId, now.AddDays(-IdleDatabaseWindowDays), timeout, ct);
        var tempdb = await DarlingFinOpsOptimizationReader.GetTempdbSummaryAsync(postgres, resolved.ServerId, now.AddHours(-TempdbWindowHours), timeout, ct);
        var waits = await DarlingFinOpsOptimizationReader.GetWaitCategorySummaryAsync(postgres, resolved.ServerId, now.AddHours(-hoursBack), timeout, ct);
        var queries = await DarlingFinOpsOptimizationReader.GetExpensiveQueriesAsync(postgres, resolved.ServerId, textStart, limit, timeout, ct);
        var grants = await DarlingFinOpsUtilizationReader.GetMemoryGrantEfficiencyAsync(postgres, resolved.ServerId, MemoryGrantWindowHours, timeout, ct);

        /* A gated section is asked only when it came back empty, as every capability check is. */
        var idleGate = idle.Count == 0 ? await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "database_size_stats", ct) : null;
        var tempdbGate = tempdb.Count == 0 ? await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "tempdb_stats", ct) : null;
        var waitGate = waits.Count == 0 ? await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "wait_stats", ct) : null;
        var queryGate = queries.Count == 0 ? await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_stats", ct) : null;
        var grantGate = grants.Count == 0 ? await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "memory_grant_stats", ct) : null;

        var gates = new[] { idleGate, tempdbGate, waitGate, queryGate, grantGate };
        if (gates.All(g => g != null))
            return idleGate!;
        var counts = new[] { idle.Count, tempdb.Count, waits.Count, queries.Count, grants.Count };
        if (counts.All(c => c == 0))
            return McpHelpers.Status("empty",
                "No idle-database, tempdb, wait, query or memory-grant data was found for this server in the windows read, so there is no optimization data to show.");

        var monthly = await FinOpsUtilizationFigures.GetMonthlyCostUsdAsync(postgres, resolved.ServerId, timeout, ct);
        var hasCost = monthly > 0m;

        var idleOrdered = OrderIdleDatabases(idle);
        return JsonSerializer.Serialize(new
        {
            server = resolved.ServerName,
            view = OptimizationView,
            hours_back = hoursBack,
            monthly_cost_usd = hasCost ? monthly : (decimal?)null,
            cost_reason = hasCost ? null : "monthly cost not set",
            idle_databases = new
            {
                status = SectionStatus(idleGate, idle.Count),
                message = idleGate == null ? null : NotCollectedMessage(idleGate),
                window_days = IdleDatabaseWindowDays,
                database_count = idleOrdered.Count,
                truncated = idleOrdered.Count > MaxIdleDatabaseRows,
                rows = idleOrdered.Take(MaxIdleDatabaseRows).Select(IdleDatabaseRow).ToList(),
            },
            tempdb_pressure = new
            {
                status = SectionStatus(tempdbGate, tempdb.Count),
                message = tempdbGate == null ? null : NotCollectedMessage(tempdbGate),
                window_hours = TempdbWindowHours,
                rows = tempdb.Select(TempdbRow).ToList(),
            },
            wait_categories = new
            {
                status = SectionStatus(waitGate, waits.Count),
                message = waitGate == null ? null : NotCollectedMessage(waitGate),
                window_hours = hoursBack,
                rows = WaitCategoryRows(waits, monthly, hoursBack),
            },
            expensive_queries = new
            {
                status = SectionStatus(queryGate, queries.Count),
                message = queryGate == null ? null : NotCollectedMessage(queryGate),
                window_hours = hoursBack,
                effective_start = McpHelpers.FormatEffectiveStart(textStart),
                rows = ExpensiveQueryRows(queries, monthly, hoursBack),
            },
            memory_grant_efficiency = new
            {
                status = SectionStatus(grantGate, grants.Count),
                message = grantGate == null ? null : NotCollectedMessage(grantGate),
                window_hours = MemoryGrantWindowHours,
                rows = MemoryGrantRows(grants),
            },
        }, McpHelpers.JsonOptions);
    }

    private static string SectionStatus(string? gate, int rowCount) =>
        gate != null ? "not_collected" : rowCount == 0 ? "empty" : "ok";
}
