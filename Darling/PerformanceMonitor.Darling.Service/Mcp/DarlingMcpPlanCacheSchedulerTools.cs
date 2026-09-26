/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The plan-cache + CPU-scheduler snapshot MCP tools — get_plan_cache_bloat, get_cpu_scheduler_pressure —
/// served over Darling's Postgres store. Each tool body mirrors the Dashboard's <c>McpDiagnosticTools</c> /
/// <c>McpSchedulerTools</c> field-for-field; Lite has since ported both names (<c>Lite/Mcp/McpPlanCacheSchedulerTools</c>),
/// and the two SKUs now share one parameter contract (below).
/// Reads flow through <see cref="DarlingPlanCacheSchedulerReader"/> — STORED reads of the latest snapshot, no
/// live monitored-server hit. The Dashboard's <c>bloat_level</c> (#1410) and <c>pressure_level</c> /
/// <c>recommendation</c> (#1410) classifications are reproduced from the reporting-view CASE logic.
///
/// <para><b>#3541 A10 — one contract on both SKUs, and the snapshot says when.</b> Both tools take
/// <c>(server_name, hours_back, as_of)</c> with the SAME parameter descriptions as Lite's
/// <c>McpPlanCacheSchedulerTools</c>: <c>hours_back</c> is the span SEARCHED for the newest snapshot, not a
/// span aggregated, and the description says so in those words. get_cpu_scheduler_pressure used to take
/// <c>server_name</c> alone here and <c>(server_name, hours_back, as_of)</c> on Lite — the same tool name
/// with two parameter surfaces, on the one tool whose answer is a CRITICAL/HIGH/MEDIUM/NORMAL verdict. Every
/// payload publishes <c>captured_at</c> (the snapshot's own <c>collection_time</c>) and <c>age_seconds</c>
/// against the window's end, so a verdict computed from a stale row cannot pass as current.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPlanCacheSchedulerTools
{
    /// <summary>
    /// get_cpu_scheduler_pressure's description, VERBATIM the text Lite's twin carries (#3541 A10): the same
    /// tool name described two ways on two servers was half of the drift this lane closed, and a shared const
    /// cannot be shared across the two assemblies, so the cross-SKU description census pins the two strings
    /// equal instead. Change one, change both.
    /// </summary>
    internal const string CpuSchedulerPressureDescription =
        "Gets CPU scheduler pressure from the latest snapshot: runnable task queue depth, worker thread utilization, queued/blocked requests, the collector's pressure warning flags, and the banded pressure_level verdict with its recommendation. Shows whether the server has enough worker threads and if tasks are queuing for CPU time. LATEST IS A TIME: this is the newest scheduler snapshot found within hours_back of as_of, not an aggregate over those hours - captured_at is the instant it was collected and age_seconds its distance from the window's end; the verdict is that instant's, so read age_seconds before reading pressure_level as current.";

    [McpServerTool(Name = "get_plan_cache_bloat"), Description("Gets plan cache composition showing single-use vs multi-use plans, with a bloat-level classification. High single-use plan counts indicate ad-hoc query bloat consuming buffer pool memory. Consider enabling 'optimize for ad hoc workloads'. LATEST IS A TIME: this is the newest plan-cache snapshot found within hours_back of as_of, not an aggregate over those hours - captured_at is the instant the snapshot was collected and age_seconds its distance from the window's end.")]
    public static async Task<string> GetPlanCacheBloat(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to search for the latest snapshot. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var rows = await DarlingPlanCacheSchedulerReader.GetPlanCacheBloatAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, cancellationToken);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "plan_cache_stats", cancellationToken)
                    ?? McpHelpers.Status("unavailable", "No plan cache statistics available in the requested time range.");

            var totalPlans = rows.Sum(r => (long)r.TotalPlans);
            var totalSingleUse = rows.Sum(r => (long)r.SingleUsePlans);
            var totalSizeMb = rows.Sum(r => (long)r.TotalSizeMb);
            var singleUseSizeMb = rows.Sum(r => (long)r.SingleUseSizeMb);
            var (bloatLevel, bloatRecommendation) = DarlingPlanCacheSchedulerReader.ClassifyPlanCacheBloat(totalPlans, totalSingleUse);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                captured_at = rows[0].CollectionTime.ToString("o"),
                age_seconds = LatestSnapshotStamp.AgeSeconds(rows[0].CollectionTime, now),
                summary = new
                {
                    total_plans = totalPlans,
                    single_use_plans = totalSingleUse,
                    single_use_percent = totalPlans > 0 ? Math.Round(100.0 * totalSingleUse / totalPlans, 1) : 0,
                    total_size_mb = totalSizeMb,
                    single_use_size_mb = singleUseSizeMb,
                    wasted_percent = totalSizeMb > 0 ? Math.Round(100.0 * singleUseSizeMb / totalSizeMb, 1) : 0,
                    bloat_level = bloatLevel,
                    bloat_recommendation = bloatRecommendation
                },
                cache_types = rows.Select(r => new
                {
                    cache_type = r.CacheObjType,
                    object_type = r.ObjType,
                    total_plans = r.TotalPlans,
                    total_size_mb = r.TotalSizeMb,
                    single_use_plans = r.SingleUsePlans,
                    single_use_size_mb = r.SingleUseSizeMb,
                    multi_use_plans = r.MultiUsePlans,
                    multi_use_size_mb = r.MultiUseSizeMb,
                    avg_use_count = r.AvgUseCount
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_plan_cache_bloat", ex);
        }
    }

    [McpServerTool(Name = "get_cpu_scheduler_pressure"), Description(CpuSchedulerPressureDescription)]
    public static async Task<string> GetCpuSchedulerPressure(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to search for the latest snapshot. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var item = await DarlingPlanCacheSchedulerReader.GetCpuSchedulerPressureAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, cancellationToken);
            if (item == null)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "cpu_scheduler_stats", cancellationToken)
                    ?? McpHelpers.Status("unavailable", "No CPU scheduler snapshot in the requested time range. The scheduler collector may not have run yet, or its newest snapshot is older than hours_back.");

            var workerUtilizationPercent = item.MaxWorkersCount > 0
                ? Math.Round(item.TotalCurrentWorkersCount * 100.0 / item.MaxWorkersCount, 2)
                : 0;
            var (pressureLevel, recommendation) = DarlingPlanCacheSchedulerReader.ClassifyCpuSchedulerPressure(
                item.TotalRunnableTasksCount, item.TotalCurrentWorkersCount, item.MaxWorkersCount,
                item.TotalQueuedRequestCount, item.WorkerThreadExhaustionWarning, item.RunnableTasksWarning,
                item.QueuedRequestsWarning);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                captured_at = item.CollectionTime.ToString("o"),
                age_seconds = LatestSnapshotStamp.AgeSeconds(item.CollectionTime, now),
                schedulers = item.SchedulerCount,
                runnable_tasks = item.TotalRunnableTasksCount,
                avg_runnable_per_scheduler = item.AvgRunnableTasksCount,
                workers = item.TotalCurrentWorkersCount,
                max_workers = item.MaxWorkersCount,
                worker_utilization_percent = workerUtilizationPercent,
                runnable_percent = item.RunnablePercent,
                queued_requests = item.TotalQueuedRequestCount,
                active_requests = item.TotalActiveRequestCount,
                pressure_level = pressureLevel,
                recommendation,
                warnings = new
                {
                    worker_thread_exhaustion = item.WorkerThreadExhaustionWarning,
                    runnable_tasks = item.RunnableTasksWarning,
                    blocked_tasks = item.BlockedTasksWarning,
                    queued_requests = item.QueuedRequestsWarning,
                    physical_memory_pressure = item.PhysicalMemoryPressureWarning
                }
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_cpu_scheduler_pressure", ex);
        }
    }
}
