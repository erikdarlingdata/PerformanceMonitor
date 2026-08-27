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
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The plan-cache + CPU-scheduler snapshot MCP tools — get_plan_cache_bloat, get_cpu_scheduler_pressure —
/// served over Darling's Postgres store. Each tool body mirrors the Dashboard's <c>McpDiagnosticTools</c> /
/// <c>McpSchedulerTools</c> field-for-field (Lite exposes neither, so the Dashboard is the only reference).
/// Reads flow through <see cref="DarlingPlanCacheSchedulerReader"/> — STORED reads of the latest snapshot, no
/// live monitored-server hit. The Dashboard's <c>bloat_level</c> (#1410) and <c>pressure_level</c> /
/// <c>recommendation</c> (#1410) classifications are reproduced from the reporting-view CASE logic.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPlanCacheSchedulerTools
{
    [McpServerTool(Name = "get_plan_cache_bloat"), Description("Gets plan cache composition showing single-use vs multi-use plans, with a bloat-level classification. High single-use plan counts indicate ad-hoc query bloat consuming buffer pool memory. Consider enabling 'optimize for ad hoc workloads'.")]
    public static async Task<string> GetPlanCacheBloat(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to analyze. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var rows = await DarlingPlanCacheSchedulerReader.GetPlanCacheBloatAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "plan_cache_stats")
                    ?? McpHelpers.Status("unavailable", "No plan cache statistics available in the requested time range.");

            var totalPlans = rows.Sum(r => (long)r.TotalPlans);
            var totalSingleUse = rows.Sum(r => (long)r.SingleUsePlans);
            var totalSizeMb = rows.Sum(r => (long)r.TotalSizeMb);
            var singleUseSizeMb = rows.Sum(r => (long)r.SingleUseSizeMb);
            var (bloatLevel, bloatRecommendation) = DarlingPlanCacheSchedulerReader.ClassifyPlanCacheBloat(totalPlans, totalSingleUse);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                collection_time = rows[0].CollectionTime.ToString("o"),
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
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_plan_cache_bloat", ex);
        }
    }

    [McpServerTool(Name = "get_cpu_scheduler_pressure"), Description("Gets CPU scheduler pressure: runnable task queue depth, worker thread utilization, and pressure warnings. Shows whether the server has enough worker threads and if tasks are queuing for CPU time.")]
    public static async Task<string> GetCpuSchedulerPressure(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var item = await DarlingPlanCacheSchedulerReader.GetCpuSchedulerPressureAsync(postgres, resolved.ServerId);
            if (item == null)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "cpu_scheduler_stats")
                    ?? McpHelpers.Status("unavailable", "No CPU scheduler data available. The scheduler collector may not have run yet.");

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
                collection_time = item.CollectionTime.ToString("o"),
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
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_cpu_scheduler_pressure", ex);
        }
    }
}
