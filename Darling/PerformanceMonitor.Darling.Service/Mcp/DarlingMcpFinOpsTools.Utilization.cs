/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage.FinOps;

namespace PerformanceMonitor.Darling.Service.Mcp;

public sealed partial class DarlingMcpFinOpsTools
{
    internal const string UtilizationView = "utilization";

    internal const string UtilizationViewLine =
        "utilization: fixed 24h verdict, health score, cost, 7-day trend.";

    internal const string UtilizationViewGuide =
        "utilization reads a fixed 24-hour window and a fixed 7-day trend; hours_back other than 24 and limit other than 10 are refused. verdict is RIGHT_SIZED, OVER_PROVISIONED, UNDER_PROVISIONED or NOT_APPLICABLE, and null when the window holds no CPU sample. health_score is 0-100: CPU 40%, memory 30%, storage 30% (memory and storage 30:30 when there is no CPU sample, with health_score_note saying so); health_band is good at 80 and above, fair at 60 and above, else poor. stolen_memory_pct is (total server memory - buffer pool) / total server memory; buffer_pool_pct is buffer pool / physical memory. free_space_pct is the latest database-size snapshot's free share, 100 when none exists. monthly_cost_usd comes from the server's registered monthly cost and annual_cost_usd is 12 times it; both are null when no cost is set. On engine edition 5 memory_basis is memory_limit and cpu_count is the vCore count (null when the service objective names none). Text numbers use the invariant culture. Times are UTC.";

    private static async Task<string> ReadUtilizationAsync(
        NpgsqlDataSource postgres, (int ServerId, string ServerName) resolved, int hoursBack, int limit, CancellationToken ct)
    {
        if (hoursBack != 24)
            return McpHelpers.Refusal("hours_back",
                $"Invalid hours_back value '{hoursBack}': view utilization reads a fixed 24-hour window and a 7-day trend; hours_back does not apply. Omit it or pass 24.");
        if (limit != DefaultLimit)
            return McpHelpers.Refusal("limit",
                $"Invalid limit value '{limit}': view utilization reads a fixed 24-hour window and a 7-day trend; limit does not apply. Omit it or pass {DefaultLimit}.");

        var dto = await DarlingFinOpsUtilizationReader.GetUtilizationEfficiencyAsync(
            postgres, resolved.ServerId, McpCommandDeadlines.ReadSeconds, ct);
        if (dto == null)
        {
            return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "memory_stats", ct)
                ?? McpHelpers.Status("empty",
                    "No memory statistics were collected for this server in the last 24 hours, so there is no utilization summary.");
        }

        var trend = await DarlingFinOpsUtilizationReader.GetProvisioningTrendAsync(
            postgres, resolved.ServerId, McpCommandDeadlines.ReadSeconds, ct);
        var totals = await FinOpsUtilizationFigures.GetLatestStorageTotalsAsync(
            postgres, resolved.ServerId, McpCommandDeadlines.ReadSeconds, ct);
        var monthly = await FinOpsUtilizationFigures.GetMonthlyCostUsdAsync(
            postgres, resolved.ServerId, McpCommandDeadlines.ReadSeconds, ct);

        var hasCpu = FinOpsUtilizationFigures.HasCpuSample(dto);
        var freePct = totals is { } t ? FinOpsUtilizationFigures.FreeSpacePct(t.AllocatedMb, t.FreeMb) : 100m;
        var score = FinOpsUtilizationFigures.HealthScore(dto, freePct);
        var vcoreEdition = ServerHardwareScope.HardwareIsTheHosts(dto.EngineEdition);
        var noVcores = vcoreEdition && dto.CpuCount <= 0;
        var hasCost = monthly > 0m;

        return JsonSerializer.Serialize(new
        {
            server = resolved.ServerName,
            view = UtilizationView,
            window_hours = 24,
            trend_days = 7,
            engine_edition = dto.EngineEdition,
            verdict = hasCpu ? dto.ProvisioningStatus : null,
            verdict_reason = hasCpu
                ? FinOpsUtilizationFigures.Explanation(dto, CultureInfo.InvariantCulture)
                : "No CPU sample in the last 24 hours, so there is no verdict.",
            health_score = score,
            health_band = FinOpsUtilizationFigures.HealthBand(score),
            health_score_note = hasCpu ? null : ServerHardwareScope.HealthScoreWithoutCpuNote,
            free_space_pct = Math.Round(freePct, 1),
            free_space_pct_reason = totals is null ? "no database size snapshot" : null,
            cpu = new
            {
                avg_cpu_pct = Math.Round(dto.AvgCpuPct, 1),
                p95_cpu_pct = Math.Round(dto.P95CpuPct, 1),
                max_cpu_pct = dto.MaxCpuPct,
                cpu_samples = dto.CpuSamples,
                cpu_count = noVcores ? (int?)null : dto.CpuCount,
                cpu_count_reason = noVcores ? "service objective names no vCores" : null,
                cpu_count_unit = vcoreEdition ? "vcores" : "cpus",
                max_workers = dto.MaxWorkersCount,
                current_workers = dto.CurrentWorkersCount,
            },
            memory = new
            {
                memory_basis = vcoreEdition ? "memory_limit" : "physical",
                physical_memory_mb = dto.PhysicalMemoryMb,
                target_memory_mb = dto.TargetMemoryMb,
                total_memory_mb = dto.TotalMemoryMb,
                buffer_pool_mb = dto.BufferPoolMb,
                memory_ratio = dto.MemoryRatio,
                buffer_pool_pct = Math.Round(FinOpsUtilizationFigures.BufferPoolPct(dto.BufferPoolMb, dto.PhysicalMemoryMb), 1),
                stolen_memory_pct = Math.Round(FinOpsUtilizationFigures.StolenMemoryPct(dto.TotalMemoryMb, dto.BufferPoolMb), 1),
                max_grant_waiters = dto.MaxGrantWaiters,
                grant_timeouts = dto.GrantTimeouts,
                forced_grants = dto.ForcedGrants,
                grant_utilization_pct = dto.GrantUtilizationPct,
            },
            monthly_cost_usd = hasCost ? monthly : (decimal?)null,
            annual_cost_usd = hasCost ? FinOpsCost.Annual(monthly) : (decimal?)null,
            cost_reason = hasCost ? null : "monthly cost not set",
            provisioning_trend = trend.Select(d => new
            {
                day = d.Day.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                avg_cpu_pct = Math.Round(d.AvgCpuPct, 1),
                p95_cpu_pct = Math.Round(d.P95CpuPct, 1),
                max_cpu_pct = d.MaxCpuPct,
                memory_ratio = d.MemoryRatio,
                verdict = d.Status,
            }).ToList(),
        }, McpHelpers.JsonOptions);
    }
}
