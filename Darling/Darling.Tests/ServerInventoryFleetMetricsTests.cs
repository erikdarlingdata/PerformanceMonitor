/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Data.Common;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4227: FinOps Server Inventory's fleet-wide idle-database check. The two raw edges the stitched rollup cannot
/// cover are the idle cutoff rounded UP to the hour and the rollup watermark; the verdict facts run the
/// provisioning decision over fleet-read rows.
/// </summary>
public sealed class ServerInventoryFleetMetricsTests
{
    [Fact]
    public void ServerMetricsSqlFor_PutsTheTwoRawEdgesAtTheCeilingHourAndTheWatermark()
    {
        var coverage = RollupCoverage.Unknown; // no successor known here — legacy-only stitch, still exercises the composer
        var idleCutoff = new DateTime(2026, 3, 1, 4, 27, 0, DateTimeKind.Unspecified);
        var watermark = new DateTime(2026, 3, 7, 22, 0, 0, DateTimeKind.Unspecified);

        var sql = ViewerDataService.ServerMetricsSqlFor(coverage, idleCutoff, watermark);

        /* The two edge literals: the ceiling hour of the idle cutoff (05:00 — 04:27 rounds UP to the next
           whole hour, since the partial hour [04:27, 05:00) is exactly the slice the rollup can't answer)
           and the watermark, verbatim. */
        Assert.Contains("TIMESTAMP '2026-03-01 05:00:00", sql, StringComparison.Ordinal);
        Assert.Contains("TIMESTAMP '2026-03-07 22:00:00", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ServerMetricsSqlFor_WhenTheIdleCutoffIsAlreadyHourAligned_CeilingEqualsTheCutoff()
    {
        var coverage = RollupCoverage.Unknown;
        var idleCutoff = new DateTime(2026, 3, 1, 4, 0, 0, DateTimeKind.Unspecified);
        var watermark = new DateTime(2026, 3, 7, 22, 0, 0, DateTimeKind.Unspecified);

        var sql = ViewerDataService.ServerMetricsSqlFor(coverage, idleCutoff, watermark);

        Assert.Contains("TIMESTAMP '2026-03-01 04:00:00", sql, StringComparison.Ordinal);
    }

    // ── A server with no CPU sample gets no verdict ──

    /// <summary>One fleet-read row shaped like <c>ServerMetricsSql</c>'s SELECT list (ordinals 0-20), so the
    /// verdict decision can be exercised without a store. Workers come back NULL, as they do for a server with
    /// no memory sample; the grant columns are COALESCEd to 0 by the SELECT.</summary>
    private static DbDataReader FleetRow(decimal? avgCpu, decimal? maxCpu, decimal? p95Cpu, int? engineEdition = null, string? edition = null,
        long cpuSamples = 288, int? totalMemoryMb = null, int? bufferPoolMb = null)
    {
        var table = new DataTable();
        table.Columns.Add("server_id", typeof(int));
        table.Columns.Add("avg_cpu_pct", typeof(decimal));
        table.Columns.Add("total_storage_gb", typeof(decimal));
        table.Columns.Add("idle_db_count", typeof(int));
        table.Columns.Add("max_cpu_pct", typeof(decimal));
        table.Columns.Add("p95_cpu_pct", typeof(decimal));
        table.Columns.Add("max_workers_count", typeof(int));
        table.Columns.Add("current_workers_count", typeof(int));
        table.Columns.Add("max_grant_waiters", typeof(long));
        table.Columns.Add("grant_timeouts", typeof(long));
        table.Columns.Add("forced_grants", typeof(long));
        table.Columns.Add("grant_utilization_pct", typeof(decimal));
        table.Columns.Add("engine_edition", typeof(int));
        table.Columns.Add("edition", typeof(string));
        table.Columns.Add("total_physical_memory_mb", typeof(int));
        table.Columns.Add("buffer_pool_mb", typeof(int));
        table.Columns.Add("mem_server_id", typeof(int));
        table.Columns.Add("alloc_mb", typeof(decimal));
        table.Columns.Add("free_mb", typeof(decimal));
        table.Columns.Add("cpu_samples", typeof(long));
        table.Columns.Add("total_server_memory_mb", typeof(int));

        table.Rows.Add(
            1,
            (object?)avgCpu ?? DBNull.Value,
            20m,
            1,
            (object?)maxCpu ?? DBNull.Value,
            (object?)p95Cpu ?? DBNull.Value,
            DBNull.Value,
            DBNull.Value,
            0L,
            0L,
            0L,
            0m,
            (object?)engineEdition ?? DBNull.Value,
            (object?)edition ?? DBNull.Value,
            (object?)totalMemoryMb ?? DBNull.Value,
            (object?)bufferPoolMb ?? DBNull.Value,
            DBNull.Value,
            DBNull.Value,
            DBNull.Value,
            cpuSamples,
            (object?)totalMemoryMb ?? DBNull.Value);

        var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        return reader;
    }

    /// <summary>
    /// A server with size and properties rows but no CPU row in the 24-hour window has a NULL average CPU.
    /// The read used to turn that into 0% CPU, and <c>Evaluate</c> called the server OVER_PROVISIONED — on
    /// real data every server without a CPU sample was told to shrink. It gets no verdict now.
    /// </summary>
    [Fact]
    public void FleetProvisioningStatusFor_ServerWithNoCpuSample_GetsNoVerdict()
    {
        using var reader = FleetRow(avgCpu: null, maxCpu: null, p95Cpu: null);

        Assert.Null(ViewerDataService.FleetProvisioningStatusFor(reader));
    }

    /// <summary>The rule is about a MISSING sample, not a quiet one: low CPU still earns OVER_PROVISIONED,
    /// the verdict every server with CPU samples got before.</summary>
    [Fact]
    public void FleetProvisioningStatusFor_ServerWithLowCpu_StillGetsOverProvisioned()
    {
        using var reader = FleetRow(avgCpu: 6m, maxCpu: 8m, p95Cpu: 7m);

        Assert.Equal(ProvisioningVerdict.OverProvisioned, ViewerDataService.FleetProvisioningStatusFor(reader));
    }

    /// <summary>A logical server's master has no service objective to resize, so the grid gets the N/A verdict.</summary>
    [Fact]
    public void FleetProvisioningStatusFor_LogicalServerMaster_GetsNotApplicable()
    {
        using var master = FleetRow(avgCpu: 6m, maxCpu: 8m, p95Cpu: 7m, engineEdition: 5, edition: "Azure SQL Database (System)");
        using var userDatabase = FleetRow(avgCpu: 6m, maxCpu: 8m, p95Cpu: 7m, engineEdition: 5, edition: "Azure SQL Database (General Purpose)");

        Assert.Equal(ProvisioningVerdict.NotApplicable, ViewerDataService.FleetProvisioningStatusFor(master));
        Assert.Equal(ProvisioningVerdict.OverProvisioned, ViewerDataService.FleetProvisioningStatusFor(userDatabase));
    }

    /// <summary>The other two verdicts are untouched: a hot server is still under-provisioned and a busy one
    /// still right-sized.</summary>
    [Fact]
    public void FleetProvisioningStatusFor_ServersWithCpuSamples_KeepTheirVerdicts()
    {
        using var hot = FleetRow(avgCpu: 60m, maxCpu: 99m, p95Cpu: 95m);
        using var busy = FleetRow(avgCpu: 40m, maxCpu: 70m, p95Cpu: 60m);

        Assert.Equal(ProvisioningVerdict.UnderProvisioned, ViewerDataService.FleetProvisioningStatusFor(hot));
        Assert.Equal(ProvisioningVerdict.RightSized, ViewerDataService.FleetProvisioningStatusFor(busy));
    }

    /// <summary>#6139: the fleet row counts its samples, so a server back from a long gap with 6 of them gets no verdict, the same answer the drill-down gives.</summary>
    [Fact]
    public void FleetProvisioningStatusFor_ServerWithSixSamples_GetsNotEnoughData()
    {
        using var few = FleetRow(avgCpu: 6m, maxCpu: 8m, p95Cpu: 7m, cpuSamples: 6);
        using var enough = FleetRow(avgCpu: 6m, maxCpu: 8m, p95Cpu: 7m, cpuSamples: 12);

        Assert.Equal(ProvisioningVerdict.NotEnoughData, ViewerDataService.FleetProvisioningStatusFor(few));
        Assert.Equal(ProvisioningVerdict.OverProvisioned, ViewerDataService.FleetProvisioningStatusFor(enough));
    }

    /// <summary>#6139: 1,000 MB held and 275 MB of it buffer pool is 72.5% stolen. That is memory pressure, not a server with RAM to give back.</summary>
    [Fact]
    public void FleetProvisioningStatusFor_HighStolenMemory_IsUnderProvisionedNotOver()
    {
        using var stolen = FleetRow(avgCpu: 6m, maxCpu: 8m, p95Cpu: 7m, totalMemoryMb: 1000, bufferPoolMb: 275);
        using var spare = FleetRow(avgCpu: 6m, maxCpu: 8m, p95Cpu: 7m, totalMemoryMb: 1000, bufferPoolMb: 800);

        Assert.Equal(ProvisioningVerdict.UnderProvisioned, ViewerDataService.FleetProvisioningStatusFor(stolen));
        Assert.Equal(ProvisioningVerdict.OverProvisioned, ViewerDataService.FleetProvisioningStatusFor(spare));
    }

    /// <summary>#6139: the grid shows the Viewer's words for the new verdict, not the raw token.</summary>
    [Fact]
    public void TheGridShowsNotEnoughDataInTheBadgesWords()
    {
        Assert.Equal("not enough data yet", new ServerPropertyRow { ProvisioningStatus = ProvisioningVerdict.NotEnoughData }.ProvisioningDisplay);
        Assert.Equal("not enough data yet", new ProvisioningTrendRow { Status = ProvisioningVerdict.NotEnoughData }.StatusDisplay);
        Assert.Equal("not enough data yet (6 samples)", ProvisioningVerdict.StatusLabel(ProvisioningVerdict.NotEnoughData, 6));
        Assert.Equal("OVER PROVISIONED", new ServerPropertyRow { ProvisioningStatus = ProvisioningVerdict.OverProvisioned }.ProvisioningDisplay);
    }
}
