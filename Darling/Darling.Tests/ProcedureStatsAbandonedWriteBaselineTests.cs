/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7782, through the production path of <see cref="ProcedureStatsCollector"/>: the real <c>ReadAsync</c> over a result set
/// shaped like its query, then its <c>WritePayload</c>. This family advances baselines in two places: the read's idle skip
/// differences a row whose seven counters are unchanged, and drops it when nothing moved, and the write differences every row
/// it stores. So the read captures every fetched key before its ranking loop, not after it. A capture after the loop would
/// snapshot the idle row's baseline as the run had just advanced it, and after a rollback the next run would read that row's
/// delta over one interval instead of two.
/// </summary>
public sealed class ProcedureStatsAbandonedWriteBaselineTests
{
    private static readonly DateTime T0 = new(2026, 11, 2, 12, 0, 0, DateTimeKind.Utc);

    private const string IdlePlan = "0x0500FF7F01";
    private const string ActivePlan = "0x0500FF7F02";

    private static CollectorContext Context(CollectorDeltaCalculator deltas, DateTime at) => new()
    {
        ServerId = 7,
        ServerName = "test-server",
        CollectionTime = at,
        Deltas = deltas,
        Target = new CollectorTargetInfo(),
        CapturePlanXml = false,
    };

    /// <summary>One run over two modules, each with its seven counters at the given value. The write's recorded payload carries each stored row's delta_execution_count and sample_interval_seconds, by plan handle.</summary>
    private static async Task<(IBaselineRollback? Rows, Dictionary<string, (long Delta, int Interval)> Stored)> RunAsync(CollectorDeltaCalculator deltas, DateTime at, long idleCounters, long activeCounters)
    {
        var context = Context(deltas, at);
        using var table = Table((IdlePlan, "idle_proc", idleCounters), (ActivePlan, "active_proc", activeCounters));
        var collector = CollectorCatalog.All.OfType<ProcedureStatsCollector>().Single();
        using var reader = table.CreateDataReader();
        var rows = await collector.ReadAsync(reader, context, CancellationToken.None);

        var stored = new Dictionary<string, (long, int)>(StringComparer.Ordinal);
        foreach (var row in rows)
        {
            var writer = new RecordingRowWriter();
            collector.WritePayload(row, writer, context);
            stored[row.PlanHandle!] = ((long)writer.Values[27]!, (int)writer.Values[36]!);
        }

        return (rows as IBaselineRollback, stored);
    }

    /// <summary>
    /// Run one sets both modules' baselines. In run two the idle module's counters are unchanged, so the read differences it
    /// and drops it, and the active module's write advances its baselines; the write is then abandoned and the host rolls the
    /// rows back. Run three stores both modules' deltas over both intervals, the idle module's included.
    /// </summary>
    [Fact]
    public async Task AnAbandonedWrite_RolledBackAsTheHostDoes_LeavesBothTheIdleAndTheStoredRowsDeltaSpanningBothIntervals()
    {
        var deltas = new CollectorDeltaCalculator();
        await RunAsync(deltas, T0, idleCounters: 100, activeCounters: 100);

        var abandoned = await RunAsync(deltas, T0.AddMinutes(1), idleCounters: 100, activeCounters: 130);
        Assert.Equal(new[] { ActivePlan }, abandoned.Stored.Keys);
        Assert.Equal((30L, 60), abandoned.Stored[ActivePlan]);
        abandoned.Rows?.RollBackBaselines();

        var next = await RunAsync(deltas, T0.AddMinutes(2), idleCounters: 160, activeCounters: 160);
        Assert.Equal((60L, 120), next.Stored[IdlePlan]);
        Assert.Equal((60L, 120), next.Stored[ActivePlan]);
    }

    /// <summary>A result set in the ordinals the read uses, one module per entry, its seven cumulative counters all at <c>Counters</c>.</summary>
    private static DataTable Table(params (string PlanHandle, string ObjectName, long Counters)[] modules)
    {
        var table = new DataTable();
        table.Columns.Add("database_name", typeof(string));
        table.Columns.Add("schema_name", typeof(string));
        table.Columns.Add("object_name", typeof(string));
        table.Columns.Add("object_type", typeof(string));
        table.Columns.Add("cached_time", typeof(DateTime));
        table.Columns.Add("last_execution_time", typeof(DateTime));
        table.Columns.Add("execution_count", typeof(long));
        table.Columns.Add("total_worker_time", typeof(long));
        table.Columns.Add("total_elapsed_time", typeof(long));
        table.Columns.Add("total_logical_reads", typeof(long));
        table.Columns.Add("total_physical_reads", typeof(long));
        table.Columns.Add("total_logical_writes", typeof(long));
        table.Columns.Add("min_worker_time", typeof(long));
        table.Columns.Add("max_worker_time", typeof(long));
        table.Columns.Add("min_elapsed_time", typeof(long));
        table.Columns.Add("max_elapsed_time", typeof(long));
        table.Columns.Add("min_logical_reads", typeof(long));
        table.Columns.Add("max_logical_reads", typeof(long));
        table.Columns.Add("min_physical_reads", typeof(long));
        table.Columns.Add("max_physical_reads", typeof(long));
        table.Columns.Add("min_logical_writes", typeof(long));
        table.Columns.Add("max_logical_writes", typeof(long));
        table.Columns.Add("total_spills", typeof(long));
        table.Columns.Add("min_spills", typeof(long));
        table.Columns.Add("max_spills", typeof(long));
        table.Columns.Add("sql_handle", typeof(string));
        table.Columns.Add("plan_handle", typeof(string));
        foreach (var (planHandle, objectName, counters) in modules)
        {
            table.Rows.Add("example_db", "dbo", objectName, "P", T0.AddHours(-1), T0,
                counters, counters, counters, counters, counters, counters,
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                counters, 0L, 0L,
                "0x0300FF7F" + objectName.Length, planHandle);
        }

        return table;
    }
}
