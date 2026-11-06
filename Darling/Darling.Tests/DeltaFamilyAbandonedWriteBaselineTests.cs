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
/// #7782, through the production path of six delta families: each family's real <c>ReadAsync</c> over a result set shaped like
/// its query, then its real <c>WritePayload</c>, which stores the row's delta and interval. A run whose write is then abandoned is
/// rolled back the way the host does it on a stamped store fault, and the next run's stored delta covers both intervals. The
/// groups and the key each read captures under are the ones its delta calls use; a family whose capture named the wrong group or
/// derived a different key would leave its baselines advanced and read 30 over 60 s here. <c>wait_stats</c> has its own test.
/// </summary>
public sealed class DeltaFamilyAbandonedWriteBaselineTests
{
    private static readonly DateTime T0 = new(2026, 11, 2, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>The postmaster start pg_wait_stats observes off its first row, the same on every run so no epoch is seen.</summary>
    private static readonly DateTime PostmasterStart = new(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>One family: a one-row result set with the differenced counter at <paramref name="Table"/>'s argument, and the stored delta and interval positions its <c>WritePayload</c> writes them to.</summary>
    private sealed record Family(string Name, Func<long, DataTable> Table, Func<DataTable, CollectorContext, Task<(IBaselineRollback? Rows, long Delta, int Interval)>> Run);

    private static readonly Dictionary<string, Family> Families = new Family[]
    {
        new("latch_stats", LatchStatsTable, (t, c) => RunAsync(Catalog<LatchStatsCollector>(), t, c, deltaPosition: 4, intervalPosition: 7)),
        new("spinlock_stats", SpinlockStatsTable, (t, c) => RunAsync(Catalog<SpinlockStatsCollector>(), t, c, deltaPosition: 6, intervalPosition: 10)),
        new("file_io_stats", FileIoStatsTable, (t, c) => RunAsync(Catalog<FileIoStatsCollector>(), t, c, deltaPosition: 13, intervalPosition: 21)),
        new("memory_grants", MemoryGrantsTable, (t, c) => RunAsync(Catalog<MemoryGrantsCollector>(), t, c, deltaPosition: 12, intervalPosition: 14)),
        new("perfmon_stats", PerfmonStatsTable, (t, c) => RunAsync(Catalog<PerfmonStatsCollector>(), t, c, deltaPosition: 4, intervalPosition: 5)),
        new("pg_wait_stats", PgWaitStatsTable, (t, c) => RunAsync(Catalog<PgWaitStatsCollector>(), t, c, deltaPosition: 6, intervalPosition: 8)),
    }.ToDictionary(f => f.Name, StringComparer.Ordinal);

    public static TheoryData<string> FamilyNames
    {
        get
        {
            var data = new TheoryData<string>();
            foreach (var name in Families.Keys)
            {
                data.Add(name);
            }

            return data;
        }
    }

    private static T Catalog<T>() => CollectorCatalog.All.OfType<T>().Single();

    internal static CollectorContext Context(CollectorDeltaCalculator deltas, DateTime at) => new()
    {
        ServerId = 7,
        ServerName = "test-server",
        CollectionTime = at,
        Deltas = deltas,
        Target = new CollectorTargetInfo(),
    };

    /// <summary>One run: the read, then the write of its one row. The write's recorded payload carries the stored delta and interval.</summary>
    private static async Task<(IBaselineRollback? Rows, long Delta, int Interval)> RunAsync<TRow>(
        CollectorDefinitionBase<TRow> collector, DataTable table, CollectorContext context, int deltaPosition, int intervalPosition)
    {
        using var reader = table.CreateDataReader();
        var rows = await collector.ReadAsync(reader, context, CancellationToken.None);

        var writer = new RecordingRowWriter();
        collector.WritePayload(rows.Single(), writer, context);
        return (rows as IBaselineRollback, (long)writer.Values[deltaPosition]!, (int)writer.Values[intervalPosition]!);
    }

    /// <summary>
    /// Run one sets the baselines. Run two's write advances them and is then abandoned, so nothing is stored; the host rolls
    /// the rows back. Run three stores a delta that covers both intervals, over the real span since run one.
    /// </summary>
    [Theory]
    [MemberData(nameof(FamilyNames))]
    public async Task AnAbandonedWrite_RolledBackAsTheHostDoes_LeavesTheNextRunsDeltaSpanningBothIntervals(string familyName)
    {
        var family = Families[familyName];
        var deltas = new CollectorDeltaCalculator();

        using var first = family.Table(100);
        await family.Run(first, Context(deltas, T0));

        using var second = family.Table(130);
        var abandoned = await family.Run(second, Context(deltas, T0.AddMinutes(1)));
        Assert.Equal(30, abandoned.Delta);
        Assert.Equal(60, abandoned.Interval);
        abandoned.Rows?.RollBackBaselines();

        using var third = family.Table(160);
        var next = await family.Run(third, Context(deltas, T0.AddMinutes(2)));
        Assert.Equal(60, next.Delta);
        Assert.Equal(120, next.Interval);
    }

    /* Result sets shaped like each family's query, in the ordinals its ReadAsync reads. The counter the test differences is
       the first delta column's source; the siblings move with it so every group's interval is measured. */

    private static DataTable LatchStatsTable(long counter)
    {
        var table = new DataTable();
        table.Columns.Add("latch_class", typeof(string));
        table.Columns.Add("waiting_requests_count", typeof(long));
        table.Columns.Add("wait_time_ms", typeof(long));
        table.Columns.Add("max_wait_time_ms", typeof(long));
        table.Rows.Add("BUFFER", counter, counter * 10, counter);
        return table;
    }

    private static DataTable SpinlockStatsTable(long counter)
    {
        var table = new DataTable();
        table.Columns.Add("spinlock_name", typeof(string));
        table.Columns.Add("collisions", typeof(long));
        table.Columns.Add("spins", typeof(long));
        table.Columns.Add("spins_per_collision", typeof(float));
        table.Columns.Add("sleep_time", typeof(long));
        table.Columns.Add("backoffs", typeof(long));
        table.Rows.Add("LOCK_HASH", counter, counter * 10, 10f, counter, counter);
        return table;
    }

    private static DataTable FileIoStatsTable(long counter)
    {
        var table = new DataTable();
        table.Columns.Add("database_name", typeof(string));
        table.Columns.Add("file_name", typeof(string));
        table.Columns.Add("file_type", typeof(string));
        table.Columns.Add("physical_name", typeof(string));
        table.Columns.Add("size_mb", typeof(decimal));
        table.Columns.Add("num_of_reads", typeof(long));
        table.Columns.Add("num_of_writes", typeof(long));
        table.Columns.Add("read_bytes", typeof(long));
        table.Columns.Add("write_bytes", typeof(long));
        table.Columns.Add("io_stall_read_ms", typeof(long));
        table.Columns.Add("io_stall_write_ms", typeof(long));
        table.Columns.Add("io_stall_queued_read_ms", typeof(long));
        table.Columns.Add("io_stall_queued_write_ms", typeof(long));
        table.Columns.Add("database_id", typeof(int));
        table.Columns.Add("file_id", typeof(int));
        table.Rows.Add("example_db", "example_db_data", "ROWS", @"D:\data\example_db.mdf", 1024m, counter, counter, counter * 8192, counter * 8192, counter, counter, counter, counter, 5, 1);
        return table;
    }

    private static DataTable MemoryGrantsTable(long counter)
    {
        var table = new DataTable();
        table.Columns.Add("resource_semaphore_id", typeof(short));
        table.Columns.Add("pool_id", typeof(int));
        table.Columns.Add("target_memory_mb", typeof(decimal));
        table.Columns.Add("max_target_memory_mb", typeof(decimal));
        table.Columns.Add("total_memory_mb", typeof(decimal));
        table.Columns.Add("available_memory_mb", typeof(decimal));
        table.Columns.Add("granted_memory_mb", typeof(decimal));
        table.Columns.Add("used_memory_mb", typeof(decimal));
        table.Columns.Add("grantee_count", typeof(int));
        table.Columns.Add("waiter_count", typeof(int));
        table.Columns.Add("timeout_error_count", typeof(long));
        table.Columns.Add("forced_grant_count", typeof(long));
        table.Rows.Add((short)0, 2, 1000m, 1000m, 1000m, 900m, 100m, 50m, 1, 0, counter, counter);
        return table;
    }

    private static DataTable PerfmonStatsTable(long counter)
    {
        var table = new DataTable();
        table.Columns.Add("object_name", typeof(string));
        table.Columns.Add("counter_name", typeof(string));
        table.Columns.Add("instance_name", typeof(string));
        table.Columns.Add("cntr_value", typeof(long));
        table.Columns.Add("cntr_type", typeof(int));
        /* PERF_COUNTER_BULK_COUNT: a rate counter, so the write differences it rather than storing a gauge's level. */
        table.Rows.Add("SQLServer:SQL Statistics", "Batch Requests/sec", "", counter, 272696576);
        return table;
    }

    /// <summary>One pg_wait_stats row (an LWLock event, a type the collector keeps), with <paramref name="waits"/> as the differenced counter.</summary>
    internal static DataTable PgWaitStatsTable(long waits) => PgWaitStatsTable((50331649L, waits));

    /// <summary>pg_wait_stats rows in the ordinals its read uses: type_id, event_id, type_name, event_name, waits, wait_time, postmaster_start_time.</summary>
    internal static DataTable PgWaitStatsTable(params (long EventId, long Waits)[] rows)
    {
        var table = new DataTable();
        table.Columns.Add("type_id", typeof(int));
        table.Columns.Add("event_id", typeof(long));
        table.Columns.Add("type_name", typeof(string));
        table.Columns.Add("event_name", typeof(string));
        table.Columns.Add("waits", typeof(long));
        table.Columns.Add("wait_time", typeof(long));
        table.Columns.Add("postmaster_start_time", typeof(DateTime));
        foreach (var (eventId, waits) in rows)
        {
            table.Rows.Add(3, eventId, "LWLock", "BufferMapping", waits, waits * 1000, PostmasterStart);
        }

        return table;
    }
}
