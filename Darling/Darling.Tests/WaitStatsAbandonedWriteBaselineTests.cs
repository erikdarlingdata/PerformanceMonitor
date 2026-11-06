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
/// #7782, through the production path: the real <see cref="WaitStatsCollector"/> read over a result set shaped like its query,
/// then its <c>WritePayload</c>, which advances the row's baselines as the store COPY would. A run whose write is then abandoned
/// is rolled back the way the host does it on a stamped store fault, and the next run's stored delta covers both intervals.
/// </summary>
public sealed class WaitStatsAbandonedWriteBaselineTests
{
    private static readonly DateTime T0 = new(2026, 11, 2, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>One run: the read, then the write of its one row. The write's recorded payload carries the stored delta and interval.</summary>
    private static async Task<(List<WaitStatsCollector.Row> Rows, long Delta, int Interval)> RunAsync(CollectorDeltaCalculator deltas, DateTime at, long waitingTasks)
    {
        var context = new CollectorContext
        {
            ServerId = 7,
            ServerName = "test-server",
            CollectionTime = at,
            Deltas = deltas,
            Target = new CollectorTargetInfo(),
        };

        using var table = new DataTable();
        table.Columns.Add("wait_type", typeof(string));
        table.Columns.Add("waiting_tasks_count", typeof(long));
        table.Columns.Add("wait_time_ms", typeof(long));
        table.Columns.Add("signal_wait_time_ms", typeof(long));
        table.Rows.Add("PAGEIOLATCH_SH", waitingTasks, waitingTasks * 10, waitingTasks);

        var collector = CollectorCatalog.All.OfType<WaitStatsCollector>().Single();
        using var reader = table.CreateDataReader();
        var rows = await collector.ReadAsync(reader, context, CancellationToken.None);

        var writer = new RecordingWriter();
        collector.WritePayload(rows.Single(), writer, context);
        return (rows, (long)writer.Values[4]!, (int)writer.Values[7]!);
    }

    /// <summary>
    /// Run one sets the baselines. Run two's write advances them and is then abandoned, so nothing is stored; the host rolls
    /// the rows back. Run three stores a delta that covers both intervals, over the real span since run one.
    /// </summary>
    [Fact]
    public async Task AnAbandonedWrite_RolledBackAsTheHostDoes_LeavesTheNextRunsDeltaSpanningBothIntervals()
    {
        var deltas = new CollectorDeltaCalculator();
        await RunAsync(deltas, T0, 100);

        var abandoned = await RunAsync(deltas, T0.AddMinutes(1), 130);
        Assert.Equal(30, abandoned.Delta);
        (abandoned.Rows as IBaselineRollback)?.RollBackBaselines();

        var next = await RunAsync(deltas, T0.AddMinutes(2), 160);
        Assert.Equal(60, next.Delta);
        Assert.Equal(120, next.Interval);
    }

    private sealed class RecordingWriter : ICollectorRowWriter
    {
        public List<object?> Values { get; } = new();
        private ICollectorRowWriter Add(object? v) { Values.Add(v); return this; }
        public ICollectorRowWriter Value(string? value) => Add(value);
        public ICollectorRowWriter Value(long value) => Add(value);
        public ICollectorRowWriter Value(long? value) => Add(value);
        public ICollectorRowWriter Value(int value) => Add(value);
        public ICollectorRowWriter Value(int? value) => Add(value);
        public ICollectorRowWriter Value(short value) => Add(value);
        public ICollectorRowWriter Value(short? value) => Add(value);
        public ICollectorRowWriter Value(double value) => Add(value);
        public ICollectorRowWriter Value(double? value) => Add(value);
        public ICollectorRowWriter Value(decimal value) => Add(value);
        public ICollectorRowWriter Value(decimal? value) => Add(value);
        public ICollectorRowWriter Value(bool value) => Add(value);
        public ICollectorRowWriter Value(bool? value) => Add(value);
        public ICollectorRowWriter Value(DateTime value) => Add(value);
        public ICollectorRowWriter Value(DateTime? value) => Add(value);
        public ICollectorRowWriter NullValue() => Add(null);
    }
}
