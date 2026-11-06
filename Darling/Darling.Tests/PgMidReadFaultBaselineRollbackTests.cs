/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7782: pg_statement_stats and pg_wait_stats advance a row's baselines inside the reader loop, and the hosts take the rows
/// as <see cref="IBaselineRollback"/> only after <c>ReadAsync</c> returns. A read that faults after its first row (a stop, a
/// transport fault, the #2673 budget arm) has advanced that row's baselines for a run that stores nothing, so the read rolls them
/// back itself before the exception leaves it. The exception still propagates, and the next run's delta covers both intervals.
/// </summary>
public sealed class PgMidReadFaultBaselineRollbackTests
{
    private static readonly DateTime T0 = new(2026, 11, 2, 12, 0, 0, DateTimeKind.Utc);

    private const long StatementQueryId = 1001;

    [Fact]
    public async Task PgWaitStats_AReadThatFaultsOnItsSecondRow_PropagatesAndLeavesTheFirstRowsBaselineWhereItStood()
    {
        var collector = CollectorCatalog.All.OfType<PgWaitStatsCollector>().Single();
        var deltas = new CollectorDeltaCalculator();

        using (var first = DeltaFamilyAbandonedWriteBaselineTests.PgWaitStatsTable(100))
        using (var reader = first.CreateDataReader())
        {
            await collector.ReadAsync(reader, DeltaFamilyAbandonedWriteBaselineTests.Context(deltas, T0), CancellationToken.None);
        }

        using (var second = DeltaFamilyAbandonedWriteBaselineTests.PgWaitStatsTable((50331649L, 130), (50331650L, 10)))
        using (var faulting = new FaultingReader(second.CreateDataReader(), faultOnRow: 2))
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await collector.ReadAsync(faulting, DeltaFamilyAbandonedWriteBaselineTests.Context(deltas, T0.AddMinutes(1)), CancellationToken.None));
        }

        using var third = DeltaFamilyAbandonedWriteBaselineTests.PgWaitStatsTable(160);
        using var thirdReader = third.CreateDataReader();
        var next = (await collector.ReadAsync(thirdReader, DeltaFamilyAbandonedWriteBaselineTests.Context(deltas, T0.AddMinutes(2)), CancellationToken.None)).Single();
        Assert.Equal(60, next.DeltaWaits);
        Assert.Equal(120, next.SampleIntervalSeconds);
    }

    [Fact]
    public async Task PgStatementStats_AReadThatFaultsOnItsSecondRow_PropagatesAndLeavesTheFirstRowsBaselineWhereItStood()
    {
        var collector = CollectorCatalog.All.OfType<PgStatementStatsCollector>().Single();
        var deltas = new CollectorDeltaCalculator();

        using (var first = StatementStatsTable(T0, (StatementQueryId, 100)))
        using (var reader = first.CreateDataReader())
        {
            await collector.ReadAsync(reader, DeltaFamilyAbandonedWriteBaselineTests.Context(deltas, T0), CancellationToken.None);
        }

        using (var second = StatementStatsTable(T0.AddMinutes(1), (StatementQueryId, 130), (StatementQueryId + 1, 10)))
        using (var faulting = new FaultingReader(second.CreateDataReader(), faultOnRow: 2))
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await collector.ReadAsync(faulting, DeltaFamilyAbandonedWriteBaselineTests.Context(deltas, T0.AddMinutes(1)), CancellationToken.None));
        }

        using var third = StatementStatsTable(T0.AddMinutes(2), (StatementQueryId, 160));
        using var thirdReader = third.CreateDataReader();
        var next = (await collector.ReadAsync(thirdReader, DeltaFamilyAbandonedWriteBaselineTests.Context(deltas, T0.AddMinutes(2)), CancellationToken.None)).Single();
        Assert.Equal(60, next.DeltaCalls);
        Assert.Equal(120, next.SampleIntervalSeconds);
    }

    /// <summary>
    /// pg_statement_stats rows in the ordinals the read uses: the identity, calls and the timings, the block and WAL counters
    /// (the Aurora-only and WAL ones NULL, as the stock view returns them), then stats_reset (NULL), stats_since (NULL, as below
    /// PostgreSQL 17), target_now at <paramref name="targetNow"/> and the dealloc count (NULL).
    /// </summary>
    private static DataTable StatementStatsTable(DateTime targetNow, params (long QueryId, long Calls)[] rows)
    {
        var table = new DataTable();
        table.Columns.Add("queryid", typeof(long));
        table.Columns.Add("database_id", typeof(long));
        table.Columns.Add("user_id", typeof(long));
        table.Columns.Add("toplevel", typeof(bool));
        table.Columns.Add("calls", typeof(long));
        table.Columns.Add("total_exec_time", typeof(double));
        table.Columns.Add("min_exec_time", typeof(double));
        table.Columns.Add("max_exec_time", typeof(double));
        table.Columns.Add("mean_exec_time", typeof(double));
        table.Columns.Add("rows", typeof(long));
        table.Columns.Add("shared_blks_hit", typeof(long));
        table.Columns.Add("shared_blks_read", typeof(long));
        table.Columns.Add("shared_blks_dirtied", typeof(long));
        table.Columns.Add("shared_blks_written", typeof(long));
        table.Columns.Add("temp_blks_read", typeof(long));
        table.Columns.Add("temp_blks_written", typeof(long));
        table.Columns.Add("blk_read_time", typeof(double));
        table.Columns.Add("blk_write_time", typeof(double));
        table.Columns.Add("storage_blks_read", typeof(long));
        table.Columns.Add("orcache_blks_hit", typeof(long));
        table.Columns.Add("storage_blk_read_time", typeof(double));
        table.Columns.Add("orcache_blk_read_time", typeof(double));
        table.Columns.Add("wal_records", typeof(long));
        table.Columns.Add("wal_fpi", typeof(long));
        table.Columns.Add("wal_bytes", typeof(long));
        table.Columns.Add("total_exec_peakmem", typeof(long));
        table.Columns.Add("max_exec_peakmem", typeof(long));
        table.Columns.Add("stats_reset", typeof(DateTime));
        table.Columns.Add("stats_since", typeof(DateTime));
        table.Columns.Add("target_now", typeof(DateTime));
        table.Columns.Add("dealloc", typeof(long));
        foreach (var (queryId, calls) in rows)
        {
            table.Rows.Add(queryId, 16384L, 10L, true, calls, calls * 2.5, 1.0, 5.0, 2.5, calls * 3,
                calls * 20, 0L, 0L, 0L, 0L, 0L, 0.0, 0.0,
                DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                DBNull.Value, DBNull.Value, targetNow, DBNull.Value);
        }

        return table;
    }

    /// <summary>A reader that yields its inner reader's rows and throws when asked for row number <c>faultOnRow</c>, the way a lost connection surfaces mid-read.</summary>
    private sealed class FaultingReader(DbDataReader inner, int faultOnRow) : DbDataReader
    {
        private int _row;

        public override bool Read()
        {
            if (++_row == faultOnRow)
            {
                throw new InvalidOperationException("the connection was lost while reading");
            }

            return inner.Read();
        }

        public override int Depth => inner.Depth;
        public override int FieldCount => inner.FieldCount;
        public override bool HasRows => inner.HasRows;
        public override bool IsClosed => inner.IsClosed;
        public override int RecordsAffected => inner.RecordsAffected;
        public override object this[int ordinal] => inner[ordinal];
        public override object this[string name] => inner[name];
        public override bool GetBoolean(int ordinal) => inner.GetBoolean(ordinal);
        public override byte GetByte(int ordinal) => inner.GetByte(ordinal);
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => inner.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        public override char GetChar(int ordinal) => inner.GetChar(ordinal);
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => inner.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
        public override string GetDataTypeName(int ordinal) => inner.GetDataTypeName(ordinal);
        public override DateTime GetDateTime(int ordinal) => inner.GetDateTime(ordinal);
        public override decimal GetDecimal(int ordinal) => inner.GetDecimal(ordinal);
        public override double GetDouble(int ordinal) => inner.GetDouble(ordinal);
        public override IEnumerator GetEnumerator() => inner.GetEnumerator();
        public override Type GetFieldType(int ordinal) => inner.GetFieldType(ordinal);
        public override float GetFloat(int ordinal) => inner.GetFloat(ordinal);
        public override Guid GetGuid(int ordinal) => inner.GetGuid(ordinal);
        public override short GetInt16(int ordinal) => inner.GetInt16(ordinal);
        public override int GetInt32(int ordinal) => inner.GetInt32(ordinal);
        public override long GetInt64(int ordinal) => inner.GetInt64(ordinal);
        public override string GetName(int ordinal) => inner.GetName(ordinal);
        public override int GetOrdinal(string name) => inner.GetOrdinal(name);
        public override string GetString(int ordinal) => inner.GetString(ordinal);
        public override object GetValue(int ordinal) => inner.GetValue(ordinal);
        public override int GetValues(object[] values) => inner.GetValues(values);
        public override bool IsDBNull(int ordinal) => inner.IsDBNull(ordinal);
        public override bool NextResult() => inner.NextResult();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                inner.Dispose();
            }

            base.Dispose(disposing);
        }
    }
}
