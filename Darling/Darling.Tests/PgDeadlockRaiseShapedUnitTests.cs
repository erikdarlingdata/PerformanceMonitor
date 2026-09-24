/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4058 r3: <see cref="PgDeadlockLogParser.FromEntry"/> and <see cref="PgDeadlockLogParser.IsRaiseShaped"/>
/// over hand-built <see cref="PgLogEntry"/> values, plus <see cref="PgDeadlocksCollector"/>'s csvlog branch
/// over a body holding one RAISE-shaped deadlock record alongside one real one.
/// </summary>
public sealed class PgDeadlockRaiseShapedUnitTests
{
    private const string Detail =
        "Process 5012 waits for ShareLock on transaction 809; blocked by process 5013."
        + "\nProcess 5013 waits for ShareLock on transaction 810; blocked by process 5012."
        + "\nProcess 5012: UPDATE accounts SET balance = balance - 1 WHERE card = 1"
        + "\nProcess 5013: UPDATE accounts SET note = 'x' WHERE id = 7";

    private static PgLogEntry DeadlockEntry(string? context = null, string? location = null) => new(
        TimestampText: "2026-09-24 00:00:00.000",
        ZoneText: "UTC",
        OccurredAtUtc: new DateTime(2026, 9, 24, 0, 0, 0, DateTimeKind.Utc),
        Pid: 5012,
        PrefixRest: string.Empty,
        Severity: "ERROR",
        Message: "deadlock detected",
        Detail: Detail,
        Hint: null,
        Statement: null,
        Context: context,
        UserName: null,
        DatabaseName: null,
        SqlState: "40P01",
        RawText: string.Empty,
        Location: location);

    /* ---- FromEntry / IsRaiseShaped ---------------------------------------------------------------------- */

    /// <summary>A real deadlock raised BY a statement inside a PL/pgSQL function carries a Context line
    /// ending " SQL statement", never " at RAISE" — this is PostgreSQL's own report and must be kept.</summary>
    [Fact]
    public void FromEntry_KeepsARealDeadlockWithAFunctionFrameEndingInSqlStatement()
    {
        var entry = DeadlockEntry(context: "SQL statement \"update accounts set balance = balance - 1\""
            + "\nPL/pgSQL function f() line 3 at SQL statement");

        Assert.False(PgDeadlockLogParser.IsRaiseShaped(entry));
        Assert.NotNull(PgDeadlockLogParser.FromEntry(entry));
    }

    [Fact]
    public void FromEntry_IsNullWhenTheContextEndsAtRaise()
    {
        var entry = DeadlockEntry(context: "PL/pgSQL function forge_deadlock() line 3 at RAISE");

        Assert.True(PgDeadlockLogParser.IsRaiseShaped(entry));
        Assert.Null(PgDeadlockLogParser.FromEntry(entry));
    }

    [Fact]
    public void FromEntry_IsNullWhenLocationNamesExecStmtRaise()
    {
        var entry = DeadlockEntry(location: "exec_stmt_raise, pl_exec.c:1");

        Assert.True(PgDeadlockLogParser.IsRaiseShaped(entry));
        Assert.Null(PgDeadlockLogParser.FromEntry(entry));
    }

    /* ---- PgDeadlocksCollector's csvlog branch ----------------------------------------------------------- */

    /* The 26-column csvlog record: field 18 (0-based) is the CONTEXT column, empty on a real deadlock —
       this fixture fills it with the PL/pgSQL frame a RAISE always appends, quoted for the embedded newline;
       every other field matches RealDeadlockRecord's own column count exactly (verified 26 fields). */
    private static string RaiseShapedDeadlockRecord() =>
        "2026-09-24 01:54:43.008 UTC,\"app_rw\",\"app_db\",5099,\"10.0.0.5:41000\",6ab482e3.61,1,"
        + "\"client backend\",2026-09-24 01:54:40 UTC,3/9,0,ERROR,40P01,\"deadlock detected\","
        + "\"" + Detail.Replace("\"", "\"\"") + "\",\"See server log for query details.\",,,"
        + "\"PL/pgSQL function forge_deadlock() line 3 at RAISE\",,,,\"client backend\",,,0\n";

    private static string RealDeadlockRecord() =>
        "2026-09-24 01:54:43.008 UTC,\"app_rw\",\"app_db\",5012,\"10.0.0.5:41000\",6ab482e3.60,1,"
        + "\"client backend\",2026-09-24 01:54:40 UTC,3/9,0,ERROR,40P01,\"deadlock detected\","
        + "\"" + Detail.Replace("\"", "\"\"") + "\",\"See server log for query details.\","
        + ",,,,,,\"client backend\",,,0\n";

    /* An unrelated RAISE ERROR, not deadlock-shaped at all: the marker message is absent, so this record
       must never add to raise_shaped_deadlocks_skipped. */
    private static string UnrelatedRaiseErrorRecord() =>
        "2026-09-24 01:54:43.008 UTC,\"app_rw\",\"app_db\",6001,\"10.0.0.5:41000\",6ab482e3.62,1,"
        + "\"client backend\",2026-09-24 01:54:40 UTC,3/9,0,ERROR,P0001,\"insufficient funds\","
        + ",,,\"PL/pgSQL function check_funds() line 2 at RAISE\",,,,\"client backend\",,,0\n";

    [Fact]
    public async Task ReadAsync_OverTheCsvRoute_SkipsARaiseShapedDeadlock_AndKeepsTheRealOne()
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "target-a",
            CollectionTime = new DateTime(2026, 9, 24, 0, 0, 0, DateTimeKind.Unspecified),
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
            PgLogUsesCsvlog = true,
        };

        var body = RaiseShapedDeadlockRecord() + RealDeadlockRecord();
        using var reader = new FakeReader(new object?[][] { new object?[] { body, "UTC" } });

        var rows = await PgDeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(5012, row.VictimPid);

        var measured = Assert.Single(context.Measurements);
        Assert.Equal(PgDeadlocksCollector.RaiseShapedDeadlocksSkippedMeasurement, measured.Label);
        Assert.Equal(1, measured.Value);
    }

    [Fact]
    public async Task ReadAsync_OverTheCsvRoute_DoesNotCountAnUnrelatedRaiseError()
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "target-a",
            CollectionTime = new DateTime(2026, 9, 24, 0, 0, 0, DateTimeKind.Unspecified),
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
            PgLogUsesCsvlog = true,
        };

        using var reader = new FakeReader(new object?[][] { new object?[] { UnrelatedRaiseErrorRecord(), "UTC" } });

        var rows = await PgDeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(rows);
        Assert.DoesNotContain(
            context.Measurements, m => m.Label == PgDeadlocksCollector.RaiseShapedDeadlocksSkippedMeasurement);
    }

    private sealed class FakeReader : System.Data.Common.DbDataReader
    {
        private readonly object?[][] _rows;
        private int _index = -1;

        public FakeReader(object?[][] rows) => _rows = rows;

        public override bool Read() => ++_index < _rows.Length;
        public override Task<bool> ReadAsync(CancellationToken cancellationToken) => Task.FromResult(Read());
        public override bool IsDBNull(int ordinal) => _rows[_index][ordinal] is null;
        public override string GetString(int ordinal) => (string)_rows[_index][ordinal]!;
        public override object GetValue(int ordinal) => _rows[_index][ordinal]!;
        public override int FieldCount => _rows.Length == 0 ? 0 : _rows[0].Length;
        public override bool HasRows => _rows.Length > 0;
        public override bool IsClosed => false;
        public override int RecordsAffected => 0;
        public override int Depth => 0;
        public override object this[int ordinal] => _rows[_index][ordinal]!;
        public override object this[string name] => throw new NotSupportedException();
        public override bool GetBoolean(int ordinal) => (bool)_rows[_index][ordinal]!;
        public override byte GetByte(int ordinal) => throw new NotSupportedException();
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override char GetChar(int ordinal) => throw new NotSupportedException();
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override string GetDataTypeName(int ordinal) => "text";
        public override DateTime GetDateTime(int ordinal) => (DateTime)_rows[_index][ordinal]!;
        public override decimal GetDecimal(int ordinal) => (decimal)_rows[_index][ordinal]!;
        public override double GetDouble(int ordinal) => (double)_rows[_index][ordinal]!;
        public override System.Collections.IEnumerator GetEnumerator() => _rows.GetEnumerator();
        public override Type GetFieldType(int ordinal) => typeof(string);
        public override float GetFloat(int ordinal) => (float)_rows[_index][ordinal]!;
        public override Guid GetGuid(int ordinal) => (Guid)_rows[_index][ordinal]!;
        public override short GetInt16(int ordinal) => (short)_rows[_index][ordinal]!;
        public override int GetInt32(int ordinal) => (int)_rows[_index][ordinal]!;
        public override long GetInt64(int ordinal) => (long)_rows[_index][ordinal]!;
        public override string GetName(int ordinal) => "log_body";
        public override int GetOrdinal(string name) => 0;
        public override int GetValues(object[] values) => throw new NotSupportedException();
        public override bool NextResult() => false;
    }
}
