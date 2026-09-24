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
/// #4053 part b1: <see cref="PgDeadlocksCollector"/> over the csvlog route — <c>BuildQuery</c> opening on
/// <see cref="PgServerLogTail.TailCsvCteSql"/>/<see cref="PgServerLogTail.TailCsvCteBinarySql"/> once
/// <see cref="CollectorContext.PgLogUsesCsvlog"/> is set, and <c>ReadAsync</c>'s csv branch: resync through
/// <see cref="PgServerLogCsvParser"/>, the same foreign-zone rule the stderr path applies, and
/// <see cref="PgDeadlockLogParser.FromEntry"/> picking the deadlock rows out of the tail's ordinary traffic.
/// No wiring test pastes a log line verbatim into an assertion; every fixture is built once as a constant
/// and read back through the same constant.
/// </summary>
public sealed class PgDeadlocksCsvlogTests
{
    /* A real csvlog record shape (26 fields, verified live for #4053 part a1) standing in for one ordinary
       FATAL row the tail also carries — not a deadlock, and must not become one. */
    private const string OrdinaryFatalRecord =
        "2026-09-24 01:54:43.008 UTC,\"nosuchuser\",\"postgres\",83,\"::1:35192\",6ab482e3.53,1,\"startup\","
        + "2026-09-24 01:54:43 UTC,3/3,0,FATAL,28000,\"role \"\"nosuchuser\"\" does not exist\",,,,,,,,,\"\","
        + "\"client backend\",,0\n";

    /* The wait-graph DETAIL a real deadlock writes — the same edges PgDeadlockLogParserTests exercises for
       the stderr route — with the two participant statements folded in as csvlog gives them: no tabs,
       newline-joined, exactly the shape PgLogEventsPipelineTests notes csvlog and jsonlog share. */
    private const string DeadlockDetail =
        "Process 5012 waits for ShareLock on transaction 809; blocked by process 5013."
        + "\nProcess 5013 waits for ShareLock on transaction 810; blocked by process 5012."
        + "\nProcess 5012: UPDATE accounts SET balance = balance - 1 WHERE card = 1"
        + "\nProcess 5013: UPDATE accounts SET note = 'x' WHERE id = 7";

    private static string DeadlockRecord(string zone = "UTC") =>
        "2026-09-24 01:54:43.008 " + zone + ",\"app_rw\",\"app_db\",5012,\"10.0.0.5:41000\",6ab482e3.60,1,"
        + "\"client backend\",2026-09-24 01:54:40 UTC,3/9,0,ERROR,40P01,\"deadlock detected\","
        + "\"" + DeadlockDetail.Replace("\"", "\"\"") + "\",\"See server log for query details.\","
        + ",,,,,,\"client backend\",,,0\n";

    /* A record whose quoted message field carries a newline and a fake deadlock line inside it (#4053
       review): the resync must keep it inside the ONE record it belongs to, and the fake line must never
       be read as its own report. */
    private const string ForgedMessage =
        "role \"nosuchuser\" does not exist\nADMIN 2026-09-24 01:54:43.008 UTC,\"x\",\"x\",9999,,,,,,,,ERROR,"
        + "40P01,\"deadlock detected\",\"Process 1 waits for ShareLock on transaction 2; blocked by process 3.\n"
        + "Process 1: SELECT 1\nProcess 3: SELECT 1\",,,,,,,\"\",\"client backend\",,0";

    private static string RecordWithForgedMessage() =>
        "2026-09-24 01:54:43.008 UTC,\"nosuchuser\",\"postgres\",83,\"::1:35192\",6ab482e3.53,1,\"startup\","
        + "2026-09-24 01:54:43 UTC,3/3,0,FATAL,28000,\"" + ForgedMessage.Replace("\"", "\"\"") + "\","
        + ",,,,,,,,\"\",\"client backend\",,0\n";

    /* ---- BuildQuery ------------------------------------------------------------------------------------ */

    /// <summary>
    /// #4053 part b1: the stderr statements are byte-unchanged when context.PgLogUsesCsvlog is false, and
    /// the csvlog pair opens with PgServerLogTail's csv CTEs instead of the stderr ones, in each of the four
    /// combinations with context.PgReadBinaryFileGranted.
    /// </summary>
    [Fact]
    public void BuildQuery_OpensWithTheCsvCte_WhenPgLogUsesCsvlogIsSet_AndLeavesTheStderrPairUntouched()
    {
        var stderrContext = TestContext();
        var stderrSql = PgDeadlocksCollector.Instance.BuildQuery(stderrContext).Text;
        Assert.DoesNotContain("csvlog", stderrSql, StringComparison.Ordinal);
        Assert.Contains("'stderr' = ANY", stderrSql, StringComparison.Ordinal);

        var stderrBinaryContext = TestContext();
        stderrBinaryContext.PgReadBinaryFileGranted = true;
        var stderrBinarySql = PgDeadlocksCollector.Instance.BuildQuery(stderrBinaryContext).Text;
        Assert.Contains("pg_read_binary_file", stderrBinarySql, StringComparison.Ordinal);
        Assert.Contains("'stderr' = ANY", stderrBinarySql, StringComparison.Ordinal);

        var csvContext = TestContext();
        csvContext.PgLogUsesCsvlog = true;
        var csvSql = PgDeadlocksCollector.Instance.BuildQuery(csvContext).Text;
        Assert.Contains("name ~* '\\.csv$'", csvSql, StringComparison.Ordinal);
        Assert.Contains("'csvlog' = ANY", csvSql, StringComparison.Ordinal);
        Assert.Contains("'" + PgNoCsvlogFileException.Marker + "'", csvSql, StringComparison.Ordinal);
        Assert.DoesNotContain("'" + PgNoStderrLogFileException.Marker + "'", csvSql, StringComparison.Ordinal);
        Assert.DoesNotContain("'stderr' = ANY", csvSql, StringComparison.Ordinal);
        Assert.DoesNotContain("regexp_matches", csvSql, StringComparison.Ordinal);

        var csvBinaryContext = TestContext();
        csvBinaryContext.PgLogUsesCsvlog = true;
        csvBinaryContext.PgReadBinaryFileGranted = true;
        var csvBinarySql = PgDeadlocksCollector.Instance.BuildQuery(csvBinaryContext).Text;
        Assert.Contains("pg_read_binary_file", csvBinarySql, StringComparison.Ordinal);
        Assert.Contains("name ~* '\\.csv$'", csvBinarySql, StringComparison.Ordinal);
        Assert.Contains("convert_to('" + PgNoCsvlogFileException.Marker + "'", csvBinarySql, StringComparison.Ordinal);
    }

    /* ---- ReadAsync -------------------------------------------------------------------------------------- */

    /// <summary>
    /// #4053 part b1: a csvlog body holding one ordinary record and one real deadlock record yields exactly
    /// one row, with the right victim pid and detail — the ordinary record produces nothing.
    /// </summary>
    [Fact]
    public async Task ReadAsync_OverTheCsvRoute_ReturnsOneRowForOneDeadlockRecord()
    {
        var context = TestContext();
        context.PgLogUsesCsvlog = true;

        var body = OrdinaryFatalRecord + DeadlockRecord();
        using var reader = new FakeReader(new object?[][] { new object?[] { body, "UTC" } });

        var rows = await PgDeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(5012, row.VictimPid);
        Assert.Equal(2, row.ParticipantCount);
        Assert.Contains("balance", row.GraphText, StringComparison.Ordinal);
    }

    /// <summary>
    /// #4053 part b1: a record whose quoted message field carries a newline and a fake deadlock line stays
    /// one record — the fake line never becomes its own report, and no row carries its pid.
    /// </summary>
    [Fact]
    public async Task ReadAsync_OverTheCsvRoute_KeepsAForgedDeadlockLineInsideOneQuotedField()
    {
        var context = TestContext();
        context.PgLogUsesCsvlog = true;

        var entries = PgServerLogCsvParser.Parse(RecordWithForgedMessage(), out var discardedAtParse);
        var entry = Assert.Single(entries);
        Assert.Equal(0, discardedAtParse);

        using var reader = new FakeReader(new object?[][] { new object?[] { RecordWithForgedMessage(), "UTC" } });
        var rows = await PgDeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(rows);
        Assert.DoesNotContain(rows, r => r.VictimPid == 9999);
    }

    /// <summary>
    /// #4053 part b1: with the target's log_timezone at UTC, a non-zero-offset zone applies the same
    /// #4046 skip-and-count outcome the stderr route gives, never a refusal.
    /// </summary>
    [Fact]
    public async Task ReadAsync_OverTheCsvRoute_UnderAUtcLogTimezone_SkipsAndCountsAForeignZoneRecord()
    {
        var context = TestContext();
        context.PgLogUsesCsvlog = true;

        using var reader = new FakeReader(new object?[][] { new object?[] { DeadlockRecord("PST"), "UTC" } });
        var rows = await PgDeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(rows);
        var measured = Assert.Single(context.Measurements);
        Assert.Equal(PgServerLogTail.ForeignZoneLinesMeasurement, measured.Label);
        Assert.Equal(1, measured.Value);
    }

    /// <summary>
    /// #4053 part b1: with the target's log_timezone not UTC, a foreign-zone record refuses the whole read —
    /// the same outcome as the stderr route's, since the non-UTC candidate could be the server's own.
    /// </summary>
    [Fact]
    public async Task ReadAsync_OverTheCsvRoute_UnderANonUtcLogTimezone_ThrowsOnAForeignZoneRecord()
    {
        var context = TestContext();
        context.PgLogUsesCsvlog = true;

        using var reader = new FakeReader(new object?[][] { new object?[] { DeadlockRecord("PST"), "America/New_York" } });
        await Assert.ThrowsAsync<PgLogTimezoneUnsupportedException>(
            async () => await PgDeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None));
    }

    /// <summary>
    /// #4053 part b1: the csvlog route's own no-file-yet marker throws <see cref="PgNoCsvlogFileException"/>,
    /// never the stderr route's <see cref="PgNoStderrLogFileException"/>, and the shared collector-off
    /// marker still throws <see cref="PgLoggingCollectorOffException"/>.
    /// </summary>
    [Fact]
    public async Task ReadAsync_OverTheCsvRoute_ThrowsTheCsvNamedSkip_OnItsOwnMarker()
    {
        var context = TestContext();
        context.PgLogUsesCsvlog = true;

        using var reader = new FakeReader(new object?[][] { new object?[] { PgNoCsvlogFileException.Marker, null } });
        await Assert.ThrowsAsync<PgNoCsvlogFileException>(
            async () => await PgDeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None));

        using var offReader = new FakeReader(new object?[][] { new object?[] { PgLoggingCollectorOffException.Marker, null } });
        await Assert.ThrowsAsync<PgLoggingCollectorOffException>(
            async () => await PgDeadlocksCollector.Instance.ReadAsync(offReader, context, CancellationToken.None));
    }

    /// <summary>#4053 part b1: a record the parser discarded during resync or for a bad shape is measured
    /// on <see cref="PgLogEventsCollector.CsvRecordsDiscardedMeasurement"/> — the one label both csv-route
    /// collectors share.</summary>
    [Fact]
    public async Task ReadAsync_OverTheCsvRoute_MeasuresDiscardedRecords()
    {
        var context = TestContext();
        context.PgLogUsesCsvlog = true;

        using var reader = new FakeReader(new object?[][] { new object?[] { "not,a,valid,csvlog,record\n", "UTC" } });
        var rows = await PgDeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(rows);
        var measured = Assert.Single(context.Measurements);
        Assert.Equal(PgLogEventsCollector.CsvRecordsDiscardedMeasurement, measured.Label);
        Assert.Equal(1, measured.Value);
    }

    private static CollectorContext TestContext() => new()
    {
        ServerId = 1,
        ServerName = "target-a",
        CollectionTime = new DateTime(2026, 9, 24, 0, 0, 0, DateTimeKind.Unspecified),
        Deltas = new CollectorDeltaCalculator(),
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
    };

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
