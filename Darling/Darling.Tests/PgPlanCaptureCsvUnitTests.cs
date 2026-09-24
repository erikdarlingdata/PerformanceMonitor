/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="PgPlanCaptureCollector"/>'s csvlog branch (#4053 part b2), over a hand-built body carrying one
/// real auto_explain record captured on a live pg18 rig — the record's shape is verbatim what the rig wrote,
/// only the identifying <c>Relation Name</c> and query id are this test's own. No SQL runs here: these
/// exercise <see cref="PgPlanCaptureCollector.ReadAsync"/> directly, the way
/// <see cref="PgPlanCaptureCollectorDefinitionTests"/> and its Lite.Tests sibling exercise the stderr branch,
/// through a fake reader over the csv statement's two columns (log_body, log_timezone).
/// </summary>
public sealed class PgPlanCaptureCsvUnitTests
{
    /* One real auto_explain csvlog record, verified live: csvlog puts severity in its own column, never
       glued onto Message the way the stderr route's log_line_prefix glues "LOG:  " on, so the message
       field itself reads "duration: N ms  plan:\n{json}". query_id (PG14+) is the last of the 26 columns —
       -3560200806914842915, this test's own choice, distinct from the query id inside the JSON body so a
       test that read the wrong source would fail rather than pass by coincidence. */
    private const string RealPlanRecord =
        "2026-09-24 04:29:03.817 UTC,\"postgres\",\"postgres\",352,\"[local]\",6ab4a70f.160,1,\"SELECT\","
        + "2026-09-24 04:29:03 UTC,6/2,781,LOG,00000,\"duration: 0.020 ms  plan:\n"
        + "{\n"
        + "  \"\"Plan\"\": {\n"
        + "    \"\"Node Type\"\": \"\"Seq Scan\"\",\n"
        + "    \"\"Relation Name\"\": \"\"plan_capture_csv_b2\"\",\n"
        + "    \"\"Filter\"\": \"\"(id > 5)\"\"\n"
        + "  }\n"
        + "}\",,,,,,,,,\"psql\",\"client backend\",,-3560200806914842915\n";

    /* A record whose quoted user field carries a newline PLUS a fake plan-shaped line (#4053's own proof
       shape, adapted from PgServerLogCsvParserTests) — under csvlog the newline and everything after it
       stays inside the ONE quoted user_name field, so this is a single FATAL-severity record whose Message
       does not start with the plan marker, never a second plan record with its own Message. */
    private static readonly string RecordWithForgedPlanInUserName =
        "2026-09-24 04:29:03.008 UTC,\"nosuchuser\n"
        + "2026-09-24 04:29:03.000 UTC,,,1,,1.1,1,,2026-09-24 04:29:03 UTC,,0,LOG,00000,"
        + "\"\"duration: 1.0 ms  plan:\n{\"\"Plan\"\": {\"\"Relation Name\"\": \"\"FORGED\"\"}}\"\"\","
        + "\"postgres\",83,\"[local]\",6ab482e3.53,1,\"startup\",2026-09-24 04:29:03 UTC,3/3,0,FATAL,28000,"
        + "\"role \"\"nosuchuser\"\" does not exist\",,,,,,,,,\"\",\"client backend\",,0\n";

    /* The cut head every tail read carries: the end of a record this window did not see the start of. */
    private const string CutHead = "0,FATAL,28000,,,,,,,,,,\"client backend\",,0\n";

    private static async Task<(List<PgPlanCaptureCollector.Row> Rows, CollectorContext Context)> ReadAsync(string body)
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "csv-plan-capture-unit",
            CollectionTime = new DateTime(2026, 9, 24, 4, 30, 0, DateTimeKind.Utc),
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
            PgLogUsesCsvlog = true,
        };

        using var reader = new FakeCsvBodyReader(new object?[][] { new object?[] { body, "UTC" } });
        var rows = await PgPlanCaptureCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        return (rows, context);
    }

    [Fact]
    public async Task ARealCsvRecord_YieldsOneRowWithTheRightQueryIdAndDuration()
    {
        var (rows, _) = await ReadAsync(CutHead + RealPlanRecord);

        var row = Assert.Single(rows);
        Assert.Equal(-3560200806914842915, row.QueryId);
        Assert.Equal(0.020, row.DurationMs, 3);
        Assert.Equal("Seq Scan", row.TopNodeType);
        Assert.Contains("plan_capture_csv_b2", row.PlanJson, StringComparison.Ordinal);

        /* Redacted: the Filter's literal condition value must not survive. */
        Assert.DoesNotContain("(id > 5)", row.PlanJson, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AQuotedUserFieldCarryingAForgedPlanLine_YieldsNoExtraRow()
    {
        var (rows, _) = await ReadAsync(CutHead + RecordWithForgedPlanInUserName + RealPlanRecord);

        /* Only the one real record's plan comes back; the forged text stayed inside the FATAL record's
           quoted user-name field and never started a plan capture of its own. */
        var row = Assert.Single(rows);
        Assert.Equal(-3560200806914842915, row.QueryId);
        Assert.DoesNotContain(rows, r => r.PlanJson.Contains("FORGED", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ADurationShapedOneDotTwoDotThree_IsSkippedAndCounted()
    {
        var forgedDuration = CutHead + RealPlanRecord.Replace(
            "duration: 0.020 ms  plan:",
            "duration: 1.2.3 ms  plan:",
            StringComparison.Ordinal);

        var (rows, context) = await ReadAsync(forgedDuration);

        Assert.Empty(rows);
        var forgedMeasurement = context.Measurements.Single(m => m.Label == PgPlanCaptureCollector.ForgedCaptureMeasurement);
        Assert.Equal(1, forgedMeasurement.Value);
    }

    /// <summary>Review round 1: a genuine log_min_duration_statement record starts with the same "duration: "
    /// text but carries no plan. It is not a capture and not forged, so it yields no row and is NOT counted as
    /// forged.</summary>
    [Fact]
    public async Task ADurationRecordWithNoPlan_IsSkippedWithoutCountingItForged()
    {
        var durationOnly = CutHead + RealPlanRecord[..RealPlanRecord.IndexOf("duration: ", StringComparison.Ordinal)]
            + "duration: 12.345 ms\",,,,,,,,,\"psql\",\"client backend\",,-3560200806914842915\n";

        var (rows, context) = await ReadAsync(durationOnly);

        Assert.Empty(rows);
        Assert.DoesNotContain(context.Measurements, m => m.Label == PgPlanCaptureCollector.ForgedCaptureMeasurement);
    }

    /// <summary>Review round 1: auto_explain writes at LOG, and the stderr route requires the LOG label. A record
    /// at another severity (a client's RAISE NOTICE, say) whose message starts with the marker is skipped, just
    /// as the stderr route skips it.</summary>
    [Fact]
    public async Task APlanShapedRecordAtNoticeSeverity_YieldsNoRow()
    {
        var notice = CutHead + RealPlanRecord.Replace(",6/2,781,LOG,00000,", ",6/2,781,NOTICE,00000,", StringComparison.Ordinal);
        Assert.NotEqual(CutHead + RealPlanRecord, notice);

        var (rows, _) = await ReadAsync(notice);

        Assert.Empty(rows);
    }

    [Fact]
    public async Task TheNoCsvlogFileMarker_ThrowsTheCsvlogNamedSkip()
    {
        using var reader = new FakeCsvBodyReader(new object?[][] { new object?[] { PgNoCsvlogFileException.Marker, null } });
        var context = new CollectorContext
        {
            ServerId = 1, ServerName = "s",
            CollectionTime = DateTime.UtcNow,
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
            PgLogUsesCsvlog = true,
        };

        await Assert.ThrowsAsync<PgNoCsvlogFileException>(
            () => PgPlanCaptureCollector.Instance.ReadAsync(reader, context, CancellationToken.None).AsTask());
    }

    /// <summary>
    /// A minimal fake reader over the csv statement's two columns (log_body, log_timezone) — the shape
    /// <see cref="PgPlanCaptureCollector.CsvQueryText"/> and <see cref="PgLogEventsCollector"/>'s own csv
    /// statement both select.
    /// </summary>
    private sealed class FakeCsvBodyReader : DbDataReader
    {
        private readonly object?[][] _rows;
        private int _index = -1;

        public FakeCsvBodyReader(object?[][] rows) => _rows = rows;

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
        public override bool GetBoolean(int ordinal) => throw new NotSupportedException();
        public override byte GetByte(int ordinal) => throw new NotSupportedException();
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override char GetChar(int ordinal) => throw new NotSupportedException();
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override string GetDataTypeName(int ordinal) => "text";
        public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();
        public override decimal GetDecimal(int ordinal) => throw new NotSupportedException();
        public override double GetDouble(int ordinal) => throw new NotSupportedException();
        public override System.Collections.IEnumerator GetEnumerator() => _rows.GetEnumerator();
        public override Type GetFieldType(int ordinal) => typeof(string);
        public override float GetFloat(int ordinal) => throw new NotSupportedException();
        public override Guid GetGuid(int ordinal) => throw new NotSupportedException();
        public override short GetInt16(int ordinal) => throw new NotSupportedException();
        public override int GetInt32(int ordinal) => throw new NotSupportedException();
        public override long GetInt64(int ordinal) => throw new NotSupportedException();
        public override string GetName(int ordinal) => ordinal == 0 ? "log_body" : "log_timezone";
        public override int GetOrdinal(string name) => name == "log_body" ? 0 : 1;
        public override int GetValues(object[] values) => throw new NotSupportedException();
        public override bool NextResult() => false;
    }
}
