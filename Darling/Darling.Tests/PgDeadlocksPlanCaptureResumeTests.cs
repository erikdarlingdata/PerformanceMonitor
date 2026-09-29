using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4699: <c>pg_deadlocks</c> and <c>pg_plan_capture</c> consume the stderr log tail's resume row. The marker is
/// always staged (a row-capped read discloses the cut instead of holding the marker), and only for a row whose fill column is NULL.
/// </summary>
public sealed class PgDeadlocksPlanCaptureResumeTests
{
    private const string ResumeRow = "pm-log-resume|4096|0|0||postgresql-2026-09-28_000000.log";
    private const string Expected = "4096|postgresql-2026-09-28_000000.log";

    private static CollectorContext Context(bool binary = false, IReadOnlyDictionary<string, string>? state = null) => new()
    {
        ServerId = 1,
        ServerName = "target-a",
        CollectionTime = new DateTime(2026, 9, 28, 0, 0, 0, DateTimeKind.Unspecified),
        Deltas = new CollectorDeltaCalculator(),
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
        PgReadBinaryFileGranted = binary,
        LogHashKey = TestLogHashKeys.Fixed,
        State = state ?? CollectorContext.NoState,
    };

    private static object?[] DeadlockFiller() => new object?[] { "not a deadlock report", "UTC" };

    private static object?[] PlanFiller() => new object?[] { 1L, 1.0, "not a plan", "%m [%p] " };

    private static object?[][] Rows(object?[] resume, Func<object?[]> filler, int fillers)
    {
        var rows = new List<object?[]> { resume };
        for (var i = 0; i < fillers; i++)
        {
            rows.Add(filler());
        }

        return rows.ToArray();
    }

    [Fact]
    public void BothCollectors_DeclareTheResumeStateKey()
    {
        Assert.Equal(PgServerLogTail.ResumeStateKeys, PgDeadlocksCollector.Instance.StateKeys);
        Assert.Equal(PgServerLogTail.ResumeStateKeys, PgPlanCaptureCollector.Instance.StateKeys);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void BothCollectors_SendTheResumeRowFirst_OnBothRoutes(bool binary)
    {
        var context = Context(binary);
        foreach (var text in new[] { PgDeadlocksCollector.Instance.BuildQuery(context).Text, PgPlanCaptureCollector.Instance.BuildQuery(context).Text })
        {
            var firstSelect = text.IndexOf("\nSELECT", text.IndexOf("resume AS (", StringComparison.Ordinal), StringComparison.Ordinal);
            Assert.Contains("pm-log-resume|", text[firstSelect..(firstSelect + 400)], StringComparison.Ordinal);
            Assert.Contains("FROM resume AS r", text[firstSelect..(firstSelect + 600)], StringComparison.Ordinal);
            Assert.Contains("LIMIT ", text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task Deadlocks_ARowLimitedRead_StagesTheMarker_AndDisclosesTheCut()
    {
        var context = Context();
        using var reader = new ListReader(Rows(new object?[] { ResumeRow, null }, DeadlockFiller, 501));
        await PgDeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        Assert.Equal(Expected, context.PendingState[PgServerLogTail.ResumeStateKey]);
        Assert.Contains(context.Measurements, m => m.Label == PgServerLogTail.MatchesLimitedMeasurement && m.Value == 1);
    }

    [Fact]
    public async Task Deadlocks_AnUnlimitedRead_StagesTheMarker_WithNoDisclosure()
    {
        var context = Context();
        using var reader = new ListReader(Rows(new object?[] { ResumeRow, null }, DeadlockFiller, 500));
        await PgDeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        Assert.Equal(Expected, context.PendingState[PgServerLogTail.ResumeStateKey]);
        Assert.DoesNotContain(context.Measurements, m => m.Label == PgServerLogTail.MatchesLimitedMeasurement);
    }

    [Fact]
    public async Task Deadlocks_AReportThatStartsWithThePrefix_WithAFilledTimezone_IsNotConsumed()
    {
        var context = Context();
        using var reader = new ListReader(new[] { new object?[] { ResumeRow, "UTC" } });
        await PgDeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        Assert.False(context.PendingState.ContainsKey(PgServerLogTail.ResumeStateKey));
    }

    [Fact]
    public async Task Plans_ARowLimitedRead_StagesTheMarker_AndDisclosesTheCut()
    {
        var context = Context();
        using var reader = new ListReader(Rows(new object?[] { null, null, ResumeRow, null }, PlanFiller, 2001));
        await PgPlanCaptureCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        Assert.Equal(Expected, context.PendingState[PgServerLogTail.ResumeStateKey]);
        Assert.Contains(context.Measurements, m => m.Label == PgServerLogTail.MatchesLimitedMeasurement && m.Value == 1);
    }

    [Fact]
    public async Task Plans_AnUnlimitedRead_StagesTheMarker_WithNoDisclosure()
    {
        var context = Context();
        using var reader = new ListReader(Rows(new object?[] { null, null, ResumeRow, null }, PlanFiller, 2000));
        await PgPlanCaptureCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        Assert.Equal(Expected, context.PendingState[PgServerLogTail.ResumeStateKey]);
        Assert.DoesNotContain(context.Measurements, m => m.Label == PgServerLogTail.MatchesLimitedMeasurement);
    }

    [Fact]
    public async Task Plans_APlanThatStartsWithThePrefix_WithAFilledLinePrefix_IsNotConsumed()
    {
        var context = Context();
        using var reader = new ListReader(new[] { new object?[] { 1L, 1.0, ResumeRow, "%m [%p] " } });
        await PgPlanCaptureCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        Assert.False(context.PendingState.ContainsKey(PgServerLogTail.ResumeStateKey));
    }

    [Fact]
    public async Task Plans_AMarkerRowStillThrows_AfterTheResumeRowWasConsumed()
    {
        var context = Context();
        using var reader = new ListReader(new[]
        {
            new object?[] { null, null, ResumeRow, null },
            new object?[] { null, null, PgLoggingCollectorOffException.Marker, null },
        });
        await Assert.ThrowsAsync<PgLoggingCollectorOffException>(
            async () => await PgPlanCaptureCollector.Instance.ReadAsync(reader, context, CancellationToken.None));
    }

    [Fact]
    public async Task Deadlocks_TheCsvRoute_StagesTheCsvKey_AndNeverTheStderrKey()
    {
        var context = Context();
        context.PgLogUsesCsvlog = true;
        using var reader = new ListReader(new[] { new object?[] { ResumeRow, null } });
        await PgDeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        Assert.Equal(Expected, context.PendingState[PgServerLogTail.ResumeStateKeyCsv]);
        Assert.False(context.PendingState.ContainsKey(PgServerLogTail.ResumeStateKey));
    }

    [Fact]
    public async Task Plans_TheCsvRoute_StagesTheCsvKey_AndNeverTheStderrKey()
    {
        var context = Context();
        context.PgLogUsesCsvlog = true;
        using var reader = new ListReader(new[] { new object?[] { ResumeRow, null } });
        await PgPlanCaptureCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        Assert.Equal(Expected, context.PendingState[PgServerLogTail.ResumeStateKeyCsv]);
        Assert.False(context.PendingState.ContainsKey(PgServerLogTail.ResumeStateKey));
    }

    [Theory]
    [InlineData(false, false, "log_resume")]
    [InlineData(true, false, "log_resume_csv")]
    [InlineData(false, true, "log_resume_json")]
    public async Task LogEvents_EveryRoute_StagesItsOwnKeyOnly(bool csv, bool json, string key)
    {
        var context = Context();
        context.PgLogUsesCsvlog = csv;
        context.PgLogUsesJsonlog = json;
        using var reader = new ListReader(new[] { new object?[] { ResumeRow, null } });
        await PgLogEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        Assert.Equal(Expected, context.PendingState[key]);
        Assert.Single(context.PendingState);
    }

    /// <summary>
    /// The key BuildQuery binds its parameters from EQUALS the key the read stages under, on every route, including a
    /// target whose jsonlog flag is set while the collector reads stderr or csvlog.
    /// </summary>
    [Theory]
    [InlineData(false, false, "log_resume")]
    [InlineData(true, false, "log_resume_csv")]
    [InlineData(false, true, "log_resume")]
    [InlineData(true, true, "log_resume_csv")]
    public async Task DeadlocksAndPlans_TheBoundKeyIsTheStagedKey_EvenWithTheJsonFlagSet(bool csv, bool json, string key)
    {
        foreach (var isPlan in new[] { false, true })
        {
            var state = new Dictionary<string, string> { [key] = "77|marked.log" };
            var context = Context(state: state);
            context.PgLogUsesCsvlog = csv;
            context.PgLogUsesJsonlog = json;

            var query = isPlan ? PgPlanCaptureCollector.Instance.BuildQuery(context) : PgDeadlocksCollector.Instance.BuildQuery(context);
            Assert.Contains(query.Parameters, p => Equals(p.Value, "marked.log"));

            var row = isPlan && !csv ? new object?[] { null, null, ResumeRow, null } : new object?[] { ResumeRow, null };
            using var reader = new ListReader(new[] { row });
            if (isPlan)
            {
                await PgPlanCaptureCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
            }
            else
            {
                await PgDeadlocksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
            }

            Assert.Equal(Expected, context.PendingState[key]);
            Assert.Single(context.PendingState);
        }
    }

    [Theory]
    [InlineData(false, false, "log_resume")]
    [InlineData(true, false, "log_resume_csv")]
    [InlineData(false, true, "log_resume_json")]
    [InlineData(true, true, "log_resume_json")]
    public async Task LogEvents_TheBoundKeyIsTheStagedKey_OnEveryRoute(bool csv, bool json, string key)
    {
        var context = Context(state: new Dictionary<string, string> { [key] = "77|marked.log" });
        context.PgLogUsesCsvlog = csv;
        context.PgLogUsesJsonlog = json;

        Assert.Contains(PgLogEventsCollector.Instance.BuildQuery(context).Parameters, p => Equals(p.Value, "marked.log"));

        using var reader = new ListReader(new[] { new object?[] { ResumeRow, null } });
        await PgLogEventsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        Assert.Equal(Expected, context.PendingState[key]);
        Assert.Single(context.PendingState);
    }

    private sealed class ListReader : System.Data.Common.DbDataReader
    {
        private readonly object?[][] _rows;
        private int _index = -1;

        public ListReader(object?[][] rows) => _rows = rows;

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
        public override string GetName(int ordinal) => "col";
        public override int GetOrdinal(string name) => 0;
        public override int GetValues(object[] values) => throw new NotSupportedException();
        public override bool NextResult() => false;
    }
}
