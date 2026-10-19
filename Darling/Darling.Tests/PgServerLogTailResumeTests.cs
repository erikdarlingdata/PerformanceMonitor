// Pins for the stderr log tail's resume marker (#4699): the state string, the resume row, the offset rule, and
// the order the runner persists the marker in.
using System;
using System.Linq;
using System.Text;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

public sealed class PgServerLogTailResumeTests
{
    private static CollectorContext Context(string? state = null) => new()
    {
        ServerId = 1,
        ServerName = "target-a",
        CollectionTime = new DateTime(2026, 9, 28, 1, 0, 0, DateTimeKind.Unspecified),
        Deltas = new CollectorDeltaCalculator(),
        LogHashKey = TestLogHashKeys.Fixed,
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
        State = state is null ? CollectorContext.NoState : new System.Collections.Generic.Dictionary<string, string> { [PgServerLogTail.ResumeStateKey] = state },
    };

    [Theory]
    [InlineData("123|postgresql-2026-09-28_000000.log", 123L, "postgresql-2026-09-28_000000.log")]
    [InlineData("0|a|b.log", 0L, "a|b.log")]
    public void TryParseResumeState_RoundTrips_IncludingABarInTheName(string value, long offset, string file)
    {
        Assert.True(PgServerLogTail.TryParseResumeState(value, out var f, out var o));
        Assert.Equal(file, f);
        Assert.Equal(offset, o);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("-5|x.log")]
    [InlineData("|x.log")]
    [InlineData("12|")]
    [InlineData("abc|x.log")]
    [InlineData("12")]
    public void TryParseResumeState_RejectsGarbage(string? value) =>
        Assert.False(PgServerLogTail.TryParseResumeState(value, out _, out _));

    [Fact]
    public void WithResume_BindsTheStateOrNulls()
    {
        var first = PgServerLogTail.WithResume("x", Context());
        Assert.Equal(new[] { "@log_resume_file", "@log_resume_offset" }, first.Parameters.Select(p => p.Name).ToArray());
        Assert.All(first.Parameters, p => Assert.Null(p.Value));

        var resumed = PgServerLogTail.WithResume("x", Context("77|f.log"));
        Assert.Equal("f.log", resumed.Parameters[0].Value);
        Assert.Equal(77L, resumed.Parameters[1].Value);
    }

    [Fact]
    public void TryConsumeResumeRow_StagesTheMarker_AndRecordsTheDisclosures()
    {
        var context = Context();
        var row = PgServerLogTail.ResumeRowPrefix + "900|2|5000|missing|pg|log.log";

        Assert.True(PgServerLogTail.TryConsumeResumeRow(row, fillColumnIsNull: true, PgServerLogTail.ResumeStateKey, context));
        Assert.Equal("900|pg|log.log", context.PendingState[PgServerLogTail.ResumeStateKey]);
        var labels = context.Measurements.ToDictionary(m => m.Label, m => m.Value);
        Assert.Equal(2, labels[PgServerLogTail.FilesSkippedByRotationMeasurement]);
        Assert.Equal(5000, labels[PgServerLogTail.BytesSkippedMeasurement]);
        Assert.Equal(1, labels[PgServerLogTail.ResumeFileMissingMeasurement]);
    }

    [Fact]
    public void TryConsumeResumeRow_SkipsAMalformedRow()
    {
        var context = Context();
        Assert.True(PgServerLogTail.TryConsumeResumeRow(PgServerLogTail.ResumeRowPrefix + "junk", true, PgServerLogTail.ResumeStateKey, context));
        Assert.Empty(context.PendingState);
    }

    [Fact]
    public void TryConsumeResumeRow_LeavesAPlantedBodyAlone_WhenTheFillColumnIsNotNull()
    {
        var context = Context();
        Assert.False(PgServerLogTail.TryConsumeResumeRow(PgServerLogTail.ResumeRowPrefix + "1|0|0||f.log", fillColumnIsNull: false, PgServerLogTail.ResumeStateKey, context));
        Assert.Empty(context.PendingState);
    }

    [Fact]
    public void NextResumeOffset_AlwaysLandsOnALineStart()
    {
        Assert.Equal(100, PgServerLogTail.NextResumeOffset(new byte[1000], 100));
        Assert.Equal(100, PgServerLogTail.NextResumeOffset(new byte[PgServerLogTail.ResumeOverlapBytes], 100));

        var noNewline = new byte[PgServerLogTail.ResumeOverlapBytes + 10];
        Array.Fill(noNewline, (byte)'a');
        Assert.Equal(100, PgServerLogTail.NextResumeOffset(noNewline, 100));

        var body = new byte[PgServerLogTail.ResumeOverlapBytes + 10];
        Array.Fill(body, (byte)'a');
        body[^4] = (byte)'\n';
        var next = PgServerLogTail.NextResumeOffset(body, 100);
        Assert.Equal(100 + body.Length - 3, next);
    }

    [Fact]
    public void TheRunner_PersistsTheMarker_OnlyAfterTheBatchWasWritten()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");
        var write = source.IndexOf("rowsWritten = await WriteBatchAsync(pgConnection, definition, rows", StringComparison.Ordinal);
        var save = source.IndexOf("SaveCollectorStateAsync(server.ServerId, definition.Name, context.PendingState", StringComparison.Ordinal);

        Assert.True(write > 0, "the COPY call moved or was renamed");
        Assert.True(save > write, "the marker must be saved after the rows are written, so a failed write re-reads");
    }

    [Fact]
    public void ADefinitionThatThrowsAfterStaging_LeavesTheMarkerToTheRunnersOrder()
    {
        /* The runner saves PendingState only after the write returns, and a read that throws never reaches
           that block. The staged marker exists only in PendingState until then. */
        var context = Context("5|f.log");
        Assert.True(PgServerLogTail.TryConsumeResumeRow(PgServerLogTail.ResumeRowPrefix + "9|0|0||f.log", true, PgServerLogTail.ResumeStateKey, context));
        Assert.Equal("5|f.log", context.State[PgServerLogTail.ResumeStateKey]);
        Assert.Equal("9|f.log", context.PendingState[PgServerLogTail.ResumeStateKey]);
    }

    [Fact]
    public void BothStderrTailTexts_CarryTheResumeParameters()
    {
        foreach (var sql in new[] { PgServerLogTail.TailCteSql, PgServerLogTail.TailCteBinarySql })
        {
            Assert.Contains("@log_resume_file", sql, StringComparison.Ordinal);
            Assert.Contains("@log_resume_offset", sql, StringComparison.Ordinal);
        }

        Assert.Equal(Encoding.UTF8.GetByteCount(PgServerLogTail.ResumeRowPrefix), PgServerLogTail.ResumeRowPrefix.Length);
    }

    private static string Lf(string sql) => sql.Replace("\r\n", "\n", StringComparison.Ordinal);

    [Theory]
    [InlineData("csv", "csvlog", false)]
    [InlineData("csv", "csvlog", true)]
    [InlineData("json", "jsonlog", false)]
    [InlineData("json", "jsonlog", true)]
    public void TheCsvAndJsonTwins_AreTheStderrTwinsWithOnlyTheFormatTestAndNameFilterSwapped(string ext, string format, bool binary)
    {
        var stderr = Lf(binary ? PgServerLogTail.TailCteBinarySql : PgServerLogTail.TailCteSql);
        var twin = Lf((ext, binary) switch
        {
            ("csv", false) => PgServerLogTail.TailCsvCteSql,
            ("csv", true) => PgServerLogTail.TailCsvCteBinarySql,
            (_, false) => PgServerLogTail.TailJsonCteSql,
            _ => PgServerLogTail.TailJsonCteBinarySql,
        });

        Assert.Equal(
            stderr
                .Replace("AND name !~* '\\.(csv|json)$'", "AND name ~* '\\." + ext + "$'", StringComparison.Ordinal)
                .Replace("AND 'stderr' = ANY", "AND '" + format + "' = ANY", StringComparison.Ordinal),
            twin);
    }

    private static CollectorContext FormatContext(bool json, System.Collections.Generic.Dictionary<string, string>? state = null) => new()
    {
        ServerId = 1,
        ServerName = "t",
        CollectionTime = DateTime.UtcNow,
        Deltas = new CollectorDeltaCalculator(),
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
        PgLogUsesCsvlog = true,
        PgLogUsesJsonlog = json,
        State = state is null ? CollectorContext.NoState : state,
    };

    [Fact]
    public void EachFormat_KeepsItsOwnMarkerKey()
    {
        Assert.Equal(new[] { "log_resume", "log_resume_csv", "log_resume_json" }, PgServerLogTail.ResumeStateKeys);

        /* A stderr marker on a csv route binds NULL parameters: no marked file, so no "missing" disclosure. */
        var csv = FormatContext(false, new() { [PgServerLogTail.ResumeStateKey] = "5|f.log" });
        var bound = PgServerLogTail.WithResume(PgServerLogTail.TailCsvCteSql, csv, PgServerLogTail.ResumeStateKeyCsv);
        /* #4735: the text tails also bind the read shift, 0 for an ordinary read, which is not a resume value. */
        Assert.All(bound.Parameters.Where(p => p.Name != PgServerLogTail.ReadShiftParameter), p => Assert.Null(p.Value));

        csv = FormatContext(false, new() { [PgServerLogTail.ResumeStateKeyCsv] = "5|f.csv" });
        bound = PgServerLogTail.WithResume(PgServerLogTail.TailCsvCteSql, csv, PgServerLogTail.ResumeStateKeyCsv);
        Assert.Contains(bound.Parameters, p => Equals(p.Value, "f.csv"));

        var staged = FormatContext(false);
        Assert.True(PgServerLogTail.TryConsumeResumeRow("pm-log-resume|9|0|0||f.csv", true, PgServerLogTail.ResumeStateKeyCsv, staged));
        Assert.Equal("9|f.csv", staged.PendingState[PgServerLogTail.ResumeStateKeyCsv]);
        Assert.False(staged.PendingState.ContainsKey(PgServerLogTail.ResumeStateKey));
    }

    [Fact]
    public void EveryCsvAndJsonTailTwin_CarriesTheResumeParameters()
    {
        foreach (var sql in new[] { PgServerLogTail.TailCsvCteSql, PgServerLogTail.TailCsvCteBinarySql, PgServerLogTail.TailJsonCteSql, PgServerLogTail.TailJsonCteBinarySql })
        {
            Assert.Contains("@log_resume_file", sql, StringComparison.Ordinal);
            Assert.Contains("@log_resume_offset", sql, StringComparison.Ordinal);
            Assert.Contains("resume AS (", sql, StringComparison.Ordinal);
        }
    }
}
