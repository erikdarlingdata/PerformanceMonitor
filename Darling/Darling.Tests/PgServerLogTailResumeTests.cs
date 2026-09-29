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

        Assert.True(PgServerLogTail.TryConsumeResumeRow(row, fillColumnIsNull: true, mayAdvance: true, context));
        Assert.Equal("900|pg|log.log", context.PendingState[PgServerLogTail.ResumeStateKey]);
        var labels = context.Measurements.ToDictionary(m => m.Label, m => m.Value);
        Assert.Equal(2, labels[PgServerLogTail.FilesSkippedByRotationMeasurement]);
        Assert.Equal(5000, labels[PgServerLogTail.BytesSkippedMeasurement]);
        Assert.Equal(1, labels[PgServerLogTail.ResumeFileMissingMeasurement]);
    }

    [Fact]
    public void TryConsumeResumeRow_DoesNotAdvance_WhenNotAllowed_AndSkipsMalformed()
    {
        var context = Context();
        Assert.True(PgServerLogTail.TryConsumeResumeRow(PgServerLogTail.ResumeRowPrefix + "9|0|0||f.log", true, false, context));
        Assert.Empty(context.PendingState);

        Assert.True(PgServerLogTail.TryConsumeResumeRow(PgServerLogTail.ResumeRowPrefix + "junk", true, true, context));
        Assert.Empty(context.PendingState);
    }

    [Fact]
    public void TryConsumeResumeRow_LeavesAPlantedBodyAlone_WhenTheFillColumnIsNotNull()
    {
        var context = Context();
        Assert.False(PgServerLogTail.TryConsumeResumeRow(PgServerLogTail.ResumeRowPrefix + "1|0|0||f.log", fillColumnIsNull: false, mayAdvance: true, context));
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
        Assert.True(PgServerLogTail.TryConsumeResumeRow(PgServerLogTail.ResumeRowPrefix + "9|0|0||f.log", true, true, context));
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
}
