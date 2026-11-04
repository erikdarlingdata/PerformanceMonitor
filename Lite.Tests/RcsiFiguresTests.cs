using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>#7448: the DB_CONFIG headline does not claim a drift when RCSI off is the only item.</summary>
public sealed class DbConfigHeadlineTests
{
    private static AdviceBlock Compose(params (string Key, double Value)[] meta)
    {
        var fact = new Fact { Key = "DB_CONFIG", Value = 1 };
        foreach (var (key, value) in meta) fact.Metadata[key] = value;
        return FactAdvice.Compose("DB_CONFIG", new System.Collections.Generic.Dictionary<string, Fact> { ["DB_CONFIG"] = fact })!;
    }

    [Fact]
    public void RcsiOffAlone_SaysWhatWasFound_WithoutDrifted()
    {
        var advice = Compose(("rcsi_off_count", 5), ("database_count", 5), ("query_store_on_count", 5));

        Assert.Equal("RCSI is off on 5 databases on this server", advice.Headline);
        Assert.DoesNotContain("drifted", advice.Headline + advice.Investigation + advice.Remediation);
    }

    [Fact]
    public void RcsiOffAlone_RemediationNamesNoSettingThatDidNotFire()
    {
        var advice = Compose(("rcsi_off_count", 5), ("database_count", 5), ("query_store_on_count", 5));

        Assert.Contains("RCSI", advice.Remediation);
        Assert.DoesNotContain("AUTO_SHRINK", advice.Remediation);
        Assert.DoesNotContain("AUTO_CLOSE", advice.Remediation);
        Assert.DoesNotContain("page verify", advice.Remediation);
    }

    [Fact]
    public void ADriftedSetting_IsNamedInTheRemediation_AndOnesThatDidNotFireAreNot()
    {
        var advice = Compose(("rcsi_off_count", 5), ("auto_shrink_on_count", 1), ("database_count", 5), ("query_store_on_count", 5));

        Assert.Contains("AUTO_SHRINK off", advice.Remediation);
        Assert.Contains("RCSI on only where", advice.Remediation);
        Assert.DoesNotContain("AUTO_CLOSE", advice.Remediation);
        Assert.DoesNotContain("page verify", advice.Remediation);
    }

    [Fact]
    public void RcsiOffBesideAnotherMovedSetting_KeepsTheDriftWording()
    {
        var advice = Compose(("rcsi_off_count", 5), ("auto_shrink_on_count", 1), ("database_count", 5), ("query_store_on_count", 5));

        Assert.Contains("drifted", advice.Headline);
    }
}

/// <summary>
/// #7448 on a seeded DuckDB store, the twin of Darling's <c>RcsiFiguresLiveTests</c>: the DB_CONFIG drill-down writes
/// real RCSI figures per database instead of constants, and the shared target gate
/// (<see cref="FactRemediation.CollectRcsiTargets"/>) can now keep a database. Four RCSI-off databases plus tempdb
/// and master: <c>ReadDb</c> (reader-blocked-by-writer waits and deadlocks), <c>WriteDb</c> (writer/writer waits
/// only), <c>QuietDb</c> (no blocking at all) and <c>OutDb</c> (reader waits OUTSIDE the analysis window).
/// </summary>
public sealed class RcsiFiguresTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = -744_801;
    private static long s_id = -74_480_000;

    private readonly DuckDbInitializer _duckDb;

    public RcsiFiguresTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private static async Task ExecAsync(DuckDBConnection connection, string sql, params object[] values)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values) cmd.Parameters.Add(new DuckDBParameter { Value = v });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>One blocked-process report per wait: each gets its own blocked spid, so each is its own wait.</summary>
    private static async Task SeedWaitsAsync(DuckDBConnection c, DateTime at, string db, string lockMode, int count, int firstSpid)
    {
        for (var i = 0; i < count; i++)
        {
            var when = at.AddMinutes(i);
            await ExecAsync(c, @"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
     blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_status, blocking_status)
VALUES ($1, $2, $3, 'rcsi', $2, $4, $5, 900, 5000, $6, 'suspended', 'running')",
                Interlocked.Decrement(ref s_id), when, ServerId, db, firstSpid + i, lockMode);
        }
    }

    private static Task SeedDeadlockAsync(DuckDBConnection c, DateTime at, string db) =>
        ExecAsync(c, @"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name)
VALUES ($1, $2, $3, 'rcsi', $2, $4, $5)",
            Interlocked.Decrement(ref s_id), at, ServerId, $"<deadlock><victim-list/><process-list>{at:O}</process-list></deadlock>", db);

    private static JsonElement Row(AnalysisFinding finding, string database) =>
        JsonSerializer.SerializeToElement(finding.DrillDown!["config_issues"]).EnumerateArray()
            .Single(r => r.GetProperty("database").GetString() == database);

    [Fact]
    public async Task RcsiOffDatabases_GetMeasuredFigures_AndOnlyReaderWriterBlockingMakesATarget()
    {
        var end = DateTime.UtcNow;
        end = new DateTime(end.Ticks - end.Ticks % TimeSpan.TicksPerSecond, DateTimeKind.Unspecified);

        using (var readLock = _duckDb.AcquireReadLock())
        using (var c = _duckDb.CreateConnection())
        {
            await c.OpenAsync(TestContext.Current.CancellationToken);
            foreach (var db in new[] { "ReadDb", "WriteDb", "QuietDb", "OutDb", "tempdb", "master" })
                await LatestValueSeed.InsertDatabaseConfigAsync(c, ServerId, end.AddHours(-1), db, autoShrink: false, rcsiOn: false);

            /* ReadDb: three shared-lock waits and one exclusive wait (75 percent readers), and two deadlocks. */
            await SeedWaitsAsync(c, end.AddHours(-2), "ReadDb", "S", 3, 100);
            await SeedWaitsAsync(c, end.AddHours(-2).AddMinutes(30), "readdb", "X", 1, 200);
            await SeedDeadlockAsync(c, end.AddHours(-2), "ReadDb");
            await SeedDeadlockAsync(c, end.AddHours(-1), "ReadDb");
            /* WriteDb: only writer/writer waits, and schema locks that stay out of the reader/writer share. */
            await SeedWaitsAsync(c, end.AddHours(-2), "WriteDb", "X", 3, 300);
            await SeedWaitsAsync(c, end.AddHours(-2).AddMinutes(30), "WriteDb", "Sch-M", 2, 400);
            /* OutDb: reader waits a day before the window, which the figures must not count. */
            await SeedWaitsAsync(c, end.AddHours(-30), "OutDb", "S", 4, 500);
            /* tempdb carries reader waits too: it still must not appear. */
            await SeedWaitsAsync(c, end.AddHours(-2), "tempdb", "S", 3, 600);
        }

        var context = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = "rcsi",
            TimeRangeStart = end.AddHours(-4),
            TimeRangeEnd = end,
        };
        var finding = new AnalysisFinding
        {
            ServerId = ServerId, RootFactKey = "DB_CONFIG", StoryPath = "DB_CONFIG", PathKeys = ["DB_CONFIG"], Severity = 0.3,
        };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], context);

        var names = JsonSerializer.SerializeToElement(finding.DrillDown!["config_issues"]).EnumerateArray()
            .Select(r => r.GetProperty("database").GetString()).Order().ToArray();
        Assert.Equal(["OutDb", "QuietDb", "ReadDb", "WriteDb"], names);

        var read = Row(finding, "ReadDb");
        Assert.Equal(4, read.GetProperty("rcsi_blocking_events").GetInt32());
        Assert.Equal(2, read.GetProperty("rcsi_deadlocks").GetInt32());
        Assert.Equal(75, read.GetProperty("rcsi_reader_writer_pct").GetInt32());

        var write = Row(finding, "WriteDb");
        Assert.Equal(5, write.GetProperty("rcsi_blocking_events").GetInt32());
        Assert.Equal(0, write.GetProperty("rcsi_deadlocks").GetInt32());
        Assert.Equal(0, write.GetProperty("rcsi_reader_writer_pct").GetInt32());

        foreach (var quiet in new[] { "QuietDb", "OutDb" })
        {
            var row = Row(finding, quiet);
            Assert.Equal(0, row.GetProperty("rcsi_blocking_events").GetInt32());
            Assert.Equal(0, row.GetProperty("rcsi_deadlocks").GetInt32());
            Assert.Equal(0, row.GetProperty("rcsi_reader_writer_pct").GetInt32());
        }

        var target = Assert.Single(FactRemediation.CollectRcsiTargets(finding));
        Assert.Equal("ReadDb", target.Database);
        Assert.Equal(4, target.Figures.BlockingEvents);
        Assert.Equal(2, target.Figures.Deadlocks);
        Assert.Equal(75, target.Figures.ReaderWriterPct);
        Assert.NotNull(FactRemediation.BuildRcsiAction(finding));
    }
}
