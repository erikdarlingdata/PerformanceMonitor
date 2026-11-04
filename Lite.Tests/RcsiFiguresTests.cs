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
            foreach (var db in new[] { "ReadDb", "WriteDb", "QuietDb", "OutDb", "SchemaDb", "DeadlockDb", "OneWaitDb", "tempdb", "master" })
                await LatestValueSeed.InsertDatabaseConfigAsync(c, ServerId, end.AddHours(-1), db, autoShrink: false, rcsiOn: false);

            /* ReadDb: nine shared-lock waits and three exclusive waits (75 percent readers), and two deadlocks. */
            await SeedWaitsAsync(c, end.AddHours(-2), "ReadDb", "S", 9, 100);
            await SeedWaitsAsync(c, end.AddHours(-2).AddMinutes(30), "readdb", "X", 3, 200);
            await SeedDeadlockAsync(c, end.AddHours(-2), "ReadDb");
            await SeedDeadlockAsync(c, end.AddHours(-1), "ReadDb");
            /* WriteDb: only writer/writer waits, and schema locks that stay out of the reader/writer share. */
            await SeedWaitsAsync(c, end.AddHours(-2), "WriteDb", "X", 10, 300);
            await SeedWaitsAsync(c, end.AddHours(-2).AddMinutes(30), "WriteDb", "Sch-M", 2, 400);
            /* OutDb: reader waits a day before the window, which the figures must not count. */
            await SeedWaitsAsync(c, end.AddHours(-30), "OutDb", "S", 4, 500);
            /* SchemaDb: only schema locks, so there are events but no reader or writer wait to take a share of. */
            await SeedWaitsAsync(c, end.AddHours(-2), "SchemaDb", "Sch-M", 2, 700);
            /* OneWaitDb: a single shared-lock wait, which is too little to judge and must not make a target. */
            await SeedWaitsAsync(c, end.AddHours(-2), "OneWaitDb", "S", 1, 800);
            /* DeadlockDb: a deadlock and no blocked-process wait at all. */
            await SeedDeadlockAsync(c, end.AddHours(-2), "DeadlockDb");
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
        Assert.Equal(["DeadlockDb", "OneWaitDb", "OutDb", "QuietDb", "ReadDb", "SchemaDb", "WriteDb"], names);

        var read = Row(finding, "ReadDb");
        Assert.Equal(12, read.GetProperty("rcsi_blocking_events").GetInt32());
        Assert.Equal(2, read.GetProperty("rcsi_deadlocks").GetInt32());
        Assert.Equal(75, read.GetProperty("rcsi_reader_writer_pct").GetInt32());

        var write = Row(finding, "WriteDb");
        Assert.Equal(12, write.GetProperty("rcsi_blocking_events").GetInt32());
        Assert.Equal(0, write.GetProperty("rcsi_deadlocks").GetInt32());
        Assert.Equal(0, write.GetProperty("rcsi_reader_writer_pct").GetInt32());

        foreach (var quiet in new[] { "QuietDb", "OutDb" })
        {
            var row = Row(finding, quiet);
            Assert.Equal(0, row.GetProperty("rcsi_blocking_events").GetInt32());
            Assert.Equal(0, row.GetProperty("rcsi_deadlocks").GetInt32());
            Assert.Equal(0, row.GetProperty("rcsi_reader_writer_pct").GetInt32());
        }

        /* Events or deadlocks without a single reader or writer wait: the counts stay and the share is unknown (null), not 0. */
        var schema = Row(finding, "SchemaDb");
        Assert.Equal(2, schema.GetProperty("rcsi_blocking_events").GetInt32());
        Assert.Equal(0, schema.GetProperty("rcsi_deadlocks").GetInt32());
        Assert.Equal(JsonValueKind.Null, schema.GetProperty("rcsi_reader_writer_pct").ValueKind);
        var oneWait = Row(finding, "OneWaitDb");
        Assert.Equal(1, oneWait.GetProperty("rcsi_blocking_events").GetInt32());
        Assert.Equal(JsonValueKind.Null, oneWait.GetProperty("rcsi_reader_writer_pct").ValueKind);
        var deadlockOnly = Row(finding, "DeadlockDb");
        Assert.Equal(0, deadlockOnly.GetProperty("rcsi_blocking_events").GetInt32());
        Assert.Equal(1, deadlockOnly.GetProperty("rcsi_deadlocks").GetInt32());
        Assert.Equal(JsonValueKind.Null, deadlockOnly.GetProperty("rcsi_reader_writer_pct").ValueKind);

        /* The consent text built from the drill-down row alone (no carried figures) states no percent for either. */
        foreach (var unknown in new[] { "SchemaDb", "DeadlockDb", "OneWaitDb" })
        {
            var action = new RemediationAction(
                "RCSI", "set", Array.Empty<ForcePlanTarget>(),
                new[] { new DbConfigTarget(unknown, DbConfigSetting.ReadCommittedSnapshotOn, "OFF") });
            var disclosure = Assert.IsType<RiskDisclosure>(FactRiskDisclosure.GetForAction(action, finding));
            var text = string.Join(" ", disclosure.RisksOfNotChanging.Select(r => r.Text));
            Assert.DoesNotContain("%", text);
            Assert.DoesNotContain("majority was writer/writer", text);
        }

        var target = Assert.Single(FactRemediation.CollectRcsiTargets(finding));
        Assert.Equal("ReadDb", target.Database);
        Assert.Equal(12, target.Figures.BlockingEvents);
        Assert.Equal(2, target.Figures.Deadlocks);
        Assert.Equal(75, target.Figures.ReaderWriterPct);
        Assert.NotNull(FactRemediation.BuildRcsiAction(finding));
    }
}
