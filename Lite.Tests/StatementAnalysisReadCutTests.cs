using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5320: Lite's analysis readers return the WHOLE statement, the statement filter judges it, and the cut comes after.
/// Each case plants a batch whose named value (a URI's <c>user:secret@</c>) sits inside the cut and whose naming
/// at-sign sits past it, so a reader that cuts in SQL first hands the filter a prefix that reads clean and the answer
/// holds the start of the secret. A plain statement past the cut is cut exactly as before. Blocking (top chains,
/// reconstructed chains, deadlocks), the same-statement pileup read and the long-running-query alert read.
/// </summary>
public class StatementAnalysisReadCutTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = -5320_77;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = -1;

    /// <summary>A plain statement of 700 characters: kept as the first 500 and nothing more.</summary>
    private static readonly string Plain = "SELECT plain_ssf " + new string('x', 683);

    public StatementAnalysisReadCutTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private async Task ExecAsync(string sql, params object?[] args)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var arg in args)
            cmd.Parameters.Add(new DuckDBParameter { Value = arg ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private static DateTime Now => DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

    private AnalysisContext Context() => new()
    {
        ServerId = ServerId,
        ServerName = "TestServer",
        TimeRangeStart = Now.AddHours(-4),
        TimeRangeEnd = Now.AddMinutes(1),
    };

    private async Task<JsonElement> DrillAsync(string factKey, string section)
    {
        var finding = new AnalysisFinding { RootFactKey = factKey, StoryPath = factKey, PathKeys = [factKey], Severity = 1.0 };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], Context());
        Assert.NotNull(finding.DrillDown);
        return JsonSerializer.SerializeToElement(finding.DrillDown[section]);
    }

    private static void AssertNamedWithheldAndPlainCutAtFiveHundred(string section, string json)
    {
        var shown = section + ": " + json[..Math.Min(json.Length, 400)];
        Assert.True(!json.Contains(StatementScrubCanary.UriSecretPartial, StringComparison.Ordinal), "secret prefix kept in " + shown);
        Assert.True(json.Contains(SensitiveStatements.PlaceholderText, StringComparison.Ordinal), "no placeholder in " + shown);
        Assert.True(json.Contains(Plain[..500], StringComparison.Ordinal), "plain statement not kept to 500 in " + shown);
        Assert.True(!json.Contains(Plain[..501], StringComparison.Ordinal), "plain statement kept past 500 in " + shown);
    }

    [Fact]
    public async Task BlockingReaders_JudgeTheWholeStatement_ThenCutAtFiveHundred()
    {
        var at = Now.AddMinutes(-30);
        await ExecAsync(@"
INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
    blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text)
VALUES ($1, $2, $3, 'TestServer', $2, 'UriDb', 71, 60, 12000, 'X', $4, $5)",
            _nextId--, at, ServerId, StatementScrubCanary.UriStatement(500), Plain);
        await ExecAsync(@"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml, database_name)
VALUES ($1, $2, $3, 'TestServer', $2, 'process1', $4, '<deadlock-list/>', 'UriDb'), ($5, $7, $3, 'TestServer', $7, 'process2', $6, '<deadlock-list/>', 'UriDb')",
            _nextId--, at, ServerId, StatementScrubCanary.UriStatement(500), _nextId--, Plain, at.AddMinutes(1));

        AssertNamedWithheldAndPlainCutAtFiveHundred("top_blocking_chains", (await DrillAsync("BLOCKING_EVENTS", "top_blocking_chains")).GetRawText());
        AssertNamedWithheldAndPlainCutAtFiveHundred("reconstructed_blocking_chains", (await DrillAsync("BLOCKING_CHAIN", "reconstructed_blocking_chains")).GetRawText());
        AssertNamedWithheldAndPlainCutAtFiveHundred("top_deadlocks", (await DrillAsync("DEADLOCKS", "top_deadlocks")).GetRawText());
    }

    [Fact]
    public async Task PileupRead_KeepsTheRawCutForIdentity_AndPrintsTheJudgedPreview()
    {
        await ExecAsync(@"
INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status, total_elapsed_time_ms)
VALUES ($1, $2, $3, 'TestServer', 71, 'UriDb', $4, 'running', 90000), ($5, $2, $3, 'TestServer', 72, 'UriDb', $6, 'running', 90000)",
            _nextId--, Now.AddMinutes(-2), ServerId, StatementScrubCanary.UriStatement(1500), _nextId--, "SELECT plain_ssf " + new string('x', 2000));

        var rows = await new PileupSnapshotReader(_duckDb).ReadWindowAsync(ServerId, Now.AddHours(-1), CancellationToken.None);

        var named = rows.Single(r => r.SessionId == 71);
        /* The raw cut is what the SQL cut left: only hashed or searched, never printed. The text a finding prints is judged whole. */
        Assert.Equal(1500, named.QueryText!.Length);
        Assert.Contains(StatementScrubCanary.UriSecretPartial, named.QueryText, StringComparison.Ordinal);
        Assert.Equal(SensitiveStatements.PlaceholderText, named.PreviewText);

        var plain = rows.Single(r => r.SessionId == 72);
        Assert.Equal(1500, plain.QueryText!.Length);
        Assert.Equal(plain.QueryText, plain.PreviewText);
    }

    [Fact]
    public async Task LongRunningQueryAlertRead_JudgesTheWholeStatement_ThenCutsAtThreeHundred()
    {
        await ExecAsync(@"
INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status, total_elapsed_time_ms, cpu_time_ms, reads, writes, wait_type)
VALUES ($1, $2, $3, 'TestServer', 71, 'UriDb', $4, 'running', 900000, 1, 1, 0, 'CXPACKET'), ($5, $2, $3, 'TestServer', 72, 'UriDb', $6, 'running', 800000, 1, 1, 0, 'CXPACKET')",
            _nextId--, Now.AddMinutes(-1), ServerId, StatementScrubCanary.UriStatement(300), _nextId--, "SELECT plain_ssf " + new string('x', 483));

        var read = await new LocalDataService(_duckDb).GetLongRunningQueriesAsync(ServerId, thresholdMinutes: 5, maxResults: 5);

        var named = read.Sessions.Single(q => q.SessionId == 71).QueryText;
        Assert.DoesNotContain(StatementScrubCanary.UriSecretPartial, named, StringComparison.Ordinal);
        Assert.Contains(SensitiveStatements.PlaceholderText, named, StringComparison.Ordinal);
        Assert.Equal(300, read.Sessions.Single(q => q.SessionId == 72).QueryText.Length);
    }

    [Fact]
    public void LiteReaders_CutNoStatementTextInSql()
    {
        foreach (var file in new[]
        {
            "Lite/Analysis/DrillDownCollector.Blocking.cs", "Lite/Analysis/PileupSnapshotReader.cs",
            "Lite/Analysis/DuckDbFactCollector.Activity.cs", "Lite/Services/LocalDataService.WaitStats.cs",
        })
        {
            var source = File.ReadAllText(Path.Combine(RepoRoot(), file));
            Assert.DoesNotMatch(@"(?i)\b(LEFT|SUBSTRING|substr)\s*\(\s*(MAX\(\s*)?(r\.)?(query_text|blocked_sql_text|blocking_sql_text|victim_sql_text)\b", source);
        }
    }

    private static string RepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null && !Directory.Exists(Path.Combine(dir.FullName, "Lite", "Analysis")))
            dir = dir.Parent;
        return dir?.FullName ?? throw new InvalidOperationException("repo root not found");
    }
}
