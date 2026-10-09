using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Darling.Tests;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5361: Lite's reconstructed-chain drill-down reads its 5,000 pair-rows WITHOUT statement text and fetches the whole text
/// of only the levels it shows, by event key, from the source each level's row came from; the parameter-sensitive
/// drill-down ranks plans without text and reads the whole text of the five it keeps. What each prints must be what it
/// printed when the read carried the whole text. The chain oracle is the old flow run on the same rows (text restored in
/// the pair read, whole-text DMV append, same reconstruction, same judge-then-cut); the plan expectation is the newest
/// row's seeded text. The seeds repeat an edge with different text per row, mix both sources, and include a plain
/// statement past the cut, a named statement the filter withholds, an astral character at the cut, a NULL text and a NULL ecid.
/// </summary>
public class StatementDrillDownTextFetchTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = -5361_77;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = -1;

    private static readonly string Plain = "SELECT plain_ssf " + new string('x', 683);
    private static readonly string AstralAtCut = new string('a', 499) + "\U0001F600" + new string('b', 100);

    public StatementDrillDownTextFetchTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private static DateTime Now => DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

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

    private Task InsertBprAsync(DateTime at, int blocked, int blocking, long waitMs, string? blockedText, string? blockingText,
        int? blockedEcid = 0, int? blockingEcid = 0) =>
        ExecAsync(@"
INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
    blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text, blocked_ecid, blocking_ecid, monitor_loop)
VALUES ($1, $2, $3, 'TestServer', $2, 'FetchDb', $4, $5, $6, 'X', $7, $8, $9, $10, 7)",
            _nextId--, at, ServerId, blocked, blocking, waitMs, blockedText, blockingText, blockedEcid, blockingEcid);

    private Task InsertDmvAsync(DateTime at, int blocked, int blocking, long waitMs, string? blockedText, string? blockingText) =>
        ExecAsync(@"
INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, database_name,
    blocked_spid, blocking_spid, wait_time_ms, blocking_status, blocked_sql_text, blocking_sql_text, blocked_ecid, blocking_ecid, monitor_loop)
VALUES ($1, $2, $3, 'TestServer', $2, 'FetchDb', $4, $5, $6, 'sleeping', $7, $8, 0, 0, -1)",
            _nextId--, at, ServerId, blocked, blocking, waitMs, blockedText, blockingText);

    private AnalysisContext Context() => new()
    {
        ServerId = ServerId,
        ServerName = "TestServer",
        TimeRangeStart = Now.AddHours(-4),
        TimeRangeEnd = Now.AddMinutes(1),
    };

    private async Task<string> ShippedAsync(string factKey, string section, AnalysisContext context)
    {
        var finding = new AnalysisFinding { RootFactKey = factKey, StoryPath = factKey, PathKeys = [factKey], Severity = 1.0 };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], context);
        Assert.NotNull(finding.DrillDown);
        return JsonSerializer.Serialize(finding.DrillDown![section]);
    }

    /// <summary>The flow before #5361 on the same store: whole text in every pair-row.</summary>
    private async Task<string> OracleChainsAsync(AnalysisContext context)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
SELECT
    {BlockingPairRowQuery.LeadingColumns},
    blocked_sql_text AS blocked_sql,
    blocking_sql_text AS blocking_sql,
    {BlockingPairRowQuery.IdentityColumns},
    contentious_object,
    {BlockingPairRowQuery.TrailingIdentityColumns}
FROM {StoredEventCopies.BlockedProcessReports("server_id = $1 AND event_time >= $2 AND event_time <= $3 " + BlockingPairRowQuery.SpidFilter)} AS ev
ORDER BY event_time DESC
LIMIT 5000";
        cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
        cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });
        var rows = new List<BlockingPairRow>();
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
                rows.Add(BlockingPairRowQuery.Read(reader));
        }

        await BlockingPairRowQuery.AppendDmvSnapshotRowsAsync(
            connection.CreateCommand, rows, context.ServerId, context.TimeRangeStart, context.TimeRangeEnd,
            default, includeText: true);
        Assert.Contains(rows, r => r.BlockedSqlText.Length > 0);

        var reconstruction = BlockingChainReconstructor.Reconstruct(
            rows, maxDepth: 50, maxPairs: 5000, stepBudget: 100_000, scopeByMonitorLoop: false);
        return JsonSerializer.Serialize(reconstruction.Chains.Take(3).Select(chain => (object)new
        {
            apex_spid = chain.ApexSpid,
            apex_sleeping = chain.ApexSleeping,
            depth = chain.Depth,
            victim_count = chain.VictimCount,
            max_wait_ms = chain.MaxWaitMs,
            levels = chain.Levels.Select(l => new
            {
                level = l.Level,
                blocking_spid = l.BlockingSpid,
                blocked_spid = l.BlockedSpid,
                lock_mode = l.LockMode,
                wait_time_ms = l.WaitTimeMs,
                blocking_sql = McpHelpers.StatementPreview(l.BlockingSqlText, DrillDownCollector.StatementPreviewLength),
                blocked_sql = McpHelpers.StatementPreview(l.BlockedSqlText, DrillDownCollector.StatementPreviewLength)
            }).ToList()
        }).ToList());
    }

    [Fact]
    public async Task ReconstructedChains_PrintTheSameLevelsAndText_AsTheWholeTextPairRead_FromBothSources()
    {
        var context = Context();
        var t0 = new DateTime(context.TimeRangeStart.AddMinutes(30).Ticks / TimeSpan.TicksPerMinute * TimeSpan.TicksPerMinute, DateTimeKind.Unspecified);
        var named = StatementScrubCanary.UriStatement(500);

        /* Chain A, blocked-process reports: 60 -> 71 -> 72 -> 73 (+ 74 with NULL ecids). The edge 60 -> 71 repeats with
           different text per row; the longest wait (the newer of the two 12,000 rows) is the row the chain keeps. */
        await InsertBprAsync(t0.AddSeconds(1), 71, 60, 5_000, "SELECT older_a1", "SELECT older_b1");
        await InsertBprAsync(t0.AddSeconds(2), 71, 60, 12_000, named, Plain);
        await InsertBprAsync(t0.AddSeconds(3), 71, 60, 12_000, AstralAtCut, AstralAtCut + "tail");
        await InsertBprAsync(t0.AddSeconds(4), 72, 71, 3_000, "SELECT long_" + new string('y', 2_000), "SELECT blocking_72");
        await InsertBprAsync(t0.AddSeconds(5), 73, 72, 2_000, null, "");
        await InsertBprAsync(t0.AddSeconds(6), 74, 72, 1_000, "SELECT null_ecid", "SELECT null_ecid_blocker", null, null);

        /* Chain B, DMV snapshots only: 80 -> 81 -> 82. */
        await InsertDmvAsync(t0.AddMinutes(5).AddSeconds(1), 81, 80, 4_000, "SELECT dmv_old", "SELECT dmv_old_blocker");
        await InsertDmvAsync(t0.AddMinutes(5).AddSeconds(30), 81, 80, 9_000, named, Plain);
        await InsertDmvAsync(t0.AddMinutes(5).AddSeconds(31), 82, 81, 2_500, "SELECT dmv_82", AstralAtCut);

        /* Chain C: an edge both sources hold in one minute: the report wins, so the report's text is printed. */
        await InsertBprAsync(t0.AddMinutes(10).AddSeconds(5), 91, 90, 6_000, "SELECT from_report", "SELECT from_report_blocker");
        await InsertDmvAsync(t0.AddMinutes(10).AddSeconds(25), 91, 90, 7_000, "SELECT from_snapshot", "SELECT from_snapshot_blocker");

        var oracle = await OracleChainsAsync(context);
        var shipped = await ShippedAsync("BLOCKING_CHAIN", "reconstructed_blocking_chains", context);

        Assert.Equal(oracle, shipped);
        Assert.Equal(3, JsonSerializer.Deserialize<JsonElement>(shipped).GetArrayLength());
        Assert.Contains(SensitiveStatements.PlaceholderText, shipped, StringComparison.Ordinal);
        Assert.DoesNotContain(StatementScrubCanary.UriSecretPartial, shipped, StringComparison.Ordinal);
        Assert.Contains(Plain[..500], shipped, StringComparison.Ordinal);
        Assert.DoesNotContain(Plain[..501], shipped, StringComparison.Ordinal);
        Assert.Contains("SELECT dmv_82", shipped, StringComparison.Ordinal);
        Assert.Contains("SELECT from_report", shipped, StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT from_snapshot", shipped, StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT older_a1", shipped, StringComparison.Ordinal);
        Assert.Contains("SELECT null_ecid", shipped, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ParameterSensitivePlans_PrintTheNewestRowsText_AndAWithheldPlanStillPrintsTheMarker()
    {
        var context = Context();
        var named = StatementScrubCanary.UriStatement(500);
        var compiledBeforeWindow = context.TimeRangeStart.AddDays(-2);

        /* plan: (database, query hash, plan hash, newest text, older text). Two plans share one query hash. */
        var plans = new (string? Db, string Hash, string PlanHash, string? Newest, string? Older)[]
        {
            ("FetchDb", "0xQH_SAME", "0xPH_0", Plain, "SELECT older_0"),
            ("FetchDb", "0xQH_SAME", "0xPH_1", named, "SELECT older_1"),
            (null, "0xQH_2", "0xPH_2", AstralAtCut, null),
            ("FetchDb", "0xQH_3", "0xPH_3", "", "SELECT older_3"),
            ("FetchDb", "0xQH_4", "0xPH_4", null, "SELECT older_4"),
            ("FetchDb", "0xQH_5", "0xPH_5", "SELECT beyond_the_cap_5", null),
        };

        for (var p = 0; p < plans.Length; p++)
        {
            var plan = plans[p];
            for (var snapshot = 0; snapshot < 3; snapshot++)
            {
                var newest = snapshot == 2;
                var text = newest ? plan.Newest : (plan.Older is null ? null : plan.Older + " snapshot " + snapshot);
                await ExecAsync(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     creation_time, execution_count, min_worker_time, max_worker_time, min_grant_kb, max_grant_kb,
     min_spills, max_spills, query_text, delta_execution_count)
VALUES ($1, $2, $3, 'TestServer', $4, $5, $6, $7, $8, 20000, $9, 1024, 2048, 0, 0, $10, 25)",
                    _nextId--, context.TimeRangeStart.AddMinutes(30 + (snapshot * 60)), ServerId, plan.Db, plan.Hash, plan.PlanHash,
                    compiledBeforeWindow, 500L + snapshot, 20_000L * (60 - p), text);
            }
        }

        var shipped = JsonSerializer.Deserialize<JsonElement>(await ShippedAsync("PARAMETER_SENSITIVITY", "parameter_sensitive_queries", context))
            .EnumerateArray().ToList();

        /* The five highest spreads print, in rank order, each with the NEWEST row's text judged and cut; a plan whose newest
           row has no text prints empty text, and the sixth plan is past the cap. */
        Assert.Equal(5, shipped.Count);
        var expected = new[]
        {
            McpHelpers.StatementPreview(Plain, 500),
            McpHelpers.StatementPreview(named, 500),
            McpHelpers.StatementPreview(AstralAtCut, 500),
            McpHelpers.StatementPreview("", 500),
            "",
        };
        for (var i = 0; i < 5; i++)
        {
            Assert.Equal(plans[i].PlanHash, shipped[i].GetProperty("query_plan_hash").GetString());
            Assert.Equal(plans[i].Db ?? "", shipped[i].GetProperty("database").GetString());
            Assert.Equal(expected[i], shipped[i].GetProperty("query_text").GetString());
        }

        var all = shipped.Select(s => s.GetRawText()).Aggregate((a, b) => a + b);
        Assert.Contains(SensitiveStatements.PlaceholderText, all, StringComparison.Ordinal);
        Assert.DoesNotContain(StatementScrubCanary.UriSecretPartial, all, StringComparison.Ordinal);
        Assert.DoesNotContain("older_", all, StringComparison.Ordinal);
        Assert.DoesNotContain("beyond_the_cap", all, StringComparison.Ordinal);
        Assert.Equal(Plain[..500], expected[0]);
    }
}
