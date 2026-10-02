/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4239, Lite half: the Active Queries grid and the wait drill-down stopped reading full plan XML for
/// every snapshot row just to enable the plan buttons. <see cref="LocalDataService.GetLatestQuerySnapshotsAsync"/>,
/// <see cref="LocalDataService.GetQuerySnapshotsByWaitTypeAsync"/> and
/// <see cref="LocalDataService.GetAllQuerySnapshotsInRangeAsync"/> now select <c>has_query_plan</c> /
/// <c>has_live_query_plan</c> presence flags instead of the payload; the plan buttons fetch the XML on click
/// via <see cref="LocalDataService.GetSnapshotPlanTextAsync"/>, keyed by the row's own capture.
/// </summary>
public sealed class QuerySnapshotPlanOnDemandTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "TestServer";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly int _serverId;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public QuerySnapshotPlanOnDemandTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "QuerySnapshotPlanOnDemand_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);

        _dataService = new LocalDataService(_duckDb);

        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    /* ───────────────────────── source pin ───────────────────────── */

    /// <summary>
    /// Fails on the pre-#4239 shape: a bare <c>query_plan, live_query_plan</c> select in any of the three
    /// reads. Written against the SOURCE text, not a fixture result, because a fixture assertion on
    /// <c>QueryPlan == null</c> would pass just as well whether the SQL never selected the payload (the fix)
    /// or selected it and the row simply had none (not the fix) — only the source proves WHICH one happened.
    /// </summary>
    [Fact]
    public void ThreeSnapshotReads_SelectPresenceFlags_NotPlanPayload()
    {
        var blocking = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Services", "LocalDataService.Blocking.cs"));
        var waitStats = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Services", "LocalDataService.WaitStats.cs"));

        Assert.DoesNotContain("    query_plan,\n    live_query_plan,", blocking);
        Assert.DoesNotContain("    q.query_plan,\n    q.live_query_plan,", waitStats);

        Assert.Contains("query_plan IS NOT NULL AS has_query_plan", blocking);
        Assert.Contains("live_query_plan IS NOT NULL AS has_live_query_plan", blocking);

        /* GetQuerySnapshotsByWaitTypeAsync AND GetAllQuerySnapshotsInRangeAsync — both, not just one. */
        var flagOccurrences = CountOccurrences(waitStats, "q.query_plan IS NOT NULL AS has_query_plan");
        Assert.True(flagOccurrences >= 2,
            $"Expected the has_query_plan flag in both WaitStats.cs reads, found {flagOccurrences} occurrence(s).");
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }
        return count;
    }

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("PerformanceMonitor.sln not found above the test output directory.");
    }

    /* ───────────────────────── flags match stored rows; fetch is byte-identical ───────────────────────── */

    [Fact]
    public async Task GetLatestQuerySnapshotsAsync_FlagsMatchStoredRows_AndFetchIsByteIdenticalToTheOldPayload()
    {
        var t = WholeSecondsNow();
        var estimatedXml = "<ShowPlanXML><Estimated>" + new string('x', 500) + "</Estimated></ShowPlanXML>";
        var liveXml = "<ShowPlanXML><Actual>" + new string('y', 500) + "</Actual></ShowPlanXML>";

        // Row A: both plans present.
        await SeedSnapshotAsync(t, sessionId: 51, requestId: 0, queryPlan: estimatedXml, liveQueryPlan: liveXml);
        // Row B: neither plan present (evicted from cache).
        await SeedSnapshotAsync(t, sessionId: 52, requestId: 0, queryPlan: null, liveQueryPlan: null);
        // Row C: pre-v34/archived shape — request_id itself is NULL, not just the plans.
        await SeedSnapshotAsync(t, sessionId: 53, requestId: null, queryPlan: estimatedXml, liveQueryPlan: null);

        var rows = await _dataService.GetLatestQuerySnapshotsAsync(_serverId, hoursBack: 1);

        var rowA = Assert.Single(rows, r => r.SessionId == 51);
        Assert.True(rowA.HasQueryPlan);
        Assert.True(rowA.HasLiveQueryPlan);
        Assert.Null(rowA.QueryPlan);
        Assert.Null(rowA.LiveQueryPlan);

        var rowB = Assert.Single(rows, r => r.SessionId == 52);
        Assert.False(rowB.HasQueryPlan);
        Assert.False(rowB.HasLiveQueryPlan);

        var rowC = Assert.Single(rows, r => r.SessionId == 53);
        Assert.True(rowC.HasQueryPlan);
        Assert.False(rowC.HasLiveQueryPlan);
        Assert.Equal(0, rowC.RequestId);

        // The click-path fetch, by each row's own capture key, returns exactly what the old bulk read used to carry.
        var fetchedA_estimated = await _dataService.ResolveSnapshotEstimatedPlanAsync(_serverId, rowA);
        var fetchedA_live = await _dataService.ResolveSnapshotLivePlanAsync(_serverId, rowA);
        Assert.Equal(estimatedXml, fetchedA_estimated);
        Assert.Equal(liveXml, fetchedA_live);

        var fetchedB_estimated = await _dataService.ResolveSnapshotEstimatedPlanAsync(_serverId, rowB);
        Assert.Null(fetchedB_estimated);

        // Row C: pre-v34 NULL request_id — the fetch must still resolve via COALESCE(request_id, 0).
        var fetchedC_estimated = await _dataService.ResolveSnapshotEstimatedPlanAsync(_serverId, rowC);
        Assert.Equal(estimatedXml, fetchedC_estimated);
    }

    [Fact]
    public async Task GetQuerySnapshotsByWaitTypeAsync_And_GetAllQuerySnapshotsInRangeAsync_FlagsMatchStoredRows()
    {
        var t = WholeSecondsNow();
        var estimatedXml = "<ShowPlanXML><Estimated>wait-drilldown</Estimated></ShowPlanXML>";
        await SeedSnapshotAsync(t, sessionId: 61, requestId: 0, queryPlan: estimatedXml, liveQueryPlan: null, waitType: "PAGEIOLATCH_SH");

        var byWaitType = await _dataService.GetQuerySnapshotsByWaitTypeAsync(_serverId, "PAGEIOLATCH_SH", hoursBack: 1);
        var waitRow = Assert.Single(byWaitType, r => r.SessionId == 61);
        Assert.True(waitRow.HasQueryPlan);
        Assert.False(waitRow.HasLiveQueryPlan);
        Assert.Null(waitRow.QueryPlan);
        Assert.Equal(estimatedXml, await _dataService.ResolveSnapshotEstimatedPlanAsync(_serverId, waitRow));

        var inRange = await _dataService.GetAllQuerySnapshotsInRangeAsync(_serverId, hoursBack: 1);
        var rangeRow = Assert.Single(inRange, r => r.SessionId == 61);
        Assert.True(rangeRow.HasQueryPlan);
        Assert.Null(rangeRow.QueryPlan);
        Assert.Equal(estimatedXml, await _dataService.ResolveSnapshotEstimatedPlanAsync(_serverId, rangeRow));
    }

    /* ───────────────────────── live-shaped row: in-row XML, no store match, no fetch needed ───────────────────────── */

    /// <summary>
    /// The ServerTab "Live Snapshot" button builds rows querying the monitored server directly —
    /// <c>CollectionTime = DateTime.UtcNow</c>, never written to the store — with the plan XML already in
    /// hand. A row shaped exactly like that (in-row plan set, flags set, a capture key that matches nothing
    /// in the store) must open its plan from the row, with no store fetch: <see cref="LocalDataService.ResolveSnapshotEstimatedPlanAsync"/>
    /// must return the in-row value even though a store lookup for this key would come back empty.
    /// </summary>
    [Fact]
    public async Task ResolveSnapshotPlan_LiveShapedRow_ReturnsInRowXml_WithNoStoreMatch()
    {
        var row = new QuerySnapshotRow
        {
            SessionId = 999,
            CollectionTime = DateTime.UtcNow, // never seeded — no row in the store shares this key
            RequestId = 0,
            QueryPlan = "<ShowPlanXML><Estimated>live-in-row</Estimated></ShowPlanXML>",
            LiveQueryPlan = "<ShowPlanXML><Actual>live-in-row</Actual></ShowPlanXML>",
            HasQueryPlan = true,
            HasLiveQueryPlan = true
        };

        Assert.Equal(row.QueryPlan, await _dataService.ResolveSnapshotEstimatedPlanAsync(_serverId, row));
        Assert.Equal(row.LiveQueryPlan, await _dataService.ResolveSnapshotLivePlanAsync(_serverId, row));
    }

    /* ───────────────────────── duplicate capture key: a NULL-plan row must not shadow the plan-bearing one ───────────────────────── */

    /// <summary>
    /// #4297. <c>v_query_snapshots</c> is the hot table UNION ALL the parquet archive, so one capture key
    /// (<c>server_id, collection_time, session_id, request_id</c>) can match more than one row. Without a
    /// presence guard, <c>LIMIT 1</c> can land on a matching row whose plan is NULL while a sibling matching
    /// row carries the real one — <see cref="LocalDataService.GetSnapshotPlanTextAsync"/> would then report
    /// "no plan available" for a row that has one.
    ///
    /// <para>The NULL-plan row is seeded FIRST, deliberately: a DuckDB table scan with no ORDER BY returns
    /// rows in insertion order, so an unguarded query reaches this row before the plan-bearing one, making the
    /// bug reproduce every run rather than by luck of scan order.</para>
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GetSnapshotPlanTextAsync_DuplicateCaptureKey_NullPlanRowDoesNotShadowThePlanBearingRow(bool live)
    {
        var t = WholeSecondsNow();
        var estimatedXml = "<ShowPlanXML><Estimated>dup-key</Estimated></ShowPlanXML>";
        var liveXml = "<ShowPlanXML><Actual>dup-key</Actual></ShowPlanXML>";

        // Row 1, seeded first: same capture key, no plan in either column.
        await SeedSnapshotAsync(t, sessionId: 81, requestId: 0, queryPlan: null, liveQueryPlan: null);
        // Row 2, seeded second, SAME capture key: carries both plans.
        await SeedSnapshotAsync(t, sessionId: 81, requestId: 0, queryPlan: estimatedXml, liveQueryPlan: liveXml);

        var fetched = await _dataService.GetSnapshotPlanTextAsync(_serverId, t, sessionId: 81, requestId: 0, live: live);

        Assert.Equal(live ? liveXml : estimatedXml, fetched);
    }

    /* ───────────────────────── before/after payload + timing, for the PR body ───────────────────────── */

    /// <summary>
    /// Not a correctness assertion beyond the sanity bound — this exists to PRODUCE the #4239 PR's before/after
    /// numbers on a seeded server, against a real DuckDB, not an estimate. Writes them to a file under the
    /// test's temp dir because MTP test output does not surface Console/Debug writes for a passing test.
    /// Seeds 3,500 rows with a ~4 KB estimated + ~4 KB live plan each (~28 MB of XML), matching the issue's
    /// own "3-4k plan-bearing snapshots" scale.
    /// </summary>
    [Fact]
    public async Task Benchmark_OldPayloadRead_Vs_NewFlagRead_Over3500PlanBearingRows()
    {
        const int rowCount = 3500;
        const int planSize = 4000;
        var estimatedXml = "<ShowPlanXML>" + new string('e', planSize) + "</ShowPlanXML>";
        var liveXml = "<ShowPlanXML>" + new string('a', planSize) + "</ShowPlanXML>";
        var t = WholeSecondsNow();

        for (var i = 0; i < rowCount; i++)
        {
            await SeedSnapshotAsync(t.AddSeconds(-i), sessionId: 100 + (i % 500), requestId: 0,
                queryPlan: estimatedXml, liveQueryPlan: liveXml);
        }

        // OLD shape: the payload select GetLatestQuerySnapshotsAsync used before #4239.
        var oldSw = Stopwatch.StartNew();
        var oldBytesBefore = GC.GetAllocatedBytesForCurrentThread();
        var oldPayloadTotal = 0L;
        {
            using var readLock = _duckDb.AcquireReadLock();
            var conn = await SeedConnectionAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT query_plan, live_query_plan FROM v_query_snapshots WHERE server_id = $1 ORDER BY collection_time DESC";
            cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                if (!reader.IsDBNull(0)) oldPayloadTotal += reader.GetString(0).Length;
                if (!reader.IsDBNull(1)) oldPayloadTotal += reader.GetString(1).Length;
            }
        }
        var oldBytes = GC.GetAllocatedBytesForCurrentThread() - oldBytesBefore;
        oldSw.Stop();

        // NEW shape: what GetLatestQuerySnapshotsAsync selects today.
        var newSw = Stopwatch.StartNew();
        var newBytesBefore = GC.GetAllocatedBytesForCurrentThread();
        var newRows = await _dataService.GetLatestQuerySnapshotsAsync(_serverId, hoursBack: 24);
        var newBytes = GC.GetAllocatedBytesForCurrentThread() - newBytesBefore;
        newSw.Stop();

        Assert.Equal(rowCount, newRows.Count);
        Assert.All(newRows, r => Assert.True(r.HasQueryPlan && r.HasLiveQueryPlan));

        var report = new StringBuilder();
        report.AppendLine($"Rows: {rowCount}, plan size: ~{planSize:N0} chars each (estimated + live)");
        report.AppendLine($"OLD (payload select): {oldSw.ElapsedMilliseconds} ms, {oldBytes:N0} bytes allocated, {oldPayloadTotal:N0} chars of plan text materialized");
        report.AppendLine($"NEW (flags only):     {newSw.ElapsedMilliseconds} ms, {newBytes:N0} bytes allocated");
        report.AppendLine($"Allocation reduction: {(1.0 - (double)newBytes / oldBytes) * 100:F1}%");
        File.WriteAllText(Path.Combine(_tempDir, "benchmark.txt"), report.ToString());

        // Real regression guard, not just descriptive: the new read must allocate markedly less.
        Assert.True(newBytes < oldBytes / 2,
            $"Expected the flags-only read to allocate well under half of the payload read's bytes. Old: {oldBytes:N0}, New: {newBytes:N0}.");
    }

    /* ───────────────────────── seeding ───────────────────────── */

    private Task SeedSnapshotAsync(DateTime at, int sessionId, int? requestId, string? queryPlan, string? liveQueryPlan, string? waitType = null) => ExecAsync(@"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status,
     blocking_session_id, wait_type, cpu_time_ms, total_elapsed_time_ms, query_plan, live_query_plan, request_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
        _nextId--, Naive(at), _serverId, ServerName, sessionId, "TestDb", "SELECT 1", "running",
        0, waitType, 1000L, 1500L, queryPlan, liveQueryPlan, requestId);

    private static DateTime WholeSecondsNow()
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task ExecAsync(string sql, params object?[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
            cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);
}
