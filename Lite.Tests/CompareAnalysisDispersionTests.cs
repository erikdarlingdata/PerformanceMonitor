/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using ModelContextProtocol.Server;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3538 A3 through the real <c>compare_analysis</c> tool over a DuckDB store: what a CALLER sees when
/// the same-hour-yesterday window is compared with today's. The banding arithmetic itself is pinned on
/// the shared helper in <see cref="ComparisonBandingTests"/>; this class pins the payload contract — the
/// rules stated on every result, one family row per physical cause, the summary's family counts beside
/// its row counts, and the coverage caveat riding on every verdict row when a side was partly seen.
///
/// <para>Rows are planted as the collector writes them: a baseline reading at each window's start whose
/// delta is unknowable, then one collection every fifteen minutes carrying the delta since the previous
/// one, across the whole four-hour window (full coverage, so nothing here is about A2's divisor).</para>
/// </summary>
public sealed class CompareAnalysisDispersionTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -3_538_300_000;
    private DuckDBConnection? _seedConn;

    public CompareAnalysisDispersionTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "CompareAnalysisDispersionTests_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        /* Windows auth so AddServer never touches the credential store — no DPAPI side effects. */
        _serverManager = new ServerManager(configDir);
        var server = new ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
        _serverManager.AddServer(server);

        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    /// <summary>
    /// The review's I/O stall, yesterday vs today: PAGEIOLATCH_SH 10% → 30% and PAGEIOLATCH_EX 5% → 20%
    /// of observed time, with CXPACKET flat at 10%. Two worse ROWS, one worse FAMILY (<c>io_pressure</c>,
    /// both latch keys as members, the larger relative move as the worst member), one stable family; the
    /// rules are stated on the payload; nothing is caveated, because both windows were fully observed.
    /// </summary>
    [Fact]
    public async Task OneIoStall_IsOneWorseFamily_AndTheRulesAreStated()
    {
        var now = DateTime.UtcNow;
        await PlantWindowAsync(now.AddHours(-24), ("PAGEIOLATCH_SH", 0.10), ("PAGEIOLATCH_EX", 0.05), ("CXPACKET", 0.10));
        await PlantWindowAsync(now, ("PAGEIOLATCH_SH", 0.30), ("PAGEIOLATCH_EX", 0.20), ("CXPACKET", 0.10));

        var result = await McpAnalysisTools.CompareAnalysis(CreateService(), _serverManager, null, 4, 28);

        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;
        Assert.Equal(JsonValueKind.Null, root.GetProperty("caveat").ValueKind);

        /* The rules travel with the numbers. */
        Assert.Contains("N=1 vs N=1", root.GetProperty("reading").GetString()!, StringComparison.Ordinal);
        var rules = root.GetProperty("band_rules");
        Assert.Contains("robust-sigma", rules.GetProperty("baseline").GetString()!, StringComparison.Ordinal);
        Assert.Contains("25%", rules.GetProperty("absolute").GetString()!, StringComparison.Ordinal);
        Assert.Contains("plan_cache_churn", rules.GetProperty("presence").GetString()!, StringComparison.Ordinal);

        var summary = root.GetProperty("summary");
        Assert.Equal(2, summary.GetProperty("worse").GetInt32());
        Assert.Equal(1, summary.GetProperty("stable").GetInt32());
        Assert.Equal(1, summary.GetProperty("families_worse").GetInt32());
        Assert.Equal(1, summary.GetProperty("families_stable").GetInt32());
        Assert.Equal(0, summary.GetProperty("new_issues").GetInt32());
        Assert.Equal(0, summary.GetProperty("plan_cache_churn_appeared").GetInt32());
        Assert.False(summary.GetProperty("coverage_caveat").GetBoolean());

        var families = root.GetProperty("families").EnumerateArray().ToList();
        Assert.Equal(2, families.Count);
        var io = families[0]; // changed families first
        Assert.Equal("io_pressure", io.GetProperty("family").GetString());
        Assert.Equal("worse", io.GetProperty("status").GetString());
        Assert.Equal("PAGEIOLATCH_EX", io.GetProperty("worst_key").GetString()); // 0.05 → 0.20 is the larger relative move
        Assert.Equal(new[] { "PAGEIOLATCH_EX", "PAGEIOLATCH_SH" }, io.GetProperty("members").EnumerateArray().Select(m => m.GetString()).ToArray());
        Assert.Equal(2, io.GetProperty("worse").GetInt32());
        Assert.Equal("parallelism", families[1].GetProperty("family").GetString());
        Assert.Equal("stable", families[1].GetProperty("status").GetString());

        var rows = root.GetProperty("facts").EnumerateArray().ToDictionary(r => r.GetProperty("key").GetString()!);
        Assert.Equal(3, rows.Count);
        foreach (var key in new[] { "PAGEIOLATCH_SH", "PAGEIOLATCH_EX" })
        {
            var row = rows[key];
            Assert.Equal("worse", row.GetProperty("status").GetString());
            Assert.Equal("absolute", row.GetProperty("band_source").GetString());
            Assert.Equal("io_pressure", row.GetProperty("family").GetString());
            Assert.Equal("both", row.GetProperty("presence").GetString());
            Assert.False(row.GetProperty("coverage_caveat").GetBoolean());
            Assert.Equal(JsonValueKind.Null, row.GetProperty("delta_sigma").ValueKind); // no per-type wait baseline exists
        }
        Assert.InRange(rows["PAGEIOLATCH_EX"].GetProperty("relative_move").GetDouble(), 0.74, 0.76);
        Assert.InRange(rows["PAGEIOLATCH_SH"].GetProperty("relative_move").GetDouble(), 0.66, 0.67);

        /* PAGEIOLATCH_SH saturates its (0.25, null) ladder at 30%: severity 1.0 on the comparison side, so
           the old ±0.1 severity band read 0.10 → 0.30 as +0.6 and would have called 0.30 → 0.60 stable. */
        Assert.Equal(1.0, rows["PAGEIOLATCH_SH"].GetProperty("ladder_position").GetDouble(), precision: 4);

        var cx = rows["CXPACKET"];
        Assert.Equal("stable", cx.GetProperty("status").GetString());
        Assert.Equal(0.0, cx.GetProperty("relative_move").GetDouble(), precision: 4);

        var churn = root.GetProperty("plan_cache_churn");
        Assert.Empty(churn.GetProperty("appeared").EnumerateArray());
        Assert.Empty(churn.GetProperty("disappeared").EnumerateArray());
        Assert.Contains("plan-cache identities", churn.GetProperty("note").GetString()!, StringComparison.Ordinal);
    }

    /// <summary>
    /// A key present in one window only is a new issue only when it registers on its ladder: WRITELOG
    /// appearing at 30% (base 0.6 on the (0.25, 0.50) ramp) is; LCK_M_IS appearing at 0.1% (base 0.02) is
    /// a trace, stable, and not counted. The rows say <c>presence</c> and <c>band_source</c> so the reader
    /// knows why one counted and the other did not.
    /// </summary>
    [Fact]
    public async Task AKeyAppearingAtATrace_IsNotANewIssue_ButOneRegisteringOnItsLadderIs()
    {
        var now = DateTime.UtcNow;
        await PlantWindowAsync(now.AddHours(-24), ("CXPACKET", 0.10));
        await PlantWindowAsync(now, ("CXPACKET", 0.10), ("WRITELOG", 0.30), ("LCK_M_IS", 0.001));

        var result = await McpAnalysisTools.CompareAnalysis(CreateService(), _serverManager, null, 4, 28);

        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;
        var summary = root.GetProperty("summary");
        Assert.Equal(1, summary.GetProperty("new_issues").GetInt32());
        Assert.Equal(1, summary.GetProperty("worse").GetInt32());
        Assert.Equal(2, summary.GetProperty("stable").GetInt32());

        var rows = root.GetProperty("facts").EnumerateArray().ToDictionary(r => r.GetProperty("key").GetString()!);
        var writelog = rows["WRITELOG"];
        Assert.Equal("worse", writelog.GetProperty("status").GetString());
        Assert.Equal("comparison_only", writelog.GetProperty("presence").GetString());
        Assert.Equal("presence", writelog.GetProperty("band_source").GetString());
        Assert.Equal(JsonValueKind.Null, writelog.GetProperty("baseline_value").ValueKind);
        Assert.Equal("log_io", writelog.GetProperty("family").GetString());

        var trace = rows["LCK_M_IS"];
        Assert.Equal("stable", trace.GetProperty("status").GetString());
        Assert.Equal("comparison_only", trace.GetProperty("presence").GetString());
        Assert.InRange(trace.GetProperty("ladder_position").GetDouble(), 0.0, 0.03);

        Assert.Equal("WRITELOG", root.GetProperty("facts")[0].GetProperty("key").GetString()); // the one changed row leads
    }

    /// <summary>
    /// The tool's description promises what the payload now carries — and says what "worse" does not mean. A
    /// caller reads this before deciding to trust a verdict. #3898 Phase 2 (D5, D6) retired the instructions
    /// row that used to duplicate this on both SKUs; the description is now the only surface.
    /// </summary>
    [Fact]
    public void Description_SaysWhatWorseMeans_AndWhatItDoesNot()
    {
        var method = typeof(McpAnalysisTools).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == "compare_analysis");
        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;

        foreach (var token in new[] { "delta_sigma", "band_source", "band_rules", "families", "plan_cache_churn", "coverage_caveat", "N=1 vs N=1", "cannot show that a change CAUSED anything" })
        {
            Assert.Contains(token, description, StringComparison.Ordinal);
        }
    }

    /* ── helpers ── */

    private AnalysisService CreateService() => new(_duckDb) { MinimumDataHours = 0 };

    /// <summary>
    /// A fully collected 4h window ending at <paramref name="windowEnd"/>: a baseline reading at the start
    /// (delta 0, unknowable) and sixteen 15-minute deltas per wait type, summing to the wait type's
    /// fraction of the window.
    /// </summary>
    private async Task PlantWindowAsync(DateTime windowEnd, params (string WaitType, double Fraction)[] waits)
    {
        var start = windowEnd.AddHours(-4);
        const int points = 16;
        foreach (var (waitType, fraction) in waits)
        {
            var perPoint = (long)Math.Round(fraction * 4 * 3_600_000 / points);
            await PlantWaitAsync(start, waitType, 0L);
            for (var i = 1; i <= points; i++)
                await PlantWaitAsync(start.AddMinutes(15 * i), waitType, perPoint);
        }
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

    private async Task PlantWaitAsync(DateTime at, string waitType, long deltaWaitMs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, 'TestServer', $4, 50, 1000000, 0, 4, $5, 0)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(at);
        P(_serverId);
        P(waitType);
        P(deltaWaitMs);
        await cmd.ExecuteNonQueryAsync();
    }
}
