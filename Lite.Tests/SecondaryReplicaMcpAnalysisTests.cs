/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5558 round 1 (M4): <c>analyze_server</c> and <c>compare_analysis</c> carry the same one-sentence note as
/// <c>get_analysis_findings</c>, under the same field name (<c>secondary_replica_note</c>), always present and null when this
/// node holds no secondary copy. Without it a short or empty answer on a secondary node reads as a clean bill of health for
/// databases the pass never looked at. Also pins the note on the service (<see cref="AnalysisService.LastSecondaryReplicaNote"/>)
/// and the cancelled-pass contract (M2): resolving the secondary set sits inside the pass's own try.
/// </summary>
public sealed class SecondaryReplicaMcpAnalysisTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -55_820_000;
    private DuckDBConnection? _seedConn;

    public SecondaryReplicaMcpAnalysisTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "SecondaryReplicaMcpTests_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        /* Windows auth so AddServer never touches the credential store. */
        _serverManager = new ServerManager(configDir);
        var server = new ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private AnalysisService CreateService() => new(_duckDb) { MinimumDataHours = 0 };

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task ExecAsync(string sql, params object[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values) cmd.Parameters.Add(new DuckDBParameter { Value = v });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>One group, this node local with <paramref name="localRole"/>, one database (SecDb) in it.</summary>
    private async Task SeedAgAsync(DateTime at, string localRole)
    {
        await ExecAsync("INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, is_local) VALUES ($1,$2,$3,'n',$4,$5,$6,$7)",
            _nextId--, at, _serverId, "AG1", "NODE1", localRole, true);
        await ExecAsync("INSERT INTO ag_database_replica_states (collection_id, collection_time, server_id, server_name, ag_name, database_name, replica_server_name, is_local) VALUES ($1,$2,$3,'n',$4,$5,$6,$7)",
            _nextId--, at, _serverId, "AG1", "SecDb", "NODE1", true);
    }

    private Task PlantWaitAsync(DateTime at, long deltaWaitMs) => ExecAsync(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, 'TestServer', 'AN5558_LITE_WAIT', 5000, $4, 0, 5000, $4, 0)", _nextId--, at, _serverId, deltaWaitMs);

    /// <summary>The note from wherever the answer's shape puts it: the root of a data-bearing answer, or the hints of a miss.</summary>
    private static string? NoteOf(string json, string field = "secondary_replica_note")
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        var carrier = root.TryGetProperty(field, out _) ? root : root.GetProperty("hints");
        var note = carrier.GetProperty(field);
        return note.ValueKind == JsonValueKind.Null ? null : note.GetString();
    }

    [Fact]
    public async Task AnalyzeServer_CarriesTheNote_OnlyWhenThisNodeHoldsASecondaryCopy()
    {
        var service = CreateService();

        /* No AG rows: the field is there and null. */
        Assert.Null(NoteOf(await McpAnalysisTools.AnalyzeServer(service, _serverManager, null, 4)));
        Assert.Null(service.LastSecondaryReplicaNote);

        /* A secondary copy: the same sentence the tabs and get_analysis_findings carry, on the answer AND on the service. */
        await SeedAgAsync(DateTime.UtcNow.AddMinutes(-1), "SECONDARY");
        var answer = await McpAnalysisTools.AnalyzeServer(service, _serverManager, null, 4);
        Assert.Equal(AgReplicaScope.SkippedNote(1), NoteOf(answer));
        Assert.Equal(AgReplicaScope.SkippedNote(1), service.LastSecondaryReplicaNote);

        /* An all-clear or unavailable message says it in prose too, not only in the field. */
        using var doc = JsonDocument.Parse(answer);
        if (doc.RootElement.TryGetProperty("message", out var message) && doc.RootElement.GetProperty("status").GetString() == "empty")
            Assert.Contains(AgReplicaScope.SkippedNote(1)!, message.GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnalyzeServer_OnAPrimary_CarriesANullNote()
    {
        await SeedAgAsync(DateTime.UtcNow.AddMinutes(-1), "PRIMARY");
        var service = CreateService();
        Assert.Null(NoteOf(await McpAnalysisTools.AnalyzeServer(service, _serverManager, null, 4)));
        Assert.Null(service.LastSecondaryReplicaNote);
    }

    /// <summary>
    /// #5558 round 2: <c>get_analysis_facts</c> returns facts the secondary filter already trimmed, so it carries the same
    /// field, always present and null when nothing was left to the primary, in every envelope it can answer with (no facts,
    /// facts without an observed window, the facts payload).
    /// </summary>
    [Fact]
    public async Task GetAnalysisFacts_CarriesTheNote_OnlyWhenThisNodeHoldsASecondaryCopy()
    {
        var service = CreateService();

        /* Nothing collected, no AG rows: an unavailable answer, the field there and null. */
        Assert.Null(NoteOf(await McpAnalysisTools.GetAnalysisFacts(service, _serverManager, null, 4)));

        /* A secondary copy, still nothing collected: the sentence is in the field and in the prose. */
        await SeedAgAsync(DateTime.UtcNow.AddMinutes(-1), "SECONDARY");
        var bare = await McpAnalysisTools.GetAnalysisFacts(service, _serverManager, null, 4);
        Assert.Equal(AgReplicaScope.SkippedNote(1), NoteOf(bare));
        using (var bareDoc = JsonDocument.Parse(bare))
            Assert.Contains(AgReplicaScope.SkippedNote(1)!, bareDoc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);

        /* Facts in the window: the facts payload carries it at the root. */
        for (var m = 5; m <= 55; m += 10) await PlantWaitAsync(DateTime.UtcNow.AddMinutes(-m), 150_000L);
        var withFacts = await McpAnalysisTools.GetAnalysisFacts(service, _serverManager, null, 4);
        using (var factsDoc = JsonDocument.Parse(withFacts))
        {
            Assert.True(factsDoc.RootElement.TryGetProperty("facts", out _), "expected the facts payload, got: " + withFacts[..Math.Min(withFacts.Length, 200)]);
            Assert.Equal(AgReplicaScope.SkippedNote(1), factsDoc.RootElement.GetProperty("secondary_replica_note").GetString());
        }
    }

    [Fact]
    public async Task GetAnalysisFacts_OnAPrimary_CarriesANullNote()
    {
        await SeedAgAsync(DateTime.UtcNow.AddMinutes(-1), "PRIMARY");
        for (var m = 5; m <= 55; m += 10) await PlantWaitAsync(DateTime.UtcNow.AddMinutes(-m), 150_000L);
        Assert.Null(NoteOf(await McpAnalysisTools.GetAnalysisFacts(CreateService(), _serverManager, null, 4)));
    }

    [Fact]
    public async Task CompareAnalysis_CarriesOneNotePerWindow_EachFromItsOwnRole()
    {
        var now = DateTime.UtcNow;
        var baselineEnd = now.AddHours(-24);

        /* The baseline window ended while this node was a secondary; the comparison window ends after a failover to
           primary. Facts in both windows keep the answer data-bearing. */
        await SeedAgAsync(baselineEnd.AddMinutes(-1), "SECONDARY");
        await SeedAgAsync(now.AddMinutes(-1), "PRIMARY");
        await PlantWaitAsync(baselineEnd.AddHours(-1), 500_000L);
        await PlantWaitAsync(now.AddHours(-1), 900_000L);

        var answer = await McpAnalysisTools.CompareAnalysis(CreateService(), _serverManager, null, 4, 28);

        using var doc = JsonDocument.Parse(answer);
        var root = doc.RootElement;
        var (baseline, comparison) = root.TryGetProperty("baseline", out var b)
            ? (b.GetProperty("secondary_replica_note"), root.GetProperty("comparison").GetProperty("secondary_replica_note"))
            : (root.GetProperty("hints").GetProperty("baseline_secondary_replica_note"), root.GetProperty("hints").GetProperty("comparison_secondary_replica_note"));
        Assert.Equal(AgReplicaScope.SkippedNote(1), baseline.GetString());
        Assert.Equal(JsonValueKind.Null, comparison.ValueKind);
    }

    [Fact]
    public async Task AnalyzeAsync_WithACancelledToken_AbandonsInsteadOfThrowing()
    {
        await SeedAgAsync(DateTime.UtcNow.AddMinutes(-1), "SECONDARY");
        var service = CreateService();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var now = DateTime.UtcNow;
        var context = new AnalysisContext
        {
            ServerId = _serverId,
            ServerName = "TestServer",
            TimeRangeStart = now.AddHours(-4),
            TimeRangeEnd = now,
            CancellationToken = cts.Token
        };

        /* The set is resolved inside the pass's try, so the abandonment takes the #2412/#2443 path: an empty list, no
           exception, the guard released, and nothing stamped as a finished pass. */
        var findings = await service.AnalyzeAsync(context);

        Assert.Empty(findings);
        Assert.False(service.IsAnalyzing);
        Assert.Null(service.LastAnalysisTime);
    }
}
