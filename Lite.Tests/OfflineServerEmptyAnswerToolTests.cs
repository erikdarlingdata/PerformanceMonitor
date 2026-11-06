/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7167: a Lite MCP read over a window that was never collected must not answer <c>empty</c>. A server with no successful
/// collection up to the window's end answers <c>unavailable</c> with the offline sentence first (the rule Darling's
/// <c>DarlingServerOfflineNote.EmptyAsync</c> applies), and a server collected a minute ago keeps its true-negative <c>empty</c>.
/// One theory case per tool family that reaches the helper: the shared window-notice wrappers, the event lists, the snapshot
/// lists, the config histories, the query trends and the Query Store regressions.
///
/// <para>Each case writes real <c>collection_log</c> rows into a throwaway DuckDB file (the shared fixture's temp folder) and
/// reads through the public tool method. The offline stamp is three days back, with a fresh ERROR row beside it: a server Lite
/// cannot reach logs failed runs every cycle, and they must not hold it "fresh".</para>
/// </summary>
public sealed class OfflineServerEmptyAnswerToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "OfflineEmptySrv";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 910000;

    public OfflineServerEmptyAnswerToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-offlineempty-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = ServerName, IsEnabled = true };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    public static TheoryData<string> Families => new()
    {
        "memory_trend",
        "query_duration_trend",
        "deadlocks",
        "blocked_process_reports",
        "active_queries",
        "waiting_tasks",
        "server_config_changes",
        "default_trace_events",
        "long_query_completions",
        "memory_pressure_events",
        "plan_corrections",
        "cpu_by_database",
        "blocking_stats",
    };

    [Theory]
    [MemberData(nameof(Families))]
    public async Task ServerWithNoRecentSuccess_AnswersUnavailable_NotEmpty(string family)
    {
        await SeedAsync(DateTime.UtcNow.AddDays(-3));
        /* The server is being tried every cycle and failing: only SUCCESS counts as a collection. */
        await SeedCollectionLogAsync("wait_stats", DateTime.UtcNow.AddMinutes(-1), "ERROR");

        var root = JsonDocument.Parse(await ReadAsync(family)).RootElement;

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.StartsWith(ServerName + " is offline: the store has had no successful collection from it since ", message, StringComparison.Ordinal);
        Assert.DoesNotContain("genuinely", message, StringComparison.Ordinal);
        /* Darling's offline answer is the bare status and message, with no window keys (DarlingServerOfflineNote.OfflineInsteadOfEmpty). */
        Assert.Equal(new[] { "message", "status" }, root.EnumerateObject().Select(p => p.Name).OrderBy(n => n, StringComparer.Ordinal));
    }

    /// <summary>
    /// #7724: a server that went offline partway through a wide window. Its newest success is three days back, inside a 7-day window, so the
    /// empty reads and both trends say what was collected through that instant and that nothing after is known. None calls the window a genuine all-clear.
    /// </summary>
    [Theory]
    [InlineData("deadlocks")]
    [InlineData("blocked_process_reports")]
    [InlineData("blocking_trend")]
    [InlineData("deadlock_trend")]
    public async Task OfflineServer_WithACollectionInTheWindow_SaysWhatWasCollectedThrough(string family)
    {
        var last = DateTime.UtcNow.AddDays(-3);
        await SeedAsync(last);
        await SeedCollectionLogAsync("wait_stats", DateTime.UtcNow.AddMinutes(-1), "ERROR");

        var service = new LocalDataService(_duckDb);
        ServerResolver.WireOfflineLookups(service, _serverManager);
        var answer = family switch
        {
            "deadlocks" => await McpBlockingTools.GetDeadlocks(service, _serverManager, ServerName, 168),
            "blocked_process_reports" => await McpBlockingTools.GetBlockedProcessReports(service, _serverManager, ServerName, 168),
            "blocking_trend" => await McpBlockingTools.GetBlockingTrend(service, _serverManager, ServerName, 168),
            _ => await McpBlockingTools.GetDeadlockTrend(service, _serverManager, ServerName, 168),
        };

        var root = JsonDocument.Parse(answer).RootElement;
        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        /* The offline lead first (an MCP caller still learns the server is offline), then the sentence; the note is the sentence alone. */
        Assert.Contains(" is offline: ", message, StringComparison.Ordinal);
        Assert.Contains(". Nothing was recorded through the last collection at ", message, StringComparison.Ordinal);
        Assert.EndsWith(". Nothing after that is known.", message, StringComparison.Ordinal);
        Assert.StartsWith("Nothing was recorded through the last collection at ", root.GetProperty("collected_through_note").GetString(), StringComparison.Ordinal);
        var through = DateTime.Parse(root.GetProperty("collected_through").GetString()!, System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal);
        Assert.InRange((through - last).TotalSeconds, -2, 2);
        Assert.DoesNotContain("genuine all-clear", answer, StringComparison.Ordinal);
    }

    private const string UnavailableEnvelope = "{\"status\":\"unavailable\",\"message\":\"The procedure stats collector has not run in this window.\"}";

    /// <summary>#7724: an <c>unavailable</c> answer for a server that is offline as of the window end gets the offline lead in front of its message, and stays <c>unavailable</c>.</summary>
    [Fact]
    public async Task WithOfflineLead_OfflineServer_PutsTheLeadBeforeTheMessage()
    {
        await SeedAsync(DateTime.UtcNow.AddDays(-3));
        await SeedCollectionLogAsync("wait_stats", DateTime.UtcNow.AddMinutes(-1), "ERROR");
        var service = new LocalDataService(_duckDb);
        ServerResolver.WireOfflineLookups(service, _serverManager);

        var answer = await McpQueryTools.WithOfflineLeadAsync(service, _serverId, DateTime.UtcNow, UnavailableEnvelope);

        var root = JsonDocument.Parse(answer).RootElement;
        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.StartsWith(ServerName + " is offline: the store has had no successful collection from it since ", message, StringComparison.Ordinal);
        Assert.EndsWith(" The procedure stats collector has not run in this window.", message, StringComparison.Ordinal);
        Assert.True(message.IndexOf(" is offline: ", StringComparison.Ordinal) < message.IndexOf("The procedure stats collector", StringComparison.Ordinal));
    }

    /// <summary>#7724: a server that is not offline gets its envelope back byte for byte.</summary>
    [Fact]
    public async Task WithOfflineLead_ServerCollectedJustNow_ReturnsTheAnswerUnchanged()
    {
        await SeedAsync(DateTime.UtcNow.AddMinutes(-1));
        var service = new LocalDataService(_duckDb);
        ServerResolver.WireOfflineLookups(service, _serverManager);

        Assert.Equal(UnavailableEnvelope, await McpQueryTools.WithOfflineLeadAsync(service, _serverId, DateTime.UtcNow, UnavailableEnvelope));
    }

    /// <summary>#7724: only an <c>unavailable</c> answer takes the lead. An <c>empty</c> answer, a data answer and an answer that does not parse or has no string message are left alone, offline server or not.</summary>
    [Theory]
    [InlineData("{\"status\":\"empty\",\"message\":\"No procedure ran.\"}")]
    [InlineData("{\"status\":\"ok\",\"message\":\"Top procedures.\",\"rows\":[]}")]
    [InlineData("{\"rows\":[]}")]
    [InlineData("{\"status\":\"unavailable\"}")]
    [InlineData("{\"status\":\"unavailable\",\"message\":42}")]
    [InlineData("{\"status\":\"unavailable\",\"message\":\"cut off")]
    [InlineData("not json at all")]
    [InlineData("")]
    public async Task WithOfflineLead_OfflineServer_LeavesEveryOtherAnswerUnchanged(string answer)
    {
        await SeedAsync(DateTime.UtcNow.AddDays(-3));
        await SeedCollectionLogAsync("wait_stats", DateTime.UtcNow.AddMinutes(-1), "ERROR");
        var service = new LocalDataService(_duckDb);
        ServerResolver.WireOfflineLookups(service, _serverManager);

        Assert.Equal(answer, await McpQueryTools.WithOfflineLeadAsync(service, _serverId, DateTime.UtcNow, answer));
    }

    /// <summary>#7724: the Top Procedures tool itself. A server offline as of the window end, with no procedure stats in the window, answers <c>unavailable</c> with the offline lead first.</summary>
    [Fact]
    public async Task TopProcedures_OnAnOfflineServerWithNoProcedureStats_OpensWithTheOfflineLead()
    {
        await SeedAsync(DateTime.UtcNow.AddDays(-3));
        await SeedCollectionLogAsync("wait_stats", DateTime.UtcNow.AddMinutes(-1), "ERROR");
        var service = new LocalDataService(_duckDb);
        ServerResolver.WireOfflineLookups(service, _serverManager);

        var root = JsonDocument.Parse(await McpQueryTools.GetTopProceduresByCpu(service, _serverManager, ServerName, 4)).RootElement;

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        Assert.StartsWith(ServerName + " is offline: ", root.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(Families))]
    public async Task ServerCollectedJustNow_KeepsItsTrueNegativeEmpty(string family)
    {
        await SeedAsync(DateTime.UtcNow.AddMinutes(-1));

        var root = JsonDocument.Parse(await ReadAsync(family)).RootElement;

        Assert.Equal("empty", root.GetProperty("status").GetString());
    }

    /// <summary>An <c>as_of</c> window that ends inside collected history is a true negative even though the server has gone quiet since.</summary>
    [Fact]
    public async Task AsOfInsideCollectedHistory_StaysEmpty_WhileANewerWindowIsUnavailable()
    {
        var lastSuccess = DateTime.UtcNow.AddDays(-3);
        await SeedAsync(lastSuccess);
        var service = new LocalDataService(_duckDb);

        var inside = JsonDocument.Parse(await McpBlockingTools.GetDeadlocks(
            service, _serverManager, ServerName, 4, as_of: lastSuccess.AddHours(-1).ToString("o"))).RootElement;
        var after = JsonDocument.Parse(await McpBlockingTools.GetDeadlocks(
            service, _serverManager, ServerName, 4, as_of: lastSuccess.AddHours(2).ToString("o"))).RootElement;

        Assert.Equal("empty", inside.GetProperty("status").GetString());
        Assert.Equal("unavailable", after.GetProperty("status").GetString());
    }

    /// <summary>#7459: "nothing collected in this window" says unavailable by itself, and on a server that has stopped being collected it still says why.</summary>
    [Fact]
    public async Task HeatmapWithNothingCollectedInTheWindow_OnAnOfflineServer_SaysItIsOffline()
    {
        await SeedAsync(DateTime.UtcNow.AddDays(-3));
        var service = new LocalDataService(_duckDb);
        ServerResolver.WireOfflineLookups(service, _serverManager);

        var root = JsonDocument.Parse(await McpQueryTools.GetQueryHeatmap(service, _serverManager, ServerName, 1)).RootElement;

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.StartsWith(ServerName + " is offline: ", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Widen hours_back", message, StringComparison.Ordinal);
    }

    /// <summary>A server with no successful collection on record keeps the tools' own answers (never collected is not offline).</summary>
    [Fact]
    public async Task ServerNeverCollected_IsNotCalledOffline()
    {
        var root = JsonDocument.Parse(await ReadAsync("deadlocks")).RootElement;

        Assert.Equal("empty", root.GetProperty("status").GetString());
    }

    /// <summary>
    /// A server added before the log's retention horizon that has only ever logged failed runs has no SUCCESS row to find. Darling reads it
    /// Offline through its registration, so Lite answers the same: <c>unavailable</c>, not a false "nothing to find". One added minutes ago is
    /// still waiting for its first collection and keeps the tools' own answer.
    /// </summary>
    [Theory]
    [InlineData(-400, "unavailable")]
    [InlineData(-0.01, "empty")]
    public async Task ServerNeverSucceeded_IsOfflineOnlyWhenAddedBeforeTheRetentionHorizon(double addedDaysAgo, string expected)
    {
        var name = "OfflineNeverSucceeded" + Guid.NewGuid().ToString("N")[..6];
        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(), ServerName = name, IsEnabled = true, CreatedDate = DateTime.Now.AddDays(addedDaysAgo),
        };
        _serverManager.AddServer(server);
        var id = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
        await SeedCollectionLogAsync("wait_stats", DateTime.UtcNow.AddMinutes(-1), "ERROR", id, name);
        var service = new LocalDataService(_duckDb);
        ServerResolver.WireOfflineLookups(service, _serverManager);

        var root = JsonDocument.Parse(await McpBlockingTools.GetDeadlocks(service, _serverManager, name, 4)).RootElement;

        Assert.Equal(expected, root.GetProperty("status").GetString());
        if (expected == "unavailable")
        {
            Assert.StartsWith(name + " is offline: the store holds no successful collection from it", root.GetProperty("message").GetString(), StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The analysis reads run their window probe on a <see cref="LocalDataService"/> of their own, not the host's. The same 400-day-old server
    /// with only failed runs must answer <c>unavailable</c> there too, as Darling's <c>get_analysis_findings</c> does, and one added minutes ago
    /// keeps <c>empty</c>.
    /// </summary>
    [Theory]
    [InlineData(-400, "unavailable")]
    [InlineData(-0.01, "empty")]
    public async Task AnalysisFindings_ForAServerThatNeverSucceeded_AreOfflineOnlyWhenAddedBeforeTheRetentionHorizon(double addedDaysAgo, string expected)
    {
        var name = "OfflineAnalysis" + Guid.NewGuid().ToString("N")[..6];
        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(), ServerName = name, IsEnabled = true, CreatedDate = DateTime.Now.AddDays(addedDaysAgo),
        };
        _serverManager.AddServer(server);
        var id = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
        await SeedCollectionLogAsync("wait_stats", DateTime.UtcNow.AddMinutes(-1), "ERROR", id, name);

        var root = JsonDocument.Parse(await McpAnalysisTools.GetAnalysisFindings(
            new AnalysisService(_duckDb) { MinimumDataHours = 0 }, _serverManager, name, 4)).RootElement;

        Assert.Equal(expected, root.GetProperty("status").GetString());
        if (expected == "unavailable")
        {
            Assert.StartsWith(name + " is offline: the store holds no successful collection from it", root.GetProperty("message").GetString(), StringComparison.Ordinal);
        }
    }

    /// <summary>The host wires the registration lookup onto the data service it serves, so a tool call reaches it with no per-call setup.</summary>
    [Fact]
    public void McpHost_WiresTheRegistrationLookup_OntoItsDataService()
    {
        var service = new LocalDataService(_duckDb);
        Assert.Null(service.RegisteredAtUtcLookup);

        _ = new McpHostService(
            service, _serverManager, new MuteRuleService(new AlertWebhookCancelTests.NoMuteRules(), new AppLoggerAdapter<MuteRuleService>()),
            _duckDb, 0, DispatchProxy.Create<IInvestigationRequestStore, McpHostGateLiveTests.NoStore>(), "t", false);

        Assert.NotNull(service.RegisteredAtUtcLookup!(_serverId));
        Assert.Null(service.RegisteredAtUtcLookup(-1));
        Assert.Equal(ServerName, service.ServerDisplayNameLookup!(_serverId));
        Assert.Null(service.ServerDisplayNameLookup(-1));
    }

    private async Task<string> ReadAsync(string family)
    {
        var service = new LocalDataService(_duckDb);
        ServerResolver.WireOfflineLookups(service, _serverManager);
        return family switch
        {
            "memory_trend" => await McpMemoryTools.GetMemoryTrend(service, _serverManager, ServerName, 4),
            "query_duration_trend" => await McpQueryTools.GetQueryDurationTrend(service, _serverManager, ServerName, 4),
            "deadlocks" => await McpBlockingTools.GetDeadlocks(service, _serverManager, ServerName, 4),
            "blocked_process_reports" => await McpBlockingTools.GetBlockedProcessReports(service, _serverManager, ServerName, 4),
            "active_queries" => await McpSessionTools.GetActiveQueries(service, _serverManager, ServerName, 4),
            "waiting_tasks" => await McpWaitTools.GetWaitingTasks(service, _serverManager, ServerName, 4),
            "server_config_changes" => await McpConfigHistoryTools.GetServerConfigChanges(service, _serverManager, ServerName, 4),
            "default_trace_events" => await McpDefaultTraceTools.GetDefaultTraceEvents(service, _serverManager, ServerName, 4),
            "long_query_completions" => await McpLongQueryTools.GetLongQueryCompletions(service, _serverManager, ServerName, 4),
            "memory_pressure_events" => await McpMemoryTools.GetMemoryPressureEvents(service, _serverManager, ServerName, 4),
            "plan_corrections" => await McpPlanCorrectionTools.GetPlanCorrections(service, _serverManager, ServerName, 4),
            "cpu_by_database" => await McpWhoDroveItTools.GetCpuByDatabase(service, _serverManager, ServerName, 4),
            "blocking_stats" => await McpHealthTools.GetBlockingStats(service, _serverManager, ServerName, 4),
            _ => throw new ArgumentOutOfRangeException(nameof(family), family, null),
        };
    }

    /// <summary>
    /// The server's last successful run at <paramref name="lastSuccessUtc"/> for every collector the cases read, plus one row of
    /// memory and query history 48 hours back so the trend tools' "has been sampled before" answer (a quiet window) is the one in play.
    /// </summary>
    private async Task SeedAsync(DateTime lastSuccessUtc)
    {
        foreach (var collector in new[]
        {
            "cpu_utilization", "memory_stats", "query_stats", "deadlocks", "blocked_process_report", "query_snapshots", "waiting_tasks",
            "server_config", "default_trace_events", "long_query_completions", "memory_pressure_events", "plan_correction", "wait_stats",
        })
        {
            await SeedCollectionLogAsync(collector, lastSuccessUtc, "SUCCESS");
        }

        await SeedMemoryAsync(DateTime.UtcNow.AddHours(-48));
        await SeedQueryAsync(DateTime.UtcNow.AddHours(-48));
    }

    private async Task SeedCollectionLogAsync(string collector, DateTime collectionTimeUtc, string status, int? serverId = null, string? serverName = null)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
VALUES ($1, $2, $3, $4, $5, 10, $6, 1)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId ?? _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverName ?? ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        await cmd.ExecuteNonQueryAsync();
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

    private async Task SeedMemoryAsync(DateTime collectionTimeUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name,
     total_physical_memory_mb, available_physical_memory_mb,
     target_server_memory_mb, total_server_memory_mb, buffer_pool_mb, plan_cache_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = 65536.0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 8192.0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 49152.0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 40000.0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 35000.0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 5000.0 });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedQueryAsync(DateTime collectionTimeUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_hash, sql_handle, last_execution_time, delta_execution_count,
     delta_worker_time, delta_elapsed_time, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
        var naive = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified);
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = naive });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = "AppDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xOFFLINEEMPTYHASH" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xSQLH" });
        cmd.Parameters.Add(new DuckDBParameter { Value = naive });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 20000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT * FROM Orders" });
        await cmd.ExecuteNonQueryAsync();
    }
}
