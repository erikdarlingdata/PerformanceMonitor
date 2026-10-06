/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Darling.Tests;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4348: Lite's MCP answers go through the statement filter. Each case plants the canary statement in the store
/// rows a tool reads, runs the tool RAW (the tool method alone, the control that proves the plant reached the output)
/// and FILTERED (the same call through a real in-process MCP server that registers the host's REAL call-tool filter
/// list, <see cref="McpHostService.AddCallToolFilters"/>), then asserts what the filtered text may hold.
/// </summary>
public sealed class StatementFilterLiteMcpTests : IClassFixture<SharedDuckDbFixture>, IAsyncDisposable
{
    private const string ServerName = "SsfLiteSrv";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly LocalDataService _service;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 910000;
    private ServerHost? _host;

    public StatementFilterLiteMcpTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-ssf-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = ServerName, IsEnabled = true };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
        _service = new LocalDataService(_duckDb);
    }

    public async ValueTask DisposeAsync()
    {
        if (_host is not null) await _host.DisposeAsync();
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    // ── the cases: one per planted read ──

    [Fact]
    public async Task GetActiveQueries_WithholdsTheCanaryStatement()
    {
        await SeedSnapshotAsync(77, StatementScrubCanary.CanaryStatement);
        await SeedSnapshotAsync(78, StatementScrubCanary.PlainStatement);

        string raw = await McpSessionTools.GetActiveQueries(_service, _serverManager, ServerName);
        string filtered = await CallFilteredAsync("get_active_queries", ("server_name", ServerName));
        AssertWithheld(raw, filtered);
    }

    [Fact]
    public async Task GetTopQueriesByCpu_WithholdsTheCanaryStatement()
    {
        await SeedQueryStatAsync("0xCANARY01", StatementScrubCanary.CanaryStatement, planXml: null);
        await SeedQueryStatAsync("0xPLAIN001", StatementScrubCanary.PlainStatement, planXml: null);

        string raw = await McpQueryTools.GetTopQueriesByCpu(_service, _serverManager, ServerName, hours_back: 24, top: 20);
        string filtered = await CallFilteredAsync("get_top_queries_by_cpu", ("server_name", ServerName));
        AssertWithheld(raw, filtered);
    }

    [Fact]
    public async Task GetBlockedProcessReports_WithholdsTheCanaryStatement()
    {
        await ExecAsync(@"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid,
     wait_time_ms, lock_mode, blocked_status, blocking_status, blocked_sql_text, blocking_sql_text)
VALUES ($1, $2, $3, $4, $2, 'SsfDb', 55, 66, 1000, 'X', 'suspended', 'running', $5, $6)",
            _nextId++, Naive(DateTime.UtcNow.AddMinutes(-5)), _serverId, ServerName,
            StatementScrubCanary.CanaryStatement, StatementScrubCanary.PlainStatement);

        string raw = await McpBlockingTools.GetBlockedProcessReports(_service, _serverManager, ServerName, full_text: true);
        string filtered = await CallFilteredAsync("get_blocked_process_reports", ("server_name", ServerName), ("full_text", true));
        AssertWithheld(raw, filtered);
    }

    [Fact]
    public async Task GetDeadlockDetail_WithholdsTheCanaryStatement_AlsoInTheCutPreview()
    {
        /* The canary sits where the 2000-character preview's cut lands inside the secret. */
        string graph = "<deadlock><victim-list><victimProcess id=\"p1\"/></victim-list><process-list>"
            + "<process id=\"p1\"><inputbuf>" + new string(' ', 1960) + StatementScrubCanary.CanaryStatement
            + "</inputbuf></process></process-list></deadlock>";
        await ExecAsync(@"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, victim_sql_text)
VALUES ($1, $2, $3, $4, $2, $5, $6)", _nextId++, Naive(DateTime.UtcNow.AddMinutes(-5)), _serverId, ServerName,
            graph, StatementScrubCanary.CanaryStatement);

        string raw = await McpBlockingTools.GetDeadlockDetail(_service, _serverManager, ServerName, full_graph: true);
        Assert.Contains("S3cret-canary-ssf", raw);
        foreach (bool full in new[] { false, true })
        {
            string filtered = await CallFilteredAsync("get_deadlock_detail", ("server_name", ServerName), ("full_graph", full));
            foreach (string needle in StatementScrubCanary.SecretNeedles) Assert.DoesNotContain(needle, filtered);
            Assert.DoesNotContain("PASSWORD", filtered, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(SensitiveStatements.PlaceholderText, filtered);
        }
    }

    [Fact]
    public async Task GetPlanXml_WithholdsTheCanaryStatement_AndKeepsTheRestOfThePlan()
    {
        string plan = StatementScrubCanary.CanaryPlan();
        await SeedQueryStatAsync("0xPLANCAN1", StatementScrubCanary.PlainStatement, plan);

        string raw = await McpPlanTools.GetPlanXml(_service, _serverManager, "0xPLANCAN1", ServerName);
        string filtered = await CallFilteredAsync("get_plan_xml", ("query_hash", "0xPLANCAN1"), ("server_name", ServerName));

        Assert.Contains("S3cret-canary-ssf", plan);
        /* The tool's own read is the filtered one (Layer 1), so even the tool method alone holds no secret. */
        foreach (string needle in StatementScrubCanary.SecretNeedles) Assert.DoesNotContain(needle, raw);
        AssertPlanFilteredKeepsTheRest(filtered);
    }

    [Fact]
    public async Task GetAlertHistory_WithholdsTheCanaryStatement()
    {
        await ExecAsync(@"
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, detail_text)
VALUES ($1, $2, $3, 'Blocking Detected', 1, 0, $4)", Naive(DateTime.UtcNow.AddMinutes(-5)), _serverId, ServerName,
            "Blocked: " + StatementScrubCanary.CanaryStatement);

        string raw = await McpAlertTools.GetAlertHistory(_service, 24);
        string filtered = await CallFilteredAsync("get_alert_history");
        AssertWithheld(raw, filtered);
    }

    // ── what the filter must leave alone ──

    [Fact]
    public async Task ACleanAnswer_ComesBackUnchanged()
    {
        await SeedSnapshotAsync(78, StatementScrubCanary.PlainStatement);

        /* as_of pins the window's clock, so the two answers differ only by the filter. */
        string asOf = DateTime.UtcNow.ToString("o");
        string raw = await McpSessionTools.GetActiveQueries(_service, _serverManager, ServerName, as_of: asOf);
        string filtered = await CallFilteredAsync("get_active_queries", ("server_name", ServerName), ("as_of", asOf));

        Assert.Equal(raw, filtered);
    }

    // ── plumbing ──

    private static void AssertWithheld(string raw, string filtered)
    {
        Assert.Contains("S3cret-canary-ssf", raw);
        foreach (string needle in StatementScrubCanary.SecretNeedles) Assert.DoesNotContain(needle, filtered);
        Assert.Contains(SensitiveStatements.PlaceholderText, filtered);
        foreach (string kept in StatementScrubCanary.KeptNeedles.Where(k => raw.Contains(k, StringComparison.Ordinal)))
            Assert.Contains(kept, filtered);
    }

    private static void AssertPlanFilteredKeepsTheRest(string filteredPlan)
    {
        foreach (string needle in StatementScrubCanary.SecretNeedles) Assert.DoesNotContain(needle, filteredPlan);
        var doc = System.Xml.Linq.XDocument.Parse(filteredPlan);
        var statements = doc.Descendants().Where(e => e.Name.LocalName == "StmtSimple")
            .Select(e => (string?)e.Attribute("StatementText")).ToList();
        Assert.Equal(SensitiveStatements.PlaceholderText, statements[0]);
        Assert.Equal(StatementScrubCanary.PlainStatement, statements[1]);
        Assert.Contains(doc.Descendants(), e => (string?)e.Attribute("ParameterCompiledValue") == "N'param-canary-ssf'");
    }

    private async Task<string> CallFilteredAsync(string tool, params (string Key, object Value)[] args)
    {
        _host ??= await ServerHost.StartAsync(_service, _serverManager);
        var arguments = args.ToDictionary(a => a.Key, a => (object?)a.Value);
        CallToolResult result = await _host.Client.CallToolAsync(tool, arguments);
        return string.Concat(result.Content.OfType<TextContentBlock>().Select(b => b.Text));
    }

    private Task SeedSnapshotAsync(int sessionId, string queryText) => ExecAsync(@"
INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status)
VALUES ($1, $2, $3, $4, $5, 'SsfDb', $6, 'running')",
        _nextId++, Naive(DateTime.UtcNow.AddMinutes(-5)), _serverId, ServerName, sessionId, queryText);

    private Task SeedQueryStatAsync(string queryHash, string queryText, string? planXml) => ExecAsync(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, query_hash, query_text, query_plan_xml, database_name,
     execution_count, total_elapsed_time, total_worker_time, total_logical_reads, total_logical_writes, total_physical_reads,
     delta_execution_count, delta_elapsed_time, delta_worker_time, delta_logical_reads, delta_logical_writes, delta_physical_reads,
     delta_rows, delta_spills, last_execution_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'SsfDb', 10, 1000, 1000, 0, 0, 0, 10, 1000, 1000, 0, 0, 0, 0, 0, $2)",
        _nextId++, Naive(DateTime.UtcNow.AddMinutes(-5)), _serverId, ServerName, queryHash, queryText, (object?)planXml ?? DBNull.Value);

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    private async Task ExecAsync(string sql, params object[] parameters)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var p in parameters) cmd.Parameters.Add(new DuckDBParameter { Value = p });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>A real in-process MCP server with Lite's tool classes, the real data service, and the host's REAL call-tool
    /// filter list.</summary>
    private sealed class ServerHost : IAsyncDisposable
    {
        private readonly ServiceProvider _provider;
        private readonly CancellationTokenSource _stop;
        private readonly Task _run;

        private ServerHost(ServiceProvider provider, CancellationTokenSource stop, Task run, McpClient client)
        {
            _provider = provider;
            _stop = stop;
            _run = run;
            Client = client;
        }

        public McpClient Client { get; }

        public static async Task<ServerHost> StartAsync(LocalDataService data, ServerManager servers)
        {
            var c2s = new Pipe();
            var s2c = new Pipe();
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton(data);
            services.AddSingleton(servers);
            var builder = services.AddMcpServer()
                .WithStreamServerTransport(c2s.Reader.AsStream(), s2c.Writer.AsStream())
                .WithTools<McpSessionTools>()
                .WithTools<McpQueryTools>()
                .WithTools<McpBlockingTools>()
                .WithTools<McpPlanTools>()
                .WithTools<McpAlertTools>();
            builder.WithRequestFilters(McpHostService.AddCallToolFilters);

            var provider = services.BuildServiceProvider();
            var stop = new CancellationTokenSource();
            var run = provider.GetRequiredService<McpServer>().RunAsync(stop.Token);
            var client = await McpClient.CreateAsync(new StreamClientTransport(c2s.Writer.AsStream(), s2c.Reader.AsStream()));
            return new ServerHost(provider, stop, run, client);
        }

        public async ValueTask DisposeAsync()
        {
            await Client.DisposeAsync();
            _stop.Cancel();
            await _provider.DisposeAsync();
            try { await _run; } catch (Exception) { /* shutdown */ }
        }
    }
}
