/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Darling.Tests;
using Lite.Tests.Helpers;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4348 (statement filter), the end-to-end proof for Lite's DuckDB store: a new row holds the marker, and an old row
/// reads back as the marker without being changed.
///
/// <para><b>Collect.</b> The canary goes in as the rows a monitored server would return, and
/// <c>RemoteCollectorService.RunCollectorDefinitionAsync</c> runs the real definition against a real DuckDB file: read,
/// filter, delta, dedupe, append. query_stats and deadlocks take the runner's Azure per-database loop, the only loop
/// with a reader seam. query_snapshots takes the server-wide loop, which opens a real SQL Server connection, so that case
/// drives the same definition and the same appender the runner uses (<c>ReadAsync</c>, then the appender row writer).</para>
///
/// <para><b>Read.</b> Rows are planted straight into the tables, as rows stored before the upgrade would be. Each tool
/// reads RAW (the control: the canary must still be there) and then through a real in-process MCP server that registers
/// the host's real call-tool filter list. The answer shows the marker and no secret needle. The planted table's bytes
/// are hashed before the reads and after: a read never rewrites a stored row.</para>
/// </summary>
public sealed class StatementScrubLiteCollectionTests : IClassFixture<SharedDuckDbFixture>, IAsyncDisposable
{
    private const string ServerName = "SsfE2eLiteSrv";
    private const string Db = "SsfE2eDb";
    private static readonly DateTime Anchor = new(2031, 6, 10, 12, 0, 0, DateTimeKind.Unspecified);

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly ServerConnection _server;
    private readonly RemoteCollectorService _collector;
    private readonly LocalDataService _service;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 930000;
    private McpHost? _host;

    public StatementScrubLiteCollectionTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-ssf-e2e-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);
        _server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = ServerName, DisplayName = ServerName, IsEnabled = true };
        _serverManager.AddServer(_server);

        /* Azure SQL Database engine edition, so query_stats and deadlocks take the per-database loop. */
        _serverManager.GetConnectionStatus(_server.Id).SqlEngineEdition = 5;
        _serverId = RemoteCollectorService.GetServerId(_server);
        _collector = new RemoteCollectorService(_duckDb, _serverManager, new ScheduleManager(_configDir));
        _service = new LocalDataService(_duckDb);
    }

    public async ValueTask DisposeAsync()
    {
        if (_host is not null) await _host.DisposeAsync();
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    // ── collect ──

    [Fact]
    public async Task QueryStats_TheCanaryThroughTheRunner_IsStoredAsTheMarker_AndThePlainStatementIsKept()
    {
        _collector.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "alpha" });
        _collector.AzureDatabaseReaderOverrideForTests = (_, _) => QueryStatsReader();

        await _collector.RunCollectorDefinitionAsync(QueryStatsCollector.Instance, _server, CancellationToken.None);

        var rows = await QueryAsync("SELECT query_hash || '|' || COALESCE(query_text, '<null>') FROM query_stats WHERE server_id = " + _serverId + " ORDER BY query_hash");
        Assert.Equal(new[] { "0xC0|" + SensitiveStatements.PlaceholderText, "0xP0|" + StatementScrubCanary.PlainStatement }, rows);
        AssertNoSecret(await WholeTableTextAsync("query_stats"));
    }

    [Fact]
    public async Task Deadlocks_TheCanaryThroughTheRunner_IsStoredAsTheMarker_InTheVictimTextAndTheGraph()
    {
        _collector.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "alpha" });
        _collector.AzureDatabaseReaderOverrideForTests = (_, _) => DeadlocksReader();

        await _collector.RunCollectorDefinitionAsync(DeadlocksCollector.Instance, _server, CancellationToken.None);

        var victims = await QueryAsync("SELECT COALESCE(victim_sql_text, '<null>') FROM deadlocks WHERE server_id = " + _serverId + " ORDER BY deadlock_time");
        Assert.Equal(new[] { SensitiveStatements.PlaceholderText, StatementScrubCanary.PlainStatement }, victims);

        var graphs = await QueryAsync("SELECT deadlock_graph_xml FROM deadlocks WHERE server_id = " + _serverId + " ORDER BY deadlock_time");
        foreach (string needle in StatementScrubCanary.SecretNeedles) Assert.DoesNotContain(needle, graphs[0], StringComparison.Ordinal);
        Assert.Contains(SensitiveStatements.PlaceholderText, graphs[0], StringComparison.Ordinal);
        Assert.Contains(StatementScrubCanary.PlainStatement, graphs[1], StringComparison.Ordinal);
        AssertNoSecret(await WholeTableTextAsync("deadlocks"));
    }

    [Fact]
    public async Task QuerySnapshots_TheCanaryThroughTheDefinitionAndTheAppender_IsStoredAsTheMarker_AndBothPlansAreFiltered()
    {
        var definition = QuerySnapshotsCollector.Instance;
        var context = new CollectorContext
        {
            ServerId = _serverId,
            ServerName = ServerName,
            CollectionTime = Anchor,
            Deltas = new RecordingCollectorDeltaCalculator(),
            Target = new CollectorTargetInfo(),
        };

        using (var reader = SnapshotsReader())
        {
            var read = await definition.ReadAsync(reader, context, CancellationToken.None);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var appender = connection.CreateAppender(definition.TargetTable);
            foreach (var row in read)
            {
                var appenderRow = appender.CreateRow();
                if (definition.IncludesCollectionId) appenderRow.AppendValue(_nextId++);
                appenderRow.AppendValue(context.CollectionTime).AppendValue(context.ServerId).AppendValue(context.ServerName);
                definition.WritePayload(row, new AppenderCollectorRowWriter { CurrentRow = appenderRow }, context);
                appenderRow.EndRow();
            }
        }

        var rows = await QueryAsync("SELECT CAST(session_id AS VARCHAR) || '|' || COALESCE(query_text, '<null>') FROM query_snapshots WHERE server_id = " + _serverId + " ORDER BY session_id");
        Assert.Equal(new[] { "81|" + SensitiveStatements.PlaceholderText, "82|" + StatementScrubCanary.PlainStatement }, rows);

        foreach (string column in new[] { "query_plan", "live_query_plan" })
        {
            var plans = await QueryAsync("SELECT " + column + " FROM query_snapshots WHERE server_id = " + _serverId + " AND session_id = 81 AND " + column + " IS NOT NULL");
            string plan = Assert.Single(plans);
            AssertPlanFilteredKeepsTheRest(plan);
        }

        AssertNoSecret(await WholeTableTextAsync("query_snapshots"));
    }

    // ── read ──

    [Fact]
    public async Task QuerySnapshots_ARowStoredBeforeTheUpgrade_ReadsBackAsTheMarker_AndTheStoredRowIsUnchanged()
    {
        await ExecAsync(@"
INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status)
VALUES ($1, $2, $3, $4, 77, 'SsfDb', $5, 'running')", _nextId++, Naive(DateTime.UtcNow.AddMinutes(-5)), _serverId, ServerName, StatementScrubCanary.CanaryStatement);
        await ExecAsync(@"
INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status)
VALUES ($1, $2, $3, $4, 78, 'SsfDb', $5, 'running')", _nextId++, Naive(DateTime.UtcNow.AddMinutes(-5)), _serverId, ServerName, StatementScrubCanary.PlainStatement);
        string before = await TableHashAsync("query_snapshots");

        string raw = await McpSessionTools.GetActiveQueries(_service, _serverManager, ServerName);
        string filtered = await CallFilteredAsync("get_active_queries", ("server_name", ServerName));
        AssertWithheld(raw, filtered);

        Assert.Equal(before, await TableHashAsync("query_snapshots"));
    }

    [Fact]
    public async Task QueryStats_ARowStoredBeforeTheUpgrade_ReadsBackAsTheMarker_TextAndPlan_AndTheStoredRowIsUnchanged()
    {
        await SeedQueryStatAsync("0xCANARY01", StatementScrubCanary.CanaryStatement, StatementScrubCanary.CanaryPlan());
        await SeedQueryStatAsync("0xPLAIN001", StatementScrubCanary.PlainStatement, null);
        string before = await TableHashAsync("query_stats");

        string raw = await McpQueryTools.GetTopQueriesByCpu(_service, _serverManager, ServerName, hours_back: 24, top: 20);
        string filtered = await CallFilteredAsync("get_top_queries_by_cpu", ("server_name", ServerName));
        AssertWithheld(raw, filtered);

        string filteredPlan = await CallFilteredAsync("get_plan_xml", ("query_hash", "0xCANARY01"), ("server_name", ServerName));
        AssertPlanFilteredKeepsTheRest(filteredPlan);

        Assert.Equal(before, await TableHashAsync("query_stats"));
    }

    [Fact]
    public async Task Deadlocks_ARowStoredBeforeTheUpgrade_ReadsBackAsTheMarker_AndTheStoredRowIsUnchanged()
    {
        await ExecAsync(@"
INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, victim_sql_text)
VALUES ($1, $2, $3, $4, $2, $5, $6)", _nextId++, Naive(DateTime.UtcNow.AddMinutes(-5)), _serverId, ServerName,
            Graph(StatementScrubCanary.CanaryStatement, StatementScrubCanary.PlainStatement), StatementScrubCanary.CanaryStatement);
        string before = await TableHashAsync("deadlocks");

        string raw = await McpBlockingTools.GetDeadlockDetail(_service, _serverManager, ServerName, full_graph: true);
        string filtered = await CallFilteredAsync("get_deadlock_detail", ("server_name", ServerName), ("full_graph", true));
        AssertWithheld(raw, filtered);

        Assert.Equal(before, await TableHashAsync("deadlocks"));
    }

    // ── plumbing ──

    private static void AssertNoSecret(string text)
    {
        foreach (string needle in StatementScrubCanary.SecretNeedles) Assert.DoesNotContain(needle, text, StringComparison.Ordinal);
    }

    private static void AssertWithheld(string raw, string filtered)
    {
        Assert.Contains("S3cret-canary-ssf", raw, StringComparison.Ordinal);
        AssertNoSecret(filtered);
        Assert.Contains(SensitiveStatements.PlaceholderText, filtered, StringComparison.Ordinal);
        foreach (string kept in StatementScrubCanary.KeptNeedles.Where(k => raw.Contains(k, StringComparison.Ordinal)))
            Assert.Contains(kept, filtered, StringComparison.Ordinal);
    }

    private static void AssertPlanFilteredKeepsTheRest(string filteredPlan)
    {
        AssertNoSecret(filteredPlan);
        var doc = System.Xml.Linq.XDocument.Parse(filteredPlan);
        var statements = doc.Descendants().Where(e => e.Name.LocalName == "StmtSimple")
            .Select(e => (string?)e.Attribute("StatementText")).ToList();
        Assert.Equal(SensitiveStatements.PlaceholderText, statements[0]);
        Assert.Equal(StatementScrubCanary.PlainStatement, statements[1]);
        Assert.Contains(doc.Descendants(), e => (string?)e.Attribute("ParameterCompiledValue") == "N'param-canary-ssf'");
    }

    private static string Graph(string victimText, string otherText) =>
        "<deadlock><victim-list><victimProcess id=\"p1\"/></victim-list><process-list>"
        + "<process id=\"p1\" spid=\"55\" waittime=\"100\" lockMode=\"X\" currentdbname=\"" + Db + "\"><executionStack/><inputbuf>"
        + System.Security.SecurityElement.Escape(victimText) + "</inputbuf></process>"
        + "<process id=\"p2\" spid=\"56\" waittime=\"200\" lockMode=\"S\" currentdbname=\"" + Db + "\"><executionStack/><inputbuf>"
        + System.Security.SecurityElement.Escape(otherText) + "</inputbuf></process>"
        + "</process-list><resource-list/></deadlock>";

    private async Task<string> CallFilteredAsync(string tool, params (string Key, object Value)[] args)
    {
        _host ??= await McpHost.StartAsync(_service, _serverManager);
        var arguments = args.ToDictionary(a => a.Key, a => (object?)a.Value);
        CallToolResult result = await _host.Client.CallToolAsync(tool, arguments, cancellationToken: TestContext.Current.CancellationToken);
        return string.Concat(result.Content.OfType<TextContentBlock>().Select(b => b.Text));
    }

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
            await _seedConn.OpenAsync(TestContext.Current.CancellationToken);
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var p in parameters) cmd.Parameters.Add(new DuckDBParameter { Value = p });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task<List<string>> QueryAsync(string sql)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        var list = new List<string>();
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            list.Add(reader.IsDBNull(0) ? "<null>" : reader.GetString(0));
        }

        return list;
    }

    /// <summary>Every column of every row of one server in a table, as one text, for a whole-row secret scan.</summary>
    private async Task<string> WholeTableTextAsync(string table) =>
        string.Concat(await QueryAsync($"SELECT CAST(t AS VARCHAR) FROM {table} t WHERE server_id = {_serverId}"));

    /// <summary>A hash of the bytes of every row of one server in a table: equal before and after means no read rewrote a row.</summary>
    private async Task<string> TableHashAsync(string table) =>
        Assert.Single(await QueryAsync(
            $"SELECT md5(COALESCE(string_agg(CAST(t AS VARCHAR), '|' ORDER BY CAST(t AS VARCHAR)), '')) || '#' || CAST(count(*) AS VARCHAR) FROM {table} t WHERE server_id = {_serverId}"));

    // ── what the monitored server hands back ──

    private static Type QueryStatsColumnType(int ordinal) => ordinal switch
    {
        3 or 4 => typeof(DateTime),
        0 or 1 or 2 or 36 or 37 or 38 or 42 => typeof(string),
        40 or 41 or 43 => typeof(int),
        _ => typeof(long),
    };

    private static DbDataReader QueryStatsReader()
    {
        var table = new DataTable();
        for (var i = 0; i < 44; i++) table.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), QueryStatsColumnType(i));
        foreach (var (hash, text) in new[]
        {
            ("0xC0", StatementScrubCanary.CanaryStatement),
            ("0xP0", StatementScrubCanary.PlainStatement),
        })
        {
            var values = new object[table.Columns.Count];
            for (var i = 0; i < values.Length; i++) values[i] = DBNull.Value;
            values[1] = hash;
            values[36] = hash + "A";
            values[37] = hash + "B";
            values[38] = text;
            table.Rows.Add(values);
        }

        return table.CreateDataReader();
    }

    private static DbDataReader SnapshotsReader()
    {
        var table = new DataTable();
        var types = new Type[35];
        for (var i = 0; i < types.Length; i++) types[i] = typeof(string);
        foreach (int i in new[] { 0, 7, 18, 19, 23, 34 }) types[i] = typeof(int);
        foreach (int i in new[] { 9, 11, 12, 13, 14, 15 }) types[i] = typeof(long);
        foreach (int i in new[] { 16, 24 }) types[i] = typeof(decimal);
        types[25] = typeof(bool);
        foreach (int i in new[] { 27, 28, 29, 30, 31, 32 }) types[i] = typeof(double);
        types[33] = typeof(DateTime);
        for (var i = 0; i < types.Length; i++) table.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), types[i]);

        foreach (var (session, text, plan) in new[]
        {
            (81, StatementScrubCanary.CanaryStatement, StatementScrubCanary.CanaryPlan()),
            (82, StatementScrubCanary.PlainStatement, (string?)null),
        })
        {
            var values = new object[types.Length];
            for (var i = 0; i < values.Length; i++) values[i] = DBNull.Value;
            values[0] = session;
            values[1] = Db;
            values[3] = text;
            values[4] = (object?)plan ?? DBNull.Value;
            values[5] = (object?)plan ?? DBNull.Value;
            values[6] = "running";
            values[26] = "0x" + session.ToString(CultureInfo.InvariantCulture);
            table.Rows.Add(values);
        }

        return table.CreateDataReader();
    }

    private static DbDataReader DeadlocksReader()
    {
        var data = new DataSet();
        var rows = new DataTable();
        rows.Columns.Add("time", typeof(DateTime));
        rows.Columns.Add("victim", typeof(string));
        rows.Columns.Add("graph", typeof(string));
        rows.Columns.Add("source", typeof(string));
        rows.Rows.Add(Anchor.AddMinutes(1), "p1", Graph(StatementScrubCanary.CanaryStatement, StatementScrubCanary.PlainStatement), DBNull.Value);
        rows.Rows.Add(Anchor.AddMinutes(2), "p1", Graph(StatementScrubCanary.PlainStatement, StatementScrubCanary.PlainStatement), DBNull.Value);

        var gate = new DataTable();
        gate.Columns.Add("count", typeof(long));
        gate.Columns.Add("gated", typeof(bool));
        gate.Rows.Add(100L, false);
        data.Tables.Add(rows);
        data.Tables.Add(gate);
        return data.CreateDataReader();
    }

    /// <summary>A real in-process MCP server with Lite's tool classes, the real data service, and the host's REAL call-tool
    /// filter list.</summary>
    private sealed class McpHost : IAsyncDisposable
    {
        private readonly ServiceProvider _provider;
        private readonly CancellationTokenSource _stop;
        private readonly Task _run;

        private McpHost(ServiceProvider provider, CancellationTokenSource stop, Task run, McpClient client)
        {
            _provider = provider;
            _stop = stop;
            _run = run;
            Client = client;
        }

        public McpClient Client { get; }

        public static async Task<McpHost> StartAsync(LocalDataService data, ServerManager servers)
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
                .WithTools<McpPlanTools>();
            /* Looked up by name: the host's filter list lands with the Lite host change (PR D), and this class has to compile
               without it so the collect cases run on a branch that does not have it yet. Without the list no read is filtered. */
            var registration = typeof(McpHostService).GetMethod("AddCallToolFilters",
                System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic);
            if (registration is not null)
            {
                builder.WithRequestFilters((Action<IMcpRequestFilterBuilder>)Delegate.CreateDelegate(typeof(Action<IMcpRequestFilterBuilder>), registration));
            }

            var provider = services.BuildServiceProvider();
            var stop = new CancellationTokenSource();
            var run = provider.GetRequiredService<McpServer>().RunAsync(stop.Token);
            var client = await McpClient.CreateAsync(new StreamClientTransport(c2s.Writer.AsStream(), s2c.Reader.AsStream()));
            return new McpHost(provider, stop, run, client);
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
