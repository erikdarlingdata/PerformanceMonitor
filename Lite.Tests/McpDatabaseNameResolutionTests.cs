/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Darling.Tests;
using DuckDB.NET.Data;
using Microsoft.Extensions.DependencyInjection;
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
/// #9065, Lite half: a caller's <c>database_name</c> resolved against the server's newest <c>database_states</c> list, through a real
/// in-process MCP server that registers the host's REAL call-tool filter list (<see cref="McpHostService.AddCallToolFilters"/>) over a
/// DuckDB store. A differently cased name reads the same rows as the stored spelling and the answer names the spelling it used; two
/// stored names that differ only by case are refused with both listed; a name the list does not hold reads as before and says so; a
/// server with no list reads as before and says nothing. Darling's twin is <c>McpDatabaseNameResolutionLiveTests</c>.
/// </summary>
public sealed class McpDatabaseNameResolutionTests : IClassFixture<SharedDuckDbFixture>, IAsyncDisposable
{
    private const string Listed = "DbnListedSrv";
    private const string Unlisted = "DbnUnlistedSrv";

    /// <summary>The Lite tools that take a <c>database_name</c> and no <c>server_name</c>: <c>find_database</c> searches every
    /// server and already ignores case, and the two query-tag writes compare object names ignoring case.</summary>
    private static readonly string[] NotResolved = { "assign_query_tag", "find_database", "unassign_query_tag" };

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly LocalDataService _service;
    private readonly int _listedId;
    private readonly int _unlistedId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 990000;
    private McpInProcessHost? _host;

    public McpDatabaseNameResolutionTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _service = new LocalDataService(_duckDb);
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-dbname-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);
        _listedId = AddServer(Listed);
        _unlistedId = AddServer(Unlisted);
    }

    private int AddServer(string name)
    {
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = name, IsEnabled = true };
        _serverManager.AddServer(server);
        return RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public async ValueTask DisposeAsync()
    {
        if (_host is not null)
        {
            await _host.DisposeAsync();
        }

        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    private async Task ExecuteAsync(string sql, params object?[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var value in values)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = value ?? DBNull.Value });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>Two long queries in SalesDB and one in each of the two names that differ only by case, on both servers; database
    /// lists on the listed server only, the older one naming a database the newest does not.</summary>
    private async Task SeedAsync()
    {
        var now = DateTime.UtcNow;
        foreach (var (serverId, server) in new[] { (_listedId, Listed), (_unlistedId, Unlisted) })
        {
            foreach (var (database, seconds) in new[] { ("SalesDB", 9), ("SalesDB", 7), ("Reports", 5), ("REPORTS", 3) })
            {
                var when = Naive(now.AddMinutes(-30));
                await ExecuteAsync(@"
INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, statement_text, duration_microseconds)
VALUES ($1, $2, $3, $4, $5, 'rpc_completed', $6, 'SELECT 1', $7)",
                    _nextId++, when, serverId, server, when, database, seconds * 1_000_000L);
            }
        }

        foreach (var (age, databases) in new[]
                 {
                     (TimeSpan.FromHours(2), new[] { "master", "salesdb_old" }),
                     (TimeSpan.FromMinutes(10), new[] { "master", "SalesDB", "Reports", "REPORTS" }),
                 })
        {
            foreach (var database in databases)
            {
                await ExecuteAsync(@"
INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby)
VALUES ($1, $2, $3, $4, $5, 5, 'ONLINE', false)",
                    _nextId++, Naive(now - age), _listedId, Listed, database);
            }
        }
    }

    /// <summary>A server with the long-query tool and the host's real filter list, over the seeded store.</summary>
    private async Task<McpInProcessHost> StartLongQueryHostAsync()
    {
        await SeedAsync();
        _host = await McpInProcessHost.StartAsync(
            new[] { typeof(McpLongQueryTools) },
            type => type == typeof(LocalDataService) || type == typeof(ServerManager),
            _ => null,
            installGuard: false,
            TestContext.Current.CancellationToken,
            builder =>
            {
                builder.Services.AddSingleton(_service);
                builder.Services.AddSingleton(_serverManager);
                builder.WithRequestFilters(McpHostService.AddCallToolFilters);
            });
        return _host;
    }

    private static async Task<(JsonElement Json, bool IsError)> LongQueriesAsync(McpInProcessHost host, string server, string? database)
    {
        var arguments = new Dictionary<string, object?> { ["server_name"] = server };
        if (database is not null)
        {
            arguments["database_name"] = database;
        }

        var result = await host.Client.CallToolAsync(
            "get_long_query_completions", arguments, cancellationToken: TestContext.Current.CancellationToken);
        return (JsonDocument.Parse(McpInProcessHost.TextOf(result)).RootElement.Clone(), result.IsError == true);
    }

    private static bool Has(JsonElement answer, string field) => answer.TryGetProperty(field, out _);

    [Fact]
    public async Task ADifferentlyCasedName_ReadsTheSameRowsAsTheStoredSpelling_AndNamesTheSpellingItUsed()
    {
        var host = await StartLongQueryHostAsync();

        var (exact, exactError) = await LongQueriesAsync(host, Listed, "SalesDB");
        Assert.False(exactError);
        Assert.Equal(2, exact.GetProperty("completions").GetArrayLength());
        Assert.False(Has(exact, McpDatabaseNameResolver.ResolvedField));
        Assert.False(Has(exact, McpDatabaseNameResolver.NoteField));

        foreach (var given in new[] { "salesdb", "SALESDB", "sAlEsDb" })
        {
            var (folded, foldedError) = await LongQueriesAsync(host, Listed, given);
            Assert.False(foldedError);
            Assert.Equal(exact.GetProperty("completions").GetRawText(), folded.GetProperty("completions").GetRawText());
            Assert.Equal("SalesDB", folded.GetProperty(McpDatabaseNameResolver.ResolvedField).GetString());
            Assert.False(Has(folded, McpDatabaseNameResolver.NoteField));
        }
    }

    [Fact]
    public async Task TwoStoredNamesThatDifferOnlyByCase_AreRefusedWithBothListed_AndEachStillReadsWhenSpelledExactly()
    {
        var host = await StartLongQueryHostAsync();

        var (refused, isError) = await LongQueriesAsync(host, Listed, "reports");
        Assert.True(isError);
        Assert.Equal("invalid", refused.GetProperty("status").GetString());
        Assert.Equal("database_name", refused.GetProperty("hints").GetProperty("parameter").GetString());
        Assert.Contains("'REPORTS', 'Reports'", refused.GetProperty("message").GetString(), StringComparison.Ordinal);

        foreach (var (given, milliseconds) in new[] { ("Reports", 5000.0), ("REPORTS", 3000.0) })
        {
            var (answer, exactError) = await LongQueriesAsync(host, Listed, given);
            Assert.False(exactError);
            var row = Assert.Single(answer.GetProperty("completions").EnumerateArray());
            Assert.Equal(milliseconds, row.GetProperty("duration_ms").GetDouble());
            Assert.False(Has(answer, McpDatabaseNameResolver.ResolvedField));
        }
    }

    [Theory]
    [InlineData("NoSuchDb")]
    [InlineData(" salesdb")]
    [InlineData("salesdb_old")]
    public async Task ANameTheNewestListDoesNotHold_ReadsAsGiven_AndTheEmptyAnswerSaysItIsNotOnTheList(string given)
    {
        var host = await StartLongQueryHostAsync();

        var (answer, isError) = await LongQueriesAsync(host, Listed, given);

        Assert.False(isError);
        Assert.Equal("empty", answer.GetProperty("status").GetString());
        Assert.Equal(McpDatabaseNameResolver.NotSeenMessage(given), answer.GetProperty(McpDatabaseNameResolver.NoteField).GetString());
        Assert.False(Has(answer, McpDatabaseNameResolver.ResolvedField));

        /* The tool's own empty answer is still there, word for word: the note is the only thing added. */
        var direct = JsonDocument.Parse(await McpLongQueryTools.GetLongQueryCompletions(
            _service, _serverManager, Listed, 24, 30, database_name: given)).RootElement;
        Assert.Equal(direct.GetProperty("message").GetString(), answer.GetProperty("message").GetString());
    }

    [Fact]
    public async Task AServerWithNoDatabaseList_ReadsAsBefore_AndSaysNothingAboutTheName()
    {
        var host = await StartLongQueryHostAsync();

        var (folded, _) = await LongQueriesAsync(host, Unlisted, "salesdb");
        Assert.Equal("empty", folded.GetProperty("status").GetString());
        Assert.False(Has(folded, McpDatabaseNameResolver.ResolvedField));
        Assert.False(Has(folded, McpDatabaseNameResolver.NoteField));

        var (exact, _) = await LongQueriesAsync(host, Unlisted, "SalesDB");
        Assert.Equal(2, exact.GetProperty("completions").GetArrayLength());

        /* No name is every database, with nothing added. */
        var (all, _) = await LongQueriesAsync(host, Listed, null);
        Assert.Equal(4, all.GetProperty("completions").GetArrayLength());
        Assert.False(Has(all, McpDatabaseNameResolver.ResolvedField));
        Assert.False(Has(all, McpDatabaseNameResolver.NoteField));
    }

    [Fact]
    public async Task TheCandidatesRead_TellsNoListFromAListWithoutTheName_AndReturnsOnlyTheMatchingSpellings()
    {
        await SeedAsync();
        var now = DateTime.UtcNow;

        Assert.Equal(new[] { "SalesDB" }, await _service.DatabaseNameCandidatesAsync(_listedId, "SALESDB", now));
        Assert.Equal(
            new[] { "REPORTS", "Reports" },
            (await _service.DatabaseNameCandidatesAsync(_listedId, "reports", now))!.OrderBy(n => n, StringComparer.Ordinal).ToArray());
        Assert.Empty((await _service.DatabaseNameCandidatesAsync(_listedId, "salesdb_old", now))!);
        Assert.Null(await _service.DatabaseNameCandidatesAsync(_unlistedId, "SalesDB", now));

        /* A list older than the look-back is no list. */
        Assert.Null(await _service.DatabaseNameCandidatesAsync(
            _listedId, "SalesDB", now + LocalDataService.FindDatabaseLookBack + TimeSpan.FromHours(1)));
    }

    /// <summary>
    /// The census: every Lite tool that takes <c>database_name</c>, called through the host's real filter list with a differently
    /// cased name, over the seeded store. The name the call carries is read where the tool would run, so a tool the filter did not
    /// cover is handed the name as the caller spelled it. The tools are every <c>[McpServerToolType]</c> class in Lite, so a tool
    /// added tomorrow is covered or fails here.
    /// </summary>
    [Fact]
    public async Task EveryToolThatTakesADatabaseName_IsHandedTheStoredSpelling_AndSaysHowItMatches()
    {
        await SeedAsync();
        var handed = new ConcurrentDictionary<string, string?>(StringComparer.Ordinal);

        /* The innermost filter, where the tool would be: it records the name the call now carries and answers in the tool's place. */
        McpRequestFilter<CallToolRequestParams, CallToolResult> recorder = next => (request, cancellationToken) =>
        {
            handed[request.Params!.Name] = request.Params.Arguments is { } arguments
                && arguments.TryGetValue("database_name", out var name) ? name.GetString() : null;
            return ValueTask.FromResult(new CallToolResult
            {
                Content = new List<ContentBlock> { new TextContentBlock { Text = "{\"status\":\"empty\",\"message\":\"census\"}" } },
            });
        };

        _host = await McpInProcessHost.StartAsync(
            global::Lite.Tests.McpWholeNumberArgumentTests.LiteToolTypes(),
            type => global::Lite.Tests.McpServedSchema.IsServiceParameter(type) && type != typeof(McpToolGuideCatalog),
            type => type == typeof(IInvestigationRequestStore)
                ? NotificationRoutesMcpTests.RouteUseStore.Create(new Dictionary<int, (int LastHour, int LastDay)>())
                : null,
            installGuard: false,
            TestContext.Current.CancellationToken,
            builder =>
            {
                builder.Services.AddSingleton(_service);
                builder.Services.AddSingleton(_serverManager);
                builder.WithRequestFilters(McpHostService.AddCallToolFilters);
                builder.WithRequestFilters(filters => filters.AddCallToolFilter(recorder));
            });

        var taking = _host.RegisteredTools
            .Select(tool => tool.ProtocolTool)
            .Where(tool => tool.InputSchema.TryGetProperty("properties", out var properties) && properties.TryGetProperty("database_name", out var _))
            .OrderBy(tool => tool.Name, StringComparer.Ordinal)
            .ToList();

        var problems = new List<string>();
        var passedOver = new List<string>();
        foreach (var tool in taking)
        {
            var properties = tool.InputSchema.GetProperty("properties");
            var arguments = new Dictionary<string, object?> { ["database_name"] = "salesdb" };
            if (properties.TryGetProperty("server_name", out var _))
            {
                arguments["server_name"] = Listed;
            }

            if (tool.InputSchema.TryGetProperty("required", out var required))
            {
                foreach (var parameter in required.EnumerateArray().Select(r => r.GetString()!).Where(r => !arguments.ContainsKey(r)))
                {
                    arguments[parameter] = Filler(properties.GetProperty(parameter));
                }
            }

            var result = await _host.Client.CallToolAsync(tool.Name, arguments, cancellationToken: TestContext.Current.CancellationToken);
            var text = McpInProcessHost.TextOf(result);
            var answer = JsonDocument.Parse(text).RootElement;
            var resolved = answer.TryGetProperty(McpDatabaseNameResolver.ResolvedField, out var field) ? field.GetString() : null;
            var described = properties.GetProperty("database_name").TryGetProperty("description", out var d) ? d.GetString() ?? string.Empty : string.Empty;
            var saysHow = described.EndsWith(McpDatabaseNameResolver.MatchingSentence, StringComparison.Ordinal);

            if (!handed.TryGetValue(tool.Name, out var name))
            {
                problems.Add($"{tool.Name}: the call never reached the tool's slot: {text}");
                continue;
            }

            if (name == "salesdb" && resolved is null)
            {
                passedOver.Add(tool.Name);
                if (saysHow)
                {
                    problems.Add($"{tool.Name}: its description says the name is matched ignoring case, and the name was not resolved.");
                }

                continue;
            }

            if (name != "SalesDB" || resolved != "SalesDB")
            {
                problems.Add($"{tool.Name}: handed '{name}', {McpDatabaseNameResolver.ResolvedField} '{resolved}'.");
            }

            if (!saysHow)
            {
                problems.Add($"{tool.Name}: the database_name description does not end with \"{McpDatabaseNameResolver.MatchingSentence}\": {described}");
            }
        }

        Assert.True(problems.Count == 0, string.Join("\n", problems));
        Assert.Equal(NotResolved, passedOver);
        Assert.True(taking.Count - passedOver.Count >= 30, $"only {taking.Count - passedOver.Count} Lite tools resolved a database_name; Lite has more than 30 that take one.");
    }

    /// <summary>A value the binder reads into a required parameter of the schema's type.</summary>
    private static object Filler(JsonElement parameter)
    {
        if (parameter.TryGetProperty("enum", out var values) && values.GetArrayLength() > 0)
        {
            return values[0].Clone();
        }

        return (parameter.TryGetProperty("type", out var type) ? type.GetString() : "string") switch
        {
            "integer" or "number" => 1,
            "boolean" => false,
            "array" => Array.Empty<string>(),
            _ => "0x0000000000000001",
        };
    }
}
