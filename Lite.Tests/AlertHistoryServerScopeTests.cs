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
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests.Helpers;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7902: Lite's <c>get_alert_history</c> has Darling's two reach controls. <c>server_name</c> scopes the page to one
/// server IN THE QUERY, ahead of the row cap, so a chatty server cannot push a quiet one's alerts out of the page; and
/// <c>hours_back</c> reaches the alert log's retention (three months, <see cref="RetentionService.ArchiveRetentionMonths"/>)
/// rather than stopping at the shared week. The tool is invoked by reflection so the parameter's absence is a named
/// failure rather than a compile error.
/// </summary>
public sealed class AlertHistoryServerScopeTests : IDisposable
{
    private const string ChattyServer = "alert-scope-chatty";
    private const string QuietServer = "alert-scope-quiet";

    /// <summary>The alert log's retention, in hours: three months at 30 days each.</summary>
    private static readonly int RetentionHours = RetentionService.ArchiveRetentionMonths * 30 * 24;

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly TestAlertDataHelper _helper;
    private readonly ServerManager _serverManager;
    private readonly int _chattyId;
    private readonly int _quietId;
    private DuckDbInitializer? _initializer;

    public AlertHistoryServerScopeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _helper = new TestAlertDataHelper(_dbPath);

        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);
        _serverManager = new ServerManager(configDir);
        _chattyId = Register(ChattyServer);
        _quietId = Register(QuietServer);
    }

    private int Register(string name)
    {
        var server = new ServerConnection { ServerName = name, DisplayName = name };
        _serverManager.AddServer(server);
        /* The id the tool resolves to: the same derivation ServerResolver uses. */
        return RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _initializer?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* Best-effort cleanup */ }
    }

    private async Task<LocalDataService> SeedAsync(params (int MinutesAgo, int ServerId, string ServerName)[] rows)
    {
        _initializer = new DuckDbInitializer(_dbPath);
        await _initializer.InitializeFromTemplateAsync();
        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            foreach (var (minutesAgo, serverId, serverName) in rows)
            {
                await _helper.InsertLiveAlertAsync(connection, DateTime.UtcNow.AddMinutes(-minutesAgo), serverId, serverName, "High CPU");
            }
        }
        return new LocalDataService(_initializer);
    }

    /// <summary>Calls <c>get_alert_history</c> by parameter name, so the test compiles whether or not the tool has
    /// <c>server_name</c>, and says so when it does not.</summary>
    private async Task<JsonElement> CallAsync(LocalDataService service, int hoursBack, int limit, string? serverName = null)
    {
        var method = typeof(McpAlertTools).GetMethod(nameof(McpAlertTools.GetAlertHistory), BindingFlags.Public | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        if (serverName is not null && parameters.All(p => p.Name != "server_name"))
        {
            Assert.Fail("Lite's get_alert_history has no server_name parameter; Darling's twin takes one.");
        }

        var args = new object?[parameters.Length];
        for (var i = 0; i < parameters.Length; i++)
        {
            var p = parameters[i];
            if (p.ParameterType == typeof(LocalDataService)) { args[i] = service; continue; }
            if (p.ParameterType == typeof(ServerManager)) { args[i] = _serverManager; continue; }
            args[i] = p.Name switch
            {
                "hours_back" => hoursBack,
                "limit" => limit,
                "server_name" => serverName,
                _ => p.HasDefaultValue ? p.DefaultValue : null,
            };
        }

        var json = await (Task<string>)method.Invoke(null, args)!;
        return JsonDocument.Parse(json).RootElement.Clone();
    }

    [Fact]
    public async Task ServerName_ScopesThePageInTheQuery_SoAChattyServerCannotCrowdAQuietOneOut()
    {
        /* Six newer rows from the chatty server and one older row from the quiet one: a page of 2 holds only chatty
           rows unless the server scope is applied ahead of the cap. */
        var service = await SeedAsync(
            (1, _chattyId, ChattyServer), (2, _chattyId, ChattyServer), (3, _chattyId, ChattyServer),
            (4, _chattyId, ChattyServer), (5, _chattyId, ChattyServer), (6, _chattyId, ChattyServer),
            (30, _quietId, QuietServer));

        var scoped = await CallAsync(service, 24, 2, QuietServer);
        Assert.Equal(1, scoped.GetProperty("alerts_returned").GetInt32());
        Assert.False(scoped.GetProperty("truncated").GetBoolean());
        Assert.All(scoped.GetProperty("alerts").EnumerateArray(), a => Assert.Equal(QuietServer, a.GetProperty("server_name").GetString()));
        /* The chosen server is echoed, as Darling's twin echoes it. */
        Assert.Equal(QuietServer, scoped.GetProperty("server").GetString());

        /* Unscoped, the same page is the chatty server's newest two: the fleet default is unchanged. */
        var fleet = await CallAsync(service, 24, 2);
        Assert.Equal(2, fleet.GetProperty("alerts_returned").GetInt32());
        Assert.True(fleet.GetProperty("truncated").GetBoolean());
        Assert.All(fleet.GetProperty("alerts").EnumerateArray(), a => Assert.Equal(ChattyServer, a.GetProperty("server_name").GetString()));
    }

    [Fact]
    public async Task ServerName_ThatMatchesNoServer_IsTheResolverError()
    {
        var service = await SeedAsync((1, _chattyId, ChattyServer));
        var json = await CallAsync(service, 24, 50, "alert-scope-no-such-server");
        Assert.StartsWith("Could not resolve server.", McpHelpers.ErrorMessageOf(json.GetRawText()), StringComparison.Ordinal);
    }

    [Fact]
    public async Task HoursBack_ReachesTheAlertLogRetention_AndIsRefusedBeyondIt()
    {
        var service = await SeedAsync((60, _quietId, QuietServer));

        /* A month, as Darling serves it: inside the retention, so the read answers. */
        var month = await CallAsync(service, 720, 50);
        Assert.False(McpHelpers.IsRefusalEnvelope(month.GetRawText()), $"hours_back 720 was refused: {month.GetRawText()}");
        Assert.Equal(1, month.GetProperty("alerts_returned").GetInt32());

        /* The retention itself is accepted; one hour past it is refused by name. */
        var atRetention = await CallAsync(service, RetentionHours, 50);
        Assert.False(McpHelpers.IsRefusalEnvelope(atRetention.GetRawText()), $"hours_back {RetentionHours} was refused: {atRetention.GetRawText()}");

        var beyond = await CallAsync(service, RetentionHours + 1, 50);
        Assert.True(McpHelpers.IsRefusalEnvelope(beyond.GetRawText()), $"hours_back {RetentionHours + 1} was not refused: {beyond.GetRawText()}");
        Assert.Equal("hours_back", beyond.GetProperty("hints").GetProperty("parameter").GetString());
    }
}
