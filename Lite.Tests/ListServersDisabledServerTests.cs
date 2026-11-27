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
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9087: Lite's <c>list_servers</c> lists every configured server. A server whose data collection is off is a row that reads
/// Disabled (the word the Lite window uses, #9085), and the answer is <c>empty</c> only when no server is configured at all.
/// </summary>
public sealed class ListServersDisabledServerTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly SharedDuckDbFixture _fixture;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;

    public ListServersDisabledServerTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _fixture = fixture;
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-9087-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    private ServerConnection AddServer(string name, bool enabled)
    {
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = name, IsEnabled = enabled, CreatedDate = DateTime.Now.AddDays(-2) };
        _serverManager.AddServer(server);
        return server;
    }

    private Task<string> ListAsync() => McpDiscoveryTools.ListServers(_serverManager, new LocalDataService(_fixture.DuckDb));

    [Fact]
    public async Task OneServerTurnedOff_BothAreListed_TheOffOneReadsDisabledWithNoConnectionStatus()
    {
        var on = AddServer("OnSrv", enabled: true);
        var off = AddServer("OffSrv", enabled: false);
        _serverManager.GetConnectionStatus(on.Id).IsOnline = true;

        /* A failed check from before the server was turned off: it must not reach the row, which reads no connection status for it. */
        var stale = _serverManager.GetConnectionStatus(off.Id);
        stale.IsOnline = false;
        stale.StatusChangedAt = new DateTime(2026, 11, 4, 11, 56, 0, DateTimeKind.Utc);
        stale.LastChecked = stale.StatusChangedAt;
        stale.ErrorMessage = "A network-related or instance-specific error occurred.";

        using var doc = JsonDocument.Parse(await ListAsync());
        Assert.Equal(2, doc.RootElement.GetProperty("server_count").GetInt32());
        var rows = doc.RootElement.GetProperty("servers").EnumerateArray().ToList();
        Assert.Equal(2, rows.Count);

        var onRow = rows.Single(r => r.GetProperty("server_name").GetString() == "OnSrv");
        Assert.Equal("Online", onRow.GetProperty("status").GetString());

        var offRow = rows.Single(r => r.GetProperty("server_name").GetString() == "OffSrv");
        Assert.Equal(ServerCardStatus.Disabled.Word(), offRow.GetProperty("status").GetString());
        Assert.Equal(JsonValueKind.String, offRow.GetProperty("reason").ValueKind);
        Assert.Equal(JsonValueKind.Null, offRow.GetProperty("connect_down_since").ValueKind);
        Assert.Equal(JsonValueKind.Null, offRow.GetProperty("last_connect_error").ValueKind);
    }

    [Fact]
    public async Task EveryServerTurnedOff_TheyAreListed_NotTheEmptyAnswer()
    {
        AddServer("OffOne", enabled: false);
        AddServer("OffTwo", enabled: false);

        using var doc = JsonDocument.Parse(await ListAsync());
        Assert.False(doc.RootElement.TryGetProperty("status", out _));
        Assert.Equal(2, doc.RootElement.GetProperty("server_count").GetInt32());
        Assert.All(
            doc.RootElement.GetProperty("servers").EnumerateArray(),
            row => Assert.Equal(ServerCardStatus.Disabled.Word(), row.GetProperty("status").GetString()));
    }

    [Fact]
    public async Task NoServerConfigured_AnswersEmptyNamingNoServers()
    {
        using var doc = JsonDocument.Parse(await ListAsync());
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal("No servers are configured.", doc.RootElement.GetProperty("message").GetString());
    }
}
