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
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7590, the Lite half: Lite connects directly and keeps its connection state in process, so a failed connect check already
/// reads Offline on the sidebar, the Overview card and <c>list_servers</c>. What it did not say was since when and why. The
/// sentence now comes from <see cref="ServerConnectionStatus.UnreachableReason"/>, the wording Darling's stored state uses.
/// Driven through the status object the server manager keeps (the same one <c>CheckConnectionAsync</c> fills), no SQL Server.
/// </summary>
public sealed class UnreachableServerReadsOfflineWithReasonTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ConnectError = "A network-related or instance-specific error occurred while establishing a connection to SQL Server.";
    private static readonly DateTime DownSince = new(2026, 11, 4, 11, 56, 0, DateTimeKind.Utc);
    private static readonly string Expected = "Server unreachable since 2026-11-04 11:56 UTC: " + ConnectError;

    private readonly SharedDuckDbFixture _fixture;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;

    public UnreachableServerReadsOfflineWithReasonTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _fixture = fixture;
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-7590-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    private ServerConnection AddServer(string name)
    {
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = name, IsEnabled = true, CreatedDate = DateTime.Now.AddDays(-2) };
        _serverManager.AddServer(server);
        return server;
    }

    private void MarkDown(ServerConnection server)
    {
        var status = _serverManager.GetConnectionStatus(server.Id);
        status.IsOnline = false;
        status.StatusChangedAt = DownSince;
        status.LastChecked = DownSince.AddMinutes(3);
        status.ErrorMessage = ConnectError;
    }

    [Fact]
    public async Task List_servers_reads_a_failed_connect_Offline_with_since_and_the_error_while_an_online_server_carries_none()
    {
        var down = AddServer("DownSrv");
        var up = AddServer("UpSrv");
        MarkDown(down);
        _serverManager.GetConnectionStatus(up.Id).IsOnline = true;

        var reply = await McpDiscoveryTools.ListServers(_serverManager, new LocalDataService(_fixture.DuckDb));
        var rows = JsonDocument.Parse(reply).RootElement.GetProperty("servers").EnumerateArray()
            .ToDictionary(e => e.GetProperty("server_name").GetString()!, e => e, StringComparer.Ordinal);

        Assert.Equal("Offline", rows["DownSrv"].GetProperty("status").GetString());
        Assert.Equal(Expected, rows["DownSrv"].GetProperty("reason").GetString());
        Assert.Equal(DownSince.ToString("o"), rows["DownSrv"].GetProperty("connect_down_since").GetString());
        Assert.Equal(ConnectError, rows["DownSrv"].GetProperty("last_connect_error").GetString());

        Assert.Equal("Online", rows["UpSrv"].GetProperty("status").GetString());
        Assert.Equal(JsonValueKind.Null, rows["UpSrv"].GetProperty("reason").ValueKind);
        Assert.Equal(JsonValueKind.Null, rows["UpSrv"].GetProperty("connect_down_since").ValueKind);
    }

    [Fact]
    public void The_reason_needs_a_failed_check_and_falls_back_to_the_last_check_when_the_change_was_never_stamped()
    {
        Assert.Null(new ServerConnectionStatus { IsOnline = true, StatusChangedAt = DownSince }.UnreachableReason);
        Assert.Null(new ServerConnectionStatus().UnreachableReason);
        Assert.Null(new ServerConnectionStatus { IsOnline = false }.UnreachableReason);

        var unstamped = new ServerConnectionStatus { IsOnline = false, LastChecked = DownSince, ErrorMessage = "  " };
        Assert.Equal("Server unreachable since 2026-11-04 11:56 UTC", unstamped.UnreachableReason);
    }

    [Fact]
    public void The_sidebar_dot_tooltip_and_the_Overview_card_tooltip_name_the_reason_only_while_the_server_is_down()
    {
        var server = AddServer("TipSrv");
        MarkDown(server);
        var status = _serverManager.GetConnectionStatus(server.Id);

        server.IsOnline = status.IsOnline;
        server.UnreachableReason = status.UnreachableReason;
        var card = new ServerSummaryItem { ServerId = 1, DisplayName = "TipSrv", IsOnline = status.IsOnline, UnreachableReason = status.UnreachableReason };

        Assert.Contains(Expected, server.DotTooltip, StringComparison.Ordinal);
        Assert.Contains(Expected, card.StatusTooltip, StringComparison.Ordinal);
        Assert.DoesNotContain("\n\n", server.DotTooltip, StringComparison.Ordinal);

        status.IsOnline = true;
        server.IsOnline = true;
        server.UnreachableReason = status.UnreachableReason;
        card.IsOnline = true;
        card.UnreachableReason = status.UnreachableReason;

        Assert.DoesNotContain("Server unreachable", server.DotTooltip, StringComparison.Ordinal);
        Assert.DoesNotContain("Server unreachable", card.StatusTooltip, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Get_collection_health_carries_the_server_connection_block_only_while_the_server_is_unreachable()
    {
        var down = AddServer("DownSrv");
        var up = AddServer("UpSrv");
        MarkDown(down);
        _serverManager.GetConnectionStatus(up.Id).IsOnline = true;
        var data = new LocalDataService(_fixture.DuckDb);

        /* No collector rows in a fresh store: the unavailable answer still says why, the way Darling's twin does. */
        using var downReply = JsonDocument.Parse(await McpHealthTools.GetCollectionHealth(data, _serverManager, "DownSrv"));
        Assert.Equal("unavailable", downReply.RootElement.GetProperty("status").GetString());
        var block = downReply.RootElement.GetProperty("server_connection");
        Assert.Equal("waiting_on_connection", block.GetProperty("status").GetString());
        Assert.Equal(DownSince.ToString("o"), block.GetProperty("since").GetString());
        Assert.Equal(ConnectError, block.GetProperty("last_connect_error").GetString());
        Assert.Equal(JsonValueKind.Null, block.GetProperty("next_connect_attempt").ValueKind);
        Assert.Contains(Expected, block.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Contains(Expected, downReply.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);

        using var upReply = JsonDocument.Parse(await McpHealthTools.GetCollectionHealth(data, _serverManager, "UpSrv"));
        Assert.Equal("unavailable", upReply.RootElement.GetProperty("status").GetString());
        Assert.False(upReply.RootElement.TryGetProperty("server_connection", out _));

        /* The same block both SKUs build, with collector rows beside it: the sentence says the rows are the runs from before. */
        var withRows = McpHealthTools.ServerConnectionBlock(_serverManager.GetConnectionStatus(down.Id), DownSince.AddMinutes(5), hasCollectorRows: true);
        using var rowsJson = JsonDocument.Parse(JsonSerializer.Serialize(withRows));
        Assert.EndsWith("The collector rows are the last runs from before the connection failed.", rowsJson.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Null(McpHealthTools.ServerConnectionBlock(_serverManager.GetConnectionStatus(up.Id), DownSince, hasCollectorRows: true));
    }
}
