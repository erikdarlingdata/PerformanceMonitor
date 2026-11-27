/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using Lite.Tests;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9640: Lite's <c>get_collector_schedules</c> and <c>reset_collector_schedules</c>. Each write is read back through a SECOND
/// <see cref="ScheduleManager"/> opened on the same folder, which is what the editor's Save is read through (the file).
/// </summary>
public sealed class McpCollectorScheduleToolTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "lite-mcp-sched-" + Guid.NewGuid().ToString("N"));
    private readonly ServerManager _servers;
    private readonly ScheduleManager _schedules;
    private readonly ServerConnection _alpha = new() { ServerName = "alpha-host", DisplayName = "alpha" };
    private readonly ServerConnection _beta = new() { ServerName = "beta-host", DisplayName = "beta" };

    public McpCollectorScheduleToolTests()
    {
        Directory.CreateDirectory(_dir);
        _servers = new ServerManager(_dir);
        _servers.AddServer(_alpha);
        _servers.AddServer(_beta);
        _schedules = new ScheduleManager(_dir);
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(_dir, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    private static JsonDocument Parse(string json) => JsonDocument.Parse(json);

    private string Get(string? collector = null, string? server = null) =>
        McpCollectorScheduleTools.Get(_schedules, _servers.GetAllServers(), collector, server);

    private string Reset(string? server = null) =>
        McpCollectorScheduleTools.Reset(_schedules, _servers.GetAllServers(), server);

    private ScheduleManager Reread() => new(_dir);

    private void GiveAlphaItsOwnSchedule()
    {
        var own = _schedules.GetSchedulesForServer(_alpha.Id).ToList();
        own.First(s => s.Name == "wait_stats").FrequencyMinutes = 7;
        _schedules.SetScheduleForServer(_alpha.Id, own);
    }

    [Fact]
    public void Reset_RefusesAndChangesNothing_WhenTwoEntriesShareTheStorageName()
    {
        GiveAlphaItsOwnSchedule();
        var twin = new ServerConnection { ServerName = _alpha.ServerName, DisplayName = "alpha twin" };

        using var doc = Parse(McpCollectorScheduleTools.Reset(_schedules, [_alpha, twin], "alpha-host"));

        Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        Assert.Contains("'alpha-host'", doc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Contains("Lite window", doc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Equal(7, Reread().GetSchedulesForServer(_alpha.Id).First(s => s.Name == "wait_stats").FrequencyMinutes);
    }

    [Fact]
    public void Reset_RefusesAndChangesNothing_WhenTwoEntriesShareOneId()
    {
        GiveAlphaItsOwnSchedule();
        var sameId = new ServerConnection { Id = _alpha.Id, ServerName = "other-host", DisplayName = "other" };

        using var doc = Parse(McpCollectorScheduleTools.Reset(_schedules, [_alpha, sameId], "alpha-host"));

        Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        Assert.Contains("shares the id", doc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Equal(7, Reread().GetSchedulesForServer(_alpha.Id).First(s => s.Name == "wait_stats").FrequencyMinutes);
    }

    [Fact]
    public void Get_ShippedDefaults_IsEmpty_AndNamesEveryServersSchedule()
    {
        using var doc = Parse(Get());
        var root = doc.RootElement;
        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Equal(0, root.GetProperty("overrides").GetArrayLength());
        Assert.All(root.GetProperty("server_schedules").EnumerateArray(), s => Assert.Equal("default", s.GetProperty("schedule").GetString()));
    }

    [Fact]
    public void Get_ListsAFleetWideChange_AndAServersOwnChange()
    {
        _schedules.UpdateSchedule("query_stats", frequencyMinutes: 5);
        GiveAlphaItsOwnSchedule();

        using var doc = Parse(Get());
        var rows = doc.RootElement.GetProperty("overrides").EnumerateArray().ToList();
        var fleet = rows.Single(r => r.GetProperty("scope").GetString() == "fleet-wide");
        Assert.Equal("query_stats", fleet.GetProperty("collector").GetString());
        Assert.Equal(5, fleet.GetProperty("frequency_minutes").GetInt32());

        /* alpha's own schedule is a copy of the default made after the change, so only wait_stats differs from it. */
        var own = rows.Single(r => r.GetProperty("scope").GetString() == "server");
        Assert.Equal("wait_stats", own.GetProperty("collector").GetString());
        Assert.Equal("alpha", own.GetProperty("server").GetString());
        Assert.Equal(7, own.GetProperty("frequency_minutes").GetInt32());

        var summary = doc.RootElement.GetProperty("server_schedules").EnumerateArray()
            .ToDictionary(s => s.GetProperty("server").GetString()!, s => s.GetProperty("schedule").GetString());
        Assert.Equal("own", summary["alpha"]);
        Assert.Equal("default", summary["beta"]);
    }

    [Fact]
    public void Get_WithACollector_ListsEveryScopesRow_AndTheShippedDefault()
    {
        using var doc = Parse(Get("QUERY_STATS"));
        var root = doc.RootElement;
        Assert.Equal("query_stats", root.GetProperty("collector").GetString());
        Assert.Equal("ok", root.GetProperty("status").GetString());
        Assert.Equal(1, root.GetProperty("code_default").GetProperty("frequency_minutes").GetInt32());
    }

    [Fact]
    public void Get_WithAServer_ListsOnlyThatServersScope()
    {
        GiveAlphaItsOwnSchedule();
        using var doc = Parse(Get(server: "beta"));
        Assert.DoesNotContain(doc.RootElement.GetProperty("overrides").EnumerateArray(), r => r.GetProperty("scope").GetString() == "server");
        Assert.Equal("beta", Assert.Single(doc.RootElement.GetProperty("server_schedules").EnumerateArray()).GetProperty("server").GetString());
    }

    [Fact]
    public void Get_AnswersAnUnknownCollector_AsUnknownCollector_LikeDarling()
    {
        using var doc = Parse(Get("no_such_collector"));
        Assert.Equal("refused", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal("unknown_collector", doc.RootElement.GetProperty("reason").GetString());
        Assert.Equal("get_collector_schedules", doc.RootElement.GetProperty("tool").GetString());
        Assert.False(doc.RootElement.GetProperty("changed").GetBoolean());
    }

    [Fact]
    public void Get_RefusesAnUnknownServer_NamingTheArgument()
    {
        using var doc = Parse(Get(server: "nowhere"));
        Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
    }

    [Fact]
    public void Reset_FleetWide_PutsTheDefaultScheduleBackToShipped_AndKeepsAServersOwnSchedule()
    {
        _schedules.UpdateSchedule("query_stats", enabled: false, frequencyMinutes: 5);
        GiveAlphaItsOwnSchedule();

        using var doc = Parse(Reset());
        var root = doc.RootElement;
        Assert.Equal("changed", root.GetProperty("status").GetString());
        Assert.Equal("fleet-wide", root.GetProperty("scope").GetString());
        Assert.Equal(1, root.GetProperty("removed").GetProperty("schedule_rows").GetInt32());
        Assert.Equal("query_stats", Assert.Single(root.GetProperty("removed_overrides").EnumerateArray()).GetProperty("collector").GetString());

        var reread = Reread();
        var qs = reread.GetDefaultSchedule().Single(s => s.Name == "query_stats");
        Assert.True(qs.Enabled);
        Assert.Equal(1, qs.FrequencyMinutes);
        Assert.True(reread.HasServerOverride(_alpha.Id));
        Assert.Equal(7, reread.GetSchedulesForServer(_alpha.Id).Single(s => s.Name == "wait_stats").FrequencyMinutes);
    }

    [Fact]
    public void Reset_FleetWide_OnShippedDefaults_ChangesNothing()
    {
        using var doc = Parse(Reset());
        Assert.Equal("unchanged", doc.RootElement.GetProperty("status").GetString());
    }

    [Fact]
    public void Reset_ForAServer_RemovesItsOwnSchedule_AndKeepsTheDefaultAndOtherServers()
    {
        _schedules.UpdateSchedule("query_stats", frequencyMinutes: 5);
        GiveAlphaItsOwnSchedule();
        var beta = _schedules.GetSchedulesForServer(_beta.Id).ToList();
        _schedules.SetScheduleForServer(_beta.Id, beta);

        using var doc = Parse(Reset("alpha"));
        var root = doc.RootElement;
        Assert.Equal("changed", root.GetProperty("status").GetString());
        Assert.Equal("alpha", root.GetProperty("scope").GetString());

        var reread = Reread();
        Assert.False(reread.HasServerOverride(_alpha.Id));
        Assert.True(reread.HasServerOverride(_beta.Id));
        Assert.Equal(5, reread.GetDefaultSchedule().Single(s => s.Name == "query_stats").FrequencyMinutes);
    }

    [Fact]
    public void Reset_ForAServerOnTheDefault_ChangesNothing()
    {
        using var doc = Parse(Reset("beta"));
        Assert.Equal("unchanged", doc.RootElement.GetProperty("status").GetString());
    }

    [Fact]
    public void Reset_RefusesAnUnknownServer_AndChangesNothing()
    {
        _schedules.UpdateSchedule("query_stats", frequencyMinutes: 5);
        using var doc = Parse(Reset("nowhere"));
        Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal(5, Reread().GetDefaultSchedule().Single(s => s.Name == "query_stats").FrequencyMinutes);
    }

    [Fact]
    public void TheWrite_IsServedOnlyWithTheSwitch_AndTheReadAlways()
    {
        var (tools, _, _) = McpToolsListBudgetTests.BuildServedTools();
        var all = new McpServerPrimitiveCollection<McpServerTool>();
        foreach (var tool in tools)
        {
            all.Add(tool);
        }

        var off = McpWritePolicy.FilterToolCollection(all, allowWrites: false).Select(t => t.ProtocolTool.Name).ToHashSet();
        var on = McpWritePolicy.FilterToolCollection(all, allowWrites: true).Select(t => t.ProtocolTool.Name).ToHashSet();
        Assert.Contains("get_collector_schedules", off);
        Assert.DoesNotContain("reset_collector_schedules", off);
        Assert.Contains("get_collector_schedules", on);
        Assert.Contains("reset_collector_schedules", on);
    }
}
