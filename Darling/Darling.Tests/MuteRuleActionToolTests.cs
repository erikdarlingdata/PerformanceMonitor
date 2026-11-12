/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7083 part 3: the create and update verbs take an <c>action</c> (mute or digest), refuse any other word with a message that names the
/// two, keep it in the stored rule and its undo, and the read returns it.
/// </summary>
public sealed class MuteRuleActionToolTests
{
    private static JsonObject Parse(string json) => JsonNode.Parse(json)!.AsObject();

    private static Task<string> Create(FakeMuteRuleStore store, string? action, string? server = "srv") =>
        DarlingMcpAlertTools.CreateMuteRuleOver(store, server, "Blocking Detected", null, null, null, null, "nightly delete", null, action: action);

    [Fact]
    public async Task Create_WithDigest_StoresIt_AndTheReadReturnsIt()
    {
        var store = new FakeMuteRuleStore();

        var answer = Parse(await Create(store, "digest"));

        Assert.Equal("created", answer["status"]!.GetValue<string>());
        var id = answer["mute_rule"]!["id"]!.GetValue<string>();
        Assert.Equal("digest", answer["mute_rule"]!["action"]!.GetValue<string>());
        Assert.True(store.Row(id)!.IsDigestAction);

        /* get_mute_rules builds each element this way from the stored row. */
        var read = DarlingMcpAlertTools.BuildMuteRuleWithState(store.Row(id)!, DateTime.UtcNow);
        Assert.Equal("digest", read["action"]!.GetValue<string>());
    }

    [Fact]
    public async Task Create_WithNoAction_IsAMute()
    {
        var store = new FakeMuteRuleStore();

        var answer = Parse(await Create(store, null));

        Assert.Equal("mute", answer["mute_rule"]!["action"]!.GetValue<string>());
        Assert.False(store.Row(answer["mute_rule"]!["id"]!.GetValue<string>())!.IsDigestAction);
    }

    [Theory]
    [InlineData("snooze")]
    [InlineData("hold")]
    public async Task Create_WithAnotherWord_IsRefused_NamingBothValues_AndWritesNothing(string action)
    {
        var store = new FakeMuteRuleStore();

        var answer = Parse(await Create(store, action));

        Assert.Equal("invalid", answer["status"]!.GetValue<string>());
        var message = answer["message"]!.GetValue<string>();
        Assert.Contains("'mute'", message);
        Assert.Contains("'digest'", message);
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public async Task Create_ViaTheWebBody_TakesTheAction_AndRefusesABadOne()
    {
        var store = new FakeMuteRuleStore();

        var ok = Parse(await DarlingMcpAlertTools.CreateMuteRuleCore(store, """{"server_name":"srv","metric_name":"High CPU","action":"digest"}"""));
        Assert.Equal("created", ok["status"]!.GetValue<string>());
        Assert.Equal("digest", ok["mute_rule"]!["action"]!.GetValue<string>());

        var bad = Parse(await DarlingMcpAlertTools.CreateMuteRuleCore(store, """{"server_name":"srv2","metric_name":"High CPU","action":"snooze"}"""));
        Assert.Equal("invalid", bad["status"]!.GetValue<string>());
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public async Task Update_ChangesTheAction_BothWays_AndTheUndoPutsTheOldOneBack()
    {
        var store = new FakeMuteRuleStore();
        var id = Parse(await Create(store, null))["mute_rule"]!["id"]!.GetValue<string>();

        var set = Parse(await DarlingMcpAlertTools.UpdateMuteRuleCore(store, id, """{"action":"digest"}"""));
        Assert.Equal("updated", set["status"]!.GetValue<string>());
        Assert.True(store.Row(id)!.IsDigestAction);

        /* The undo is the update that sets the previous value back. */
        var undo = set["undo"]!["arguments"]!.AsObject();
        var undone = Parse(await DarlingMcpAlertTools.UpdateMuteRuleCore(store, id, undo["changes_json"]!.GetValue<string>()));
        Assert.Equal("updated", undone["status"]!.GetValue<string>());
        Assert.False(store.Row(id)!.IsDigestAction);

        /* Null resets to the default, a mute. */
        await DarlingMcpAlertTools.UpdateMuteRuleCore(store, id, """{"action":"digest"}""");
        var reset = Parse(await DarlingMcpAlertTools.UpdateMuteRuleCore(store, id, """{"action":null}"""));
        Assert.Equal("updated", reset["status"]!.GetValue<string>());
        Assert.False(store.Row(id)!.IsDigestAction);
    }

    [Fact]
    public async Task Update_WithAnotherWord_IsRefused_AndChangesNothing()
    {
        var store = new FakeMuteRuleStore();
        var id = Parse(await Create(store, "digest"))["mute_rule"]!["id"]!.GetValue<string>();
        var before = store.UpdateCalls;

        var answer = Parse(await DarlingMcpAlertTools.UpdateMuteRuleCore(store, id, """{"action":"snooze"}"""));

        Assert.Equal("invalid", answer["status"]!.GetValue<string>());
        Assert.Contains("'mute' or 'digest'", answer["message"]!.GetValue<string>());
        Assert.True(store.Row(id)!.IsDigestAction);
        Assert.Equal(before, store.UpdateCalls);
    }
}
