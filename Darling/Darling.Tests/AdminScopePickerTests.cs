/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Text.Json;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7949: the Admin scope pickers (Notification Routes, Environment Profiles, People grants) are built from the Servers read, so a disabled
/// server is in them, and a route or profile whose stored scope is not in a list keeps it. The pages run on a fake DOM in
/// admin-server-edit-harness.mjs; the fake service lists an enabled server and a disabled one (server 7) on the Servers read, and only the
/// enabled one on the fleet read.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class AdminScopePickerTests
{
    private static string[] Options(JsonElement e) => e.GetProperty("options").EnumerateArray().Select(o => o.GetString()!).ToArray();

    /* The issue's trigger: Edit a route scoped to a disabled server, then Save. The write used to carry scope_server_id null (every server). */
    [Fact]
    public void EditingARouteScopedToADisabledServer_SavesTheSameScope()
    {
        var result = AdminServerEditBehaviourTests.Run("routeScope:disabled");

        Assert.Equal("s7", result.GetProperty("shown").GetString());
        Assert.Contains("s7|Server: Bravo (disabled)", Options(result));
        var body = Assert.Single(result.GetProperty("writes").EnumerateArray().ToArray());
        Assert.Equal(7, body.GetProperty("scope_server_id").GetInt32());
        Assert.Equal(JsonValueKind.Null, body.GetProperty("scope_tag_id").ValueKind);
        Assert.Empty(result.GetProperty("notes").EnumerateArray());
    }

    /* Both lists failed as the tab painted: the stored scope is still a choice, still selected, still saved, and the tab says the lists were not read. */
    [Fact]
    public void EditingARoute_WhenTheListsCouldNotBeRead_KeepsItsScopeAndSaysSo()
    {
        var result = AdminServerEditBehaviourTests.Run("routeScope:unreadable");

        Assert.Equal("s7", result.GetProperty("shown").GetString());
        Assert.Contains("s7|Current scope: server id 7", Options(result));
        var body = Assert.Single(result.GetProperty("writes").EnumerateArray().ToArray());
        Assert.Equal(7, body.GetProperty("scope_server_id").GetInt32());
        Assert.Single(result.GetProperty("notes").EnumerateArray());
    }

    /* A profile for a disabled server could not be opened or removed: the picker had no option for it. */
    [Fact]
    public void AProfileForADisabledServer_CanBeOpenedAndRemoved()
    {
        var result = AdminServerEditBehaviourTests.Run("profileScope:disabled");

        Assert.True(result.GetProperty("hasServer7").GetBoolean());
        Assert.Contains("s7|Server: Bravo (disabled)", Options(result));
        var body = Assert.Single(result.GetProperty("writes").EnumerateArray().ToArray());
        Assert.Equal(7, body.GetProperty("scope_server_id").GetInt32());
        Assert.Equal("", body.GetProperty("profile_text").GetString());
    }

    [Fact]
    public void AProfileCanBeRemoved_WhenTheListsCouldNotBeRead()
    {
        var result = AdminServerEditBehaviourTests.Run("profileScope:unreadable");

        Assert.True(result.GetProperty("hasServer7").GetBoolean());
        var body = Assert.Single(result.GetProperty("writes").EnumerateArray().ToArray());
        Assert.Equal(7, body.GetProperty("scope_server_id").GetInt32());
    }

    /* The shared reader behind all three pickers, People's grant box included: every configured server, the disabled one marked. */
    [Fact]
    public void TheScopeLists_HoldEveryServer_AndMarkTheDisabledOne()
    {
        var result = AdminServerEditBehaviourTests.Run("scopes:disabled");

        var servers = result.GetProperty("servers").EnumerateArray().ToArray();
        Assert.Equal(new[] { 3, 7 }, servers.Select(s => s.GetProperty("id").GetInt32()).ToArray());
        Assert.Equal("Bravo (disabled)", servers[1].GetProperty("name").GetString());
        Assert.True(result.GetProperty("complete").GetBoolean());
    }

    [Fact]
    public void TheScopeLists_SayWhenTheyCouldNotBeRead()
    {
        Assert.False(AdminServerEditBehaviourTests.Run("scopes:unreadable").GetProperty("complete").GetBoolean());
    }

    /* One read fails: the list still says it is incomplete. With the Servers read down, the server list falls back to the fleet's enabled servers
       and the disabled one is missing (only the Servers read knows it), and both the Routes and Profiles tabs show the notice. */
    [Fact]
    public void WhenOnlyTheServersReadFails_TheListsFallBackToTheFleetAndSayTheyAreIncomplete()
    {
        var result = AdminServerEditBehaviourTests.Run("scopes:serversFail");

        Assert.False(result.GetProperty("complete").GetBoolean());
        var servers = result.GetProperty("servers").EnumerateArray().ToArray();
        Assert.Equal(new[] { 3 }, servers.Select(s => s.GetProperty("id").GetInt32()).ToArray());
        Assert.Equal("Alpha", servers[0].GetProperty("name").GetString());
        Assert.False(servers[0].GetProperty("disabled").GetBoolean());
        Assert.Equal(new[] { 2 }, result.GetProperty("tags").EnumerateArray().Select(t => t.GetProperty("id").GetInt32()).ToArray());

        Assert.Single(AdminServerEditBehaviourTests.Run("routeScope:serversFail").GetProperty("notes").EnumerateArray());
        Assert.Single(AdminServerEditBehaviourTests.Run("profileScope:serversFail").GetProperty("notes").EnumerateArray());
    }

    /* The mirror: with the fleet read down there are no tags to offer, the servers still come from the Servers read (disabled one marked), and
       the tabs say the lists are incomplete. */
    [Fact]
    public void WhenOnlyTheFleetReadFails_TheServersStayAndTheListsSayTheyAreIncomplete()
    {
        var result = AdminServerEditBehaviourTests.Run("scopes:fleetFails");

        Assert.False(result.GetProperty("complete").GetBoolean());
        var servers = result.GetProperty("servers").EnumerateArray().ToArray();
        Assert.Equal(new[] { 3, 7 }, servers.Select(s => s.GetProperty("id").GetInt32()).ToArray());
        Assert.Equal("Bravo (disabled)", servers[1].GetProperty("name").GetString());
        Assert.Empty(result.GetProperty("tags").EnumerateArray());

        Assert.Single(AdminServerEditBehaviourTests.Run("routeScope:fleetFails").GetProperty("notes").EnumerateArray());
        Assert.Single(AdminServerEditBehaviourTests.Run("profileScope:fleetFails").GetProperty("notes").EnumerateArray());
    }
}
