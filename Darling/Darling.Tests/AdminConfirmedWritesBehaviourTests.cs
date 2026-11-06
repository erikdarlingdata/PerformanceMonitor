/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Admin writes that ask before they go (#7954), run under Node through the admin harness (<c>admin-server-edit-harness.mjs</c>,
/// scenario <c>confirmWrites</c>): Remove on a threshold override, the service-wide agent switch in both directions, Undo of a non-plan
/// agent change, and the People tab's Remove sign-in, Remove grant and Disable. A click alone asks and writes nothing; a confirmed click
/// writes once. Turning the agent switch on asks through a warning panel, as the per-server action switches do, and writes only from it.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class AdminConfirmedWritesBehaviourTests
{
    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString()!).ToArray();

    // The target, the write's url, and how its confirm sentence opens.
    public static TheoryData<string, string, string> Confirmed => new()
    {
        { "override", "/api/threshold-overrides", "Remove the cpu_percent override for alpha? Its value 95 goes away" },
        { "switchOff", "/api/agent-changes/switch", "Turn off agent changes?" },
        { "undo", "/api/agent-changes/c-1/undo", "Undo agent change c-1 (recompile module on alpha)?" },
        { "peopleIdentity", "/api/people/remove_person_identity", "Remove the sign-in Web sign-in abcdefgh... from Ann?" },
        { "peopleGrant", "/api/people/remove_person_grant", "Remove the grant \"Approve server actions (Kill sessions), All servers\" from Ann?" },
        { "peopleDisable", "/api/people/set_person_enabled", "Disable Ann?" },
    };

    [Theory]
    [MemberData(nameof(Confirmed))]
    public void AClickAlone_AsksOnce_AndWritesNothing(string target, string url, string asks)
    {
        var page = AdminServerEditBehaviourTests.Run("confirmWrites:" + target + "/no");

        var question = Assert.Single(Strings(page.GetProperty("confirms")));
        Assert.StartsWith(asks, question, StringComparison.Ordinal);
        Assert.Empty(page.GetProperty("writes").EnumerateArray());
        Assert.DoesNotContain(url, page.GetProperty("writes").EnumerateArray().Select(w => w.GetProperty("url").GetString()));
    }

    [Theory]
    [MemberData(nameof(Confirmed))]
    public void AConfirmedClick_WritesOnce(string target, string url, string asks)
    {
        var page = AdminServerEditBehaviourTests.Run("confirmWrites:" + target + "/yes");

        Assert.StartsWith(asks, Assert.Single(Strings(page.GetProperty("confirms"))), StringComparison.Ordinal);
        var write = Assert.Single(page.GetProperty("writes").EnumerateArray());
        Assert.Equal(url, write.GetProperty("url").GetString());
        var body = write.GetProperty("body");
        switch (target)
        {
            case "override":
                Assert.True(body.GetProperty("clear").GetBoolean());
                Assert.Equal("cpu_percent", body.GetProperty("key").GetString());
                Assert.Equal("alpha", body.GetProperty("server_name").GetString());
                break;
            case "switchOff":
                Assert.False(body.GetProperty("enabled").GetBoolean());
                break;
            case "peopleIdentity":
                Assert.Equal(11, body.GetProperty("identity_id").GetInt32());
                break;
            case "peopleGrant":
                Assert.Equal(21, body.GetProperty("grant_id").GetInt32());
                break;
            case "peopleDisable":
                Assert.False(body.GetProperty("enabled").GetBoolean());
                break;
        }
    }

    [Theory]
    [InlineData("no", 0)]
    [InlineData("yes", 1)]
    public void TurningTheAgentSwitchOn_ShowsTheWarningPanel_AndWritesOnlyFromItsButton(string answer, int writes)
    {
        var page = AdminServerEditBehaviourTests.Run("confirmWrites:switchOn/" + answer);

        Assert.Empty(Strings(page.GetProperty("confirms")));
        Assert.Equal(1, page.GetProperty("panelShown").GetInt32());
        Assert.Equal(0, page.GetProperty("panelAfter").GetInt32());
        var sent = page.GetProperty("writes").EnumerateArray().ToArray();
        Assert.Equal(writes, sent.Length);
        if (writes == 1)
        {
            Assert.Equal("/api/agent-changes/switch", sent[0].GetProperty("url").GetString());
            Assert.True(sent[0].GetProperty("body").GetProperty("enabled").GetBoolean());
        }
    }
}
