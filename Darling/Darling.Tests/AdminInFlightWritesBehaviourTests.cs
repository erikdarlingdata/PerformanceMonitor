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
/// The Admin writes whose button is disabled while the write is in flight (#7952, #7954), run under Node through the admin harness
/// (<c>admin-server-edit-harness.mjs</c>, scenario <c>inFlight</c>): the Routes form's Save, the route Delete button, the Profiles form's Save,
/// and the Agent changes tab's switch and Undo, which disable every button of that tab. The write is held at the service while the button is
/// pressed again, once as a click, which a disabled button never receives, and once forced past the disabled state. One write goes, the button
/// is disabled while it is in flight, and it is enabled again once the answer lands.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class AdminInFlightWritesBehaviourTests
{
    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Theory]
    [InlineData("routeSave", "PUT /api/notification-routes/7")]
    [InlineData("routeDelete", "DELETE /api/notification-routes/7")]
    [InlineData("profileSave", "PUT /api/environment-profiles")]
    [InlineData("agentSwitch", "PUT /api/agent-changes/switch")]
    [InlineData("agentUndo", "PUT /api/agent-changes/c-1/undo")]
    public void ASecondPressWhileTheWriteIsInFlight_SendsNothing_AndTheButtonIsDisabledMeanwhile(string target, string write)
    {
        var page = AdminServerEditBehaviourTests.Run("inFlight:" + target);

        var during = page.GetProperty("during");
        Assert.True(during.GetProperty("disabled").GetBoolean(), "the pressed button is disabled while its write is in flight");
        Assert.Equal(write, Assert.Single(Strings(during.GetProperty("writes"))));

        // The second press, as a click and forced past the disabled state, sent nothing more.
        Assert.Equal(write, Assert.Single(Strings(page.GetProperty("afterSecondPress").GetProperty("writes"))));

        var after = page.GetProperty("after");
        Assert.Equal(write, Assert.Single(Strings(after.GetProperty("writes"))));
        Assert.False(after.GetProperty("disabled").GetBoolean(), "the button is enabled again once the answer lands");
    }

    // The Agent changes tab tracks its buttons: a switch or undo write disables the switch and every Undo, not only the one pressed (#7954).
    [Theory]
    [InlineData("agentSwitch")]
    [InlineData("agentUndo")]
    public void AnAgentChangesWrite_DisablesTheSwitchAndUndo_UntilTheAnswerLands(string target)
    {
        var page = AdminServerEditBehaviourTests.Run("inFlight:" + target);

        var during = page.GetProperty("during").GetProperty("tracked");
        Assert.True(during.GetProperty("switch").GetBoolean(), "the switch is disabled while the write is in flight");
        Assert.True(during.GetProperty("undo").GetBoolean(), "Undo is disabled while the write is in flight");

        var after = page.GetProperty("after").GetProperty("tracked");
        Assert.False(after.GetProperty("switch").GetBoolean());
        Assert.False(after.GetProperty("undo").GetBoolean());
    }
}
