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
/// #7951: the Admin route is polled every 60 s. The poll's render leaves a tab as it is while a box in it has the focus, and the Alert
/// Settings editors keep what was typed, and the Preview's result, across a rebuild (the shipped page scripts, run under Node on the
/// fake DOM of admin-server-edit-harness.mjs, whose focus model blurs a node taken off the page).
/// </summary>
[Trait("Cost", "Slow")]
public sealed class AdminPollBehaviourTests
{
    /* The box being typed in is the same node after the tick, still on the page and still focused: the tab was not rebuilt. */
    [Fact]
    public void APollWhileARoutesBoxHasTheFocus_LeavesThatBoxOnThePage_StillFocused()
    {
        var result = AdminServerEditBehaviourTests.Run("adminPoll:focus");

        Assert.True(result.GetProperty("onPage").GetBoolean());
        Assert.True(result.GetProperty("focused").GetBoolean());
        Assert.True(result.GetProperty("sameNode").GetBoolean());
    }

    /* With the focus in the Title box the tick leaves the editor alone; with the focus dropped the tick rebuilds it from the read, and the
       typed Title and full-backup age are still in the new boxes while the untouched boxes show the saved values. A successful Save sends
       the typed values and the repaint shows the read's. */
    [Fact]
    public void TextTypedIntoTheAlertSettingsEditors_SurvivesAPoll_WithTheFocusKeptAndAfterARebuild()
    {
        var result = AdminServerEditBehaviourTests.Run("adminPoll:draft");

        var kept = result.GetProperty("focusKept");
        Assert.True(kept.GetProperty("sameNode").GetBoolean());
        Assert.True(kept.GetProperty("focused").GetBoolean());
        Assert.Equal("Typed title", kept.GetProperty("title").GetString());

        var rebuilt = result.GetProperty("rebuilt");
        Assert.False(rebuilt.GetProperty("sameNode").GetBoolean());
        Assert.Equal("Typed title", rebuilt.GetProperty("title").GetString());
        Assert.Equal("07:30", rebuilt.GetProperty("sendAt").GetString());
        Assert.Equal("UTC", rebuilt.GetProperty("zone").GetString());
        Assert.Equal("200", rebuilt.GetProperty("backup").GetString());
        Assert.Equal("60", rebuilt.GetProperty("log").GetString());
        Assert.Equal("336", rebuilt.GetProperty("checkdb").GetString());

        var write = Assert.Single(result.GetProperty("writes").EnumerateArray().ToArray());
        Assert.Equal("Typed title", write.GetProperty("title").GetString());
        Assert.Equal("Daily", result.GetProperty("afterSave").GetString());
    }

    /* The poll rebuilt the editor while the Preview's read was in flight: the new box says it is building, and the read's answer lands in
       the box on the page, not in the one taken off it. */
    [Fact]
    public void APreviewInFlightAcrossAPoll_LandsInTheBoxOnThePage()
    {
        var result = AdminServerEditBehaviourTests.Run("adminPoll:preview");

        Assert.True(result.GetProperty("newBox").GetBoolean());
        Assert.Contains("Building the preview", result.GetProperty("whileHeld").GetString(), StringComparison.Ordinal);
        Assert.Contains("PREVIEW BODY 123", result.GetProperty("onPage").GetString(), StringComparison.Ordinal);
        Assert.Equal(1, result.GetProperty("reads").GetInt32());
    }
}
