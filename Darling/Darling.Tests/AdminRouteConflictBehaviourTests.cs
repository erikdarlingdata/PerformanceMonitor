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
/// The Admin Notification Routes form after a save the service refuses with 409 (#7952), run under Node through the admin harness
/// (<c>admin-server-edit-harness.mjs</c>, scenario <c>routeConflict</c>). The version the form read is stale once the route was
/// changed elsewhere, so the page must not keep sending it: the refusal shows the service's sentence, the form stops editing the
/// route, and the next Edit reads the current route, whose version the next Save carries.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class AdminRouteConflictBehaviourTests
{
    private const string Older = "2026-01-01T00:00:00.1234567Z";
    private const string Newer = "2026-02-02T00:00:00.7654321Z";
    private const string Sentence = "This route was changed or removed since you opened it. Reload it and make the change again.";

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void ASaveRefusedWith409_ShowsTheSentence_StopsEditingTheRoute_AndTheNextEditSavesWithTheCurrentVersion()
    {
        var page = AdminServerEditBehaviourTests.Run("routeConflict");

        var afterRefusal = page.GetProperty("afterRefusal");
        Assert.Equal([Older], Strings(afterRefusal.GetProperty("puts")));
        Assert.Contains(Strings(afterRefusal.GetProperty("strips")), s => s.StartsWith("strip error|", StringComparison.Ordinal) && s.Contains(Sentence, StringComparison.Ordinal));
        // The form is no longer the edit of route 7 that carried the refused version.
        Assert.Equal("Add route", Assert.Single(Strings(afterRefusal.GetProperty("saveLabel"))));

        // Pressing the Save on screen once more sends no PUT with the stale version.
        Assert.Equal([Older], Strings(page.GetProperty("afterSecondPress").GetProperty("puts")));

        // Edit reads the route as it is now, and Save carries that version, which the service accepts.
        var afterEditAndSave = page.GetProperty("afterEditAndSave");
        Assert.Equal([Older, Newer], Strings(afterEditAndSave.GetProperty("puts")));
        Assert.Contains("strip notice|Route 7 updated.", Strings(afterEditAndSave.GetProperty("strips")));
    }
}
