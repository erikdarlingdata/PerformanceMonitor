/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The Known Patterns page (#6334), the shipped <c>known-patterns.js</c> run under Node on a fake DOM
/// (<c>known-patterns-harness.mjs</c>): what a save posts, that the note is drawn as text, that a refusal from the server is shown
/// and keeps what was typed, the edit and delete calls, and the read-only seat. Skipped when Node is not installed.
/// </summary>
public sealed class KnownPatternsPageBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "known-patterns-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the known patterns harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the known patterns harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void ASave_PostsEveryField_AsJson_ToTheAddRoute_AndClosesTheForm()
    {
        var r = Run("save");

        var write = r.GetProperty("writes")[0];
        Assert.Equal("POST", write.GetProperty("method").GetString());
        Assert.Equal("/api/known-patterns", write.GetProperty("url").GetString());
        Assert.Equal("application/json", write.GetProperty("contentType").GetString());
        var body = write.GetProperty("req");
        Assert.Equal(7, body.GetProperty("server_id").GetInt32());
        Assert.Equal("High CPU", body.GetProperty("metric_name").GetString());
        Assert.Equal("02:00", body.GetProperty("time_of_day").GetString());
        Assert.Equal("America/New_York", body.GetProperty("time_zone").GetString());
        Assert.Equal(45, body.GetProperty("window_minutes").GetInt32());
        Assert.Equal("Nightly backup runs here.", body.GetProperty("note").GetString());
        Assert.True(r.GetProperty("expiresIsUtcIso").GetBoolean());
        Assert.True(r.GetProperty("formClosed").GetBoolean());
        Assert.Equal(1, r.GetProperty("rows").GetInt32());

        /* The zone box starts as the browser's zone, and the picker offers any server plus each registered one. */
        Assert.Equal(r.GetProperty("browserZone").GetString(), r.GetProperty("zoneDefault").GetString());
        Assert.Equal(new[] { "=Every server", "7=Server A", "9=srv-b" }, r.GetProperty("serverChoices").EnumerateArray().Select(x => x.GetString()).ToArray());
    }

    [Fact]
    public void ABlankScope_SendsNulls_AndNoZoneOrWindowWithoutATimeOfDay()
    {
        var body = Run("anyScope").GetProperty("body");

        Assert.Equal(JsonValueKind.Null, body.GetProperty("server_id").ValueKind);
        Assert.Equal(JsonValueKind.Null, body.GetProperty("metric_name").ValueKind);
        Assert.Equal(JsonValueKind.Null, body.GetProperty("time_of_day").ValueKind);
        Assert.Equal(JsonValueKind.Null, body.GetProperty("time_zone").ValueKind);
        Assert.Equal(JsonValueKind.Null, body.GetProperty("window_minutes").ValueKind);
        Assert.Equal(JsonValueKind.Null, body.GetProperty("expires_at_utc").ValueKind);
        Assert.Equal("Known quiet.", body.GetProperty("note").GetString());
    }

    [Fact]
    public void ARefusalFromTheServer_IsShownVerbatim_AndTheFormKeepsWhatWasTyped()
    {
        var r = Run("refusal");

        Assert.Equal("A known pattern needs a note: what the operator knows about it.", r.GetProperty("refusal").GetString());
        Assert.True(r.GetProperty("formStillOpen").GetBoolean());
        Assert.Equal("High CPU", r.GetProperty("typedKept").GetString());
        Assert.Equal(1, r.GetProperty("writes").GetInt32());
    }

    [Fact]
    public void ANoteWithMarkup_IsDrawnAsText_NeverAsElements()
    {
        var r = Run("noteIsText");

        Assert.Equal(r.GetProperty("hostile").GetString(), r.GetProperty("noteCell").GetString());
        Assert.Equal(0, r.GetProperty("imgNodes").GetInt32());
        Assert.Equal(0, r.GetProperty("boldNodes").GetInt32());
    }

    [Fact]
    public void AnEdit_FillsTheForm_AndPutsTheWholeEntryToItsId()
    {
        var r = Run("edit");

        var filled = r.GetProperty("prefilled");
        Assert.Equal("7", filled.GetProperty("server").GetString());
        Assert.Equal("High CPU", filled.GetProperty("metric").GetString());
        Assert.Equal("02:00", filled.GetProperty("time").GetString());
        Assert.Equal("America/New_York", filled.GetProperty("zone").GetString());
        Assert.Equal("45", filled.GetProperty("window").GetString());
        Assert.Equal("Nightly backup.", filled.GetProperty("note").GetString());

        var write = Assert.Single(r.GetProperty("writes").EnumerateArray());
        Assert.Equal("PUT", write.GetProperty("method").GetString());
        Assert.Equal("/api/known-patterns/5", write.GetProperty("url").GetString());
        var body = write.GetProperty("req");
        Assert.Equal("Nightly backup, moved.", body.GetProperty("note").GetString());
        Assert.Equal(7, body.GetProperty("server_id").GetInt32());
        Assert.Equal(45, body.GetProperty("window_minutes").GetInt32());
        /* The stored expiry (UTC, no suffix) goes through the local-time box and back as the same instant. */
        Assert.Equal("2030-01-02T03:04:00.000Z", body.GetProperty("expires_at_utc").GetString());
    }

    [Fact]
    public void ADelete_ConfirmsFirst_ThenSendsDeleteToTheId_AndRedrawsTheList()
    {
        var r = Run("remove");

        var write = Assert.Single(r.GetProperty("writes").EnumerateArray());
        Assert.Equal("DELETE", write.GetProperty("method").GetString());
        Assert.Equal("/api/known-patterns/5", write.GetProperty("url").GetString());
        Assert.Equal("application/json", write.GetProperty("contentType").GetString());
        Assert.Equal(1, r.GetProperty("confirmAsked").GetInt32());
        Assert.Equal(0, r.GetProperty("rowsAfter").GetInt32());
    }

    [Fact]
    public void AReadOnlySeat_SeesTheList_WithoutAddEditOrDelete()
    {
        var r = Run("readOnly");

        Assert.Equal(1, r.GetProperty("rows").GetInt32());
        Assert.False(r.GetProperty("newButton").GetBoolean());
        Assert.False(r.GetProperty("editButton").GetBoolean());
        Assert.False(r.GetProperty("deleteButton").GetBoolean());
    }

    [Fact]
    public void TheNoteCounter_CountsAgainstFiveHundred_AndFlagsAnOverlongNote()
    {
        var r = Run("counter");

        Assert.Equal("501 / 500", r.GetProperty("over").GetString());
        Assert.Contains("field-error", r.GetProperty("overClass").GetString());
        Assert.Equal("10 / 500", r.GetProperty("under").GetString());
    }

    [Fact]
    public void TheZoneFallsBackToTheBrowsersOnlyWhenATimeOfDayIsSet_AndEachStatusMapsToItsKind()
    {
        var r = Run("pure");

        Assert.Equal("Europe/Paris", r.GetProperty("timeOnlyZone").GetProperty("time_zone").GetString());
        Assert.Equal(JsonValueKind.Null, r.GetProperty("noTimeNoZone").GetProperty("time_zone").ValueKind);
        Assert.Equal(JsonValueKind.Null, r.GetProperty("noTimeNoZone").GetProperty("window_minutes").ValueKind);
        Assert.Equal("The refusal.", r.GetProperty("interpret400").GetProperty("message").GetString());
        Assert.Equal("notfound", r.GetProperty("interpret404").GetString());
        Assert.Equal("readonly", r.GetProperty("interpret403").GetString());
    }
}
