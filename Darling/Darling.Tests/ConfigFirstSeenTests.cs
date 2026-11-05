/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7662: the Config Changes time is the first capture that holds the new value, and the config snapshots run on connect and once a day.
/// A change carries the capture before it, so the page can say the change happened between the two, and a <c>state_desc</c> change
/// takes the minute <c>database_states</c> (collected every minute) first saw the new state. The web rules run as shipped under Node.
/// </summary>
public sealed class ConfigFirstSeenTests
{
    private static readonly DateTime Before = new(2026, 11, 4, 18, 38, 0, DateTimeKind.Unspecified);
    private static readonly DateTime After = new(2026, 11, 5, 12, 50, 45, DateTimeKind.Unspecified);

    [Fact]
    public void AServerChange_CarriesTheCaptureBeforeIt()
    {
        var snapshots = new List<ConfigChangeDiff.ServerConfigSnapshot>
        {
            new(Before, "max server memory (MB)", 1000, 1000, true, true),
            new(After, "max server memory (MB)", 2000, 2000, true, true),
        };

        var change = Assert.Single(ConfigChangeDiff.DiffServerConfigChanges(snapshots, Before.AddDays(-1), After.AddDays(1)));

        Assert.Equal(After, change.ChangeTime);
        Assert.Equal(Before, change.PreviousCaptureTime);
    }

    [Fact]
    public void ADatabaseStateChange_TakesTheMinuteDatabaseStatesSawIt()
    {
        var seen = new DateTime(2026, 11, 4, 18, 41, 0, DateTimeKind.Unspecified);
        var changes = new List<ConfigChangeDiff.DatabaseConfigChange>
        {
            new(After, "Sales", "state_desc", "RECOVERY_PENDING", "ONLINE", Before),
            new(After, "Sales", "recovery_model", "FULL", "SIMPLE", Before),
        };
        var readings = new List<ConfigChangeDiff.DatabaseStateObservation>
        {
            new(Before.AddMinutes(1), "Sales", "RECOVERY_PENDING"),
            new(seen.AddMinutes(-1), "Sales", "RECOVERY_PENDING"),
            new(seen, "Sales", "ONLINE"),
            new(seen.AddMinutes(1), "Sales", "ONLINE"),
        };

        var result = ConfigChangeDiff.ApplyDatabaseStateTimes(changes, readings, Before.AddDays(-1), After.AddDays(1));

        var state = result.Single(c => c.SettingName == "state_desc");
        Assert.Equal(seen, state.ChangeTime);
        Assert.Null(state.PreviousCaptureTime);
        var other = result.Single(c => c.SettingName == "recovery_model");
        Assert.Equal(After, other.ChangeTime);
        Assert.Equal(Before, other.PreviousCaptureTime);
    }

    [Fact]
    public void ADatabaseStateChange_WithNoMatchingReading_KeepsTheSnapshotSpan()
    {
        var changes = new List<ConfigChangeDiff.DatabaseConfigChange> { new(After, "Sales", "state_desc", "OFFLINE", "ONLINE", Before) };
        var readings = new List<ConfigChangeDiff.DatabaseStateObservation> { new(Before.AddMinutes(5), "Sales", "OFFLINE") };

        var state = Assert.Single(ConfigChangeDiff.ApplyDatabaseStateTimes(changes, readings, Before.AddDays(-1), After.AddDays(1)));

        Assert.Equal(After, state.ChangeTime);
        Assert.Equal(Before, state.PreviousCaptureTime);
    }

    [Fact]
    public void ADatabaseStateChange_ReDatedBeforeTheWindow_IsNotAChangeOfTheWindow()
    {
        /* The prior snapshot is at -40 h, the state moved at -30 h, the next snapshot is at -2 h: a 24 h window holds the snapshot but
           not the minute the state was seen, so the row is not a change of the window. */
        var now = After;
        var changes = new List<ConfigChangeDiff.DatabaseConfigChange>
        {
            new(now.AddHours(-2), "Sales", "state_desc", "OFFLINE", "ONLINE", now.AddHours(-40)),
            new(now.AddHours(-2), "Sales", "recovery_model", "FULL", "SIMPLE", now.AddHours(-40)),
        };
        var readings = new List<ConfigChangeDiff.DatabaseStateObservation> { new(now.AddHours(-30), "Sales", "ONLINE") };

        var inWindow = ConfigChangeDiff.ApplyDatabaseStateTimes(changes, readings, now.AddHours(-24), now);
        Assert.Equal("recovery_model", Assert.Single(inWindow).SettingName);

        /* The same reading inside a 48 h window dates the row to the minute it was seen. */
        var wider = ConfigChangeDiff.ApplyDatabaseStateTimes(changes, readings, now.AddHours(-48), now);
        Assert.Equal(now.AddHours(-30), wider.Single(c => c.SettingName == "state_desc").ChangeTime);
    }

    [Fact]
    public void TheViewerGrid_ShowsTheSpan_WhenThereIsACaptureBefore()
    {
        var span = ConfigChangeRowText.FirstSeen(After, Before);
        var single = ConfigChangeRowText.FirstSeen(After, null);

        Assert.StartsWith("between ", span, StringComparison.Ordinal);
        Assert.EndsWith(" and " + single, span, StringComparison.Ordinal);
        Assert.DoesNotContain("between", single, StringComparison.Ordinal);
    }

    private static JsonElement RunNode()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "config-first-seen-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
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
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the first-seen harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the first-seen harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void TheWebGrid_ShowsTheSpanFromThePriorCapture_AndTheSnapshotAge()
    {
        var result = RunNode();

        Assert.Equal("First seen", result.GetProperty("label").GetString());
        var span = result.GetProperty("span").GetString()!;
        Assert.StartsWith("between ", span, StringComparison.Ordinal);
        Assert.Contains(" and ", span, StringComparison.Ordinal);
        Assert.DoesNotContain("between", result.GetProperty("single").GetString()!, StringComparison.Ordinal);
        var subtitle = result.GetProperty("subtitle").GetString()!;
        Assert.StartsWith("latest snapshot, captured ", subtitle, StringComparison.Ordinal);
        Assert.EndsWith("(3h ago)", subtitle, StringComparison.Ordinal);
        Assert.Equal(JsonValueKind.Null, result.GetProperty("otherSubtitle").ValueKind);
    }

    [Fact]
    public void ThePanelHeading_GetsTheSnapshotAge_OnlyForALatestSnapshotPanelWhoseAnswerHasCapturedAt()
    {
        var result = RunNode();

        /* applyDataSubtitle writes the heading itself: a "latest snapshot" panel with captured_at in its answer gets the age. */
        var written = result.GetProperty("appliedSnapshot").GetString()!;
        Assert.StartsWith(" latest snapshot, captured ", written, StringComparison.Ordinal);
        Assert.EndsWith("(3h ago)", written, StringComparison.Ordinal);
        /* An answer with no captured_at, another subtitle, and a descriptor that names its own subtitle all keep what they had. */
        Assert.Equal(" latest snapshot", result.GetProperty("appliedNoCapturedAt").GetString());
        Assert.Equal(" hourly collection", result.GetProperty("appliedOtherSubtitle").GetString());
        Assert.Equal(" from the answer", result.GetProperty("appliedSubtitleFrom").GetString());
    }
}
