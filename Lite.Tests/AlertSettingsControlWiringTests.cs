/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Drift guard over the alert settings windows in BOTH apps (#1840 review). Adding a threshold box is
/// four edits, not one: load it, save it, gray it out with the rest, and reset it on Restore Defaults.
/// The first two are impossible to forget — the box does nothing without them — while the last two fail
/// silently and only ever surface as a user noticing one field stayed editable. That is exactly how
/// <c>AlertBlockingWaitSecondsBox</c> shipped in Lite while Darling got all four, and writing this guard
/// found a second live instance (<c>AlertLongRunningQueryMaxResultsBox</c>) already on dev.
///
/// <para>Source-parsing rather than a WPF harness on purpose: instantiating a <c>SettingsWindow</c> needs
/// an STA thread, an <c>App</c> with loaded settings, and a live store. The invariant is textual anyway —
/// "this identifier appears in these methods" — so parsing the file tests the thing that actually
/// regresses. The apps are checked separately because their control sets legitimately differ (Darling has
/// AG boxes Lite has no concept of); what is pinned is that each app is internally consistent.</para>
/// </summary>
public sealed class AlertSettingsControlWiringTests
{
    /// <summary>
    /// Boxes that belong to alert DELIVERY rather than to a metric threshold: cooldown, delivery mode,
    /// per-event cap, and the excluded-databases filter. Restore Defaults resets them, but they are
    /// deliberately NOT gated on "Alerts Enabled" — both apps agree on this, so it is a design choice and
    /// not drift. Anything else appearing here means someone added a threshold box and forgot a call site.
    /// </summary>
    private static readonly string[] s_deliveryScopedBoxes =
    {
        "AlertCooldownBox", "AlertDeliveryModeBox", "AlertPerEventMaxBox", "AlertExcludedDatabasesBox"
    };

    public static TheoryData<string, string, string> Windows() => new()
    {
        { "Lite", Path.Combine("Lite", "Windows", "SettingsWindow.xaml.cs"), "LoadAlertSettings" },
        { "Darling", Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml.cs"), "SeedAlertControlsFrom" }
    };

    [Theory]
    [MemberData(nameof(Windows))]
    public void EveryLoadedThresholdBox_IsAlsoGatedAndReset(string app, string relativePath, string loadMethod)
    {
        var source = File.ReadAllText(FindRepoFile(relativePath));

        var loaded = ThresholdBoxesIn(source, loadMethod, app);
        var gated = ThresholdBoxesIn(source, "UpdateAlertControlStates", app);
        var reset = ThresholdBoxesIn(source, "RestoreAlertDefaultsButton_Click", app);

        Assert.NotEmpty(loaded);

        var ungated = loaded.Except(gated).Except(s_deliveryScopedBoxes).OrderBy(n => n, StringComparer.Ordinal).ToList();
        Assert.True(ungated.Count == 0,
            $"{app}: these threshold boxes are loaded but never disabled in UpdateAlertControlStates, so " +
            $"unchecking \"Alerts Enabled\" leaves them editable: {string.Join(", ", ungated)}");

        var unreset = loaded.Except(reset).OrderBy(n => n, StringComparer.Ordinal).ToList();
        Assert.True(unreset.Count == 0,
            $"{app}: these threshold boxes are loaded but never reset in RestoreAlertDefaultsButton_Click, so " +
            $"\"Restore Defaults\" leaves a stale value behind: {string.Join(", ", unreset)}");
    }

    /// <summary>
    /// The <c>Alert*Box</c> identifiers named inside one method body. Checkboxes are excluded: the enable
    /// gate touches them and Restore Defaults deliberately does not (the per-alert on/off switches are the
    /// user's, not a default to stomp).
    /// </summary>
    private static HashSet<string> ThresholdBoxesIn(string source, string methodName, string app)
    {
        var body = MethodBody(source, methodName, app);
        return Regex.Matches(body, @"\bAlert\w*Box\b")
            .Select(m => m.Value)
            .Where(n => !n.EndsWith("CheckBox", StringComparison.Ordinal))
            .ToHashSet(StringComparer.Ordinal);
    }

    /// <summary>
    /// Lite's other half of the same "adding a knob is more than one edit" problem, and the half the test
    /// above cannot see. Lite persists to settings.json rather than a store, and <c>SaveAlertSettings</c>
    /// does both jobs in one method: it copies each control into an <c>App.Alert*</c> static, then writes
    /// those statics into the <c>root[...]</c> JSON document. Forgetting the second line is silent and
    /// survives every manual test — the setting takes effect immediately and only reverts on the NEXT
    /// launch, by which point nobody connects the two. #2391 added four file-growth controls to this
    /// method; this pins that all four (and every sibling) actually reach disk.
    ///
    /// <para>Note this does NOT mean loader/writer key symmetry: the writer parses the existing
    /// settings.json into <c>root</c> and mutates it, so hand-edited keys it never touches are preserved
    /// on save. The invariant is narrower — whatever the UI can CHANGE, the UI must WRITE.</para>
    /// </summary>
    [Fact]
    public void LiteSaveAlertSettings_PersistsEveryStaticItAssigns()
    {
        var source = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "Windows", "SettingsWindow.xaml.cs")));
        var body = MethodBody(source, "SaveAlertSettings", "Lite");

        /* "App.Foo =" but not "App.Foo ==" — the assignments the Save button makes. */
        var assigned = Regex.Matches(body, @"App\.(\w+)\s*=(?!=)")
            .Select(m => m.Groups[1].Value)
            .ToHashSet(StringComparer.Ordinal);
        var persisted = Regex.Matches(body, @"root\[""[^""]+""\]\s*=\s*App\.(\w+)")
            .Select(m => m.Groups[1].Value)
            .ToHashSet(StringComparer.Ordinal);

        Assert.NotEmpty(assigned);

        /* AlertExcludedDatabases is the one legitimate exception: it is a collection, so it reaches the
           document as a JsonArray built a few lines earlier rather than as a bare "= App.X" assignment. */
        var unpersisted = assigned.Except(persisted)
            .Except(new[] { "AlertExcludedDatabases" })
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        Assert.True(unpersisted.Count == 0,
            "Lite: SaveAlertSettings copies these controls into App statics but never writes them to " +
            "settings.json, so the setting applies for this session and silently reverts on next launch: " +
            string.Join(", ", unpersisted));
    }

    private static string MethodBody(string source, string methodName, string app)
    {
        /* Not every anchor returns void — SaveAlertSettings returns bool to report whether it succeeded. */
        var signature = source.IndexOf("void " + methodName, StringComparison.Ordinal);
        if (signature < 0)
        {
            signature = source.IndexOf("bool " + methodName, StringComparison.Ordinal);
        }

        Assert.True(signature >= 0, $"{app}: no method named {methodName} — this guard's anchor moved and it is testing nothing.");

        var open = source.IndexOf('{', signature);
        Assert.True(open >= 0, $"{app}: {methodName} has no body.");

        var depth = 0;
        for (var i = open; i < source.Length; i++)
        {
            if (source[i] == '{') depth++;
            else if (source[i] == '}' && --depth == 0) return source[open..i];
        }

        throw new InvalidOperationException($"{app}: unbalanced braces scanning {methodName}.");
    }

    private static string FindRepoFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }

        throw new FileNotFoundException($"Could not locate {relativePath} walking up from {AppContext.BaseDirectory}");
    }
}
