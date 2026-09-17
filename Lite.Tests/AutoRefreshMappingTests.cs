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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3479 — pins the auto-refresh interval mapping to the XAML combo it gives meaning to. The interval
/// picker's items live in <c>ServerTab.xaml</c>; what each index MEANS lives in
/// <c>ServerTab.AutoRefreshSecondsForIndex</c>, whose forerunner was a private switch with a silent
/// default arm — a fourth combo item added without a fourth arm would not error, it would quietly run
/// at one minute while displaying something else. Now that the same mapping also feeds settings.json
/// (the fix for the interval resetting on every launch), that drift would additionally PERSIST wrong,
/// so this guard makes it a test failure that names the item instead.
///
/// <para>Text-scans the SOURCE XAML located from this file's compile-time path, exactly like
/// <c>GridPayloadColumnOrderPinTests</c> — no WPF instantiation, because the invariant is textual:
/// "every item the XAML offers has its own arm in both directions, and the labels say what the arms
/// do." The mapping methods themselves are called for real; they are static and load no UI.</para>
/// </summary>
public sealed class AutoRefreshMappingTests
{
    private static readonly string ServerTabXaml = Path.Combine("Lite", "Controls", "ServerTab.xaml");

    [Fact]
    public void EveryComboItem_HasItsOwnInterval_AndRoundTrips()
    {
        var items = ComboItemLabels();

        /* Floor, not equality: the point is that the scan still sees the combo. Three items today. */
        Assert.True(items.Count >= 3,
            $"Only {items.Count} AutoRefreshIntervalCombo items parsed from {ServerTabXaml} — " +
            "the XAML scan is broken, not the combo.");

        var seconds = new List<int>();
        for (var index = 0; index < items.Count; index++)
        {
            var forIndex = ServerTab.AutoRefreshSecondsForIndex(index);

            /* The label IS the contract with the user: an item that says "5m" must map to 300, or the
               picker shows one interval while the timer (and now settings.json) runs another. */
            Assert.True(forIndex == LabelSeconds(items[index]),
                $"AutoRefreshIntervalCombo item {index} is labeled '{items[index]}' " +
                $"({LabelSeconds(items[index])}s) but AutoRefreshSecondsForIndex maps it to {forIndex}s. " +
                "Teach the switch the label's value, or fix the label.");

            /* Restore must return exactly this index, which is what fails when an item is added without
               an arm: the new index falls into the default arm's seconds, and mapping those seconds back
               lands on the default index, not the new one. */
            Assert.True(ServerTab.AutoRefreshIndexForSeconds(forIndex) == index,
                $"AutoRefreshIntervalCombo item {index} ('{items[index]}') does not round-trip: " +
                $"{forIndex}s restores as index {ServerTab.AutoRefreshIndexForSeconds(forIndex)}. " +
                "An interval that cannot round-trip resets to the default on every launch — " +
                "the exact defect #3479 was filed about.");

            seconds.Add(forIndex);
        }

        /* Two items sharing seconds would round-trip one of them onto the other. */
        Assert.Equal(seconds.Count, seconds.Distinct().Count());
    }

    [Fact]
    public void UnofferedStoredSeconds_RestoreTheXamlDefault()
    {
        var xamlDefaultIndex = ComboDefaultIndex();

        /* A hand-edited settings.json, a zero, a negative — none of these may pick a combo position
           that does not exist or leave the picker blank over a running timer. They land on the same
           index a fresh install shows. */
        foreach (var junk in new[] { 0, -1, 45, 90, int.MaxValue })
        {
            Assert.True(ServerTab.AutoRefreshIndexForSeconds(junk) == xamlDefaultIndex,
                $"A stored interval of {junk}s restored to index " +
                $"{ServerTab.AutoRefreshIndexForSeconds(junk)}, not the XAML default " +
                $"index {xamlDefaultIndex}.");
        }

        /* And the forward default arm agrees: an impossible index runs at the same interval the
           default item advertises, which is the old private switch's behavior, kept on purpose. */
        var defaultSeconds = ServerTab.AutoRefreshSecondsForIndex(xamlDefaultIndex);
        Assert.Equal(defaultSeconds, ServerTab.AutoRefreshSecondsForIndex(-1));
        Assert.Equal(defaultSeconds, ServerTab.AutoRefreshSecondsForIndex(ComboItemLabels().Count));
    }

    /// <summary>
    /// The fresh-install contract: with no settings.json, every tab constructs from
    /// <c>App.AutoRefreshEnabled</c> / <c>App.AutoRefreshIntervalSeconds</c> — the XAML attribute values
    /// only survive until the constructor body overwrites them. If the statics' initializers and the
    /// XAML attributes disagree, the XAML becomes documentation of a default nobody gets.
    /// </summary>
    [Fact]
    public void AppDefaults_MatchTheXamlDefaults()
    {
        Assert.Equal(CheckBoxDefault(), PerformanceMonitorLite.App.AutoRefreshEnabled);
        Assert.Equal(
            ComboDefaultIndex(),
            ServerTab.AutoRefreshIndexForSeconds(PerformanceMonitorLite.App.AutoRefreshIntervalSeconds));
    }

    /// <summary>"30s" -> 30, "1m" -> 60, "5m" -> 300. Fails loudly on a shape it cannot read rather
    /// than guessing, so a renamed item breaks the test that depends on it, not silently.</summary>
    private static int LabelSeconds(string label)
    {
        var match = Regex.Match(label, "^(?<n>\\d+)(?<unit>[sm])$");
        Assert.True(match.Success,
            $"AutoRefreshIntervalCombo item label '{label}' is not <number>s or <number>m — " +
            "teach LabelSeconds the new shape alongside the new item.");

        var n = int.Parse(match.Groups["n"].Value);
        return match.Groups["unit"].Value == "m" ? n * 60 : n;
    }

    private static List<string> ComboItemLabels() =>
        Regex.Matches(ComboElement(), "<ComboBoxItem\\s+Content=\"(?<label>[^\"]+)\"")
            .Select(m => m.Groups["label"].Value)
            .ToList();

    private static int ComboDefaultIndex()
    {
        var match = Regex.Match(ComboElement(), "SelectedIndex=\"(?<index>\\d+)\"");
        Assert.True(match.Success, "AutoRefreshIntervalCombo carries no SelectedIndex in the XAML.");
        return int.Parse(match.Groups["index"].Value);
    }

    private static bool CheckBoxDefault()
    {
        var source = ReadRepoFile(ServerTabXaml);
        var tagStart = source.IndexOf("x:Name=\"AutoRefreshCheckBox\"", StringComparison.Ordinal);
        Assert.True(tagStart >= 0, "AutoRefreshCheckBox not found in ServerTab.xaml.");

        var tag = source[tagStart..source.IndexOf("/>", tagStart, StringComparison.Ordinal)];
        var match = Regex.Match(tag, "IsChecked=\"(?<value>True|False)\"");
        Assert.True(match.Success, "AutoRefreshCheckBox carries no IsChecked in the XAML.");
        return match.Groups["value"].Value == "True";
    }

    /// <summary>The combo's full element text, opening tag through its closing tag, so item counting
    /// cannot pick up a neighboring combo's items.</summary>
    private static string ComboElement()
    {
        var source = ReadRepoFile(ServerTabXaml);
        var nameAt = source.IndexOf("x:Name=\"AutoRefreshIntervalCombo\"", StringComparison.Ordinal);
        Assert.True(nameAt >= 0, "AutoRefreshIntervalCombo not found in ServerTab.xaml.");

        var start = source.LastIndexOf("<ComboBox", nameAt, StringComparison.Ordinal);
        var end = source.IndexOf("</ComboBox>", nameAt, StringComparison.Ordinal);
        Assert.True(start >= 0 && end > start,
            "AutoRefreshIntervalCombo's element could not be delimited — did it become self-closing?");

        return source[start..end];
    }

    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
