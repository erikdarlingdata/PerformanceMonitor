/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// THE CAPABILITY-INVENTORY PIN (parity board — front-end SHELL COLLAPSE, Darling side; twin of Lite's
/// <c>ServerTabCapabilityPinTests</c>). The upcoming collapse hoists duplicated code-behind out of
/// <c>PerformanceMonitor.Darling.Viewer/ViewerServerTab.*</c> (and the history/drill windows it opens) into
/// shared <c>Ui</c>/<c>Common</c>. WPF's generated <c>Connect()</c> only errors on a DANGLING handler
/// reference; it does NOT catch REMOVING the XAML wire, nor a dropped code-behind
/// <c>new DataGridFilterManager&lt;T&gt;()</c> / chart-menu wiring / a grid's <c>RowStyle</c> (how its whole
/// context menu attaches). Those are the "went missing" failure modes.
///
/// MECHANISM = RATCHET, not golden: each section captures a capability SET from the CURRENT source and
/// asserts the committed BASELINE is a SUBSET of it (a capability may never disappear; NEW ones are free —
/// no regen as later waves add grids/tabs). Deliberate removal is an explicit baseline edit in the same PR,
/// never a silent wholesale regenerate. Regenerate baselines ONLY on a real intended surface change via
/// <c>PM_PIN_REGEN=1</c>. Text-scans SOURCE (XAML + .cs) located from this file's compile-time path — NO WPF
/// / Npgsql / assembly load, like the other Darling parity pins (see <c>ThemeCompletenessTests</c>).
/// </summary>
[Trait("Stage", "Guard")]
public sealed class ViewerServerTabCapabilityPinTests
{
    [Fact]
    public void Tabs_NoneDisappeared()
    {
        AssertRatchet("tabs", ScanTabs(), floor: 15);
    }

    [Fact]
    public void FilterManagers_NoneDisappeared()
    {
        var (mgrs, primitives, matches) = ScanFilterManagers();
        Assert.True(matches == primitives,
            $"filter-manager scan under-matched: {primitives} `new DataGridFilterManager<` but {matches} carried a " +
            "parseable grid arg. A ctor takes a non-identifier first arg — tighten the scan before trusting this pin.");
        AssertRatchet("filtermgrs", mgrs, floor: 20);
    }

    [Fact]
    public void ChartWiring_NoneDisappeared()
    {
        AssertRatchet("chartwiring", ScanChartWiring(), floor: 15);
    }

    [Fact]
    public void ContextMenuCommands_NoneDisappeared()
    {
        AssertRatchet("menucmds", ScanMenuCommands(), floor: 15);
    }

    [Fact]
    public void GridStyleMenuMap_NoneDisappeared()
    {
        // Section E: which grid POINTS at which RowStyle. Section D sees the menu resource; only THIS sees the
        // grid->menu binding. Drop a grid's RowStyle and its `Grid=>StyleKey` pair flips to `Grid=>(none)`.
        AssertRatchet("gridstyle", ScanGridStyleMap(), floor: 20);
    }

    [Fact]
    public void GridEventWires_NoneDisappeared()
    {
        // Section F: XAML MouseDoubleClick/SelectionChanged wires (grid double-click opens plan/drill; the
        // inner-TabControl SelectionChanged wires are the routing). Removing the wire compiles clean.
        AssertRatchet("eventwires", ScanEventWires(), floor: 6);
    }

    /* ---------------- scans ---------------- */

    private static ISet<string> ScanTabs()
    {
        return Regex.Matches(ViewerXaml(), @"<TabItem\b[^>]*?\bHeader=""([^""]+)""")
            .Select(m => m.Groups[1].Value.Trim())
            .Where(s => s.Length > 0)
            .ToHashSet(StringComparer.Ordinal);
    }

    private static (ISet<string> names, int primitives, int matches) ScanFilterManagers()
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        int primitives = 0, matches = 0;
        foreach (var file in ViewerServerTabCsFiles().Concat(WindowCsFiles()))
        {
            var src = File.ReadAllText(file);
            primitives += Regex.Matches(src, @"new\s+DataGridFilterManager<").Count;
            foreach (Match m in Regex.Matches(src, @"new\s+DataGridFilterManager<[^>]+>\s*\(\s*(\w+)"))
            {
                matches++;
                names.Add($"{Path.GetFileName(file)}:{m.Groups[1].Value}");
            }
        }
        return (names, primitives, matches);
    }

    private static ISet<string> ScanChartWiring()
    {
        // Chart FIELD NAME as first arg to ANY wiring primitive — Darling routes every wired chart through
        // BuildChartContextMenu(chart, ...) (directly for the menu-only CollectorDurationChart, or nested in
        // AddChart/WaitDrillDownMenuItem). Anchored on the chart, not the builder, so a rename does not trip it.
        var src = string.Join("\n", ViewerServerTabCsFiles().Select(File.ReadAllText));
        return Regex.Matches(src,
                @"(?:SetupChartContextMenu|BuildChartContextMenu|AddChartDrillDownMenuItem|AddWaitDrillDownMenuItem|WireChartContextMenus|WireChartDrillDowns)\s*\(\s*(\w+)")
            .Select(m => m.Groups[1].Value)
            .Where(n => n.EndsWith("Chart", StringComparison.Ordinal))
            .ToHashSet(StringComparer.Ordinal);
    }

    private static ISet<string> ScanMenuCommands()
    {
        // Menu-QUALIFIED (key:Header=>Click) so shared Copy/Export commands across Darling's five grid menus
        // (DataGridContextMenu / QueryGridContextMenu / QueryPlanGridContextMenu / QuerySnapshotGridContextMenu /
        // Blocked / Deadlock) do not dedup, and a command dropped from one specific menu is caught.
        var xaml = ViewerXaml();
        var cmds = new HashSet<string>(StringComparer.Ordinal);
        foreach (Match cm in Regex.Matches(xaml, @"<ContextMenu\b[^>]*?\bx:Key=""(\w+)""(.*?)</ContextMenu>", RegexOptions.Singleline))
        {
            var key = cm.Groups[1].Value;
            foreach (Match mi in Regex.Matches(cm.Groups[2].Value, @"<MenuItem\b[^>]*?>", RegexOptions.Singleline))
            {
                var tag = mi.Value;
                var h = Regex.Match(tag, @"\bHeader=""([^""]+)""");
                var c = Regex.Match(tag, @"\bClick=""(\w+)""");
                if (h.Success && c.Success)
                {
                    cmds.Add($"{key}:{h.Groups[1].Value}=>{c.Groups[1].Value}");
                }
            }
        }
        return cmds;
    }

    private static ISet<string> ScanGridStyleMap()
    {
        var xaml = ViewerXaml();
        var map = new HashSet<string>(StringComparer.Ordinal);
        foreach (Match dg in Regex.Matches(xaml, @"<DataGrid\b[^>]*?>", RegexOptions.Singleline))
        {
            var tag = dg.Value;
            var name = Regex.Match(tag, @"\bx:Name=""(\w+)""");
            if (!name.Success)
            {
                continue;
            }
            var style = Regex.Match(tag, @"\bRowStyle=""\{(?:StaticResource|DynamicResource)\s+(\w+)\}""");
            map.Add($"{name.Groups[1].Value}=>{(style.Success ? style.Groups[1].Value : "(none)")}");
        }
        return map;
    }

    private static ISet<string> ScanEventWires()
    {
        // BOTH wiring styles: XAML `Event="Handler"` attributes AND code-behind `element.Event += Handler`
        // (Darling wires its history-grid double-clicks in code — WireHistoryDrillDowns — so the XAML scan
        // alone would miss them).
        var wires = new HashSet<string>(StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(ViewerXaml(), @"\b(MouseDoubleClick|SelectionChanged)=""(\w+)"""))
        {
            wires.Add($"{m.Groups[1].Value}=>{m.Groups[2].Value}");
        }
        foreach (var file in ViewerServerTabCsFiles())
        {
            foreach (Match m in Regex.Matches(File.ReadAllText(file), @"\.(MouseDoubleClick|SelectionChanged)\s*\+=\s*(\w+)"))
            {
                wires.Add($"{m.Groups[1].Value}=>{m.Groups[2].Value}");
            }
        }
        return wires;
    }

    /* ---------------- ratchet + source location ---------------- */

    private static void AssertRatchet(string section, ISet<string> current, int floor)
    {
        Assert.True(current.Count >= floor,
            $"capability section '{section}' scanned only {current.Count} items (< floor {floor}) — the scan is " +
            "likely broken (source moved / pattern drift). Fix the scan; do not lower the floor.");

        var file = Path.Combine(BaselineDir(), $"viewerservertab-{section}.baseline.txt");

        if (Regen())
        {
            Directory.CreateDirectory(BaselineDir());
            File.WriteAllLines(file, current.OrderBy(x => x, StringComparer.Ordinal));
            return;
        }

        Assert.True(File.Exists(file),
            $"baseline '{Path.GetFileName(file)}' is missing — run the pin once with environment variable " +
            "PM_PIN_REGEN=1 to generate it, then commit it.");

        var baseline = File.ReadAllLines(file)
            .Select(l => l.Trim())
            .Where(l => l.Length > 0 && !l.StartsWith("#", StringComparison.Ordinal))
            .ToHashSet(StringComparer.Ordinal);

        var missing = baseline.Where(b => !current.Contains(b))
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToList();

        Assert.True(missing.Count == 0,
            $"capability section '{section}': item(s) in the baseline are NO LONGER present in the source — a " +
            "capability silently went missing during the collapse:\n  " +
            string.Join("\n  ", missing) +
            $"\nIf this removal is INTENTIONAL, delete those lines from Darling.Tests/CapabilityPins/" +
            $"viewerservertab-{section}.baseline.txt in THIS PR (never regenerate wholesale — that masks accidental drops).");
    }

    private static bool Regen() =>
        string.Equals(Environment.GetEnvironmentVariable("PM_PIN_REGEN"), "1", StringComparison.Ordinal);

    private static string ViewerXaml() =>
        File.ReadAllText(Path.Combine(ViewerDir(), "ViewerServerTab.xaml"));

    private static IEnumerable<string> ViewerServerTabCsFiles() =>
        Directory.EnumerateFiles(ViewerDir(), "*.cs", SearchOption.TopDirectoryOnly)
            .Where(f => Path.GetFileName(f).StartsWith("ViewerServerTab", StringComparison.OrdinalIgnoreCase));

    private static IEnumerable<string> WindowCsFiles() =>
        Directory.EnumerateFiles(ViewerDir(), "*.xaml.cs", SearchOption.TopDirectoryOnly)
            .Where(f =>
            {
                var n = Path.GetFileName(f);
                return n.EndsWith("HistoryWindow.xaml.cs", StringComparison.OrdinalIgnoreCase) ||
                       n.Equals("WaitDrillDownWindow.xaml.cs", StringComparison.OrdinalIgnoreCase);
            });

    private static string ViewerDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Viewer"));

    private static string BaselineDir([CallerFilePath] string thisFile = "") =>
        Path.Combine(Path.GetDirectoryName(thisFile)!, "CapabilityPins");
}
