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
using Xunit;

namespace Lite.Tests;

/// <summary>
/// THE CAPABILITY-INVENTORY PIN (parity board — front-end SHELL COLLAPSE, Lite side). The upcoming collapse
/// hoists duplicated code-behind out of <c>Lite/Controls/ServerTab.*</c> (and the history/drill windows it
/// opens) into shared <c>Ui</c>/<c>Common</c>. WPF's generated <c>Connect()</c> only errors on a DANGLING
/// handler reference (XAML naming a method that does not exist); it does NOT catch REMOVING the XAML wire,
/// nor a dropped code-behind <c>new DataGridFilterManager&lt;T&gt;()</c> / chart-menu wiring / a grid's
/// <c>RowStyle</c> (how its whole context menu attaches). Those are the "went missing" failure modes.
///
/// MECHANISM = RATCHET, not golden (mirrors <see cref="CollectorViewerCoverageTests"/> /
/// <see cref="CrossAppMcpToolInventoryPinTests"/> — a subset assertion + a non-vacuous floor + a scan
/// self-check, NOT an exact snapshot). Each section captures a capability SET from the CURRENT source and
/// asserts the committed BASELINE is a SUBSET of it: a captured capability may never disappear, but NEW ones
/// are free (no regen needed as later waves add grids/tabs). Deliberately removing a capability is an
/// explicit baseline edit in the same PR — never a silent wholesale regenerate (the trap an exact golden
/// falls into when a PR both adds and drops). Regenerate the baselines ONLY on a real, intended surface
/// change by running with <c>PM_PIN_REGEN=1</c>.
///
/// Text-scans SOURCE (XAML + .cs), located from this file's compile-time path — NO WPF / DuckDB / assembly
/// load, exactly like the other parity pins.
/// </summary>
public sealed class ServerTabCapabilityPinTests
{
    /* ---------------- section facts ---------------- */

    [Fact]
    public void Tabs_NoneDisappeared()
    {
        var tabs = ScanTabs();
        AssertRatchet("tabs", tabs, floor: 15);
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
        var charts = ScanChartWiring();
        AssertRatchet("chartwiring", charts, floor: 15);
    }

    [Fact]
    public void ContextMenuCommands_NoneDisappeared()
    {
        var cmds = ScanMenuCommands();
        AssertRatchet("menucmds", cmds, floor: 15);
    }

    [Fact]
    public void GridStyleMenuMap_NoneDisappeared()
    {
        // Section E: which grid POINTS at which RowStyle (its whole context menu attaches via RowStyle).
        // Section D sees the menu resource; only THIS sees the grid->menu binding. Drop a grid's RowStyle and
        // its `Grid=>StyleKey` pair flips to `Grid=>(none)` -> the baseline pair vanishes -> red.
        var map = ScanGridStyleMap();
        AssertRatchet("gridstyle", map, floor: 20);
    }

    [Fact]
    public void GridEventWires_NoneDisappeared()
    {
        // Section F: XAML MouseDoubleClick/SelectionChanged wires (grid double-click opens plan tabs / drill;
        // MainTabControl SelectionChanged is the routing wire). Removing the wire compiles clean — not
        // compiler-caught in the removal direction.
        var wires = ScanEventWires();
        AssertRatchet("eventwires", wires, floor: 6);
    }

    /// <summary>
    /// #2833 regression pin: the ServerTab Ctrl+V plan-paste handler must never call
    /// <c>Clipboard.GetText()</c> bare -- an unguarded read throws <c>COMException</c>
    /// (<c>CLIPBRD_E_CANT_OPEN</c>) when another process momentarily holds the clipboard and crashed the app.
    /// The read now routes through the guarded <c>PerformanceMonitor.Ui.ClipboardText.TryRead</c> helper.
    /// </summary>
    [Fact]
    public void ServerTabPastePath_NoBareClipboardGetText()
    {
        foreach (var file in ServerTabCsFiles())
        {
            var source = File.ReadAllText(file);
            Assert.False(source.Contains("Clipboard.GetText(", StringComparison.Ordinal),
                $"a bare 'Clipboard.GetText(' reappeared in {Path.GetFileName(file)} -- route it through " +
                "PerformanceMonitor.Ui.ClipboardText.TryRead so a CLIPBRD_E_CANT_OPEN can't crash the app (#2833).");
        }
    }

    /* ---------------- scans ---------------- */

    private static ISet<string> ScanTabs()
    {
        var xaml = ServerTabXaml();
        return Regex.Matches(xaml, @"<TabItem\b[^>]*?\bHeader=""([^""]+)""")
            .Select(m => m.Groups[1].Value.Trim())
            .Where(s => s.Length > 0)
            .ToHashSet(StringComparer.Ordinal);
    }

    private static (ISet<string> names, int primitives, int matches) ScanFilterManagers()
    {
        // File-qualified so a per-site drop is visible even when two files bind the same grid name
        // (the history windows all bind "HistoryDataGrid"). ServerTab.* + the windows it opens (P4: the pin's
        // protected surface must be a superset of the collapse's edit surface — C1 de-dups the window filter
        // plumbing).
        var names = new HashSet<string>(StringComparer.Ordinal);
        int primitives = 0, matches = 0;
        foreach (var file in ServerTabCsFiles().Concat(WindowCsFiles()))
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
        // The chart FIELD NAME as first arg to ANY wiring primitive — anchored on the chart, not a specific
        // builder method, so a later rename of the builder does not trip the pin (plan D3/D4).
        var src = string.Join("\n", ServerTabCsFiles().Select(File.ReadAllText));
        return Regex.Matches(src,
                @"(?:SetupChartContextMenu|BuildChartContextMenu|AddChartDrillDownMenuItem|AddWaitDrillDownMenuItem|WireChartContextMenus|WireChartDrillDowns)\s*\(\s*(\w+)")
            .Select(m => m.Groups[1].Value)
            .Where(n => n.EndsWith("Chart", StringComparison.Ordinal))
            .ToHashSet(StringComparer.Ordinal);
    }

    private static ISet<string> ScanMenuCommands()
    {
        // Menu-QUALIFIED (key:Header=>Click) so the shared Copy/Export commands — byte-identical across the
        // three grid menus — do not dedup into one, AND a command dropped from ONE specific menu is caught.
        var xaml = ServerTabXaml();
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
        var xaml = ServerTabXaml();
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
        // (Darling wires its history-grid double-clicks in code; catch that style too for parity + robustness).
        var wires = new HashSet<string>(StringComparer.Ordinal);
        foreach (Match m in Regex.Matches(ServerTabXaml(), @"\b(MouseDoubleClick|SelectionChanged)=""(\w+)"""))
        {
            wires.Add($"{m.Groups[1].Value}=>{m.Groups[2].Value}");
        }
        foreach (var file in ServerTabCsFiles())
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

        var file = Path.Combine(BaselineDir(), $"servertab-{section}.baseline.txt");

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
            $"\nIf this removal is INTENTIONAL, delete those lines from Lite.Tests/CapabilityPins/" +
            $"servertab-{section}.baseline.txt in THIS PR (never regenerate wholesale — that masks accidental drops).");
    }

    private static bool Regen() =>
        string.Equals(Environment.GetEnvironmentVariable("PM_PIN_REGEN"), "1", StringComparison.Ordinal);

    private static string ServerTabXaml() =>
        File.ReadAllText(Path.Combine(ControlsDir(), "ServerTab.xaml"));

    private static IEnumerable<string> ServerTabCsFiles() =>
        Directory.EnumerateFiles(ControlsDir(), "*.cs", SearchOption.TopDirectoryOnly)
            .Where(f => Path.GetFileName(f).StartsWith("ServerTab", StringComparison.OrdinalIgnoreCase));

    private static IEnumerable<string> WindowCsFiles() =>
        Directory.EnumerateFiles(WindowsDir(), "*.xaml.cs", SearchOption.TopDirectoryOnly);

    private static string ControlsDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls"));

    private static string WindowsDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Windows"));

    private static string BaselineDir([CallerFilePath] string thisFile = "") =>
        Path.Combine(Path.GetDirectoryName(thisFile)!, "CapabilityPins");
}
