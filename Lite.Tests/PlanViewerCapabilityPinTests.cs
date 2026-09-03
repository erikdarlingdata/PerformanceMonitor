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
using Xunit;

namespace Lite.Tests;

/// <summary>
/// CAPABILITY PIN for the STANDALONE Plan Viewer (front-end SHELL COLLAPSE, Lite side). The collapse hoisted
/// ~300 near-verbatim lines out of <c>Lite/MainWindow.PlanViewer.cs</c> (and the viewer's twin) into the shared
/// <see cref="PerformanceMonitor.Ui.StandalonePlanViewerController"/>; this file keeps only the app-specific
/// outer-container reveal/close plus the XAML-wired drag/drop/key forwarders. The failure modes this guards:
///
///  - a plan-loading / drag-drop / paste / open-file capability silently dropped while moving code into the
///    controller (WPF's generated <c>Connect()</c> does NOT catch a REMOVED behavior, only a dangling handler);
///  - a per-app shell WIRE (<c>PreviewDrop="MainWindowPlanViewer_Drop"</c> etc.) removed from the app's
///    MainWindow.xaml -- compiles clean, the handler just stops firing;
///  - the thin per-app entry methods (Open / Close) dropped from the code-behind.
///
/// MECHANISM = RATCHET (mirrors <see cref="ServerTabCapabilityPinTests"/>): a fixed probe table is matched
/// against the COMBINED plan-viewer surface (this app's MainWindow.xaml + MainWindow.PlanViewer.cs + the shared
/// controller), and the committed BASELINE of matched probe names must remain a SUBSET of what currently
/// matches. A capability may never disappear; adding new probes that match is free. Intentionally removing a
/// capability is an explicit baseline edit in the SAME PR (delete the line), never a wholesale regenerate.
/// Regenerate ONLY on a real surface change via <c>PM_PIN_REGEN=1</c>. Text-scans SOURCE, located from this
/// file's compile-time path -- NO WPF / assembly load, like the other parity pins.
/// </summary>
public sealed class PlanViewerCapabilityPinTests
{
    /* Probe = (stable capability name, a substring that proves it is still present). Substrings, not regex,
       so parens/asterisks in the file-dialog filter are literal. Wire probes use the ="Handler" form so they
       match ONLY the XAML attribute (the code-behind method definition is `Handler(` with a paren). */
    private static readonly (string Name, string Pattern)[] Probes =
    {
        // Core plan-viewer behavior (now in the shared controller)
        ("add-tab-sentinel",           "__PLAN_ADD_TAB__"),
        ("open-file-button",           "Open .sqlplan File"),
        ("paste-button",               "Paste XML"),
        ("empty-state-label",          "New Plan"),
        ("file-dialog-filter",         "SQL Plan Files (*.sqlplan)"),
        ("multi-file-open",            "Multiselect = true"),
        ("drag-filedrop",              "DataFormats.FileDrop"),
        ("drag-copy-effect",           "DragDropEffects.Copy"),
        ("clipboard-paste",            "Clipboard.GetText"),
        ("render-loadplan",            ".LoadPlan("),
        ("open-file-dialog",           "OpenFileDialog"),
        ("ctrl-v-paste",               "Key.V"),
        ("viewer-cleanup",             ".Cleanup()"),
        ("empty-state-icon",           "Segoe MDL2 Assets"),
        ("unique-sub-tab-label",       "GetUniqueSubTabLabel"),
        ("shared-plan-viewer-control", "new PlanViewerControl"),
        // Per-app XAML shell wires (removal compiles clean)
        ("wire-open",                  "=\"OpenPlanViewerButton_Click\""),
        ("wire-close",                 "=\"MainWindowPlanViewerClose_Click\""),
        ("wire-dragover",              "=\"MainWindowPlanViewer_DragOver\""),
        ("wire-drop",                  "=\"MainWindowPlanViewer_Drop\""),
        ("wire-keydown",               "=\"MainWindowPlanViewer_KeyDown\""),
        // Per-app thin entry-method definitions
        ("entry-open",                 "OpenPlanViewerButton_Click(object sender"),
        ("entry-close",                "MainWindowPlanViewerClose_Click(object sender"),
        // Re-entrancy guard for the "+"-sentinel auto-add (#2825): the deferral latch must stay present so the
        // FinOps "View Stored Plan" crash (Items.Insert mid container-generation) cannot silently regress.
        ("reentrancy-defer-add",       "_addTabInsertDeferred"),
    };

    [Fact]
    public void PlanViewerCapabilities_NoneDisappeared()
    {
        var matched = ScanCapabilities();
        AssertRatchet("planviewer", matched, floor: 20);
    }

    /* ---------------- scan ---------------- */

    private static ISet<string> ScanCapabilities()
    {
        var combined = CombinedSource();
        return Probes.Where(p => combined.Contains(p.Pattern, StringComparison.Ordinal))
            .Select(p => p.Name)
            .ToHashSet(StringComparer.Ordinal);
    }

    private static string CombinedSource()
    {
        var parts = new List<string>
        {
            File.ReadAllText(AppXaml()),
            File.ReadAllText(AppPlanViewerCs()),
        };
        var controller = ControllerCs();
        // Tolerate the controller being absent (pre-hoist baselining): the union still finds the app-side tokens.
        if (File.Exists(controller))
        {
            parts.Add(File.ReadAllText(controller));
        }
        return string.Join("\n", parts);
    }

    /* ---------------- ratchet + source location ---------------- */

    private static void AssertRatchet(string section, ISet<string> current, int floor)
    {
        Assert.True(current.Count >= floor,
            $"plan-viewer capability scan matched only {current.Count} probes (< floor {floor}) -- the scan is " +
            "likely broken (source moved / renamed). Fix the scan; do not lower the floor.");

        var file = Path.Combine(BaselineDir(), $"{section}.baseline.txt");

        if (Regen())
        {
            Directory.CreateDirectory(BaselineDir());
            File.WriteAllLines(file, current.OrderBy(x => x, StringComparer.Ordinal));
            return;
        }

        Assert.True(File.Exists(file),
            $"baseline '{Path.GetFileName(file)}' is missing -- run the pin once with environment variable " +
            "PM_PIN_REGEN=1 to generate it, then commit it.");

        var baseline = File.ReadAllLines(file)
            .Select(l => l.Trim())
            .Where(l => l.Length > 0 && !l.StartsWith("#", StringComparison.Ordinal))
            .ToHashSet(StringComparer.Ordinal);

        var missing = baseline.Where(b => !current.Contains(b))
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToList();

        Assert.True(missing.Count == 0,
            "plan-viewer capability(ies) in the baseline are NO LONGER present in the source -- a capability " +
            "silently went missing during the collapse:\n  " +
            string.Join("\n  ", missing) +
            "\nIf this removal is INTENTIONAL, delete those lines from Lite.Tests/CapabilityPins/" +
            $"{section}.baseline.txt in THIS PR (never regenerate wholesale -- that masks accidental drops).");
    }

    private static bool Regen() =>
        string.Equals(Environment.GetEnvironmentVariable("PM_PIN_REGEN"), "1", StringComparison.Ordinal);

    private static string AppXaml([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "MainWindow.xaml"));

    private static string AppPlanViewerCs([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "MainWindow.PlanViewer.cs"));

    private static string ControllerCs([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Ui", "StandalonePlanViewerController.cs"));

    private static string BaselineDir([CallerFilePath] string thisFile = "") =>
        Path.Combine(Path.GetDirectoryName(thisFile)!, "CapabilityPins");
}
