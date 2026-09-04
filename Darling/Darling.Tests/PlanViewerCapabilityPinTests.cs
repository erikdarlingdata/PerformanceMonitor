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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// CAPABILITY PIN for the STANDALONE Plan Viewer (front-end SHELL COLLAPSE, Darling-viewer side) -- the twin of
/// Lite.Tests' <c>PlanViewerCapabilityPinTests</c>. The collapse hoisted ~300 near-verbatim lines out of the
/// viewer's <c>MainWindow.PlanViewer.cs</c> into the shared
/// <see cref="PerformanceMonitor.Ui.StandalonePlanViewerController"/>; this file keeps only the app-specific
/// outer-container reveal/close (<c>MainTabs</c> vs the no-servers empty state), the store->viewer entry
/// (<c>OpenStoredPlanInPlanViewer</c>, fed by <c>FinOpsTab.PlanRequested</c>), and the XAML-wired drag/drop/key
/// forwarders.
///
/// MECHANISM = RATCHET (see the Lite twin for the full rationale): a fixed probe table is matched against the
/// COMBINED plan-viewer surface (this app's MainWindow.xaml + MainWindow.PlanViewer.cs + the shared controller),
/// and the committed BASELINE of matched probe names must remain a SUBSET of what currently matches. A capability
/// may never disappear; new matching probes are free. Intentional removal is an explicit baseline-line delete in
/// the same PR; regenerate ONLY via <c>PM_PIN_REGEN=1</c>. Text-scans SOURCE, located from this file's
/// compile-time path -- NO WPF / assembly load.
/// </summary>
public sealed class PlanViewerCapabilityPinTests
{
    /* Probe = (stable capability name, a substring that proves it is still present). Wire probes use the
       ="Handler" form so they match ONLY the XAML attribute, not the code-behind method definition. */
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
        ("clipboard-paste",            "ClipboardText.TryRead"), // #2833: reads route through the guarded helper
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
        // Darling-only: the FinOps "View Plan" store->viewer entry
        ("entry-stored-plan",          "OpenStoredPlanInPlanViewer("),
        // Re-entrancy guard for the "+"-sentinel auto-add (#2825): the deferral latch must stay present so the
        // FinOps "View Stored Plan" crash (Items.Insert mid container-generation) cannot silently regress.
        ("reentrancy-defer-add",       "_addTabInsertDeferred"),
    };

    [Fact]
    public void PlanViewerCapabilities_NoneDisappeared()
    {
        var matched = ScanCapabilities();
        AssertRatchet("planviewer", matched, floor: 21);
    }

    /// <summary>
    /// #2833 regression pin: the plan-viewer paste paths must NEVER call <c>Clipboard.GetText()</c> bare
    /// again. An unguarded read throws <c>COMException</c> (<c>CLIPBRD_E_CANT_OPEN</c>) whenever another
    /// process momentarily holds the clipboard and took the whole app down (this surfaced in the Darling
    /// viewer). Every read now routes through <c>PerformanceMonitor.Ui.ClipboardText.TryRead</c> -- the one
    /// allowed occurrence, which lives in its own file that this scan deliberately does not include. Guards
    /// the SHARED paste surface (the controller and <c>PlanViewerControl.Interaction.cs</c>, used by all
    /// three front ends) plus this app's own plan-viewer code-behind.
    /// </summary>
    [Fact]
    public void PlanViewerPastePaths_NoBareClipboardGetText()
    {
        foreach (var file in PastePathCsFiles())
        {
            Assert.True(File.Exists(file), $"paste-path source not found: {file} (scan is broken -- fix the path).");
            var source = File.ReadAllText(file);
            Assert.False(source.Contains("Clipboard.GetText(", StringComparison.Ordinal),
                $"a bare 'Clipboard.GetText(' reappeared in {Path.GetFileName(file)} -- route it through " +
                "PerformanceMonitor.Ui.ClipboardText.TryRead so a CLIPBRD_E_CANT_OPEN can't crash the app (#2833).");
        }
    }

    /// <summary>
    /// #2870 regression pin: the shared standalone Plan Viewer paste handlers must keep their per-surface
    /// re-entrancy guard. #2837 moved the clipboard can't-open retry off a synchronous <c>Thread.Sleep</c> onto
    /// an awaited <c>Task.Delay</c>, which keeps the UI pump responsive but yields the thread for the retry
    /// window; the old sleep had incidentally serialized input, so without a guard a HELD Ctrl+V (OS key-repeat)
    /// can put several reads in flight at once and each success calls <c>LoadPlan</c> -- several "Pasted Plan"
    /// tabs from one keypress. The guard is a <c>_pasteInProgress</c> flag set synchronously before the awaited
    /// read and cleared in a <c>finally</c>, so a repeat paste during the retry window is dropped. Guards the
    /// SHARED paste surface used by all three front ends: the controller's Paste XML button + Ctrl+V
    /// <c>HandleKeyDown</c>, and <c>PlanViewerControl.Interaction.cs</c>'s Ctrl+V handler. (The app-side
    /// MainWindow.PlanViewer.cs only FORWARDS to the controller, so it carries no guard of its own.) Drop the
    /// guard from either shared file and this fails.
    /// </summary>
    [Fact]
    public void PlanViewerPastePaths_HaveReentrancyGuard()
    {
        foreach (var file in ReentrancyGuardedPasteCsFiles())
        {
            Assert.True(File.Exists(file), $"paste-path source not found: {file} (scan is broken -- fix the path).");
            var source = File.ReadAllText(file);
            Assert.True(source.Contains("_pasteInProgress", StringComparison.Ordinal),
                $"the paste re-entrancy guard '_pasteInProgress' is gone from {Path.GetFileName(file)} -- a HELD " +
                "Ctrl+V during a busy clipboard can again spawn concurrent paste handlers and load several " +
                "'Pasted Plan' tabs from one keypress (#2870). Restore the guard around the awaited clipboard read.");
        }
    }

    /// <summary>
    /// #2870 hardening pin: the guard must span read + PARSE + tab-create, not just the clipboard read. The real
    /// plan parse runs off the UI thread inside <c>PlanViewerControl.LoadPlan</c> (a <c>Task.Run</c>), so if the
    /// paste handler FIRE-AND-FORGETS the load (<c>async void</c> loader, or <c>_ = LoadPlan...</c>), the
    /// <c>finally</c> clears <c>_pasteInProgress</c> the moment the load is KICKED OFF, not when it FINISHES --
    /// and a held Ctrl+V re-enters during the (often longer) parse window and spawns a second tab: the same
    /// #2870 symptom, moved from the read window to the parse window. This asserts every <c>_pasteInProgress</c>
    /// guard block in the shared controller and <c>PlanViewerControl.Interaction.cs</c> AWAITS the load
    /// (<c>await LoadPlan...</c> -- covers <c>LoadPlan</c>, <c>LoadPlanIntoSubTab</c>,
    /// <c>LoadPlanIntoActivePlanSubTab</c>). The build enforces the loaders are Task-returning (you cannot
    /// <c>await</c> an <c>async void</c>), so await-in-guard + a green build == the guard spans the load.
    /// </summary>
    [Fact]
    public void PlanViewerPaste_GuardSpansTheLoad()
    {
        AssertGuardBlocksAwaitLoad(File.ReadAllText(ControllerCs()), "StandalonePlanViewerController.cs", "await LoadPlan");
        AssertGuardBlocksAwaitLoad(File.ReadAllText(InteractionCs()), "PlanViewerControl.Interaction.cs", "await LoadPlan");
    }

    /// <summary>
    /// #2828 regression pin: the inner plan <c>TabControl</c>'s <c>SelectionChanged</c> subscription must be
    /// wired in the controller CONSTRUCTOR (exactly once for the controller's lifetime), NEVER inside
    /// <c>EnsureInitialized</c>. <c>Reset()</c> (Plan Viewer close) clears the <c>_initialized</c> guard, so a
    /// subscription living in <c>EnsureInitialized</c> re-ran on every reopen; the controller instance and its
    /// injected <c>TabControl</c> both persist across open/close cycles, so N cycles leaked N handlers (a
    /// delegate/CPU leak). Guards the SHARED controller: if the subscription drifts back into
    /// <c>EnsureInitialized</c>, or a second subscription appears, this fails.
    /// </summary>
    [Fact]
    public void PlanViewerSelectionHandler_SubscribedOnceInConstructor()
    {
        var controller = ControllerCs();
        Assert.True(File.Exists(controller),
            $"controller source not found: {controller} (scan is broken -- fix the path).");
        var source = File.ReadAllText(controller);

        var ctorIdx = source.IndexOf("public StandalonePlanViewerController(", StringComparison.Ordinal);
        var ensureIdx = source.IndexOf("public void EnsureInitialized()", StringComparison.Ordinal);
        Assert.True(ctorIdx >= 0, "constructor signature not found -- scan is broken (renamed?); fix the scan.");
        Assert.True(ensureIdx >= 0, "EnsureInitialized signature not found -- scan is broken (renamed?); fix the scan.");
        Assert.True(ctorIdx < ensureIdx, "constructor no longer precedes EnsureInitialized -- fix the scan.");

        var firstSub = source.IndexOf("SelectionChanged +=", StringComparison.Ordinal);
        var lastSub = source.LastIndexOf("SelectionChanged +=", StringComparison.Ordinal);
        Assert.True(firstSub >= 0, "the inner TabControl's 'SelectionChanged +=' subscription is gone entirely (#2828).");
        Assert.True(firstSub == lastSub,
            "more than one 'SelectionChanged +=' subscription on the inner plan TabControl -- it must be wired " +
            "EXACTLY once, in the constructor (#2828).");
        Assert.True(firstSub > ctorIdx && firstSub < ensureIdx,
            "the inner plan TabControl's 'SelectionChanged +=' subscription must live in the CONSTRUCTOR, not in " +
            "EnsureInitialized -- Reset() clears _initialized, so re-subscribing there leaks a handler on every " +
            "Plan Viewer reopen (#2828).");
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
            "\nIf this removal is INTENTIONAL, delete those lines from Darling.Tests/CapabilityPins/" +
            $"{section}.baseline.txt in THIS PR (never regenerate wholesale -- that masks accidental drops).");
    }

    private static bool Regen() =>
        string.Equals(Environment.GetEnvironmentVariable("PM_PIN_REGEN"), "1", StringComparison.Ordinal);

    private static string AppXaml([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml"));

    private static string AppPlanViewerCs([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Viewer", "MainWindow.PlanViewer.cs"));

    private static string ControllerCs([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "..", "PerformanceMonitor.Ui", "StandalonePlanViewerController.cs"));

    private static string InteractionCs([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "..", "PerformanceMonitor.Ui", "PlanViewerControl.Interaction.cs"));

    // The .cs paste-path sources #2833 guards (XAML carries no clipboard read, so it is excluded here).
    private static IEnumerable<string> PastePathCsFiles()
    {
        yield return AppPlanViewerCs();
        yield return ControllerCs();
        yield return InteractionCs();
    }

    // The .cs paste-path sources whose async paste handlers #2870 guards with the _pasteInProgress re-entrancy
    // flag. The app-side MainWindow.PlanViewer.cs only FORWARDS to the controller (no inline paste), so the
    // guarded handlers live entirely in the shared controller + PlanViewerControl.Interaction.cs.
    private static IEnumerable<string> ReentrancyGuardedPasteCsFiles()
    {
        yield return ControllerCs();
        yield return InteractionCs();
    }

    /// <summary>
    /// Asserts that EVERY <c>_pasteInProgress = true;</c> ... <c>_pasteInProgress = false;</c> block in
    /// <paramref name="source"/> contains <paramref name="awaitLoadToken"/> (the load is awaited inside the
    /// guard, so the guard spans the parse, not just the clipboard read -- #2870). Fails if a guard block
    /// fire-and-forgets the load, or if a guard has no matching finally reset.
    /// </summary>
    private static void AssertGuardBlocksAwaitLoad(string source, string where, string awaitLoadToken)
    {
        const string open = "_pasteInProgress = true;";
        const string close = "_pasteInProgress = false;";
        var blocks = 0;
        var idx = 0;
        while ((idx = source.IndexOf(open, idx, StringComparison.Ordinal)) >= 0)
        {
            var end = source.IndexOf(close, idx + open.Length, StringComparison.Ordinal);
            Assert.True(end > idx,
                $"a '{open}' guard in {where} has no matching '{close}' reset -- the guard's finally is gone (#2870).");
            var block = source.Substring(idx, end - idx);
            Assert.True(block.Contains(awaitLoadToken, StringComparison.Ordinal),
                $"a _pasteInProgress guard block in {where} does not await the load ('{awaitLoadToken}' not found in " +
                "the guarded block) -- a fire-and-forget load clears the guard when the parse is KICKED OFF, not " +
                "when it FINISHES, so a held Ctrl+V re-enters during the parse window and spawns a second tab (the " +
                "#2870 symptom, moved to the parse). Await the load inside the guarded try.");
            blocks++;
            idx = end + close.Length;
        }
        Assert.True(blocks > 0,
            $"no _pasteInProgress guard block found in {where} -- the scan is broken or the guard was removed (#2870).");
    }

    private static string BaselineDir([CallerFilePath] string thisFile = "") =>
        Path.Combine(Path.GetDirectoryName(thisFile)!, "CapabilityPins");
}
