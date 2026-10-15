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
[Trait("Stage", "Guard")]
public sealed class ServerTabCapabilityPinTests
{
    /* ---------------- section facts ---------------- */

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

    /// <summary>
    /// #2870 regression pin: the ServerTab Ctrl+V plan-paste handler must keep its <c>_pasteInProgress</c>
    /// re-entrancy guard. #2837 moved the clipboard can't-open retry off a synchronous <c>Thread.Sleep</c> onto
    /// an awaited <c>Task.Delay</c>, which yields the UI thread for the retry window; the old sleep had
    /// incidentally serialized input, so without a guard a HELD Ctrl+V (OS key-repeat) can put several reads in
    /// flight at once and open several "Pasted Plan" tabs from one keypress. The flag is set before the awaited
    /// read and cleared in a <c>finally</c>. Scans the COMBINED ServerTab source (the guard lives in
    /// <c>ServerTab_KeyDown</c> in ServerTab.xaml.cs, one of the partial files), so drop the guard and this fails.
    /// </summary>
    [Fact]
    public void ServerTabPastePath_HasReentrancyGuard()
    {
        var combined = string.Join("\n", ServerTabCsFiles().Select(File.ReadAllText));
        Assert.True(combined.Contains("_pasteInProgress", StringComparison.Ordinal),
            "the ServerTab paste re-entrancy guard '_pasteInProgress' is gone -- a HELD Ctrl+V during a busy " +
            "clipboard can again spawn concurrent paste handlers and open several 'Pasted Plan' tabs from one " +
            "keypress (#2870). Restore the guard around the awaited clipboard read in ServerTab_KeyDown.");
    }

    /// <summary>
    /// #2870 hardening pin: the ServerTab paste guard must span read + PARSE + tab-create, not just the clipboard
    /// read. <c>OpenPlanTab</c> awaits <c>PlanViewerControl.LoadPlan</c>, whose real parse runs off the UI thread
    /// (a <c>Task.Run</c>). If <c>ServerTab_KeyDown</c> fire-and-forgets <c>OpenPlanTab</c> (<c>async void</c>, or
    /// <c>_ = OpenPlanTab(...)</c>), the <c>finally</c> clears <c>_pasteInProgress</c> when the parse is KICKED
    /// OFF, not when it FINISHES, and a held Ctrl+V re-enters during the parse. This asserts the guarded block
    /// AWAITS <c>OpenPlanTab</c>. The build enforces <c>OpenPlanTab</c> is Task-returning (you cannot
    /// <c>await</c> an <c>async void</c>), so await-in-guard + a green build == the guard spans the load.
    /// </summary>
    [Fact]
    public void ServerTabPaste_GuardSpansTheLoad()
    {
        var combined = string.Join("\n", ServerTabCsFiles().Select(File.ReadAllText));
        AssertGuardBlocksAwaitLoad(combined, "Lite ServerTab", "await OpenPlanTab");
    }

    /// <summary>
    /// Asserts that EVERY <c>_pasteInProgress = true;</c> ... <c>_pasteInProgress = false;</c> block in
    /// <paramref name="source"/> contains <paramref name="awaitLoadToken"/> (the load is awaited inside the
    /// guard, so the guard spans the parse, not just the clipboard read -- #2870).
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
                "#2870 symptom, moved to the parse). Await the load inside the guarded try (ServerTab_KeyDown).");
            blocks++;
            idx = end + close.Length;
        }
        Assert.True(blocks > 0,
            $"no _pasteInProgress guard block found in {where} -- the scan is broken or the guard was removed (#2870).");
    }

    /* ---------------- scans ---------------- */

    /* ---------------- ratchet + source location ---------------- */

    private static IEnumerable<string> ServerTabCsFiles() =>
        Directory.EnumerateFiles(ControlsDir(), "*.cs", SearchOption.TopDirectoryOnly)
            .Where(f => Path.GetFileName(f).StartsWith("ServerTab", StringComparison.OrdinalIgnoreCase));

    private static string ControlsDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls"));
}
