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
/// <c>OnRefreshTimerTick</c>'s freshness fan-out has to sit ABOVE its tab early-return. That is a property
/// of WHERE the calls are written rather than of which calls exist, and it is the one property
/// <see cref="ViewerFleetTimerGuardTests"/> cannot see (#2923).
///
/// <para><b>The hole this closes.</b>
/// <see cref="ViewerFleetTimerGuardTests.NoStoreRead_IsFiredByBothFleetTimers"/> cuts the tick at its first
/// <c>return;</c> and intersects only the region ABOVE it with <c>OnOverviewTimerTick</c>. Move the fan-out
/// below that return and the region empties, the intersection empties with it, and the assertion is
/// satisfied BECAUSE the regression happened. Measured by mutation: relocating the three unconditional
/// calls below the early-return leaves all four of that class's facts GREEN. Its staleness guard —
/// <c>Assert.True(firstReturn.Success, …)</c> — fires only when the early-return DISAPPEARS, and a
/// relocation leaves the <c>return;</c> exactly where it was.</para>
///
/// <para><b>The relocation is a named regression, not a tidy-up.</b> #2907, in the same breath as the
/// duplication that pin does catch: moving the fan-out below the early-return "would stop the alert poll
/// and the store-size read entirely while the Overview or a per-server tab is up, which is the opposite of
/// what 'regardless of the visible tab' is there for." The Overview is the tab that ships selected, so the
/// fan-out would be dead for most operators most of the time with every existing assertion green.</para>
///
/// <para><b>The pinned set is derived, with a name list only as its staleness guard.</b> Every
/// fire-and-forget in the tick is checked, so a fan-out call added later is covered without anyone
/// remembering this file; the three names #2907 argues about are asserted to still BE in that set, so a
/// rename is loud rather than vacuously green. One corollary is deliberate: a new <c>_ = X()</c> written
/// BELOW the early-return fails this pin. Everything below that return is tab-conditional by construction,
/// so a fire-and-forget that genuinely belongs there wants a sentence here saying why.</para>
///
/// <para><b>Offsets, because a count cannot control a relocation.</b> Moving code leaves every occurrence
/// count invariant — one <c>_ = RefreshServerStatusAsync();</c> before, one after — so an anchor tally
/// reports the mutation as applied whether it applied or not, and reports this pin as red-capable when it
/// is not. The comparison itself is the only control, which is why every message here carries the offsets.
/// Measured on the shipped source as of this pin, and DESCRIPTIVE — nothing below asserts an absolute
/// value, only an ordering: with the body taken from its opening brace, the first <c>return;</c> is at
/// 2455, the four fan-out calls at 1041, 1082, 1322 and 1755, and the visible-tab refresh at 2886.
/// (#2923 cites 2454 for the return, which is the same place counted from just inside the brace, and 2881
/// for <c>_ = RefreshServerStatusAsync();</c>, which that call has at no convention — it lands on the
/// awaited visible-tab refresh at the bottom of the method. The fan-out is 1,414 characters ABOVE the
/// return, so the direction the issue concludes is right even though the number it quotes reads as
/// below.)</para>
///
/// <para><b>The walk is <see cref="CSharpSourceWalker"/> (#2913/#2925), not a sixth private copy</b> —
/// brace-matching its output to find a method body is what that class's own summary says the output is for.
/// Worth being exact about what it buys HERE, because it is less than #2923 claims. The tick's
/// comment-only mentions are ONE each of <c>RefreshServerStatusAsync</c>, <c>PollAlertsAsync</c> and
/// <c>RefreshVisibleAsync</c>, none of <c>RefreshStoreSizeAsync</c> and none of <c>return</c> — the
/// issue's 2/1/2/2/2 are the totals including the code. And none of them is call-shaped, so raw and
/// stripped agree on every call offset in this tick and stripping does not change today's answer. What it
/// buys is the NEXT comment: those three mentions sit above the early-return, at offsets 328, 472 and 622,
/// so one comment written <c>PollAlertsAsync()</c> instead of <c>PollAlertsAsync</c> — or any widening of
/// the match to a bare name — turns prose into an above-the-return hit and this pin green on the
/// regression.
/// <see cref="TheAboveTheReturnCheck_SeesARelocatedFanOut_OnlyThroughTheWalker"/> runs that case both
/// ways, so the claim is a test rather than a sentence.</para>
/// </summary>
public sealed class ViewerFleetTimerFanOutPositionTests
{
    /// <summary>The tick whose fan-out position is the subject; the Overview tick has no early-return to
    /// split on and is <see cref="ViewerFleetTimerGuardTests"/>'s side of the pair.</summary>
    private const string RefreshTick = "OnRefreshTimerTick";

    /// <summary>The tab-conditional half: awaited, and the one call that must sit BELOW the return.</summary>
    private const string VisibleRefresh = "RefreshVisibleAsync";

    /// <summary>
    /// The three calls #2907 argues about by name. Not the pinned set — that is derived — but a staleness
    /// guard on it, so a rename cannot leave behind a derived set that is trivially satisfied.
    /// </summary>
    private static readonly string[] s_namedFanOut =
    [
        "PollAlertsAsync",
        "RefreshServerStatusAsync",
        "RefreshStoreSizeAsync",
    ];

    /// <summary>A fire-and-forget call — the shape that makes a tick a fan-out.</summary>
    private static readonly Regex s_fireAndForget = new(
        @"_\s*=\s*(\w+)\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>The same expression <see cref="ViewerFleetTimerGuardTests"/> splits on, deliberately: the
    /// two pins have to agree on where the unconditional region ends, or this one is green about a boundary
    /// the other one is not using.</summary>
    private static readonly Regex s_earlyReturn = new(
        @"\breturn\s*;",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Fact]
    public void TheUnconditionalFanOut_IsFiredAboveTheTabEarlyReturn()
    {
        var body = StrippedTickBody(RefreshTick);
        var split = EarlyReturnOffset(body);
        var fired = FiredNames(body);

        /* Floor first: an empty set satisfies the position assertion vacuously, which is the shape of
           failure #2923 is about. Four when this landed — status, store size, alert poll, AG probe. */
        Assert.True(
            fired.Count >= 4,
            $"only {fired.Count} fire-and-forget call(s) parsed out of {RefreshTick} — the sweep is not "
            + "reading MainWindow, so the position assertion below would hold over nothing");

        var renamed = s_namedFanOut.Where(n => !fired.Contains(n)).ToList();

        Assert.True(
            renamed.Count == 0,
            $"{RefreshTick} no longer fires {string.Join(", ", renamed)}; this pin's name list is stale, and "
            + "a stale list is what would let the derived check hold while the calls #2907 argues about are "
            + "gone");

        var offenders = FanOutNotAbove(body, split);

        Assert.True(
            offenders.Count == 0,
            $"{RefreshTick}'s first return; is at body offset {split} and these fan-out calls are no longer "
            + "above it, so they stop firing entirely while the Overview or a per-server tab is up — the "
            + $"regression #2907 named and #2923 found unguarded: {string.Join("; ", offenders)}");
    }

    /// <summary>
    /// The other half of the ordering, and the reason the sibling pin's split means anything: the
    /// visible-tab refresh has to stay BELOW the early-return. Above it, this timer and the Overview's own
    /// would refresh the same grid every cycle at the same interval — which is the double-refresh the
    /// return exists to prevent, and exactly the narrow claim #2907's comment makes about the GRID while
    /// the freshness reads above the return are deliberately unconditional.
    /// </summary>
    [Fact]
    public void TheVisibleTabRefresh_IsAwaitedBelowTheTabEarlyReturn()
    {
        var body = StrippedTickBody(RefreshTick);
        var split = EarlyReturnOffset(body);
        var offsets = CallOffsets(body, VisibleRefresh);

        Assert.True(
            offsets.Count > 0,
            $"{RefreshTick} no longer calls {VisibleRefresh}, so this pin's ordering has nothing left to "
            + "hold — the tab-conditional half of the tick has moved or gone");

        Assert.True(
            offsets.All(o => o > split),
            $"{VisibleRefresh} is called at offset(s) {string.Join(",", offsets)} in {RefreshTick}, at or "
            + $"above its first return; ({split}) — so it runs on the tabs that return exists to skip and "
            + "the Overview grid is refreshed twice a cycle by two timers at one interval");
    }

    /// <summary>
    /// The control, in CI rather than in a commit message: the position check has to actually REPORT a
    /// fan-out relocated below the early-return, and it has to be the walker that lets it. A tally cannot
    /// control a relocation, because a relocation leaves every tally where it was — so the control is the
    /// direction of the comparison, run twice over one fixture.
    ///
    /// <para>The fixture's comment names its calls in call shape, which is one keystroke from what the
    /// shipped comments already say. Over raw text those mentions supply an above-the-return hit for every
    /// relocated call and the check reports nothing; over the walker's output they are blanked and it
    /// reports all three. A failure of the second assertion is a statement about this fixture — it has
    /// stopped demonstrating the miss — and not about <c>MainWindow</c>.</para>
    /// </summary>
    [Fact]
    public void TheAboveTheReturnCheck_SeesARelocatedFanOut_OnlyThroughTheWalker()
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(RelocatedFanOutFixture);
        var seen = FanOutNotAbove(stripped, EarlyReturnOffset(stripped));

        Assert.True(
            seen.Count == s_namedFanOut.Length,
            "the position check does not report a fan-out relocated below the early-return, so "
            + $"{nameof(TheUnconditionalFanOut_IsFiredAboveTheTabEarlyReturn)} cannot fail in the one "
            + $"direction #2923 exists for: [{string.Join("; ", seen)}]");

        var missed = FanOutNotAbove(RelocatedFanOutFixture, EarlyReturnOffset(RelocatedFanOutFixture));

        Assert.True(
            missed.Count == 0,
            "the raw-text half of this control no longer misses the relocation, so the fixture has stopped "
            + $"showing what the walker is for here: [{string.Join("; ", missed)}]");
    }

    /// <summary>
    /// <c>OnRefreshTimerTick</c>'s body as the #2923 mutation leaves it — fan-out below the early-return —
    /// carrying comment prose above that return which names every relocated call in call shape.
    /// </summary>
    private const string RelocatedFanOutFixture = """
        {
            /* Fires the freshness fan-out every cycle regardless of the visible tab: _ = PollAlertsAsync();
               and _ = RefreshServerStatusAsync(); and _ = RefreshStoreSizeAsync(); — prose that says the
               fan-out is up here while the code doing it is not. */
            if (ReferenceEquals(MainTabs.SelectedItem, OverviewTab))
            {
                return;
            }

            _ = RefreshServerStatusAsync();
            _ = RefreshStoreSizeAsync();
            _ = PollAlertsAsync();

            await RefreshVisibleAsync();
        }
        """;

    // ── The position check, shared by the pin and its control ─────────────────────────────

    /// <summary>
    /// Every fire-and-forget target in a tick body with no call above <paramref name="split"/>, reported
    /// with its offsets because those are the one thing a relocation cannot leave invariant. Empty is the
    /// passing state.
    ///
    /// <para>ANY call shape counts as "above", not only the fire-and-forget the name was collected by:
    /// this pin is about WHERE a call is, and whether it is awaited up there is the single-flight question
    /// <see cref="ViewerFleetTimerGuardTests"/> already owns.</para>
    /// </summary>
    private static List<string> FanOutNotAbove(string body, int split)
    {
        var offenders = new List<string>();

        foreach (var name in FiredNames(body).OrderBy(n => n, StringComparer.Ordinal))
        {
            var offsets = CallOffsets(body, name);

            if (offsets.Any(o => o < split))
            {
                continue;
            }

            offenders.Add($"{name} called at offset(s) {string.Join(",", offsets)}, all below {split}");
        }

        return offenders;
    }

    /// <summary>The distinct names a body fires unawaited.</summary>
    private static HashSet<string> FiredNames(string body) =>
        s_fireAndForget.Matches(body)
            .Select(m => m.Groups[1].Value)
            .ToHashSet(StringComparer.Ordinal);

    /// <summary>Every offset at which <paramref name="name"/> is invoked, whatever syntax invoked it.</summary>
    private static List<int> CallOffsets(string body, string name) =>
        Regex.Matches(body, @"\b" + Regex.Escape(name) + @"\s*\(")
            .Select(m => m.Index)
            .ToList();

    /// <summary>
    /// Offset of the tick's first bail-out <c>return;</c> — the boundary between what runs regardless of
    /// the visible tab and what does not. Fails rather than returning a sentinel: with no return there is
    /// no position left to pin, and a check that silently degrades to "no boundary, nothing below it" is
    /// the pass-for-the-wrong-reason this pin exists to remove.
    /// </summary>
    private static int EarlyReturnOffset(string body)
    {
        var match = s_earlyReturn.Match(body);

        Assert.True(
            match.Success,
            $"{RefreshTick} has no bail-out return;, so there is no boundary for the fan-out to sit above — "
            + "and ViewerFleetTimerGuardTests' region split is stale in the same breath");

        return match.Index;
    }

    // ── Source access ─────────────────────────────────────────────────────────────────────

    /// <summary>
    /// One named method's brace-balanced body, cut from the viewer shell AFTER
    /// <see cref="CSharpSourceWalker"/> has blanked its comments and literals — so a brace in prose cannot
    /// unbalance the cut and a mention in prose cannot be read as a call. The walk preserves length, so an
    /// offset into this body is the same offset the raw file has.
    /// </summary>
    private static string StrippedTickBody(string name)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(ShellPath()));
        var signature = Regex.Match(code, @"\bvoid\s+" + Regex.Escape(name) + @"\s*\(");

        Assert.True(signature.Success, $"{name} is no longer declared in MainWindow.xaml.cs");

        /* Past the parameter list before looking for the body brace: a parameter default can carry one. */
        var i = signature.Index + signature.Length - 1;
        var depth = 0;

        for (; i < code.Length; i++)
        {
            if (code[i] == '(')
            {
                depth++;
            }
            else if (code[i] == ')' && --depth == 0)
            {
                break;
            }
        }

        var open = code.IndexOf('{', i);

        Assert.True(open >= 0, $"{name} has no body in MainWindow.xaml.cs");

        return BraceBalanced(code, open);
    }

    /// <summary>The brace-balanced block starting at <paramref name="open"/>, that brace included.</summary>
    private static string BraceBalanced(string code, int open)
    {
        var depth = 0;

        for (var i = open; i < code.Length; i++)
        {
            if (code[i] == '{')
            {
                depth++;
            }
            else if (code[i] == '}' && --depth == 0)
            {
                return code[open..(i + 1)];
            }
        }

        return code[open..];
    }

    private static string ShellPath()
    {
        var path = Path.Combine(
            RepoRoot(),
            "Darling",
            "PerformanceMonitor.Darling.Viewer",
            "MainWindow.xaml.cs");

        Assert.True(File.Exists(path), $"the viewer shell is not where this pin looks: {path}");

        return path;
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;

        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);

        return dir!;
    }
}
