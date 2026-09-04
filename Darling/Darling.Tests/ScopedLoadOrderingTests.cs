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
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A load that paints a surface shared across scopes must not paint when a newer load for that same
/// surface has started (#2933). Two halves: the mechanism, tested behaviourally, and its placement at every
/// consuming site, which is lexical and can only be read out of the source.
///
/// <para><b>Why a generation and not #2924's verify-then-drop.</b> <see cref="ViewerScopedPaintGuardTests"/>
/// pins three sites that re-read the UI scope after the store read and drop the paint when it moved. That is
/// a drop rather than a silent loss only because something is certain to repaint — in the Darling viewer the
/// coalescing replay in <c>RefreshVisibleAsync</c> / <c>RefreshActiveInnerTabAsync</c> /
/// <c>RefreshActiveSubTabAsync</c>, which re-reads the scope at the re-entered load's own entry. <b>Lite has
/// no replay anywhere</b>: <c>ServerTab._isRefreshing</c> and <c>RecommendationsTab._isBusy</c> are bail-only
/// and <c>FinOpsTab</c> has no load guard at all, so the same drop there suppresses a paint nothing will redo.
/// A generation cannot do that: the only condition under which it drops is that a newer load for the same
/// surface has already started, so the surface always has a live writer.</para>
///
/// <para><b>And it orders same-scope reads</b>, which a scope verify cannot. Server A to B and back to A
/// leaves the third read's scope equal to the first's, so an answer still in flight for the first compares
/// current and lands last. That was raised in review on #2929 and declined there as pre-existing; the
/// generation closes it, in both <c>DatabaseStateOverridesWindow</c> twins, which is why those two sites
/// appear in this table as well as in that one. The two tests are not redundant — a load can be the newest
/// and still answer for a departed scope (nothing newer started), and a load can carry a matching scope and
/// still be superseded.</para>
///
/// <para><b>The idiom is Lite's own.</b> <c>ServerTab.Pickers.cs</c> already solves exactly this at three
/// sites with a plain <c>int</c> generation per chart (<c>_waitStatsPickerGen</c>,
/// <c>_memoryClerksPickerGen</c>, <c>_perfmonPickerGen</c>), comment and all. Those three stay as they are
/// and are asserted still present below: they are the precedent, and deleting one would silently un-fix a
/// site this pin's argument leans on. What is new is the key. A bare <c>int</c> field can be claimed by one
/// loader and checked by another with nothing to complain, and <c>FinOpsTab</c> owns fifteen surfaces, so the
/// key is <c>nameof</c> the loader and the ledger is one object.</para>
///
/// <para><b>Why one counter per surface and not one per control.</b> The safety argument is "a newer load for
/// THIS surface is running". One counter shared across <c>FinOpsTab</c>'s grids would let a single-grid
/// Refresh discard the other fourteen paints of a whole-tab <c>LoadPerServerDataAsync</c> pass, which is the
/// silent loss this mechanism exists to avoid, reached from the other direction.</para>
///
/// <para><b>What is deliberately NOT in this table.</b> <c>ServerTab</c>'s twenty-one main-tab
/// <c>Refresh*Async</c> loaders. Their races route through the bail-only <c>_isRefreshing</c>, where the
/// user-visible half is the DROPPED trigger — a time-range change mid-pass leaves the charts on the old
/// window while the combo shows the new one — and no drop-based mechanism can fix a load that never started.
/// That wants the viewer's replay, which needs <c>_isRefreshing</c> separated from the event-suppression duty
/// it also serves in <c>TimeDisplayMode_SelectionChanged</c>, <c>ServerTab.DrillDown.cs</c> and
/// <c>ServerTab.Grids.cs</c>, and that is a load-architecture change rather than a guard.</para>
/// </summary>
public sealed class ScopedLoadOrderingTests
{
    // ══ The mechanism ═════════════════════════════════════════════════════════════════════════
    // Behavioural, not source-walked: ScopedLoadGenerations is ordinary logic with no WPF in it, so
    // the property worth pinning — which claim survives — is reachable directly.

    /// <summary>Generations rise, so "newer" is decidable at all.</summary>
    [Fact]
    public void Claim_HandsOutARisingGenerationForASurface()
    {
        var loads = new ScopedLoadGenerations();

        Assert.Equal(1, loads.Claim("grid"));
        Assert.Equal(2, loads.Claim("grid"));
        Assert.Equal(3, loads.Claim("grid"));
    }

    /// <summary>
    /// Only the newest claim paints. This is the whole mechanism: three overlapping loads of one grid, and
    /// whichever COMPLETES first, exactly one of them is allowed to paint.
    /// </summary>
    [Fact]
    public void TheNewestClaim_IsTheOnlyOneNotSuperseded()
    {
        var loads = new ScopedLoadGenerations();

        var first = loads.Claim("grid");
        var second = loads.Claim("grid");
        var third = loads.Claim("grid");

        Assert.True(loads.Superseded("grid", first));
        Assert.True(loads.Superseded("grid", second));
        Assert.False(loads.Superseded("grid", third));
    }

    /// <summary>
    /// The A to B and back to A case, which is why this is a generation and not a scope comparison. The
    /// first and third loads here carry the SAME scope — the ledger never sees a scope, so the only thing
    /// separating them is identity, and it does.
    /// </summary>
    [Fact]
    public void AnEarlierClaim_IsSupersededEvenWhenTheScopeReturnedToItsValue()
    {
        var loads = new ScopedLoadGenerations();

        var onServerAlpha = loads.Claim("grid");
        loads.Claim("grid");                       // moved to beta
        var backOnServerAlpha = loads.Claim("grid");

        /* Both of these read server alpha. A verify of "is alpha still selected" passes for BOTH, so the
           first would paint if it landed last. */
        Assert.True(loads.Superseded("grid", onServerAlpha));
        Assert.False(loads.Superseded("grid", backOnServerAlpha));
    }

    /// <summary>
    /// Surfaces are independent, so a single-grid Refresh cannot discard a whole-tab load's other paints.
    /// </summary>
    [Fact]
    public void AClaimOnOneSurface_DoesNotSupersedeAnother()
    {
        var loads = new ScopedLoadGenerations();

        var recommendations = loads.Claim("recommendations");
        var utilization = loads.Claim("utilization");

        loads.Claim("utilization");

        Assert.False(loads.Superseded("recommendations", recommendations));
        Assert.True(loads.Superseded("utilization", utilization));
    }

    /// <summary>
    /// A surface that was never claimed reads as superseded, not as current. A check spelled differently
    /// from its claim is the one way this mechanism can be got wrong, and the safe answer to it is to drop
    /// rather than to pass while guarding nothing.
    /// </summary>
    [Fact]
    public void AnUnclaimedSurface_ReadsAsSuperseded()
    {
        var loads = new ScopedLoadGenerations();

        loads.Claim("LoadRecommendationsAsync");

        /* One transposed character in the key, which is exactly what a hand-written claim/check pair
           can drift into, and what nameof at both ends makes impossible. */
        Assert.True(loads.Superseded("LoadRecommendationsaAsync", 1));

        /* And the right key with the wrong generation, so this is not passing only on the typo. */
        Assert.True(loads.Superseded("LoadRecommendationsAsync", 2));
        Assert.False(loads.Superseded("LoadRecommendationsAsync", 1));
    }

    /// <summary>An empty or null key is a caller bug, not a surface.</summary>
    [Fact]
    public void AnEmptyKey_Throws()
    {
        var loads = new ScopedLoadGenerations();

        Assert.Throws<ArgumentException>(() => loads.Claim(""));
        Assert.Throws<ArgumentNullException>(() => loads.Claim(null!));
        Assert.Throws<ArgumentException>(() => loads.Superseded("", 1));
    }

    // ══ The placement ═════════════════════════════════════════════════════════════════════════

    /// <summary>One loader that paints a surface shared across scopes.</summary>
    /// <param name="Dir">Repo-relative directory segments holding the source.</param>
    /// <param name="File">Source file declaring the loader.</param>
    /// <param name="ReturnType">Return-type token the declaration must still carry, so a signature change
    /// surfaces as "no longer declared" rather than as an empty body that satisfies everything.</param>
    /// <param name="Method">The loader, which is also its own ledger key.</param>
    /// <param name="ChromeAllowed">Paints that may sit outside the guard because they are chrome the load
    /// must release whoever won — a button re-enabled in a <c>finally</c>.</param>
    private sealed record Site(
        string[] Dir,
        string File,
        string ReturnType,
        string Method,
        string[] ChromeAllowed = null!);

    private static readonly string[] s_finOps = ["Lite", "Controls"];
    private static readonly string[] s_liteWindows = ["Lite", "Windows"];
    private static readonly string[] s_viewer = ["Darling", "PerformanceMonitor.Darling.Viewer"];

    private static Site FinOps(string method, params string[] chrome) =>
        new(s_finOps, "FinOpsTab.xaml.cs", "Task", method, chrome);

    private static readonly Site[] s_sites =
    [
        /* FinOpsTab: no load guard at all, fifteen grids, and every one of them shared across servers. */
        FinOps("LoadRecommendationsAsync"),
        FinOps("LoadUtilizationAsync"),
        FinOps("LoadDatabaseResourcesAsync"),
        FinOps("LoadApplicationConnectionsAsync"),
        FinOps("LoadDatabaseSizesAsync"),
        FinOps("LoadPvsStatsAsync"),
        FinOps("LoadServerInventoryAsync"),
        FinOps("LoadStorageGrowthAsync"),
        FinOps("LoadIdleDatabasesAsync"),
        FinOps("LoadTempdbSummaryAsync"),
        FinOps("LoadHighImpactQueriesAsync"),
        FinOps("LoadWaitCategorySummaryAsync"),
        FinOps("LoadExpensiveQueriesAsync"),
        FinOps("LoadMemoryGrantEfficiencyAsync"),
        new Site(s_finOps, "FinOpsTab.xaml.cs", "void", "RunIndexAnalysis_Click", ["RunIndexAnalysisButton.IsEnabled"]),

        new Site(s_finOps, "FinOpsTab.Locking.cs", "Task", "PopulateLockingDbSelectorAsync"),
        new Site(s_finOps, "FinOpsTab.Locking.cs", "Task", "LoadIndexLockingGridAsync"),
        new Site(s_finOps, "FinOpsTab.ObjectHeatmap.cs", "Task", "LoadObjectGrowthAsync"),
        new Site(s_finOps, "FinOpsTab.ObjectHeatmap.cs", "Task", "LoadObjectIndexDetailAsync"),

        /* ServerTab's Compare dropdown sets no in-flight flag, and its grids are on screen while two
           changes overlap, so no visibility change comes along to repaint them. */
        new Site(s_finOps, "ServerTab.Comparison.cs", "Task", "RefreshQueryStatsComparisonAsync"),
        new Site(s_finOps, "ServerTab.Comparison.cs", "Task", "RefreshProcStatsComparisonAsync"),
        new Site(s_finOps, "ServerTab.Comparison.cs", "Task", "RefreshQueryStoreComparisonAsync"),

        /* The two fleet-wide history tabs: no load guard, and their server/hours combos sit directly above
           the grid they scope. */
        new Site(s_finOps, "AlertsHistoryTab.xaml.cs", "Task", "LoadAlertsAsync"),
        new Site(s_finOps, "JobHistoryTab.xaml.cs", "Task", "LoadJobsAsync"),
        new Site(s_finOps, "JobHistoryTab.xaml.cs", "Task", "UpdateAgentStatusAsync"),

        /* #2929's second and third sites, now ordered as well as scope-verified. Both twins, because the
           same-scope hole is identical in them and fixing one half is how a parity gap starts. */
        new Site(s_liteWindows, "DatabaseStateOverridesWindow.xaml.cs", "Task", "LoadAsync"),
        new Site(s_viewer, "DatabaseStateOverridesWindow.xaml.cs", "Task", "LoadAsync"),
    ];

    /// <summary>
    /// Every site claims a generation for ITS OWN name, exactly once, before it awaits anything. Once,
    /// because a second claim inside one load supersedes the load making it and it can then never paint;
    /// before the await, because a claim below the read leaves the read unordered.
    /// </summary>
    [Fact]
    public void EverySite_ClaimsItsOwnKeyOnceBeforeItsFirstAwait()
    {
        AssertEverySiteIsStillHere();

        foreach (var site in s_sites)
        {
            var body = LoadBody(site);
            var claims = Matches(body, Claim(site)).ToList();

            Assert.True(
                claims.Count == 1,
                $"{site.File}:{site.Method} claims a generation {claims.Count} time(s); exactly one, keyed "
                + $"nameof({site.Method}), is the contract");

            Assert.True(
                claims[0] < FirstAwait(site, body),
                $"{site.File}:{site.Method} claims its generation BELOW its first await, which leaves the "
                + "read it is meant to order already in flight");
        }
    }

    /// <summary>
    /// Every site checks its own key after the read, and every check LEAVES without painting. A check that
    /// notices the supersession and falls through is the defect with a diagnostic attached.
    /// </summary>
    [Fact]
    public void EveryChecksConsequence_IsAReturnThatPaintsNothing()
    {
        AssertEverySiteIsStillHere();

        foreach (var site in s_sites)
        {
            var body = LoadBody(site);
            var checks = Checks(site, body);

            Assert.True(
                checks.Count >= 1,
                $"{site.File}:{site.Method} never checks whether it was superseded");

            foreach (var check in checks)
            {
                var tail = body[(check.Index + check.Length)..];

                Assert.Matches(new Regex(@"^\s*(\{\s*return\s*;\s*\}|return\s*;)"), tail);
            }
        }
    }

    /// <summary>
    /// Every paint below the read is protected by a supersession check — one that sits above it with NO
    /// <c>await</c> in between. Offsets rather than counts, because the mutation this exists for MOVES a
    /// check below a paint and a move leaves every count invariant.
    ///
    /// <para><b>The await clause is the half that was missing</b>, and review on this change caught it.
    /// "Some check above it" is satisfied by four paints that each follow their own <c>await</c> and share
    /// one check made after the FIRST of them — which is exactly what <c>LoadUtilizationAsync</c> looked
    /// like, four <c>ItemsSource = await ...</c> assignments where the assignment IS the await statement, so
    /// three of the four painted before the only check ran. A paint with an <c>await</c> between it and its
    /// nearest check is a paint nothing checked.</para>
    /// </summary>
    [Fact]
    public void EveryPaintAfterTheRead_SitsBelowASupersessionCheck()
    {
        AssertEverySiteIsStillHere();

        foreach (var site in s_sites)
        {
            var body = LoadBody(site);
            var firstAwait = FirstAwait(site, body);
            var checks = Checks(site, body).Select(m => m.Index).ToList();

            Assert.True(
                checks.Count >= 1,
                $"{site.File}:{site.Method} never checks whether it was superseded");

            var paints = Paints(site, body);

            /* Positive control for the negative below, through the identical form: the paint sweep finds
               paints in this method, so an empty offender list is a fact about the guard and not about the
               regex having matched nothing. */
            Assert.True(
                paints.Count >= 1,
                $"no paint found in {site.File}:{site.Method}, so every assertion about paints here would "
                + "hold vacuously — the paint sweep is not reading this method");

            var afterRead = paints.Where(p => p > firstAwait).ToList();

            Assert.True(
                afterRead.Count >= 1,
                $"no paint in {site.File}:{site.Method} sits after its read, so this pin has nothing to say "
                + "about it and the site does not belong in the table");

            var unguarded = Unguarded(body, checks, afterRead)
                .Select(p => Line(body, p))
                .ToList();

            Assert.True(
                unguarded.Count == 0,
                $"{site.File}:{site.Method} paints before asking whether a newer load for the same surface "
                + "has started, so the earlier of two overlapping reads can land last — at body line(s) "
                + string.Join(", ", unguarded));
        }
    }

    /// <summary>
    /// A <c>catch</c> that paints must make the check on its own side of the <c>catch</c>. A load's error
    /// path paints too, and a superseded load's failure must not overwrite the answer the newest load has
    /// already put on screen.
    /// </summary>
    [Fact]
    public void EveryPaintingCatch_ChecksOnItsOwnSideOfTheCatch()
    {
        AssertEverySiteIsStillHere();

        var painting = 0;

        foreach (var site in s_sites)
        {
            var body = LoadBody(site);

            foreach (var catchBlock in CatchBlocks(body))
            {
                var paints = Paints(site, catchBlock);

                if (paints.Count == 0)
                {
                    continue;
                }

                painting++;
                var checks = Checks(site, catchBlock).Select(m => m.Index).ToList();

                var unguarded = Unguarded(catchBlock, checks, paints);

                Assert.True(
                    unguarded.Count == 0,
                    $"{site.File}:{site.Method} paints on an error path whose supersession check is on the "
                    + "other side of the catch, so a superseded load's failure lands on the newest load's "
                    + $"answer — at catch-block line(s) {string.Join(", ", unguarded.Select(p => Line(catchBlock, p)))}");
            }
        }

        /* Positive control: if no catch in the table paints at all this assertion is vacuous, and the one
           that does (RunIndexAnalysis_Click's) is the reason it exists. */
        Assert.True(painting >= 1, "no catch in the table paints, so this assertion holds vacuously");
    }

    /// <summary>
    /// The claim and every check name the SAME surface, and that surface is the loader's own name. A key
    /// that drifts between the claim and the check makes the check ask about a surface nothing claimed, and
    /// <see cref="AnUnclaimedSurface_ReadsAsSuperseded"/> is what stops that failing open — but it fails
    /// CLOSED, which drops every paint at that site forever, so it still has to be caught here.
    /// </summary>
    [Fact]
    public void TheClaimAndTheChecks_NameTheLoaderItself()
    {
        AssertEverySiteIsStillHere();

        foreach (var site in s_sites)
        {
            var body = LoadBody(site);

            /* Positive control, identical Regex form on the identical needle in a haystack where it does
               belong: the ledger IS used in this body, so a zero count from the keyed patterns below is a
               fact about the KEY rather than about the ledger being absent. */
            Assert.Matches(new Regex(@"_loads\s*\.\s*(Claim|Superseded)\s*\("), body);

            var keyed = Matches(body, Claim(site)).Count() + Checks(site, body).Count;
            var any = Regex.Matches(body, @"_loads\s*\.\s*(?:Claim|Superseded)\s*\(").Count;

            Assert.True(
                keyed == any,
                $"{site.File}:{site.Method} makes {any} ledger call(s) but only {keyed} of them are keyed "
                + $"nameof({site.Method}); a key that differs between the claim and the check asks about a "
                + "surface nothing claimed");
        }
    }

    /// <summary>
    /// The three plain-<c>int</c> generation guards this mechanism generalises are still in place. They are
    /// the precedent the choice rests on, and each one is a live fix: deleting one restores the interleave
    /// its own comment describes, with nothing else in Lite to catch it.
    /// </summary>
    [Fact]
    public void ThePickerChartsKeepTheirOwnGenerations()
    {
        var body = Stripped(SourceFile(s_finOps, "ServerTab.Pickers.cs"));

        foreach (var field in new[] { "_waitStatsPickerGen", "_memoryClerksPickerGen", "_perfmonPickerGen" })
        {
            Assert.Matches(new Regex(@"var\s+gen\s*=\s*\+\+" + Regex.Escape(field) + @"\s*;"), body);
            Assert.Matches(new Regex(@"if\s*\(\s*gen\s*!=\s*" + Regex.Escape(field) + @"\s*\)\s*return\s*;"), body);
        }
    }

    // ── Site mechanics ────────────────────────────────────────────────────────────────────────

    private static void AssertEverySiteIsStillHere() =>
        Assert.True(s_sites.Length == 27, $"{s_sites.Length} site(s) in the table; this pin covers 27");

    private static Regex Claim(Site site) =>
        new(@"_loads\s*\.\s*Claim\s*\(\s*nameof\s*\(\s*" + Regex.Escape(site.Method) + @"\s*\)\s*\)");

    private static List<Match> Checks(Site site, string body) =>
        Regex.Matches(
                body,
                @"if\s*\(\s*_loads\s*\.\s*Superseded\s*\(\s*nameof\s*\(\s*" + Regex.Escape(site.Method)
                + @"\s*\)\s*,\s*gen\s*\)\s*\)")
            .OrderBy(m => m.Index)
            .ToList();

    private static IEnumerable<int> Matches(string body, Regex pattern) =>
        pattern.Matches(body).Select(m => m.Index).OrderBy(i => i);

    private static int FirstAwait(Site site, string body)
    {
        var i = body.IndexOf("await", StringComparison.Ordinal);

        Assert.True(i >= 0, $"{site.File}:{site.Method} no longer awaits anything — this pin is stale");

        return i;
    }

    /// <summary>
    /// Offsets of the paints: an assignment into a XAML-generated control's UI property, or a call to one of
    /// the two helpers that push a list into a grid. Derived from the shape rather than from a per-site list
    /// of surface names, so a paint added to a guarded method is covered without a second edit here.
    /// <paramref name="site"/>'s chrome allowance is subtracted — a button re-enabled in a <c>finally</c> has
    /// to fire whoever won the race.
    /// </summary>
    private static List<int> Paints(Site site, string body)
    {
        var chrome = site.ChromeAllowed ?? [];

        return Regex.Matches(
                body,
                @"\b[A-Z]\w*\s*\.\s*(?:ItemsSource|Text|Visibility|Content|IsEnabled|SelectedIndex|SelectedItem)"
                + @"\s*=(?!=)|\.\s*UpdateData\s*\(|\bApplyViewModel\s*\(")
            .Where(m => !chrome.Any(c => m.Value.Replace(" ", "", StringComparison.Ordinal)
                .StartsWith(c, StringComparison.Ordinal)))
            .Select(m => m.Index)
            .OrderBy(i => i)
            .ToList();
    }

    /// <summary>Every <c>catch</c> block in the body, as brace-balanced spans.</summary>
    private static IEnumerable<string> CatchBlocks(string body)
    {
        foreach (var m in Regex.Matches(body, @"\bcatch\b[^{]*\{").Cast<Match>())
        {
            yield return CSharpSourceWalker.BraceBalanced(body, m.Index + m.Length - 1);
        }
    }

    /// <summary>
    /// The paints no check protects: a paint is protected when SOME check sits above it with no
    /// <c>await</c> between the two. The await clause is what makes this stronger than an ordering test —
    /// see <see cref="EveryPaintAfterTheRead_SitsBelowASupersessionCheck"/> for the shape it exists to
    /// catch.
    /// </summary>
    private static List<int> Unguarded(string body, List<int> checks, IEnumerable<int> paints) =>
        paints
            .Where(p => !checks.Any(c => c < p && !Regex.IsMatch(body[c..p], @"\bawait\b")))
            .ToList();

    private static int Line(string body, int index) => body[..index].Count(c => c == '\n') + 1;

    // ── Source access ─────────────────────────────────────────────────────────────────────────

    private static string LoadBody(Site site) =>
        Body(Stripped(SourceFile(site.Dir, site.File)), site.ReturnType, site.Method);

    /// <summary>
    /// The block body of the named method, anchored on the declared return type so a signature change
    /// surfaces as "no longer declared" rather than as an empty body that satisfies every assertion. Brace
    /// matching is <see cref="CSharpSourceWalker.BraceBalanced"/>'s and the input is always
    /// <see cref="Stripped"/>, so a brace in a literal or a comment is blanked before it is counted.
    /// </summary>
    private static string Body(string stripped, string declaredType, string name)
    {
        var m = new Regex(Regex.Escape(declaredType) + @"\s+" + Regex.Escape(name) + @"\s*\(").Match(stripped);

        Assert.True(m.Success, $"`{declaredType} {name}(` is no longer declared where this pin looks for it");

        var close = stripped.IndexOf(')', m.Index + m.Length - 1);

        Assert.True(close >= 0, $"{name}'s parameter list does not close");

        var open = stripped.IndexOf('{', close);

        Assert.True(open >= 0, $"{name} has no block body");

        return CSharpSourceWalker.BraceBalanced(stripped, open);
    }

    private static readonly Dictionary<string, string> s_stripped = new(StringComparer.Ordinal);

    private static string Stripped(string path) =>
        s_stripped.TryGetValue(path, out var cached)
            ? cached
            : s_stripped[path] = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path));

    private static string SourceFile(string[] dir, string name)
    {
        var path = Path.Combine([RepoRoot(), .. dir, name]);

        Assert.True(File.Exists(path), $"source not found: {path}");

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
