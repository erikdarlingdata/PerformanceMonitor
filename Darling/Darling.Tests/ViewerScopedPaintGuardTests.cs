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
/// A viewer surface that carries EVERY scope's answer must not paint an answer read for a scope the
/// operator has since left (#2924). The scope is captured before the store read and re-verified after it,
/// and the verify sits above every paint.
///
/// <para><b>The sites, and why they are the sites.</b> A sweep of all 190 Darling viewer sources for UI
/// state captured before an <c>await</c> and applied after it produced 45 other candidates, and every one
/// of them fails the test on a different clause: it is a one-shot action on a clicked target (where acting
/// on what the user clicked is the whole point), or it reads no mutable scope at all, or it paints into a
/// surface belonging exclusively to the captured scope, or its scope-changing control re-triggers the load
/// through a replay that re-reads the scope — <c>RefreshVisibleAsync</c>,
/// <c>ViewerServerTab.RefreshActiveInnerTabAsync</c> and <c>FinOpsTab.RefreshActiveSubTabAsync</c> all
/// have that shape and between them cover the per-server tabs, FinOps and every aggregate tab. What is
/// left is a shared surface whose load has no replay: the status bar's collector-health field and the
/// database-state override window's grid.</para>
///
/// <para><b>The third site is Lite's twin of the second</b>, and it is in this table rather than in a
/// parity pin because the two files' <c>LoadAsync</c> was line-for-line identical — the same
/// <c>SelectedServerId</c>, the same unguarded <c>ServerCombo_SelectionChanged</c>, the same
/// <c>_rows</c> model a later Save writes from. Splitting one invariant across two pins is how the thing
/// this pin guards against happens: #2913 exists because five copies of one walk had already drifted.</para>
///
/// <para><b>Why a DROP here and a REPLAY one method up.</b> <see cref="ViewerFleetTimerGuardTests"/> pins
/// the opposite choice for <c>RefreshServerStatusAsync</c>, which must offer <c>replayIfBusy</c> because a
/// freshness read that started before an add/edit/remove cannot answer for the new server and nothing else
/// is going to ask. At every site here something else always asks: the control that changes the scope
/// starts a correctly-scoped load of its own, so a mismatching answer is stale by definition and its
/// replacement is already in flight. Replaying would issue a third read for a scope two calls are already
/// answering. That is why <see cref="TheScopeChangingControl_StillIssuesItsOwnReRead"/> is load-bearing
/// rather than decorative: delete that call and the drop stops being a drop and becomes a silent loss.</para>
///
/// <para><b>Why this is a SOURCE pin and not a behavioural test.</b> The property is lexical — a verify
/// between the read and every paint — and once it holds there is nothing behavioural left: the decision is
/// <c>captured != current</c>, and a test of that is a test of <c>!=</c>. The input worth varying is
/// unreachable rather than untested, because both scopes come from live WPF controls
/// (<c>MainTabs.SelectedItem</c> needs a <c>ViewerServerTab</c> in a visual tree) and neither
/// <c>MainWindow</c> nor a <c>Window</c> subclass has a headless seam to instantiate.
/// <see cref="ViewerFleetTimerGuardTests"/> reached the same conclusion about the same file for the same
/// reason.</para>
///
/// <para><b>Why the ordering assertion compares OFFSETS.</b> The mutation it exists for MOVES the verify
/// below a paint rather than deleting it, and a move leaves the first site's verify count at one, its paint
/// count at ten and its await count at two — every count a name-and-tally control could check is invariant
/// across it, which is why one was not used.</para>
/// </summary>
public sealed class ViewerScopedPaintGuardTests
{
    /// <summary>
    /// One shared-surface load whose scope can change under it.
    /// </summary>
    /// <param name="Dir">Repo-relative directory segments holding the source.</param>
    /// <param name="File">Source file declaring the load.</param>
    /// <param name="ReturnType">Return-type token the load's declaration must still carry, so a signature
    /// change surfaces as "no longer declared" rather than as an empty body that satisfies everything.</param>
    /// <param name="Method">The load.</param>
    /// <param name="ScopeReader">The expression that answers "which scope is on screen", written exactly as
    /// the source writes it — the capture and the verify must both be this, or they compare two things and
    /// mean neither.</param>
    /// <param name="ScopeReaderType">Declared type of <paramref name="ScopeReader"/>.</param>
    /// <param name="RawRead">The raw control read that must live ONLY inside the scope reader.</param>
    /// <param name="Captured">The local the capture went into.</param>
    /// <param name="PaintTargets">The surfaces shared across scopes. A field counts: it is what a later
    /// save reads from.</param>
    /// <param name="HandlerFile">Source file declaring the change handler.</param>
    /// <param name="Handler">The scope-changing control's handler, which must still start its own load —
    /// the re-read that makes dropping correct rather than lossy.</param>
    private sealed record Site(
        string[] Dir,
        string File,
        string ReturnType,
        string Method,
        string ScopeReader,
        string ScopeReaderType,
        string RawRead,
        string Captured,
        string[] PaintTargets,
        string HandlerFile,
        string Handler);

    private static readonly Site[] s_sites =
    [
        new Site(
            ["Darling", "PerformanceMonitor.Darling.Viewer"],
            "MainWindow.ServerManagement.cs",
            "Task",
            "UpdateCollectorHealthTextAsync",
            "SelectedTabCollectorScope()",
            "int?",
            "MainTabs.SelectedItem",
            "serverId",
            ["CollectorHealthText"],
            "MainWindow.xaml.cs",
            "MainTabs_SelectionChanged"),
        new Site(
            ["Darling", "PerformanceMonitor.Darling.Viewer"],
            "DatabaseStateOverridesWindow.xaml.cs",
            "Task",
            "LoadAsync",
            "SelectedServerId",
            "int?",
            "ServerCombo.SelectedItem",
            "serverId",
            ["StatesGrid", "StatusText", "ExpectedColumn", "_rows"],
            "DatabaseStateOverridesWindow.xaml.cs",
            "ServerCombo_SelectionChanged"),
        new Site(
            ["Lite", "Windows"],
            "DatabaseStateOverridesWindow.xaml.cs",
            "Task",
            "LoadAsync",
            "SelectedServerId",
            "int?",
            "ServerCombo.SelectedItem",
            "serverId",
            ["StatesGrid", "StatusText", "ExpectedColumn", "_rows"],
            "DatabaseStateOverridesWindow.xaml.cs",
            "ServerCombo_SelectionChanged"),
    ];

    /// <summary>
    /// The scope must be read through the site's one expression and not spelled out in the load, because a
    /// verify written in different terms from the capture compares two things and means neither.
    /// </summary>
    [Fact]
    public void TheScope_IsReadThroughOneExpressionAndNotInlined()
    {
        AssertEverySiteIsStillHere();

        foreach (var site in s_sites)
        {
            var body = LoadBody(site);
            var reader = ScopeReaderBody(site);

            /* Positive control for the negative below, through the identical Contains form on the identical
               needle: the raw read is findable where it belongs, so its absence from the load is a fact
               about the load and not about the needle. */
            Assert.Contains(site.RawRead, reader, StringComparison.Ordinal);

            Assert.DoesNotContain(site.RawRead, body, StringComparison.Ordinal);

            var uses = Regex.Matches(body, ReaderPattern(site)).Count;

            Assert.True(
                uses >= 2,
                $"{site.Method} uses {site.ScopeReader} {uses} time(s); the capture and the post-read "
                + "verify are two, so fewer means one of them is gone or is spelled differently");
        }
    }

    /// <summary>
    /// The verify must compare a FRESH read against the CAPTURED scope, and leave on inequality. An
    /// inverted comparison drops exactly the answers it should paint and paints exactly the ones it should
    /// drop, which is worse than having no verify at all.
    /// </summary>
    [Fact]
    public void TheVerify_ComparesAFreshReadAgainstTheCapturedScope()
    {
        AssertEverySiteIsStillHere();

        foreach (var site in s_sites)
        {
            Assert.Matches(
                new Regex(@"if\s*\(\s*" + ReaderPattern(site) + @"\s*!=\s*" + Regex.Escape(site.Captured) + @"\s*\)"),
                LoadBody(site));
        }
    }

    /// <summary>
    /// Every verify's consequence must LEAVE the load without painting. A verify that notices the mismatch,
    /// logs it and falls through is the defect with a diagnostic attached; one that clears the surface is
    /// still answering for a scope that is no longer on screen.
    /// </summary>
    [Fact]
    public void EveryVerifysConsequence_IsAReturnThatPaintsNothing()
    {
        AssertEverySiteIsStillHere();

        foreach (var site in s_sites)
        {
            var body = LoadBody(site);
            var verifies = Verifies(site, body);

            Assert.True(verifies.Count >= 1, $"{site.Method} has no scope verify after its store read");

            foreach (var verify in verifies)
            {
                var open = body.IndexOf('{', verify);

                Assert.True(open >= 0, $"a scope verify in {site.Method} has no block body");

                var consequence = CSharpSourceWalker.BraceBalanced(body, open);

                Assert.Matches(new Regex(@"\breturn\s*;"), consequence);

                foreach (var target in site.PaintTargets)
                {
                    Assert.DoesNotContain(target, consequence, StringComparison.Ordinal);
                }
            }
        }
    }

    /// <summary>
    /// Every paint that follows the store read must sit BELOW a scope verify, on the SAME side of any
    /// <c>catch</c> as the verify it relies on.
    ///
    /// <para>The <c>catch</c> clause is the second half of the rule and it is not decoration. A load's
    /// error path paints too — it blanks the grid and writes the failure into the status line — and a
    /// failure reading a departed scope must not blank the surface the operator is now looking at. A paint
    /// whose nearest preceding verify is on the other side of a <c>catch</c> is a paint the verify was left
    /// behind by.</para>
    /// </summary>
    [Fact]
    public void EveryPaintAfterTheRead_SitsBelowAVerifyOnItsOwnPath()
    {
        AssertEverySiteIsStillHere();

        foreach (var site in s_sites)
        {
            var body = LoadBody(site);

            var firstAwait = body.IndexOf("await", StringComparison.Ordinal);
            Assert.True(firstAwait >= 0, $"{site.Method} no longer awaits anything — this pin is stale");

            var verifies = Verifies(site, body);
            Assert.True(verifies.Count >= 1, $"{site.Method} has no scope verify after its store read");

            var paints = Paints(site, body);

            Assert.True(
                paints.Count >= 3,
                $"only {paints.Count} paint(s) into {string.Join("/", site.PaintTargets)} found in "
                + $"{site.Method}; the load paints across a success, an empty and an error branch, so a low "
                + "count means the sweep is not reading the method");

            var afterRead = paints.Where(i => i > firstAwait).ToList();

            Assert.True(
                afterRead.Count >= 1,
                $"no paint in {site.Method} sits after the store read, so this assertion would hold vacuously");

            var unverified = afterRead
                .Where(p => !verifies.Any(v => v < p))
                .Select(p => Line(body, p))
                .ToList();

            Assert.True(
                unverified.Count == 0,
                $"{site.Method} paints before the captured scope is re-verified, so an answer read for a "
                + "scope the operator has since left still lands on screen — at body line(s) "
                + string.Join(", ", unverified));

            var acrossACatch = afterRead
                .Where(p => Regex.IsMatch(body[verifies.Last(v => v < p)..p], @"\bcatch\b"))
                .Select(p => Line(body, p))
                .ToList();

            Assert.True(
                acrossACatch.Count == 0,
                $"{site.Method} paints on a path whose nearest scope verify is on the other side of a "
                + "catch, so the error path answers for a scope that may have left — at body line(s) "
                + string.Join(", ", acrossACatch));
        }
    }

    /// <summary>
    /// The drop is only a drop because the scope-changing control issues its own correctly-scoped load.
    /// Remove that and the same code becomes a silent loss: the operator changes scope, the in-flight read
    /// discards itself, and the surface keeps the previous scope's answer until something else refreshes
    /// it — the exact symptom the drop exists to remove, arrived at from the other direction.
    /// </summary>
    [Fact]
    public void TheScopeChangingControl_StillIssuesItsOwnReRead()
    {
        AssertEverySiteIsStillHere();

        foreach (var site in s_sites)
        {
            /* Positive control for the assertion below, same Contains form, same needle, in the file that
               declares the load — so a failure means the CALL is gone, not that the needle never matched. */
            Assert.Contains(site.Method, Stripped(SourceFile(site.Dir, site.File)), StringComparison.Ordinal);

            var handler = Body(Stripped(SourceFile(site.Dir, site.HandlerFile)), "void", site.Handler);

            Assert.Contains(site.Method, handler, StringComparison.Ordinal);
        }
    }

    // ── Site mechanics ────────────────────────────────────────────────────────────────────

    private static void AssertEverySiteIsStillHere() =>
        Assert.True(s_sites.Length == 3, $"{s_sites.Length} site(s) in the table; this pin covers three");

    /// <summary>The scope reader as a regex — a call's parens are matched loosely so formatting cannot
    /// break the match, but the NAME is exact.</summary>
    private static string ReaderPattern(Site site) =>
        site.ScopeReader.EndsWith("()", StringComparison.Ordinal)
            ? @"\b" + Regex.Escape(site.ScopeReader[..^2]) + @"\s*\(\s*\)"
            : @"\b" + Regex.Escape(site.ScopeReader) + @"\b";

    /// <summary>
    /// Offsets of the scope verifies — every <c>if</c> after the store read whose condition mentions the
    /// scope reader. After the read, because the CAPTURE is also an <c>if</c> at one of these sites
    /// (<c>if (SelectedServerId is not int serverId)</c>) and a capture is not a verify.
    /// </summary>
    private static List<int> Verifies(Site site, string body)
    {
        var firstAwait = body.IndexOf("await", StringComparison.Ordinal);

        return Regex.Matches(body, @"if\s*\([^{};]*" + ReaderPattern(site) + @"[^{};]*\)")
            .Select(m => m.Index)
            .Where(i => i > firstAwait)
            .OrderBy(i => i)
            .ToList();
    }

    /// <summary>
    /// Offsets of the paints — an assignment to a shared surface, whether written as
    /// <c>Surface.Property = </c> or as a bare <c>_field = </c>. <c>==</c> is excluded so a comparison is
    /// not read as a paint.
    /// </summary>
    private static List<int> Paints(Site site, string body) =>
        site.PaintTargets
            .SelectMany(t => Regex.Matches(body, @"\b" + Regex.Escape(t) + @"\s*(?:\.\s*\w+\s*)?=(?!=)")
                .Select(m => m.Index))
            .OrderBy(i => i)
            .ToList();

    private static int Line(string body, int index) => body[..index].Count(c => c == '\n') + 1;

    // ── Source access ─────────────────────────────────────────────────────────────────────

    private static string LoadBody(Site site) =>
        Body(Stripped(SourceFile(site.Dir, site.File)), site.ReturnType, site.Method);

    private static string ScopeReaderBody(Site site) =>
        Body(
            Stripped(SourceFile(site.Dir, site.File)),
            site.ScopeReaderType,
            site.ScopeReader.EndsWith("()", StringComparison.Ordinal) ? site.ScopeReader[..^2] : site.ScopeReader);

    /// <summary>
    /// The body of the named member, given the declared type its declaration must still carry — a block or
    /// an expression body, and a method or a property, since one site's scope reader is
    /// <c>=&gt; ...;</c> on a property and the other's is <c>=&gt; ...;</c> on a method. Pinning the type in
    /// the anchor means a signature change surfaces as "no longer declared" rather than as a silently empty
    /// body that satisfies every assertion above.
    /// </summary>
    private static string Body(string stripped, string declaredType, string name)
    {
        var declaration = new Regex(Regex.Escape(declaredType) + @"\s+" + Regex.Escape(name) + @"\b");
        var m = declaration.Match(stripped);

        Assert.True(m.Success, $"`{declaredType} {name}` is no longer declared where this pin looks for it");

        var i = SkipWhitespace(stripped, m.Index + m.Length);

        /* A method's parameter list sits between the name and the body; a property has none. */
        if (i < stripped.Length && stripped[i] == '(')
        {
            var close = stripped.IndexOf(')', i);

            Assert.True(close >= 0, $"{name}'s parameter list does not close");

            i = SkipWhitespace(stripped, close + 1);
        }

        Assert.True(i < stripped.Length, $"{name} has no body");

        if (stripped[i] == '{')
        {
            return CSharpSourceWalker.BraceBalanced(stripped, i);
        }

        Assert.True(
            stripped[i] == '=' && i + 1 < stripped.Length && stripped[i + 1] == '>',
            $"{name} has neither a block nor an expression body");

        var end = stripped.IndexOf(';', i);

        Assert.True(end >= 0, $"{name}'s expression body does not terminate");

        return stripped[(i + 2)..end];
    }

    private static int SkipWhitespace(string text, int i)
    {
        while (i < text.Length && char.IsWhiteSpace(text[i]))
        {
            i++;
        }

        return i;
    }

    private static readonly Dictionary<string, string> s_stripped = new(StringComparer.Ordinal);

    /// <summary>
    /// Every source this pin reads arrives through here, with comments and string literals blanked. Brace
    /// matching over the result is <see cref="CSharpSourceWalker.BraceBalanced"/>'s — this file keeps no walk
    /// of its own — and the blanking is the precondition that makes it correct: a brace in prose or in a
    /// literal is gone before the walk can count it.
    /// </summary>
    private static string Stripped(string path)
    {
        if (s_stripped.TryGetValue(path, out var cached))
        {
            return cached;
        }

        return s_stripped[path] = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path));
    }

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
