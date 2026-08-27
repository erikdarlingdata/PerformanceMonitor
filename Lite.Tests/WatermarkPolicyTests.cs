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
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #1556/#2102: the catch-up clamp boundaries. A stale query_store watermark must not point its cutoff
/// far into the past — the per-database query's cost grows with window width, so a wide one-shot window
/// either blows the commit limit (#1556, days-wide) or times out every cycle and wedges the database
/// permanently (#2102, hours-wide). The clamp floors a stale watermark to now-MaxCatchup; a fresh
/// watermark and a null watermark pass through untouched.
/// </summary>
public sealed class WatermarkPolicyTests
{
    private static readonly DateTime Now = new(2026, 7, 17, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void ClampCatchup_Null_StaysNull()
    {
        /* Null = nothing collected yet; the definition's documented first-run window applies, not a clamp. */
        Assert.Null(WatermarkPolicy.ClampCatchup(null, Now));
    }

    [Fact]
    public void ClampCatchup_WithinHorizon_ReturnedUnchanged()
    {
        /* A routine restart: the watermark is minutes old and never clamps. */
        var tenMinutesAgo = Now.AddMinutes(-10);
        Assert.Equal(tenMinutesAgo, WatermarkPolicy.ClampCatchup(tenMinutesAgo, Now));

        var justInside = Now - WatermarkPolicy.MaxCatchup + TimeSpan.FromSeconds(1);
        Assert.Equal(justInside, WatermarkPolicy.ClampCatchup(justInside, Now));
    }

    [Fact]
    public void ClampCatchup_ExactlyAtHorizon_NotClamped()
    {
        /* The floor is strict (< floor clamps): a watermark exactly MaxCatchup old is at the horizon,
           not past it. */
        var atHorizon = Now - WatermarkPolicy.MaxCatchup;
        Assert.Equal(atHorizon, WatermarkPolicy.ClampCatchup(atHorizon, Now));
    }

    [Fact]
    public void ClampCatchup_StalerThanHorizon_FlooredToNowMinusMaxCatchup()
    {
        /* The field incidents: a stale watermark is floored to now-MaxCatchup so one cycle's window is
           bounded; the skipped range is the backfill worker's job. */
        var floor = Now - WatermarkPolicy.MaxCatchup;

        Assert.Equal(floor, WatermarkPolicy.ClampCatchup(Now.AddDays(-3), Now));
        Assert.Equal(floor, WatermarkPolicy.ClampCatchup(Now.AddHours(-6), Now));

        var justPast = Now - WatermarkPolicy.MaxCatchup - TimeSpan.FromSeconds(1);
        Assert.Equal(floor, WatermarkPolicy.ClampCatchup(justPast, Now));
    }

    [Fact]
    public void ClampCatchup_FutureWatermark_ReturnedUnchanged()
    {
        /* A watermark ahead of now (clock skew) is not older than the floor, so it is left alone. */
        var future = Now.AddHours(1);
        Assert.Equal(future, WatermarkPolicy.ClampCatchup(future, Now));
    }

    [Fact]
    public void MaxCatchup_IsOneHour_AndMatchesTheBackfillSliceSpan()
    {
        /* Drift tripwire: one hour is the live path's one-query cost envelope — the width the fleet
           proves every day under Query Store's 900s flush cadence. It was 24h until #2102 showed the
           clamp sat far above the cost tipping point on big databases and never interrupted the
           timeout spiral. The equality half is the design invariant: NO path, live or backfill, may
           window wider than the other, or one of them re-becomes the wide-window casualty. */
        Assert.Equal(TimeSpan.FromHours(1), WatermarkPolicy.MaxCatchup);
        Assert.Equal(QueryStoreBackfillState.MaxSliceSpan, WatermarkPolicy.MaxCatchup);
    }

    /// <summary>
    /// #2344: the read floor must sit STRICTLY OLDER than the clamp horizon, because that ordering is the
    /// whole safety argument. A floor at or newer than the horizon could hide a row the clamp would have
    /// honoured; older by any margin cannot, since every outcome is max(stored, now - MaxCatchup) and a row
    /// below the horizon produces the same answer found or not.
    /// </summary>
    [Fact]
    public void ReadFloor_SitsStrictlyOlderThanTheClampHorizon()
    {
        var floor = WatermarkPolicy.ReadFloor(Now);

        Assert.NotNull(floor);
        Assert.True(floor < Now - WatermarkPolicy.MaxCatchup,
            "the read floor must be older than the clamp horizon, or the bound could hide a row the clamp would honour");
        Assert.Equal(Now - WatermarkPolicy.MaxCatchup - WatermarkPolicy.ReadFloorMargin, floor);
    }

    /// <summary>
    /// The equivalence the bound rests on, stated as a test rather than a comment: for any watermark at or
    /// below the read floor, the CLAMPED result is the horizon — identical to what the caller derives when
    /// the bounded read returns nothing at all (null falls back to query_store's 60-minute window, which is
    /// the same instant as the horizon). So bounding the read cannot change a single caller's outcome.
    /// </summary>
    [Theory]
    [InlineData(4)]
    [InlineData(6)]
    [InlineData(48)]
    [InlineData(24 * 90)]
    public void AnyWatermarkBelowTheReadFloor_ClampsToTheSameInstantAsFindingNothing(int hoursOld)
    {
        var floor = WatermarkPolicy.ReadFloor(Now)!.Value;
        var buried = Now.AddHours(-hoursOld);
        Assert.True(buried <= floor, "fixture must sit at or below the read floor");

        /* Found-but-old and not-found-at-all reach the same place. */
        var clampedIfFound = WatermarkPolicy.ClampCatchup(buried, Now);
        Assert.Equal(Now - WatermarkPolicy.MaxCatchup, clampedIfFound);
        Assert.Equal(Now.AddMinutes(-60), clampedIfFound);
        Assert.Null(WatermarkPolicy.ClampCatchup(null, Now));
    }

    /// <summary>A default input yields no floor — callers pass it straight through as "unbounded".</summary>
    [Fact]
    public void ReadFloor_OnDefault_IsNull()
    {
        Assert.Null(WatermarkPolicy.ReadFloor(default));
    }

    /// <summary>
    /// The horizon's NUMBER lives in <see cref="WatermarkPolicy.MaxCatchup"/> and nowhere else.
    ///
    /// <para><b>Why this needed asserting.</b> #2102 moved the horizon from 24 hours to one. The constant
    /// moved, the behaviour moved, and the operator-facing WARNINGs moved with it because they interpolate
    /// <c>MaxCatchup.TotalHours</c> instead of restating it. What did not move were nine comments across five
    /// files, each calling it "the 24h catch-up clamp". Nothing misbehaved, no test went red, and no log line
    /// disagreed — so there was no instrument in this repo that could see it. It was found the way stale prose
    /// always is: someone read the source, believed it, and reasoned from a 24-hour lever that had not existed
    /// for months (#2468, filed on exactly that premise).</para>
    ///
    /// <para>So the assertion is not "the comments say 1h" — that is the same defect with a fresher number in
    /// it, and it would go stale the next time the horizon moves. It is that no source discussing the clamp
    /// states an hours figure at all. The number has one home, and prose has to point at it.</para>
    ///
    /// <para><b>If this fails on something that has nothing to do with the horizon, read this first.</b> The
    /// trigger list is broader than the concept, on purpose and at a known cost. "catch-up" is ordinary English
    /// in this codebase — the compression backlog catches up, a cold-start sweep body catches up, WAL replay
    /// catches up — and none of those is <see cref="WatermarkPolicy.MaxCatchup"/>. A future comment that pairs
    /// one of them with an unrelated hours figure inside the window will fail this test, and it will be a
    /// SCOPE problem in the list below, not the horizon drifting. Fix it by narrowing the trigger, never by
    /// widening the window's tolerance for figures.</para>
    ///
    /// <para>Narrowing to "catch-up clamp" is the obvious escape and it does not work: two of the twelve sites
    /// this caught named the clamp without the word "clamp" adjacent ("roll past 24h on their own", two lines
    /// under "legitimate catch-up"), and two more named it without the words "catch-up" at all
    /// ("24h-clamped outage holes"). A precise trigger would have missed the ones that hide best. The false
    /// positive is the price of that, and it is the cheaper failure — it is loud, and it is this paragraph.</para>
    /// </summary>
    [Fact]
    public void TheCatchUpHorizon_IsWrittenDownInExactlyOnePlace()
    {
        /* The concept, wherever it is named. "clamp fires" / "-clamped" / "clamp-bounded" are in the list
           because the site in DarlingWorker that carried this defect named the clamp without ever using the
           words "catch-up" — a trigger list built only from the obvious phrase would have missed it. */
        const string mentions = @"catch-up|ClampCatchup|MaxCatchup|clamp fires|clamp-bounded|-clamped";

        /* An hours figure in any spelling. Minutes are legitimate and common nearby (the 60-minute
           first-run window, the 15-minute adaptive floor), so the pattern is deliberately hours-only. */
        const string hours = @"\b\d+\s*-?\s*(h\b|hr\b|hrs\b|hours?\b|hour-)";

        var offenders = new List<string>();

        foreach (var file in ClampSources())
        {
            var text = File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal);

            foreach (Match mention in Regex.Matches(text, mentions, RegexOptions.IgnoreCase))
            {
                /* A window rather than a line: these are wrapped block comments, and the figure and the
                   concept routinely land on different lines. */
                var from = Math.Max(0, mention.Index - 220);
                var to = Math.Min(text.Length, mention.Index + 220);
                var figure = Regex.Match(text[from..to], hours, RegexOptions.IgnoreCase);

                if (figure.Success)
                {
                    offenders.Add($"{Path.GetFileName(file)}: '{figure.Value}' near '{mention.Value}'");
                }
            }
        }

        Assert.True(
            offenders.Count == 0,
            "the catch-up horizon's number belongs to WatermarkPolicy.MaxCatchup and nowhere else — " +
            "name the concept and let the constant carry the figure: " + string.Join("; ", offenders.Distinct()));
    }

    /// <summary>Every shipped source that could describe the clamp: the shared collectors, the Darling
    /// service's runners, and Lite's twin of them. <c>WatermarkPolicy.cs</c> is excluded because it is the
    /// one place allowed to write the number down — including its own record of what the horizon used to
    /// be, which is history worth keeping rather than prose that has gone stale.</summary>
    private static IEnumerable<string> ClampSources()
    {
        var repo = RepoRoot();

        var roots = new[]
        {
            Path.Combine(repo, "PerformanceMonitor.Collectors"),
            Path.Combine(repo, "Darling", "PerformanceMonitor.Darling.Service"),
            Path.Combine(repo, "Lite", "Services"),
        };

        foreach (var root in roots)
        {
            Assert.True(Directory.Exists(root), $"{root} is gone — find where it moved before editing this test");

            foreach (var file in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
            {
                /* Skip build output. A local build drops generated .cs under obj/, and a scan that reads
                   them is asserting about artifacts rather than about the source anyone will edit. */
                if (file.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                    || file.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                {
                    continue;
                }

                if (!string.Equals(Path.GetFileName(file), "WatermarkPolicy.cs", StringComparison.Ordinal))
                {
                    yield return file;
                }
            }
        }
    }

    /// <summary>
    /// A guard that CI skips on the PRs it guards is a guard that has silently stopped guarding — the
    /// same failure the horizon pin above exists to close, one layer out.
    ///
    /// <para>Raised by review on #2471. That pin lives in <c>Lite.Tests</c> and walks the Darling service
    /// tree, but <c>build.yml</c>'s "Run Lite tests" step gates on <c>lite</c> / <c>core</c> / <c>root</c>.
    /// <c>core</c> covers <c>PerformanceMonitor.Collectors</c> and <c>lite</c> covers <c>Lite/Services</c>,
    /// so two of the three trees were fine — but <c>Darling/PerformanceMonitor.Darling.Service</c> belongs
    /// only to <c>darling</c>, and three of the twelve sites the pin was written for live there. A
    /// Darling-only PR reintroducing one would have fired <c>darling</c>, skipped this suite, and been
    /// caught a day later by the nightly.</para>
    ///
    /// <para>So the filter entry is load-bearing, and a filter entry is exactly the kind of thing that gets
    /// tidied away by someone trimming what looks like an over-broad path. It is asserted rather than
    /// commented — <c>Darling.Tests</c>' CI-worker-sizing guards already set the precedent for a test
    /// reading these workflows.</para>
    /// </summary>
    [Fact]
    public void TheLiteSuite_RunsOnEveryTreeTheHorizonPinScans()
    {
        var yaml = File.ReadAllText(Path.Combine(RepoRoot(), ".github", "workflows", "build.yml"))
            .Replace("\r\n", "\n", StringComparison.Ordinal);

        var at = yaml.IndexOf("\n            lite:\n", StringComparison.Ordinal);
        Assert.True(at > 0, "build.yml's 'lite' path filter is gone — find where it moved before editing this test");

        /* The block runs to the next area key at the same indent; its own entries are indented deeper. */
        var rest = yaml[(at + 1)..];
        var next = Regex.Match(rest, "\n            [a-z_]+:\n");
        var block = next.Success ? rest[..next.Index] : rest;

        Assert.Contains("Darling/PerformanceMonitor.Darling.Service/**/!(*.md)", block, StringComparison.Ordinal);

        /* The step that consumes it. If "Run Lite tests" ever stops reading `lite`, the entry above is
           decoration and this test is the only thing that would notice. */
        var step = yaml.IndexOf("name: Run Lite tests", StringComparison.Ordinal);
        Assert.True(step > 0, "the 'Run Lite tests' step is gone — find where it moved before editing this test");
        Assert.Contains("steps.filter.outputs.lite == 'true'", yaml[step..(step + 400)], StringComparison.Ordinal);
    }

    /// <summary>The repo root, located by walking up from this file's compile-time path — the same idiom
    /// the other source-scanning suites use.</summary>
    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            if (Directory.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.Collectors")))
            {
                return dir.FullName;
            }
        }

        throw new DirectoryNotFoundException($"could not locate the repo root walking up from {thisFile}");
    }
}
