/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The viewer's command deadlines (#2874). Every command in <c>PerformanceMonitor.Darling.Viewer</c> runs on a
/// deadline from <see cref="ViewerCommandDeadlines"/> rather than Npgsql's undocumented 30 s default, the defect
/// class behind three production failures (#2810, #2871, #2796): exceeding that ceiling surfaces as
/// <c>Exception while reading from stream</c>, which reads as a network fault rather than a deadline. These tests
/// check that every viewer command sets one, and
/// pin the deadline values, the fan-out widths they scale with, and the pool size they are derived from.
///
/// <para><b>Values are pinned as BANDS, never as equalities.</b> Each band encodes the reasoning that
/// produced the number — the measured floor below it and the shared budget above it — so a future
/// re-derivation inside the band is free and a drift out of it has to argue with the derivation on
/// <see cref="ViewerCommandDeadlines"/>. Freezing the numbers themselves would couple three
/// deliberate values to one test, which is what the <c>.Storage</c> pin declined to do.</para>
/// </summary>
[Trait("Stage", "Guard")] // #5798: walks the repo tree, so the guard stage runs it
public sealed class ViewerCommandTimeoutTests
{
    /// <summary>
    /// Both ways a command is constructed in this codebase — <c>new NpgsqlCommand(</c> and
    /// <c>.CreateCommand(</c>. The second is the shape #2874's original census missed entirely, and
    /// here it is 191 of the 193 sites.
    /// </summary>
    private static readonly Regex s_commandCtor = new(
        @"new\s+(?:[A-Za-z_][A-Za-z0-9_]*\s*\.\s*)*NpgsqlCommand\s*\(|\.CreateCommand\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Every command the viewer project creates sets an explicit deadline (#2874). Kept as a source scan because a
    /// new command built without a deadline fails nowhere else: it silently runs on Npgsql's 30 s default, and no
    /// behavior test can reach a command that does not exist yet.
    /// </summary>
    [Fact]
    public void EveryViewerCommand_SetsAnExplicitDeadline()
    {
        var offenders = new List<string>();
        var total = 0;

        foreach (var path in ViewerSources())
        {
            var text = File.ReadAllText(path);

            /* Both halves of the question are asked of STRIPPED text, which is character-aligned with its
               input so an offset means the same thing in either. A construction named only in prose is not a
               construction, and a deadline merely SPELLED in a comment is not a deadline - judged raw, a note
               explaining where the deadline used to be stands in for the deadline. */
            var code = CSharpSourceWalker.StripCommentsAndStrings(text);

            foreach (Match ctor in s_commandCtor.Matches(code))
            {
                total++;

                if (!CommandDeadlineScanner.SetsAnExplicitDeadline(code, ctor.Index))
                {
                    var line = text.Take(ctor.Index).Count(c => c == '\n') + 1;
                    offenders.Add($"{Path.GetFileName(path)}:{line}");
                }
            }
        }

        /* 193 sites at the time this pin landed; the floor guards against the sweep silently reading
           an empty or wrong directory, not against refactors that change the count. */
        Assert.True(total >= 150, $"the viewer scan matched only {total} command constructions — the sweep is not reading the project");

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} viewer command(s) inherit Npgsql's 30s default instead of an explicit deadline: "
            + string.Join(", ", offenders));
    }

    /// <summary>
    /// The interactive/refresh deadline, bounded on both sides — full derivation on
    /// <see cref="ViewerCommandDeadlines.InteractiveReadSeconds"/>. Short form: the heaviest shipped
    /// per-server read measured 3.01 s COLD on a V109 store seeded to production per-server density
    /// across the full 30-day retention horizon, and nothing encloses these reads, so the deadline is
    /// itself the budget and has to sit under the default it replaces rather than at it.
    /// </summary>
    [Fact]
    public void TheInteractiveReadDeadline_StaysInsideItsJustifiedBand()
    {
        var seconds = ViewerCommandDeadlines.InteractiveReadSeconds;

        Assert.True(
            seconds > 3,
            $"interactive read deadline {seconds}s is at or under the 3.01 s worst measured shipped read "
            + "(TopQueriesSql over a 30-day custom range at production density) — it would fail a legitimate panel");

        Assert.True(
            seconds < 30,
            $"interactive read deadline {seconds}s is not meaningfully under the inherited Npgsql default it "
            + "replaces. These reads have no enclosing budget and compete for a ten-connection pool that one "
            + "control can take entirely, so every second granted here is a second the whole viewer can spend "
            + "holding permits while the user watches a spinner");
    }

    /// <summary>
    /// The command-plane deadline, pinned RELATIONALLY against the budget it actually sits inside
    /// rather than against a copied number: <c>PollCommandResultAsync</c> re-issues the poll until
    /// <see cref="ViewerDataService.DefaultCommandTimeout"/>, and checks that budget only BETWEEN
    /// iterations — so a read deadline at or above the loop's budget puts the read back in charge of
    /// how long the dialog waits, which is the defect the constant exists to close.
    /// </summary>
    [Fact]
    public void TheCommandPlaneDeadline_StaysWellInsideItsEnclosingBudget()
    {
        var seconds = ViewerCommandDeadlines.CommandPlaneSeconds;
        var enclosing = ViewerDataService.DefaultCommandTimeout.TotalSeconds;

        Assert.True(
            seconds >= 1,
            $"command-plane deadline {seconds}s leaves no room over the 3.9 ms cold single-row poll, and the "
            + "delete is what removes a credential-bearing args_json row");

        Assert.True(
            seconds * 2 < enclosing,
            $"command-plane deadline {seconds}s is not comfortably inside the {enclosing}s poll-loop budget it "
            + "runs under — the loop checks its budget only between iterations, so one read this long overshoots "
            + "the wait the dialog promises");
    }

    /// <summary>
    /// The connect-gate deadline. Bounded below by a catalog probe that does not grow with the store
    /// (79 ms cold for all 85 schema sentinels) and above by the connect preference's own ceiling —
    /// these are the first two statements after connect, they run before the window is usable, and
    /// both swallow their own failures, so overshooting mis-classifies the store silently.
    /// </summary>
    [Fact]
    public void TheConnectGateDeadline_StaysInsideItsJustifiedBand()
    {
        var seconds = ViewerCommandDeadlines.ConnectGateSeconds;

        Assert.True(
            seconds >= 1,
            $"connect-gate deadline {seconds}s leaves no room over the 79 ms cold schema probe; a probe that "
            + "times out fails OPEN or SAFE, so it hides write affordances rather than reporting anything");

        Assert.True(
            seconds <= 60,
            $"connect-gate deadline {seconds}s exceeds the 60s ceiling the viewer's own connection-timeout "
            + "preference is clamped to, so the gate could outlast the connect it follows");
    }

    /// <summary>
    /// The two ceilings answer different questions, and this is the pin that says so in the only terms
    /// available without a store: where each one sits relative to the CONCURRENT band #2901 measured
    /// (24.4-64.1 s per read for ten concurrent 30-day reads, against 3.01 s for the same read alone).
    ///
    /// <para>Deliberately NOT a timing test. The failure it guards needs a Windows host and a store dense
    /// enough to reach the band; a test that measured anything here would pass on a fast machine and prove
    /// nothing. What it asserts instead is arithmetic on shipped constants, which holds identically
    /// everywhere.</para>
    /// </summary>
    [Fact]
    public void TheFanOutDeadline_IsDerivedFromTheConcurrentBand_NotTheSoloOne()
    {
        var solo = ViewerCommandDeadlines.InteractiveReadSeconds;

        /* The EXPLICIT pool, not the process-published one (#3016): this asserts the derivation, and a
           derivation asserted against whatever another test class last published is asserting less. */
        var widest = ViewerCommandDeadlines.FanOutReadSeconds(
            ViewerCommandDeadlines.MeasuredFanOutWidth, ViewerSettings.ManagedMaxPoolSize);

        Assert.True(
            solo < 24,
            $"the solo read deadline {solo}s has risen into the concurrent band (24.4-64.1 s). Either it is no "
            + "longer derived from the 3.01 s solo measurement, or the two regimes have been collapsed back "
            + "into one constant — which is the defect #3004 exists to undo, in the other direction");

        Assert.True(
            widest > 64,
            $"the fan-out deadline at the pool ceiling is {widest}s, which does not cover the 64.1 s worst read "
            + "measured for ten concurrent 30-day reads on a store at production density. A ceiling under the "
            + "measurement it is supposed to bound fails every read in the widest fan-out on every attempt, "
            + "auto-refresh included — a deadline nothing can finish under is not a deadline");

        /* The band has ONE measurement in it, at ten wide. A bring-your-own store with Npgsql's default
           hundred-connection pool must not be handed ten times the allowance on the strength of that one
           point — 700 s is not a deadline anybody has measured or would wait for. */
        Assert.Equal(
            widest,
            ViewerCommandDeadlines.FanOutReadSeconds(
                ViewerCommandDeadlines.MeasuredFanOutWidth, ViewerStorePool.NpgsqlDefaultMaxPoolSize));

        Assert.True(
            ViewerCommandDeadlines.MeasuredFanOutWidth <= ViewerSettings.ManagedMaxPoolSize,
            $"the measured fan-out width {ViewerCommandDeadlines.MeasuredFanOutWidth} exceeds the managed pool "
            + $"{ViewerSettings.ManagedMaxPoolSize}, so the shipped seat can no longer reach the width the "
            + "per-lane allowance was divided out of — the allowance is then an extrapolation on the default "
            + "install, not only on a BYO one");
    }

    /// <summary>
    /// The fan-out ceiling can only ever grant MORE time than a solo read gets, and it stops growing at the
    /// smaller of two bounds: the permits this seat has, and the width the per-lane allowance was measured
    /// at. Swept across POOL SIZES as well as widths (#3016) — the pool used to be a compile-time constant
    /// and a sweep over widths alone could not tell a threaded pool size from a hardcoded one.
    ///
    /// <para>Three of the pool sizes carry a specific claim. At <c>ManagedMaxPoolSize</c> nothing about the
    /// shipped seat may move. At <c>NpgsqlDefaultMaxPoolSize</c> — a bring-your-own string that names no
    /// pool size — the answer must be the SAME as at ten, because the extra permits buy contention nobody
    /// has priced. Below ten it must be strictly SMALLER, which is the half a hardcoded constant got wrong
    /// in the expensive direction: a pool of three cannot produce ten-way contention, so a ten-lane
    /// allowance there is 49 s of a stalled panel holding permits nothing else can have.</para>
    /// </summary>
    [Fact]
    public void TheFanOutDeadline_IsMonotonicAndClampedAtThePool()
    {
        var measured = ViewerCommandDeadlines.MeasuredFanOutWidth;
        var pools = new[]
        {
            1, 2, 3, ViewerSettings.ManagedMaxPoolSize, ViewerStorePool.NpgsqlDefaultMaxPoolSize, int.MaxValue,
        };

        foreach (var pool in pools)
        {
            var bound = System.Math.Min(pool, measured);
            var previous = 0;

            for (var width = -3; width <= bound * 4; width++)
            {
                var seconds = ViewerCommandDeadlines.FanOutReadSeconds(width, pool);

                Assert.True(
                    seconds >= ViewerCommandDeadlines.InteractiveReadSeconds,
                    $"pool {pool} width {width} yields {seconds}s, under the solo floor — a declared fan-out "
                    + "must never buy a read LESS time than the same read issued alone");

                Assert.True(
                    seconds >= previous,
                    $"pool {pool} width {width} yields {seconds}s, below width {width - 1}'s {previous}s");

                Assert.True(
                    seconds <= ViewerCommandDeadlines.FanOutLaneSeconds * measured,
                    $"pool {pool} width {width} yields {seconds}s, above the {measured}-lane allowance the "
                    + "concurrent band was measured at — every second past it is extrapolated from a single "
                    + "data point");

                previous = seconds;
            }

            Assert.Equal(
                ViewerCommandDeadlines.FanOutReadSeconds(bound, pool),
                ViewerCommandDeadlines.FanOutReadSeconds(bound * 4, pool));
        }

        Assert.Equal(
            ViewerCommandDeadlines.FanOutReadSeconds(measured, ViewerSettings.ManagedMaxPoolSize),
            ViewerCommandDeadlines.FanOutReadSeconds(measured, ViewerStorePool.NpgsqlDefaultMaxPoolSize));

        Assert.True(
            ViewerCommandDeadlines.FanOutReadSeconds(measured, 3)
            < ViewerCommandDeadlines.FanOutReadSeconds(measured, ViewerSettings.ManagedMaxPoolSize),
            "a three-permit pool is priced the same as a ten-permit one, so the deadline is not reading the "
            + "pool size at all — it is reading a constant that only describes the managed derivation");

        /* The overload the 187 command sites actually reach must agree with the pure one at the pool this
           process published; otherwise the derivation above is pinned on a path nothing calls. */
        for (var width = -3; width <= measured * 4; width++)
        {
            Assert.Equal(
                ViewerCommandDeadlines.FanOutReadSeconds(width, ViewerStorePool.MaxPoolSize),
                ViewerCommandDeadlines.FanOutReadSeconds(width));
        }
    }

    /// <summary>
    /// The ambient width itself: one when nobody declared a fan-out (so an unscoped read keeps exactly the
    /// solo ceiling), the declared value inside a scope, the enclosing value again after it, and the PRODUCT
    /// when scopes nest — because a read two scopes deep really does contend with both widths.
    ///
    /// <para>The clamp is asserted against <see cref="ViewerReadFanOut.MaxConcurrentReads"/> rather than
    /// against <c>ViewerSettings.ManagedMaxPoolSize</c> (#3016). The two are the same number on the shipped
    /// managed seat, so this reads as a rename here and is not one: the bound is now the smaller of the
    /// permits this seat has and the width the allowance was measured at, and pinning the constant would
    /// pass identically whether or not the pool is read at all. <see cref="ViewerReadFanOut.ConcurrentReadsFor"/>
    /// is swept below at pool sizes this process is not configured for, which is where the difference
    /// shows.</para>
    /// </summary>
    [Fact]
    public void TheFanOutWidth_DefaultsToOne_AndNestsByMultiplying()
    {
        Assert.Equal(1, ViewerReadFanOut.CurrentWidth);

        using (ViewerReadFanOut.Of(3))
        {
            Assert.Equal(3, ViewerReadFanOut.CurrentWidth);

            using (ViewerReadFanOut.Of(2))
            {
                Assert.Equal(6, ViewerReadFanOut.CurrentWidth);
            }

            Assert.Equal(3, ViewerReadFanOut.CurrentWidth);
        }

        Assert.Equal(1, ViewerReadFanOut.CurrentWidth);

        /* A count from a runtime collection — what the two per-server fan-outs pass through Lanes. */
        using (ViewerReadFanOut.Of(ViewerReadFanOut.MaxConcurrentReads * 7))
        {
            Assert.Equal(ViewerReadFanOut.MaxConcurrentReads, ViewerReadFanOut.CurrentWidth);
        }

        using (ViewerReadFanOut.Of(0))
        {
            Assert.Equal(1, ViewerReadFanOut.CurrentWidth);
        }

        /* The bound the clamp above uses, at pool sizes no test can publish without racing another class:
           BELOW the measured width the pool wins, AT or ABOVE it the measurement does, and a nonsense pool
           still leaves one lane rather than zero. */
        Assert.Equal(
            ViewerReadFanOut.ConcurrentReadsFor(ViewerStorePool.MaxPoolSize),
            ViewerReadFanOut.MaxConcurrentReads);

        Assert.Equal(1, ViewerReadFanOut.ConcurrentReadsFor(1));
        Assert.Equal(3, ViewerReadFanOut.ConcurrentReadsFor(3));

        Assert.Equal(
            ViewerCommandDeadlines.MeasuredFanOutWidth,
            ViewerReadFanOut.ConcurrentReadsFor(ViewerSettings.ManagedMaxPoolSize));

        Assert.Equal(
            ViewerCommandDeadlines.MeasuredFanOutWidth,
            ViewerReadFanOut.ConcurrentReadsFor(ViewerStorePool.NpgsqlDefaultMaxPoolSize));

        Assert.Equal(
            ViewerCommandDeadlines.MeasuredFanOutWidth,
            ViewerReadFanOut.ConcurrentReadsFor(int.MaxValue));

        Assert.Equal(1, ViewerReadFanOut.ConcurrentReadsFor(0));
        Assert.Equal(1, ViewerReadFanOut.ConcurrentReadsFor(-7));
    }

    /// <summary>
    /// The effective pool size is read off the CONNECTION STRING, which is the fact both the deadline and
    /// the concurrency bound were missing (#3016). <c>ManagedMaxPoolSize</c> is applied to exactly one of
    /// the two strings the viewer can be handed — the managed derivation — and a bring-your-own
    /// <c>postgres.connectionString</c> is used verbatim.
    ///
    /// <para><b>The hundred is Npgsql's, measured off its own builder at the pinned version rather than
    /// quoted from its documentation</b>, and asserted here so a version bump that moves the default fails
    /// a build instead of silently changing what a BYO store is assumed to have. The fallbacks go to the
    /// managed constant rather than to the hundred: a string this cannot read is a string
    /// <c>NpgsqlDataSource.Create</c> will reject, and the operator needs that error rather than a second
    /// one invented by a timeout calculation.</para>
    /// </summary>
    [Fact]
    public void TheEffectivePoolSize_ReadsTheStringNotTheManagedConstant()
    {
        Assert.Equal(
            ViewerStorePool.NpgsqlDefaultMaxPoolSize,
            ViewerStorePool.MaxPoolSizeOf("Host=127.0.0.1;Port=5432;Database=darling;Username=admin"));

        Assert.Equal(
            7,
            ViewerStorePool.MaxPoolSizeOf("Host=127.0.0.1;Database=darling;MaxPoolSize=7"));

        Assert.Equal(
            7,
            ViewerStorePool.MaxPoolSizeOf("Host=127.0.0.1;Database=darling;Maximum Pool Size=7"));

        Assert.Equal(
            ViewerSettings.ManagedMaxPoolSize,
            ViewerStorePool.MaxPoolSizeOf(
                $"Host=127.0.0.1;Database=darling;MaxPoolSize={ViewerSettings.ManagedMaxPoolSize}"));

        Assert.Equal(ViewerSettings.ManagedMaxPoolSize, ViewerStorePool.MaxPoolSizeOf(null));
        Assert.Equal(ViewerSettings.ManagedMaxPoolSize, ViewerStorePool.MaxPoolSizeOf(""));
        Assert.Equal(ViewerSettings.ManagedMaxPoolSize, ViewerStorePool.MaxPoolSizeOf("   "));
        Assert.Equal(
            ViewerSettings.ManagedMaxPoolSize,
            ViewerStorePool.MaxPoolSizeOf("this is not a connection string at all"));

        /* Two unparseable shapes, because Npgsql raises two different exception types for them and a
           narrower catch would let one escape a constructor whose call site does not catch — the fallback
           would then REPLACE the connect-time error it exists to defer to. Measured at 10.0.3: a bad value
           for a recognised keyword is ArgumentException; this one is KeyNotFoundException, which does not
           derive from it. Found in review on #3034. */
        Assert.Equal(
            ViewerSettings.ManagedMaxPoolSize,
            ViewerStorePool.MaxPoolSizeOf("Host=127.0.0.1;Database=darling;MaxPoolSize=not-a-number"));

        Assert.Equal(ViewerSettings.ManagedMaxPoolSize, ViewerStorePool.MaxPoolSizeOf("====="));

        /* A pool size of zero PARSES — Npgsql validates it at connect, not at the builder — so the floor
           below one is what keeps a zero out of the lane arithmetic rather than the catch above. */
        Assert.Equal(
            ViewerSettings.ManagedMaxPoolSize,
            ViewerStorePool.MaxPoolSizeOf("Host=127.0.0.1;Database=darling;MaxPoolSize=0"));

        Assert.True(
            ViewerStorePool.NpgsqlDefaultMaxPoolSize > ViewerSettings.ManagedMaxPoolSize,
            $"Npgsql's default pool ({ViewerStorePool.NpgsqlDefaultMaxPoolSize}) is no longer larger than the "
            + $"managed seat's ({ViewerSettings.ManagedMaxPoolSize}), so the BYO half of #3016 — a fan-out "
            + "handed more permits than the deadline was sized for — no longer describes anything, and this "
            + "family's reasoning needs re-reading rather than this number nudging");
    }

    /// <summary>
    /// The lane split every fleet-wide fan-out goes through: a partition of the input — every item once,
    /// in order — into no more lanes than the seat has permits (#3016).
    ///
    /// <para><b>Contiguity is asserted, not incidental.</b> Both callers concatenate the lanes' results in
    /// lane order and rely on that reproducing the input order: <c>MainWindow</c>'s overview re-sorts
    /// afterwards, but the FinOps inventory overlay mutates the rows it was handed and hands the SAME list
    /// to its filter manager, so a split that reordered anything would reorder the grid. A stride split
    /// would satisfy every other assertion here and break that one, which is why it is spelled out.</para>
    /// </summary>
    [Fact]
    public void TheLaneSplit_PartitionsInOrder_AndNeverExceedsThePermits()
    {
        var cap = ViewerReadFanOut.MaxConcurrentReads;

        Assert.True(cap >= 1, $"the concurrency bound is {cap}; a fan-out cannot have fewer than one lane");
        Assert.Empty(ViewerReadFanOut.Lanes(System.Array.Empty<int>()));

        for (var count = 1; count <= cap * 4 + 3; count++)
        {
            var items = Enumerable.Range(0, count).ToArray();
            var lanes = ViewerReadFanOut.Lanes(items);

            Assert.True(
                lanes.Count <= cap,
                $"{count} items split into {lanes.Count} lanes, above the {cap} the pool can serve — the reads "
                + "past the permits are not contenders, they are queued and failing");

            Assert.Equal(System.Math.Min(count, cap), lanes.Count);

            /* A partition: every item exactly once, and in input order when the lanes are concatenated. */
            Assert.Equal(items, lanes.SelectMany(lane => lane).ToArray());

            var shortest = lanes.Min(lane => lane.Count);
            var longest = lanes.Max(lane => lane.Count);

            Assert.True(
                longest - shortest <= 1,
                $"{count} items split into lanes of {shortest}..{longest} — an unbalanced split makes the whole "
                + "fan-out as slow as its longest lane for no reason");
        }
    }

    private static IEnumerable<string> ViewerSources()
    {
        var dir = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer");

        Assert.True(Directory.Exists(dir), $"viewer project directory not found: {dir}");

        /* RECURSIVE, unlike the .Storage pin's TopDirectoryOnly, minus the build outputs. The claim
           is "every command THIS PROJECT creates", and a file added under a future subdirectory is
           still this project's - the viewer already carries a Themes/ folder, so subdirectories are
           not hypothetical here. bin/ and obj/ are excluded by path segment rather than by name
           match, because that is where the generated .AssemblyInfo.cs and .g.cs land during a CI
           build and they are not source. */
        var paths = Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories)
            .Where(p => !IsBuildOutput(dir, p))
            .OrderBy(p => p, System.StringComparer.Ordinal)
            .ToArray();

        Assert.True(paths.Length >= 150, $"the viewer sweep found only {paths.Length} files — the project has moved");

        return paths;
    }

    /// <summary>
    /// True when a path sits under the project's <c>bin</c> or <c>obj</c> tree. Compared as PATH
    /// SEGMENTS, so a source file that merely has "obj" in its name is not excluded.
    /// </summary>
    private static bool IsBuildOutput(string projectDir, string path)
    {
        var relative = Path.GetRelativePath(projectDir, path);
        var segments = relative.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

        return segments.Any(s =>
            string.Equals(s, "bin", System.StringComparison.OrdinalIgnoreCase)
            || string.Equals(s, "obj", System.StringComparison.OrdinalIgnoreCase));
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
