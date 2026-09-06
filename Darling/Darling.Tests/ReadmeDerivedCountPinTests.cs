/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Drift guard between <c>Darling/README.md</c>'s counted claims and the code that produces them (#3072).
///
/// <para><b>Why this exists.</b> The README said "Twelve PostgreSQL data-read tools" while the server exposed
/// thirty-two, and the sweep that found it turned up the same shape in nine other places — the collector
/// inventory read 48/41/7 against a catalog of 69/42/27, and the worker-sizing paragraph read 57/68 for 55
/// hypertables while <c>build.yml</c> and <c>nightly.yml</c> carried 72/83 for 70 and CI ENFORCED those
/// (<see cref="CiClusterWorkerSizingTests"/>). Every one of them was correct when written. A number in prose
/// has no way to notice that the thing it counts has moved, so the ones worth stating are asserted here and
/// the ones that were only restating an adjacent list were deleted instead.</para>
///
/// <para><b>Why a parse rather than a constant.</b> Documentation cannot call into the product, so the figures
/// have to be literals in Markdown. This reads the REAL README (copied beside the test binary by the csproj,
/// so there is no second copy to go stale) and requires each one to equal what the catalog produces today. Add
/// a collector and this fails on the pull request that adds it, naming the sentence and the number to change.
/// Digits, not words, in the pinned sentences — the same choice
/// <c>CrossAppMcpToolInventoryPinTests.DarlingInstructionsCensus_MatchesTheScannedInventory</c> made so its
/// census stayed parseable.</para>
///
/// <para><b>The guard is itself guarded.</b> Every extraction asserts its pattern MATCHED before comparing a
/// value, and <see cref="EveryPin_ReportsAnInjectedDrift"/> runs the identical extractions over a mutated copy
/// and requires each to report the mutation. A regex that silently stopped matching — a reworded sentence, a
/// bolded number, a line rewrapped — is otherwise indistinguishable from a clean tree by its result alone,
/// which is the failure mode this whole family of source-parsing tests exists to catch.</para>
/// </summary>
public sealed class ReadmeDerivedCountPinTests
{
    /* The derived quantities, restated ONCE each. Everything below compares against these rather than against
       a second literal, so no assertion here can be "right" about a stale catalog. */
    private static int CollectorTotal => CollectorCatalog.All.Count;

    private static int SqlServerCollectors =>
        CollectorCatalog.All.Count(c => c.TargetEngine == CollectorTargetEngine.SqlServer);

    private static int PostgresCollectors =>
        CollectorCatalog.All.Count(c => c.TargetEngine == CollectorTargetEngine.PostgreSql);

    private static int Hypertables => TimescaleSupport.HypertableCount;

    private static int BackgroundWorkers => Hypertables + 2;

    private static int WorkerProcesses => 3 + BackgroundWorkers + 8;

    /// <summary>
    /// PostgreSQL collectors that apply to a self-hosted (non-Aurora) PG17 writer — everything except the
    /// Aurora-gated ones. Derived through the real gate rather than by subtracting a remembered number,
    /// because the gate is where the condition lives.
    /// </summary>
    private static int SelfHostedPostgresCollectors =>
        CollectorCatalog.All.Count(c => c.TargetEngine == CollectorTargetEngine.PostgreSql
            && CollectorCatalog.AppliesTo(c, new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = 17,
                PostgresVersionNum = 170007,
                IsAurora = false,
                IsInRecovery = false,
            }));

    /// <summary>
    /// The pinned sentences. Each entry is a NAMED pattern plus the derived values its capture groups must
    /// equal, so a failure says which sentence and which number rather than "a regex did not match".
    /// </summary>
    private static IEnumerable<(string Name, Regex Pattern, Func<int[]> Expected)> Pins()
    {
        yield return (
            "the collector-inventory sentence",
            new Regex(@"owns all (\d+) collector definitions — (\d+) for SQL Server and (\d+) for PostgreSQL"),
            () => new[] { CollectorTotal, SqlServerCollectors, PostgresCollectors });

        yield return (
            "the V1 migration row",
            new Regex(@"One table per collector, all (\d+), generated from the shared collector definitions"),
            () => new[] { CollectorTotal });

        yield return (
            "the worker-sizing paragraph",
            new Regex(@"Today that is \*\*(\d+)\*\* and \*\*(\d+)\*\* for (\d+) hypertables \(the (\d+) collector tables plus"),
            () => new[] { BackgroundWorkers, WorkerProcesses, Hypertables, CollectorTotal });

        yield return (
            "the empty-PostgreSQL-tables sentence",
            new Regex(@"still carries the (\d+) PostgreSQL tables, empty"),
            () => new[] { PostgresCollectors });

        yield return (
            "the self-hosted PostgreSQL sentence",
            new Regex(@"the other (\d+) collectors run while those two sit out"),
            () => new[] { SelfHostedPostgresCollectors });

        yield return (
            "the outage-predictor sentence",
            new Regex(@"Three of the (\d+) are outage predictors"),
            () => new[] { PostgresCollectors });
    }

    [Fact]
    public void EveryPinnedSentence_MatchesTheCatalog()
    {
        var readme = ReadReadme();

        foreach (var (name, pattern, expected) in Pins())
        {
            AssertPin(readme, name, pattern, expected());
        }
    }

    /// <summary>
    /// The <c>--test-connection</c> sample transcript quotes the service's own per-target gate summary. Only
    /// the DENOMINATOR is pinned — it is the collector count, so it moves with the catalog — while the
    /// per-target numerator and skip list depend on the probe and belong to
    /// <see cref="DarlingCliCommandsTests"/>, which already derives them from the catalog for the real output.
    /// The README's copy of that output was the part nothing held.
    /// </summary>
    [Fact]
    public void TestConnectionTranscript_DenominatorsMatchThePostgresCollectorCount()
    {
        var readme = ReadReadme();

        var denominators = Regex.Matches(readme, @"(?:all|of) (\d+) PostgreSQL collectors apply")
            .Select(m => int.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture))
            .ToList();

        Assert.True(denominators.Count >= 3,
            $"Only {denominators.Count} '<n> PostgreSQL collectors apply' line(s) found in the --test-connection "
            + "transcript in Darling/README.md; the sample shows three targets, so the scan has stopped matching. "
            + "Fix the pattern rather than the count.");

        Assert.All(denominators, d => Assert.Equal(PostgresCollectors, d));
    }

    /// <summary>
    /// Non-vacuity, the way <see cref="CiClusterWorkerSizingTests.ParsedSettings_Comparison_FailsOnAnInjectedDrift"/>
    /// does it: mutate a COPY of the README so each pinned number is wrong, and require the identical
    /// extraction to REPORT the mutation. If any of these passed, that pin's regex matched nothing and its
    /// assertion in <see cref="EveryPinnedSentence_MatchesTheCatalog"/> was doing nothing.
    /// </summary>
    [Fact]
    public void EveryPin_ReportsAnInjectedDrift()
    {
        var readme = ReadReadme();

        foreach (var (name, pattern, expected) in Pins())
        {
            var real = pattern.Match(readme);
            Assert.True(real.Success, $"{name}: pattern matched nothing, so its pin is vacuous.");

            /* Bump every captured digit group by one, in place, right-to-left so earlier offsets hold. */
            var mutated = readme;
            foreach (var group in real.Groups.Cast<Group>().Skip(1).OrderByDescending(g => g.Index))
            {
                var bumped = (int.Parse(group.Value, CultureInfo.InvariantCulture) + 1)
                    .ToString(CultureInfo.InvariantCulture);
                mutated = mutated.Remove(group.Index, group.Length).Insert(group.Index, bumped);
            }

            Assert.NotEqual(readme, mutated);

            var after = pattern.Match(mutated);
            Assert.True(after.Success, $"{name}: the mutation broke the pattern, so this proves nothing.");

            var values = after.Groups.Cast<Group>().Skip(1)
                .Select(g => int.Parse(g.Value, CultureInfo.InvariantCulture)).ToArray();
            var want = expected();

            Assert.Equal(want.Length, values.Length);
            for (var i = 0; i < want.Length; i++)
            {
                Assert.Equal(want[i] + 1, values[i]);
            }
        }
    }

    private static void AssertPin(string readme, string name, Regex pattern, int[] expected)
    {
        var match = pattern.Match(readme);

        /* Named explicitly rather than left to a bare group-index exception: the failure message has to tell
           someone who just added a collector WHICH sentence in the README to change, and that they should not
           "fix" the pattern to make this pass. */
        Assert.True(match.Success,
            $"Darling/README.md no longer contains {name} in the pinned shape ({pattern}). It restates the "
            + "collector catalog, so keep it parseable — or delete the number from the prose, which is the other "
            + "sanctioned outcome (#3072) and needs this pin removed with it.");

        var actual = match.Groups.Cast<Group>().Skip(1)
            .Select(g => int.Parse(g.Value, CultureInfo.InvariantCulture)).ToArray();

        Assert.Equal(expected.Length, actual.Length);

        for (var i = 0; i < expected.Length; i++)
        {
            Assert.True(expected[i] == actual[i],
                $"{name}: capture {i + 1} reads {actual[i]} but the catalog produces {expected[i]}. "
                + "Update the sentence in Darling/README.md.");
        }
    }

    private static string ReadReadme()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", "Darling-README.md");
        Assert.True(File.Exists(path),
            "Darling-README.md was not copied beside the test binary. Darling.Tests.csproj links Darling/README.md "
            + "into Fixtures\\ so this guard parses the real one; restore that item rather than pointing the test "
            + "at a copy that can go stale.");
        return File.ReadAllText(path);
    }
}
