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
    /// A self-hosted PostgreSQL 17 writer, for the gates that need a target to answer.
    /// </summary>
    private static CollectorTargetInfo SelfHostedPg17 => new()
    {
        Engine = CollectorTargetEngine.PostgreSql,
        PostgresMajorVersion = 17,
        PostgresVersionNum = 170007,
        IsAurora = false,
        IsInRecovery = false,
    };

    /// <summary>
    /// Every collector/extension dependency the catalog declares, as <c>collector/extension</c> — the
    /// authority the permissions paragraph's list is pinned to (#3187).
    ///
    /// <para><b>Not <c>PgExtensionAvailabilityCollector</c>'s roster</b>, which is the nearby list that
    /// looks like this one and answers a different question. That roster exists so ABSENCE is reportable,
    /// so it carries <c>hypopg</c>, <c>pg_trgm</c> and <c>pg_cron</c> — which no collector reads — and
    /// deliberately omits <c>pg_wait_sampling</c>, which a collector does. Pinning prose to it would be
    /// wrong in both directions at once.</para>
    /// </summary>
    private static IReadOnlySet<string> DependencyPairs =>
        CollectorCatalog.All
            .SelectMany(c => c.RequiredPgExtensions.Select(e => $"{c.Name}/{e.ExtensionName}"))
            .ToHashSet(StringComparer.Ordinal);

    /// <summary>
    /// The declared extensions whose install needs <c>shared_preload_libraries</c> and a server restart.
    /// The expensive half of the paragraph's advice: a <c>CREATE EXTENSION</c> is a statement, this is a
    /// maintenance window.
    /// </summary>
    private static IReadOnlySet<string> PreloadExtensions =>
        CollectorCatalog.All
            .SelectMany(c => c.RequiredPgExtensions)
            .Where(e => e.InstallKind == PgExtensionInstallKind.SharedPreloadLibraries)
            .Select(e => e.ExtensionName)
            .ToHashSet(StringComparer.Ordinal);

    /// <summary>
    /// The declared extensions that have to be created in EVERY database rather than only the connect
    /// database — derived from the collector's own <c>RunsPerDatabase</c> rather than declared a second
    /// time, because a per-database read is what makes its extension per-database.
    ///
    /// <para>Reached by reflection, and that is the one weakness here: <c>RunsPerDatabase</c> is declared
    /// on <c>ICollectorDefinition&lt;TRow&gt;</c> rather than on the non-generic surface
    /// <see cref="CollectorCatalog.All"/> exposes, so it cannot be asked without the row type. A missing
    /// method is asserted rather than treated as false — a reflection lookup that silently stops finding
    /// anything would turn this pin into a claim that no extension is per-database.</para>
    /// </summary>
    private static IReadOnlySet<string> PerDatabaseExtensions
    {
        get
        {
            var found = new HashSet<string>(StringComparer.Ordinal);

            foreach (var definition in CollectorCatalog.All.Where(c => c.RequiredPgExtensions.Count > 0))
            {
                var method = definition.GetType().GetMethod(
                    "RunsPerDatabase", new[] { typeof(CollectorTargetInfo) });

                Assert.True(method is not null,
                    $"{definition.Name}: RunsPerDatabase(CollectorTargetInfo) was not found by reflection, so "
                    + "the per-database set cannot be derived and this pin would silently report the empty set.");

                if ((bool)method!.Invoke(definition, new object[] { SelfHostedPg17 })!)
                {
                    foreach (var extension in definition.RequiredPgExtensions)
                    {
                        found.Add(extension.ExtensionName);
                    }
                }
            }

            return found;
        }
    }

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

    /// <summary>
    /// The pinned NAME SETS — the same guard as <see cref="Pins"/> one resolution finer. A count going
    /// stale and a list going stale are the same defect, and the list is the one that shipped wrong: the
    /// permissions paragraph was once written naming four of the six collectors that need an extension,
    /// with every check green on that commit, because an enumeration is only better than a count if
    /// something breaks when it is incomplete.
    ///
    /// <para>Each entry captures ONE span of the README and turns it into a set, which is then required to
    /// equal what the catalog declares — in BOTH directions. Missing catches the defect that shipped;
    /// extra catches its mirror image, a paragraph claiming a dependency nothing has.</para>
    /// </summary>
    private static IEnumerable<(
        string Name,
        Regex Pattern,
        Func<IReadOnlySet<string>> Expected,
        Func<string, IReadOnlySet<string>> Extract)> SetPins()
    {
        /* `collector` (`extension`  /  `collector` (the `extension` module — the second shape is
           pg_wait_sampling, whose extension and collector share a name. The FIRST backticked name inside
           the parentheses is the extension, which is what lets an entry carry a trailing aside
           ("pg_stat_kcache, which sits on top of pg_stat_statements") without that aside reading as a
           second dependency. */
        var pair = new Regex(@"`([a-z0-9_]+)` \((?:the )?`([a-z0-9_]+)`");

        yield return (
            "the extension-dependent collector list",
            new Regex(@"The collectors that need something installed, named rather than counted: (.+?)\. Every other collector needs nothing installed"),
            () => DependencyPairs,
            span => pair.Matches(span)
                .Select(m => $"{m.Groups[1].Value}/{m.Groups[2].Value}")
                .ToHashSet(StringComparer.Ordinal));

        yield return (
            "the shared_preload_libraries sentence",
            new Regex(@"\*\*Needing `shared_preload_libraries` and a server restart\*\* \(a parameter-group change plus a reboot on Aurora/RDS\), and inert until then whatever else is installed: (.+?)\."),
            () => PreloadExtensions,
            BacktickedNames);

        yield return (
            "the per-database extension sentence",
            new Regex(@"\*\*Additionally per database\*\*, because the collectors that read them run per database and `CREATE EXTENSION` in one database does not create them in another: (.+?)\."),
            () => PerDatabaseExtensions,
            BacktickedNames);
    }

    private static IReadOnlySet<string> BacktickedNames(string span) =>
        Regex.Matches(span, @"`([a-z0-9_]+)`")
            .Select(m => m.Groups[1].Value)
            .ToHashSet(StringComparer.Ordinal);

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
    public void EveryPinnedNameSet_MatchesTheCatalog()
    {
        var readme = ReadReadme();

        foreach (var (name, pattern, expected, extract) in SetPins())
        {
            AssertSetPin(readme, name, pattern, expected(), extract);
        }
    }

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
    ///
    /// <para>Both families are proved here rather than only the numeric one. A set pin cannot be mutated by
    /// bumping a digit, so it is mutated by DROPPING a name — once per member, so that every name in the
    /// sentence is individually shown to be load-bearing. One mutation per pin would only prove the
    /// sentence is read at all, which is not the property that failed: the paragraph was read, and was
    /// short two names.</para>
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

        foreach (var (name, pattern, expected, extract) in SetPins())
        {
            var real = pattern.Match(readme);
            Assert.True(real.Success, $"{name}: pattern matched nothing, so its pin is vacuous.");

            var want = expected();
            Assert.NotEmpty(want);

            foreach (var member in want)
            {
                /* The token the README actually spells. A pair member is collector/extension and the README
                   spells both, so either half proves the point; the extension half is taken because it is
                   the half that went missing. */
                var slash = member.IndexOf('/', StringComparison.Ordinal);
                var token = slash < 0 ? member : member[(slash + 1)..];

                var span = real.Groups[1];
                var at = span.Value.IndexOf($"`{token}`", StringComparison.Ordinal);
                Assert.True(at >= 0,
                    $"{name}: `{token}` is not inside the pinned span, so dropping it would prove nothing "
                    + "about this pin. Either the sentence spells it differently or the span is wrong.");

                var mutated = readme
                    .Remove(span.Index + at, token.Length + 2)
                    .Insert(span.Index + at, "`zzz_drift_probe`");

                var after = pattern.Match(mutated);
                Assert.True(after.Success, $"{name}: the mutation broke the pattern, so this proves nothing.");

                Assert.False(extract(after.Groups[1].Value).Contains(member),
                    $"{name}: dropping `{token}` from the README still extracted {member}, so this pin does "
                    + "not actually depend on that name being there.");
            }
        }
    }

    /// <summary>
    /// One set pin, compared both ways. Missing is the defect that shipped — a dependency the product has
    /// and the documentation does not mention. Extra is its mirror image and just as wrong to publish.
    /// </summary>
    private static void AssertSetPin(
        string readme,
        string name,
        Regex pattern,
        IReadOnlySet<string> expected,
        Func<string, IReadOnlySet<string>> extract)
    {
        var match = pattern.Match(readme);

        Assert.True(match.Success,
            $"Darling/README.md no longer contains {name} in the pinned shape ({pattern}). It restates a set "
            + "the collector catalog declares, so keep it parseable — a reworded sentence has to fail here "
            + "rather than quietly stop being checked, which is the whole reason this file asserts the match "
            + "before it compares anything.");

        var actual = extract(match.Groups[1].Value);

        var missing = expected.Except(actual, StringComparer.Ordinal)
            .OrderBy(v => v, StringComparer.Ordinal).ToArray();
        var extra = actual.Except(expected, StringComparer.Ordinal)
            .OrderBy(v => v, StringComparer.Ordinal).ToArray();

        Assert.True(missing.Length == 0,
            $"{name}: the collector catalog declares {string.Join(", ", missing)}, which Darling/README.md "
            + "does not name. Add it to that sentence — a declared dependency the permissions section omits "
            + "is one nobody installing this product is told to install, and it fails as a non-fatal skip "
            + "that stores nothing rather than as an error.");

        Assert.True(extra.Length == 0,
            $"{name}: Darling/README.md names {string.Join(", ", extra)}, which no collector declares as a "
            + "dependency. Either remove it from the sentence, or declare it on the collector that needs it "
            + "(RequiredPgExtensions) — do not widen this sentence to match a list built for another "
            + "question, such as pg_extension_availability's reportable-absence roster.");
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
