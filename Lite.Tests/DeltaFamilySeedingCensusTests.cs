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
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3540 A4, the measurement contract's rule 7: <b>a host restart seeds EVERY delta family, keys and pass
/// window, and an unseeded family is a census failure.</b>
///
/// <para>The defect this keeps closed: the two seeders covered four of ten families for a year, and nothing
/// said so. Each family that was not seeded took the first-sighting path after every restart or deploy and
/// stored one interval of fabricated quiet per restart — and the per-group pass window that the #2235
/// series-age rescue reads was never seeded at all, so the rescue was inert on exactly the cycle it exists
/// for. Both were invisible to every test that exercised a seeded family, which is all of them.</para>
///
/// <para>The census is read from the two hosts' SOURCES rather than from their types, the way the twinned
/// pins in this project read a surface that exists once per SKU: Lite.Tests cannot reference the Darling
/// service assembly, and a census that enumerated Lite alone would certify half the product (a lesson this
/// campaign has already paid for once). What it asserts, per family in
/// <see cref="CollectorDeltaCalculator.DeltaFamilyCollectors"/> and per host that monitors it: a seed read
/// over the family's table; a delta-group list equal to the groups the collector actually passes; a
/// <c>SeedPasses</c> over that list; and a per-family guard, so one family's failure cannot cost the rest
/// their continuity. The one family that cannot be key-seeded is named, with the store fact that makes it
/// so asserted beside it, so the exemption dies the day the fact does.</para>
/// </summary>
public sealed class DeltaFamilySeedingCensusTests
{
    private const string LiteSeeder = "Lite/Services/DeltaCalculator.cs";
    private const string DarlingSeeder = "Darling/PerformanceMonitor.Darling.Service/DarlingDeltaCalculator.cs";
    private const string CollectorsDir = "PerformanceMonitor.Collectors";

    /// <summary>
    /// Families whose KEYS the store cannot reproduce, so the host seeds their pass window only. query_stats
    /// keys its deltas on <c>sql_handle:statement_start_offset:statement_end_offset:plan_handle</c> and the
    /// store persists neither offset — asserted below against the catalog, so a rung that adds them turns
    /// this exemption red and demands the key seed. Not a permanent exemption: a follow-up on #3540.
    /// </summary>
    private static readonly HashSet<string> PassWindowOnly = new(StringComparer.Ordinal) { "query_stats" };

    private static readonly Regex s_seedFamilyCall = new(@"SeedFamilyAsync\(\s*""([^""]+)""\s*,", RegexOptions.Compiled);
    private static readonly Regex s_seedSqlConst = new(@"public const string (\w+SeedSql) = @""([^""]*)"";", RegexOptions.Compiled | RegexOptions.Singleline);
    private static readonly Regex s_groupsArray = new(@"internal static readonly string\[\] (\w+Groups) =\s*\{([^}]*)\}", RegexOptions.Compiled | RegexOptions.Singleline);
    private static readonly Regex s_quoted = new(@"""([^""]+)""", RegexOptions.Compiled);
    private static readonly Regex s_collectorGroup = new(@"CalculateDelta\w*\(\s*context\.ServerId\s*,\s*""([^""]+)""", RegexOptions.Compiled);
    private static readonly Regex s_namePin = new(@"override string Name\s*=>\s*""([^""]+)""", RegexOptions.Compiled);

    private sealed record Host(string Label, string Source, Func<ICollectorSchemaInfo, bool> Monitors);

    private static IEnumerable<Host> Hosts()
    {
        /* Lite stores exactly the SQL Server collectors — DuckDbSchemaGenerator's own table set, which is
           why its DuckDB has no pg_* tables to seed from. Darling's central store carries every family. */
        var liteTables = DuckDbSchemaGenerator.CollectorTableNames().ToHashSet(StringComparer.Ordinal);
        yield return new Host("Lite", ReadRepoFile(LiteSeeder), d => liteTables.Contains(d.TargetTable));
        yield return new Host("Darling", ReadRepoFile(DarlingSeeder), _ => true);
    }

    private static IReadOnlyList<ICollectorSchemaInfo> Families() =>
        CollectorDeltaCalculator.DeltaFamilyCollectors
            .OrderBy(f => f, StringComparer.Ordinal)
            .Select(f =>
            {
                var d = CollectorCatalog.Find(f);
                Assert.True(d is not null, $"delta family '{f}' has no catalog definition — the census cannot inspect it");
                return d!;
            })
            .ToList();

    /// <summary>
    /// THE census. Every family a host monitors is routed through that host's per-family guard, has a seed
    /// read over its own table (or, for the named pass-window-only family, a pass read over it and NO key
    /// read), and nothing is routed that is not a family — both directions, so an eleventh family cannot
    /// ship unseeded and a retired one cannot linger.
    /// </summary>
    [Fact]
    public void EveryDeltaFamily_IsSeededByEveryHostThatMonitorsIt()
    {
        var families = Families();
        Assert.Equal(10, families.Count); /* the floor that makes the equalities below mean something */

        foreach (var host in Hosts())
        {
            var monitored = families.Where(host.Monitors).Select(d => d.Name).OrderBy(n => n, StringComparer.Ordinal).ToList();
            Assert.NotEmpty(monitored);

            var routed = s_seedFamilyCall.Matches(host.Source).Select(m => m.Groups[1].Value).OrderBy(n => n, StringComparer.Ordinal).ToList();
            Assert.True(
                monitored.SequenceEqual(routed, StringComparer.Ordinal),
                $"{host.Label}: the families routed through SeedFamilyAsync are [{string.Join(", ", routed)}] but the delta families it monitors are [{string.Join(", ", monitored)}] — an unseeded family fabricates one interval of quiet after every restart (#3540 A4)");

            var consts = s_seedSqlConst.Matches(host.Source).ToDictionary(m => m.Groups[1].Value, m => m.Groups[2].Value, StringComparer.Ordinal);

            foreach (var family in families.Where(host.Monitors))
            {
                var pascal = Pascal(family.Name);

                if (PassWindowOnly.Contains(family.Name))
                {
                    Assert.True(consts.TryGetValue(pascal + "PassSeedSql", out var passSql),
                        $"{host.Label}: '{family.Name}' is pass-window-only and must declare {pascal}PassSeedSql");
                    Assert.Contains($"FROM {family.TargetTable}", passSql, StringComparison.Ordinal);
                    Assert.Contains("GROUP BY server_id, collection_time", passSql, StringComparison.Ordinal);
                    Assert.False(consts.ContainsKey(pascal + "SeedSql"),
                        $"{host.Label}: '{family.Name}' now has a key seed — remove it from PassWindowOnly so the census describes the product");
                    continue;
                }

                Assert.True(consts.TryGetValue(pascal + "SeedSql", out var sql),
                    $"{host.Label}: delta family '{family.Name}' has no {pascal}SeedSql — its keys are not restored on restart");
                Assert.Contains($"FROM {family.TargetTable}", sql, StringComparison.Ordinal);
                /* Every seed read is bounded to the cutoff — the #1772 rule, in whichever shape. */
                Assert.Contains("collection_time >= $1", sql, StringComparison.Ordinal);
                /* And selects the timestamp, so the gap policy can judge the restored baseline. */
                Assert.Contains("collection_time", sql[..sql.IndexOf("FROM", StringComparison.Ordinal)], StringComparison.Ordinal);
            }
        }
    }

    /// <summary>
    /// The delta GROUP names a host seeds are exactly the ones the family's collector passes as
    /// <c>collectorName</c> — read from the collector's own <c>CalculateDelta*(context.ServerId, "…"</c>
    /// calls. The pass window is keyed by these strings, so a seeder that spelled one differently, or
    /// missed one, would arm nothing for it and nothing would say so.
    /// </summary>
    [Fact]
    public void EveryHostsGroupList_EqualsTheGroupsItsCollectorActuallyPasses()
    {
        var collectorGroups = CollectorGroupsByFamily();

        foreach (var host in Hosts())
        {
            var arrays = s_groupsArray.Matches(host.Source).ToDictionary(
                m => m.Groups[1].Value,
                m => s_quoted.Matches(m.Groups[2].Value).Select(q => q.Groups[1].Value).OrderBy(g => g, StringComparer.Ordinal).ToList(),
                StringComparer.Ordinal);

            foreach (var family in Families().Where(host.Monitors))
            {
                var arrayName = Pascal(family.Name) + "Groups";
                Assert.True(arrays.TryGetValue(arrayName, out var seeded), $"{host.Label}: no {arrayName} array for '{family.Name}'");
                Assert.True(collectorGroups.TryGetValue(family.Name, out var passed), $"no collector source passes deltas for '{family.Name}'");

                Assert.True(
                    passed!.SequenceEqual(seeded!, StringComparer.Ordinal),
                    $"{host.Label}: {arrayName} is [{string.Join(", ", seeded!)}] but {family.Name}'s collector passes [{string.Join(", ", passed)}]");

                /* The list is USED: handed to SeedPasses, and (for key-seeded families) every group is the
                   collectorName of a Seed(...) call, so the array is a contract and not a comment. */
                Assert.Contains($"SeedPasses(passes, {arrayName});", host.Source, StringComparison.Ordinal);
                if (!PassWindowOnly.Contains(family.Name))
                {
                    foreach (var group in seeded!)
                    {
                        Assert.Matches(new Regex(@"Seed\(serverId,\s*""" + Regex.Escape(group) + @""""), host.Source);
                    }
                }
            }

            /* No group list for a family the host does not monitor, and none for a non-family. */
            var expectedArrays = Families().Where(host.Monitors).Select(f => Pascal(f.Name) + "Groups").OrderBy(n => n, StringComparer.Ordinal);
            Assert.Equal(expectedArrays, arrays.Keys.OrderBy(n => n, StringComparer.Ordinal));
        }
    }

    /// <summary>
    /// The two hosts' seed reads for the families they share are byte-identical — the "mirrored verbatim"
    /// claim both files make, which is what lets each side's shape pins stand for the other.
    /// </summary>
    [Fact]
    public void TheSharedSeedReads_AreByteIdenticalAcrossTheTwoHosts()
    {
        var lite = s_seedSqlConst.Matches(ReadRepoFile(LiteSeeder)).ToDictionary(m => m.Groups[1].Value, m => m.Groups[2].Value, StringComparer.Ordinal);
        var darling = s_seedSqlConst.Matches(ReadRepoFile(DarlingSeeder)).ToDictionary(m => m.Groups[1].Value, m => m.Groups[2].Value, StringComparer.Ordinal);

        var shared = lite.Keys.Intersect(darling.Keys, StringComparer.Ordinal).OrderBy(k => k, StringComparer.Ordinal).ToList();
        Assert.Equal(8, shared.Count); /* the six SQL Server key seeds, the memory-grant seed and the query_stats pass seed */

        foreach (var name in shared)
        {
            Assert.True(string.Equals(lite[name], darling[name], StringComparison.Ordinal), $"{name} differs between the two hosts");
        }

        /* Everything Lite has, Darling has; Darling's extras are exactly the PostgreSQL pair. */
        Assert.Empty(lite.Keys.Except(darling.Keys, StringComparer.Ordinal));
        Assert.Equal(new[] { "PgStatementStatsSeedSql", "PgWaitStatsSeedSql" }, darling.Keys.Except(lite.Keys, StringComparer.Ordinal).OrderBy(k => k, StringComparer.Ordinal));
    }

    /// <summary>
    /// The per-family guard exists and catches: a family whose read throws logs and yields to the next,
    /// rather than costing every later family its continuity the way the single try did before #3540.
    /// </summary>
    [Fact]
    public void EachHost_GuardsEveryFamilyIndividually()
    {
        foreach (var host in Hosts())
        {
            var guard = Regex.Match(host.Source, @"Task SeedFamilyAsync\([^)]*\)\s*\{(.*?)\n    \}", RegexOptions.Singleline);
            Assert.True(guard.Success, $"{host.Label}: no SeedFamilyAsync guard");
            Assert.Contains("catch (Exception ex)", guard.Groups[1].Value, StringComparison.Ordinal);
            Assert.Contains("LogWarning(ex,", guard.Groups[1].Value, StringComparison.Ordinal);
            Assert.Contains("{Family}", guard.Groups[1].Value, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The pass-window-only exemption rests on a store fact: query_stats persists neither statement offset.
    /// Asserted against the catalog so the day a rung adds them, this fails and the key seed is owed.
    /// </summary>
    [Fact]
    public void ThePassWindowOnlyExemption_RestsOnTheOffsetsNotBeingStored()
    {
        Assert.All(PassWindowOnly, family => Assert.Contains(family, CollectorDeltaCalculator.DeltaFamilyCollectors));

        var queryStats = CollectorCatalog.Find("query_stats")!;
        Assert.DoesNotContain(queryStats.PayloadColumns, c => c.Name == "statement_start_offset");
        Assert.DoesNotContain(queryStats.PayloadColumns, c => c.Name == "statement_end_offset");

        /* And the collector really does key on them, so the exemption is about THIS key and not a guess. */
        var source = ReadRepoFile(CollectorsDir + "/QueryStatsCollector.cs");
        Assert.Contains("$\"{row.SqlHandle}:{row.StatementStartOffset}:{row.StatementEndOffset}:{row.PlanHandle}\"", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The twin of DarlingWorker's reconcile-remove wire: Lite's one deep-cleanup for a departing server
    /// drops the delta baselines and pass window too, so a remove-and-re-add inside the gap policy's hour
    /// starts from a first pass rather than subtracting a different server's counters.
    /// </summary>
    [Fact]
    public void BothHosts_ClearTheDeltaCacheWhenAServerLeavesMonitoring()
    {
        var lite = ReadRepoFile("Lite/MainWindow.xaml.cs");
        var forget = lite[lite.IndexOf("private async Task ForgetServerRuntimeStateAsync(", StringComparison.Ordinal)..];
        forget = forget[..forget.IndexOf("\n    }", StringComparison.Ordinal)];
        Assert.Contains("_collectorService?.DeltaCalculator?.ClearServer(removedServerId);", forget, StringComparison.Ordinal);

        var worker = ReadRepoFile("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs");
        var reconcile = worker[worker.IndexOf("private void ReconcileServers(", StringComparison.Ordinal)..];
        reconcile = reconcile[..reconcile.IndexOf("\n    }", StringComparison.Ordinal)];
        Assert.Contains("_selfAlerts?.Forget(id);", reconcile, StringComparison.Ordinal);
        Assert.Contains("_deltas?.ClearServer(id);", reconcile, StringComparison.Ordinal);
    }

    /* ---------------- helpers ---------------- */

    /// <summary>The delta groups each collector passes, read from the collector sources under the shared
    /// project and keyed by the collector's <c>Name</c> pin.</summary>
    private static Dictionary<string, List<string>> CollectorGroupsByFamily()
    {
        var dir = Path.Combine(RepoRoot(), CollectorsDir);
        var result = new Dictionary<string, List<string>>(StringComparer.Ordinal);

        foreach (var file in Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories))
        {
            var source = File.ReadAllText(file);
            var groups = s_collectorGroup.Matches(source).Select(m => m.Groups[1].Value).Distinct(StringComparer.Ordinal).OrderBy(g => g, StringComparer.Ordinal).ToList();
            if (groups.Count == 0)
            {
                continue;
            }

            var name = s_namePin.Match(source);
            Assert.True(name.Success, $"{Path.GetFileName(file)} passes deltas but has no Name => \"...\" pin");
            result[name.Groups[1].Value] = groups;
        }

        Assert.True(result.Count > 0, "no collector passes deltas — the census regex is broken");
        return result;
    }

    /// <summary><c>pg_statement_stats</c> → <c>PgStatementStats</c>: the naming both hosts use for their constants.</summary>
    private static string Pascal(string snake) =>
        string.Concat(snake.Split('_').Select(p => char.ToUpperInvariant(p[0]) + p[1..]));

    /// <summary>Reads a repo file with CRLF normalised, so multi-line regexes here read the same on every
    /// checkout. The root is the tracked solution file, a marker that survives a git worktree (whose
    /// <c>.git</c> is a file) — the RepoFile idiom Darling.Tests consolidated on.</summary>
    private static string ReadRepoFile(string relative) =>
        File.ReadAllText(Path.Combine(RepoRoot(), relative.Replace('/', Path.DirectorySeparatorChar))).Replace("\r\n", "\n", StringComparison.Ordinal);

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile);
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.True(dir is not null, $"PerformanceMonitor.sln not found walking up from {thisFile}");
        return dir!;
    }
}
