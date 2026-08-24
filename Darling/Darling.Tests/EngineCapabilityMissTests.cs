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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2511, the derivation half: whether a collector can EVER run on an engine edition, answered by the
/// collectors' OWN gates rather than by a list somebody typed.
///
/// <para>The assertions here are deliberately about PROPERTIES of the derivation, not an enumeration of
/// today's gaps. An enumerated "these twelve are missing on Azure SQL DB" would be a second copy of the
/// gates — it would conflict with any PR that changes one (#2512 is open against
/// <c>TempDbStatsCollector</c>'s), and its failure would say "update the list" rather than "look at the
/// gate". What is worth pinning is that the derivation distinguishes an ENGINE gap from the fixable facts
/// that are not one.</para>
///
/// <para><b>What none of it proves, and where that lives now.</b> Every gate this class can reach is fixed at
/// test time, so every assertion here would hold just as well against a hard-coded set of gaps that happened
/// to match today's gates — measured, not assumed: swapping the sweep for exactly such a list leaves this
/// entire class green. <c>CollectorEngineCapabilityMovingGateTests</c> is the half that MOVES a gate and
/// watches the answer move with it (#2518).</para>
/// </summary>
public sealed class CollectorEngineCapabilityTests
{
    private const int Enterprise = 3;
    private const int AzureSqlDb = CollectorEngineCapability.AzureSqlDatabaseEngineEdition;
    private const int AzureMi = CollectorEngineCapability.AzureManagedInstanceEngineEdition;

    /// <summary>Every SQL Server collector that no target of this engine edition can run.</summary>
    private static string[] GapsOn(int engineEdition) => CollectorCatalog.All
        .Where(c => c.TargetEngine == CollectorTargetEngine.SqlServer)
        .Select(c => c.Name)
        .Where(name => !CollectorEngineCapability.IsCollectedOnEngineEdition(name, engineEdition))
        .OrderBy(n => n, StringComparer.Ordinal)
        .ToArray();

    /// <summary>
    /// The measurement the issue was filed on: <c>sys.dm_xe_sessions</c> does not exist on Azure SQL
    /// Database, so <c>system_health</c> can never be read there — and it reads fine everywhere else.
    /// </summary>
    [Fact]
    public void SystemHealth_IsAPermanentGapOnAzureSqlDb_AndOnNothingElse()
    {
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineEdition("system_health_events", AzureSqlDb));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition("system_health_events", Enterprise));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition("system_health_events", AzureMi));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition("system_health_events", 2));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition("system_health_events", 4));
    }

    /// <summary>
    /// A gate that reads a FIXABLE fact is not an engine gap. <c>running_jobs</c> and <c>agent_status</c>
    /// need a non-RDS host; on a box edition that is something an operator can change, so the honest answer
    /// there is "collected" and the read keeps its <c>unavailable</c> vocabulary, which is what sends
    /// someone to look. Only the Azure SQL DB half of those same gates is permanent.
    ///
    /// <para>#2559 removed the most fixable fact of all from these gates: msdb access is a GRANT, and
    /// gating on it meant the grant did not take effect until the connection was rebuilt. All three
    /// collectors now attempt regardless and fail into PERMISSIONS. They are still asserted here because
    /// what this test protects is unchanged — none of the three may be reported as a permanent engine gap
    /// on a box edition.</para>
    ///
    /// <para>This is the assertion that would fail if the sweep stopped varying a dimension: a sweep fixed
    /// at <c>IsAwsRds = true</c> would report these as permanent engine gaps on an ordinary Enterprise box
    /// and tell an operator to stop looking at a problem they could have fixed in a minute.</para>
    /// </summary>
    [Fact]
    public void AFixableGate_IsNotReportedAsAnEngineGap()
    {
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition("job_history", Enterprise));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition("running_jobs", Enterprise));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition("agent_status", Enterprise));

        Assert.False(CollectorEngineCapability.IsCollectedOnEngineEdition("job_history", AzureSqlDb));
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineEdition("running_jobs", AzureSqlDb));
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineEdition("agent_status", AzureSqlDb));
    }

    /// <summary>
    /// A VERSION floor is not an engine gap either, and the three collectors that carry one all name Azure
    /// SQL DB as an explicit escape from it — so Azure SQL DB collects them. The issue listed these among
    /// the "conditional gates" as though they were Azure gaps; they are the opposite, and a capability
    /// helper that got this backwards would tell an Azure user their Query Store health is unobtainable.
    /// </summary>
    [Fact]
    public void AVersionFloor_IsNotAnEngineGap_AndAzureSqlDbEscapesTheThreeThatHaveOne()
    {
        foreach (var name in new[] { "query_store_health", "plan_correction", "pvs_stats", "query_stats", "query_store" })
        {
            Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(name, AzureSqlDb), name);
            Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(name, Enterprise), name);
            Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition(name, 12), name);
        }
    }

    /// <summary>
    /// The box editions and Managed Instance have NO permanent gaps: every gate in the library that names
    /// Azure names Azure SQL DATABASE specifically. Stated as a derived zero rather than as prose, so the day
    /// a gate starts excluding Managed Instance this says so instead of a message quietly never appearing.
    /// </summary>
    [Fact]
    public void BoxEditionsAndManagedInstance_HaveNoPermanentGaps()
    {
        Assert.Empty(GapsOn(Enterprise));
        Assert.Empty(GapsOn(2));
        Assert.Empty(GapsOn(4));
        Assert.Empty(GapsOn(AzureMi));
    }

    /// <summary>A non-vacuity floor: the sweep must actually be finding gaps on Azure SQL DB, or every
    /// assertion above about "not a gap" passes for free.</summary>
    [Fact]
    public void AzureSqlDb_HasRealGaps_SoTheOtherAssertionsAreNotVacuous()
    {
        var gaps = GapsOn(AzureSqlDb);
        Assert.Contains("system_health_events", gaps);
        Assert.True(gaps.Length >= 10, "expected the Azure SQL DB gap set to be substantial, got: " + string.Join(", ", gaps));
    }

    /// <summary>
    /// "We do not know" must never render as "this will never work". A server that has not completed a
    /// connect — and every PostgreSQL target, whose connector stamps 0 because <c>SERVERPROPERTY</c> does not
    /// exist there — makes no claim at all.
    /// </summary>
    [Fact]
    public void UnknownEngineEdition_MakesNoClaim()
    {
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition("system_health_events", CollectorEngineCapability.UnknownEngineEdition));
        Assert.Null(CollectorEngineCapability.NotCollectedMessage("srv", 0, engineKind: null, "system_health_events"));
    }

    /// <summary>
    /// A PostgreSQL collector asked about a SQL Server edition is not a gap — the question does not apply.
    /// Without this, a Postgres read that ever passed its own collector name alongside a non-zero edition
    /// would manufacture a confident claim about an engine it does not run on.
    /// </summary>
    [Fact]
    public void APostgresCollector_IsNeverAGapOnASqlServerEdition()
    {
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition("pg_wait_stats", Enterprise));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition("pg_blocking", AzureSqlDb));
    }

    /// <summary>An unknown name follows <c>CollectorCatalog</c>'s own true-on-miss default: a typo must not
    /// manufacture a permanent-gap claim. <see cref="EngineCapabilityReadWiringTests"/> is what stops that
    /// default hiding a mis-wired read.</summary>
    [Fact]
    public void AnUnknownCollectorName_MakesNoClaim()
    {
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineEdition("no_such_collector", AzureSqlDb));
        Assert.Null(CollectorEngineCapability.NotCollectedMessage("srv", AzureSqlDb, engineKind: null, "no_such_collector"));
    }

    /// <summary>
    /// The message names the server, the engine (by name AND by number, so an operator can match it to
    /// <c>SERVERPROPERTY</c>), and the collector; and it says the gap is permanent rather than pending.
    /// </summary>
    [Fact]
    public void TheMessage_NamesTheEngineAndRefusesToSendAnyoneLooking()
    {
        var message = CollectorEngineCapability.NotCollectedMessage("azure-db-01", AzureSqlDb, MonitoredEngineKind.SqlServer, "system_health_events");
        Assert.NotNull(message);
        Assert.Contains("azure-db-01", message, StringComparison.Ordinal);
        Assert.Contains("Azure SQL Database", message, StringComparison.Ordinal);
        Assert.Contains("EngineEdition 5", message, StringComparison.Ordinal);
        Assert.Contains("system_health_events", message, StringComparison.Ordinal);
        Assert.Contains("the system_health extended-events ring buffer", message, StringComparison.Ordinal);
        Assert.Contains("and never will.", message, StringComparison.Ordinal);

        /* The words the OLD message used are the ones that sent an Azure operator hunting. */
        Assert.DoesNotContain("check that collection is running", message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Darling's connector keeps ONE edition table, the shared one. The existing
    /// <c>DarlingCliCommandsTests.DescribeEngineEdition_MapsKnownEditions</c> pins the strings; this pins
    /// that the two entry points are the same function rather than two switches that happen to agree today.
    /// </summary>
    [Fact]
    public void TheConnectorAndTheCapabilityHelper_ShareOneEditionTable()
    {
        foreach (var edition in new[] { 1, 2, 3, 4, 5, 6, 8, 9, 11, 999 })
        {
            Assert.Equal(
                CollectorEngineCapability.DescribeEngineEdition(edition),
                DarlingServerConnector.DescribeEngineEdition(edition));
        }
    }

    /// <summary>
    /// The prose table cannot outlive the gates it describes: every key must be a real collector, and every
    /// key must be a collector its OWN <c>AppliesTo</c> gate shuts out somewhere — an entry for a collector
    /// nothing gates is prose no message can ever reach, which is how a stale explanation survives unnoticed.
    ///
    /// <para><b>"Its own gate", not "any gap at all" (#2532).</b> Since the kind axis landed, EVERY collector
    /// is a permanent gap on some engine the store can record — a SQL Server one on both PostgreSQL tokens,
    /// a PostgreSQL one on <c>sqlserver</c> — purely because of its DIALECT. So a check phrased as "is this a
    /// gap somewhere" would now pass for every name anyone typed, which is the guard-that-stopped-guarding
    /// shape. The dialect half is excluded deliberately, leaving exactly the original claim: the entry
    /// describes a collector whose own precondition excludes it on some target it could otherwise be sent
    /// at. That covers the twelve Azure SQL Database entries unchanged and the two Aurora-only PostgreSQL
    /// ones on the same terms.</para>
    /// </summary>
    [Fact]
    public void EveryCapturePathEntry_NamesARealCollectorThatIsActuallyGatedSomewhere()
    {
        var catalog = CollectorCatalog.All.ToDictionary(c => c.Name, StringComparer.Ordinal);
        var wired = EngineCapabilityReadWiringTests.AllWiredCollectors();

        Assert.NotEmpty(wired);

        foreach (var name in CollectorEngineCapability.CapturePathByCollector.Keys)
        {
            Assert.True(catalog.ContainsKey(name), $"CapturePathByCollector names '{name}', which is not a CollectorCatalog collector");
            Assert.True(
                ExcludedByItsOwnGateSomewhere(catalog[name]) || wired.Contains(name),
                $"CapturePathByCollector describes '{name}', but its AppliesTo gate now lets it run on every target " +
                "of its own engine AND no shipped read asks the capability question about it — so the prose is " +
                "unreachable. Drop the entry, re-check the gate, or wire the read that wanted it");
        }
    }

    /// <summary>
    /// True when a definition's own <c>AppliesTo</c> — not the dispatch gate's engine half — excludes it from
    /// every target of some engine the store can record. Derived from the two axes rather than listed, and
    /// deliberately asked only about engines this definition's DIALECT matches, so a foreign-dialect gap
    /// (which every collector has, on every other engine) cannot answer it.
    /// </summary>
    private static bool ExcludedByItsOwnGateSomewhere(ICollectorSchemaInfo definition)
    {
        if (definition.TargetEngine == CollectorTargetEngine.SqlServer
            && !CollectorEngineCapability.IsCollectedOnEngineEdition(definition, AzureSqlDb))
        {
            return true;
        }

        return MonitoredEngineKind.All
            .Where(kind => MonitoredEngineKind.EngineOf(kind) == definition.TargetEngine)
            .Any(kind => !CollectorEngineCapability.IsCollectedOnEngineKind(definition, kind));
    }

    /// <summary>
    /// The sweep must actually vary the dimensions the gates read. A sweep that quietly collapsed to one
    /// target shape would still answer correctly for today's Azure gates and would start over-claiming the
    /// moment a version- or msdb-shaped gate mattered, which is the failure
    /// <see cref="AFixableGate_IsNotReportedAsAnEngineGap"/> would then report from a distance.
    /// <para>This is the FORWARD direction only, and it is a hand-written list: it says the sweep varies
    /// these dimensions, not that these dimensions are everything the gates read. The converse lives in
    /// <c>CollectorEngineCapabilitySweepDimensionTests</c>, which derives both halves — the facts read out of
    /// each gate's IL, the facts varied out of the sweep's own output — so a gate written on an unswept fact
    /// fails the build instead of silently manufacturing a permanent gap (#2518).</para>
    /// </summary>
    [Fact]
    public void TheSweep_VariesEveryDimensionTheGatesRead()
    {
        var targets = CollectorEngineCapability.TargetsWithEngineEdition(Enterprise).ToArray();

        Assert.True(targets.Length >= 20, $"only {targets.Length} target shapes swept");
        Assert.All(targets, t => Assert.False(t.IsAzureSqlDb));
        Assert.All(targets, t => Assert.False(t.IsAzureManagedInstance));
        Assert.All(targets, t => Assert.Equal(CollectorTargetEngine.SqlServer, t.Engine));

        Assert.Contains(targets, t => t.SqlMajorVersion == 0);
        Assert.Contains(targets, t => t.SqlMajorVersion == 13);
        Assert.Contains(targets, t => t.SqlMajorVersion >= 17);
        Assert.Contains(targets, t => t.HasMsdbAccess);
        Assert.Contains(targets, t => !t.HasMsdbAccess);
        Assert.Contains(targets, t => t.IsAwsRds);
        Assert.Contains(targets, t => !t.IsAwsRds);

        /* The Azure flags follow the edition rather than the sweep — they are what the probe derives from it. */
        Assert.All(
            CollectorEngineCapability.TargetsWithEngineEdition(AzureSqlDb),
            t => Assert.True(t.IsAzureSqlDb && !t.IsAzureManagedInstance));
    }
}

/// <summary>
/// #2511, the wiring half: which READS ask the capability question, and whether the two SKUs ask it the
/// same way. Read off both shipped MCP trees rather than from a list here — a transcribed list of wired
/// reads would go stale in the direction that makes it pass.
///
/// <para><b>Why a source scan.</b> The failure mode is not a wrong answer, it is a read that never asks:
/// it compiles, every other test passes, and an Azure caller keeps getting the old message. Nothing
/// behavioural sees that on the SKU that was not touched, which is exactly how the divergence
/// <c>McpMissMessageParityPinTests</c> exists for got there.</para>
/// </summary>
public sealed class EngineCapabilityReadWiringTests
{
    private const string DarlingMcp = "Darling/PerformanceMonitor.Darling.Service/Mcp";
    private const string LiteMcp = "Lite/Mcp";

    private static readonly Regex ToolMark = new(@"McpServerTool\(Name = ""([a-z_0-9]+)""", RegexOptions.Compiled);

    /* The last argument of a NotCollectedStatusAsync call: a quoted collector name, or a const that names
       one. No argument in these calls contains a parenthesis, so the non-greedy [^)]* is safe. */
    private static readonly Regex WiringCall = new(
        @"NotCollectedStatusAsync\([^)]*?,\s*(?:""([a-z_0-9]+)""|([A-Za-z_][A-Za-z0-9_]*))\)",
        RegexOptions.Compiled);

    private static readonly Regex CollectorConst = new(
        @"private const string (\w+) = ""([a-z_0-9]+)"";", RegexOptions.Compiled);

    /// <summary>
    /// Every collector name a shipped read asks the capability question about, across both SKUs. Exposed so
    /// <see cref="CollectorEngineCapabilityTests.EveryCapturePathEntry_NamesARealCollectorThatIsActuallyGatedSomewhere"/>
    /// can hold the prose table to the reads, rather than the reads being the only side that is checked.
    /// </summary>
    internal static SortedSet<string> AllWiredCollectors()
    {
        var wired = new SortedSet<string>(StringComparer.Ordinal);

        foreach (var directory in new[] { DarlingMcp, LiteMcp })
        {
            foreach (var collectors in WiredReads(directory).Values)
            {
                wired.UnionWith(collectors);
            }
        }

        return wired;
    }

    /// <summary>tool name → the collector names its miss path asks the capability question about.</summary>
    internal static SortedDictionary<string, SortedSet<string>> WiredReads(string mcpDirectory)
    {
        var wired = new SortedDictionary<string, SortedSet<string>>(StringComparer.Ordinal);

        foreach (var file in RepoFilesIn(mcpDirectory))
        {
            var source = File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal);
            var consts = CollectorConst.Matches(source)
                .ToDictionary(m => m.Groups[1].Value, m => m.Groups[2].Value, StringComparer.Ordinal);
            var marks = ToolMark.Matches(source);

            foreach (Match call in WiringCall.Matches(source))
            {
                var collector = call.Groups[1].Success
                    ? call.Groups[1].Value
                    : consts.TryGetValue(call.Groups[2].Value, out var resolved) ? resolved : call.Groups[2].Value;

                /* The enclosing tool is the last McpServerTool mark before the call. */
                var owner = marks.Where(m => m.Index < call.Index).LastOrDefault();
                Assert.True(owner is not null, $"{Path.GetFileName(file)}: a capability call sits outside any MCP tool");

                if (!wired.TryGetValue(owner!.Groups[1].Value, out var collectors))
                {
                    wired[owner.Groups[1].Value] = collectors = new SortedSet<string>(StringComparer.Ordinal);
                }

                collectors.Add(collector);
            }
        }

        return wired;
    }

    private static HashSet<string> ToolsIn(string mcpDirectory)
    {
        var tools = new HashSet<string>(StringComparer.Ordinal);
        foreach (var file in RepoFilesIn(mcpDirectory))
        {
            foreach (Match mark in ToolMark.Matches(File.ReadAllText(file)))
            {
                tools.Add(mark.Groups[1].Value);
            }
        }

        return tools;
    }

    [Theory]
    [InlineData(DarlingMcp)]
    [InlineData(LiteMcp)]
    public void EveryWiredRead_NamesARealCollectorWhoseCapabilityBranchCanFire(string mcpDirectory)
    {
        var wired = WiredReads(mcpDirectory);
        var catalog = CollectorCatalog.All.Select(c => c.Name).ToHashSet(StringComparer.Ordinal);

        /* A scan that parsed nothing passes for free — the worst outcome a check like this can have. */
        Assert.True(wired.Count >= 17, $"only {wired.Count} wired reads found under {mcpDirectory} — the scan is broken, not the surface");

        foreach (var (tool, collectors) in wired)
        {
            foreach (var collector in collectors)
            {
                Assert.True(
                    catalog.Contains(collector),
                    $"{tool} asks the capability question about '{collector}', which is not a CollectorCatalog name — " +
                    "an unknown name answers \"supported\" and silently restores the message this change removed");

                Assert.True(
                    CollectorEngineCapability.CapturePathByCollector.ContainsKey(collector),
                    $"{tool} names '{collector}', which has no CapturePathByCollector entry — its message would fall back " +
                    "to the generic phrasing; add the noun phrase next to the others");

                Assert.True(
                    ReachableOnSomeEngine(collector),
                    $"{tool} asks about '{collector}', but no engine the store can record produces a message for it, so " +
                    "the capability branch is dead code — either a gate opened up and this read should stop asking, or " +
                    "a gate changed by accident");
            }
        }
    }

    /// <summary>
    /// True when SOME (engine kind, engine edition) pair the store can actually hold makes
    /// <see cref="CollectorEngineCapability.NotCollectedMessage"/> speak about this collector — i.e. the
    /// branch the read just added is reachable rather than dead.
    ///
    /// <para><b>Weaker than the Azure-specific check it replaced, and deliberately so (#2532).</b> That check
    /// said "this collector is gated off on Azure SQL Database", which was the right claim while the EDITION
    /// axis was the only one: a read wired to a collector that runs on every SQL Server had nothing to say
    /// and the assertion caught it. The KIND axis changes that — every SQL Server collector is a permanent
    /// gap on a PostgreSQL target and every PostgreSQL collector on a SQL Server one — so a read wired to
    /// <c>wait_stats</c> now has something true and useful to say, and an assertion that still demanded an
    /// Azure gap would refuse the wiring this issue is about. What is left is the property that actually
    /// matters at a call site: the branch can fire. The sharper per-collector claims live where they belong,
    /// in the derivation tests, over the gates rather than over the reads.</para>
    /// </summary>
    private static bool ReachableOnSomeEngine(string collectorName)
    {
        var kinds = new string?[] { null }.Concat(MonitoredEngineKind.All);
        var editions = new[]
        {
            CollectorEngineCapability.UnknownEngineEdition,
            3, /* Enterprise — a box edition, where nothing is an edition gap today */
            CollectorEngineCapability.AzureSqlDatabaseEngineEdition,
            CollectorEngineCapability.AzureManagedInstanceEngineEdition,
        };

        return kinds.Any(kind => editions.Any(edition =>
            CollectorEngineCapability.NotCollectedMessage("probe", edition, kind, collectorName) is not null));
    }

    /// <summary>
    /// The two SKUs wire the SAME reads to the SAME collectors. Compared only across tools BOTH surfaces
    /// expose, so a tool that exists on one SKU only (Darling's get_ag_health) is not reported as drift —
    /// and a tool that exists on both but is wired on one is, which is the drift that matters.
    /// </summary>
    [Fact]
    public void BothSkus_WireTheSameReadsToTheSameCollectors()
    {
        var darling = WiredReads(DarlingMcp);
        var lite = WiredReads(LiteMcp);
        var darlingTools = ToolsIn(DarlingMcp);
        var liteTools = ToolsIn(LiteMcp);

        var shared = darlingTools.Intersect(liteTools, StringComparer.Ordinal).ToArray();
        Assert.True(shared.Length >= 50, $"only {shared.Length} tools are common to both SKUs — the scan is broken");

        var drift = new List<string>();
        foreach (var tool in shared.OrderBy(t => t, StringComparer.Ordinal))
        {
            var d = darling.TryGetValue(tool, out var dc) ? string.Join("+", dc) : "";
            var l = lite.TryGetValue(tool, out var lc) ? string.Join("+", lc) : "";
            if (!string.Equals(d, l, StringComparison.Ordinal))
            {
                drift.Add($"{tool}: Darling=[{d}] Lite=[{l}]");
            }
        }

        Assert.True(
            drift.Count == 0,
            "these reads exist on both SKUs but do not ask the engine-capability question identically, so an " +
            "Azure caller is told different stories depending on which server they are pointed at: " +
            string.Join("; ", drift));
    }

    /// <summary>The nine health-parser reads — the family the issue was filed on — are all wired, on both
    /// SKUs. Named explicitly because they are the one set the live evidence covers.</summary>
    [Theory]
    [InlineData(DarlingMcp)]
    [InlineData(LiteMcp)]
    public void AllNineHealthParserReads_AskTheQuestion(string mcpDirectory)
    {
        var wired = WiredReads(mcpDirectory);
        var healthParser = wired.Keys.Where(k => k.StartsWith("get_health_parser_", StringComparison.Ordinal)).ToArray();

        Assert.Equal(9, healthParser.Length);
        Assert.All(healthParser, tool => Assert.Equal(new[] { "system_health_events" }, wired[tool].ToArray()));
    }

    private static string[] RepoFilesIn(string relativeDirectory, [CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relativeDirectory);
            if (Directory.Exists(candidate))
            {
                var files = Directory.GetFiles(candidate, "*.cs");
                Assert.NotEmpty(files);
                return files;
            }
        }

        Assert.Fail($"could not find {relativeDirectory} above {thisFile}");
        return Array.Empty<string>();
    }
}

/// <summary>
/// #2511 end to end against a live store: the SAME read, on servers that differ only in what the
/// registry records about their engine, must answer differently. Both directions, because a pin that only
/// asserts the Azure branch would pass just as well if the read had stopped distinguishing anything and
/// started saying <c>not_collected</c> to everyone.
///
/// <para>#2530 adds the second axis to the same demonstration: a PostgreSQL target differs from the box
/// only in <c>servers.engine_kind</c> — its <c>sql_engine_edition</c> is 0, the same value a server that
/// has never connected carries — and that one column is what turns "check that collection is running"
/// into the true answer.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class EngineCapabilityMissLivePostgresTests
{
    private const string AzureServerName = "darling-engine-cap-azure";
    private const string BoxServerName = "darling-engine-cap-box";
    private const string PostgresServerName = "darling-engine-cap-postgres";
    private const string UnprobedServerName = "darling-engine-cap-unprobed";

    private static readonly int AzureServerId = ServerIdHelper.GetDeterministicHashCode(AzureServerName);
    private static readonly int BoxServerId = ServerIdHelper.GetDeterministicHashCode(BoxServerName);
    private static readonly int PostgresServerId = ServerIdHelper.GetDeterministicHashCode(PostgresServerName);
    private static readonly int UnprobedServerId = ServerIdHelper.GetDeterministicHashCode(UnprobedServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheSameEmptyRead_AnswersNotCollectedOnAzureSqlDb_AndUnavailableOnABox()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live engine-capability test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterAsync(connection, ct, AzureServerId, AzureServerName, engineEdition: 5);
            await RegisterAsync(connection, ct, BoxServerId, BoxServerName, engineEdition: 3);

            /* Neither server has a single collected row: the ONLY difference is the engine edition. */

            /* ── Azure SQL Database: not_collected, naming the engine and the collector ── */
            var azureHealth = await DarlingMcpHealthParserTools.GetSystemHealth(postgres, AzureServerName);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(azureHealth));
            Assert.Contains("Azure SQL Database", azureHealth, StringComparison.Ordinal);
            Assert.Contains("system_health_events", azureHealth, StringComparison.Ordinal);
            Assert.Contains(AzureServerName, azureHealth, StringComparison.Ordinal);

            /* The never-captured branch of get_health_parser_significant_waits was the message quoted in the
               issue. It must no longer tell an Azure caller to start a session that cannot exist. */
            var azureWaits = await DarlingMcpHealthParserTools.GetSignificantWaits(postgres, AzureServerName);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(azureWaits));
            Assert.DoesNotContain("system_health session is started", azureWaits, StringComparison.Ordinal);

            /* A read from a different family, sharing nothing but the helper. */
            var azureFlags = await DarlingMcpConfigTools.GetTraceFlags(postgres, AzureServerName);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(azureFlags));
            Assert.Contains("trace_flags", azureFlags, StringComparison.Ordinal);

            /* A third family, and deliberately NOT get_tempdb_trend: #2512 measured the tempdb DMVs
               returning real data on Azure SQL Database (GP and Hyperscale), so #2516 opens that gate and
               tempdb_stats stops being a permanent gap. Picking it as the example here would tie this test
               to a gate that is moving. sys.configurations, DBCC TRACESTATUS and the default trace are
               absent from the engine itself, so their gates are the durable ones to demonstrate with. */
            var azureTrace = await DarlingMcpDefaultTraceTools.GetDefaultTraceEvents(postgres, AzureServerName);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(azureTrace));
            Assert.Contains("default_trace_events", azureTrace, StringComparison.Ordinal);

            /* ── An Enterprise box, same empty store: every one of them keeps its own miss ── */
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpHealthParserTools.GetSystemHealth(postgres, BoxServerName)));

            var boxWaits = await DarlingMcpHealthParserTools.GetSignificantWaits(postgres, BoxServerName);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(boxWaits));
            Assert.Contains("system_health session is started", boxWaits, StringComparison.Ordinal);

            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpConfigTools.GetTraceFlags(postgres, BoxServerName)));
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpDefaultTraceTools.GetDefaultTraceEvents(postgres, BoxServerName)));

            /* A read whose collector runs everywhere is untouched on BOTH servers — the helper must not have
               become a blanket "Azure gets not_collected" rule. */
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(await DarlingMcpConfigTools.GetDatabaseConfig(postgres, AzureServerName)));
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(await DarlingMcpConfigTools.GetDatabaseConfig(postgres, BoxServerName)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #2530, the engine-KIND axis through the real reads. Three servers with an identical (empty) store
    /// behind them, all three at engine edition 0 so the EDITION axis can say nothing about any of them:
    /// an Aurora PostgreSQL target, a stock PostgreSQL target, and a server that has simply never
    /// connected. The first two must answer <c>not_collected</c> naming their engine; the third must keep
    /// its old miss, because "we do not know" rendering as "this will never work" is the same defect
    /// wearing the fix's clothes.
    ///
    /// <para>Holding the edition at 0 across all three is what makes this a test of the new axis rather
    /// than of the old one: there is no other column in which these rows differ.</para>
    /// </summary>
    [Fact]
    public async Task APostgresTarget_AnswersNotCollectedNamingTheEngine_WhileAnUnprobedServerKeepsItsMiss()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live engine-capability test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            /* Edition 0 on ALL THREE — what a PostgreSQL connect stamps, and what an unconnected server
               carries. The engine kind is the only thing that differs. */
            await RegisterAsync(connection, ct, PostgresServerId, PostgresServerName, engineEdition: 0,
                engineKind: MonitoredEngineKind.AuroraPostgres);
            await RegisterAsync(connection, ct, BoxServerId, BoxServerName, engineEdition: 0,
                engineKind: MonitoredEngineKind.Postgres);
            await RegisterAsync(connection, ct, UnprobedServerId, UnprobedServerName, engineEdition: 0,
                engineKind: null);

            /* ── Aurora: not_collected, naming the engine, and NOT sending anyone to check collection ── */
            var auroraHealth = await DarlingMcpHealthParserTools.GetSystemHealth(postgres, PostgresServerName);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(auroraHealth));
            Assert.Contains("Aurora PostgreSQL", auroraHealth, StringComparison.Ordinal);
            Assert.Contains("system_health_events", auroraHealth, StringComparison.Ordinal);
            Assert.Contains(PostgresServerName, auroraHealth, StringComparison.Ordinal);

            /* The message the issue quoted. It must no longer point at a session that cannot exist on an
               engine that has no extended events at all. */
            var auroraWaits = await DarlingMcpHealthParserTools.GetSignificantWaits(postgres, PostgresServerName);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(auroraWaits));
            Assert.DoesNotContain("system_health session is started", auroraWaits, StringComparison.Ordinal);

            /* A read from a different family, sharing nothing with the one above but the helper. */
            var auroraServerConfig = await DarlingMcpConfigTools.GetServerConfig(postgres, PostgresServerName);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(auroraServerConfig));
            Assert.Contains("server_config", auroraServerConfig, StringComparison.Ordinal);
            Assert.Contains("Aurora PostgreSQL", auroraServerConfig, StringComparison.Ordinal);

            /* The limit this line used to assert is GONE (#2532). It read: only the reads #2511 wired to
               the capability helper can answer on either axis, so get_database_config — whose collector runs
               on every SQL Server, which gave the edition axis nothing to say — still reported the old
               `unavailable` on a PostgreSQL target. The kind axis makes EVERY SQL Server read a permanent
               gap there, and the wiring has now caught up, so the read that was the named example of the
               gap is the one asserted here.

               Kept as an explicit pair rather than deleted: this is the read the issue quoted, and a pin
               that only ever covered collectors #2511 had already wired could not tell the wiring apart
               from the twelve gates it started with. */
            var auroraDatabaseConfig = await DarlingMcpConfigTools.GetDatabaseConfig(postgres, PostgresServerName);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(auroraDatabaseConfig));
            Assert.Contains("database_config", auroraDatabaseConfig, StringComparison.Ordinal);
            Assert.Contains("Aurora PostgreSQL", auroraDatabaseConfig, StringComparison.Ordinal);
            Assert.DoesNotContain("may not have run yet", auroraDatabaseConfig, StringComparison.Ordinal);

            /* And a read from a family that has NOTHING to do with configuration, whose collector is gated
               off on no engine edition at all — so nothing but the kind axis could ever make it speak. */
            var auroraWaitStats = await DarlingMcpDataTools.GetWaitStats(postgres, PostgresServerName);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(auroraWaitStats));
            Assert.Contains("wait_stats", auroraWaitStats, StringComparison.Ordinal);
            Assert.Contains("Aurora PostgreSQL", auroraWaitStats, StringComparison.Ordinal);

            /* ── Stock PostgreSQL: the same answer, named for the engine it actually is ── */
            var stockFlags = await DarlingMcpConfigTools.GetTraceFlags(postgres, BoxServerName);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(stockFlags));
            Assert.Contains("trace_flags", stockFlags, StringComparison.Ordinal);
            Assert.Contains("runs PostgreSQL.", stockFlags, StringComparison.Ordinal);
            Assert.DoesNotContain("Aurora", stockFlags, StringComparison.Ordinal);

            /* ── And the server nobody has probed keeps every one of its old misses ── */
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpHealthParserTools.GetSystemHealth(postgres, UnprobedServerName)));
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpConfigTools.GetTraceFlags(postgres, UnprobedServerName)));

            var unprobedWaits = await DarlingMcpHealthParserTools.GetSignificantWaits(postgres, UnprobedServerName);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(unprobedWaits));
            Assert.Contains("system_health session is started", unprobedWaits, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// A registry row that never got an edition (a server that has not completed a connect) must keep the
    /// old miss. "We do not know" rendering as "this will never work" would be the same defect wearing the
    /// fix's clothes.
    /// </summary>
    [Fact]
    public async Task AServerWithNoProbedEdition_KeepsItsOldMiss()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live engine-capability test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await RegisterAsync(connection, ct, BoxServerId, BoxServerName, engineEdition: null);

            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpHealthParserTools.GetSystemHealth(postgres, BoxServerName)));
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpDefaultTraceTools.GetDefaultTraceEvents(postgres, BoxServerName)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>Column list copied from <c>DarlingMcpTestData.RegisterServerAsync</c>, plus the two columns
    /// this test exists to vary. <paramref name="engineKind"/> null is the pre-V82 row — no claim.</summary>
    private static async Task RegisterAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, int? engineEdition,
        string? engineKind = null)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, sql_major_version, engine_kind, created_date, modified_date)
VALUES ($1, $2, $3, TRUE, $4, 15, $5, $6, $6)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_engine_edition = EXCLUDED.sql_engine_edition, engine_kind = EXCLUDED.engine_kind;", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue((object?)engineEdition ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)engineKind ?? DBNull.Value);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM servers WHERE server_id IN ({AzureServerId}, {BoxServerId}, {PostgresServerId}, {UnprobedServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
