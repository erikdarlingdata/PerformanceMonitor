/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// THE forcing function for the collector gate-surface collapse (parity audit §01, Tier 0). Before this,
/// Lite gated in two places — the shared <see cref="ICollectorSchemaInfo.AppliesTo"/> AND its own
/// <c>IsCollectorSupported</c> — while Darling ran only <c>AppliesTo</c>, so every gate Lite kept in the
/// second layer (server_config/trace_flags on Azure SQL DB, the query_stats/query_store version gate, and
/// the running_jobs/job_history/agent_status msdb gate) was silently ignored by Darling. That layer is gone.
/// These pins assert:
/// <list type="number">
/// <item>each moved gate's <c>AppliesTo</c> truth table (Azure SQL DB / AWS RDS / no-msdb / pre-2016), so a
/// gate can't silently regress;</item>
/// <item>that the by-name surface Lite dispatches through (<see cref="CollectorCatalog.AppliesTo(string, CollectorTargetInfo)"/>)
/// agrees with the definition's own <c>AppliesTo</c> that Darling's runner calls — the proof both SKUs now
/// gate identically off ONE surface;</item>
/// <item>the <see cref="CollectorTargetInfo.HasMsdbAccess"/> default (unknown ⇒ assume access) and the
/// unknown-collector-name fall-through.</item>
/// <item>(round 2, from the #1500 review) that every collector name Lite's <c>RunCollectorAsync</c> dispatch
/// switch handles is a real <see cref="CollectorCatalog"/> name — because the pre-dispatch
/// <see cref="CollectorCatalog.AppliesTo(string, CollectorTargetInfo)"/> returns true-on-miss for an unknown
/// name, a switch string that drifted from its definition's <c>Name</c> would make the gate silently no-op.</item>
/// </list>
/// </summary>
public sealed class CollectorGateSurfacePinTests
{
    /* Representative targets spanning every gate dimension. */
    private static readonly CollectorTargetInfo OnPrem2016 = new() { SqlMajorVersion = 13 };
    private static readonly CollectorTargetInfo OnPrem2014 = new() { SqlMajorVersion = 12 };
    private static readonly CollectorTargetInfo AzureSqlDb = new() { IsAzureSqlDb = true, SqlMajorVersion = 12 };
    private static readonly CollectorTargetInfo AzureMi = new() { IsAzureManagedInstance = true, SqlMajorVersion = 12 };
    private static readonly CollectorTargetInfo AwsRds = new() { IsAwsRds = true, SqlMajorVersion = 15 };
    private static readonly CollectorTargetInfo NoMsdb = new() { SqlMajorVersion = 15, HasMsdbAccess = false };
    private static readonly CollectorTargetInfo Unknown = new();

    private static readonly CollectorTargetInfo[] AllTargets =
        { OnPrem2016, OnPrem2014, AzureSqlDb, AzureMi, AwsRds, NoMsdb, Unknown };

    [Fact]
    public void ServerConfig_AppliesTo_SkipsOnlyAzureSqlDb()
    {
        Assert.False(ServerConfigCollector.Instance.AppliesTo(AzureSqlDb));  /* no sys.configurations */
        Assert.True(ServerConfigCollector.Instance.AppliesTo(AzureMi));
        Assert.True(ServerConfigCollector.Instance.AppliesTo(AwsRds));
        Assert.True(ServerConfigCollector.Instance.AppliesTo(OnPrem2016));
        Assert.True(ServerConfigCollector.Instance.AppliesTo(NoMsdb));
        Assert.True(ServerConfigCollector.Instance.AppliesTo(Unknown));
    }

    /// <summary>
    /// tempdb_stats applies EVERYWHERE, Azure SQL Database included (#2512).
    ///
    /// <para><b>This assertion was flipped, and it is worth being precise about what it used to pin.</b>
    /// It was written for the #2150 field report — 11x consecutive on an Azure SQL DB elastic pool with
    /// error 262, "VIEW DATABASE PERFORMANCE STATE permission denied in database 'tempdb'" — and it pinned
    /// TWO different claims in one <c>Assert.False</c>. The first, that the three-part
    /// <c>tempdb.sys.dm_db_file_space_usage</c> reference cannot be served on Azure SQL Database, was
    /// checkable and is false: the collector's SQL runs verbatim on GP_S_Gen5_2 and HS_S_Gen5_2 and returns
    /// real, moving numbers (see <see cref="TempDbStatsCollector.AppliesTo"/> for the measurement). The
    /// second, that a login might not be able to READ it, is true — but it is a property of the login, not
    /// of the tier, so it belongs to the fault classifier
    /// (<see cref="SqlServerPermissionErrors.IsPermissionDenied"/>, which now covers 262) and not to a gate
    /// that denies every properly-permissioned Azure target to spare the one that is not.</para>
    ///
    /// <para><b>The other half of this pin never changed and is the reason it survives rather than being
    /// deleted.</b> Managed Instance was never gated — it has a real tempdb and full DMV access — and the
    /// original comment says outright that asserting BOTH directions is what stops an "anything Azure"
    /// gate creeping in. That risk runs the other way now: this must not be re-narrowed to Azure SQL DB
    /// later on the strength of the stale doc comment, so both directions still get asserted.</para>
    /// </summary>
    [Fact]
    public void TempDbStats_AppliesTo_EveryTarget_IncludingAzureSqlDb()
    {
        /* #2512: the gate is gone. The DMVs bind and return real data on both Azure SQL DB tiers. */
        Assert.True(TempDbStatsCollector.Instance.AppliesTo(AzureSqlDb));
        /* Never gated, and must stay that way — MI has a real tempdb. */
        Assert.True(TempDbStatsCollector.Instance.AppliesTo(AzureMi));
        Assert.True(TempDbStatsCollector.Instance.AppliesTo(AwsRds));
        Assert.True(TempDbStatsCollector.Instance.AppliesTo(OnPrem2016));
        Assert.True(TempDbStatsCollector.Instance.AppliesTo(OnPrem2014));
        Assert.True(TempDbStatsCollector.Instance.AppliesTo(NoMsdb));
        Assert.True(TempDbStatsCollector.Instance.AppliesTo(Unknown));

        /* The surface the runners actually call — the composed engine gate — must agree, or Darling
           would still skip what Lite now runs. AppliesTo alone cannot see that half. */
        Assert.True(CollectorCatalog.AppliesTo(TempDbStatsCollector.Instance, AzureSqlDb));
        Assert.True(CollectorCatalog.AppliesTo(TempDbStatsCollector.Instance, AzureMi));
        Assert.True(CollectorCatalog.AppliesTo(TempDbStatsCollector.Instance.Name, AzureSqlDb));
    }

    [Fact]
    public void TraceFlags_AppliesTo_SkipsOnlyAzureSqlDb()
    {
        Assert.False(TraceFlagsCollector.Instance.AppliesTo(AzureSqlDb));    /* no DBCC TRACESTATUS */
        Assert.True(TraceFlagsCollector.Instance.AppliesTo(AzureMi));
        Assert.True(TraceFlagsCollector.Instance.AppliesTo(AwsRds));
        Assert.True(TraceFlagsCollector.Instance.AppliesTo(OnPrem2016));
        Assert.True(TraceFlagsCollector.Instance.AppliesTo(Unknown));
    }

    [Fact]
    public void QueryStats_AppliesTo_SkipsOnlyPreSql2016OnPrem()
    {
        Assert.False(QueryStatsCollector.Instance.AppliesTo(OnPrem2014)); /* pre-2016 lacks columns read */
        Assert.True(QueryStatsCollector.Instance.AppliesTo(OnPrem2016));
        Assert.True(QueryStatsCollector.Instance.AppliesTo(AzureSqlDb));  /* low ProductMajorVersion, DMV OK */
        Assert.True(QueryStatsCollector.Instance.AppliesTo(AzureMi));
        Assert.True(QueryStatsCollector.Instance.AppliesTo(Unknown));     /* 0 ⇒ assume newest */
    }

    [Fact]
    public void QueryStore_AppliesTo_SkipsOnlyPreSql2016OnPrem()
    {
        Assert.False(QueryStoreCollector.Instance.AppliesTo(OnPrem2014)); /* Query Store shipped in 2016 */
        Assert.True(QueryStoreCollector.Instance.AppliesTo(OnPrem2016));
        Assert.True(QueryStoreCollector.Instance.AppliesTo(AzureSqlDb));
        Assert.True(QueryStoreCollector.Instance.AppliesTo(AzureMi));
        Assert.True(QueryStoreCollector.Instance.AppliesTo(Unknown));
    }

    [Fact]
    public void RunningJobs_AppliesTo_SkipsAzureSqlDbAndNoMsdb()
    {
        Assert.False(RunningJobsCollector.Instance.AppliesTo(AzureSqlDb));
        /* #2575: RDS COLLECTS. It was skipped on the reasoning that the syssessions join needs sysadmin,
           which RDS never grants, so the read "would ERROR every cycle". Measured across 84 RDS instances:
           SUCCESS on every one, zero non-SUCCESS rows. And a denial is SQL 229, which CollectorTargetFault
           now classifies as a PERMISSIONS degrade rather than an ERROR - the gate was guarding a failure
           mode the fault classifier did not yet handle when it was written. */
        Assert.True(RunningJobsCollector.Instance.AppliesTo(AwsRds));
        Assert.False(RunningJobsCollector.Instance.AppliesTo(NoMsdb));
        Assert.True(RunningJobsCollector.Instance.AppliesTo(AzureMi));
        Assert.True(RunningJobsCollector.Instance.AppliesTo(OnPrem2016));
        Assert.True(RunningJobsCollector.Instance.AppliesTo(Unknown));
    }

    [Fact]
    public void JobHistory_AppliesTo_SkipsAzureSqlDbAndNoMsdb_ButNotRds()
    {
        Assert.False(JobHistoryCollector.Instance.AppliesTo(AzureSqlDb));
        Assert.False(JobHistoryCollector.Instance.AppliesTo(NoMsdb));
        Assert.True(JobHistoryCollector.Instance.AppliesTo(AwsRds));      /* never touches syssessions */
        Assert.True(JobHistoryCollector.Instance.AppliesTo(AzureMi));
        Assert.True(JobHistoryCollector.Instance.AppliesTo(OnPrem2016));
        Assert.True(JobHistoryCollector.Instance.AppliesTo(Unknown));
    }

    [Fact]
    public void AgentStatus_AppliesTo_SkipsAzureSqlDbAndNoMsdb()
    {
        Assert.False(AgentStatusCollector.Instance.AppliesTo(AzureSqlDb));
        Assert.True(AgentStatusCollector.Instance.AppliesTo(AwsRds));   /* #2575: RDS has Agent and collects */
        Assert.False(AgentStatusCollector.Instance.AppliesTo(NoMsdb));
        Assert.True(AgentStatusCollector.Instance.AppliesTo(AzureMi));
        Assert.True(AgentStatusCollector.Instance.AppliesTo(OnPrem2016));
        Assert.True(AgentStatusCollector.Instance.AppliesTo(Unknown));
    }

    [Fact]
    public void CatalogByNameGate_AgreesWithDefinitionAppliesTo_ForEveryCollectorAndTarget()
    {
        /* The parity crux: Lite consults CollectorCatalog.AppliesTo(NAME, target) pre-dispatch, Darling's
           runner calls CollectorCatalog.AppliesTo(DEFINITION, target). If those ever disagreed the two SKUs
           would gate differently — exactly the drift this collapse removes.
        
           Both COMPOSED forms, deliberately. This used to compare the raw definition.AppliesTo(target)
           against the by-name form, which was equivalent only while every collector was SQL Server: the
           composed overload also requires definition.TargetEngine == target.Engine, so a PostgreSQL
           definition whose own AppliesTo returns true unconditionally (the slots collector) legitimately
           disagrees with its raw gate when handed a SQL Server target. Comparing raw-to-composed would force
           either a wrong assertion or a filtered loop; comparing composed-to-composed is the claim that
           actually matters and holds for every collector against every target. */
        foreach (var definition in CollectorCatalog.All)
        {
            foreach (var target in AllTargets)
            {
                /* The expectation is SPELLED OUT rather than delegated to the other overload. Comparing
                   AppliesTo(definition, t) against AppliesTo(name, t) is nearly circular — the by-name
                   overload just looks the name up and calls the by-definition one, so only a corrupt
                   name->definition map could fail it, and a bug in the composed rule itself would pass.
                   Stating the rule independently means BOTH the lookup and the composition are pinned. */
                var expected = definition.TargetEngine == target.Engine && definition.AppliesTo(target);

                Assert.Equal(expected, CollectorCatalog.AppliesTo(definition, target));
                Assert.Equal(expected, CollectorCatalog.AppliesTo(definition.Name, target));
            }
        }
    }

    [Fact]
    public void CatalogGate_UnknownCollectorName_IsNotGated()
    {
        /* A typo'd/renamed name is not silently skipped — it falls through to true so the host's dispatch
           switch raises its "Unknown collector" error instead of the gate masking it. */
        Assert.True(CollectorCatalog.AppliesTo("no_such_collector", AzureSqlDb));
        Assert.True(CollectorCatalog.AppliesTo("no_such_collector", NoMsdb));
    }

    [Fact]
    public void HasMsdbAccess_DefaultsToTrue_SoUnknownTargetsStillAttemptAgentCollectors()
    {
        /* The probe returns NULL ⇒ assume access; every bare CollectorTargetInfo must mirror that, or the
           three Agent collectors would silently gate off on the unknown path. */
        Assert.True(new CollectorTargetInfo().HasMsdbAccess);
        Assert.True(RunningJobsCollector.Instance.AppliesTo(new CollectorTargetInfo()));
        Assert.True(JobHistoryCollector.Instance.AppliesTo(new CollectorTargetInfo()));
        Assert.True(AgentStatusCollector.Instance.AppliesTo(new CollectorTargetInfo()));
    }

    // ── PIN C (round 2, from the #1500 review): dispatch-name hardening ──────────────────────────────────
    // The pins above prove Lite's by-name gate (CollectorCatalog.AppliesTo(name, target)) agrees with each
    // definition's AppliesTo FOR CATALOG NAMES. They can't catch a switch string in RunCollectorAsync drifting
    // from its definition's Name (a rename, a typo): the pre-dispatch AppliesTo(name) returns true-on-miss for
    // the unknown string, so the gate silently no-ops and the collector runs where it should have been skipped.
    // The dispatch string list has no canonical export, so it is scanned from RemoteCollectorService.cs source.

    private const string RemoteCollectorServicePath = "Lite/Services/RemoteCollectorService.cs";

    [Fact]
    public void RunCollectorDispatchNames_AreAllRealCatalogCollectors()
    {
        var dispatchNames = ExtractRunCollectorDispatchNames();

        /* Non-vacuous floor: the real switch has ~35 arms — a scan that found a handful is broken, not passing. */
        Assert.True(dispatchNames.Count >= 30,
            $"the RunCollectorAsync dispatch-switch scan found only {dispatchNames.Count} arms — the parse is likely broken");

        var catalog = CollectorCatalog.All.Select(d => d.Name).ToHashSet(StringComparer.Ordinal);
        var orphans = FindOrphanDispatchNames(dispatchNames, catalog);

        Assert.True(orphans.Count == 0,
            "RunCollectorAsync dispatches collector name(s) absent from CollectorCatalog.All — " +
            "CollectorCatalog.AppliesTo(name) returns true-on-miss for these, so their target gate silently " +
            "no-ops (they run even where a definition's AppliesTo would skip them): " + string.Join(", ", orphans));
    }

    [Fact]
    public void DispatchNameCatalogCheck_IsNonVacuous()
    {
        /* Prove the check has teeth so a future extraction/regex change can't make it vacuously green: a real
           dispatch name yields no orphan, and a fake one that is NOT a catalog collector is flagged. */
        var catalog = CollectorCatalog.All.Select(d => d.Name).ToHashSet(StringComparer.Ordinal);
        Assert.Contains("wait_stats", catalog);   // sanity: the catalog side of the check is populated

        var real = ExtractRunCollectorDispatchNames();
        Assert.Empty(FindOrphanDispatchNames(real, catalog));

        var withFake = real.Append("totally_not_a_real_collector").ToList();
        Assert.Equal(new[] { "totally_not_a_real_collector" }, FindOrphanDispatchNames(withFake, catalog));
    }

    private static List<string> FindOrphanDispatchNames(IEnumerable<string> dispatchNames, ISet<string> catalogNames) =>
        dispatchNames.Where(n => !catalogNames.Contains(n)).Distinct(StringComparer.Ordinal)
            .OrderBy(n => n, StringComparer.Ordinal).ToList();

    /// <summary>
    /// Scans the collector-name string literals from <c>RunCollectorAsync</c>'s dispatch switch expression in
    /// <c>RemoteCollectorService.cs</c>: the region from <c>collectorName switch</c> to its <c>_ =&gt;</c>
    /// default arm, matching each <c>"name" =&gt;</c> arm. The default arm (no quotes) and the two upstream
    /// <c>collectorName == "…"</c> XE-ensure guards (before the switch) are excluded by construction.
    /// </summary>
    private static List<string> ExtractRunCollectorDispatchNames()
    {
        var src = ParitySource.ReadFile(RemoteCollectorServicePath);

        var switchIdx = src.IndexOf("collectorName switch", StringComparison.Ordinal);
        Assert.True(switchIdx > 0, "could not find the 'collectorName switch' dispatch in RemoteCollectorService.cs");

        var defaultArmIdx = src.IndexOf("_ =>", switchIdx, StringComparison.Ordinal);
        Assert.True(defaultArmIdx > switchIdx, "could not find the dispatch switch's default '_ =>' arm");

        var switchBody = src.Substring(switchIdx, defaultArmIdx - switchIdx);

        return Regex.Matches(switchBody, @"^\s*""([a-z0-9_]+)""\s*=>", RegexOptions.Multiline)
            .Select(m => m.Groups[1].Value)
            .ToList();
    }
}
