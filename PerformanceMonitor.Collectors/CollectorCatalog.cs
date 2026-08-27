/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Every collector definition in the library, as the engine-neutral schema surface. This is the
/// single enumeration storage hosts build from — Darling generates its full Postgres schema by
/// walking this list. A new definition MUST be added here (the catalog test pins the count).
/// </summary>
public static class CollectorCatalog
{
    public static IReadOnlyList<ICollectorSchemaInfo> All { get; } = new ICollectorSchemaInfo[]
    {
        WaitStatsCollector.Instance,
        LatchStatsCollector.Instance,
        SpinlockStatsCollector.Instance,
        CpuSchedulerStatsCollector.Instance,
        PlanCacheStatsCollector.Instance,
        TempDbStatsCollector.Instance,
        MemoryGrantsCollector.Instance,
        CpuUtilizationCollector.Instance,
        MemoryStatsCollector.Instance,
        MemoryClerksCollector.Instance,
        MemoryPressureEventsCollector.Instance,
        FileIoStatsCollector.Instance,
        ServerPropertiesCollector.Instance,
        ServerConfigCollector.Instance,
        DatabaseConfigCollector.Instance,
        DatabaseStateCollector.Instance,
        TraceFlagsCollector.Instance,
        DatabaseScopedConfigCollector.Instance,
        QueryStoreHealthCollector.Instance,
        SessionStatsCollector.Instance,
        SessionSummaryStatsCollector.Instance,
        WaitingTasksCollector.Instance,
        ProcedureStatsCollector.Instance,
        RunningJobsCollector.Instance,
        PerfmonStatsCollector.Instance,
        DmvBlockingSnapshotCollector.Instance,
        DatabaseSizeStatsCollector.Instance,
        IndexObjectStatsCollector.Instance,
        QueryStatsCollector.Instance,
        QuerySnapshotsCollector.Instance,
        QueryStoreCollector.Instance,
        DeadlocksCollector.Instance,
        BlockedProcessReportCollector.Instance,
        LongQueryCompletionsCollector.Instance,
        SystemHealthEventsCollector.Instance,
        DefaultTraceEventsCollector.Instance,
        JobHistoryCollector.Instance,
        AgentStatusCollector.Instance,
        AgReplicaStatesCollector.Instance,
        AgDatabaseReplicaStatesCollector.Instance,
        PlanCorrectionCollector.Instance,
        PvsStatsCollector.Instance,
        /* PostgreSQL definitions. They live in the same catalog as the T-SQL ones on purpose: the
           schema generator walks this list to create tables, and one store can hold data from both
           engines, so splitting the catalog per engine would fragment DDL generation. Dispatch is
           kept honest by the engine gate in AppliesTo(definition, target). */
        PgWaitStatsCollector.Instance,
        PgStatementStatsCollector.Instance,
        PgWraparoundStatsCollector.Instance,
        PgServerConfigCollector.Instance,
        PgDeadlocksCollector.Instance,
        PgXminHorizonCollector.Instance,
        PgReplicationSlotsCollector.Instance,
        PgAutovacuumStatsCollector.Instance,
        PgIoStatsCollector.Instance,
        PgBlockingCollector.Instance,
        PgDatabaseStatsCollector.Instance,
        PgIndexUsageStatsCollector.Instance,
        PgTableBloatStatsCollector.Instance,
        PgSessionStatesCollector.Instance,
        PgPlanCaptureReadinessCollector.Instance,
        PgWriteStatsCollector.Instance,
        PgExtensionAvailabilityCollector.Instance,
        PgLockStatsCollector.Instance,
        PgWaitSamplingCollector.Instance,
        PgKernelStatsCollector.Instance,
        PgPredicateStatsCollector.Instance,
        PgPlanCaptureCollector.Instance,
        PgColumnStatsCollector.Instance,
        PgReplicationStatsCollector.Instance,
        PgBufferUsageCollector.Instance,
        PgIndexBloatCollector.Instance,
    };

    /// <summary>Name → definition, for the by-name target-gate lookup. Built once from <see cref="All"/>.</summary>
    private static readonly Dictionary<string, ICollectorSchemaInfo> s_byName =
        All.ToDictionary(c => c.Name, StringComparer.Ordinal);

    /// <summary>
    /// The single authoritative target-gate check both SKUs share: whether <paramref name="collectorName"/>
    /// applies to <paramref name="target"/>. Delegates to the definition's
    /// <see cref="ICollectorSchemaInfo.AppliesTo"/> override — the gate CONDITION lives there and nowhere
    /// else. Darling's collector runner calls <c>definition.AppliesTo(target)</c> directly; Lite consults
    /// this by name for its pre-dispatch SKIPPED log (a genuine skip with no collection_log row, vs. the
    /// SUCCESS/0-rows a gated collector would otherwise record). An unknown name returns <c>true</c> (not
    /// gated) so a typo surfaces as the dispatch switch's "Unknown collector" rather than a silent skip.
    /// </summary>
    public static bool AppliesTo(string collectorName, CollectorTargetInfo target) =>
        s_byName.TryGetValue(collectorName, out var definition) ? AppliesTo(definition, target) : true;

    /// <summary>
    /// The definition registered under <paramref name="collectorName"/>, or <c>null</c> when the catalog
    /// does not know the name.
    /// <para>The single name → definition step, exposed so a caller that carries BOTH forms of a question
    /// can delegate the by-name form to the by-definition one rather than keeping a second copy of the
    /// lookup AND a second copy of the true-on-miss rule that goes with it — see
    /// <see cref="CollectorEngineCapability.IsCollectedOnEngineEdition(string, int)"/>. Returning the
    /// definition rather than a <c>bool</c> is what makes that delegation possible; a <c>Contains</c>
    /// would leave the caller to look the definition up a second way.</para>
    /// </summary>
    public static ICollectorSchemaInfo? Find(string collectorName) =>
        s_byName.TryGetValue(collectorName, out var definition) ? definition : null;

    /// <summary>
    /// The full dispatch gate: a definition runs only when its
    /// <see cref="ICollectorSchemaInfo.TargetEngine"/> matches the target's
    /// <see cref="CollectorTargetInfo.Engine"/> AND its own
    /// <see cref="ICollectorDefinition{TRow}.AppliesTo"/> gate passes. Both runners call this rather
    /// than <c>AppliesTo</c> directly, so a definition written in one engine's dialect can never be
    /// sent to the other — the individual definitions stay free to reason only about the hosting
    /// flavour and version floors within their own engine.
    /// <para>Both defaults are <see cref="CollectorTargetEngine.SqlServer"/>, so this is behaviour-
    /// identical to the previous direct <c>AppliesTo</c> call for every definition and target that
    /// exists today.</para>
    /// </summary>
    public static bool AppliesTo(ICollectorSchemaInfo definition, CollectorTargetInfo target) =>
        EngineMatches(definition, target) && definition.AppliesTo(target);

    /// <summary>
    /// The engine half of the gate alone, by name — for callers that want to drop a foreign-dialect
    /// collector BEFORE dispatch rather than let it run and report zero rows. Darling's sweep uses
    /// this so a wrong-engine collector produces no <c>collection_log</c> row at all: a gated run
    /// would otherwise be recorded as SUCCESS, and with two engines in one catalog that would mean a
    /// fake success per foreign collector per cycle on every server.
    /// <para>An unknown name returns <c>true</c> (not filtered), matching
    /// <see cref="AppliesTo(string, CollectorTargetInfo)"/>, so a typo still surfaces as the dispatch
    /// switch's "unknown collector" rather than a silent disappearance.</para>
    /// </summary>
    public static bool EngineMatches(string collectorName, CollectorTargetInfo target) =>
        !s_byName.TryGetValue(collectorName, out var definition) || EngineMatches(definition, target);

    /// <summary>
    /// The engine half of the gate alone, by definition — the by-name overload above is this plus the
    /// catalog's true-on-miss lookup. Split out so the comparison itself lives in ONE place: a caller
    /// holding a definition (a test double, or anything walking <see cref="All"/>) would otherwise
    /// re-spell <c>definition.TargetEngine == target.Engine</c> and own a second copy of the rule.
    /// </summary>
    public static bool EngineMatches(ICollectorSchemaInfo definition, CollectorTargetInfo target) =>
        definition.TargetEngine == target.Engine;

    /// <summary>
    /// True when <paramref name="collectorName"/>'s query carries the deliberate short
    /// <c>SET LOCK_TIMEOUT</c> guard, so the runners' catch sites can classify SQL error 1222 as a
    /// <c>YIELDED</c> row by name (#1805) — the flag CONDITION lives on the definition
    /// (<see cref="ICollectorSchemaInfo.YieldsOnLockTimeout"/>) and nowhere else. An unknown name
    /// returns <c>false</c> — the opposite default from <see cref="AppliesTo"/>, deliberately: a
    /// 1222 from a collector this catalog does not know is unexpected and must stay an ERROR.
    /// </summary>
    public static bool YieldsOnLockTimeout(string collectorName) =>
        s_byName.TryGetValue(collectorName, out var definition) && definition.YieldsOnLockTimeout;
}
