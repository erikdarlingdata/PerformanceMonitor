/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Whether a collector can EVER run against a server of a given SQL Server engine edition — and the one
/// sentence both SKUs return to a caller whose read is served by a collector that cannot (#2511).
///
/// <para><b>The defect this closes.</b> Twelve collectors gate themselves off on Azure SQL Database, and the
/// reads they feed did not know. On a live Azure SQL Database (<c>EngineEdition</c> 5, General Purpose and
/// Hyperscale alike) <c>sys.dm_xe_sessions</c> does not exist, so the <c>system_health</c> ring buffer can
/// never be read there; <see cref="SystemHealthEventsCollector"/> gates off correctly, and all nine
/// health-parser reads then told the operator to "check that collection is running for this server and that
/// its system_health session is started". Collection WAS running, and the session cannot be started on that
/// engine. A confident, specific, wrong instruction is worse than silence.</para>
///
/// <para><b>The answer is derived, never transcribed.</b> The capability question is answered by asking the
/// collector's OWN gate — <see cref="CollectorCatalog.AppliesTo(string, CollectorTargetInfo)"/>, the same
/// surface both runners dispatch through — over every target shape an engine edition permits. A hand-kept
/// list of "collectors Azure SQL DB does not have" would go stale in exactly the direction that makes it
/// pass: a gate that opened up would leave the list claiming a permanent gap that no longer exists, and
/// nothing would say so. Here, opening a gate silently stops the claim, which is the correct direction to
/// fail in.</para>
///
/// <para><b>Why "every target shape" rather than one representative target.</b> Engine edition is the only
/// fact the store holds for certain (<c>servers.sql_engine_edition</c>, stamped by the registration upsert on
/// every connect). Version, msdb access and RDS-ness are separate facts, and two of them are FIXABLE — an
/// operator can grant msdb access, and an upgrade moves the version floor. So the claim made here is
/// deliberately the narrow one: <i>there is no target with this engine edition, under any combination of the
/// other facts, for which this collector runs.</i> That is what makes "permanent" honest. A collector gated
/// off only for want of msdb access, or only below a version floor, is NOT reported as an engine gap — its
/// read keeps the <c>unavailable</c> vocabulary, which is what sends an operator to look, correctly.</para>
///
/// <para><b>The message lives here, not in the two MCP trees.</b> Both SKUs must answer this byte-identically,
/// and every shared sentence that lives twice has eventually been reworded once
/// (<c>McpMissMessageParityPinTests</c> exists because of it). One function called from both surfaces makes
/// parity structural rather than pinned.</para>
///
/// <para><b>Two axes, asked in order (#2530).</b> Engine KIND is asked first and engine EDITION second,
/// because a PostgreSQL target has no edition at all — <c>SERVERPROPERTY</c> does not exist there, the
/// connector stamps 0, and the edition axis therefore (correctly) declines to claim anything about it. Kind
/// is the coarser and more permanent fact of the two: an edition can change under an operator (a migration
/// to Azure SQL Database, an upgrade), while a target's DIALECT is what decides whether a collector's query
/// text could ever be sent at it. Both axes are answered by asking
/// <see cref="CollectorCatalog.AppliesTo(ICollectorSchemaInfo, CollectorTargetInfo)"/> — the collectors' own
/// dispatch gate — over every target shape the axis permits, for the same reason: a hand-kept list of "what
/// PostgreSQL does not have" would go stale in the direction that keeps passing.</para>
///
/// <para><b>Each axis sweeps what it does not fix (#2532).</b> The edition sweep fixes the two Azure flags
/// and varies msdb access, RDS-ness and the SQL major version; the kind sweep fixes
/// <see cref="CollectorTargetInfo.IsAurora"/> on the PostgreSQL side and varies the PostgreSQL version floors
/// and the recovery state. Same discipline both times: a fact an operator can change is never allowed to
/// produce a "never will", and a fact nothing can change is what a permanence claim is made of. That is why
/// <c>pg_wait_stats</c> — reading <c>aurora_stat_system_waits()</c>, which core PostgreSQL has in no version
/// — is a permanent gap on stock PostgreSQL, while <c>pg_stat_io</c>'s PG16 floor and the writer-only
/// autovacuum read are not and keep the <c>unavailable</c> vocabulary.</para>
/// </summary>
public static class CollectorEngineCapability
{
    /// <summary>The probe returned no edition — a server that has never connected, or a PostgreSQL target
    /// (<c>SERVERPROPERTY</c> does not exist there and the connector stamps 0). No capability claim is made
    /// for it on the EDITION axis: "we do not know" must never render as "this will never work". Since
    /// #2530 the store records engine KIND separately, so a PostgreSQL target IS distinguishable from an
    /// unconnected one — but that distinction is made there, not here, and this constant keeps meaning
    /// exactly what it meant.</summary>
    public const int UnknownEngineEdition = 0;

    /// <summary><c>SERVERPROPERTY('EngineEdition')</c> for Azure SQL Database — the one edition that produces
    /// permanent gaps today, and the one the live probe in #2511 measured.</summary>
    public const int AzureSqlDatabaseEngineEdition = 5;

    /// <summary><c>SERVERPROPERTY('EngineEdition')</c> for Azure SQL Managed Instance.</summary>
    public const int AzureManagedInstanceEngineEdition = 8;

    /// <summary>
    /// The SQL major versions swept when asking whether ANY target of an engine edition runs a collector.
    /// <para>0 is "unknown, assume newest" (the value every version gate in the library already treats as a
    /// pass) and 99 is a version above any floor. The real majors in between are carried anyway so a future
    /// gate written as a RANGE — supported on 15 and 16 but not 17 — is answered correctly rather than by
    /// whichever single representative value happened to be chosen here.</para>
    /// <para>Dropping a major from this list is not a cosmetic edit: a gate that only that major satisfies
    /// then matches no swept shape and is reported as a permanent engine gap.
    /// <c>AVersionGate_IsAnsweredAcrossTheRealMajors_NotByOneRepresentativeValue</c> asserts every value here
    /// is reachable on its own (#2518).</para>
    /// </summary>
    private static readonly int[] MajorVersionSweep = { 0, 11, 12, 13, 14, 15, 16, 17, 99 };

    /// <summary>
    /// What a gated-off collector would have captured, in a noun phrase that completes "…so X is not
    /// collected for this server". PROSE ONLY: nothing here decides whether a gap exists, so an entry that
    /// falls out of date makes a message vaguer, never wrong. A collector with no entry still gets a correct
    /// message through the fallback in <see cref="NotCollectedMessage"/>.
    /// <para>Public so the tests can hold it to the catalog: every key must be a real collector name, and
    /// every key must be either a collector its OWN gate shuts out somewhere or one a shipped read asks the
    /// capability question about, so an entry cannot outlive the reason it exists. The "its own gate" half
    /// deliberately excludes the DIALECT gap every collector now has on every other engine — a check that
    /// counted that would pass for any name anyone typed.</para>
    /// </summary>
    public static readonly IReadOnlyDictionary<string, string> CapturePathByCollector =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["system_health_events"] = "the system_health extended-events ring buffer",
            ["server_config"] = "the sys.configurations instance settings",
            ["trace_flags"] = "the DBCC TRACESTATUS trace-flag list",
            ["default_trace_events"] = "the built-in default trace",
            ["cpu_scheduler_stats"] = "the sys.dm_os_schedulers scheduler snapshot",
            ["memory_pressure_events"] = "the RING_BUFFER_RESOURCE_MONITOR ring buffer",
            ["running_jobs"] = "the SQL Agent running-job snapshot",
            ["job_history"] = "the SQL Agent job history",
            ["agent_status"] = "the SQL Agent service status",
            ["database_states"] = "the sys.databases state snapshot",
            ["ag_replica_states"] = "the Always On availability replica states",
            ["ag_database_replica_states"] = "the Always On per-database replica states",

            /* The collectors that run on EVERY SQL Server, and therefore had nothing to say while the
               EDITION axis was the only one (#2532). On a PostgreSQL target every one of them is a
               permanent gap, so the reads they serve now ask - and a read that asks without a noun phrase
               here falls back to "the data this read is served from", which is true and tells an operator
               nothing they did not already know. */
            ["cpu_utilization"] = "the SQL Server CPU utilization history",
            ["wait_stats"] = "the sys.dm_os_wait_stats cumulative totals",
            ["waiting_tasks"] = "the sys.dm_os_waiting_tasks samples",
            ["memory_stats"] = "the instance memory snapshot - server memory, buffer pool and plan cache",
            ["memory_clerks"] = "the sys.dm_os_memory_clerks consumer list",
            ["memory_grant_stats"] = "the resource-semaphore and memory-grant snapshot",
            ["file_io_stats"] = "the sys.dm_io_virtual_file_stats per-file I/O totals",
            ["tempdb_stats"] = "the tempdb space-usage snapshot",
            ["perfmon_stats"] = "the sys.dm_os_performance_counters values",
            ["query_stats"] = "the sys.dm_exec_query_stats plan-cache history",
            ["query_snapshots"] = "the sys.dm_exec_requests active-query snapshots",
            ["procedure_stats"] = "the sys.dm_exec_procedure_stats history",
            ["query_store"] = "the Query Store runtime statistics",
            ["query_store_health"] = "the per-database Query Store configuration and health",
            ["plan_cache_stats"] = "the plan-cache composition statistics",
            ["plan_correction"] = "the sys.dm_db_tuning_recommendations automatic plan corrections",
            ["pvs_stats"] = "the persistent version store statistics",
            ["session_stats"] = "the connection and session snapshot",
            ["blocked_process_report"] = "the blocked process report capture",
            ["deadlocks"] = "the captured deadlock graphs",
            ["latch_stats"] = "the sys.dm_os_latch_stats totals",
            ["spinlock_stats"] = "the sys.dm_os_spinlock_stats totals",
            ["long_query_completions"] = "the long-running query completion capture",
            ["database_config"] = "the sys.databases per-database settings",
            ["database_scoped_config"] = "the sys.database_scoped_configurations settings",
            ["database_size_stats"] = "the per-database file size and space usage",
            ["index_object_stats"] = "the per-index usage, size and contention statistics",
            ["server_properties"] = "the SERVERPROPERTY instance properties",
            /* The PostgreSQL side of the same table (#2532). The first two read an Aurora-only surface, so
               they are permanent gaps on stock PostgreSQL — the mirror of the twelve above, which are
               permanent gaps on Azure SQL Database. The other six are here because their reads ask the
               capability question on the DIALECT axis: a SQL Server target asked get_pg_xmin_horizon used
               to be told nothing was holding its horizon back, which is a confident all-clear about a
               mechanism that engine does not have. */
            ["pg_wait_stats"] = "the aurora_stat_system_waits() cumulative wait counters",
            ["pg_cpu_utilization"] = "AWS Performance Insights' os.cpuUtilization.total.avg instance CPU reading",
            ["pg_statement_stats"] = "the aurora_stat_statements() per-statement history",
            ["pg_wait_sampling"] = "the pg_wait_sampling extension's sampled wait profile",
            ["pg_kernel_stats"] = "the pg_stat_kcache extension's per-statement OS CPU and device I/O",
            ["pg_predicate_stats"] = "the pg_qualstats extension's sampled predicate selectivity",
            ["pg_index_bloat"] = "the pgstattuple extension's measured index leaf density and fragmentation",
            ["pg_column_stats"] = "the pg_stats per-column distribution statistics",
            ["pg_buffer_usage"] = "the pg_buffercache extension's shared buffer pool contents",
            ["pg_extension_availability"] = "the per-database extension inventory",
            ["pg_lock_stats"] = "the sampled pg_locks activity",
            ["pg_write_stats"] = "the checkpoint and WAL write counters",
            ["pg_server_config"] = "the server's pg_settings configuration snapshot",
            ["pg_deadlocks"] = "the deadlock reports PostgreSQL writes to its server log",
            ["pg_replication_stats"] = "the pg_stat_replication connected-replica states",
            /* Named for the LOG, because that is where the gap actually is: auto_explain writes
               plans nowhere else, and on Aurora and RDS there is no filesystem to read them from
               (#2538). A generic phrasing here would say the collector did not run and leave the
               reader to guess whether that is fixable. */
            ["pg_plan_capture"] = "the auto_explain plans written to the server log",
            ["pg_blocking"] = "the pg_blocking_pids() lock-wait samples",
            ["pg_io_stats"] = "the pg_stat_io per-backend I/O counters",
            ["pg_autovacuum_stats"] = "the pg_stat_user_tables autovacuum backlog",
            ["pg_replication_slots"] = "the pg_replication_slots WAL-retention snapshot",
            ["pg_wraparound_stats"] = "the per-database transaction-ID freeze headroom",
            ["pg_xmin_horizon"] = "the xmin-horizon holders behind vacuum",
            ["pg_database_stats"] = "the pg_stat_database per-database counters - temp-file spills, cache "
                                  + "hit ratio, deadlocks and the commit/rollback split",
            ["pg_index_usage_stats"] = "the pg_stat_user_indexes per-index scan counts, sizes and "
                                     + "droppability facts",
            ["pg_table_bloat_stats"] = "the statistics-based per-table bloat estimate and its dead-tuple "
                                     + "counts",
            ["pg_session_states"] = "the pg_stat_activity session states behind a pinned xmin horizon - "
                                  + "who is idle in transaction, for how long, and whether they hold an "
                                  + "xid or a snapshot",
        };

    /// <summary>
    /// Human-readable <c>SERVERPROPERTY('EngineEdition')</c> description. The single copy: Darling's
    /// connector delegates here rather than keeping a second switch, because two edition tables in one repo
    /// drift and the one that drifts is never the one being read.
    /// </summary>
    public static string DescribeEngineEdition(int engineEdition) => engineEdition switch
    {
        1 => "Personal/Desktop",
        2 => "Standard",
        3 => "Enterprise",
        4 => "Express",
        AzureSqlDatabaseEngineEdition => "Azure SQL Database",
        6 => "Azure Synapse Analytics",
        AzureManagedInstanceEngineEdition => "Azure SQL Managed Instance",
        9 => "Azure SQL Edge",
        11 => "Azure Synapse serverless SQL pool",
        _ => $"Unknown ({engineEdition})",
    };

    /// <summary>
    /// Every target shape an engine edition permits, for the exhaustive gate sweep. The two Azure flags are
    /// FIXED by the edition (they are what the probe derives them from); everything else varies, because
    /// none of it is implied by the edition and all of it can differ server to server.
    /// <para>Public so a test can assert the sweep actually spans the dimensions the gates read, rather than
    /// trusting that it does.</para>
    /// <para><b>Adding a field to <see cref="CollectorTargetInfo"/> that a SQL Server gate reads means adding
    /// it here too.</b> A fact this sweep never varies sits at its CLR default in every shape, so a gate
    /// written on it fails all of them and the derivation reports a permanent engine gap for a collector that
    /// runs — silently, because the gap set still looks plausible. That is not left to review:
    /// <c>EveryFactASqlServerGateReads_IsVariedBySweepOrFixedByEdition</c> decodes every SQL Server gate's IL
    /// for the <see cref="CollectorTargetInfo"/> getters it calls and fails if any of them names a fact this
    /// sweep leaves constant (#2518). Vary it here, or derive it from the engine edition the way the two
    /// Azure flags are — there is no third option and no list to add it to instead.</para>
    /// </summary>
    public static IEnumerable<CollectorTargetInfo> TargetsWithEngineEdition(int engineEdition)
    {
        foreach (var major in MajorVersionSweep)
        {
            foreach (var hasMsdbAccess in new[] { true, false })
            {
                foreach (var isAwsRds in new[] { false, true })
                {
                    yield return new CollectorTargetInfo
                    {
                        Engine = CollectorTargetEngine.SqlServer,
                        IsAzureSqlDb = engineEdition == AzureSqlDatabaseEngineEdition,
                        IsAzureManagedInstance = engineEdition == AzureManagedInstanceEngineEdition,
                        IsAwsRds = isAwsRds,
                        HasMsdbAccess = hasMsdbAccess,
                        SqlMajorVersion = major,
                    };
                }
            }
        }
    }

    /// <summary>
    /// The PostgreSQL major versions swept when asking whether ANY target of an engine KIND runs a collector
    /// (#2532). Same shape and same reasoning as <see cref="MajorVersionSweep"/> one engine over: 0 is
    /// "unknown, assume newest" and 99 is above any floor, with the real majors carried so a gate written as
    /// a RANGE is answered rather than by whichever representative value happened to be picked.
    /// <para>13 is the floor because that is the oldest major any monitored target could plausibly be; the
    /// list is deliberately generous, since a major MISSING from it can only make the derivation claim a gap
    /// that a real target does not have.</para>
    /// </summary>
    private static readonly int[] PostgresMajorVersionSweep = { 0, 13, 14, 15, 16, 17, 99 };

    /// <summary>
    /// The <c>server_version_num</c> values swept alongside the major. Carried as its OWN dimension rather
    /// than derived from the major, because the gates that will read it are MINOR-version gates
    /// (<c>aurora_stat_resource_usage()</c> needs 16.9+/17.5+ and is absent on 17.4), and a sweep that
    /// derived the minor from the major would answer every such gate from one arbitrary minor.
    /// <para><b>The cross product includes incoherent pairs</b> — major 16 alongside version 170005 — and
    /// that is deliberate. A superset of the real target shapes can only ever find MORE shapes that pass a
    /// gate, so its error direction is under-claiming: a gate satisfiable only by a combination no real
    /// server has would be reported as collected, and the read keeps its <c>unavailable</c> vocabulary. The
    /// opposite error — a shape that no sweep produces, so a gate matches nothing and the derivation
    /// announces a permanent gap — is the one this whole mechanism exists to prevent, and a superset cannot
    /// make it.</para>
    /// </summary>
    private static readonly int[] PostgresVersionNumSweep = { 0, 160008, 160009, 170004, 170005, 999999 };

    /// <summary>
    /// Every target shape an engine KIND permits, for the exhaustive gate sweep — the engine-kind twin of
    /// <see cref="TargetsWithEngineEdition"/> (#2532).
    ///
    /// <para><b>What the kind fixes, and what it must not.</b> On the PostgreSQL side the kind fixes exactly
    /// one fact: <see cref="CollectorTargetInfo.IsAurora"/>, which is what separates the two PostgreSQL
    /// tokens and is not something an operator can change — a stock PostgreSQL server does not become Aurora.
    /// Everything else varies, because none of it is implied by the kind and all of it is FIXABLE: an upgrade
    /// moves <see cref="CollectorTargetInfo.PostgresMajorVersion"/> and
    /// <see cref="CollectorTargetInfo.PostgresVersionNum"/> past a floor, and a connection to the writer
    /// moves <see cref="CollectorTargetInfo.IsInRecovery"/>. Claiming permanence over any of those would be
    /// the #2511 over-claim one engine over.</para>
    ///
    /// <para><b>The SQL Server arm varies the Azure flags</b>, where
    /// <see cref="TargetsWithEngineEdition"/> fixes them — because the KIND does not decide an edition. That
    /// is what keeps the two axes from answering each other's question: a collector gated off on Azure SQL
    /// Database still runs on some SQL Server, so it is not a kind gap, and the edition axis is left to say
    /// so with the edition named. The impossible both-Azure-flags shape is generated rather than skipped, for
    /// the superset reason above and because skipping it would break the full-cross-product property that
    /// makes conjunctive gates answerable.</para>
    ///
    /// <para><b>An unknown, absent or unrecognised kind yields NOTHING.</b> Callers must ask
    /// <see cref="MonitoredEngineKind.IsKnown"/> first; an empty sweep read as "no shape runs it" would turn
    /// silence into the most confident claim in the vocabulary.</para>
    ///
    /// <para><b>Adding a field to <see cref="CollectorTargetInfo"/> that a PostgreSQL gate reads means adding
    /// it here too.</b> A fact this sweep never varies sits at its CLR default in every shape, so a gate
    /// written on it fails all of them and the derivation reports a permanent engine gap for a collector that
    /// runs. <c>EveryFactAPostgresGateReads_IsVariedBySweepOrFixedByKind</c> decodes every PostgreSQL gate's
    /// IL and fails the build when it happens — the twin of the #2518 guard on the edition axis.</para>
    /// </summary>
    public static IEnumerable<CollectorTargetInfo> TargetsWithEngineKind(string? engineKind)
    {
        switch (MonitoredEngineKind.EngineOf(engineKind))
        {
            case CollectorTargetEngine.PostgreSql:
                var isAurora = MonitoredEngineKind.IsAurora(engineKind);

                foreach (var major in PostgresMajorVersionSweep)
                {
                    foreach (var versionNum in PostgresVersionNumSweep)
                    {
                        foreach (var isInRecovery in new[] { false, true })
                        {
                            yield return new CollectorTargetInfo
                            {
                                Engine = CollectorTargetEngine.PostgreSql,
                                IsAurora = isAurora,
                                PostgresMajorVersion = major,
                                PostgresVersionNum = versionNum,
                                IsInRecovery = isInRecovery,
                            };
                        }
                    }
                }

                break;

            case CollectorTargetEngine.SqlServer:
                foreach (var major in MajorVersionSweep)
                {
                    foreach (var hasMsdbAccess in new[] { true, false })
                    {
                        foreach (var isAwsRds in new[] { false, true })
                        {
                            foreach (var isAzureSqlDb in new[] { false, true })
                            {
                                foreach (var isAzureManagedInstance in new[] { false, true })
                                {
                                    yield return new CollectorTargetInfo
                                    {
                                        Engine = CollectorTargetEngine.SqlServer,
                                        IsAzureSqlDb = isAzureSqlDb,
                                        IsAzureManagedInstance = isAzureManagedInstance,
                                        IsAwsRds = isAwsRds,
                                        HasMsdbAccess = hasMsdbAccess,
                                        SqlMajorVersion = major,
                                    };
                                }
                            }
                        }
                    }
                }

                break;
        }
    }

    /// <summary>A bare SQL Server target, for the engine half of the dispatch gate alone.</summary>
    private static readonly CollectorTargetInfo SqlServerProbe = new() { Engine = CollectorTargetEngine.SqlServer };

    /// <summary>The same, one engine over — for the engine-KIND axis (#2530).</summary>
    private static readonly CollectorTargetInfo PostgresProbe = new() { Engine = CollectorTargetEngine.PostgreSql };

    /// <summary>
    /// The bare probe for an engine kind, or <c>null</c> for a kind this build does not recognise. Used only
    /// to ask which of the two reasons a kind gap has — a foreign DIALECT, which the dispatch gate's engine
    /// half stops, or the collector's own gate — so the message can name the real one. The gap itself is
    /// decided by <see cref="IsCollectedOnEngineKind(ICollectorSchemaInfo, string?)"/> over the sweep, never
    /// here.
    /// </summary>
    private static CollectorTargetInfo? ProbeForKind(string? engineKind) =>
        MonitoredEngineKind.EngineOf(engineKind) switch
        {
            CollectorTargetEngine.PostgreSql => PostgresProbe,
            CollectorTargetEngine.SqlServer => SqlServerProbe,
            _ => null,
        };

    /// <summary>The closing sentence both axes end on. One copy, because the two messages have to agree
    /// about what a permanent gap is; a second wording would eventually say something subtly different about
    /// whether it is worth chasing.</summary>
    private const string PermanentGapEpilogue =
        "This is a permanent engine capability gap, not a collection outage: checking collection health, " +
        "enabling a collector or starting a capture cannot change it.";

    /// <summary>
    /// Where the same question IS answered on this target, for the gaps that have a sibling (#2625).
    ///
    /// <para>
    /// "and never will" is the right thing to say about the SOURCE and the wrong thing to leave an operator
    /// with when another collector covers the question. A stock-PostgreSQL operator reading the
    /// <c>pg_wait_stats</c> gap message learns that Aurora's wait instrumentation is unreachable, which is
    /// true, and concludes that wait analysis is unavailable, which is false - <c>pg_wait_sampling</c> has
    /// been sampling the same events all along.
    /// </para>
    ///
    /// <para>
    /// Deliberately sparse. An entry is a promise that the named collector answers substantially the same
    /// question on the same target, and a pointer at something merely adjacent is worse than none: it sends
    /// the reader to a panel that does not answer what they asked, and spends the credibility the rest of
    /// this message depends on. A gap with no honest sibling gets no pointer.
    /// </para>
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, string> CoveredInsteadBy =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            /* Aurora's aurora_stat_system_waits() vs. the pg_wait_sampling extension: different sources,
               same question - which wait events this server is spending time in. */
            ["pg_wait_stats"] = "pg_wait_sampling",
        };

    /// <summary>
    /// The sentence appended to a permanent-gap message when <see cref="CoveredInsteadBy"/> names a sibling,
    /// and nothing at all when it does not.
    /// </summary>
    private static string CoveredInsteadSuffix(string collectorName) =>
        CoveredInsteadBy.TryGetValue(collectorName, out var sibling)
            ? $" The same question IS answered on this server by the {sibling} collector, which reads a " +
              $"different source for it — check that panel rather than this one."
            : string.Empty;

    /// <summary>
    /// True when SOME server of this engine edition runs <paramref name="collectorName"/> — i.e. the
    /// collector is not excluded by the engine alone.
    /// <para>Unknown (0) editions answer TRUE. So does an unknown collector name, matching
    /// <see cref="CollectorCatalog.AppliesTo(string, CollectorTargetInfo)"/>'s own true-on-miss default: a
    /// typo must not silently manufacture a permanent-gap claim. The test that scans the reads' collector
    /// names against the catalog is what keeps that default from hiding one.</para>
    /// </summary>
    public static bool IsCollectedOnEngineEdition(string collectorName, int engineEdition)
    {
        var definition = CollectorCatalog.Find(collectorName);

        /* True-on-miss, and it is the LOOKUP that owns that rule rather than the sweep: an unknown name has
           no gate to ask, so there is nothing to derive an answer from and the honest answer is "no claim". */
        return definition is null || IsCollectedOnEngineEdition(definition, engineEdition);
    }

    /// <summary>
    /// The same question asked of a DEFINITION rather than a catalog name. The by-name overload above is
    /// exactly this plus <see cref="CollectorCatalog.Find"/>'s true-on-miss lookup, so the two cannot answer
    /// differently — there is one sweep, not two.
    ///
    /// <para><b>Why the pair exists (#2518).</b> By name, this function can only ever be handed a gate that
    /// SHIPS, and a shipped gate is fixed at test time. Every assertion anyone can write against the by-name
    /// form is therefore a statement about today's collectors, and would pass just as well against a
    /// hard-coded set of gaps that happened to match them — which is precisely the failure the derivation
    /// exists to prevent. Taking a definition lets a caller hand the sweep a gate it CONTROLS and move it,
    /// so "the answer follows the gate" becomes something that can be demonstrated rather than believed.
    /// It is the same by-name/by-definition pair
    /// <see cref="CollectorCatalog.AppliesTo(ICollectorSchemaInfo, CollectorTargetInfo)"/> already carries,
    /// for the same reason: the definition is the thing that owns the answer, and the name is a way of
    /// finding one.</para>
    /// </summary>
    public static bool IsCollectedOnEngineEdition(ICollectorSchemaInfo definition, int engineEdition)
    {
        if (engineEdition == UnknownEngineEdition)
        {
            return true;
        }

        /* A PostgreSQL collector is not "missing" from a SQL Server engine edition — the question does not
           apply to it. Without this the sweep would report all eight PG definitions as permanent gaps on
           every SQL Server edition, because the dispatch gate it asks includes the engine half. */
        if (!CollectorCatalog.EngineMatches(definition, SqlServerProbe))
        {
            return true;
        }

        foreach (var target in TargetsWithEngineEdition(engineEdition))
        {
            if (CollectorCatalog.AppliesTo(definition, target))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// True when SOME server of this engine KIND runs <paramref name="collectorName"/> — the second axis
    /// (#2530), and the one that separates a PostgreSQL target from a SQL Server that has never connected.
    /// <para>An absent, unrecognised, or unclassifiable kind answers TRUE, as does an unknown collector name
    /// — the same true-on-miss default the edition axis carries, for the same reason: nothing to ask means
    /// nothing to claim.</para>
    /// </summary>
    public static bool IsCollectedOnEngineKind(string collectorName, string? engineKind)
    {
        var definition = CollectorCatalog.Find(collectorName);

        return definition is null || IsCollectedOnEngineKind(definition, engineKind);
    }

    /// <summary>
    /// The engine-KIND question asked of a DEFINITION rather than a catalog name — the pair exists for the
    /// reason the edition axis's does: by name this can only ever be handed a gate that ships, so nothing
    /// asserted through it could distinguish a derivation from a list that happens to match.
    ///
    /// <para><b>The whole dispatch gate, over a sweep (#2532).</b> #2530 asked only
    /// <see cref="CollectorCatalog.EngineMatches(ICollectorSchemaInfo, CollectorTargetInfo)"/> — the engine
    /// half — and deliberately left the collectors' own <c>AppliesTo</c> out of it, because a gate on a
    /// FIXABLE fact must never be reported as permanent. That is still the rule; what changed is that the
    /// sweep now separates the fixable facts from the one the kind fixes, so the question can be asked in
    /// full: <i>is there any target of this engine kind, under any combination of the facts the kind does not
    /// decide, for which this collector runs?</i> <see cref="TargetsWithEngineKind"/> varies the PostgreSQL
    /// version floors and the recovery state — an upgrade and a writer connection move those — and fixes
    /// <see cref="CollectorTargetInfo.IsAurora"/>, which nothing moves. So <c>pg_stat_io</c>'s PG16 floor and
    /// the writer-only autovacuum read keep the <c>unavailable</c> vocabulary, while <c>pg_wait_stats</c>
    /// reading <c>aurora_stat_system_waits()</c> is a permanent gap on stock PostgreSQL, which is what it
    /// is.</para>
    ///
    /// <para>It is still DERIVED in both directions: flip a definition's
    /// <see cref="ICollectorSchemaInfo.TargetEngine"/> and the dialect answer flips with it; shut its
    /// <c>AppliesTo</c> on Aurora-ness and the stock-PostgreSQL answer flips with that. Both are demonstrated
    /// by moving a gate rather than asserted about the shipped ones
    /// (<c>CollectorEngineCapabilityMovingGateTests</c>), and
    /// <c>EveryFactAPostgresGateReads_IsVariedBySweepOrFixedByKind</c> is what stops the sweep quietly
    /// leaving a fact at its default and manufacturing a gap for a collector that runs (#2518's twin).</para>
    ///
    /// <para><b>An unknown kind still claims nothing</b>, and the check is explicit rather than falling out
    /// of an empty sweep: <see cref="TargetsWithEngineKind"/> yields no shapes for a token this build does
    /// not recognise, and "no shape runs it" would otherwise read as the most confident claim in the
    /// vocabulary instead of the silence it has to be.</para>
    /// </summary>
    public static bool IsCollectedOnEngineKind(ICollectorSchemaInfo definition, string? engineKind)
    {
        /* Not known to be anything — the pre-#2530 state of every row, of every store an older service
           wrote, and of every token a newer one writes. No claim, which is the same silence
           UnknownEngineEdition keeps. */
        if (!MonitoredEngineKind.IsKnown(engineKind))
        {
            return true;
        }

        foreach (var target in TargetsWithEngineKind(engineKind))
        {
            if (CollectorCatalog.AppliesTo(definition, target))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// The <c>not_collected</c> explanation for a read whose collector cannot run on this server's engine, or
    /// <c>null</c> when the engine DOES support it (in which case the read keeps its own miss vocabulary —
    /// <c>empty</c> for a genuine all-clear, <c>unavailable</c> for a gap worth chasing).
    ///
    /// <para>Returning the decision and the words as ONE value is deliberate: a caller that asked "is it
    /// gated?" and then separately built a message could answer one question and print the other.</para>
    ///
    /// <para><b>KIND before EDITION (#2530).</b> A PostgreSQL target's <paramref name="engineEdition"/> is 0,
    /// which the edition axis reads as "no claim" — correctly, since it genuinely knows nothing. Asking the
    /// kind axis first is what turns that silence into the true answer, and it must stay first: reversing the
    /// order would return null for every PostgreSQL target and the read would fall back to
    /// <c>unavailable</c>, which is exactly the wrong-cause message this closes.</para>
    ///
    /// <para><paramref name="engineKind"/> is <c>null</c> for a target whose kind the store does not record —
    /// a pre-#2530 row, a server that has not connected since the rung landed, or a SKU with no engine-kind
    /// column at all (Lite, which has no PostgreSQL target seam). Null makes NO claim on this axis; the
    /// edition axis then answers exactly as it did before.</para>
    /// </summary>
    public static string? NotCollectedMessage(string serverName, int engineEdition, string? engineKind, string collectorName)
    {
        var definition = CollectorCatalog.Find(collectorName);

        /* True-on-miss on the name, once, so neither axis below has to re-decide it. */
        if (definition is null)
        {
            return null;
        }

        if (!IsCollectedOnEngineKind(definition, engineKind))
        {
            /* "runs X" rather than "is an X target", because the descriptions are noun phrases and one of them
               starts with a vowel — an indefinite article in the template would read "a Aurora PostgreSQL".
               Article agreement belongs in the template or nowhere, never in a per-entry special case that
               the next entry gets wrong; the same reasoning the capture-path phrasing already follows. */
            var engine = $"{serverName} runs {MonitoredEngineKind.DescribeEngineKind(engineKind)}. ";

            /* TWO reasons a collector is a kind gap, and they must not be described with one sentence
               (#2532). A DIALECT mismatch is stopped by the dispatch gate's engine half before the
               collector's own gate is consulted at all; a same-dialect gap — pg_wait_stats on stock
               PostgreSQL — is the collector's own AppliesTo, exactly as on the edition axis. Saying "written
               against PostgreSQL and never sent at another engine" about a PostgreSQL collector on a
               PostgreSQL server would be plainly false to the one operator best placed to notice. */
            if (ProbeForKind(engineKind) is { } probe && !CollectorCatalog.EngineMatches(definition, probe))
            {
                return engine +
                       $"The {collectorName} collector is written against " +
                       $"{DescribeTargetEngine(definition.TargetEngine)} and the dispatch gate's engine half never " +
                       $"sends it at another engine, so this server does not collect " +
                       $"{CapturePathOf(collectorName)}, and never will. {PermanentGapEpilogue}" +
                       CoveredInsteadSuffix(collectorName);
            }

            /* Deliberately the edition axis's own wording, verbatim past the engine name: the two are the
               same finding — the collector's own gate excludes every target of this shape — and two
               spellings of it would eventually disagree about whether it is worth chasing. */
            return engine +
                   $"The {collectorName} collector does not run on that engine — its own AppliesTo gate " +
                   $"excludes it — so this server does not collect {CapturePathOf(collectorName)}, and never " +
                   $"will. {PermanentGapEpilogue}" + CoveredInsteadSuffix(collectorName);
        }

        if (IsCollectedOnEngineEdition(definition, engineEdition))
        {
            return null;
        }

        /* "this server does not collect X" rather than "X is not collected", so the sentence reads correctly
           whether the capture path is singular ("the system_health extended-events ring buffer") or plural
           ("the Always On availability replica states"). Number agreement belongs in the template, not in a
           per-entry special case that the next entry would get wrong. */
        return $"{serverName} runs on {DescribeEngineEdition(engineEdition)} (EngineEdition {engineEdition}). " +
               $"The {collectorName} collector does not run on that engine — its own AppliesTo gate excludes it — " +
               $"so this server does not collect {CapturePathOf(collectorName)}, and never will. " +
               PermanentGapEpilogue + CoveredInsteadSuffix(collectorName);
    }

    /// <summary>What a gated-off collector would have captured. No entry is a vaguer sentence, never a wrong
    /// one — whichever axis called this has already decided that a gap exists.</summary>
    private static string CapturePathOf(string collectorName) =>
        CapturePathByCollector.TryGetValue(collectorName, out var described)
            ? described
            : "the data this read is served from";

    /// <summary>The engine a definition's query DIALECT targets, in words. Deliberately separate from
    /// <see cref="MonitoredEngineKind.DescribeEngineKind"/>: that describes a SERVER, which may be Aurora,
    /// and no collector is written against Aurora as opposed to PostgreSQL.</summary>
    private static string DescribeTargetEngine(CollectorTargetEngine engine) => engine switch
    {
        CollectorTargetEngine.PostgreSql => "PostgreSQL",
        _ => "SQL Server",
    };
}
