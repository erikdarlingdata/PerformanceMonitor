/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// Collects facts from Darling's Postgres store for the analysis engine — Lite's
/// <c>DuckDbFactCollector</c> ported method-for-method (Phase-5 analysis slice AN2a): the same
/// 28 collect methods across the same seven partial files, the same fact keys / values /
/// metadata shapes, the same emission order, and the same per-method degrade-to-no-facts error
/// posture — a missing table or empty store yields "no facts", never an exception. Since #2826
/// that degradation is REPORTED rather than silent on both sides (see
/// <see cref="ReportCollectionFailure"/> and Lite's counterpart): the catches used to be empty,
/// which made a query cancelled by its command timeout byte-identical to a server that genuinely
/// had nothing to report, and on the dogfood box that hid 325 cancellations in one day. Lite's analysis SQL was
/// deliberately written in the PG-shared dialect and the V4 passthrough <c>v_&lt;table&gt;</c>
/// views exist precisely so these queries run VERBATIM — every query string here is
/// byte-identical to Lite's.
///
/// <para>
/// The documented PG-port deviations, none of which touch a query's text:
/// <list type="bullet">
/// <item><description>/* PG port: Lite wraps every read in <c>_duckDb.AcquireReadLock()</c> —
/// DuckDB is a single-writer embedded file and Lite serializes readers against the writer.
/// Postgres is MVCC; no read lock exists or is needed, so the lock line is dropped from every
/// method. */</description></item>
/// <item><description>/* PG port: parameters bind positionally as <c>$N</c> exactly like Lite's
/// DuckDB SQL already did, via Npgsql's positional <c>AddWithValue</c> (the PgFindingStore
/// pattern); every <see cref="DateTime"/> is bound naive-UTC Kind-Unspecified — the
/// PgCollectorRowWriter discipline, because Npgsql 6+ maps Kind-Utc to <c>timestamptz</c> and
/// rejects it against the store's naive <c>timestamp</c> columns. AnalysisContext times are
/// used as-is (host-UTC window semantics — no offset math). */</description></item>
/// <item><description>/* PG port: Lite's BigInteger-tolerant <c>ToInt64</c> exists because
/// DuckDB hands wide aggregates back boxed as <see cref="System.Numerics.BigInteger"/>; Npgsql
/// never does — Postgres returns <c>numeric</c> aggregates as <see cref="decimal"/>, which
/// <see cref="Convert.ToInt64(object)"/> already handles — so the BigInteger branch is dropped
/// (see <see cref="PgBlockingPairRowQuery.ToInt64"/>). */</description></item>
/// <item><description>/* PG port: the SQL moves from inline literals to <c>public const</c>
/// fields (text unchanged) so Darling.Tests can pin the dialect ungated — the
/// PgFindingStore/DarlingAlertReadAdapter convention. */</description></item>
/// </list>
/// No query used <c>NOW()</c>/<c>CURRENT_TIMESTAMP</c> (every window bound is a parameter), no
/// <c>QUALIFY</c> appears (that lives in BaselineProvider, a different slice), and the one
/// <c>any_value()</c> use is standard SQL:2023, in Postgres since 16 (the product's minimum PG
/// is 17) — all pinned by <c>PgFactCollectorTests</c>.
/// </para>
/// </summary>
public sealed partial class PgFactCollector : IFactCollector
{
    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;

    /// <summary>
    /// The logger is OPTIONAL and defaulted, matching <see cref="PgBaselineProvider"/>,
    /// <see cref="PgAnomalyDetector"/> and <see cref="PgPlanFetcher"/> in this project — so #2826
    /// cost no call-site churn, and a test constructing a collector without one still compiles.
    /// </summary>
    public PgFactCollector(NpgsqlDataSource postgres, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logger = logger;
    }

    /// <summary>
    /// Reports a fact-collection failure that is being swallowed, so a collector that CANNOT run is
    /// distinguishable from one that ran and found nothing (#2826).
    ///
    /// <para>Every collect method here ends in a catch that degrades to "no facts". That is the right
    /// behaviour — one unavailable table must not lose a server its other twenty-seven facts — but
    /// until now the catch body was empty, so a <c>PlanRegressionSql</c> cancelled by its inherited
    /// 30 s Npgsql deadline produced output byte-identical to <c>if (offenderCount == 0) return;</c>.
    /// On the dogfood box that happened 325 times in one day and plan-regression detection was, in
    /// effect, off. The collected data is unaffected either way, which is exactly why nothing else
    /// noticed.</para>
    ///
    /// <para>Three outcomes, three levels, because they need three different reactions:</para>
    /// <list type="bullet">
    /// <item><description><b>Command timeout</b> — WARNING. The query outgrew its deadline, which is
    /// a growth signal and the thing #2826 exists to surface. Classified by
    /// <see cref="PgBaselineProvider.IsCommandTimeout"/> — the house discipline of detecting this
    /// STRUCTURALLY (57014, or a <see cref="TimeoutException"/> in the chain) rather than by message
    /// text, because Npgsql renders its own client-side deadline as "Exception while reading from
    /// stream", which read literally says the network broke.</description></item>
    /// <item><description><b>The schema does not have what the query asked for</b> — DEBUG. Two
    /// SQLSTATEs, and BOTH are needed: 42P01 undefined_table and 42703 undefined_column. This is the
    /// case the original comments were actually written for, and they name both shapes —
    /// "Table may not exist" at most sites, "Columns may not exist yet (pre-migration)" at
    /// <c>CollectServerMetadataFactsAsync</c>, whose <c>engine_edition</c> / <c>product_version</c>
    /// arrived in a later migration rung. A rolling deploy that puts the analysis service ahead of a
    /// not-yet-migrated store (or the reverse) raises 42703 there, NOT 42P01, and classifying only
    /// the table case would log an ERROR every pass for the whole migration window — for exactly the
    /// transient, self-resolving condition this arm exists to keep quiet. It stays quiet by default;
    /// quiet is not the same as invisible, so it is still emitted and can be turned up.</description></item>
    /// <item><description><b>Anything else</b> — ERROR. Assuming a fault is a missing table is what
    /// produced this defect; an unrecognised failure is a fault until someone says otherwise.</description></item>
    /// </list>
    ///
    /// <para>The site names itself via <see cref="CallerMemberNameAttribute"/> rather than a literal,
    /// so a renamed or copy-pasted collect method cannot report under the wrong name.</para>
    /// </summary>
    private void ReportCollectionFailure(
        Exception ex,
        AnalysisContext context,
        [CallerMemberName] string collectMethod = "")
    {
        if (PgBaselineProvider.IsCommandTimeout(ex))
        {
            _logger?.LogWarning(
                "[PgFactCollector] {CollectMethod} did not finish within its command timeout on server {ServerId} ({ServerName}) — that analysis input is MISSING for this pass, which is not the same as the server having none. The store side logs this as 'canceling statement due to user request'. If it repeats, the window this query scans has outgrown the timeout: {Message}",
                collectMethod, context.ServerId, context.ServerName, ex.Message);
        }
        else if (ex is PostgresException { SqlState: "42P01" or "42703" })
        {
            _logger?.LogDebug(
                "[PgFactCollector] {CollectMethod} skipped on server {ServerId} ({ServerName}): the store does not have a table or column it reads (SQLSTATE {SqlState}), which is the pre-migration / version-skew case, so it contributes no facts. {Message}",
                collectMethod, context.ServerId, context.ServerName,
                ((PostgresException)ex).SqlState, ex.Message);
        }
        else
        {
            _logger?.LogError(
                "[PgFactCollector] {CollectMethod} failed on server {ServerId} ({ServerName}) and contributes no facts this pass: {Message}",
                collectMethod, context.ServerId, context.ServerName, ex.Message);
        }
    }

    public async Task<List<Fact>> CollectFactsAsync(AnalysisContext context)
    {
        var facts = new List<Fact>();

        await CollectWaitStatsFactsAsync(context, facts);
        FactCollectorHelpers.GroupGeneralLockWaits(facts, context);
        FactCollectorHelpers.GroupParallelismWaits(facts, context);
        await CollectBlockingFactsAsync(context, facts);
        await CollectBlockingChainFactsAsync(context, facts);
        await CollectDeadlockFactsAsync(context, facts);
        await CollectServerConfigFactsAsync(context, facts);
        await CollectMemoryFactsAsync(context, facts);
        await CollectDatabaseSizeFactAsync(context, facts);
        await CollectServerMetadataFactsAsync(context, facts);
        await CollectCpuUtilizationFactsAsync(context, facts);
        await CollectRunnableTaskFactsAsync(context, facts);
        await CollectIoLatencyFactsAsync(context, facts);
        await CollectTempDbFactsAsync(context, facts);
        await CollectMemoryGrantFactsAsync(context, facts);
        await CollectQueryStatsFactsAsync(context, facts);
        await CollectParameterSensitivityFactsAsync(context, facts);
        await CollectPlanRegressionFactsAsync(context, facts);
        await CollectBadActorFactsAsync(context, facts);
        await CollectPerfmonFactsAsync(context, facts);
        await CollectMemoryClerkFactsAsync(context, facts);
        await CollectPlanCacheFactsAsync(context, facts);
        await CollectMemoryPressureEventFactsAsync(context, facts);
        await CollectDatabaseConfigFactsAsync(context, facts);
        await CollectFileAutogrowthFactsAsync(context, facts);
        await CollectProcedureStatsFactsAsync(context, facts);
        await CollectActiveQueryFactsAsync(context, facts);
        await CollectRunningJobFactsAsync(context, facts);
        await CollectSessionFactsAsync(context, facts);
        await CollectTraceFlagFactsAsync(context, facts);
        await CollectServerPropertiesFactsAsync(context, facts);
        await CollectDiskSpaceFactsAsync(context, facts);
        await CollectPlanAdvisoryFactsAsync(context, facts);

        return facts;
    }

    /// <summary>
    /// Every query this collector executes, for the ungated dialect/hygiene pins in
    /// Darling.Tests (no QUALIFY, no bare NOW()/CURRENT_TIMESTAMP, no read_parquet, $N
    /// positional parameters only, and every FROM/JOIN target resolves to a V4 passthrough
    /// view or a V1/V2 table).
    /// </summary>
    public static IReadOnlyList<string> AllSql { get; } = new[]
    {
        WaitStatsSql,
        BlockingSql,
        BlockingChainSql,
        PgBlockingPairRowQuery.DmvSnapshotSql,
        DeadlocksSql,
        ServerConfigSql,
        MemoryStatsSql,
        DatabaseSizeSql,
        ServerMetadataSql,
        CpuUtilizationSql,
        RunnableTaskStatsSql,
        IoLatencySql,
        TempDbSql,
        MemoryGrantSql,
        QueryStatsSql,
        ParameterSensitivitySql,
        PlanRegressionSql,
        BadActorSql,
        PerfmonSql,
        MemoryClerkSql,
        PlanCacheStatsSql,
        MemoryPressureEventsSql,
        DatabaseConfigSql,
        FileAutogrowthSql,
        ProcedureStatsSql,
        ActiveQuerySql,
        RunningJobsSql,
        SessionStatsSql,
        TraceFlagsSql,
        ServerPropertiesSql,
        DiskSpaceSql,
        PlanAdvisorySql
    };

    // Single impl lives in PgBlockingPairRowQuery (shared with the pair-row reader);
    // delegate so the check isn't duplicated — the Lite shape, minus DuckDB's BigInteger case.
    private static long ToInt64(object value) => PgBlockingPairRowQuery.ToInt64(value);

    /// <summary>Kind-Unspecified for parameter binds — Npgsql 6+ rejects Kind-Utc against
    /// <c>timestamp</c> (the PgCollectorRowWriter / PgFindingStore discipline).</summary>
    private static DateTime AsNaive(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Unspecified);
}
