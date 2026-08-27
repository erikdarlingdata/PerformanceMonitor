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
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// Enriches findings with drill-down data from Darling's Postgres store — Lite's
/// <c>DrillDownCollector</c> ported method-for-method (Phase-5 analysis slice AN3): the same
/// enrichment dispatch (story-path keys → collectors, the D7/WS3/WS4 cheap-config drill-downs
/// running below the 0.5 display gate, the expensive collectors behind it), the same drill-down
/// dictionary keys and row shapes, the same per-finding catch-and-keep error posture, and the
/// same partial-file split (Blocking / Config / Plans / Queries / Storage). Lite's analysis SQL
/// was deliberately written in the PG-shared dialect and the V4 passthrough <c>v_&lt;table&gt;</c>
/// views exist precisely so these queries run VERBATIM — every query string here is
/// byte-identical to Lite's.
///
/// <para>
/// The documented PG-port deviations, none of which touch a query's text (the PgFactCollector
/// AN2a conventions):
/// <list type="bullet">
/// <item><description>/* PG port: Lite wraps every read in <c>_duckDb.AcquireReadLock()</c> —
/// DuckDB is a single-writer embedded file and Lite serializes readers against the writer.
/// Postgres is MVCC; no read lock exists or is needed, so the lock line is dropped from every
/// method. */</description></item>
/// <item><description>/* PG port: parameters bind positionally as <c>$N</c> exactly like Lite's
/// DuckDB SQL already did, via Npgsql's positional <c>AddWithValue</c>; every
/// <see cref="DateTime"/> is bound naive-UTC Kind-Unspecified — the PgCollectorRowWriter
/// discipline, because Npgsql 6+ maps Kind-Utc to <c>timestamptz</c> and rejects it against the
/// store's naive <c>timestamp</c> columns. AnalysisContext times are used as-is (host-UTC window
/// semantics — no offset math). */</description></item>
/// <item><description>/* PG port: Lite logs enrichment failures through its static
/// <c>AppLogger</c>; Darling injects an optional <see cref="ILogger"/> (the PgFindingStore
/// convention) — same message, same don't-null-out posture. */</description></item>
/// <item><description>/* PG port: the SQL moves from inline literals to <c>public const</c>
/// fields (text unchanged) so Darling.Tests can pin the dialect ungated — the
/// PgFindingStore/PgFactCollector convention. */</description></item>
/// </list>
/// No query uses <c>NOW()</c>/<c>CURRENT_TIMESTAMP</c> (every window bound is a parameter), no
/// <c>QUALIFY</c> appears, and the one <c>any_value()</c> use (the plan-regression drill-down,
/// same as the fact collector's plan-regression detector) is standard SQL:2023, in Postgres
/// since 16 (the product's minimum PG is 17) — all pinned by the AN3 tests.
/// </para>
/// </summary>
public sealed partial class PgDrillDownCollector
{
    private readonly NpgsqlDataSource _postgres;
    private readonly IPlanFetcher? _planFetcher;
    private readonly ILogger? _logger;
    private const int TextLimit = 500;

    public PgDrillDownCollector(NpgsqlDataSource postgres, IPlanFetcher? planFetcher = null, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _planFetcher = planFetcher;
        _logger = logger;
    }

    /// <summary>
    /// Every query this collector executes, for the ungated dialect/hygiene pins in
    /// Darling.Tests (no QUALIFY, no bare NOW()/CURRENT_TIMESTAMP, no read_parquet, $N
    /// positional parameters only, and every FROM/JOIN target resolves to a V4 passthrough
    /// view or a V1/V2 table). The reconstructed-chain query shares the
    /// <see cref="PgBlockingPairRowQuery"/> fragments with the fact collector, so the pair-row
    /// column order and the SPID-0 phantom filter cannot drift between the two consumers.
    /// </summary>
    public static IReadOnlyList<string> AllSql { get; } = new[]
    {
        TopDeadlocksSql,
        TopBlockingChainsSql,
        ReconstructedChainsSql,
        LockModeBreakdownSql,
        ConfigIssuesSql,
        PlanHandleLookupSql,
        PlanAdvisoryXmlSql,
        SpikePeakSql,
        QueriesAtSpikeSql,
        TopCpuQueriesSql,
        TopSpillingQueriesSql,
        ParameterSensitiveSql,
        RegressedQueriesSql,
        BadActorDetailSql,
        PendingGrantsSql,
        FileLatencyBreakdownSql,
        AutogrowthPercentFilesSql,
        TempDbBreakdownSql
    };

    /// <summary>
    /// Enriches each finding's DrillDown dictionary based on its story path.
    /// </summary>
    public async Task EnrichFindingsAsync(List<AnalysisFinding> findings, AnalysisContext context)
    {
        foreach (var finding in findings)
        {
            /* #2299: between findings is the natural abandon point — the per-finding catch below
               deliberately does NOT swallow shutdown residue, so this throw (and any residue from
               a drill-down mid-read) unwinds the pass to the service's single Information line. */
            context.CancellationToken.ThrowIfCancellationRequested();

            try
            {
                finding.DrillDown = new Dictionary<string, object>();
                var pathKeys = finding.StoryPath.Split(" → ", StringSplitOptions.RemoveEmptyEntries).ToHashSet();

                /* D7: the config drill-down is a single cheap config-table read and is
                   required to build config/RCSI/db-config advice, which legitimately scores
                   below 0.5 (RCSI-off base severity is 0.3). Collect it regardless of the
                   0.5 display gate. */
                if (pathKeys.Contains("DB_CONFIG"))
                    await CollectConfigIssues(finding, context);

                /* WS3: the percent-autogrowth drill-down is a single cheap config-table read
                   and is required to render the copy-paste MODIFY FILE fix for a
                   FILE_AUTOGROWTH_PERCENT finding, which scores 0.3 (advisory). Collect it
                   regardless of the 0.5 display gate, like the config drill-down above. */
                if (pathKeys.Contains("FILE_AUTOGROWTH_PERCENT"))
                    await CollectAutogrowthPercentFiles(finding, context);

                /* WS4: re-parse the top collected query plans to render the specific missing
                   indexes / warnings for a MISSING_INDEX or PLAN_WARNING finding, which scores 0.4
                   (advisory). Run regardless of the 0.5 display gate, like the config drill-downs
                   above — the fact carries only counts, so the strings live here. */
                if (pathKeys.Contains("MISSING_INDEX") || pathKeys.Contains("PLAN_WARNING"))
                    await CollectPlanAdvisoryDetail(finding, context, pathKeys);

                // Below the 0.5 display gate, only the cheap config drill-down above runs;
                // the expensive collectors (plan fetches, multi-row reads) are skipped.
                if (finding.Severity < 0.5)
                {
                    if (finding.DrillDown.Count == 0)
                        finding.DrillDown = null;
                    continue;
                }

                if (pathKeys.Contains("DEADLOCKS"))
                    await CollectTopDeadlocks(finding, context);

                if (pathKeys.Contains("BLOCKING_EVENTS"))
                    await CollectTopBlockingChains(finding, context);

                if (pathKeys.Contains("BLOCKING_CHAIN"))
                    await CollectReconstructedBlockingChains(finding, context);

                if (pathKeys.Contains("CPU_SPIKE"))
                    await CollectQueriesAtSpike(finding, context);

                if (pathKeys.Contains("CPU_SQL_PERCENT") || pathKeys.Contains("CPU_SPIKE"))
                    await CollectTopCpuQueries(finding, context);

                if (pathKeys.Contains("QUERY_SPILLS"))
                    await CollectTopSpillingQueries(finding, context);

                if (pathKeys.Contains("IO_READ_LATENCY_MS") || pathKeys.Contains("IO_WRITE_LATENCY_MS"))
                    await CollectFileLatencyBreakdown(finding, context);

                if (pathKeys.Contains("LCK") || pathKeys.Contains("LCK_M_S") || pathKeys.Contains("LCK_M_IS"))
                    await CollectLockModeBreakdown(finding, context);

                if (pathKeys.Contains("TEMPDB_USAGE"))
                    await CollectTempDbBreakdown(finding, context);

                if (pathKeys.Contains("MEMORY_GRANT_PENDING"))
                    await CollectPendingGrants(finding, context);

                if (pathKeys.Any(k => k.StartsWith("BAD_ACTOR_", StringComparison.OrdinalIgnoreCase)))
                    await CollectBadActorDetail(finding, context);

                if (pathKeys.Contains("PARAMETER_SENSITIVITY"))
                    await CollectParameterSensitiveQueries(finding, context);

                if (pathKeys.Contains("PLAN_REGRESSION"))
                    await CollectRegressedQueries(finding, context);

                // Plan analysis: for findings with top queries, analyze their cached plans
                await CollectPlanAnalysis(finding, context);

                // Remove empty drill-down dictionaries
                if (finding.DrillDown.Count == 0)
                    finding.DrillDown = null;
            }
            catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
            {
                _logger?.LogError("[PgDrillDownCollector] Drill-down failed for {StoryPath}: {ExceptionType}: {Message}",
                    finding.StoryPath, ex.GetType().Name, ex.Message);
                // Don't null out — keep whatever was collected before the error
            }
        }
    }

    private static void CollectPlanNodes(PlanNode node, List<PlanNode> nodes)
    {
        nodes.Add(node);
        foreach (var child in node.Children)
            CollectPlanNodes(child, nodes);
    }

    /// <summary>Kind-Unspecified for parameter binds — Npgsql 6+ rejects Kind-Utc against
    /// <c>timestamp</c> (the PgCollectorRowWriter / PgFactCollector discipline).</summary>
    private static DateTime AsNaive(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Unspecified);
}
