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

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// Drill-down for a PostgreSQL-target pass (#3542) — the engine-sibling of <see cref="PgDrillDownCollector"/>
/// behind the same <see cref="IDrillDownCollector"/> seam, and the same walk: per finding, split the story
/// path into keys, run the drill-downs those keys ask for, keep whatever was collected when one fails, and let
/// shutdown residue unwind to the pass's one Information line (#2299) rather than swallowing it per finding.
///
/// <para>Skeleton in the plumbing lane: the walk is real, the drill-downs are declared by story-path key and
/// return immediately. Lane 7 fills the top-statement detail (<c>pg_statement_stats</c> joined to
/// <c>pg_statement_text</c> — normalised text, never live SQL, by the V86 privacy design); lane 6 the temp
/// offenders on the same read. The SQL Server twin's <c>CreationTimeClockFrameDisciplineTests</c> de-skew
/// counts are pinned by exact file path and do NOT list these files: <c>pg_statement_stats</c> stamps
/// <c>collection_time</c> host-UTC and needs no de-skew, and a <c>DeSkew</c> call added here would have to be
/// added to that table too.</para>
/// </summary>
public sealed partial class PgTargetDrillDownCollector : IDrillDownCollector
{
    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;

    public PgTargetDrillDownCollector(NpgsqlDataSource postgres, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logger = logger;
    }

    /// <summary>
    /// Enriches each finding's DrillDown dictionary based on its story path — <see cref="PgDrillDownCollector"/>'s
    /// walk with the PostgreSQL keys. The 0.5 display gate is the same: below it only a drill-down a content lane
    /// has marked cheap-and-required (the way the SQL Server config drill-down is) may run.
    /// </summary>
    public async Task EnrichFindingsAsync(List<AnalysisFinding> findings, AnalysisContext context)
    {
        ArgumentNullException.ThrowIfNull(findings);
        ArgumentNullException.ThrowIfNull(context);

        foreach (var finding in findings)
        {
            /* #2299: between findings is the natural abandon point — the per-finding catch below deliberately
               does NOT swallow shutdown residue, so this throw (and any residue from a drill-down mid-read)
               unwinds the pass to the service's single Information line. */
            context.CancellationToken.ThrowIfCancellationRequested();

            try
            {
                finding.DrillDown = new Dictionary<string, object>();
                var pathKeys = finding.StoryPath.Split(" → ", StringSplitOptions.RemoveEmptyEntries).ToHashSet(StringComparer.Ordinal);

                if (finding.Severity >= 0.5)
                {
                    /* Lane 16 of #3691: the deadlock exemplars, on EITHER deadlock root. On the measured population
                       the regular rate never reached 5 per hour (2 in the worst hour → 0.2), so PG_DEADLOCK_RATE
                       never roots a card there; what roots is the first-occurrence anomaly at the 1-per-hour bar,
                       and a drill-down keyed on the regular fact alone would run for nobody who has the problem.
                       Both facts are emitted only with a non-zero counter, so "on the path" is "deadlocks > 0". */
                    if (pathKeys.Contains(PgTargetFactKeys.DeadlockRate) || pathKeys.Contains(PgTargetFactKeys.AnomalyDeadlockRate))
                        await CollectDeadlockExemplarsAsync(finding, context);

                    if (pathKeys.Any(k => k.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal))
                        || pathKeys.Contains(PgTargetFactKeys.TempSpill))
                    {
                        await CollectTopStatementsAsync(finding, context, pathKeys);
                    }
                }

                if (finding.DrillDown.Count == 0)
                    finding.DrillDown = null;
            }
            catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
            {
                _logger?.LogError("[PgTargetDrillDownCollector] Drill-down failed for {StoryPath}: {ExceptionType}: {Message}",
                    finding.StoryPath, ex.GetType().Name, ex.Message);
                /* Don't null out — keep whatever was collected before the error. */
            }
        }
    }

    /// <summary>Kind-Unspecified for parameter binds — Npgsql 6+ rejects Kind-Utc against <c>timestamp</c>.</summary>
    private static DateTime AsNaive(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /* ── Drill-downs, one per file. ── */

    private partial Task CollectTopStatementsAsync(AnalysisFinding finding, AnalysisContext context, HashSet<string> pathKeys);

    private partial Task CollectDeadlockExemplarsAsync(AnalysisFinding finding, AnalysisContext context);
}
