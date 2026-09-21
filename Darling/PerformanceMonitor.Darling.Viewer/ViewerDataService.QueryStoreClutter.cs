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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Query Store CLUTTER read for the Viewer (#3797) — the desktop half of <c>get_query_store_clutter</c>.
///
/// <para><b>No SQL here, on purpose.</b> The four arms run the SAME statements the MCP tool runs, from
/// <see cref="DarlingQueryStoreClutterReader"/> in <c>PerformanceMonitor.Darling.Storage</c>, and the verdict,
/// the reason tokens, the derived figures and the recommendation prose all come from
/// <see cref="QueryStoreClutter"/> beside it. That is the <c>DarlingPg*Reader</c> rule (#2530) applied to a
/// SQL Server read: a second copy of the composition would be a second place a database could be called
/// Critical, and the copy that drifts is never the one being read.</para>
///
/// <para><b>What IS here</b> is the projection — the grid cannot bind a nullable record graph, so each arm's
/// figures are flattened onto one row, and the stamps are passed through
/// <see cref="ViewerTimeHelper.ForDisplay"/> like every other timestamp the Viewer shows.</para>
///
/// <para><b>The measurements stay NUMBERS and nulls, and the em-dash is the XAML's job.</b> This is a grid
/// whose whole purpose is ranking, so a column has to sort by magnitude; a pre-formatted string sorts
/// lexicographically, which puts 9 above 1,234 while looking like a working sort — the same defect class as
/// a band sorted alphabetically. So every measured figure is a nullable numeric that WPF sorts correctly, and
/// each column's binding carries <c>StringFormat</c> for the value and <c>TargetNullValue</c> for the
/// absence, which is where the em-dash every other Viewer grid uses for "not measured" comes from. Only the
/// genuinely textual cells — the band, the reason tokens, the exclusion and the capture mode — are strings,
/// and those carry the dash themselves.</para>
///
/// <para><b>The replica exclusion is a COLUMN, never a filter.</b> A database whose <c>readonly_reason</c>
/// carries the readable-secondary bit stays in the grid with <c>Excluded</c> = "excluded" and
/// <c>ExcludedReason</c> naming the architecture; dropping the row would leave an operator who knows the
/// database exists unable to tell "not cluttered" from "not shown" (topology ruling, 2026-09-20).</para>
/// </summary>
public partial class ViewerDataService
{
    /// <summary>The Viewer's per-database clutter row: every arm flattened, every absence an em-dash.</summary>
    public sealed class QueryStoreClutterRow
    {
        public string DatabaseName { get; init; } = "";

        /// <summary>The band the composition assigned, spelled exactly as every other band on this wire
        /// spells it (<see cref="HealthSeverity"/>, #3703) so the row's colour and the MCP payload agree.</summary>
        public string Verdict { get; init; } = "";

        /// <summary>The reason tokens that raised the band, comma-joined — the colour is never the only
        /// evidence on the row.</summary>
        public string Reasons { get; init; } = "";

        /// <summary>"excluded" or an em-dash. Its own column beside <see cref="ExcludedReason"/> so the
        /// grid can be sorted and filtered by it.</summary>
        public string Excluded { get; init; } = "";

        /// <summary>Why the row is excluded, in the composition's own token
        /// (<c>qs_read_only_replica</c>) — never blank on an excluded row.</summary>
        public string ExcludedReason { get; init; } = "";

        /* Nullable throughout: a null is an arm the window held nothing for, and it must not sort or read
           as a zero. The grid renders it through TargetNullValue, so the absence looks like every other
           unmeasured cell in the Viewer while still sorting as "no value". */
        public double? RunsSlowestPct { get; init; }
        public double? SlowestSharePct { get; init; }
        public int? SlowestItemMsP50 { get; init; }
        public double? DominanceRatio { get; init; }
        public int? PlansPerQueryP95 { get; init; }
        public int? PlansPerQueryMax { get; init; }
        public double? NewPlansPerDay { get; init; }
        public double? OneShotFraction { get; init; }
        public int? DistinctPlans { get; init; }
        public string ActualState { get; init; } = "";

        /// <summary>The V137 (#3796) <c>query_capture_mode</c> verbatim. An em-dash is a health capture
        /// older than that rung — the mode was never asked for — and is NOT the engine's <c>NONE</c>.</summary>
        public string CaptureMode { get; init; } = "";

        public long? MaxPlansPerQuery { get; init; }
        public long? StaleQueryThresholdDays { get; init; }
        public double? PctOfCap { get; init; }

        /// <summary>When the options row this verdict read was captured, already on the display clock, so the
        /// column sorts chronologically rather than by the text of a formatted stamp.</summary>
        public DateTime? OptionsCaptured { get; init; }

        /// <summary>The composition's prose for this row's reasons, one sentence per reason — the same
        /// sentences the MCP payload and the web panel carry. Shown as the row's tooltip and in the
        /// detail pane under the grid.</summary>
        public string Recommendation { get; init; } = "";
    }

    /// <summary>The per-SERVER overhead proxy, already worded — the block the grid cannot hold because it
    /// does not belong to any one database.</summary>
    public sealed class QueryStoreOverheadSummary
    {
        public IReadOnlyList<QueryStoreOverheadWaitRow> Waits { get; init; } = Array.Empty<QueryStoreOverheadWaitRow>();

        /// <summary>The sleep waits <c>IgnoredWaitDefaults</c> drops at collection, named so the proxy is
        /// not mistaken for the whole of Query Store's cost.</summary>
        public string ExcludedWaitTypes { get; init; } = "";

        /// <summary>The clerk line, already saying whether an absence is "no captures" or "outside the
        /// collector's top 25" — a RANK, not a zero.</summary>
        public string MemoryClerk { get; init; } = "";

        /// <summary>Set when every Query-Store-bearing database on this server is a readable secondary:
        /// no per-database verdict is issued, and the sentence says to read the primary.</summary>
        public string? ServerNote { get; init; }
    }

    /// <summary>One non-sleep <c>QDS_*</c> wait type over the window.</summary>
    public sealed class QueryStoreOverheadWaitRow
    {
        public string WaitType { get; init; } = "";

        /// <summary>Null when no row in the window carried a knowable interval — a rate that cannot be
        /// formed, which is not a rate of zero.</summary>
        public double? WaitMsPerHour { get; init; }

        public long WaitMsTotal { get; init; }
        public long WaitingTasks { get; init; }
        public long MeasuredSeconds { get; init; }
        public DateTime LastObserved { get; init; }
    }

    /// <summary>Both halves of one read: the per-database rows and the one per-server overhead block.</summary>
    public sealed record QueryStoreClutterResult(
        List<QueryStoreClutterRow> Databases,
        QueryStoreOverheadSummary Overhead);

    /// <summary>
    /// The clutter view for one server over the toolbar's window. Runs the four shared arms, composes them
    /// with the shared arithmetic, and projects the result for the grid.
    ///
    /// <para><paramref name="databaseNames"/> is the tab's database filter, applied HERE rather than in the
    /// SQL: the arms' denominators are per server (how often a database was the slowest of all of them), so
    /// filtering inside the read would change what the remaining rows MEAN. Filtering the composed rows
    /// keeps every figure the figure it would have been with the filter off.</para>
    /// </summary>
    public async Task<QueryStoreClutterResult> GetQueryStoreClutterAsync(
        int serverId,
        DateTime startUtc,
        DateTime endUtc,
        IReadOnlyList<string>? databaseNames = null,
        CancellationToken cancellationToken = default)
    {
        var serverIds = new[] { serverId };
        var readCost = await DarlingQueryStoreClutterReader.GetReadCostAsync(_dataSource, serverIds, startUtc, endUtc, cancellationToken);
        var planChurn = await DarlingQueryStoreClutterReader.GetPlanChurnAsync(_dataSource, serverIds, startUtc, endUtc, cancellationToken);
        var config = await DarlingQueryStoreClutterReader.GetConfigAsync(_dataSource, serverIds, startUtc, endUtc, cancellationToken);
        var waits = await DarlingQueryStoreClutterReader.GetQdsWaitsAsync(_dataSource, serverIds, startUtc, endUtc, cancellationToken);
        var clerk = await DarlingQueryStoreClutterReader.GetQueryStoreClerkAsync(_dataSource, serverId, startUtc, endUtc, cancellationToken);

        var composed = QueryStoreClutter.Compose(readCost, planChurn, config);

        var wanted = databaseNames is { Count: > 0 }
            ? new HashSet<string>(databaseNames, StringComparer.OrdinalIgnoreCase)
            : null;

        var rows = composed
            .Where(r => wanted is null || wanted.Contains(r.DatabaseName))
            .Select(Project)
            .ToList();

        return new QueryStoreClutterResult(rows, ProjectOverhead(waits, clerk, config));
    }

    private static QueryStoreClutterRow Project(QueryStoreClutter.DatabaseClutter row)
    {
        var a = row.ReadCost;
        var b = row.PlanChurn;
        var c = row.Config;

        return new QueryStoreClutterRow
        {
            DatabaseName = row.DatabaseName,
            Verdict = row.Verdict.ToString(),
            Reasons = row.Reasons.Count == 0 ? Absent : string.Join(", ", row.Reasons),
            Excluded = row.Excluded ? "excluded" : Absent,
            ExcludedReason = row.ExcludedReason ?? Absent,
            RunsSlowestPct = a is null ? null : Math.Round(QueryStoreClutter.RunsSlowestPct(a), 1),
            SlowestSharePct = a is null ? null : Math.Round(a.SlowestSharePctP50, 1),
            SlowestItemMsP50 = a?.SlowestItemMsP50,
            DominanceRatio = a is null ? null : Round(QueryStoreClutter.DominanceRatio(a), 2),
            PlansPerQueryP95 = b?.PlansPerQueryP95,
            PlansPerQueryMax = b?.PlansPerQueryMax,
            NewPlansPerDay = b is null ? null : Round(QueryStoreClutter.NewPlansPerDay(b), 1),
            OneShotFraction = b is null ? null : Round(QueryStoreClutter.NeverSeenTwiceFraction(b), 3),
            DistinctPlans = b?.DistinctPlans,
            ActualState = c is null ? Absent : c.ActualState,
            /* A pre-rung capture and an engine-set NONE are different facts and must not render alike; the
               dash is the one this grid already uses for "not measured". */
            CaptureMode = c?.QueryCaptureMode is { Length: > 0 } mode ? mode : Absent,
            MaxPlansPerQuery = c?.MaxPlansPerQuery,
            StaleQueryThresholdDays = c?.StaleQueryThresholdDays,
            PctOfCap = c is null ? null : Round(QueryStoreClutter.PctOfCap(c), 1),
            OptionsCaptured = c is null ? null : ViewerTimeHelper.ForDisplay(c.CapturedAt),
            Recommendation = string.Join(" ", QueryStoreClutter.Recommendations(row)),
        };
    }

    private static QueryStoreOverheadSummary ProjectOverhead(
        IReadOnlyList<DarlingQueryStoreClutterReader.QdsWaitRow> waits,
        DarlingQueryStoreClutterReader.ClerkRow clerk,
        IReadOnlyList<DarlingQueryStoreClutterReader.ConfigRow> config)
    {
        var included = waits
            .Where(w => !QueryStoreClutter.IsExcludedWaitType(w.WaitType))
            .Select(w => new QueryStoreOverheadWaitRow
            {
                WaitType = w.WaitType,
                WaitMsPerHour = Round(QueryStoreClutter.WaitMsPerHour(w.RatedWaitMs, w.MeasuredSeconds), 1),
                WaitMsTotal = w.WaitMsTotal,
                WaitingTasks = w.WaitingTasksTotal,
                MeasuredSeconds = w.MeasuredSeconds,
                LastObserved = ViewerTimeHelper.ForDisplay(w.LastObserved),
            })
            .ToList();

        var clerkLine = clerk.Captures == 0
            ? "No memory_clerks capture in this window, so nothing is known about MEMORYCLERK_QUERYDISKSTORE."
            : clerk.ClerkSamples == 0
                ? "MEMORYCLERK_QUERYDISKSTORE never appeared in this window. memory_clerks stores the top 25 clerks over 1 MB per capture, so that is a RANK, not a zero."
                : string.Create(CultureInfo.CurrentCulture, $"MEMORYCLERK_QUERYDISKSTORE: {clerk.LatestMemoryMb:N1} MB latest, {clerk.MaxMemoryMb:N1} MB window max, on {clerk.ClerkSamples:N0} of {clerk.Captures:N0} captures.")
                  + (clerk.LatestClerkCapture == clerk.LatestCapture
                      ? " It is in the newest capture's top 25."
                      : " It has since dropped out of the collector's top 25, so the latest figure is its last observed footprint, not its current one.");

        return new QueryStoreOverheadSummary
        {
            Waits = included,
            ExcludedWaitTypes = string.Join(", ", QueryStoreClutter.ExcludedQdsWaitTypes),
            MemoryClerk = clerkLine,
            ServerNote = QueryStoreClutter.IsReplicaServer(config)
                ? "Every Query-Store-bearing database on this server is a readable secondary (readonly_reason bit 8): its Query Store is its primary's, replicated, and no per-database verdict is issued here. The overhead below is real on a replica and is reported. Read the primary for the clutter."
                : null,
        };
    }

    /// <summary>The em-dash every Viewer grid uses for a value that was not measured — never a zero. Only the
    /// TEXT cells carry it here; a numeric cell's absence is a null the XAML renders through
    /// <c>TargetNullValue</c>, so the column still sorts by magnitude.</summary>
    private const string Absent = "—";

    /// <summary>Rounds a nullable figure, keeping null as null — an unmeasured quantity never becomes 0.</summary>
    private static double? Round(double? value, int digits) => value.HasValue ? Math.Round(value.Value, digits) : null;
}
