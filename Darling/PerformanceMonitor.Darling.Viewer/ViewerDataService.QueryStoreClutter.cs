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
/// figures are flattened onto one row with the absences rendered as the em-dash every other Viewer grid uses
/// for "not measured", and the stamps passed through <see cref="ViewerTimeHelper.ForDisplay"/> like every
/// other timestamp the Viewer shows.</para>
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

        public string RunsSlowestPct { get; init; } = "";
        public string SlowestSharePct { get; init; } = "";
        public string SlowestItemMsP50 { get; init; } = "";
        public string DominanceRatio { get; init; } = "";
        public string PlansPerQueryP95 { get; init; } = "";
        public string PlansPerQueryMax { get; init; } = "";
        public string NewPlansPerDay { get; init; } = "";
        public string OneShotFraction { get; init; } = "";
        public string DistinctPlans { get; init; } = "";
        public string ActualState { get; init; } = "";

        /// <summary>The V137 (#3796) <c>query_capture_mode</c> verbatim. An em-dash is a health capture
        /// older than that rung — the mode was never asked for — and is NOT the engine's <c>NONE</c>.</summary>
        public string CaptureMode { get; init; } = "";

        public string MaxPlansPerQuery { get; init; } = "";
        public string StaleQueryThresholdDays { get; init; } = "";
        public string PctOfCap { get; init; } = "";

        /// <summary>When the options row this verdict read was captured, on the display clock.</summary>
        public string OptionsCaptured { get; init; } = "";

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
        public string WaitMsPerHour { get; init; } = "";
        public string WaitMsTotal { get; init; } = "";
        public string WaitingTasks { get; init; } = "";
        public string MeasuredSeconds { get; init; } = "";
        public string LastObserved { get; init; } = "";
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
            RunsSlowestPct = a is null ? Absent : Fixed(QueryStoreClutter.RunsSlowestPct(a), 1),
            SlowestSharePct = a is null ? Absent : Fixed(a.SlowestSharePctP50, 1),
            SlowestItemMsP50 = a is null ? Absent : Whole(a.SlowestItemMsP50),
            DominanceRatio = a is null ? Absent : Fixed(QueryStoreClutter.DominanceRatio(a), 2),
            PlansPerQueryP95 = b is null ? Absent : Whole(b.PlansPerQueryP95),
            PlansPerQueryMax = b is null ? Absent : Whole(b.PlansPerQueryMax),
            NewPlansPerDay = b is null ? Absent : Fixed(QueryStoreClutter.NewPlansPerDay(b), 1),
            OneShotFraction = b is null ? Absent : Fixed(QueryStoreClutter.NeverSeenTwiceFraction(b), 3),
            DistinctPlans = b is null ? Absent : Whole(b.DistinctPlans),
            ActualState = c is null ? Absent : c.ActualState,
            /* A pre-rung capture and an engine-set NONE are different facts and must not render alike; the
               dash is the one this grid already uses for "not measured". */
            CaptureMode = c?.QueryCaptureMode is { Length: > 0 } mode ? mode : Absent,
            MaxPlansPerQuery = c is null ? Absent : Whole(c.MaxPlansPerQuery),
            StaleQueryThresholdDays = c is null ? Absent : Whole(c.StaleQueryThresholdDays),
            PctOfCap = c is null ? Absent : Fixed(QueryStoreClutter.PctOfCap(c), 1),
            OptionsCaptured = c is null
                ? Absent
                : ViewerTimeHelper.ForDisplay(c.CapturedAt).ToString("yyyy-MM-dd HH:mm", CultureInfo.CurrentCulture),
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
                WaitMsPerHour = Fixed(QueryStoreClutter.WaitMsPerHour(w.RatedWaitMs, w.MeasuredSeconds), 1),
                WaitMsTotal = Whole(w.WaitMsTotal),
                WaitingTasks = Whole(w.WaitingTasksTotal),
                MeasuredSeconds = Whole(w.MeasuredSeconds),
                LastObserved = ViewerTimeHelper.ForDisplay(w.LastObserved).ToString("yyyy-MM-dd HH:mm", CultureInfo.CurrentCulture),
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

    /// <summary>The em-dash every Viewer grid uses for a value that was not measured — never a zero.</summary>
    private const string Absent = "—";

    private static string Fixed(double? value, int digits) =>
        value.HasValue ? value.Value.ToString("N" + digits.ToString(CultureInfo.InvariantCulture), CultureInfo.CurrentCulture) : Absent;

    private static string Fixed(double value, int digits) =>
        value.ToString("N" + digits.ToString(CultureInfo.InvariantCulture), CultureInfo.CurrentCulture);

    private static string Whole(long value) => value.ToString("N0", CultureInfo.CurrentCulture);
}
