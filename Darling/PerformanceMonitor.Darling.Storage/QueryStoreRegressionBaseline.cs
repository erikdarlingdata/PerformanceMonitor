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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The 7-day baseline of the Query Store regressions read (get_query_store_regressions, its web page and the viewer's
/// Regressions grid), built from per-day totals instead of re-reading the wide rows on every call (#5506, on #5448).
///
/// <para><b>Why.</b> The read compares a recent window with the 7 days before it. Measured on a 210-server store, almost every
/// call took the wide-table arm and read every wide row of 7 days plus a margin, about 1.2 KB each, to rebuild a baseline
/// whose closed days never change. <c>collect.plan_regression_daily</c> (V168, #5448) already holds one row per plan per day:
/// executions and the sums of <c>avg_cpu_time_us * execution_count</c> and <c>avg_duration_us * execution_count</c>. A whole
/// day that is VALID (<c>built_seq = late_seq</c>, <see cref="PlanRegressionDaily.BuiltDaysSql"/>) is read from there; every
/// other part of the baseline window is read live from the source the read already uses.</para>
///
/// <para><b>The seam is exact, by construction.</b> Both halves assign an interval to the day of its latest snapshot's
/// <c>last_execution_time</c> (naive UTC), the column <c>plan_regression_daily</c> rows by. The baseline is the intervals with
/// <c>last_execution_time</c> in [baseline start, window start). The built half takes the rows of the run days (whole days that
/// are valid and lie wholly inside that range); the live half takes the rows in the range whose
/// <c>last_execution_time::date</c> is NOT a run day. Each row has one <c>last_execution_time</c>, so it is in exactly one half:
/// none is counted twice and none is dropped. The recent window is <c>last_execution_time &gt;= window start</c>, the same
/// timestamp, so no interval is in both the baseline and the recent window either (the old definition, by
/// <c>collection_time</c>, counted an interval that straddled the window start in both). The index ranges on
/// <c>first_execution_time</c> the live half uses (<see cref="LowEnd"/>, <see cref="HighStart"/>) skip the heap of the run
/// days; they only narrow the scan, the date test is what decides membership, and an interval's
/// <c>first_execution_time</c> is within a day before its <c>last_execution_time</c>
/// (<see cref="PlanRegressionDaily.BuildDaySql"/>'s bound).</para>
///
/// <para><b>Regular executions only.</b> <c>collect.query_store_interval_latest</c>, and so the daily totals, keep only
/// <c>execution_type_desc = 'Regular'</c>. So the whole read (baseline, both halves, and the recent window) counts Regular
/// executions: a baseline that kept Aborted and Exception runs on some days and not others would not add up, and a recent window
/// that kept them against a baseline that did not would flag every query that times out at a steady rate.
/// <c>get_query_store_aborts</c> reports the others.</para>
///
/// <para><b>Aborted and Exception runs beside the cost (#5506, Erik 2026-10-21).</b> Counting only Regular runs hides what a
/// timeout or an error does: one 30 s timeout among 1,000 fast runs barely moves an average, and blending them in would hide
/// it the same way. So the read reports THREE groups per query, baseline and recent, side by side: Regular (the cost the gate
/// compares), Aborted (timeouts, cancels) and Exception (errors), each with an execution count and CPU and duration per
/// execution (weighted by execution count, NULL when the count is 0), and a count per hour for each, because the recent window
/// and the baseline differ in length. The Aborted and Exception rows are read live from the wide table (or the raw table) for
/// both sides; the daily totals hold none. A query is flagged for COST (Regular CPU up more than <see cref="CostGatePercent"/>%),
/// for TIMEOUTS or ERRORS (that group's per-hour rate in the recent window at least <see cref="FailureRateMultiple"/> times the
/// baseline's, or any at all where the baseline had none, with at least <see cref="FailureMinRecentRuns"/> such runs in the
/// recent window), or for several; <c>regression_reason</c> says which.</para>
/// </summary>
public static class QueryStoreRegressionBaseline
{
    /// <summary>The cost gate (#5506 keeps today's): a query qualifies for <c>cost</c> when its Regular CPU per execution is
    /// more than this percent above its baseline.</summary>
    public const int CostGatePercent = PerformanceMonitor.Common.QueryStoreRegressionRules.CostGatePercent;

    /// <summary>#6781: a cost row must also add at least this many ms of CPU over the recent window
    /// (<see cref="PerformanceMonitor.Common.QueryStoreRegressionRules.CostMinAddedCpuMs"/>).</summary>
    public const int CostMinAddedCpuMs = PerformanceMonitor.Common.QueryStoreRegressionRules.CostMinAddedCpuMs;

    /// <summary>A rise in timeouts or errors qualifies when the recent per-hour rate is at least this many times the baseline's
    /// (#5506). A nonzero recent rate where the baseline had none qualifies whatever the multiple.</summary>
    public const int FailureRateMultiple = PerformanceMonitor.Common.QueryStoreRegressionRules.FailureRateMultiple;

    /// <summary>A rise in timeouts or errors needs at least this many such runs in the recent window (#5506), so one stray
    /// timeout on a quiet query does not flag it.</summary>
    public const int FailureMinRecentRuns = PerformanceMonitor.Common.QueryStoreRegressionRules.FailureMinRecentRuns;

    /// <summary>A timeouts-only or errors-only row is HIGH when its rate is at least this many times the baseline's (or the
    /// baseline had none), MEDIUM otherwise (#5506). Cost rows keep the duration bands; a failure row never gets CRITICAL,
    /// which stays the band of a duration more than doubled.</summary>
    public const int FailureHighMultiple = PerformanceMonitor.Common.QueryStoreRegressionRules.FailureHighMultiple;

    /// <summary>Rows with no cost rise (timeouts or errors only) may take at most this share of the row cap, as
    /// <c>cap / FailureShareDivisor</c>, so a flood of error rows cannot push every cost row past the cap (#5506). Cost rows
    /// take the rest, and either side takes what the other leaves free.</summary>
    public const int FailureShareDivisor = PerformanceMonitor.Common.QueryStoreRegressionRules.FailureShareDivisor;

    /// <summary>The partial index predicate and the read's predicate for the rows that are not Regular (#5506). The same text
    /// in both places, so the planner can match the read to the background partial index
    /// (<see cref="QueryStoreBackgroundIndexes.WideNonRegularFirstExec"/>). Without the index the read still answers, just
    /// slower. Hand-kept in step with that spec's <c>IndexDefinition</c>; the live test builds the index and checks the plan
    /// uses it.</summary>
    public const string NonRegularPredicate = "execution_type_desc <> 'Regular'";

    /// <summary>
    /// What <see cref="ResolveAsync"/> decided: the run days to read from the daily totals, and the two bounds of the live
    /// half's <c>first_execution_time</c> ranges. <see cref="LowEnd"/>: the live low range is
    /// <c>first_execution_time &lt; LowEnd</c>. <see cref="HighStart"/>: the live high range is
    /// <c>first_execution_time &gt;= HighStart</c>. With no run day both are the window start, so the low range alone is the
    /// whole baseline and the high range is empty.
    /// </summary>
    public sealed record Plan(DateOnly[] RunDays, DateTime LowEnd, DateTime HighStart)
    {
        /// <summary>How many days came from the daily totals.</summary>
        public int BuiltDayCount => RunDays.Length;

        /// <summary>The table arms' three Regular arms read <c>query_store_interval_latest</c> instead of the wide table
        /// (#5506, <see cref="LatestCoversRegularArms"/>). False on every fallback, so the arms read the wide table as before.</summary>
        public bool RegularFromLatest { get; init; }

        /// <summary>The recent window's whole built days read from the daily totals (#5506), one run of consecutive days.
        /// Empty by default and on every fallback, so the recent arm reads every row live, today's read.</summary>
        public DateOnly[] RecentDays { get; init; } = Array.Empty<DateOnly>();

        /// <summary>The recent arm's live low range is <c>first_execution_time &lt; RecentLowEnd</c>: the first recent day's
        /// midnight. Bound only when <see cref="RecentDays"/> is not empty.</summary>
        public DateTime RecentLowEnd { get; init; }

        /// <summary>The recent arm's live high range is <c>first_execution_time &gt;= RecentHighStart</c>: the last recent day's
        /// midnight, raised by <see cref="WithMeasuredHighStart"/> to the next midnight minus the measured margin. Bound only
        /// when <see cref="RecentDays"/> is not empty.</summary>
        public DateTime RecentHighStart { get; init; }

        /// <summary>
        /// The start of the hours the read HOLDS (#6750), the value of <see cref="HoursHeldStartToken"/>: set by the caller
        /// through <see cref="WithHoursHeldStart"/> to the later of the start the plan reads from and THIS SERVER's data start,
        /// the same instant the answer reports as <c>effective_start</c> (the per-server <see cref="DataWindowFloor"/> probe).
        /// The plan's start alone is the same for every server, and the store-wide chunk floor reads the oldest chunk of
        /// every server, so neither says when this server's data begins. Null for a plan built by hand: callers then bind the
        /// start they bind for the row filters, which is the read before this property existed. Used only by the
        /// <c>windows</c> CTE: every row filter keeps the baseline start, so the rows read do not depend on it.
        /// </summary>
        public DateTime? HoursHeldStart { get; init; }

        /// <summary>The start of the hours the FAILURE arm (<c>nonregular_rows</c>) holds (#6750), the value of
        /// <see cref="FailureHoursHeldStartToken"/>. Null keeps <see cref="HoursHeldStart"/>: on the raw path both arms read one
        /// table, so they hold the same hours.</summary>
        public DateTime? FailureHoursHeldStart { get; init; }

        /// <summary>The cost (Regular) baseline start the plan was chosen from, when <see cref="ResolveAsync"/> moved it down to
        /// the baseline's first midnight (#6750, <see cref="ExtendToBuiltFirstDay"/>). Null when it kept the start it was given.
        /// The table readers apply it to their spans with <see cref="WithPlanStart"/>, so the binds, the hours held and
        /// <c>cost_baseline_start</c> follow it.</summary>
        public DateTime? RegularStart { get; init; }
    }

    /// <summary>
    /// Where each arm of the table read starts (#6750), from <see cref="ResolveSpansAsync"/>. <see cref="RegularStart"/> serves the
    /// Regular (cost) baseline and the recent arm, <see cref="FailureStart"/> serves <c>nonregular_rows</c> (the Aborted and Exception
    /// runs, which only the wide table holds). <see cref="RegularFromLatestAllowed"/> false sends the Regular arms to the wide table,
    /// whose proven start is <see cref="FailureStart"/>, so the two starts are then equal. <see cref="LatestFloor"/> is latest's own
    /// floor for this server, for the floor clause of <see cref="LatestCoversRegularArms"/>. <see cref="BaselineDay"/> is the
    /// requested baseline start's midnight, set only where the Regular start came from latest's floor and so lies after it: the
    /// built days may then move the start down to it (<see cref="ExtendToBuiltFirstDay"/>).
    /// </summary>
    public sealed record ArmSpans(
        DateTime RegularStart,
        QueryStoreIntervalWide.WideStartBound RegularBound,
        DateTime FailureStart,
        QueryStoreIntervalWide.WideStartBound FailureBound,
        bool RegularFromLatestAllowed,
        DateTime? LatestFloor)
    {
        /// <summary>The requested baseline start rounded down to midnight, where the built first day may serve the cost
        /// baseline (#6750). Null on every other path.</summary>
        public DateTime? BaselineDay { get; init; }
    }

    /// <summary>
    /// The exact start of each arm of the table read (#6750). Before this every arm bound the gate's clamp, max(baseline start, raw's
    /// floor). On the large production store raw keeps about 4 days, so at 168 hours the cost baseline was empty and the recent arm
    /// was cut to raw's floor, while <c>query_store_interval_latest</c> (the Regular rows, 15 days) and the wide table (the Aborted and
    /// Exception rows, 10 days) still held them.
    /// <list type="bullet">
    /// <item>Raw's floor at or before <paramref name="baselineStart"/> (the clamp is the baseline start): every arm starts there, the
    /// read before #6750.</item>
    /// <item>The failure arm: <paramref name="wide"/>'s <see cref="QueryStoreIntervalWide.WideReadPlan.ReadStart"/>, the wide table's
    /// <see cref="QueryStoreIntervalWide.ExactBelowFloorStart"/> after its cadence and gap verdict.</item>
    /// <item>The Regular arms: <see cref="QueryStoreIntervalWide.ExactBelowFloorStart"/> over latest, from the baseline start rounded
    /// down to midnight (a baseline of 7 to 8 days, so its first day can come whole from the daily totals instead of a live edge),
    /// latest's coverage claim and latest's floor plus <see cref="QueryStoreIntervalWide.PurgeEdgeMargin"/>, with the cadence and gap
    /// verdict keyed on latest's floor. Where that start does not exist or the verdict refuses, the clamp. Where latest cannot serve
    /// from it (a pending batch, no coverage, coverage or floor after it) or the wide table reaches further, the Regular arms read the
    /// wide table from the failure arm's start. Latest drops whole days at 15 days, so on a full store its floor plus 26 hours is
    /// 02:00 of the baseline's first day. That start is not final: <see cref="ResolveAsync"/> moves it down to the midnight
    /// (<see cref="ArmSpans.BaselineDay"/>) when that day is a whole valid build at the head of the run
    /// (<see cref="ExtendToBuiltFirstDay"/>).</item>
    /// </list>
    /// Reads latest's coverage inputs and its floor (<see cref="QueryStoreIntervalLatest.PlainTableFloorSql"/>, which the retention
    /// pass warms after a drain) inside the caller's transaction. A fault propagates, so the caller falls back to raw.
    /// </summary>
    public static async Task<ArmSpans> ResolveSpansAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, DateTime baselineStart, QueryStoreIntervalWide.WideReadPlan wide,
        int commandTimeoutSeconds, ILogger? logger, CancellationToken cancellationToken)
    {
        baselineStart = DateTime.SpecifyKind(baselineStart, DateTimeKind.Unspecified);
        DateTime? filledSince;
        bool hasPending;
        await using (var inputs = new NpgsqlCommand(QueryStoreIntervalLatest.ReadSourceInputsSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds })
        {
            inputs.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = serverId });
            await using var reader = await inputs.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            filledSince = reader.IsDBNull(0) ? null : reader.GetDateTime(0);
            hasPending = reader.GetBoolean(1);
        }

        DateTime? latestFloor;
        await using (var floor = new NpgsqlCommand(QueryStoreIntervalLatest.PlainTableFloorSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds })
        {
            floor.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = serverId });
            latestFloor = await floor.ExecuteScalarAsync(cancellationToken) as DateTime?;
        }

        var clamp = DateTime.SpecifyKind(wide.ClampedStart, DateTimeKind.Unspecified);
        var failureStart = DateTime.SpecifyKind(wide.ReadStart, DateTimeKind.Unspecified);
        var wideOnly = new ArmSpans(failureStart, wide.StartBound, failureStart, wide.StartBound, false, latestFloor);
        if (clamp <= baselineStart)
        {
            /* Raw holds the whole baseline: every arm starts at the baseline start, as before #6750. */
            return new ArmSpans(clamp, wide.StartBound, failureStart, wide.StartBound, true, latestFloor);
        }

        if (hasPending || filledSince is not DateTime f)
        {
            return wideOnly;
        }

        var regularStart = clamp;
        var regularBound = QueryStoreIntervalWide.WideStartBound.RawFloor;
        var below = QueryStoreIntervalWide.ExactBelowFloorStart(clamp, baselineStart.Date, f, latestFloor);
        if (below is { } b && latestFloor is DateTime lf)
        {
            var verdict = await QueryStoreIntervalWide.CadenceAllowsAsync(
                connection, serverId, lf, commandTimeoutSeconds, logger, cancellationToken, QueryStoreIntervalLatest.TableName);
            if (verdict == QueryStoreIntervalWide.BelowFloorVerdict.Allowed)
            {
                regularStart = b.Start;
                regularBound = b.Bound;
            }
            else
            {
                regularBound = verdict == QueryStoreIntervalWide.BelowFloorVerdict.LogNotYetCovering
                    ? QueryStoreIntervalWide.WideStartBound.RawFloorLogNotYetCovering
                    : QueryStoreIntervalWide.WideStartBound.RawFloorSlowCadence;
            }
        }

        if (!LatestCoversRegularArms(f, hasPending, latestFloor, regularStart) || regularStart > failureStart)
        {
            return wideOnly;
        }

        logger?.LogDebug(
            "#6750 regressions spans for server {ServerId}: Regular arms from {RegularStart:o} on latest ({RegularBound}), failure arm from {FailureStart:o} ({FailureBound})",
            serverId, regularStart, regularBound, failureStart, wide.StartBound);
        /* #6750: a start from latest's floor that lies inside the baseline's first day may still move down to its midnight, where
           the built totals of that day serve it (ResolveAsync, ExtendToBuiltFirstDay). */
        var baselineDay = regularBound == QueryStoreIntervalWide.WideStartBound.TablePurgeEdge && regularStart > baselineStart.Date
                ? baselineStart.Date
                : (DateTime?)null;
        return new ArmSpans(regularStart, regularBound, failureStart, wide.StartBound, true, latestFloor) { BaselineDay = baselineDay };
    }

    /// <summary>
    /// <paramref name="spans"/> with the cost start <see cref="ResolveAsync"/> chose (#6750, <see cref="Plan.RegularStart"/>), so the
    /// Regular binds, <see cref="WithArmStarts"/>, the edge-margin lookup and <c>cost_baseline_start</c> all follow it. Unchanged when
    /// the plan kept the start it was given.
    /// </summary>
    public static ArmSpans WithPlanStart(ArmSpans spans, Plan plan) =>
        plan.RegularStart is DateTime start && start != spans.RegularStart ? spans with { RegularStart = start } : spans;

    /// <summary>
    /// <paramref name="plan"/> with <see cref="Plan.HoursHeldStart"/> set (#6750): the later of <paramref name="planStart"/>, the
    /// start the plan's statement reads from (the requested baseline start on the raw arm, the gate's clamp on the table arm),
    /// and <paramref name="dataStart"/>, where this server's Query Store data starts. <paramref name="dataStart"/> is the value
    /// the answer reports as <c>effective_start</c> (the <see cref="DataWindowFloor"/> probe over the baseline window), so the
    /// note and the divisors name one instant. Null (no coverage, or the probe failed) keeps <paramref name="planStart"/>.
    /// </summary>
    public static Plan WithHoursHeldStart(Plan plan, DateTime planStart, DateTime? dataStart) =>
        plan with { HoursHeldStart = dataStart is DateTime data && data > planStart ? data : planStart };

    /// <summary>
    /// The table read's plan with the start of each arm's hours held (#6750): <see cref="Plan.HoursHeldStart"/> for the Regular arms and
    /// <see cref="Plan.FailureHoursHeldStart"/> for the failure arm. An arm that starts below raw's floor (<paramref name="clamp"/>)
    /// holds its hours from its own start: that start is the table's proof of complete history for this server, and it is at least the
    /// server's own floor in that table plus a day. An arm at the clamp keeps the rule of <see cref="WithHoursHeldStart"/>, the later of
    /// its start and <paramref name="dataStart"/> (this server's raw data start), so a store whose raw tier holds the whole baseline
    /// reads as before.
    /// <para>The below-floor hours are deliberately not capped by <paramref name="dataStart"/>:</para>
    /// <list type="number">
    /// <item>A below-floor start is at least this server's own floor in that table plus <see cref="QueryStoreIntervalWide.PurgeEdgeMargin"/> (26 h).
    /// The coverage claim and the cadence and gap verdict prove complete history from there. The one exception is the cost start at the
    /// baseline's first midnight (<see cref="ExtendToBuiltFirstDay"/>): that day comes whole from a valid build, which summed the day while
    /// latest still held the day before it, so the baseline holds its hours from the midnight.</item>
    /// <item>Raw's data start for this server is at or after raw's floor, because the older raw chunks are dropped. Capping by it would
    /// shrink the divisor back to raw's few days and overstate the baseline failure rates.</item>
    /// <item>A young server is unaffected. Its own table floor plus 26 h is after raw's floor, so it gets no below-floor start, and its arm
    /// keeps the <see cref="WithHoursHeldStart"/> rule (the later of the arm start and its data start).</item>
    /// </list>
    /// </summary>
    public static Plan WithArmStarts(Plan plan, ArmSpans spans, DateTime clamp, DateTime? dataStart)
    {
        var at = DateTime.SpecifyKind(clamp, DateTimeKind.Unspecified);
        DateTime Held(DateTime armStart) => armStart < at ? armStart : dataStart is DateTime d && d > armStart ? d : armStart;
        return plan with { HoursHeldStart = Held(spans.RegularStart), FailureHoursHeldStart = Held(spans.FailureStart) };
    }

    /// <summary>Whether the plan's statement reads the recent window's days from the daily totals: latest arms and at least one
    /// recent day (#5506). Both readers pick the SQL text and bind the three extra parameters on this one answer.</summary>
    public static bool ReadsRecentDays(Plan plan) => plan.RegularFromLatest && plan.RecentDays.Length > 0;

    /// <summary>
    /// Whether the table arms may take their three Regular arms from <c>query_store_interval_latest</c> (#5506). That table
    /// holds the same latest snapshot per Regular interval as the wide table, at about a third of the row width, so a cold
    /// read takes about a third of the disk reads. It may answer only where it provably holds every row the arms reach from
    /// <paramref name="baselineStart"/>: a coverage row (<paramref name="filledSince"/>) at or before it, no pending batch (a batch
    /// that stored raw and wide but missed latest is not in the claim), and latest's own floor (<paramref name="latestFloor"/>,
    /// <see cref="QueryStoreIntervalLatest.PlainTableFloorSql"/>) at least <see cref="QueryStoreIntervalWide.PurgeEdgeMargin"/>
    /// before it. The floor clause is the wide gate's clause 3 for latest: retention drops latest by whole days of
    /// <c>first_execution_time</c>, and an interval that ran at or after the baseline start may have begun up to a day before it.
    /// Before #6750 the clause was not needed, because the start passed here was always the wide gate's clamp and latest keeps
    /// 15 days against the wide table's 10. The per-arm start (<see cref="ResolveSpansAsync"/>) reaches below that clamp, so the
    /// floor is now checked. A null floor (latest holds no row for the server) passes, as the clause did not exist before.
    /// Otherwise the arms read the wide table.
    /// <para>#6750: the clause is about the LIVE arms, the only part that reads latest's rows as they are now. When the cost
    /// baseline starts at its first midnight from the built totals (<see cref="ExtendToBuiltFirstDay"/>), the live low arm reads
    /// nothing below the run's first day, so <see cref="ResolveAsync"/> checks the start the live arms would read from without
    /// that day (latest's floor plus the margin), never the lower start the built day serves.</para>
    /// </summary>
    public static bool LatestCoversRegularArms(DateTime? filledSince, bool hasPending, DateTime? latestFloor, DateTime baselineStart)
    {
        var start = DateTime.SpecifyKind(baselineStart, DateTimeKind.Unspecified);
        return !hasPending && filledSince is DateTime f && f <= start
            && (latestFloor is not DateTime h || h + QueryStoreIntervalWide.PurgeEdgeMargin <= start);
    }

    /// <summary>
    /// The high live range's start raised from the last run day's midnight to the next midnight minus
    /// <paramref name="edgeMargin"/> (#5506), on the latest form only. A row of the high range has its last execution on a
    /// day after the run, so at or after that next midnight, and its first execution is within one interval before its last
    /// (both inside one Query Store interval, on the monitored server's clock). The measured margin
    /// (<see cref="QueryStoreIntervalWide.ResolveEdgeMarginAsync"/>) is at least the server's longest interval, so no row of
    /// the high range starts below the raised bound. Never below today's bound, so the full 26-hour margin leaves it as it
    /// is. Not applied to the wide form: measured on a production store, the narrower range flipped the wide table's plan
    /// from a bitmap scan to a slower plain index scan, while on latest it cut the last run day's node from about 116 MB to
    /// 12 MB read cold, the plan unchanged.
    /// </summary>
    public static Plan WithMeasuredHighStart(Plan plan, TimeSpan edgeMargin)
    {
        if (!plan.RegularFromLatest)
        {
            return plan;
        }

        if (plan.BuiltDayCount > 0)
        {
            var raised = plan.HighStart.AddDays(1) - edgeMargin;
            if (raised > plan.HighStart)
            {
                plan = plan with { HighStart = raised };
            }
        }

        /* The recent arm's high range, by the same argument: a row of it has its last execution at or after the last recent
           day's next midnight, and its first execution within one interval before that. */
        if (plan.RecentDays.Length > 0)
        {
            var raised = plan.RecentHighStart.AddDays(1) - edgeMargin;
            if (raised > plan.RecentHighStart)
            {
                plan = plan with { RecentHighStart = raised };
            }
        }

        return plan;
    }

    /// <summary>The plan that reads nothing from the daily totals: the whole baseline is live.</summary>
    public static Plan AllLive(DateTime windowStart) =>
        new(Array.Empty<DateOnly>(), DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified), DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified));

    /// <summary>
    /// Picks the run days for a baseline of [<paramref name="baselineStart"/>, <paramref name="windowStart"/>) from the built
    /// days of one server. A day is taken only when it is a WHOLE valid build (no watermark: a partial build leaves part of its
    /// day live, and the open days read live) that lies wholly inside the baseline: its midnight is at or after the baseline
    /// start and its next midnight is at or before the window start. The days must be one run of consecutive days, because the
    /// live half's index ranges are the two ends outside it: the LONGEST run is taken (the later one on a tie) and a valid day
    /// beside a stale one is read live. The read runs inside the caller's transaction, under a savepoint it always rolls back
    /// to, so a store without the table (or a failed statement) costs the speedup and leaves the transaction usable; the
    /// answer is then the all-live plan.
    ///
    /// <para><b>Two gates the built days need (#5506, the review of #6260).</b> (1) <b>Pending.</b> A day reads valid
    /// (<c>built_seq = late_seq</c>) even when a late batch for it failed its apply into <c>query_store_interval_latest</c> and
    /// went pending (the trigger bump rolls back with the failed apply), while the wide table already holds those executions.
    /// So the totals would undercount. PLAN_REGRESSION takes built days only after <c>QueryStoreIntervalLatest.ReadsTableAsync</c>
    /// says there is no pending batch (<c>ReadSourceInputsSql</c>); this read asks the same question first, and with any pending
    /// batch for the server it reads every day live. (2) <b>The raw floor.</b> On the raw arm
    /// (<paramref name="clampToRawFloor"/>) the live rows start where raw's oldest chunk does, and the tool's
    /// <c>effective_start</c> says so; a whole day below that floor must not come from the totals, or the baseline would hold
    /// days the notice says were not read. The start is raised to the floor (the same <c>query_store_stats</c> chunk catalog
    /// <c>RawChunkFloorSql</c> reads). The wide arm already passes the gate's clamped start.</para>
    ///
    /// <para><b>The Regular arms' table.</b> The same inputs also decide <see cref="Plan.RegularFromLatest"/>
    /// (<see cref="LatestCoversRegularArms"/>): a pending batch, or latest's coverage starting after the baseline start, sends
    /// the table arms back to the wide table, and so does any fault here (the all-live plan).</para>
    ///
    /// <para><b>The recent window's whole days (#5506).</b> With <paramref name="windowEnd"/> given, the same built days also
    /// give <see cref="Plan.RecentDays"/> (<see cref="ChooseRecent"/>), only where every gate holds: the Regular arms read
    /// latest (no pending batch, coverage at or before the baseline start), this is not the raw arm, and the window end is at
    /// or after latest's <c>applied_through</c>, read here in the same snapshot. The daily totals have no upper
    /// <c>collection_time</c> bound, so a window that ends before a row of the day was collected must read that day live.
    /// Any fault leaves no recent day.</para>
    /// </summary>
    public static async Task<Plan> ResolveAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, DateTime baselineStart, DateTime windowStart,
        int commandTimeoutSeconds, ILogger? logger, CancellationToken cancellationToken, bool clampToRawFloor = false, DateTime? windowEnd = null,
        ArmSpans? spans = null)
    {
        const string savepoint = "qs_regression_built_days";
        List<PlanRegressionDaily.BuiltDay> built;
        bool regularFromLatest;
        var recentAllowed = false;
        await transaction.SaveAsync(savepoint, cancellationToken);
        try
        {
            DateTime? filledSince;
            bool hasPending;
            bool hasTimescale;
            await using (var inputs = new NpgsqlCommand(QueryStoreIntervalLatest.ReadSourceInputsSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds })
            {
                inputs.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = serverId });
                await using var reader = await inputs.ExecuteReaderAsync(cancellationToken);
                await reader.ReadAsync(cancellationToken);
                filledSince = reader.IsDBNull(0) ? null : reader.GetDateTime(0);
                hasPending = reader.GetBoolean(1);
                hasTimescale = reader.GetBoolean(2);
            }

            if (hasPending)
            {
                logger?.LogDebug("#5506 regressions read: server {ServerId} has a pending interval_latest batch, so its baseline is read live", serverId);
                return AllLive(windowStart);
            }

            /* Decided on the start the caller passed (the wide gate's clamp on the table arm), before any raw clamp below:
               the raw arm reads no latest rows. */
            /* #6750: the latest floor clause. The table read passes the floor ResolveSpansAsync read (and its verdict); a caller with
               none reads it here, except on the raw arm, which reads no latest rows. */
            var latestFloor = spans?.LatestFloor;
            if (spans is null && !clampToRawFloor)
            {
                await using var floor = new NpgsqlCommand(QueryStoreIntervalLatest.PlainTableFloorSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
                floor.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = serverId });
                latestFloor = await floor.ExecuteScalarAsync(cancellationToken) as DateTime?;
            }

            /* #6750: checked on the start the live arms read from (baselineStart here), not on the built first day's midnight. */
            regularFromLatest = (spans?.RegularFromLatestAllowed ?? true) && LatestCoversRegularArms(filledSince, hasPending, latestFloor, baselineStart);

            /* Read before the built days: that read swallows its own fault, which leaves the transaction aborted. */
            if (regularFromLatest && !clampToRawFloor && windowEnd is DateTime end)
            {
                await using var applied = new NpgsqlCommand(QueryStoreIntervalLatest.AppliedThroughSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
                applied.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = serverId });
                recentAllowed = await applied.ExecuteScalarAsync(cancellationToken) is DateTime appliedThrough
                    && DateTime.SpecifyKind(end, DateTimeKind.Unspecified) >= appliedThrough;
            }

            if (clampToRawFloor && hasTimescale)
            {
                await using var floors = new NpgsqlCommand(QueryStoreIntervalLatest.RawChunkFloorSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
                if (await floors.ExecuteScalarAsync(cancellationToken) is DateTime rawFloor && rawFloor > baselineStart)
                {
                    baselineStart = rawFloor;
                }
            }

            /* #6750: from the baseline's first midnight when the built first day may serve it, the same date as before otherwise. */
            var builtFrom = spans?.BaselineDay is DateTime day && day < baselineStart.Date ? day : baselineStart.Date;
            built = await QueryStoreIntervalLatest.ReadBuiltDaysAsync(
                connection, transaction, serverId, builtFrom, commandTimeoutSeconds, logger,
                ex => ex is OperationCanceledException, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(ex, "#5506 regressions read: the built-days gate failed for server {ServerId}; reading the baseline live", serverId);
            return AllLive(windowStart);
        }
        finally
        {
            await transaction.RollbackAsync(savepoint, CancellationToken.None);
        }

        /* #6750: the cost baseline may start at its first midnight, from that day's built totals. Only on latest's arms: the
           wide form and the raw arm keep the start they were given. */
        var costStart = baselineStart;
        if (regularFromLatest && !clampToRawFloor && spans?.BaselineDay is DateTime baselineDay)
        {
            costStart = ExtendToBuiltFirstDay(built, baselineDay, baselineStart, windowStart);
        }

        var plan = Choose(built, costStart, windowStart) with { RegularFromLatest = regularFromLatest };
        if (costStart != baselineStart)
        {
            logger?.LogDebug("#6750 regressions read: server {ServerId}'s cost baseline starts at {CostStart:o}, its first day from the daily totals", serverId, costStart);
            plan = plan with { RegularStart = costStart };
        }

        return recentAllowed && windowEnd is DateTime windowEndValue
            ? WithRecentDays(plan, ChooseRecent(built, costStart, windowStart, windowEndValue))
            : plan;
    }

    /// <summary>
    /// The cost baseline's start, moved down to <paramref name="baselineDay"/> (the requested start's midnight) when the built
    /// totals of that day can serve it (#6750), else <paramref name="regularStart"/> unchanged. Moved only when all hold:
    /// <list type="bullet">
    /// <item><paramref name="regularStart"/> (latest's floor plus <see cref="QueryStoreIntervalWide.PurgeEdgeMargin"/>) lies after the
    /// midnight and inside that same day. On a full store latest drops whole days at 15 days, so the start is 02:00 of the first
    /// day, which <see cref="Choose"/> could never take whole.</item>
    /// <item>The day is a whole valid build in <paramref name="built"/> (<see cref="PlanRegressionDaily.BuiltDaysSql"/> returns only
    /// days at or after <c>filled_since::date + 1</c>), and the run <see cref="Choose"/> picks from the midnight starts on it: the
    /// days from it to the rest of the run are consecutive whole valid builds. A gap, or a stale first day, keeps the start.</item>
    /// </list>
    /// <para><b>Why the built day is exact although latest no longer holds the day before it.</b> The build of day D summed latest's
    /// rows while latest still held [D - 1, D + 1): the builder reaches 14 days back and latest keeps 15. A valid build
    /// (<c>built_seq = late_seq</c>) means no late row for D landed since. Rows dropped from latest after the build do not change the
    /// build. The 26-hour bound protects only the live read of latest, and with the run starting at the midnight the live low arm
    /// reads nothing below the run's first day.</para>
    /// </summary>
    internal static DateTime ExtendToBuiltFirstDay(IEnumerable<PlanRegressionDaily.BuiltDay> built, DateTime baselineDay, DateTime regularStart, DateTime windowStart)
    {
        var day = DateTime.SpecifyKind(baselineDay.Date, DateTimeKind.Unspecified);
        var start = DateTime.SpecifyKind(regularStart, DateTimeKind.Unspecified);
        if (start <= day || start >= day.AddDays(1))
        {
            return regularStart;
        }

        var run = Choose(built, day, windowStart).RunDays;
        return run.Length > 0 && run[0] == DateOnly.FromDateTime(day) ? day : regularStart;
    }

    /// <summary>The selection of <see cref="ResolveAsync"/>, over the built days it read.</summary>
    internal static Plan Choose(IEnumerable<PlanRegressionDaily.BuiltDay> built, DateTime baselineStart, DateTime windowStart)
    {
        var start = DateTime.SpecifyKind(baselineStart, DateTimeKind.Unspecified);
        var end = DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified);
        var run = LongestRun(WholeDays(built)
            .Where(d => d.ToDateTime(TimeOnly.MinValue) >= start && d.ToDateTime(TimeOnly.MinValue).AddDays(1) <= end));
        if (run.Count == 0)
        {
            return AllLive(windowStart);
        }

        var first = DateTime.SpecifyKind(run[0].ToDateTime(TimeOnly.MinValue), DateTimeKind.Unspecified);
        var last = DateTime.SpecifyKind(run[^1].ToDateTime(TimeOnly.MinValue), DateTimeKind.Unspecified);
        /* A row of a day after the run last_execution_time >= (last day + 1), and its first execution is within a day before it
           (BuildDaySql's own bound): first_execution_time >= the last run day's midnight. A row of a day before the run has a
           first execution below the first run day's midnight. */
        return new Plan(run.ToArray(), first, last);
    }

    /// <summary>
    /// The recent window's days to read from the daily totals (#5506): the whole valid builds D with D at or after the first
    /// midnight at or after <paramref name="windowStart"/>, D + 1 day at or before <paramref name="windowEnd"/>, and D - 1 day
    /// at or after <paramref name="baselineStart"/>, the longest run of consecutive days (the later on a tie), or none.
    ///
    /// <para><b>Why each bound.</b> The daily row of D holds the latest rows with <c>last_execution_time</c> in [D, D + 1),
    /// <c>first_execution_time</c> in [D - 1, D + 1) and <c>collection_time &gt;= D - 1</c> (<see cref="PlanRegressionDaily.BuildDaySql"/>).
    /// The recent arm keeps <c>last_execution_time &gt;= window start</c> (the first bound),
    /// <c>first_execution_time &lt;= window end + slack</c> (the second), and <c>collection_time &gt;= baseline start</c>:
    /// with D - 1 at or after the baseline start the build's collection bound is the tighter one, so the two keep the same
    /// rows. With D - 1 below it, a row of D collected in [D - 1, baseline start) would be in the totals and not in the live
    /// read. The recent arm's upper <c>collection_time</c> bound is the caller's <c>applied_through</c> gate.</para>
    /// </summary>
    internal static DateOnly[] ChooseRecent(IEnumerable<PlanRegressionDaily.BuiltDay> built, DateTime baselineStart, DateTime windowStart, DateTime windowEnd)
    {
        var baseline = DateTime.SpecifyKind(baselineStart, DateTimeKind.Unspecified);
        var start = DateTime.SpecifyKind(windowStart, DateTimeKind.Unspecified);
        var end = DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified);
        var firstMidnight = start == start.Date ? start : start.Date.AddDays(1);
        return LongestRun(WholeDays(built).Where(d =>
        {
            var midnight = d.ToDateTime(TimeOnly.MinValue);
            return midnight >= firstMidnight && midnight.AddDays(1) <= end && midnight.AddDays(-1) >= baseline;
        })).ToArray();
    }

    /// <summary>The plan with <paramref name="recentDays"/> as its recent days and their live range bounds: the low range ends
    /// at the first day's midnight, the high range starts at the last day's midnight (a row of a later day has its first
    /// execution within a day before its last, at or after the last day's next midnight).</summary>
    internal static Plan WithRecentDays(Plan plan, DateOnly[] recentDays) =>
        recentDays.Length == 0
            ? plan with { RecentDays = Array.Empty<DateOnly>() }
            : plan with
            {
                RecentDays = recentDays,
                RecentLowEnd = DateTime.SpecifyKind(recentDays[0].ToDateTime(TimeOnly.MinValue), DateTimeKind.Unspecified),
                RecentHighStart = DateTime.SpecifyKind(recentDays[^1].ToDateTime(TimeOnly.MinValue), DateTimeKind.Unspecified),
            };

    /// <summary>The whole builds (no watermark), each day once, in order.</summary>
    private static IEnumerable<DateOnly> WholeDays(IEnumerable<PlanRegressionDaily.BuiltDay> built) =>
        built.Where(b => b.Through is null).Select(b => b.Day);

    /// <summary>The longest run of consecutive days among <paramref name="candidates"/> (the later run on a tie), or none.</summary>
    private static List<DateOnly> LongestRun(IEnumerable<DateOnly> candidates)
    {
        var days = candidates.Distinct().OrderBy(d => d).ToList();
        if (days.Count == 0)
        {
            return days;
        }

        var bestFrom = 0;
        var bestLength = 0;
        var runFrom = 0;
        for (var i = 1; i <= days.Count; i++)
        {
            if (i == days.Count || days[i] != days[i - 1].AddDays(1))
            {
                var length = i - runFrom;
                if (length >= bestLength)
                {
                    bestFrom = runFrom;
                    bestLength = length;
                }

                runFrom = i;
            }
        }

        return days.GetRange(bestFrom, bestLength);
    }

    /// <summary>The tokens of the shared statements: the baseline start parameter, the run days (date[]), and the two bounds
    /// of the live half's <c>first_execution_time</c> ranges (timestamp). Each twin binds its own parameter numbers
    /// (<see cref="Bind"/>), because the MCP read has a row cap at $5 and the viewer's does not.</summary>
    public const string BaselineStartToken = "@@B";
    public const string RunDaysToken = "@@DAYS";
    public const string LowEndToken = "@@LOW";
    public const string HighStartToken = "@@HIGH";

    /// <summary>The start of the hours the read holds (#6750), a timestamp parameter used ONLY by the <c>windows</c> CTE
    /// (<c>base_hours</c> and <c>recent_hours</c>); <see cref="Plan.HoursHeldStart"/> is its value, this server's data start or the plan's start, whichever is later. Every row filter keeps
    /// <see cref="BaselineStartToken"/>. Bound right after the databases left out, on every form (MCP <c>$11</c>, viewer
    /// <c>$10</c>), so the recent days' three parameters follow it (MCP <c>$12</c> to <c>$14</c>, viewer <c>$11</c> to
    /// <c>$13</c>) and no number is left unbound.</summary>
    public const string HoursHeldStartToken = "@@HELD";

    /// <summary>The failure arm's baseline start (#6750): <c>nonregular_rows</c>' row filters, on every form (the raw arm binds it to
    /// the same value as <see cref="BaselineStartToken"/>). MCP <c>$12</c>, viewer <c>$11</c>. <see cref="Bind"/> replaces it before
    /// <see cref="BaselineStartToken"/>, whose text it starts with.</summary>
    public const string FailureBaselineStartToken = "@@BF";

    /// <summary>
    /// The table path's work_mem for the regressions statement (#6750), run by the MCP read and the Viewer's. Measured on the large
    /// production store, a cold 168 hour read with the full baseline spilled a 97 MB external merge in the recent arm's sort, over the
    /// store's 64 MB work_mem (the mcp and viewer roles set no override). SET LOCAL inside the read's own transaction, right before the
    /// statement, so it ends with that transaction and no other read pays it.
    /// </summary>
    public const string TableReadWorkMemSql = "SET LOCAL work_mem = '256MB'";

    /// <summary>The start of the hours the failure arm holds (#6750): the <c>windows</c> CTE's <c>base_fail_hours</c> and
    /// <c>recent_fail_hours</c>, which every Aborted and Exception per-hour term divides by. MCP <c>$13</c>, viewer <c>$12</c>.
    /// Replaced before <see cref="HoursHeldStartToken"/>, whose text it starts with.</summary>
    public const string FailureHoursHeldStartToken = "@@HELDF";

    /// <summary>The recent window's tokens (#5506): its days read from the daily totals (date[]), and the end of its live low
    /// range and the start of its live high range (timestamp). Only the recent-days form names them
    /// (<see cref="Statement"/> with <c>recentFromDaily</c>); MCP binds them as $14 to $16, the viewer as $13 to $15 (#6750 put <see cref="HoursHeldStartToken"/> and the two failure tokens before them).</summary>
    public const string RecentDaysToken = "@@RDAYS";
    public const string RecentLowEndToken = "@@RLOW";
    public const string RecentHighStartToken = "@@RHIGH";

    /// <summary>The row cap: <c>$5</c> in the MCP read (a parameter), the literal 50 in the viewer's (#5506).</summary>
    public const string LimitToken = "@@LIMIT";

    /// <summary>The databases to leave out (#6654): a <c>text[]</c> parameter, NULL when none, bound by the twin
    /// (<c>DatabaseFilter.ExcludeParameter</c>) as <c>$10</c> in the MCP read and <c>$9</c> in the viewer's.</summary>
    public const string ExcludeToken = "@@X";

    /// <summary>The statement with its tokens bound to the twin's parameter numbers, for example <c>$6</c>, <c>$7</c>,
    /// <c>$8</c>, <c>$9</c>.</summary>
    public static string Bind(
        string sql, string baselineStart, string runDays, string lowEnd, string highStart, string limit, string exclude,
        string hoursHeldStart, string failureBaselineStart, string failureHoursHeldStart,
        string? recentDays = null, string? recentLowEnd = null, string? recentHighStart = null)
    {
        if (recentDays is not null && recentLowEnd is not null && recentHighStart is not null)
        {
            sql = sql.Replace(RecentDaysToken, "(" + recentDays + "::date[])", StringComparison.Ordinal)
                     .Replace(RecentLowEndToken, "(" + recentLowEnd + "::timestamp)", StringComparison.Ordinal)
                     .Replace(RecentHighStartToken, "(" + recentHighStart + "::timestamp)", StringComparison.Ordinal);
        }

        return sql.Replace(LimitToken, limit, StringComparison.Ordinal)
           .Replace(ExcludeToken, exclude, StringComparison.Ordinal)
           .Replace(FailureHoursHeldStartToken, failureHoursHeldStart, StringComparison.Ordinal)
           .Replace(HoursHeldStartToken, hoursHeldStart, StringComparison.Ordinal)
           .Replace(FailureBaselineStartToken, failureBaselineStart, StringComparison.Ordinal)
           .Replace(BaselineStartToken, baselineStart, StringComparison.Ordinal)
           .Replace(RunDaysToken, "(" + runDays + "::date[])", StringComparison.Ordinal)
           .Replace(LowEndToken, "(" + lowEnd + "::timestamp)", StringComparison.Ordinal)
           .Replace(HighStartToken, "(" + highStart + "::timestamp)", StringComparison.Ordinal);
    }

    /// <summary>
    /// The whole statement both twins run, before <see cref="Bind"/> (#5506): the arms, the aggregates and the suffix, one
    /// factory so the twins cannot drift. <paramref name="recentFromDaily"/> (table arms on latest only) is the form that reads
    /// the recent window's whole days from the daily totals (<see cref="RecentDaysArmsSql"/>); every other form is the text
    /// it was before.
    /// </summary>
    public static string Statement(bool wide, bool useDailyTotals, bool regularFromLatest = false, bool recentFromDaily = false)
    {
        var recent = wide && regularFromLatest && recentFromDaily;
        return (wide ? TableArmsSql(regularFromLatest, recent) : RawArmsSql)
            + "\n" + AggregatesSql(useDailyTotals, recent) + "\n" + (recent ? RecentDaysSuffixSql : SuffixSql);
    }

    /// <summary>
    /// The raw read's arms (#5506). ONE dedupe over every Regular snapshot collected from the baseline start to the window end
    /// keeps the latest snapshot per interval (#1841); the baseline and the recent window then split that set by the
    /// interval's <c>last_execution_time</c>, so an interval is on one side only. The baseline's live half drops the run
    /// days. $1 server_id, $2 window start, $3 window end, $4 database filter. It has no <c>first_execution_time</c> index to
    /// skip the run days with, so this arm still reads the whole span; it is the fallback, and the baseline it computes is
    /// the same one.
    /// </summary>
    public const string RawArmsSql = """
        WITH latest AS (
            /* LOAD-BEARING (correctness, not just perf) - #1841. The rows are CUMULATIVE per-interval snapshots and the
               collector re-fetches the OPEN interval every cycle, so the SAME interval is stored repeatedly with a growing
               execution_count. Keep the LATEST snapshot per interval, once, over the whole span, so an interval that crosses
               the window start is the latest snapshot's alone. Every outcome is deduped here (#5506); the Regular cost arms
               below keep Regular only, because the daily totals hold no other kind, and the Aborted and Exception rows
               are their own arm (nonregular_rows). */
            SELECT
                database_name,
                query_id,
                plan_id,
                execution_type_desc,
                query_text,
                execution_count,
                avg_duration_us,
                avg_cpu_time_us,
                last_execution_time,
                ROW_NUMBER() OVER
                (
                    PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                    ORDER BY collection_time DESC, execution_count DESC
                ) AS rn
            FROM query_store_stats
            WHERE server_id = $1
            AND   collection_time >= @@B
            AND   collection_time <= $3
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),
        deduped_baseline AS (
            SELECT database_name, query_id, plan_id, execution_count, avg_duration_us, avg_cpu_time_us, 1 AS rn
            FROM latest
            WHERE rn = 1
            AND   execution_type_desc = 'Regular'
            AND   last_execution_time >= @@B
            AND   last_execution_time < $2
            AND   NOT (CAST(last_execution_time AS date) = ANY(@@DAYS))
        ),
        deduped_recent AS (
            SELECT database_name, query_id, plan_id, query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time, 1 AS rn
            FROM latest
            WHERE rn = 1
            AND   execution_type_desc = 'Regular'
            AND   last_execution_time >= $2
        ),
        nonregular_rows AS (
            /* #5506: the Aborted and Exception rows, the same dedupe, both sides in one set. is_recent splits them at the
               window start by the interval's last_execution_time, the line the Regular arms use. */
            SELECT database_name, query_id, execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time,
                   (last_execution_time >= $2) AS is_recent
            FROM latest
            WHERE rn = 1
            AND   execution_type_desc <> 'Regular'
            AND   last_execution_time >= @@BF
        ),
        """;

    /// <summary>
    /// The interval table's arms (#5506, #5809): <c>query_store_interval_wide</c> holds ONE row per interval, its latest
    /// snapshot, so no dedupe runs. The baseline's live half is two ranges of <c>first_execution_time</c>, the rows before
    /// the run days and the rows from the last run day on (<see cref="Plan"/>), so the heap of the run days is never
    /// touched; the date test inside each range is what decides membership. The recent arm keeps the interval's
    /// <c>last_execution_time</c> at or after the window start, the same line the baseline stops at.
    /// </summary>
    public static readonly string WideArmsSql = TableArms(regularFromLatest: false);

    /// <summary>
    /// <see cref="WideArmsSql"/> with its three Regular arms on <c>query_store_interval_latest</c> (#5506), used where
    /// <see cref="Plan.RegularFromLatest"/> holds. That table keeps one row per Regular interval identity, its latest
    /// snapshot (the wide table's identity minus <c>execution_type_desc</c>, which is always Regular there), so the arms read
    /// the same rows without the <c>execution_type_desc</c> filter, at about 400 bytes a row against about 1.1 KB, through
    /// the same <c>(server_id, first_execution_time)</c> btree per day partition. The Aborted and Exception arm stays on the
    /// wide table: latest holds none of those rows.
    /// </summary>
    public static readonly string LatestArmsSql = TableArms(regularFromLatest: true);

    /// <summary>
    /// <see cref="LatestArmsSql"/> with the recent window's whole days read from <c>plan_regression_daily</c> (#5506), used
    /// where <see cref="Plan.RecentDays"/> is not empty. Measured cold at 168 hours on a production store, the recent arm was
    /// almost all of the read: it read every Regular interval of the window. Here it reads two live ranges, the rows before
    /// the first recent day (<c>first_execution_time &lt; @@RLOW</c>) and the rows from the last one on
    /// (<c>&gt;= @@RHIGH</c>), each without the rows dated on a recent day, plus the days' per-plan totals. Each row has one
    /// <c>last_execution_time</c>, so it is in exactly one part (the seam argument of <see cref="ChooseRecent"/>). All three
    /// parts emit pre-weighted <c>execs</c>, <c>cpu_us</c> and <c>dur_us</c>, and the totals carry no <c>query_text</c>
    /// (<see cref="RecentDaysSuffixSql"/> reads the days' sample where the text table has none).
    /// </summary>
    public static readonly string RecentDaysArmsSql = TableArms(regularFromLatest: true, recentFromDaily: true);

    /// <summary><see cref="LatestArmsSql"/> or <see cref="WideArmsSql"/>, or <see cref="RecentDaysArmsSql"/> on latest with
    /// <paramref name="recentFromDaily"/>.</summary>
    public static string TableArmsSql(bool regularFromLatest, bool recentFromDaily = false) =>
        regularFromLatest ? (recentFromDaily ? RecentDaysArmsSql : LatestArmsSql) : WideArmsSql;

    /// <summary>The recent arm of <see cref="RecentDaysArmsSql"/>: two live ranges and the daily totals of the recent days.</summary>
    private static string RecentFromDailyCte => $$"""
        deduped_recent AS MATERIALIZED (
            SELECT database_name, query_id, plan_id, query_text, CAST(execution_count AS numeric) AS execs,
                   CAST(avg_cpu_time_us AS numeric) * execution_count AS cpu_us, CAST(avg_duration_us AS numeric) * execution_count AS dur_us, last_execution_time
            FROM {{QueryStoreIntervalLatest.TableName}}
            WHERE server_id = $1
            AND   collection_time >= @@B
            AND   collection_time <= $3
            AND   first_execution_time >= $2 - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   first_execution_time < @@RLOW
            AND   last_execution_time >= $2
            AND   NOT (CAST(last_execution_time AS date) = ANY(@@RDAYS))
            AND   ($4::text[] IS NULL OR database_name = ANY($4))

            UNION ALL

            SELECT database_name, query_id, plan_id, query_text, CAST(execution_count AS numeric) AS execs,
                   CAST(avg_cpu_time_us AS numeric) * execution_count AS cpu_us, CAST(avg_duration_us AS numeric) * execution_count AS dur_us, last_execution_time
            FROM {{QueryStoreIntervalLatest.TableName}}
            WHERE server_id = $1
            AND   collection_time >= @@B
            AND   collection_time <= $3
            AND   first_execution_time >= @@RHIGH
            AND   first_execution_time <= $3 + {{QueryStoreIntervalWide.FirstExecUpperSlackSql}}
            AND   last_execution_time >= $2
            AND   NOT (CAST(last_execution_time AS date) = ANY(@@RDAYS))
            AND   ($4::text[] IS NULL OR database_name = ANY($4))

            UNION ALL

            SELECT d.database_name, d.query_id, d.plan_id, CAST(NULL AS text) AS query_text, d.execs, d.cpu_us_sum AS cpu_us, d.dur_us_sum AS dur_us, d.last_exec AS last_execution_time
            FROM plan_regression_daily AS d
            WHERE d.server_id = $1
            AND   d.day = ANY(@@RDAYS)
            AND   ($4::text[] IS NULL OR d.database_name = ANY($4))
        ),
        """;

    private static string TableArms(bool regularFromLatest, bool recentFromDaily = false)
    {
        var regular = regularFromLatest ? QueryStoreIntervalLatest.TableName : QueryStoreIntervalWide.TableName;
        /* Latest holds Regular rows only, so its arms carry no outcome filter. */
        var onlyRegular = regularFromLatest ? "" : "\n    AND   execution_type_desc = 'Regular'";
        /* On latest the recent arm is MATERIALIZED (#5506, measured on a production store). Inlined, its narrower rows made the
           planner feed recent_performance's GROUP BY from the (server_id, database_name, query_id, first_execution_time) index
           in key order, one random heap read per row: a cold 168-hour read took 60.6 s. Materialized, it scans the
           (server_id, first_execution_time) range and sorts, the wide table's plan shape: 10.9 s. The wide text is unchanged. */
        var recentCte = regularFromLatest ? "deduped_recent AS MATERIALIZED (" : "deduped_recent AS (";
        /* #6750: the low live arm reads [@@B, @@LOW). When the run starts at the cost start (the first day from the daily totals)
           that range is empty, but the first_execution_time range below @@B still scanned latest's day before it: 16 MB and about
           0.2 s cold on the large production store, for no row. "@@LOW > @@B" compares two parameters, so the planner folds it
           into a one-time filter and that scan never runs. It holds for every row the arm could keep, so the answer is unchanged. */
        var arms = $$"""
        WITH deduped_baseline AS (
            SELECT database_name, query_id, plan_id, execution_count, avg_duration_us, avg_cpu_time_us, 1 AS rn
            FROM {{regular}}
            WHERE server_id = $1{{onlyRegular}}
            AND   first_execution_time >= @@B - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   first_execution_time < @@LOW
            AND   @@LOW > @@B
            AND   last_execution_time >= @@B
            AND   last_execution_time < $2
            AND   NOT (CAST(last_execution_time AS date) = ANY(@@DAYS))
            AND   ($4::text[] IS NULL OR database_name = ANY($4))

            UNION ALL

            SELECT database_name, query_id, plan_id, execution_count, avg_duration_us, avg_cpu_time_us, 1 AS rn
            FROM {{regular}}
            WHERE server_id = $1{{onlyRegular}}
            AND   first_execution_time >= @@HIGH
            AND   first_execution_time < $2
            AND   last_execution_time >= @@B
            AND   last_execution_time < $2
            AND   NOT (CAST(last_execution_time AS date) = ANY(@@DAYS))
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),
        {{recentCte}}
            SELECT database_name, query_id, plan_id, query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time, 1 AS rn
            FROM {{regular}}
            WHERE server_id = $1{{onlyRegular}}
            AND   collection_time >= @@B
            AND   collection_time <= $3
            AND   first_execution_time >= $2 - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   first_execution_time <= $3 + {{QueryStoreIntervalWide.FirstExecUpperSlackSql}}
            AND   last_execution_time >= $2
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),
        nonregular_rows AS (
            /* #5506: the Aborted and Exception rows of the baseline and the recent window, read live in one scan. The predicate
               is the partial background index's own (NonRegularPredicate), so on a store that has built it the scan reads only
               those rows; on one that has not, it reads what the Regular arms read, just slower. The daily totals hold no
               such row. is_recent splits at the window start by last_execution_time, as the Regular arms do. */
            SELECT database_name, query_id, execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time,
                   (last_execution_time >= $2) AS is_recent
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   {{NonRegularPredicate}}
            AND   first_execution_time >= @@BF - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
            AND   first_execution_time <= $3 + {{QueryStoreIntervalWide.FirstExecUpperSlackSql}}
            AND   last_execution_time >= @@BF
            AND   (last_execution_time < $2 OR collection_time <= $3)
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),
        """;
        if (!recentFromDaily)
        {
            return arms;
        }

        /* The recent-days form swaps the recent arm whole; the baseline arms and nonregular_rows are the latest text's. */
        var from = arms.IndexOf(recentCte, StringComparison.Ordinal);
        var to = arms.IndexOf("nonregular_rows AS (", StringComparison.Ordinal);
        if (from < 0 || to < from)
        {
            throw new InvalidOperationException("#5506: the recent arm of the latest text was not found");
        }

        return arms[..from] + RecentFromDailyCte + "\n" + arms[to..];
    }

    /// <summary>The run days' per-plan totals from <c>plan_regression_daily</c> (#5448), as <c>baseline_parts</c>' first part.
    /// Every day read here is a whole valid build, and the live arm holds every other row of the baseline window, so no
    /// interval is in both.</summary>
    private const string DailyPartSql = """
            SELECT
                d.database_name,
                d.query_id,
                d.plan_id,
                d.execs AS execs,
                d.cpu_us_sum AS cpu_us,
                d.dur_us_sum AS dur_us
            FROM plan_regression_daily AS d
            WHERE d.server_id = $1
            AND   d.day = ANY(@@DAYS)
            AND   ($4::text[] IS NULL OR d.database_name = ANY($4))

            UNION ALL

""";

    private const string LivePartSql = """
            SELECT
                b.database_name,
                b.query_id,
                b.plan_id,
                CAST(b.execution_count AS numeric) AS execs,
                CAST(b.avg_cpu_time_us AS numeric) * b.execution_count AS cpu_us,
                CAST(b.avg_duration_us AS numeric) * b.execution_count AS dur_us
            FROM deduped_baseline AS b
            WHERE b.rn = 1
""";

    /// <summary>
    /// The two aggregates both twins share, after their arms (#5506). <c>baseline_parts</c> is the run days' per-plan totals
    /// from <c>plan_regression_daily</c> (only when <paramref name="useDailyTotals"/>: with no run day the table is not named,
    /// so a store without it still answers) unioned with the live half's weighted totals; <c>baseline_performance</c> and
    /// <c>recent_performance</c> both divide the sum of cost by the sum of executions, the weighted per-execution average
    /// (before this the baseline was an average of interval averages). Cost is CPU and duration only: logical reads are not
    /// read.
    /// </summary>
    public static string AggregatesSql(bool useDailyTotals, bool recentPreWeighted = false) =>
        "        baseline_parts AS (\n" + (useDailyTotals ? DailyPartSql : "") + LivePartSql + "        ),\n"
        + (recentPreWeighted ? s_aggregatesTailPreWeightedSql : AggregatesTailSql);

    /// <summary>Today's <c>recent_performance</c>, which weights each interval's averages by its count.</summary>
    private const string RecentPerformanceSql = """
        recent_performance AS (
            SELECT
                database_name,
                query_id,
                MAX(query_text) AS query_text_sample,
                CAST(SUM(CAST(avg_duration_us AS numeric) * execution_count) AS double precision) / NULLIF(SUM(execution_count), 0) / 1000.0 AS avg_duration_ms,
                CAST(SUM(CAST(avg_cpu_time_us AS numeric) * execution_count) AS double precision) / NULLIF(SUM(execution_count), 0) / 1000.0 AS avg_cpu_time_ms,
                CAST(SUM(execution_count) AS bigint) AS exec_count,
                CAST(COUNT(DISTINCT plan_id) AS integer) AS plan_count,
                MAX(last_execution_time) AS last_execution_time
            FROM deduped_recent
            WHERE rn = 1
            GROUP BY database_name, query_id
        ),
        """;

    /// <summary>
    /// <c>recent_performance</c> over the recent-days arm (#5506), whose rows are already weighted. The same numeric sums and
    /// the same casts as <see cref="RecentPerformanceSql"/>: the sum of <c>CAST(avg AS numeric) * execution_count</c> per
    /// interval and the daily <c>cpu_us_sum</c>, built by that expression, add up exactly, so the output is the same.
    /// </summary>
    private const string RecentPerformancePreWeightedSql = """
        recent_performance AS (
            SELECT
                database_name,
                query_id,
                MAX(query_text) AS query_text_sample,
                CAST(SUM(dur_us) AS double precision) / NULLIF(SUM(execs), 0) / 1000.0 AS avg_duration_ms,
                CAST(SUM(cpu_us) AS double precision) / NULLIF(SUM(execs), 0) / 1000.0 AS avg_cpu_time_ms,
                CAST(SUM(execs) AS bigint) AS exec_count,
                CAST(COUNT(DISTINCT plan_id) AS integer) AS plan_count,
                MAX(last_execution_time) AS last_execution_time
            FROM deduped_recent
            GROUP BY database_name, query_id
        ),
        """;

    private static readonly string s_aggregatesTailPreWeightedSql = PreWeightedTail();

    private static string PreWeightedTail()
    {
        if (!AggregatesTailSql.Contains(RecentPerformanceSql, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("#5506: recent_performance was not found in the aggregates");
        }

        return AggregatesTailSql.Replace(RecentPerformanceSql, RecentPerformancePreWeightedSql, StringComparison.Ordinal);
    }

    private const string AggregatesTailSql = """
        baseline_performance AS (
            SELECT
                database_name,
                query_id,
                CAST(SUM(dur_us) AS double precision) / NULLIF(SUM(execs), 0) / 1000.0 AS avg_duration_ms,
                CAST(SUM(cpu_us) AS double precision) / NULLIF(SUM(execs), 0) / 1000.0 AS avg_cpu_time_ms,
                CAST(SUM(execs) AS bigint) AS exec_count,
                CAST(COUNT(DISTINCT plan_id) AS integer) AS plan_count
            FROM baseline_parts
            GROUP BY database_name, query_id
        ),
        recent_performance AS (
            SELECT
                database_name,
                query_id,
                MAX(query_text) AS query_text_sample,
                CAST(SUM(CAST(avg_duration_us AS numeric) * execution_count) AS double precision) / NULLIF(SUM(execution_count), 0) / 1000.0 AS avg_duration_ms,
                CAST(SUM(CAST(avg_cpu_time_us AS numeric) * execution_count) AS double precision) / NULLIF(SUM(execution_count), 0) / 1000.0 AS avg_cpu_time_ms,
                CAST(SUM(execution_count) AS bigint) AS exec_count,
                CAST(COUNT(DISTINCT plan_id) AS integer) AS plan_count,
                MAX(last_execution_time) AS last_execution_time
            FROM deduped_recent
            WHERE rn = 1
            GROUP BY database_name, query_id
        ),
        failure_performance AS (
            /* #5506: Aborted (timeouts, cancels) and Exception (errors) per query, baseline and recent side by side: the run
               count, and the summed cost the per-execution figures divide (weighted by execution count). */
            SELECT
                database_name,
                query_id,
                CAST(COALESCE(SUM(execution_count) FILTER (WHERE NOT is_recent AND execution_type_desc = 'Aborted'), 0) AS bigint) AS base_aborted_execs,
                CAST(SUM(CAST(avg_cpu_time_us AS numeric) * execution_count) FILTER (WHERE NOT is_recent AND execution_type_desc = 'Aborted') AS double precision) AS base_aborted_cpu_us,
                CAST(SUM(CAST(avg_duration_us AS numeric) * execution_count) FILTER (WHERE NOT is_recent AND execution_type_desc = 'Aborted') AS double precision) AS base_aborted_dur_us,
                CAST(COALESCE(SUM(execution_count) FILTER (WHERE is_recent AND execution_type_desc = 'Aborted'), 0) AS bigint) AS recent_aborted_execs,
                CAST(SUM(CAST(avg_cpu_time_us AS numeric) * execution_count) FILTER (WHERE is_recent AND execution_type_desc = 'Aborted') AS double precision) AS recent_aborted_cpu_us,
                CAST(SUM(CAST(avg_duration_us AS numeric) * execution_count) FILTER (WHERE is_recent AND execution_type_desc = 'Aborted') AS double precision) AS recent_aborted_dur_us,
                CAST(COALESCE(SUM(execution_count) FILTER (WHERE NOT is_recent AND execution_type_desc = 'Exception'), 0) AS bigint) AS base_exception_execs,
                CAST(SUM(CAST(avg_cpu_time_us AS numeric) * execution_count) FILTER (WHERE NOT is_recent AND execution_type_desc = 'Exception') AS double precision) AS base_exception_cpu_us,
                CAST(SUM(CAST(avg_duration_us AS numeric) * execution_count) FILTER (WHERE NOT is_recent AND execution_type_desc = 'Exception') AS double precision) AS base_exception_dur_us,
                CAST(COALESCE(SUM(execution_count) FILTER (WHERE is_recent AND execution_type_desc = 'Exception'), 0) AS bigint) AS recent_exception_execs,
                CAST(SUM(CAST(avg_cpu_time_us AS numeric) * execution_count) FILTER (WHERE is_recent AND execution_type_desc = 'Exception') AS double precision) AS recent_exception_cpu_us,
                CAST(SUM(CAST(avg_duration_us AS numeric) * execution_count) FILTER (WHERE is_recent AND execution_type_desc = 'Exception') AS double precision) AS recent_exception_dur_us,
                MAX(last_execution_time) FILTER (WHERE is_recent) AS recent_last_time
            FROM nonregular_rows
            GROUP BY database_name, query_id
        )
        """;

    /// <summary>
    /// The projection, the flags, the ranking and the cap of both twins (#5506), after the aggregates. Bound with
    /// <see cref="Bind"/>: <c>@@LIMIT</c> is the row cap. A query is a candidate when it has recent Regular runs, or recent
    /// Aborted or Exception runs reaching <see cref="FailureMinRecentRuns"/>; <c>is_cost</c> is Regular CPU up more than
    /// <see cref="CostGatePercent"/>% with at least <see cref="CostMinAddedCpuMs"/> ms of added CPU over the window
    /// (#6781, <see cref="PerformanceMonitor.Common.QueryStoreRegressionRules.CostTestSql"/>), <c>is_timeouts</c> and <c>is_errors</c> are that group's recent per-hour rate at least
    /// <see cref="FailureRateMultiple"/> times the baseline's (any at all where the baseline had none) with at least
    /// <see cref="FailureMinRecentRuns"/> recent runs. The per-hour rates divide by each window's own length (the baseline
    /// window is [baseline start, window start), the recent window [window start, window end]).
    ///
    /// <para><b>Severity.</b> A cost row keeps the duration bands (CRITICAL over 100%, HIGH over 50%, MEDIUM over 25%, else LOW).
    /// A failure row is HIGH when its rate is at least <see cref="FailureHighMultiple"/> times the baseline's or the baseline
    /// had none, else MEDIUM; a row with both takes the higher band. A failure never reads CRITICAL: that stays the band of a
    /// query whose duration more than doubled, so a timeout flood cannot outrank the worst cost regressions.</para>
    ///
    /// <para><b>The cap.</b> Cost rows (including rows that also have a failure reason) are ranked by added duration, failure-only
    /// rows by their recent failed runs. Failure-only rows take at most <c>cap / <see cref="FailureShareDivisor"/></c> slots
    /// (rounded down), more only when there are fewer cost rows than the rest of the cap; cost rows take every other slot. So a flood of
    /// error rows cannot push the cost rows past the cap, and a store with no cost regression still shows its failures. Cost rows
    /// come first in the output.</para>
    /// </summary>
    public static readonly string SuffixSql = Suffix("f.query_text_sample", "r.query_text_sample");

    /// <summary>The comments around the self-statement test in <c>qualified</c>, so <see cref="WithSelfKeys"/> finds it in any form of
    /// <see cref="Statement"/>. Both callers apply <see cref="WithSelfKeys"/> BEFORE <see cref="QueryStoreIntervalWide.WithEdgeMargin"/>,
    /// and the markers carry no margin literal, so the order does not matter for finding them. It matters for one detail: on the
    /// recent-days forms the replaced region holds <see cref="RecentDaysTextProbe"/>, the one place the margin appears inside the self
    /// test, so the substitution removes an occurrence <see cref="QueryStoreIntervalWide.WithEdgeMargin"/> would otherwise narrow. That
    /// call is a plain replace of every occurrence, so nothing depends on it.</summary>
    private const string SelfTestOpen = "/* self-test: */ (";

    private const string SelfTestClose = ") /* :self-test */";

    /// <summary>
    /// #6750 part 3: the self-statement test once the key table is backfilled. <see cref="QueryStoreSelfKeys"/> holds the key of every
    /// stored text that carries the marker, so a query whose text is in <c>query_store_text</c> is the monitor's own exactly when its
    /// key is there, and the test reads no text. The <c>MAX(query_text)</c> sample test is a second conjunct, tested inline with no
    /// I/O, and it runs on every row, not only on a row with no text row as in the probe's <c>COALESCE</c>. The two tests differ in
    /// one case: a query whose stored text is not the monitor's but whose inline sample carries the marker. The probe lists it, and
    /// this test hides it. That case does not occur on a store today: under the separate text fetch the inline text is NULL, and the
    /// rows that carried it before the cutover were dropped at collection when they carried the marker. The recent-days form's extra
    /// sample from latest is not read here, for the same reason. <c>f.query_text_sample</c> is hardcoded although <see cref="Suffix"/>
    /// takes the sample as a parameter: it resolves through <c>flagged AS f</c> to <c>joined</c>'s <c>r.query_text_sample</c>, so it
    /// exists in every form, the pre-weighted recent-days one included.
    /// </summary>
    private static readonly string SelfKeysTest =
        QueryStoreSelfStatement.NotSelfOfKey("$1", "f.database_name", "f.query_id") + " AND " + QueryStoreSelfStatement.NotSelfOfText("f.query_text_sample");

    /// <summary>
    /// #6750 part 3: <paramref name="sql"/> (any bound form of <see cref="Statement"/>) with the self-statement text probe replaced by
    /// the key-table test (<see cref="SelfKeysTest"/>). The caller passes it only when <see cref="QueryStoreSelfKeys.ReadyAsync"/>
    /// answered true inside the read's own snapshot. On every store today the rows, the ranks and every total are the same (the one
    /// case where they could differ is in <see cref="SelfKeysTest"/>'s summary): only the cost changes, from one
    /// primary-key probe of the text table per flagged row to a primary-key probe of the key table, which holds no text. A text with no test in it is returned
    /// as it is, so it keeps the probe.
    /// </summary>
    public static string WithSelfKeys(string sql)
    {
        var open = sql.IndexOf(SelfTestOpen, StringComparison.Ordinal);
        var close = open < 0 ? -1 : sql.IndexOf(SelfTestClose, open, StringComparison.Ordinal);
        return close < 0 ? sql : string.Concat(sql.AsSpan(0, open + SelfTestOpen.Length), SelfKeysTest, sql.AsSpan(close));
    }

    /// <summary>
    /// <see cref="SuffixSql"/> for the recent-days form (#5506). The daily totals hold no <c>query_text</c>, so the
    /// <c>MAX(query_text)</c> sample covers only the live ranges. Where the text table has no row for the key (the only
    /// place the sample is read: the self-statement test and the projection), the recent days' own <c>MAX(query_text)</c>
    /// is probed from latest and the larger kept. <c>GREATEST</c> and <c>MAX</c> both ignore NULLs, so this is
    /// <c>MAX</c> over the whole recent set, today's sample. <c>COALESCE</c> runs the probe only when the lookup is NULL.
    /// </summary>
    public static readonly string RecentDaysSuffixSql =
        Suffix($"GREATEST(f.query_text_sample, {RecentDaysTextProbe("f")})", $"GREATEST(r.query_text_sample, {RecentDaysTextProbe("r")})");

    /// <summary>The recent days' <c>MAX(query_text)</c> for one key, over exactly the rows today's recent arm keeps on those
    /// days (its own predicates plus the date test): a probe of latest's <c>(server_id, database_name, query_id,
    /// first_execution_time)</c> index.</summary>
    private static string RecentDaysTextProbe(string alias) =>
        $"(SELECT MAX(l.query_text) FROM {QueryStoreIntervalLatest.TableName} AS l WHERE l.server_id = $1 AND l.database_name = {alias}.database_name AND l.query_id = {alias}.query_id"
        + $" AND l.first_execution_time >= $2 - {QueryStoreIntervalWide.PurgeEdgeMarginSql} AND l.first_execution_time <= $3 + {QueryStoreIntervalWide.FirstExecUpperSlackSql}"
        + " AND CAST(l.last_execution_time AS date) = ANY(@@RDAYS) AND l.last_execution_time >= $2"
        + " AND l.collection_time >= @@B AND l.collection_time <= $3)";

    /// <summary>
    /// The suffix over the shared arms. <c>windows</c> holds the hours each side actually HOLDS (#6750), not the hours its window
    /// names. The baseline holds <c>$2 - @@HELD</c>, and none when the start <c>@@HELD</c> (<see cref="Plan.HoursHeldStart"/>: the later
    /// of the plan's start and this server's data start, the answer's <c>effective_start</c>) sits at or after the recent window's start <c>$2</c>: <c>GREATEST(.., 0)</c> then gives 0, which the
    /// <c>NULLIF(.., 0)</c> on every per-hour term turns into NULL, where a negative divisor gave a negative rate. The recent side
    /// holds <c>$3 - GREATEST($2, @@HELD)</c>. The row filters keep <c>@@B</c>, the requested start, not <c>@@HELD</c>: an interval
    /// whose last snapshot was collected after the raw floor but whose <c>last_execution_time</c> is before it is still read, so
    /// raising the filters would drop it. Only the divisors use the hours held. #6750: the failure arm has its own pair,
    /// <c>base_fail_hours</c> and <c>recent_fail_hours</c> from <c>@@HELDF</c> (<see cref="Plan.FailureHoursHeldStart"/>), because on the
    /// table path it reads the wide table from its own start while the Regular arms read latest from theirs. Every Aborted and Exception
    /// per-hour term divides by that pair, the Regular per-hour terms by <c>base_hours</c> and <c>recent_hours</c>. Where the server's data holds the whole baseline
    /// (<c>@@HELD = @@B</c>) both terms are what they were before, so those reads are unchanged. A baseline that holds no hours (<c>base_hours = 0</c>) counts as empty: its timeout and error counts are not compared, so a row with enough recent runs is listed as it is against an empty baseline (rank 3), and the wire keeps <c>baseline_*_per_hour</c> NULL.
    /// </summary>
    private static string Suffix(string flaggedSample, string rankedSample) => $$"""
        ,
        windows AS (
            SELECT
                GREATEST(CAST(EXTRACT(EPOCH FROM (CAST($2 AS timestamp) - CAST(@@HELD AS timestamp))) / 3600.0 AS double precision), 0) AS base_hours,
                CAST(EXTRACT(EPOCH FROM (CAST($3 AS timestamp) - GREATEST(CAST($2 AS timestamp), CAST(@@HELD AS timestamp)))) / 3600.0 AS double precision) AS recent_hours,
                GREATEST(CAST(EXTRACT(EPOCH FROM (CAST($2 AS timestamp) - CAST(@@HELDF AS timestamp))) / 3600.0 AS double precision), 0) AS base_fail_hours,
                CAST(EXTRACT(EPOCH FROM (CAST($3 AS timestamp) - GREATEST(CAST($2 AS timestamp), CAST(@@HELDF AS timestamp)))) / 3600.0 AS double precision) AS recent_fail_hours
        ),
        candidate_keys AS (
            SELECT database_name, query_id FROM recent_performance
            UNION
            SELECT database_name, query_id FROM failure_performance
            WHERE recent_aborted_execs >= {{FailureMinRecentRuns}} OR recent_exception_execs >= {{FailureMinRecentRuns}}
        ),
        joined AS (
            SELECT
                k.database_name,
                k.query_id,
                b.avg_duration_ms AS base_dur_ms,
                r.avg_duration_ms AS recent_dur_ms,
                b.avg_cpu_time_ms AS base_cpu_ms,
                r.avg_cpu_time_ms AS recent_cpu_ms,
                COALESCE(b.exec_count, 0) AS base_execs,
                COALESCE(r.exec_count, 0) AS recent_execs,
                COALESCE(b.plan_count, 0) AS base_plans,
                COALESCE(r.plan_count, 0) AS recent_plans,
                r.query_text_sample,
                COALESCE(r.last_execution_time, f.recent_last_time) AS last_execution_time,
                COALESCE(f.base_aborted_execs, 0) AS base_aborted_execs,
                COALESCE(f.recent_aborted_execs, 0) AS recent_aborted_execs,
                f.base_aborted_cpu_us,
                f.recent_aborted_cpu_us,
                f.base_aborted_dur_us,
                f.recent_aborted_dur_us,
                COALESCE(f.base_exception_execs, 0) AS base_exception_execs,
                COALESCE(f.recent_exception_execs, 0) AS recent_exception_execs,
                f.base_exception_cpu_us,
                f.recent_exception_cpu_us,
                f.base_exception_dur_us,
                f.recent_exception_dur_us,
                w.base_hours,
                w.recent_hours,
                w.base_fail_hours,
                w.recent_fail_hours
            FROM candidate_keys AS k
            LEFT JOIN recent_performance AS r ON r.database_name = k.database_name AND r.query_id = k.query_id
            LEFT JOIN baseline_performance AS b ON b.database_name = k.database_name AND b.query_id = k.query_id
            LEFT JOIN failure_performance AS f ON f.database_name = k.database_name AND f.query_id = k.query_id
            CROSS JOIN windows AS w
        ),
        flagged AS (
            SELECT
                j.*,
                (j.recent_dur_ms - j.base_dur_ms) * 100.0 / NULLIF(j.base_dur_ms, 0) AS duration_regression_percent,
                (j.recent_cpu_ms - j.base_cpu_ms) * 100.0 / NULLIF(j.base_cpu_ms, 0) AS cpu_regression_percent,
                /* #6781: CPU per run up more than the gate AND at least CostMinAddedCpuMs of added CPU over the window. */
                {{PerformanceMonitor.Common.QueryStoreRegressionRules.CostTestSql("j")}} AS is_cost,
                COALESCE(j.recent_aborted_execs >= {{FailureMinRecentRuns}}
                    AND (j.base_aborted_execs = 0 OR j.base_fail_hours = 0
                         OR (j.recent_aborted_execs / NULLIF(j.recent_fail_hours, 0)) >= {{FailureRateMultiple}} * (j.base_aborted_execs / NULLIF(j.base_fail_hours, 0))), false) AS is_timeouts,
                COALESCE(j.recent_exception_execs >= {{FailureMinRecentRuns}}
                    AND (j.base_exception_execs = 0 OR j.base_fail_hours = 0
                         OR (j.recent_exception_execs / NULLIF(j.recent_fail_hours, 0)) >= {{FailureRateMultiple}} * (j.base_exception_execs / NULLIF(j.base_fail_hours, 0))), false) AS is_errors
            FROM joined AS j
        ),
        qualified AS (
            SELECT
                f.*,
                CASE
                    WHEN NOT f.is_cost THEN 0
                    WHEN f.duration_regression_percent > 100 THEN 4
                    WHEN f.duration_regression_percent > 50 THEN 3
                    WHEN f.duration_regression_percent > 25 THEN 2
                    ELSE 1
                END AS cost_rank,
                GREATEST(
                    CASE WHEN NOT f.is_timeouts THEN 0
                         WHEN f.base_aborted_execs = 0 OR f.base_fail_hours = 0
                              OR (f.recent_aborted_execs / NULLIF(f.recent_fail_hours, 0)) >= {{FailureHighMultiple}} * (f.base_aborted_execs / NULLIF(f.base_fail_hours, 0)) THEN 3
                         ELSE 2 END,
                    CASE WHEN NOT f.is_errors THEN 0
                         WHEN f.base_exception_execs = 0 OR f.base_fail_hours = 0
                              OR (f.recent_exception_execs / NULLIF(f.recent_fail_hours, 0)) >= {{FailureHighMultiple}} * (f.base_exception_execs / NULLIF(f.base_fail_hours, 0)) THEN 3
                         ELSE 2 END) AS failure_rank,
                (f.recent_dur_ms - f.base_dur_ms) * f.recent_execs AS additional_duration_ms
            FROM flagged AS f
            WHERE (f.is_cost OR f.is_timeouts OR f.is_errors)
            /* #6654: a dropped database's queries are left out here, before the rank and the cap, so they cannot fill the page. */
           {{DatabaseFilter.ExcludeClause("f.database_name", ExcludeToken)}}
            /* #6654: the monitor's own statement is left out before the cap too. The collector's drop of it never fires on Darling
               (the text arrives separately, so the row it tests is empty), so the stored statement is found by the shared read-side
               predicate: the marker against the text in query_store_text, with the MAX(query_text) sample as its fallback.
               #5506: the text test is a primary-key probe per row, and it reads only recent-side columns, so written as a plain
               AND the planner ran it below the baseline join on every candidate key (37.8k probes, 12 s cold on a measured store)
               while the flags below keep about 3k. The CASE puts the flags in front of it and reads the baseline-side columns the
               flags come from, so the probe runs only on rows that already pass them. The result is the same: a row the flags
               drop was dropped by the WHERE above anyway. */
            AND   CASE WHEN (f.is_cost OR f.is_timeouts OR f.is_errors)
                       THEN {{SelfTestOpen}}{{QueryStoreSelfStatement.NotSelfOfKeyedText("$1", "f.database_name", "f.query_id", flaggedSample)}}{{SelfTestClose}}
                       ELSE false END
        ),
        ranked AS (
            SELECT
                q.*,
                ROW_NUMBER() OVER
                (
                    PARTITION BY q.is_cost
                    ORDER BY CASE WHEN q.is_cost THEN COALESCE(q.additional_duration_ms, 0) ELSE q.recent_aborted_execs + q.recent_exception_execs END DESC,
                             q.database_name, q.query_id
                ) AS class_rn,
                COUNT(*) FILTER (WHERE q.is_cost) OVER () AS cost_total,
                COUNT(*) FILTER (WHERE NOT q.is_cost) OVER () AS failure_total,
                COUNT(*) OVER () AS total_count
            FROM qualified AS q
        ),
        slots AS (
            /* Failure-only rows: their share of the cap, or more when the cost rows leave slots free. Cost rows take the rest. */
            SELECT
                GREATEST(LEAST(MAX(failure_total), CAST(@@LIMIT AS integer) / {{FailureShareDivisor}}),
                         LEAST(MAX(failure_total), CAST(@@LIMIT AS integer) - MAX(cost_total))) AS failure_slots
            FROM ranked
        )
        SELECT
            r.database_name,
            r.query_id,
            r.base_dur_ms AS baseline_duration_ms,
            r.recent_dur_ms AS recent_duration_ms,
            r.duration_regression_percent,
            r.base_cpu_ms AS baseline_cpu_ms,
            r.recent_cpu_ms AS recent_cpu_ms,
            r.cpu_regression_percent,
            COALESCE(r.additional_duration_ms, 0) AS additional_duration_ms,
            r.base_execs AS baseline_exec_count,
            r.recent_execs AS recent_exec_count,
            r.base_plans AS baseline_plan_count,
            r.recent_plans AS recent_plan_count,
            CASE GREATEST(r.cost_rank, r.failure_rank)
                WHEN 4 THEN 'CRITICAL'
                WHEN 3 THEN 'HIGH'
                WHEN 2 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS severity,
            /* #2150: text comes from collect.query_store_text now, and this projection's grain is exactly
               that table's key, so it resolves here as a keyed lookup per output row, not inside the aggregate.
               #5506: it is a scalar subquery, not a join. The planner expected about 3M rows in ranked where
               the real count is 3 to 4 thousand, so a join hashed the server's whole text table to label 30
               rows. The primary key is (server_id, database_name, query_id), so the subquery returns one row
               or NULL, as the join did.
               The MAX(query_text) sample stays as the fallback: it is where text lived before the
               cutover, and it is what keeps the regression rows readable for existing history. */
            COALESCE((SELECT x.query_sql_text
                      FROM query_store_text AS x
                      WHERE x.server_id = $1
                      AND   x.database_name = r.database_name
                      AND   x.query_id = r.query_id), {{rankedSample}}) AS query_text_sample,
            r.last_execution_time,
            /* #5506: why the row is here: cost, timeouts, errors, or a combination in that order, comma separated. */
            array_to_string(ARRAY_REMOVE(ARRAY[
                CASE WHEN r.is_cost THEN 'cost' END,
                CASE WHEN r.is_timeouts THEN 'timeouts' END,
                CASE WHEN r.is_errors THEN 'errors' END], NULL), ',') AS regression_reason,
            r.base_aborted_execs AS base_aborted_count,
            r.recent_aborted_execs AS recent_aborted_count,
            r.base_aborted_cpu_us / NULLIF(r.base_aborted_execs, 0) / 1000.0 AS base_aborted_cpu_ms,
            r.recent_aborted_cpu_us / NULLIF(r.recent_aborted_execs, 0) / 1000.0 AS recent_aborted_cpu_ms,
            r.base_aborted_dur_us / NULLIF(r.base_aborted_execs, 0) / 1000.0 AS base_aborted_dur_ms,
            r.recent_aborted_dur_us / NULLIF(r.recent_aborted_execs, 0) / 1000.0 AS recent_aborted_dur_ms,
            r.base_exception_execs AS base_exception_count,
            r.recent_exception_execs AS recent_exception_count,
            r.base_exception_cpu_us / NULLIF(r.base_exception_execs, 0) / 1000.0 AS base_exception_cpu_ms,
            r.recent_exception_cpu_us / NULLIF(r.recent_exception_execs, 0) / 1000.0 AS recent_exception_cpu_ms,
            r.base_exception_dur_us / NULLIF(r.base_exception_execs, 0) / 1000.0 AS base_exception_dur_ms,
            r.recent_exception_dur_us / NULLIF(r.recent_exception_execs, 0) / 1000.0 AS recent_exception_dur_ms,
            /* Executions per hour, so the baseline (7 days) and the recent window (hours_back) compare in one unit. */
            r.base_execs / NULLIF(r.base_hours, 0) AS baseline_regular_per_hour,
            r.recent_execs / NULLIF(r.recent_hours, 0) AS recent_regular_per_hour,
            /* #6750: the failure rates divide by the hours the failure arm's own table holds (base_fail_hours, recent_fail_hours). */
            r.base_aborted_execs / NULLIF(r.base_fail_hours, 0) AS baseline_aborted_per_hour,
            r.recent_aborted_execs / NULLIF(r.recent_fail_hours, 0) AS recent_aborted_per_hour,
            r.base_exception_execs / NULLIF(r.base_fail_hours, 0) AS baseline_exception_per_hour,
            r.recent_exception_execs / NULLIF(r.recent_fail_hours, 0) AS recent_exception_per_hour,
            /* #6128: the rows that passed the flags, before the cap. */
            r.total_count
        FROM ranked AS r
        CROSS JOIN slots AS s
        WHERE (r.is_cost AND r.class_rn <= CAST(@@LIMIT AS integer) - s.failure_slots)
        OR    (NOT r.is_cost AND r.class_rn <= s.failure_slots)
        ORDER BY r.is_cost DESC,
                 CASE WHEN r.is_cost THEN COALESCE(r.additional_duration_ms, 0) END DESC,
                 r.recent_aborted_execs + r.recent_exception_execs DESC,
                 r.database_name,
                 r.query_id
        LIMIT @@LIMIT
        """;
}
