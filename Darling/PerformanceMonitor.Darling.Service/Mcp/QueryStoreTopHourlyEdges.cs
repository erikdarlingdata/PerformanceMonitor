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
using System.Text;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #9280: the daily Query Store statement list reads a partial first day and a partial last day from the wide table, about a day of
/// one server's raw rows per call. This takes the whole hours of those two edges that the validity ledger says are built
/// (<c>collect.query_store_top_hourly</c>, <see cref="QueryStoreComposeStamp.TopHourlyTable"/>) from the summary instead, and keeps the
/// wide table for the rest: the partial hour at each end and any hour that is not built, is stale or is not in the summary yet.
/// <see cref="Segment"/> decides which; <see cref="BuildSql"/> turns that into the statement. With no valid hour the caller keeps
/// <see cref="DarlingDataReader.QueryStoreTopDailyTableSql"/> as it is.
/// </summary>
internal static class QueryStoreTopHourlyEdges
{
    /// <summary>Where the edge hours go: <see cref="SummaryHours"/> are read from the hourly table, the ranges from the wide table.
    /// <see cref="LeadRanges"/> lie in <c>[ReadStart, S)</c> and <see cref="TrailRanges"/> in <c>[E, end)</c>, each a half-open
    /// <c>[Start, End)</c> by <c>collection_time</c>, ascending, with touching ranges merged.</summary>
    internal sealed record Plan(
        IReadOnlyList<DateTime> SummaryHours,
        IReadOnlyList<(DateTime Start, DateTime End)> LeadRanges,
        IReadOnlyList<(DateTime Start, DateTime End)> TrailRanges,
        DateTime WindowEnd)
    {
        /// <summary>False when no edge hour is served from the summary: the statement is then today's.</summary>
        public bool UsesSummary => SummaryHours.Count > 0;
    }

    /// <summary>The first whole hour at or after <paramref name="value"/>.</summary>
    internal static DateTime CeilHour(DateTime value)
    {
        var floor = FloorHour(value);
        return floor == value ? floor : floor.AddHours(1);
    }

    /// <summary>The whole hour at or before <paramref name="value"/>, as an unspecified-kind time.</summary>
    internal static DateTime FloorHour(DateTime value) =>
        new(value.Ticks - value.Ticks % TimeSpan.TicksPerHour, DateTimeKind.Unspecified);

    /// <summary>
    /// Splits the two edges of a daily read. <paramref name="readStart"/> and <paramref name="end"/> are the read's own bounds, the daily span
    /// is <c>[spanStart, spanEnd)</c> and <paramref name="validHours"/> the hours the ledger says the hourly table holds exactly. A whole hour
    /// inside an edge that is valid is a summary hour. Everything else inside an edge is a raw range, so a hour not in the list,
    /// the partial hours at the window's ends and the hours past the newest built one all read from the wide table.
    /// </summary>
    internal static Plan Segment(DateTime readStart, DateTime end, DateOnly spanStart, DateOnly spanEnd, IEnumerable<DateTime> validHours)
    {
        var valid = new HashSet<DateTime>(validHours.Select(FloorHour));
        var summary = new List<DateTime>();
        var lead = Edge(readStart, spanStart.ToDateTime(TimeOnly.MinValue), valid, summary);
        var trail = Edge(spanEnd.ToDateTime(TimeOnly.MinValue), end, valid, summary);
        return new Plan(summary, lead, trail, end);
    }

    private static List<(DateTime Start, DateTime End)> Edge(DateTime from, DateTime to, HashSet<DateTime> valid, List<DateTime> summary)
    {
        var ranges = new List<(DateTime Start, DateTime End)>();
        if (from >= to)
        {
            return ranges;
        }

        void Raw(DateTime start, DateTime stop)
        {
            if (ranges.Count > 0 && ranges[^1].End == start)
            {
                ranges[^1] = (ranges[^1].Start, stop);
            }
            else
            {
                ranges.Add((start, stop));
            }
        }

        var hour = CeilHour(from);
        if (hour >= to)
        {
            Raw(from, to);
            return ranges;
        }

        if (from < hour)
        {
            Raw(from, hour);
        }

        for (; hour.AddHours(1) <= to; hour = hour.AddHours(1))
        {
            if (valid.Contains(hour))
            {
                summary.Add(hour);
            }
            else
            {
                Raw(hour, hour.AddHours(1));
            }
        }

        if (hour < to)
        {
            Raw(hour, to);
        }

        return ranges;
    }

    /// <summary>The first parameter after <c>$10</c> (the candidate limit): the summary hours, a <c>timestamp[]</c>. The ranges follow it, a
    /// start and an end each, the lead ranges first.</summary>
    internal const int HoursParameter = 11;

    private const string ExecutionTypeTest = "AND   ($6::text IS NULL OR execution_type_desc = $6)";

    /// <summary>The select list of every arm after the first, as <see cref="DarlingDataReader.QueryStoreTopDailyTableSql"/>'s second arm has it, for an arm that reads wide rows.</summary>
    private const string RawColumns = """
            SELECT
                database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role, module_name,
                CAST(execution_count AS numeric) AS ec,
                CAST(avg_duration_us AS numeric) * execution_count,
                CAST(avg_cpu_time_us AS numeric) * execution_count,
                CAST(avg_logical_io_reads AS numeric) * execution_count,
                CAST(avg_logical_io_writes AS numeric) * execution_count,
                CAST(avg_physical_io_reads AS numeric) * execution_count,
                CAST(avg_rowcount AS numeric) * execution_count,
                last_execution_time, query_plan_hash
        """;

    /// <summary>The select list of an arm that reads the hourly summary: the summary's own columns, in the order the days arm projects them.</summary>
    private const string SummaryColumns = """
            SELECT
                database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role, module_name,
                execution_count_sum,
                avg_duration_us_wsum,
                avg_cpu_time_us_wsum,
                avg_logical_io_reads_wsum,
                avg_logical_io_writes_wsum,
                avg_physical_io_reads_wsum,
                avg_rowcount_wsum,
                last_execution_time_max, query_plan_hash_max
        """;

    /// <summary>One raw arm over <c>[$start, $end)</c> of the wide table by <c>collection_time</c>: today's edge text with the range's own
    /// bounds. The <c>first_execution_time</c> floor is the range start less the margin, and its upper bound the range end plus the slack
    /// (<c>&lt;=</c> for the range that ends at the window's end, as the trailing edge has it today, <c>&lt;</c> for the others).</summary>
    private static string RawArm(int startParameter, int endParameter, bool endsAtWindowEnd)
    {
        var s = $"${startParameter}::timestamp";
        var e = $"${endParameter}::timestamp";
        var upper = endsAtWindowEnd ? "<=" : "<";
        return $"""
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   collection_time >= {s}
            AND   collection_time < {e}
            AND   first_execution_time >= {s} - {QueryStoreIntervalWide.PurgeEdgeMarginSql}
            AND   first_execution_time {upper} {e} + {QueryStoreIntervalWide.FirstExecUpperSlackSql}
            AND   ($5::text[] IS NULL OR database_name = ANY($5))
        """;
    }

    private static string SummaryArm() => $"""
            FROM query_store_top_hourly
            WHERE server_id = $1
            AND   hour = ANY(${HoursParameter}::timestamp[])
            AND   ($5::text[] IS NULL OR database_name = ANY($5))
        """;

    /// <summary>
    /// <see cref="DarlingDataReader.QueryStoreTopDailyTableSql"/> with its two edge arms replaced by the plan's: one raw arm per range and
    /// one summary arm. Everything else, the days arm, <c>ranked</c> and the tail, is that statement's own text, so the answer's columns,
    /// grouping and order are the same. $1 to $10 mean what they mean there; $11 is the summary hours and the ranges' bounds follow
    /// (lead ranges first, then trail ranges). An edge with no raw range keeps its slot as an arm that reads nothing
    /// (<c>WHERE false</c>), because the statement's first arm names the columns and each slot is followed by its own arms in the template.
    /// </summary>
    internal static string BuildSql(Plan plan)
    {
        var next = HoursParameter + 1;
        string Arms(IReadOnlyList<(DateTime Start, DateTime End)> ranges, bool withSummary)
        {
            var arms = new List<(string Columns, string Text)>();
            foreach (var range in ranges)
            {
                var text = RawArm(next, next + 1, range.End == plan.WindowEnd);
                next += 2;
                arms.Add((RawColumns, text));
            }

            if (withSummary)
            {
                arms.Add((SummaryColumns, SummaryArm()));
            }

            /* The slot opens after the template's raw select list, so its first arm is a raw one: a slot with no raw range opens with an
               arm that reads nothing. */
            if (arms.Count == 0 || arms[0].Columns == SummaryColumns)
            {
                arms.Insert(0, (RawColumns, "            FROM query_store_interval_wide\n            WHERE false"));
            }

            var builder = new StringBuilder(arms[0].Text);
            for (var i = 1; i < arms.Count; i++)
            {
                builder.Append("\n            ").Append(ExecutionTypeTest).Append("\n            UNION ALL\n")
                    .Append(arms[i].Columns).Append('\n').Append(arms[i].Text);
            }

            return builder.ToString();
        }

        var lead = Arms(plan.LeadRanges, withSummary: false);
        var trail = Arms(plan.TrailRanges, withSummary: true);
        return DarlingDataReader.QueryStoreTopDailyTableSql
            .Replace(DarlingDataReader.QueryStoreTopDailyLeadingEdgeSql, lead, StringComparison.Ordinal)
            .Replace(DarlingDataReader.QueryStoreTopDailyTrailingEdgeSql, trail, StringComparison.Ordinal);
    }

    /// <summary>The summary hours as the <c>timestamp[]</c> parameter's values (<see cref="HoursParameter"/>).</summary>
    internal static DateTime[] HourValues(Plan plan) =>
        plan.SummaryHours.Select(h => DateTime.SpecifyKind(h, DateTimeKind.Unspecified)).ToArray();

    /// <summary>The range bounds after <see cref="HoursParameter"/>, in parameter order: each range's start then its end, lead ranges first.</summary>
    internal static IEnumerable<DateTime> RangeValues(Plan plan)
    {
        foreach (var (start, stop) in plan.LeadRanges.Concat(plan.TrailRanges))
        {
            yield return DateTime.SpecifyKind(start, DateTimeKind.Unspecified);
            yield return DateTime.SpecifyKind(stop, DateTimeKind.Unspecified);
        }
    }
}
