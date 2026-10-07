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
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The daily retained-history audit (#5450, proposal 3): the pure half. For each hourly rollup the store builds,
/// the audited UTC day's per-hour server count and sample total are compared with the median of the seven days
/// before it, for the same hour of day, so the daily load curve is the yardstick. An hour is flagged when its
/// server count OR its sample total is under half of usual. The samples test is required: on a store that
/// collected badly for ten days, rollup rows still arrived every hour from 42-52 servers while samples per day
/// fell 75-85%, so a presence-only check would have passed every one of those days.
///
/// <para><b>Which rollups.</b> The live hourly aggregates (<see cref="TimescaleSupport.HourlyAggregates"/>), so a new
/// hourly rollup is audited without touching this file. The three frozen legacy hourlies are not on that list:
/// they stopped advancing at #3653 (LC), so auditing them would flag every day. A plain-PostgreSQL store builds no
/// continuous aggregates, so every relation is absent there and the audit quietly does nothing.</para>
///
/// <para><b>Samples.</b> Every live rollup carries <c>sample_count</c>. One that does not (decided from its CREATE
/// text, so a future one cannot be forgotten) is counted with <c>COUNT(*)</c> instead: rows per hour, a weaker
/// signal that still catches a hole.</para>
///
/// <para><b>Usual.</b> The median over the prior days that have ANY rows in this rollup. A day with no rows at all
/// is a store that was not building the rollup yet (or an earlier outage), not a day of usual, and counting it as
/// zero would drag the median down and hide the next hole. An hour missing inside a day that has rows counts as
/// zero, which is the hole the audit exists to find. A rollup with fewer than <see cref="MinimumPriorDays"/> such
/// days has no usual yet and is skipped.</para>
/// </summary>
internal static class CollectionHistoryAudit
{
    /// <summary>The days before the audited day that make up "usual".</summary>
    internal const int PriorDays = 7;

    /// <summary>The fewest prior days with data a rollup needs before it is audited.</summary>
    internal const int MinimumPriorDays = 3;

    /// <summary>An hour is flagged when a count is under this fraction of its usual.</summary>
    internal const double ShortfallFraction = 0.5;

    /// <summary>The audit runs on the first pass at or after this time of day (UTC), for the previous UTC day.</summary>
    internal static readonly TimeSpan DueTimeOfDay = TimeSpan.FromHours(1);

    /// <summary>
    /// A pass before this time of day does not judge the audited day's last hour. An hourly aggregate's end_offset
    /// is one hour, so the 23:00 bucket is materialized by the refresh that runs after 01:00Z, and the refresh
    /// phase grid places every hourly refresh inside the first half hour. Judging the bucket earlier would call
    /// a refresh that had not run yet a hole.
    /// </summary>
    internal static readonly TimeSpan LastHourSettledTimeOfDay = TimeSpan.FromMinutes(90);

    /// <summary>The audit read's own deadline, longer than the 10 s alert-pass reads because it aggregates eight days.</summary>
    internal const int CommandTimeoutSeconds = 60;

    /// <summary>One hourly rollup to audit: its relation, and whether it has a <c>sample_count</c> column.</summary>
    internal sealed record Rollup(string Relation, bool HasSampleCount);

    /// <summary>One hour bucket read from a rollup: distinct servers and the sample total in it.</summary>
    internal readonly record struct HourBucket(DateTime HourUtc, int Servers, long Samples);

    /// <summary>
    /// Consecutive flagged hours. <see cref="EndHour"/> is exclusive, so hours 18, 19 and 20 are 18:00-21:00Z.
    /// The counts are the lowest the range's hours reached for each test, each with the usual for that same hour.
    /// </summary>
    internal sealed record FlaggedRange(
        int StartHour, int EndHour, int LowestServers, double UsualServers, long LowestSamples, double UsualSamples);

    /// <summary>The live hourly rollups, derived from the store's own list.</summary>
    internal static IReadOnlyList<Rollup> Rollups { get; } = TimescaleSupport.HourlyAggregates
        .Select(a => new Rollup("collect." + a.View, a.CreateSql.Contains("AS sample_count", StringComparison.Ordinal)))
        .ToArray();

    private static readonly Regex s_relation = new(
        "^[a-z_][a-z0-9_]*(\\.[a-z_][a-z0-9_]*)?$", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The one heavy statement per rollup, with a time-range predicate on <c>bucket</c> so chunk exclusion applies.
    /// $1 is the first bucket read and $2 the end, exclusive.
    /// </summary>
    internal static string BuildSql(Rollup rollup)
    {
        if (!s_relation.IsMatch(rollup.Relation))
        {
            throw new ArgumentException("Not a plain relation name: " + rollup.Relation, nameof(rollup));
        }

        var samples = rollup.HasSampleCount ? "COALESCE(SUM(sample_count), 0)::bigint" : "COUNT(*)::bigint";
        return $@"
SELECT bucket, COUNT(DISTINCT server_id)::int, {samples}
FROM {rollup.Relation}
WHERE bucket >= $1
AND   bucket <  $2
GROUP BY bucket";
    }

    /// <summary>The first bucket the audit reads for <paramref name="auditDayUtc"/>: seven days before it.</summary>
    internal static DateTime ReadFrom(DateTime auditDayUtc) => auditDayUtc.Date.AddDays(-PriorDays);

    /// <summary>The end (exclusive) of what the audit reads: the end of the audited day.</summary>
    internal static DateTime ReadTo(DateTime auditDayUtc) => auditDayUtc.Date.AddDays(1);

    /// <summary>
    /// Reads one rollup's hour buckets, or null when the relation does not exist (a plain-PostgreSQL store, or an
    /// aggregate not built yet). The existence check is a catalog lookup; the aggregate is the one heavy read.
    /// </summary>
    internal static async Task<IReadOnlyList<HourBucket>?> ReadAsync(
        NpgsqlDataSource postgres, Rollup rollup, DateTime fromUtc, DateTime toUtc, CancellationToken cancellationToken)
    {
        var sql = BuildSql(rollup);
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);

        await using (var exists = new NpgsqlCommand("SELECT to_regclass($1) IS NOT NULL", connection)
        { CommandTimeout = CommandTimeoutSeconds })
        {
            exists.Parameters.AddWithValue(rollup.Relation);
            if (await exists.ExecuteScalarAsync(cancellationToken) is not true)
            {
                return null;
            }
        }

        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = CommandTimeoutSeconds };
        /* The column is a timestamp written as UTC; Npgsql refuses a Kind=Utc value for it, so bind it Unspecified. */
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(fromUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(toUtc, DateTimeKind.Unspecified) });

        var buckets = new List<HourBucket>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            buckets.Add(new HourBucket(
                DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc),
                reader.GetInt32(1),
                reader.GetInt64(2)));
        }

        return buckets;
    }

    /// <summary>
    /// The median of <paramref name="values"/>: the middle one, or the mean of the middle two. Zero for none.
    /// </summary>
    internal static double Median(IReadOnlyList<long> values)
    {
        if (values.Count == 0)
        {
            return 0;
        }

        var sorted = values.OrderBy(v => v).ToArray();
        var mid = sorted.Length / 2;
        return sorted.Length % 2 == 1 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2.0;
    }

    /// <summary>
    /// The flagged ranges for the audited day, or null when the rollup has fewer than
    /// <see cref="MinimumPriorDays"/> prior days with rows (no usual yet). Empty means audited and clean.
    /// <paramref name="judgeLastHour"/> is false when a pass is too early to trust the day's last bucket
    /// (<see cref="LastHourSettledTimeOfDay"/>).
    /// </summary>
    internal static IReadOnlyList<FlaggedRange>? Analyze(
        DateTime auditDayUtc, IReadOnlyList<HourBucket> buckets, bool judgeLastHour)
    {
        var auditDay = auditDayUtc.Date;
        var byHour = new Dictionary<DateTime, HourBucket>();
        foreach (var bucket in buckets)
        {
            byHour[bucket.HourUtc] = bucket;
        }

        var priorDaysWithData = new List<DateTime>();
        for (var d = 1; d <= PriorDays; d++)
        {
            var day = auditDay.AddDays(-d);
            var hasRows = buckets.Any(b => b.HourUtc.Date == day && (b.Servers > 0 || b.Samples > 0));
            if (hasRows)
            {
                priorDaysWithData.Add(day);
            }
        }

        if (priorDaysWithData.Count < MinimumPriorDays)
        {
            return null;
        }

        var lastHourToJudge = judgeLastHour ? 23 : 22;
        var flagged = new List<(int Hour, int Servers, double UsualServers, long Samples, double UsualSamples)>();
        for (var hour = 0; hour <= lastHourToJudge; hour++)
        {
            var servers = new List<long>();
            var samples = new List<long>();
            foreach (var day in priorDaysWithData)
            {
                if (byHour.TryGetValue(day.AddHours(hour), out var prior))
                {
                    servers.Add(prior.Servers);
                    samples.Add(prior.Samples);
                }
                else
                {
                    servers.Add(0);
                    samples.Add(0);
                }
            }

            var usualServers = Median(servers);
            var usualSamples = Median(samples);
            byHour.TryGetValue(auditDay.AddHours(hour), out var actual);

            var serversLow = usualServers > 0 && actual.Servers < ShortfallFraction * usualServers;
            var samplesLow = usualSamples > 0 && actual.Samples < ShortfallFraction * usualSamples;
            if (serversLow || samplesLow)
            {
                flagged.Add((hour, actual.Servers, usualServers, actual.Samples, usualSamples));
            }
        }

        return MergeRanges(flagged);
    }

    private static List<FlaggedRange> MergeRanges(
        List<(int Hour, int Servers, double UsualServers, long Samples, double UsualSamples)> flagged)
    {
        var ranges = new List<FlaggedRange>();
        var i = 0;
        while (i < flagged.Count)
        {
            var j = i;
            while (j + 1 < flagged.Count && flagged[j + 1].Hour == flagged[j].Hour + 1)
            {
                j++;
            }

            var group = flagged.GetRange(i, j - i + 1);
            var lowServers = group.OrderBy(h => h.Servers).First();
            var lowSamples = group.OrderBy(h => h.Samples).First();
            ranges.Add(new FlaggedRange(
                group[0].Hour, group[^1].Hour + 1,
                lowServers.Servers, lowServers.UsualServers, lowSamples.Samples, lowSamples.UsualSamples));
            i = j + 1;
        }

        return ranges;
    }

    /// <summary>The rollup's name without its schema, for text.</summary>
    internal static string DisplayName(Rollup rollup)
    {
        var dot = rollup.Relation.IndexOf('.', StringComparison.Ordinal);
        return dot < 0 ? rollup.Relation : rollup.Relation[(dot + 1)..];
    }

    /// <summary>One range as <c>18:00-21:00Z</c>; the end hour 24 reads <c>24:00Z</c>.</summary>
    internal static string RangeLabel(FlaggedRange range) =>
        string.Create(CultureInfo.InvariantCulture, $"{range.StartHour:00}:00-{range.EndHour:00}:00Z");

    /// <summary>Hours flagged across <paramref name="findings"/>.</summary>
    internal static int FlaggedHours(IEnumerable<(Rollup Rollup, IReadOnlyList<FlaggedRange> Ranges)> findings) =>
        findings.Sum(f => f.Ranges.Sum(r => r.EndHour - r.StartHour));

    /// <summary>
    /// The alert's text: the day, then for each flagged rollup its flagged ranges with the counts against usual.
    /// </summary>
    internal static string Render(
        DateTime auditDayUtc, IReadOnlyList<(Rollup Rollup, IReadOnlyList<FlaggedRange> Ranges)> findings)
    {
        var text = new StringBuilder();
        text.Append(CultureInfo.InvariantCulture,
            $"Retained hourly history for {auditDayUtc:yyyy-MM-dd} (UTC) fell below half of the usual pace for that hour in {findings.Count} rollup(s), ")
            .Append("where usual is the median of the 7 days before. ");
        foreach (var (rollup, ranges) in findings)
        {
            text.Append(DisplayName(rollup)).Append(": ");
            text.Append(string.Join("; ", ranges.Select(r => string.Create(CultureInfo.InvariantCulture,
                $"{RangeLabel(r)} (servers as low as {r.LowestServers:N0} vs usual {Math.Round(r.UsualServers):N0}, samples as low as {r.LowestSamples:N0} vs usual {Math.Round(r.UsualSamples):N0})"))));
            text.Append(". ");
        }

        text.Append("A service outage, a restart, or collection that fell behind leaves this shape; compare with the Collection Stopped and Collection Gap At Start alerts for the same day.");
        return text.ToString();
    }
}
