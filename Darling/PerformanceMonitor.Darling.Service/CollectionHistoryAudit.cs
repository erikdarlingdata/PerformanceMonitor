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
/// The daily retained-history audit (#5450, proposal 3): the pure half. For each source the store keeps, the
/// audited UTC day's per-hour counts are compared with the median of the seven days before it, for the same hour
/// of day, so the daily load curve is the yardstick. An hour is flagged when a count is under half of usual.
///
/// <para><b>Two kinds of source, two tests.</b> The six live hourly aggregates
/// (<see cref="TimescaleSupport.HourlyAggregates"/>) are QUERY-WORKLOAD rollups: a row exists only for a query or
/// plan that ran, and their <c>sample_count</c> moves with the workload, so a quiet weekend would read as a
/// failure. They get the SERVER-COUNT half test only: presence, which catches a hole (a stopped collector, a
/// rollup whose refresh stopped). The perfmon baseline
/// (<see cref="TimescaleSupport.PerfmonIntervalBaselineView"/>) has one row per server per perfmon collection pass,
/// whatever the workload, so it also gets the COLLECTION-PASS half test: per hour, <c>COUNT(*)</c> is the
/// collection passes and <c>COUNT(DISTINCT server_id)</c> the servers. That is the detector for case 2 of the
/// issue, where rows arrived every hour and only the volume fell (collection passes at 20% of usual).</para>
///
/// <para><b>Only servers enabled now.</b> Every count, on the audited day and the seven before it, counts only
/// servers that are enabled now (<c>config.config_monitored_servers.is_enabled</c>). A server removed or disabled
/// on purpose then leaves "usual" and "actual" alike, instead of flagging for the four days it takes the median
/// to catch up.</para>
///
/// <para><b>Usual.</b> The median over the prior days that have ANY rows in this source. A day with no rows at all
/// is a store that was not building the source yet (or an earlier outage), not a day of usual, and counting it as
/// zero would drag the median down and hide the next hole. An hour missing inside a day that has rows counts as
/// zero, which is the hole the audit exists to find. A source with fewer than <see cref="MinimumPriorDays"/> such
/// days has no usual yet and is skipped. An hour flags only when its usual is at least 1: with an even number of
/// prior days a median of 0.5 would otherwise let one quiet hour flag on a small fleet.</para>
///
/// <para><b>Ready.</b> The audit runs at 03:00Z or later and always judges hour 23. A source is ready when it has
/// ANY bucket at or after the audited day's 23:00 bucket (<see cref="IsSettled"/>), read up to the current hour, so
/// a genuine hole at 23:00, including one that runs past midnight, is told apart from a bucket not materialized yet
/// as soon as collection resumes. A source that is not ready is retried like a failed read, and keeps what it
/// flagged: at the give-up those hours reach the alert, marked as not complete.</para>
///
/// <para><b>Plain PostgreSQL.</b> A plain-PostgreSQL store builds no continuous aggregates, so every relation is
/// absent there and the audit quietly does nothing. The three frozen legacy hourlies are not audited: they stopped
/// advancing at #3653 (LC), so auditing them would flag every day.</para>
/// </summary>
internal static class CollectionHistoryAudit
{
    /// <summary>The days before the audited day that make up "usual".</summary>
    internal const int PriorDays = 7;

    /// <summary>The fewest prior days with data a source needs before it is audited.</summary>
    internal const int MinimumPriorDays = 3;

    /// <summary>An hour is flagged when a count is under this fraction of its usual.</summary>
    internal const double ShortfallFraction = 0.5;

    /// <summary>The audit runs on the first pass at or after this time of day (UTC), for the previous UTC day.
    /// By 03:00Z the heaviest rollup (refreshed at :15 past 01:00Z) and the hierarchical corrected rollup (refreshed
    /// at 02:0X) have both materialized the audited day's last hour.</summary>
    internal static readonly TimeSpan DueTimeOfDay = TimeSpan.FromHours(3);

    /// <summary>The audit read's own deadline, longer than the 10 s alert-pass reads because it aggregates eight days.</summary>
    internal const int CommandTimeoutSeconds = 60;

    /// <summary>One source to audit: its relation, and whether it is judged on collection passes (a row per
    /// server per pass) as well as on servers.</summary>
    internal sealed record Rollup(string Relation, bool JudgesPasses);

    /// <summary>One hour bucket read from a source: distinct servers, and rows (collection passes) in it. Passes
    /// is 0 for a source that does not judge them.</summary>
    internal readonly record struct HourBucket(DateTime HourUtc, int Servers, long Passes);

    /// <summary>
    /// Consecutive flagged hours. <see cref="EndHour"/> is exclusive, so hours 18, 19 and 20 are 18:00-21:00Z.
    /// The counts are the lowest the range's hours reached for each test, each with the usual for that same hour.
    /// </summary>
    internal sealed record FlaggedRange(
        int StartHour, int EndHour, int LowestServers, double UsualServers, long LowestPasses, double UsualPasses);

    /// <summary>The audited sources: the live hourly aggregates (servers only), then the perfmon baseline (servers
    /// and collection passes). Derived from the store's own lists.</summary>
    internal static IReadOnlyList<Rollup> Rollups { get; } = TimescaleSupport.HourlyAggregates
        .Select(a => new Rollup("collect." + a.View, false))
        .Append(new Rollup("collect." + TimescaleSupport.PerfmonIntervalBaselineView, true))
        .ToArray();

    private static readonly Regex s_relation = new(
        "^[a-z_][a-z0-9_]*(\\.[a-z_][a-z0-9_]*)?$", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The one heavy statement per source, with a time-range predicate on <c>bucket</c> so chunk exclusion applies,
    /// over servers enabled now only. $1 is the first bucket read and $2 the end, exclusive.
    /// </summary>
    internal static string BuildSql(Rollup rollup)
    {
        if (!s_relation.IsMatch(rollup.Relation))
        {
            throw new ArgumentException("Not a plain relation name: " + rollup.Relation, nameof(rollup));
        }

        var passes = rollup.JudgesPasses ? "COUNT(*)::bigint" : "0::bigint";
        return $@"
SELECT r.bucket, COUNT(DISTINCT r.server_id)::int, {passes}
FROM {rollup.Relation} AS r
JOIN config.config_monitored_servers AS c
  ON c.server_id = r.server_id
 AND c.is_enabled
WHERE r.bucket >= $1
AND   r.bucket <  $2
GROUP BY r.bucket";
    }

    /// <summary>The first bucket the audit reads for <paramref name="auditDayUtc"/>: seven days before it.</summary>
    internal static DateTime ReadFrom(DateTime auditDayUtc) => auditDayUtc.Date.AddDays(-PriorDays);

    /// <summary>
    /// The earliest end (exclusive) of what the audit reads: one hour past the audited day, so the next day's 00:00
    /// bucket is in the read. The read itself ends later, at the current hour (<see cref="ReadTo(DateTime, DateTime)"/>).
    /// </summary>
    internal static DateTime ReadTo(DateTime auditDayUtc) => auditDayUtc.Date.AddDays(1).AddHours(1);

    /// <summary>
    /// The end (exclusive) of what the audit reads: the current hour (<paramref name="nowUtc"/> truncated to the
    /// hour, so only whole buckets), never earlier than <see cref="ReadTo(DateTime)"/>. Readiness
    /// (<see cref="IsSettled"/>) looks for ANY bucket at or after the audited day's 23:00 bucket, so a hole that
    /// runs past 01:00Z (an outage from 22:30Z to 01:30Z) is told apart from a bucket not materialized yet as soon as
    /// collection resumes, instead of never (H1 of the #5461 round 2 review: with the read fixed at 01:00Z the
    /// later buckets that prove the hole real were outside it, so the whole day stayed "not ready"). Analyze ignores
    /// every bucket after the audited day, so reading further changes no count.
    /// </summary>
    internal static DateTime ReadTo(DateTime auditDayUtc, DateTime nowUtc)
    {
        var floor = ReadTo(auditDayUtc);
        var currentHour = DateTime.SpecifyKind(nowUtc.Date.AddHours(nowUtc.Hour), DateTimeKind.Utc);
        return currentHour > floor ? currentHour : floor;
    }

    /// <summary>
    /// True when the source has ANY bucket at or after the audited day's 23:00 bucket (up to the current hour, see
    /// <see cref="ReadTo(DateTime, DateTime)"/>): the day's last hour had its chance to materialize. A source that is not settled is "not ready" and is handled like a failed read.
    /// </summary>
    internal static bool IsSettled(DateTime auditDayUtc, IReadOnlyList<HourBucket> buckets)
    {
        var lastHour = auditDayUtc.Date.AddHours(23);
        return buckets.Any(b => b.HourUtc >= lastHour);
    }

    /// <summary>
    /// Reads one source's hour buckets, or null when the relation does not exist (a plain-PostgreSQL store, or an
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
    /// All 24 hours are judged, hour 23 included; whether the source had its chance to materialize hour 23 is
    /// <see cref="IsSettled"/>, which the caller checks.
    /// </summary>
    internal static IReadOnlyList<FlaggedRange>? Analyze(DateTime auditDayUtc, IReadOnlyList<HourBucket> buckets)
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
            var hasRows = buckets.Any(b => b.HourUtc.Date == day && (b.Servers > 0 || b.Passes > 0));
            if (hasRows)
            {
                priorDaysWithData.Add(day);
            }
        }

        if (priorDaysWithData.Count < MinimumPriorDays)
        {
            return null;
        }

        var flagged = new List<(int Hour, int Servers, double UsualServers, long Passes, double UsualPasses)>();
        for (var hour = 0; hour <= 23; hour++)
        {
            var servers = new List<long>();
            var passes = new List<long>();
            foreach (var day in priorDaysWithData)
            {
                if (byHour.TryGetValue(day.AddHours(hour), out var prior))
                {
                    servers.Add(prior.Servers);
                    passes.Add(prior.Passes);
                }
                else
                {
                    servers.Add(0);
                    passes.Add(0);
                }
            }

            var usualServers = Median(servers);
            var usualPasses = Median(passes);
            byHour.TryGetValue(auditDay.AddHours(hour), out var actual);

            /* L3 (review of #5461): usual must be at least 1, so an even-count median of 0.5 cannot flag a quiet hour. */
            var serversLow = usualServers >= 1 && actual.Servers < ShortfallFraction * usualServers;
            var passesLow = usualPasses >= 1 && actual.Passes < ShortfallFraction * usualPasses;
            if (serversLow || passesLow)
            {
                flagged.Add((hour, actual.Servers, usualServers, actual.Passes, usualPasses));
            }
        }

        return MergeRanges(flagged);
    }

    private static List<FlaggedRange> MergeRanges(
        List<(int Hour, int Servers, double UsualServers, long Passes, double UsualPasses)> flagged)
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
            var lowPasses = group.OrderBy(h => h.Passes).First();
            ranges.Add(new FlaggedRange(
                group[0].Hour, group[^1].Hour + 1,
                lowServers.Servers, lowServers.UsualServers, lowPasses.Passes, lowPasses.UsualPasses));
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

    /// <summary>The distinct hours of the day flagged by any source in <paramref name="findings"/> (L2 of the #5461
    /// round 2 review: one outage that thins seven sources is that many hours, not seven times as many).</summary>
    internal static int FlaggedHours(IEnumerable<(Rollup Rollup, IReadOnlyList<FlaggedRange> Ranges)> findings) =>
        findings.SelectMany(f => f.Ranges)
            .SelectMany(r => Enumerable.Range(r.StartHour, r.EndHour - r.StartHour))
            .Distinct()
            .Count();

    /// <summary>
    /// The alert's text: the day, then the flagged ranges with the counts against usual, one line for all the
    /// sources whose flagged ranges are identical (the three Query Store rollups share one signal), then any
    /// source that could not be read for this day. A source in <paramref name="incomplete"/> had not caught up to the
    /// end of the day when the audit gave up: its ranges are judged on what it held, and are marked as not complete.
    /// </summary>
    internal static string Render(
        DateTime auditDayUtc, IReadOnlyList<(Rollup Rollup, IReadOnlyList<FlaggedRange> Ranges)> findings,
        IReadOnlyList<Rollup>? unread = null, IReadOnlySet<string>? incomplete = null)
    {
        var text = new StringBuilder();
        text.Append(CultureInfo.InvariantCulture,
            $"Retained history for {auditDayUtc:yyyy-MM-dd} (UTC) fell below half of the usual pace for that hour in {findings.Count} source(s), ")
            .Append("where usual is the median of the 7 days before and only servers enabled now are counted. ");

        var groups = new List<(bool JudgesPasses, bool Incomplete, List<Rollup> Sources, IReadOnlyList<FlaggedRange> Ranges)>();
        foreach (var (rollup, ranges) in findings)
        {
            var isIncomplete = incomplete is not null && incomplete.Contains(rollup.Relation);
            var at = groups.FindIndex(g =>
                g.JudgesPasses == rollup.JudgesPasses && g.Incomplete == isIncomplete && g.Ranges.SequenceEqual(ranges));
            if (at >= 0)
            {
                groups[at].Sources.Add(rollup);
            }
            else
            {
                groups.Add((rollup.JudgesPasses, isIncomplete, new List<Rollup> { rollup }, ranges));
            }
        }

        foreach (var (judgesPasses, isIncomplete, sources, ranges) in groups)
        {
            text.Append(string.Join(", ", sources.Select(DisplayName))).Append(": ");
            text.Append(string.Join("; ", ranges.Select(r => judgesPasses
                ? string.Create(CultureInfo.InvariantCulture,
                    $"{RangeLabel(r)} (servers as low as {r.LowestServers:N0} vs usual {Math.Round(r.UsualServers):N0}, collection passes as low as {r.LowestPasses:N0} vs usual {Math.Round(r.UsualPasses):N0})")
                : string.Create(CultureInfo.InvariantCulture,
                    $"{RangeLabel(r)} (servers as low as {r.LowestServers:N0} vs usual {Math.Round(r.UsualServers):N0})"))));
            text.Append(isIncomplete ? " [not complete: the source had not caught up to the end of the day, judged on what it held]. " : ". ");
        }

        if (unread is { Count: > 0 })
        {
            text.Append("Not read for this day: ").Append(string.Join(", ", unread.Select(DisplayName)))
                .Append(" (a read failed, or the source had not caught up to the end of the day). ");
        }

        text.Append("A service outage, a restart, or collection that fell behind leaves this shape; compare with the Collection Stopped and Collection Gap At Start alerts for the same day. ")
            .Append("A change to the perfmon collection schedule reads as fewer passes and can trigger this for a few days, until the 7-day median catches up.");
        return text.ToString();
    }
}
