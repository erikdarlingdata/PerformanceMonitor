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
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;
using Rollup = PerformanceMonitor.Darling.Service.CollectionHistoryAudit.Rollup;
using HourBucket = PerformanceMonitor.Darling.Service.CollectionHistoryAudit.HourBucket;

namespace Darling.Tests;

/// <summary>
/// #5450 proposal 3: the daily retained-history audit. Pure rules (hour-of-day medians, the half test on each
/// count, range merging, the three-day minimum), the evaluator's once-a-day and one-alert rules, and live reads
/// of a seeded rollup.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The live tests reach DARLING_TEST_PG only to
   CREATE and DROP their own database through ScratchPostgres and work entirely inside it. */
public sealed class CollectionHistoryAuditTests
{
    private static readonly CancellationToken Ct = TestContext.Current.CancellationToken;
    private static readonly DateTime AuditDay = new(2026, 10, 10, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>A workload rollup: judged on servers only.</summary>
    private static readonly Rollup QueryStats = new("collect.query_stats_interval_hourly", false);

    /// <summary>The perfmon baseline: judged on servers and collection passes.</summary>
    private static readonly Rollup Perfmon = new("collect.perfmon_interval_baseline", true);

    /// <summary>The normal load curve: servers flat, passes climbing with the hour, so a pass test that
    /// ignored the hour of day would be wrong somewhere.</summary>
    private static (int Servers, long Passes) Usual(int hour) => (40, 1000 + (hour * 100));

    private static IEnumerable<HourBucket> Day(
        DateTime day, Func<int, (int Servers, long Passes)?> shape)
    {
        for (var hour = 0; hour < 24; hour++)
        {
            if (shape(hour) is { } v)
            {
                yield return new HourBucket(day.AddHours(hour), v.Servers, v.Passes);
            }
        }
    }

    private static List<HourBucket> Week(int priorDays = 7) =>
        Enumerable.Range(1, priorDays)
            .SelectMany(d => Day(AuditDay.AddDays(-d), h => Usual(h)))
            .ToList();

    private static List<HourBucket> WithAuditDay(List<HourBucket> week, Func<int, (int Servers, long Passes)?> shape)
    {
        week.AddRange(Day(AuditDay, shape));
        return week;
    }

    [Fact]
    public void ANormalDay_FlagsNothing()
    {
        var buckets = WithAuditDay(Week(), h => Usual(h));
        var ranges = CollectionHistoryAudit.Analyze(AuditDay, buckets);
        Assert.NotNull(ranges);
        Assert.Empty(ranges!);
    }

    [Fact]
    public void AThreeHourHole_IsOneRange_WithTheCountsAgainstUsual()
    {
        var buckets = WithAuditDay(Week(), h => h is >= 18 and <= 20 ? null : Usual(h));
        var range = Assert.Single(CollectionHistoryAudit.Analyze(AuditDay, buckets)!);
        Assert.Equal(18, range.StartHour);
        Assert.Equal(21, range.EndHour);
        Assert.Equal("18:00-21:00Z", CollectionHistoryAudit.RangeLabel(range));
        Assert.Equal(0, range.LowestServers);
        Assert.Equal(40, range.UsualServers);
        Assert.Equal(0, range.LowestPasses);
    }

    [Fact]
    public void PassesAtTwentyPercent_WithEveryServerPresent_IsFlagged()
    {
        /* Case 2 of the issue: rows arrived every hour from every server and only the volume fell. */
        var buckets = WithAuditDay(Week(), h => h is >= 5 and <= 7 ? (40, Usual(h).Passes / 5) : Usual(h));
        var range = Assert.Single(CollectionHistoryAudit.Analyze(AuditDay, buckets)!);
        Assert.Equal("05:00-08:00Z", CollectionHistoryAudit.RangeLabel(range));
        Assert.Equal(40, range.LowestServers);
        Assert.Equal(300, range.LowestPasses);
        Assert.Equal(1500, range.UsualPasses);
    }

    [Fact]
    public void AWorkloadSourceCarriesNoPasses_SoAQuietWeekendWithEveryServerPresentIsClean()
    {
        /* H2 of the #5461 review: a workload rollup is read for servers only (passes are 0 in every bucket), so a
           weekend with a fraction of the queries but every server present flags nothing. */
        var week = Enumerable.Range(1, 7).SelectMany(d => Day(AuditDay.AddDays(-d), h => (40, 0L))).ToList();
        var buckets = WithAuditDay(week, h => (40, 0L));
        Assert.Empty(CollectionHistoryAudit.Analyze(AuditDay, buckets)!);

        /* ... and the hole in servers is still found. */
        var holey = WithAuditDay(Enumerable.Range(1, 7).SelectMany(d => Day(AuditDay.AddDays(-d), h => (40, 0L))).ToList(),
            h => h == 9 ? (10, 0L) : (40, 0L));
        Assert.Equal("09:00-10:00Z", CollectionHistoryAudit.RangeLabel(Assert.Single(CollectionHistoryAudit.Analyze(AuditDay, holey)!)));
    }

    [Theory]
    [InlineData(19, false)]   // 19 of 40 servers: under half
    [InlineData(20, true)]    // exactly half is not under half
    public void TheHalfTest_IsStrict_OnServersAlone(int servers, bool clean)
    {
        var buckets = WithAuditDay(Week(), h => h == 5 ? (servers, Usual(5).Passes) : Usual(h));
        Assert.Equal(clean, CollectionHistoryAudit.Analyze(AuditDay, buckets)!.Count == 0);
    }

    [Theory]
    [InlineData(749, false)]  // passes 749 of 1500: under half
    [InlineData(750, true)]   // exactly half
    public void TheHalfTest_IsStrict_OnPassesAlone(long passes, bool clean)
    {
        var buckets = WithAuditDay(Week(), h => h == 5 ? (40, passes) : Usual(h));
        Assert.Equal(clean, CollectionHistoryAudit.Analyze(AuditDay, buckets)!.Count == 0);
    }

    [Fact]
    public void AnHourWhoseUsualIsUnderOne_NeverFlags()
    {
        /* L3 of the #5461 review: four prior days of [0, 0, 1, 1] servers is a median of 0.5, and an actual of 0 is under
           half of it, but half a server is not a usual worth judging. Usual 1 (two of the four days at 1, the rest
           above) is. */
        var prior = new[] { 0, 0, 1, 1 };
        var buckets = new List<HourBucket>();
        for (var d = 0; d < 4; d++)
        {
            buckets.AddRange(Day(AuditDay.AddDays(-(d + 1)), h => (h == 7 ? prior[d] : 5, 0L)));
        }

        buckets.AddRange(Day(AuditDay, h => h == 7 ? (0, 0L) : (5, 0L)));
        Assert.Empty(CollectionHistoryAudit.Analyze(AuditDay, buckets)!);

        /* Usual exactly 1 with an actual of 0 still flags. */
        var one = new List<HourBucket>();
        for (var d = 1; d <= 4; d++)
        {
            one.AddRange(Day(AuditDay.AddDays(-d), h => (h == 7 ? 1 : 5, 0L)));
        }

        one.AddRange(Day(AuditDay, h => h == 7 ? (0, 0L) : (5, 0L)));
        Assert.Equal("07:00-08:00Z", CollectionHistoryAudit.RangeLabel(Assert.Single(CollectionHistoryAudit.Analyze(AuditDay, one)!)));
    }

    [Fact]
    public void UsualIsTheMedianForThatHour_SoOneBadPriorDayDoesNotMoveIt()
    {
        var week = Week();
        /* One prior day with a hole at hour 12: the median over seven days is still the normal value. */
        week.RemoveAll(b => b.HourUtc == AuditDay.AddDays(-3).AddHours(12));
        var buckets = WithAuditDay(week, h => Usual(h));
        Assert.Empty(CollectionHistoryAudit.Analyze(AuditDay, buckets)!);
        Assert.Equal(2.0, CollectionHistoryAudit.Median(new long[] { 1, 2, 3 }));
        Assert.Equal(2.5, CollectionHistoryAudit.Median(new long[] { 4, 1, 3, 2 }));
        Assert.Equal(0, CollectionHistoryAudit.Median(Array.Empty<long>()));
    }

    [Fact]
    public void FewerThanThreePriorDaysWithData_IsSkipped_NotAudited()
    {
        var buckets = WithAuditDay(Week(priorDays: 2), h => null);
        Assert.Null(CollectionHistoryAudit.Analyze(AuditDay, buckets));

        /* Three is enough, and the audited day's own hole is then found. */
        var three = WithAuditDay(Week(priorDays: 3), h => h == 9 ? null : Usual(h));
        var range = Assert.Single(CollectionHistoryAudit.Analyze(AuditDay, three)!);
        Assert.Equal(9, range.StartHour);
        Assert.Equal(10, range.EndHour);
    }

    [Fact]
    public void APriorDayWithNoRowsAtAll_IsNotADayOfUsual()
    {
        /* Prior days 1-3 empty (a store that was not building the source yet), days 4-7 normal: usual is the
           normal value, not zero, so the hole on the audited day is still found. */
        var buckets = Enumerable.Range(4, 4).SelectMany(d => Day(AuditDay.AddDays(-d), h => Usual(h))).ToList();
        buckets.AddRange(Day(AuditDay, h => h == 3 ? null : Usual(h)));
        var range = Assert.Single(CollectionHistoryAudit.Analyze(AuditDay, buckets)!);
        Assert.Equal(3, range.StartHour);
    }

    [Fact]
    public void SeparateRuns_AreSeparateRanges_AndTheLastHourIsAlwaysJudged()
    {
        var buckets = WithAuditDay(Week(), h => h is 2 or 3 or 10 or 23 ? null : Usual(h));
        var ranges = CollectionHistoryAudit.Analyze(AuditDay, buckets)!;
        Assert.Equal(new[] { "02:00-04:00Z", "10:00-11:00Z", "23:00-24:00Z" }, ranges.Select(CollectionHistoryAudit.RangeLabel));
    }

    [Fact]
    public void ASourceIsSettled_OnlyWhenItsNewestBucketReachesTheDaysLastHour()
    {
        var through22 = WithAuditDay(Week(), h => h <= 22 ? Usual(h) : null);
        Assert.False(CollectionHistoryAudit.IsSettled(AuditDay, through22));

        /* The 23:00 bucket, or any later one (the read runs an hour into the next day), settles it. */
        Assert.True(CollectionHistoryAudit.IsSettled(AuditDay, WithAuditDay(Week(), h => Usual(h))));
        var holeAt23 = WithAuditDay(Week(), h => h == 23 ? null : Usual(h));
        holeAt23.Add(new HourBucket(AuditDay.AddDays(1), 40, 1000));
        Assert.True(CollectionHistoryAudit.IsSettled(AuditDay, holeAt23));
        Assert.False(CollectionHistoryAudit.IsSettled(AuditDay, new List<HourBucket>()));

        Assert.Equal(AuditDay.AddDays(1).AddHours(1), CollectionHistoryAudit.ReadTo(AuditDay));
        Assert.Equal(TimeSpan.FromHours(3), CollectionHistoryAudit.DueTimeOfDay);
    }

    [Fact]
    public void TheReadRunsUpToTheCurrentHour_NeverEarlierThanOneHourPastTheAuditedDay()
    {
        /* H1 of the #5461 round 2 review: the read ends at the current hour, so a bucket after a hole that runs past
           01:00Z is in it. A bucket at 01:00Z or 02:00Z settles a source whose 23:00 bucket is missing. */
        var tick = new DateTime(2026, 10, 11, 3, 5, 0, DateTimeKind.Utc);
        Assert.Equal(new DateTime(2026, 10, 11, 3, 0, 0, DateTimeKind.Utc), CollectionHistoryAudit.ReadTo(AuditDay, tick));
        Assert.Equal(DateTimeKind.Utc, CollectionHistoryAudit.ReadTo(AuditDay, tick).Kind);
        Assert.Equal(AuditDay.AddDays(2).AddHours(3), CollectionHistoryAudit.ReadTo(AuditDay, AuditDay.AddDays(2).AddHours(3).AddMinutes(59)));

        /* Never earlier than the day's own end plus one hour, whatever the clock says. */
        Assert.Equal(CollectionHistoryAudit.ReadTo(AuditDay), CollectionHistoryAudit.ReadTo(AuditDay, AuditDay.AddHours(5)));

        var holeThroughMidnight = WithAuditDay(Week(), h => h == 23 ? null : Usual(h));
        holeThroughMidnight.Add(new HourBucket(AuditDay.AddDays(1).AddHours(2), 40, Usual(2).Passes));
        Assert.True(CollectionHistoryAudit.IsSettled(AuditDay, holeThroughMidnight));
    }

    [Fact]
    public void TheFlaggedHours_AreTheDistinctHoursOfTheDay_NotHoursTimesSources()
    {
        /* L2 of the #5461 round 2 review: one three-hour outage that thins every source is three hours. */
        var shared = new[] { new CollectionHistoryAudit.FlaggedRange(18, 21, 0, 40, 0, 0) };
        var overlapping = new[] { new CollectionHistoryAudit.FlaggedRange(20, 22, 0, 40, 0, 0) };
        var sources = Enumerable.Range(0, 7).Select(i => new Rollup("collect.s" + i, false)).ToArray();

        Assert.Equal(3, CollectionHistoryAudit.FlaggedHours(sources.Select(r => (r, (IReadOnlyList<CollectionHistoryAudit.FlaggedRange>)shared))));
        Assert.Equal(4, CollectionHistoryAudit.FlaggedHours(new (Rollup, IReadOnlyList<CollectionHistoryAudit.FlaggedRange>)[]
        {
            (sources[0], shared), (sources[1], overlapping),
        }));
    }

    [Fact]
    public void TheSql_HasARangePredicateOnBucket_AndCountsOnlyServersEnabledNow()
    {
        var servers = CollectionHistoryAudit.BuildSql(QueryStats);
        Assert.Contains("FROM collect.query_stats_interval_hourly AS r", servers, StringComparison.Ordinal);
        Assert.Contains("WHERE r.bucket >= $1", servers, StringComparison.Ordinal);
        Assert.Contains("r.bucket <  $2", servers, StringComparison.Ordinal);
        Assert.Contains("COUNT(DISTINCT r.server_id)", servers, StringComparison.Ordinal);
        Assert.Contains("0::bigint", servers, StringComparison.Ordinal);
        Assert.DoesNotContain("sample_count", servers, StringComparison.Ordinal);

        /* M1 of the #5461 review: servers enabled now only, on every day the read covers. */
        Assert.Contains("JOIN config.config_monitored_servers AS c", servers, StringComparison.Ordinal);
        Assert.Contains("AND c.is_enabled", servers, StringComparison.Ordinal);

        var passes = CollectionHistoryAudit.BuildSql(Perfmon);
        Assert.Contains("FROM collect.perfmon_interval_baseline AS r", passes, StringComparison.Ordinal);
        Assert.Contains("COUNT(*)::bigint", passes, StringComparison.Ordinal);
        Assert.Contains("AND c.is_enabled", passes, StringComparison.Ordinal);

        Assert.Throws<ArgumentException>(() => CollectionHistoryAudit.BuildSql(new Rollup("x; DROP TABLE y", true)));
    }

    [Fact]
    public void TheAuditedSources_AreTheLiveHourlyAggregatesForServers_AndThePerfmonBaselineForPasses()
    {
        var names = CollectionHistoryAudit.Rollups.Select(r => r.Relation).ToArray();
        Assert.Equal(
            TimescaleSupport.HourlyAggregates.Select(a => "collect." + a.View)
                .Append("collect." + TimescaleSupport.PerfmonIntervalBaselineView),
            names);
        Assert.DoesNotContain("collect." + TimescaleSupport.QueryStatsHourlyView, names);
        Assert.DoesNotContain("collect." + TimescaleSupport.ProcedureStatsHourlyView, names);

        /* Only the perfmon baseline is judged on passes; the six workload rollups never are (H2). */
        Assert.Equal(new[] { "collect." + TimescaleSupport.PerfmonIntervalBaselineView },
            CollectionHistoryAudit.Rollups.Where(r => r.JudgesPasses).Select(r => r.Relation));

        /* Each one the audit reads has the columns the statement names. */
        foreach (var (createSql, view) in TimescaleSupport.HourlyAggregates)
        {
            Assert.Contains("AS bucket", createSql, StringComparison.Ordinal);
            Assert.Contains("server_id", createSql, StringComparison.Ordinal);
            Assert.Contains("collect." + view, names);
        }

        Assert.Contains("AS bucket", TimescaleSupport.CreatePerfmonIntervalBaselineSql, StringComparison.Ordinal);
        Assert.Contains("collection_time", TimescaleSupport.CreatePerfmonIntervalBaselineSql, StringComparison.Ordinal);
    }

    [Fact]
    public void SourcesWithIdenticalFlaggedRanges_ShareOneLine_AndUnreadSourcesAreNamed()
    {
        var shared = new[] { new CollectionHistoryAudit.FlaggedRange(18, 21, 0, 40, 0, 0) };
        var other = new[] { new CollectionHistoryAudit.FlaggedRange(4, 5, 10, 40, 0, 0) };
        var a = new Rollup("collect.query_store_stats_hourly", false);
        var b = new Rollup("collect.query_store_stats_interval_hourly", false);
        var c = new Rollup("collect.query_stats_interval_hourly", false);
        var text = CollectionHistoryAudit.Render(AuditDay, new (Rollup, IReadOnlyList<CollectionHistoryAudit.FlaggedRange>)[]
        {
            (a, shared), (c, other), (b, shared),
        }, new[] { new Rollup("collect.procedure_stats_interval_hourly", false) });

        Assert.Contains("query_store_stats_hourly, query_store_stats_interval_hourly: 18:00-21:00Z (servers as low as 0 vs usual 40)", text, StringComparison.Ordinal);
        Assert.Contains("query_stats_interval_hourly: 04:00-05:00Z (servers as low as 10 vs usual 40)", text, StringComparison.Ordinal);
        Assert.Equal(1, text.Split("18:00-21:00Z").Length - 1);
        Assert.Contains("Not read for this day: procedure_stats_interval_hourly", text, StringComparison.Ordinal);
        Assert.DoesNotContain("Not read", CollectionHistoryAudit.Render(AuditDay, new (Rollup, IReadOnlyList<CollectionHistoryAudit.FlaggedRange>)[] { (a, shared) }), StringComparison.Ordinal);
    }

    [Fact]
    public void TheMetric_IsSelfMonitorFamily_Warning_AndNumeric()
    {
        Assert.Equal("Collection Gaps In History", DarlingSelfAlertEvaluator.CollectionGapsInHistoryMetric);
        Assert.Equal(AlertFamily.SelfMonitor, AlertFamily.Of(DarlingSelfAlertEvaluator.CollectionGapsInHistoryMetric));
        Assert.Equal("WARNING", AlertSeverity.ForMetric(DarlingSelfAlertEvaluator.CollectionGapsInHistoryMetric).BadgeText);
        Assert.False(PerformanceMonitor.Common.AlertMetricClassifier.IsStateOnly(DarlingSelfAlertEvaluator.CollectionGapsInHistoryMetric));

        /* The stamp key is its own row, distinct from the three daily documents'. */
        var others = new[]
        {
            PgSelfAlertDeliveryStampStore.CostDigestStateKey, PgSelfAlertDeliveryStampStore.FleetSweepRollupStateKey,
            PgSelfAlertDeliveryStampStore.AnalysisSinglesDigestStateKey,
        };
        Assert.DoesNotContain(PgSelfAlertDeliveryStampStore.HistoryAuditStateKey, others);
    }

    /* ---------------- the evaluator: once a day, one alert, failure isolation ---------------- */

    private sealed class MemoryStampStore : ISelfAlertDeliveryStampStore
    {
        public Dictionary<string, DateTime> Stamps { get; } = new(StringComparer.Ordinal);

        public Task<DateTime?> GetDeliveredAtUtcAsync(string stateKey, CancellationToken cancellationToken) =>
            Task.FromResult(Stamps.TryGetValue(stateKey, out var at) ? at : (DateTime?)null);

        public Task RecordDeliveredAtUtcAsync(string stateKey, DateTime deliveredAtUtc, CancellationToken cancellationToken)
        {
            Stamps[stateKey] = deliveredAtUtc;
            return Task.CompletedTask;
        }
    }

    private sealed class NoHistoryStore : IAlertHistoryStore
    {
        public Task RecordAlertAsync(AlertHistoryRecord record) => Task.CompletedTask;

        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) => Task.FromResult<DateTime?>(null);
    }

    /// <summary>One "process": its own evaluator, deliverer and clock over a stamp store the test shares.</summary>
    private sealed class Process
    {
        public DarlingSelfAlertTests.RecordingDeliverer Deliverer { get; } = new();
        public DarlingSelfAlertTests.FakeSettings Settings { get; } = new();
        public DateTime Now { get; set; }
        public AlertReadFailureCounter ReadFailures { get; } = new();
        public DarlingSelfAlertTests.CapturingLogger Log { get; } = new();
        public DarlingSelfAlertEvaluator Evaluator { get; }

        public Process(MemoryStampStore stamps, DateTime now)
        {
            Now = now;
            Evaluator = new DarlingSelfAlertEvaluator(
                Settings, Deliverer, new NoHistoryStore(), _ => false,
                logger: Log, utcNow: () => Now, readFailures: ReadFailures, deliveryStamps: stamps);
        }

        public int Reads { get; private set; }

        public Task RunAsync(IReadOnlyList<HourBucket> queryStats, IReadOnlyList<HourBucket>? perfmon = null) =>
            Evaluator.ApplyCollectionHistoryAuditAsync(
                (rollup, from, to, token) =>
                {
                    Reads++;
                    Assert.Equal(AuditDay.AddDays(-7), from);
                    /* The read runs up to the current hour (every tick of these tests is on the day after the audited day
                       or later), so a bucket after a hole across midnight is in it. */
                    Assert.Equal(Now.Date.AddHours(Now.Hour), to);
                    return Task.FromResult<IReadOnlyList<HourBucket>?>(
                        rollup.Relation == QueryStats.Relation ? queryStats : perfmon);
                },
                new[] { QueryStats, Perfmon }, Ct);

        /// <summary>A run whose reader is the test's own: per-relation behaviour, and a count of reads of each.</summary>
        public Task RunWithAsync(Func<Rollup, DateTime, IReadOnlyList<HourBucket>?> reader, Dictionary<string, int>? reads = null) =>
            Evaluator.ApplyCollectionHistoryAuditAsync(
                (rollup, from, to, token) =>
                {
                    if (reads is not null)
                    {
                        reads[rollup.Relation] = (reads.TryGetValue(rollup.Relation, out var seen) ? seen : 0) + 1;
                    }

                    return Task.FromResult(reader(rollup, from));
                },
                new[] { QueryStats, Perfmon }, Ct);
    }

    private static readonly DateTime FirstPass = new(2026, 10, 11, 3, 5, 0, DateTimeKind.Utc);

    [Fact]
    public async Task OneAlertPerRun_NamingTheDayAndEachFlaggedRollup()
    {
        var p = new Process(new MemoryStampStore(), FirstPass.AddHours(1));
        var holey = WithAuditDay(Week(), h => h is >= 18 and <= 20 ? null : Usual(h));
        var thin = WithAuditDay(Week(), h => h is >= 5 and <= 7 ? (40, Usual(h).Passes / 5) : Usual(h));

        await p.RunAsync(holey, thin);

        var fired = Assert.Single(p.Deliverer.Outcomes);
        Assert.Equal("Collection Gaps In History", fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.Equal("collectionhistoryaudit:2026-10-10", fired.ServerKey);
        Assert.Contains("2026-10-10", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("query_stats_interval_hourly: 18:00-21:00Z (servers as low as 0 vs usual 40)", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("perfmon_interval_baseline: 05:00-08:00Z (servers as low as 40 vs usual 40, collection passes as low as 300 vs usual 1,500)", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("Not read", fired.DetailText, StringComparison.Ordinal);
        Assert.Equal(6, fired.NumericCurrentValue);
    }

    [Fact]
    public async Task ANormalDay_RaisesNothing_AndStillSpendsTheDay()
    {
        var stamps = new MemoryStampStore();
        var p = new Process(stamps, FirstPass.AddHours(1));
        var normal = WithAuditDay(Week(), h => Usual(h));

        await p.RunAsync(normal, normal);
        Assert.Empty(p.Deliverer.Outcomes);
        Assert.Equal(2, p.Reads);

        /* An hour later the day is spent: no second read of either source. */
        p.Now = p.Now.AddHours(1);
        await p.RunAsync(normal, normal);
        Assert.Equal(2, p.Reads);
    }

    [Fact]
    public async Task ARestartOnTheSameDay_DoesNotRaiseItTwice_AndTheNextDayDoes()
    {
        var stamps = new MemoryStampStore();
        var holey = WithAuditDay(Week(), h => h is >= 18 and <= 20 ? null : Usual(h));

        var first = new Process(stamps, FirstPass);
        await first.RunAsync(holey, holey);
        Assert.Single(first.Deliverer.Outcomes);
        Assert.Equal(new DateTime(2026, 10, 11, 3, 0, 0, DateTimeKind.Utc), stamps.Stamps["history_audit_slot"]);

        /* A fresh process over the same store, hours later the same UTC day. */
        var second = new Process(stamps, FirstPass.AddHours(10));
        await second.RunAsync(holey, holey);
        Assert.Empty(second.Deliverer.Outcomes);
        Assert.Equal(0, second.Reads);

        /* The next UTC day, at 03:00Z or after, is due again (and audits the day before it). */
        var third = new Process(stamps, new DateTime(2026, 10, 12, 3, 0, 0, DateTimeKind.Utc));
        Assert.True(await DueAsync(third, holey));
    }

    private static async Task<bool> DueAsync(Process p, List<HourBucket> buckets)
    {
        var read = false;
        await p.Evaluator.ApplyCollectionHistoryAuditAsync(
            (rollup, from, to, token) =>
            {
                read = true;
                return Task.FromResult<IReadOnlyList<HourBucket>?>(buckets);
            },
            new[] { QueryStats }, Ct);
        return read;
    }

    [Fact]
    public async Task TheSlotIsThreeOClock_NothingIsReadBeforeIt_OrWithAlertsOff()
    {
        /* H1 of the #5461 review: the audit slot is 03:00Z, after the heaviest rollup (01:15) and the corrected
           rollup (02:0X) have both materialized the audited day's last hour. */
        var early = new Process(new MemoryStampStore(), new DateTime(2026, 10, 11, 2, 59, 0, DateTimeKind.Utc));
        Assert.False(await DueAsync(early, Week()));
        early.Now = new DateTime(2026, 10, 11, 3, 0, 0, DateTimeKind.Utc);
        Assert.True(await DueAsync(early, Week()));

        var off = new Process(new MemoryStampStore(), FirstPass);
        off.Settings.AlertsEnabled = false;
        Assert.False(await DueAsync(off, Week()));
    }

    [Theory]
    [InlineData(3, 0)]
    [InlineData(3, 59)]
    [InlineData(14, 20)]
    [InlineData(23, 59)]
    public async Task AHoleInTheLastHour_IsJudged_AtEveryTick_OnceTheBucketAfterItExists(int hour, int minute)
    {
        /* L1 of the #5461 review: a hole at 23:00 with a bucket after it, whenever the tick falls from 03:00Z on. The
           01:30Z rule it replaced did judge hour 23 at every one of these ticks, so this guards the 03:00Z slot and
           the always-judged hour together, not that rule. The midnight test below is the one that was red before. */
        var p = new Process(new MemoryStampStore(), new DateTime(2026, 10, 11, hour, minute, 0, DateTimeKind.Utc));
        var holeAt23 = WithAuditDay(Week(), h => h == 23 ? null : Usual(h));
        holeAt23.Add(new HourBucket(AuditDay.AddDays(1), 40, Usual(0).Passes));

        await p.RunAsync(holeAt23, holeAt23);

        var fired = Assert.Single(p.Deliverer.Outcomes);
        Assert.Contains("query_stats_interval_hourly: 23:00-24:00Z", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("23:00-24:00Z", fired.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ASourceThatIsNotReady_IsRetriedOnLaterTicks_AndNothingIsRaisedOrStampedUntilItIs()
    {
        var stamps = new MemoryStampStore();
        var p = new Process(stamps, FirstPass);
        var through22 = WithAuditDay(Week(), h => h <= 22 ? Usual(h) : null);
        var settled = WithAuditDay(Week(), h => h is >= 18 and <= 20 ? null : Usual(h));
        var reads = new Dictionary<string, int>();

        /* Perfmon is ready and holey; the workload rollup has not materialized hour 23 yet. */
        await p.RunWithAsync((r, from) => r.Relation == QueryStats.Relation ? through22 : settled, reads);
        Assert.Empty(p.Deliverer.Outcomes);
        Assert.Empty(stamps.Stamps);

        /* A later tick re-reads ONLY the unready source; it is ready now, so the one alert goes out, naming no
           unread source, and the day is stamped. */
        p.Now = FirstPass.AddHours(1);
        await p.RunWithAsync((r, from) => settled, reads);
        Assert.Equal(2, reads[QueryStats.Relation]);
        Assert.Equal(1, reads[Perfmon.Relation]);
        var fired = Assert.Single(p.Deliverer.Outcomes);
        Assert.DoesNotContain("Not read", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("perfmon_interval_baseline: 18:00-21:00Z", fired.DetailText, StringComparison.Ordinal);
        Assert.Equal(new DateTime(2026, 10, 11, 3, 0, 0, DateTimeKind.Utc), stamps.Stamps["history_audit_slot"]);
    }

    [Fact]
    public async Task ASourceThatNeverBecomesReadable_IsGivenUpOnAtTheNextSlot_WithOneWarning_AndTheAlertNamesIt()
    {
        var stamps = new MemoryStampStore();
        var p = new Process(stamps, FirstPass);
        var holey = WithAuditDay(Week(), h => h is >= 18 and <= 20 ? null : Usual(h));
        var day10 = AuditDay.AddDays(-7);

        /* The workload rollup times out on every tick; perfmon reads. Held, not raised, until the slot after. */
        IReadOnlyList<HourBucket>? Reader(Rollup r, DateTime from)
        {
            if (from != day10 || r.Relation == QueryStats.Relation)
            {
                throw new TimeoutException("statement timeout");
            }

            return holey;
        }

        foreach (var tick in new[] { FirstPass, FirstPass.AddHours(8), FirstPass.AddHours(22), new DateTime(2026, 10, 12, 2, 59, 0, DateTimeKind.Utc) })
        {
            p.Now = tick;
            await p.RunWithAsync(Reader);
            Assert.Empty(p.Deliverer.Outcomes);
            Assert.Empty(stamps.Stamps);
        }

        /* 03:00Z the next day: give up on the unread source, one warning naming it, and raise what was read. */
        p.Now = new DateTime(2026, 10, 12, 3, 0, 0, DateTimeKind.Utc);
        await p.RunWithAsync(Reader);

        var fired = Assert.Single(p.Deliverer.Outcomes);
        Assert.Contains("2026-10-10", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("perfmon_interval_baseline: 18:00-21:00Z", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("Not read for this day: query_stats_interval_hourly", fired.DetailText, StringComparison.Ordinal);
        Assert.Single(p.Log.Entries, e => e.Message.Contains("gave up on collect.query_stats_interval_hourly", StringComparison.Ordinal));
    }

    /// <summary>A run whose reader serves each source from its own list, cut at the bounds the evaluator passes, as the
    /// real read is: a bucket outside [from, to) is not seen.</summary>
    private static Task RunBoundedAsync(Process p, Func<Rollup, IReadOnlyList<HourBucket>?> source, params Rollup[] rollups) =>
        p.Evaluator.ApplyCollectionHistoryAuditAsync(
            (rollup, from, to, token) =>
            {
                var all = source(rollup);
                return Task.FromResult<IReadOnlyList<HourBucket>?>(
                    all?.Where(b => b.HourUtc >= from && b.HourUtc < to).ToList());
            },
            rollups.Length == 0 ? new[] { QueryStats, Perfmon } : rollups, Ct);

    [Fact]
    public async Task AnOutageFrom2230To0130_IsRaisedOnceCollectionResumes_NotAfterTheGiveUp()
    {
        /* H1 of the #5461 round 2 review. No row in any source from 23:00Z to 01:00Z; rows again at 01:00Z. With the
           read fixed at 01:00Z no bucket after the hole was in it, so every source read "not ready" forever and the
           day, hole and any unrelated thin hour included, was never judged. */
        var stamps = new MemoryStampStore();
        var p = new Process(stamps, FirstPass);
        var days = WithAuditDay(Week(), h => h == 23 ? null : Usual(h));
        days.Add(new HourBucket(AuditDay.AddDays(1).AddHours(1), 40, Usual(1).Passes));
        days.Add(new HourBucket(AuditDay.AddDays(1).AddHours(2), 40, Usual(2).Passes));

        await RunBoundedAsync(p, _ => days);

        var fired = Assert.Single(p.Deliverer.Outcomes);
        Assert.Contains("23:00-24:00Z", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("Not read", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("not complete", fired.DetailText, StringComparison.Ordinal);
        Assert.Equal(1, fired.NumericCurrentValue);
        Assert.Equal(new DateTime(2026, 10, 11, 3, 0, 0, DateTimeKind.Utc), stamps.Stamps["history_audit_slot"]);
    }

    [Fact]
    public async Task ASourceThatNeverBecomesReady_KeepsItsFlaggedHours_AndTheGiveUpAlertMarksThemNotComplete()
    {
        /* H1 of the #5461 round 2 review: a rollup whose refresh stopped at 10:00Z. Perfmon is clean. Every read finds
           the rollup not ready; its hole (hour 10 to 24, zero servers) must reach the alert at the give-up. */
        var stamps = new MemoryStampStore();
        var p = new Process(stamps, FirstPass);
        var stopped = WithAuditDay(Week(), h => h <= 9 ? Usual(h) : null);
        var clean = WithAuditDay(Week(), h => Usual(h));
        IReadOnlyList<HourBucket>? Source(Rollup r) => r.Relation == QueryStats.Relation ? stopped : clean;

        foreach (var tick in new[] { FirstPass, FirstPass.AddHours(8), new DateTime(2026, 10, 12, 2, 59, 0, DateTimeKind.Utc) })
        {
            p.Now = tick;
            await RunBoundedAsync(p, Source);
            Assert.Empty(p.Deliverer.Outcomes);
            Assert.Empty(stamps.Stamps);
        }

        p.Now = new DateTime(2026, 10, 12, 3, 0, 0, DateTimeKind.Utc);
        await RunBoundedAsync(p, Source);

        var fired = Assert.Single(p.Deliverer.Outcomes);
        Assert.Contains("query_stats_interval_hourly: 10:00-24:00Z (servers as low as 0 vs usual 40) [not complete", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("Not read for this day: query_stats_interval_hourly", fired.DetailText, StringComparison.Ordinal);
        Assert.Equal(14, fired.NumericCurrentValue);
        Assert.Single(p.Log.Entries, e => e.Message.Contains("gave up on collect.query_stats_interval_hourly", StringComparison.Ordinal));
    }

    [Fact]
    public async Task AGiveUpWithNoFlaggedHours_RaisesNothing_AndLogsTheWarning()
    {
        var p = new Process(new MemoryStampStore(), FirstPass);
        var clean = WithAuditDay(Week(), h => Usual(h));

        p.Now = FirstPass;
        await p.RunWithAsync((r, from) => r.Relation == QueryStats.Relation ? throw new TimeoutException("statement timeout") : clean);
        p.Now = new DateTime(2026, 10, 12, 3, 0, 0, DateTimeKind.Utc);
        await p.RunWithAsync((r, from) => r.Relation == QueryStats.Relation && from == AuditDay.AddDays(-7) ? throw new TimeoutException("statement timeout") : clean);

        Assert.Empty(p.Deliverer.Outcomes);
        Assert.Single(p.Log.Entries, e => e.Message.Contains("gave up on collect.query_stats_interval_hourly", StringComparison.Ordinal));
    }

    /// <summary>A deliverer that applies the cooldown the way the real one does for a self-alert (no incidents): one
    /// send per (server key, metric) pair, the repeat throttled. Throttled counts as delivered upstream, so a
    /// throttled alert simply never reaches a channel.</summary>
    private sealed class CooldownDeliverer : IAlertDeliverer
    {
        private readonly HashSet<string> _sent = new(StringComparer.Ordinal);
        public List<AlertOutcome> Delivered { get; } = new();
        public List<AlertOutcome> Throttled { get; } = new();

        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            (_sent.Add(outcome.ServerKey + "|" + outcome.MetricName) ? Delivered : Throttled).Add(outcome);
            return Task.CompletedTask;
        }

        public async Task<AlertDelivery?> DeliverAndReportAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            await DeliverAsync(outcome, cancellationToken);
            return null;
        }
    }

    [Fact]
    public async Task TheGiveUpAlertForOneDay_AndTheNextDaysAudit_AreBothDelivered_InOneTick()
    {
        /* M1 of the #5461 round 2 review: at the first tick on or after D+2 03:00Z the give-up for D fires, then the
           same call audits D+1. Under one delivery key the second was throttled by the cooldown and never reached a
           channel; with the audit day in the key they are two incidents. */
        var cooldown = new CooldownDeliverer();
        var stamps = new MemoryStampStore();
        var settings = new DarlingSelfAlertTests.FakeSettings();
        var now = FirstPass;
        var evaluator = new DarlingSelfAlertEvaluator(
            settings, cooldown, new NoHistoryStore(), _ => false, utcNow: () => now, deliveryStamps: stamps);

        var dayOne = WithAuditDay(Week(), h => h is >= 18 and <= 20 ? null : Usual(h));
        var dayTwo = Week().Concat(Day(AuditDay, h => Usual(h))).Concat(Day(AuditDay.AddDays(1), h => h is >= 5 and <= 6 ? null : Usual(h))).ToList();
        var firstDayFrom = AuditDay.AddDays(-7);

        Task Run() => evaluator.ApplyCollectionHistoryAuditAsync(
            (rollup, from, to, token) =>
            {
                if (from == firstDayFrom)
                {
                    /* Day D: the workload rollup keeps timing out; perfmon reads and is holey. */
                    return rollup.Relation == QueryStats.Relation
                        ? throw new TimeoutException("statement timeout")
                        : Task.FromResult<IReadOnlyList<HourBucket>?>(dayOne);
                }

                return Task.FromResult<IReadOnlyList<HourBucket>?>(dayTwo);
            },
            new[] { QueryStats, Perfmon }, Ct);

        await Run();
        now = new DateTime(2026, 10, 12, 2, 59, 0, DateTimeKind.Utc);
        await Run();
        Assert.Empty(cooldown.Delivered);

        now = new DateTime(2026, 10, 12, 3, 0, 0, DateTimeKind.Utc);
        await Run();

        Assert.Equal(2, cooldown.Delivered.Count);
        Assert.Empty(cooldown.Throttled);
        Assert.Contains("2026-10-10", cooldown.Delivered[0].DetailText, StringComparison.Ordinal);
        Assert.Contains("Not read for this day: query_stats_interval_hourly", cooldown.Delivered[0].DetailText, StringComparison.Ordinal);
        Assert.Contains("2026-10-11", cooldown.Delivered[1].DetailText, StringComparison.Ordinal);
        Assert.NotEqual(cooldown.Delivered[0].ServerKey, cooldown.Delivered[1].ServerKey);
        Assert.StartsWith("collectionhistoryaudit:2026-10-1", cooldown.Delivered[0].ServerKey, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheAlertText_SaysAPerfmonScheduleChangeCanTriggerItForAFewDays()
    {
        var p = new Process(new MemoryStampStore(), FirstPass.AddHours(1));
        var holey = WithAuditDay(Week(), h => h is >= 18 and <= 20 ? null : Usual(h));
        await p.RunAsync(holey, holey);

        var fired = Assert.Single(p.Deliverer.Outcomes);
        Assert.Contains("A change to the perfmon collection schedule", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("for a few days", fired.DetailText, StringComparison.Ordinal);
        Assert.Equal(3, fired.NumericCurrentValue);
    }

    [Fact]
    public async Task AFailedRead_IsRetriedAlone_WarnsOnEachFailure_AndTheAlertIsRaisedOnceEverySourceIsRead()
    {
        var p = new Process(new MemoryStampStore(), FirstPass.AddHours(1));
        var holey = WithAuditDay(Week(), h => h == 12 ? null : Usual(h));
        var reads = new Dictionary<string, int>();

        await p.RunWithAsync((r, from) => r.Relation == QueryStats.Relation
            ? throw new TimeoutException("statement timeout")
            : holey, reads);
        Assert.Empty(p.Deliverer.Outcomes);
        Assert.Single(p.Log.Entries, e => e.Message.Contains("could not read", StringComparison.Ordinal));
        Assert.Equal(1L, p.ReadFailures.ReadInstance().ReadFailures);

        p.Now = p.Now.AddHours(1);
        await p.RunWithAsync((r, from) => holey, reads);
        Assert.Equal(1, reads[Perfmon.Relation]);
        Assert.Equal(2, reads[QueryStats.Relation]);
        var fired = Assert.Single(p.Deliverer.Outcomes);
        Assert.StartsWith("Retained history for 2026-10-10", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("12:00-13:00Z", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("Not read", fired.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task EveryReadFailing_SpendsNothing_SoTheNextTickAsksAgain()
    {
        var stamps = new MemoryStampStore();
        var p = new Process(stamps, FirstPass);
        await p.Evaluator.ApplyCollectionHistoryAuditAsync(
            (rollup, from, to, token) => throw new TimeoutException("down"),
            new[] { QueryStats, Perfmon }, Ct);
        Assert.Empty(p.Deliverer.Outcomes);
        Assert.Empty(stamps.Stamps);

        p.Now = FirstPass.AddHours(1);
        var holey = WithAuditDay(Week(), h => h == 4 ? null : Usual(h));
        await p.RunAsync(holey, holey);
        Assert.Single(p.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AnAbsentSource_AndANewStore_AreQuiet_NotAnAlertOrAWarning()
    {
        var p = new Process(new MemoryStampStore(), FirstPass.AddHours(1));
        /* Perfmon is absent (null); query stats has two prior days only. */
        await p.RunAsync(WithAuditDay(Week(priorDays: 2), h => null), null);
        Assert.Empty(p.Deliverer.Outcomes);
        Assert.Empty(p.Log.Entries);

        /* Both done, so the day is spent. */
        var reads = p.Reads;
        p.Now = p.Now.AddHours(1);
        await p.RunAsync(WithAuditDay(Week(priorDays: 2), h => null), null);
        Assert.Equal(reads, p.Reads);
    }

    [Fact]
    public async Task AFirstPassAtFourteenHundred_StillAnchorsTheNextAuditOnThreeOClock()
    {
        var stamps = new MemoryStampStore();
        var afternoon = new DateTime(2026, 10, 11, 14, 20, 0, DateTimeKind.Utc);
        var p = new Process(stamps, afternoon);
        await p.RunAsync(WithAuditDay(Week(), h => Usual(h)), null);
        Assert.Equal(new DateTime(2026, 10, 11, 3, 0, 0, DateTimeKind.Utc), stamps.Stamps["history_audit_slot"]);
    }

    /* ---------------- live: seeded sources, through the real read ---------------- */

    private static string? Env() => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(Ct);
    }

    /// <summary>
    /// Seeds 22 days from 2026-09-26 in the shape of the real sources. Servers 1-12 are enabled and 13-14 are
    /// disabled but still have rows (M1: they must not count). <c>audit_hourly</c> is a workload rollup (servers
    /// only: several query rows per server and hour); <c>audit_passes</c> is the perfmon baseline's shape (a row per
    /// server per collection pass, four a hour). Days: 2026-10-10 has a 3-hour hole at 18-20Z in both;
    /// 2026-10-14 has passes at 25% from 5-7Z with every server present; 2026-10-17 is weekend-shaped, one third of
    /// the queries with every server and every pass present.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection connection)
    {
        await ExecAsync(connection, @"
CREATE SCHEMA IF NOT EXISTS collect;
CREATE SCHEMA IF NOT EXISTS config;
CREATE TABLE config.config_monitored_servers (server_id integer PRIMARY KEY, is_enabled boolean NOT NULL);
INSERT INTO config.config_monitored_servers SELECT s, s <= 12 FROM generate_series(1, 14) AS s;
CREATE TABLE collect.audit_hourly (bucket timestamp NOT NULL, server_id integer NOT NULL, query_key integer NOT NULL);
CREATE TABLE collect.audit_passes (bucket timestamp NOT NULL, server_id integer NOT NULL, collection_time timestamp NOT NULL);");

        await ExecAsync(connection, @"
INSERT INTO collect.audit_hourly (bucket, server_id, query_key)
SELECT b, s, q
FROM generate_series(TIMESTAMP '2026-09-26', TIMESTAMP '2026-10-17 23:00', INTERVAL '1 hour') AS b
CROSS JOIN generate_series(1, 14) AS s
CROSS JOIN generate_series(1, 3) AS q
WHERE NOT (b::date = DATE '2026-10-10' AND extract(hour FROM b) BETWEEN 18 AND 20)
AND   NOT (b::date = DATE '2026-10-17' AND q > 1);

INSERT INTO collect.audit_passes (bucket, server_id, collection_time)
SELECT b, s, b + p * INTERVAL '15 minutes'
FROM generate_series(TIMESTAMP '2026-09-26', TIMESTAMP '2026-10-17 23:00', INTERVAL '1 hour') AS b
CROSS JOIN generate_series(1, 14) AS s
CROSS JOIN generate_series(0, 3) AS p
WHERE NOT (b::date = DATE '2026-10-10' AND extract(hour FROM b) BETWEEN 18 AND 20)
AND   NOT (b::date = DATE '2026-10-14' AND extract(hour FROM b) BETWEEN 5 AND 7 AND p > 0);");
    }

    [Fact]
    public async Task Live_SeededSources_FlagTheHoleAndTheThinPasses_AndStayQuietOnANormalDayAndAWeekend()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Env()), "Set DARLING_TEST_PG to run the #5450 live tests.");
        var ok = false;
        var scratch = await ScratchPostgres.CreateAsync(Env()!, Ct);
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(Ct);
            await SeedAsync(connection);
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var queries = new Rollup("collect.audit_hourly", false);
            var passes = new Rollup("collect.audit_passes", true);

            async Task<IReadOnlyList<CollectionHistoryAudit.FlaggedRange>?> AuditAsync(Rollup rollup, DateTime day)
            {
                var buckets = await CollectionHistoryAudit.ReadAsync(
                    source, rollup, CollectionHistoryAudit.ReadFrom(day), CollectionHistoryAudit.ReadTo(day), Ct);
                Assert.NotNull(buckets);
                Assert.True(CollectionHistoryAudit.IsSettled(day, buckets!));
                return CollectionHistoryAudit.Analyze(day, buckets!);
            }

            DateTime Utc(int month, int day) => new(2026, month, day, 0, 0, 0, DateTimeKind.Utc);

            /* The hole, in both sources. Usual counts only the 12 enabled servers, not the 14 with rows (M1), and
               usual passes are 12 servers x 4 passes. */
            var hole = Assert.Single((await AuditAsync(queries, Utc(10, 10)))!);
            Assert.Equal("18:00-21:00Z", CollectionHistoryAudit.RangeLabel(hole));
            Assert.Equal(12, hole.UsualServers);
            Assert.Equal(0, hole.LowestServers);
            var passHole = Assert.Single((await AuditAsync(passes, Utc(10, 10)))!);
            Assert.Equal("18:00-21:00Z", CollectionHistoryAudit.RangeLabel(passHole));
            Assert.Equal(12, passHole.UsualServers);
            Assert.Equal(48, passHole.UsualPasses);

            /* A normal day on both sides is quiet, in both sources. */
            foreach (var day in new[] { Utc(10, 9), Utc(10, 16) })
            {
                Assert.Empty((await AuditAsync(queries, day))!);
                Assert.Empty((await AuditAsync(passes, day))!);
            }

            /* Case 2: every server present, passes at a quarter. The workload rollup (servers only) cannot see it;
               the pass count can. */
            var thin = Assert.Single((await AuditAsync(passes, Utc(10, 14)))!);
            Assert.Equal("05:00-08:00Z", CollectionHistoryAudit.RangeLabel(thin));
            Assert.Equal(12, thin.LowestServers);
            Assert.Equal(12, thin.LowestPasses);
            Assert.Equal(48, thin.UsualPasses);
            Assert.Empty((await AuditAsync(queries, Utc(10, 14)))!);

            /* H2: a weekend-shaped day, a third of the queries but every server and every pass, flags nothing. */
            Assert.Empty((await AuditAsync(queries, Utc(10, 17)))!);
            Assert.Empty((await AuditAsync(passes, Utc(10, 17)))!);

            /* M1: a server disabled now is out of both the audited day and usual. Disabling 6 of 12 servers flags
               nothing, where counting them on prior days only would have flagged every hour. */
            await ExecAsync(connection, "UPDATE config.config_monitored_servers SET is_enabled = false WHERE server_id BETWEEN 7 AND 12;");
            Assert.Empty((await AuditAsync(queries, Utc(10, 16)))!);
            Assert.Empty((await AuditAsync(passes, Utc(10, 16)))!);
            await ExecAsync(connection, "UPDATE config.config_monitored_servers SET is_enabled = true WHERE server_id BETWEEN 7 AND 12;");

            /* The same sources through the evaluator and the real read: one alert for the thin day, none for a
               restart, none for the weekend-shaped day. */
            async Task RunAsync(Process process) => await process.Evaluator.ApplyCollectionHistoryAuditAsync(
                (r, from, to, token) => CollectionHistoryAudit.ReadAsync(source, r, from, to, token),
                new[] { queries, passes, new Rollup("collect.history_audit_absent_hourly", false) }, Ct);

            var stamps = new MemoryStampStore();
            var run = new Process(stamps, new DateTime(2026, 10, 15, 3, 0, 0, DateTimeKind.Utc));
            await RunAsync(run);
            var fired = Assert.Single(run.Deliverer.Outcomes);
            Assert.Contains("audit_passes: 05:00-08:00Z", fired.DetailText, StringComparison.Ordinal);
            Assert.DoesNotContain("audit_hourly", fired.DetailText, StringComparison.Ordinal);
            Assert.DoesNotContain("absent", fired.DetailText, StringComparison.Ordinal);

            var restarted = new Process(stamps, new DateTime(2026, 10, 15, 9, 0, 0, DateTimeKind.Utc));
            await RunAsync(restarted);
            Assert.Empty(restarted.Deliverer.Outcomes);

            var weekend = new Process(new MemoryStampStore(), new DateTime(2026, 10, 18, 3, 0, 0, DateTimeKind.Utc));
            await RunAsync(weekend);
            Assert.Empty(weekend.Deliverer.Outcomes);

            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(ok, async () => await scratch.DisposeAsync());
        }
    }

    [Fact]
    public async Task Live_TheRealSourceList_ReadsNullOnAPlainStore_AndIsReadableOnTheAggregates()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Env()), "Set DARLING_TEST_PG to run the #5450 live tests.");
        var ok = false;
        var scratch = await ScratchPostgres.CreateAsync(Env()!, Ct);
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(Ct);
            await PgMigrations.MigrateAsync(connection, Ct);
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* A plain store has none of the aggregates: every real source reads null, and the audit does nothing. */
            foreach (var rollup in CollectionHistoryAudit.Rollups)
            {
                Assert.Null(await CollectionHistoryAudit.ReadAsync(
                    source, rollup, CollectionHistoryAudit.ReadFrom(AuditDay), CollectionHistoryAudit.ReadTo(AuditDay), Ct));
            }

            /* The TimescaleDB shape: build the real aggregates (empty), and the statement is valid against each. */
            Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, Ct), "TimescaleDB must be enabled on the test cluster");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, Ct);
            foreach (var (createSql, _) in TimescaleSupport.HourlyAggregates)
            {
                await ExecAsync(connection, createSql);
            }

            await ExecAsync(connection, TimescaleSupport.CreatePerfmonIntervalBaselineSql);

            foreach (var rollup in CollectionHistoryAudit.Rollups)
            {
                var buckets = await CollectionHistoryAudit.ReadAsync(
                    source, rollup, CollectionHistoryAudit.ReadFrom(AuditDay), CollectionHistoryAudit.ReadTo(AuditDay), Ct);
                Assert.NotNull(buckets);
                Assert.Empty(buckets!);
            }

            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(ok, async () => await scratch.DisposeAsync());
        }
    }
}
