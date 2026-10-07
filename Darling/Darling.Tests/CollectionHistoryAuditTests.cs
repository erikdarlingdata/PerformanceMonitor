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
    private static readonly Rollup QueryStats = new("collect.query_stats_interval_hourly", true);
    private static readonly Rollup ProcStats = new("collect.procedure_stats_interval_hourly", true);

    /// <summary>The normal load curve: servers flat, samples climbing with the hour, so a sample test that
    /// ignored the hour of day would be wrong somewhere.</summary>
    private static (int Servers, long Samples) Usual(int hour) => (40, 1000 + (hour * 100));

    private static IEnumerable<HourBucket> Day(
        DateTime day, Func<int, (int Servers, long Samples)?> shape)
    {
        for (var hour = 0; hour < 24; hour++)
        {
            if (shape(hour) is { } v)
            {
                yield return new HourBucket(day.AddHours(hour), v.Servers, v.Samples);
            }
        }
    }

    private static List<HourBucket> Week(int priorDays = 7) =>
        Enumerable.Range(1, priorDays)
            .SelectMany(d => Day(AuditDay.AddDays(-d), h => Usual(h)))
            .ToList();

    private static List<HourBucket> WithAuditDay(List<HourBucket> week, Func<int, (int Servers, long Samples)?> shape)
    {
        week.AddRange(Day(AuditDay, shape));
        return week;
    }

    [Fact]
    public void ANormalDay_FlagsNothing()
    {
        var buckets = WithAuditDay(Week(), h => Usual(h));
        var ranges = CollectionHistoryAudit.Analyze(AuditDay, buckets, judgeLastHour: true);
        Assert.NotNull(ranges);
        Assert.Empty(ranges!);
    }

    [Fact]
    public void AThreeHourHole_IsOneRange_WithTheCountsAgainstUsual()
    {
        var buckets = WithAuditDay(Week(), h => h is >= 18 and <= 20 ? null : Usual(h));
        var range = Assert.Single(CollectionHistoryAudit.Analyze(AuditDay, buckets, judgeLastHour: true)!);
        Assert.Equal(18, range.StartHour);
        Assert.Equal(21, range.EndHour);
        Assert.Equal("18:00-21:00Z", CollectionHistoryAudit.RangeLabel(range));
        Assert.Equal(0, range.LowestServers);
        Assert.Equal(40, range.UsualServers);
        Assert.Equal(0, range.LowestSamples);
    }

    [Fact]
    public void SamplesAtTwentyPercent_WithEveryServerPresent_IsFlagged()
    {
        /* Case 2 of the issue: rows arrived every hour from every server and only the samples fell. */
        var buckets = WithAuditDay(Week(), h => h is >= 5 and <= 7 ? (40, Usual(h).Samples / 5) : Usual(h));
        var range = Assert.Single(CollectionHistoryAudit.Analyze(AuditDay, buckets, judgeLastHour: true)!);
        Assert.Equal("05:00-08:00Z", CollectionHistoryAudit.RangeLabel(range));
        Assert.Equal(40, range.LowestServers);
        Assert.Equal(300, range.LowestSamples);
        Assert.Equal(1500, range.UsualSamples);
    }

    [Theory]
    [InlineData(19, false)]   // 19 of 40 servers: under half
    [InlineData(20, true)]    // exactly half is not under half
    public void TheHalfTest_IsStrict_OnServersAlone(int servers, bool clean)
    {
        var buckets = WithAuditDay(Week(), h => h == 5 ? (servers, Usual(5).Samples) : Usual(h));
        Assert.Equal(clean, CollectionHistoryAudit.Analyze(AuditDay, buckets, judgeLastHour: true)!.Count == 0);
    }

    [Theory]
    [InlineData(749, false)]  // samples 749 of 1500: under half
    [InlineData(750, true)]   // exactly half
    public void TheHalfTest_IsStrict_OnSamplesAlone(long samples, bool clean)
    {
        var buckets = WithAuditDay(Week(), h => h == 5 ? (40, samples) : Usual(h));
        Assert.Equal(clean, CollectionHistoryAudit.Analyze(AuditDay, buckets, judgeLastHour: true)!.Count == 0);
    }

    [Fact]
    public void UsualIsTheMedianForThatHour_SoOneBadPriorDayDoesNotMoveIt()
    {
        var week = Week();
        /* One prior day with a hole at hour 12: the median over seven days is still the normal value. */
        week.RemoveAll(b => b.HourUtc == AuditDay.AddDays(-3).AddHours(12));
        var buckets = WithAuditDay(week, h => Usual(h));
        Assert.Empty(CollectionHistoryAudit.Analyze(AuditDay, buckets, judgeLastHour: true)!);
        Assert.Equal(2.0, CollectionHistoryAudit.Median(new long[] { 1, 2, 3 }));
        Assert.Equal(2.5, CollectionHistoryAudit.Median(new long[] { 4, 1, 3, 2 }));
        Assert.Equal(0, CollectionHistoryAudit.Median(Array.Empty<long>()));
    }

    [Fact]
    public void FewerThanThreePriorDaysWithData_IsSkipped_NotAudited()
    {
        var buckets = WithAuditDay(Week(priorDays: 2), h => null);
        Assert.Null(CollectionHistoryAudit.Analyze(AuditDay, buckets, judgeLastHour: true));

        /* Three is enough, and the audited day's own hole is then found. */
        var three = WithAuditDay(Week(priorDays: 3), h => h == 9 ? null : Usual(h));
        var range = Assert.Single(CollectionHistoryAudit.Analyze(AuditDay, three, judgeLastHour: true)!);
        Assert.Equal(9, range.StartHour);
        Assert.Equal(10, range.EndHour);
    }

    [Fact]
    public void APriorDayWithNoRowsAtAll_IsNotADayOfUsual()
    {
        /* Prior days 1-3 empty (a store that was not building the rollup yet), days 4-7 normal: usual is the
           normal value, not zero, so the hole on the audited day is still found. */
        var buckets = Enumerable.Range(4, 4).SelectMany(d => Day(AuditDay.AddDays(-d), h => Usual(h))).ToList();
        buckets.AddRange(Day(AuditDay, h => h == 3 ? null : Usual(h)));
        var range = Assert.Single(CollectionHistoryAudit.Analyze(AuditDay, buckets, judgeLastHour: true)!);
        Assert.Equal(3, range.StartHour);
    }

    [Fact]
    public void SeparateRuns_AreSeparateRanges_AndTheLastHourIsJudgedOnlyWhenSettled()
    {
        var shape = new Func<int, (int, long)?>(h => h is 2 or 3 or 10 or 23 ? null : Usual(h));
        var buckets = WithAuditDay(Week(), h => shape(h));

        var ranges = CollectionHistoryAudit.Analyze(AuditDay, buckets, judgeLastHour: true)!;
        Assert.Equal(new[] { "02:00-04:00Z", "10:00-11:00Z", "23:00-24:00Z" }, ranges.Select(CollectionHistoryAudit.RangeLabel));

        /* Before 01:30Z the 23:00 bucket may simply not be materialized yet. */
        var early = CollectionHistoryAudit.Analyze(AuditDay, buckets, judgeLastHour: false)!;
        Assert.Equal(new[] { "02:00-04:00Z", "10:00-11:00Z" }, early.Select(CollectionHistoryAudit.RangeLabel));
    }

    [Fact]
    public void TheSql_HasARangePredicateOnBucket_AndFallsBackToCountStarWithoutSampleCount()
    {
        var with = CollectionHistoryAudit.BuildSql(QueryStats);
        Assert.Contains("FROM collect.query_stats_interval_hourly", with, StringComparison.Ordinal);
        Assert.Contains("WHERE bucket >= $1", with, StringComparison.Ordinal);
        Assert.Contains("bucket <  $2", with, StringComparison.Ordinal);
        Assert.Contains("SUM(sample_count)", with, StringComparison.Ordinal);

        var without = CollectionHistoryAudit.BuildSql(new Rollup("collect.no_samples_hourly", false));
        Assert.DoesNotContain("sample_count", without, StringComparison.Ordinal);
        Assert.Contains("COUNT(*)", without, StringComparison.Ordinal);

        Assert.Throws<ArgumentException>(() => CollectionHistoryAudit.BuildSql(new Rollup("x; DROP TABLE y", true)));
    }

    [Fact]
    public void TheAuditedRollups_AreTheLiveHourlyAggregates_NotTheFrozenLegacyTrio()
    {
        var names = CollectionHistoryAudit.Rollups.Select(r => r.Relation).ToArray();
        Assert.Equal(TimescaleSupport.HourlyAggregates.Select(a => "collect." + a.View), names);
        Assert.DoesNotContain("collect." + TimescaleSupport.QueryStatsHourlyView, names);
        Assert.DoesNotContain("collect." + TimescaleSupport.ProcedureStatsHourlyView, names);

        /* Each one the audit reads has the columns the statement names, or an explicit fallback. */
        foreach (var (createSql, view) in TimescaleSupport.HourlyAggregates)
        {
            Assert.Contains("AS bucket", createSql, StringComparison.Ordinal);
            Assert.Contains("server_id", createSql, StringComparison.Ordinal);
            var rollup = CollectionHistoryAudit.Rollups.Single(r => r.Relation == "collect." + view);
            Assert.Equal(createSql.Contains("AS sample_count", StringComparison.Ordinal), rollup.HasSampleCount);
        }
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

        public Task RunAsync(IReadOnlyList<HourBucket> queryStats, IReadOnlyList<HourBucket>? procStats = null) =>
            Evaluator.ApplyCollectionHistoryAuditAsync(
                (rollup, from, to, token) =>
                {
                    Reads++;
                    Assert.Equal(AuditDay.AddDays(-7), from);
                    Assert.Equal(AuditDay.AddDays(1), to);
                    return Task.FromResult<IReadOnlyList<HourBucket>?>(
                        rollup.Relation == QueryStats.Relation ? queryStats : procStats);
                },
                new[] { QueryStats, ProcStats }, Ct);
    }

    private static readonly DateTime FirstPass = new(2026, 10, 11, 1, 5, 0, DateTimeKind.Utc);

    [Fact]
    public async Task OneAlertPerRun_NamingTheDayAndEachFlaggedRollup()
    {
        var p = new Process(new MemoryStampStore(), FirstPass.AddHours(1));
        var holey = WithAuditDay(Week(), h => h is >= 18 and <= 20 ? null : Usual(h));
        var thin = WithAuditDay(Week(), h => h is >= 5 and <= 7 ? (40, Usual(h).Samples / 5) : Usual(h));

        await p.RunAsync(holey, thin);

        var fired = Assert.Single(p.Deliverer.Outcomes);
        Assert.Equal("Collection Gaps In History", fired.MetricName);
        Assert.Equal(AlertSeverityLevel.Warning, fired.Severity);
        Assert.Equal("collectionhistoryaudit", fired.ServerKey);
        Assert.Contains("2026-10-10", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("query_stats_interval_hourly: 18:00-21:00Z (servers as low as 0 vs usual 40", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("procedure_stats_interval_hourly: 05:00-08:00Z (servers as low as 40 vs usual 40, samples as low as 300 vs usual 1,500)", fired.DetailText, StringComparison.Ordinal);
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

        /* An hour later the day is spent: no second read of either rollup. */
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
        Assert.Equal(new DateTime(2026, 10, 11, 1, 0, 0, DateTimeKind.Utc), stamps.Stamps["history_audit_slot"]);

        /* A fresh process over the same store, hours later the same UTC day. */
        var second = new Process(stamps, FirstPass.AddHours(10));
        await second.RunAsync(holey, holey);
        Assert.Empty(second.Deliverer.Outcomes);
        Assert.Equal(0, second.Reads);

        /* The next UTC day, at 01:00Z or after, is due again (and audits the day before it). */
        var third = new Process(stamps, new DateTime(2026, 10, 12, 1, 0, 0, DateTimeKind.Utc));
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
    public async Task BeforeOneOClock_AndWithAlertsOff_NothingIsRead()
    {
        var early = new Process(new MemoryStampStore(), new DateTime(2026, 10, 11, 0, 59, 0, DateTimeKind.Utc));
        Assert.False(await DueAsync(early, Week()));

        var off = new Process(new MemoryStampStore(), FirstPass);
        off.Settings.AlertsEnabled = false;
        Assert.False(await DueAsync(off, Week()));
    }

    [Fact]
    public async Task AFailedRead_SkipsThatRollup_WarnsOnce_AndTheAuditContinues()
    {
        var p = new Process(new MemoryStampStore(), FirstPass.AddHours(1));
        var holey = WithAuditDay(Week(), h => h == 12 ? null : Usual(h));

        await p.Evaluator.ApplyCollectionHistoryAuditAsync(
            (rollup, from, to, token) => rollup.Relation == QueryStats.Relation
                ? throw new TimeoutException("statement timeout")
                : Task.FromResult<IReadOnlyList<HourBucket>?>(holey),
            new[] { QueryStats, ProcStats }, Ct);

        var fired = Assert.Single(p.Deliverer.Outcomes);
        Assert.StartsWith("Retained hourly history for 2026-10-10", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("query_stats_interval_hourly", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("procedure_stats_interval_hourly: 12:00-13:00Z", fired.DetailText, StringComparison.Ordinal);
        Assert.Single(p.Log.Entries, e => e.Message.Contains("could not read", StringComparison.Ordinal));
    }

    [Fact]
    public async Task EveryReadFailing_SpendsNothing_SoTheNextTickAsksAgain()
    {
        var stamps = new MemoryStampStore();
        var p = new Process(stamps, FirstPass);
        await p.Evaluator.ApplyCollectionHistoryAuditAsync(
            (rollup, from, to, token) => throw new TimeoutException("down"),
            new[] { QueryStats, ProcStats }, Ct);
        Assert.Empty(p.Deliverer.Outcomes);
        Assert.Empty(stamps.Stamps);

        p.Now = FirstPass.AddHours(1);
        var holey = WithAuditDay(Week(), h => h == 4 ? null : Usual(h));
        await p.RunAsync(holey, holey);
        Assert.Single(p.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AnAbsentRollup_AndANewStore_AreQuiet_NotAnAlertOrAWarning()
    {
        var p = new Process(new MemoryStampStore(), FirstPass.AddHours(1));
        /* Procedure stats is absent (null); query stats has two prior days only. */
        await p.RunAsync(WithAuditDay(Week(priorDays: 2), h => null), null);
        Assert.Empty(p.Deliverer.Outcomes);
        Assert.Empty(p.Log.Entries);
    }

    [Fact]
    public async Task AFirstPassAtFourteenHundred_StillAnchorsTheNextAuditOnOneOClock()
    {
        var stamps = new MemoryStampStore();
        var afternoon = new DateTime(2026, 10, 11, 14, 20, 0, DateTimeKind.Utc);
        var p = new Process(stamps, afternoon);
        await p.RunAsync(WithAuditDay(Week(), h => Usual(h)), null);
        Assert.Equal(new DateTime(2026, 10, 11, 1, 0, 0, DateTimeKind.Utc), stamps.Stamps["history_audit_slot"]);
    }

    /* ---------------- live: a seeded rollup, through the real read ---------------- */

    private static string? Env() => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(Ct);
    }

    /// <summary>Seeds 21 days from 2026-09-26: a normal curve, then 2026-10-10 with a 3-hour hole at 18-20Z and
    /// 2026-10-14 with samples at 20% of usual from 5-7Z with every server present. Several rows per bucket and
    /// server, as a real rollup has several statements per server.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, string table, bool withSampleCount)
    {
        var columns = withSampleCount ? ", sample_count bigint" : string.Empty;
        await ExecAsync(connection, $@"
CREATE SCHEMA IF NOT EXISTS collect;
CREATE TABLE collect.{table} (bucket timestamp NOT NULL, server_id integer NOT NULL, query_key integer NOT NULL{columns});");

        var valueExpr = withSampleCount
            ? @"CASE WHEN b::date = DATE '2026-10-14' AND extract(hour FROM b) BETWEEN 5 AND 7 THEN (100 + extract(hour FROM b)::int * 10) / 5 ELSE 100 + extract(hour FROM b)::int * 10 END"
            : "0";
        var insertColumns = withSampleCount ? ", sample_count" : string.Empty;
        var selectValue = withSampleCount ? ", " + valueExpr : string.Empty;
        await ExecAsync(connection, $@"
INSERT INTO collect.{table} (bucket, server_id, query_key{insertColumns})
SELECT b, s, q{selectValue}
FROM generate_series(TIMESTAMP '2026-09-26', TIMESTAMP '2026-10-16 23:00', INTERVAL '1 hour') AS b
CROSS JOIN generate_series(1, 12) AS s
CROSS JOIN generate_series(1, 3) AS q
WHERE NOT (b::date = DATE '2026-10-10' AND extract(hour FROM b) BETWEEN 18 AND 20);");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Live_ASeededRollup_FlagsTheHoleAndTheThinSamples_AndStaysQuietOnANormalDay(bool withSampleCount)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Env()), "Set DARLING_TEST_PG to run the #5450 live tests.");
        var ok = false;
        var scratch = await ScratchPostgres.CreateAsync(Env()!, Ct);
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(Ct);
            var table = withSampleCount ? "audit_hourly" : "audit_nosamples_hourly";
            await SeedAsync(connection, table, withSampleCount);
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var rollup = new Rollup("collect." + table, withSampleCount);

            async Task<IReadOnlyList<CollectionHistoryAudit.FlaggedRange>?> AuditAsync(DateTime day)
            {
                var buckets = await CollectionHistoryAudit.ReadAsync(
                    source, rollup, CollectionHistoryAudit.ReadFrom(day), CollectionHistoryAudit.ReadTo(day), Ct);
                Assert.NotNull(buckets);
                return CollectionHistoryAudit.Analyze(day, buckets!, judgeLastHour: true);
            }

            /* The hole: servers 0 in 18-20Z against 12 usual. */
            var hole = Assert.Single((await AuditAsync(new DateTime(2026, 10, 10, 0, 0, 0, DateTimeKind.Utc)))!);
            Assert.Equal("18:00-21:00Z", CollectionHistoryAudit.RangeLabel(hole));
            Assert.Equal(12, hole.UsualServers);

            /* A normal day on both sides is quiet. */
            Assert.Empty((await AuditAsync(new DateTime(2026, 10, 9, 0, 0, 0, DateTimeKind.Utc)))!);
            Assert.Empty((await AuditAsync(new DateTime(2026, 10, 16, 0, 0, 0, DateTimeKind.Utc)))!);

            var thinDay = new DateTime(2026, 10, 14, 0, 0, 0, DateTimeKind.Utc);
            var thin = await AuditAsync(thinDay);
            if (withSampleCount)
            {
                /* Every server present, samples at a fifth: only the samples test can see it. */
                var range = Assert.Single(thin!);
                Assert.Equal("05:00-08:00Z", CollectionHistoryAudit.RangeLabel(range));
                Assert.Equal(12, range.LowestServers);
                Assert.True(range.LowestSamples < 0.5 * range.UsualSamples);
            }
            else
            {
                /* Without sample_count the rows are counted instead, and the seed keeps every row. */
                Assert.Empty(thin!);
            }

            /* The same rollup through the evaluator and the real read: one alert, and none for a restart. */
            if (withSampleCount)
            {
                var stamps = new MemoryStampStore();
                var run = new Process(stamps, new DateTime(2026, 10, 15, 3, 0, 0, DateTimeKind.Utc));
                await run.Evaluator.ApplyCollectionHistoryAuditAsync(
                    (r, from, to, token) => CollectionHistoryAudit.ReadAsync(source, r, from, to, token),
                    new[] { rollup, new Rollup("collect.history_audit_absent_hourly", true) }, Ct);
                var fired = Assert.Single(run.Deliverer.Outcomes);
                Assert.Contains("audit_hourly: 05:00-08:00Z", fired.DetailText, StringComparison.Ordinal);
                Assert.DoesNotContain("absent", fired.DetailText, StringComparison.Ordinal);

                var restarted = new Process(stamps, new DateTime(2026, 10, 15, 9, 0, 0, DateTimeKind.Utc));
                await restarted.Evaluator.ApplyCollectionHistoryAuditAsync(
                    (r, from, to, token) => CollectionHistoryAudit.ReadAsync(source, r, from, to, token),
                    new[] { rollup }, Ct);
                Assert.Empty(restarted.Deliverer.Outcomes);
            }

            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(ok, async () => await scratch.DisposeAsync());
        }
    }

    [Fact]
    public async Task Live_TheRealRollupList_ReadsNullOnAPlainStore_AndIsReadableOnTheAggregates()
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

            /* A plain store has none of the aggregates: every real rollup reads null, and the audit does nothing. */
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
