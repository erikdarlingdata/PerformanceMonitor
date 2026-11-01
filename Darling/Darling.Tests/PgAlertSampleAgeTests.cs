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
using System.Text.RegularExpressions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;
using static Darling.Tests.RepoFile;
using Finding = PerformanceMonitor.Alerting.PostgresAlertEvaluator.Finding;

namespace Darling.Tests;

/// <summary>
/// #5642, ruling 6: the PostgreSQL alert families that fire from <c>DarlingWorker</c> carry the newest-sample age too,
/// computed once at fire time from the instant the family's own read already returned and persisted with the alert's
/// context. The families whose read returns an instant are High CPU (the counted sample), Deadlocks (the newest
/// report), Blocking (the newest capture) and Poison Wait (the newest collection). The Tier 0 trio (wraparound,
/// vacuum horizon, replication slots) and Long-Running Query read their row's collection time as a last column and
/// stamp it the same way; a read that returns none gives no line.
/// </summary>
public sealed class PgAlertSampleAgeTests
{
    private static readonly DateTime Now = new(2026, 10, 11, 15, 48, 49, DateTimeKind.Utc);

    [Fact]
    public void TheAgeIsComputedAtFireTime_FromTheInstant()
    {
        var instant = new DateTime(2026, 10, 11, 15, 43, 29, DateTimeKind.Unspecified);

        var pg = AlertSampleAge.Stamp(null, "CPU", instant, Now);

        Assert.NotNull(pg?.NewestSample);
        Assert.Equal(TimeSpan.FromSeconds(320), pg!.NewestSample!.Age);
        Assert.Equal(DateTimeKind.Utc, pg.NewestSample.InstantUtc.Kind);
        Assert.Equal("Newest CPU sample: 15:43:29 UTC, 5m 20s old", AlertDetailText.SampleAgeLine(pg.NewestSample));
    }

    /// <summary>
    /// One definition, aliased not copied (#3684: two copies pinned equal fail only after BOTH drift). The SQL
    /// Server engine and Darling's PostgreSQL worker both stamp through <c>AlertSampleAge</c> in
    /// PerformanceMonitor.Notifications, and neither file declares a stamp or newest-instant helper of its own.
    /// </summary>
    [Fact]
    public void TheEngineAndTheWorker_BothStampThroughTheOneSharedHelper_AndDeclareNoCopy()
    {
        var engine = ReadRepoFile("PerformanceMonitor.Alerting", "AlertEngine.cs");
        var worker = ServerConnectBackoffTests.ReadWorkerSource();

        Assert.Contains("AlertSampleAge.Stamp(", engine, StringComparison.Ordinal);
        Assert.Contains("AlertSampleAge.Newest(", engine, StringComparison.Ordinal);
        Assert.Contains("AlertSampleAge.Stamp(", worker, StringComparison.Ordinal);
        Assert.Contains("AlertSampleAge.Newest(", worker, StringComparison.Ordinal);

        var declaration = new Regex(@"static\s+\S+\s+(?:StampNewestSample|StampPgNewestSample|NewestOf|NewestPgInstant)\s*\(");
        Assert.DoesNotMatch(declaration, engine);
        Assert.DoesNotMatch(declaration, worker);
        Assert.DoesNotContain("context.NewestSample = new AlertNewestSample(", engine, StringComparison.Ordinal);
        Assert.DoesNotContain("context.NewestSample = new AlertNewestSample(", worker, StringComparison.Ordinal);
    }

    [Fact]
    public void ANullMinValueOrLocalInstant_GivesNoAge_AndNullStaysNull()
    {
        Assert.Null(AlertSampleAge.Stamp(null, "CPU", null, Now));
        Assert.Null(AlertSampleAge.Stamp(null, "CPU", DateTime.MinValue, Now));
        Assert.Null(AlertSampleAge.Stamp(null, "CPU", DateTime.SpecifyKind(Now.AddMinutes(-1), DateTimeKind.Local), Now));

        var kept = new AlertContext();
        Assert.Same(kept, AlertSampleAge.Stamp(kept, "CPU", null, Now));
        Assert.Null(kept.NewestSample);
    }

    [Fact]
    public void AFutureDatedSample_AgesAsZero()
    {
        var context = AlertSampleAge.Stamp(null, "deadlock", Now.AddMinutes(3), Now);
        Assert.Equal(TimeSpan.Zero, context!.NewestSample!.Age);
    }

    [Fact]
    public void TheNewestOfAFamilysInstants_IgnoresDefaults_AndIsNullWhenThereAreNone()
    {
        Assert.Null(AlertSampleAge.Newest(Array.Empty<DateTime>()));
        Assert.Null(AlertSampleAge.Newest(new DateTime[] { default, default }));
        Assert.Equal(Now, AlertSampleAge.Newest(new DateTime[] { Now.AddMinutes(-9), default, Now, Now.AddMinutes(-1) }));
        Assert.Equal(Now, AlertSampleAge.Newest(new DateTime?[] { null, Now.AddMinutes(-9), Now, default }));
        Assert.Null(AlertSampleAge.Newest(new DateTime?[] { null, default }));
    }

    /// <summary>
    /// High CPU's detail text is hand-built and the fire passes no Details, so the deliverer's one append is the only
    /// place the line enters: the text carries it once, and a text that already has it is left alone.
    /// </summary>
    [Fact]
    public void ACpuFiresHandBuiltText_GetsTheLineOnce()
    {
        var context = AlertSampleAge.Stamp(null, "CPU", Now.AddSeconds(-320), Now);
        const string handBuilt = "  Capacity: 91% of ceiling\n  Threshold: 80%\n  Sustained: 3 consecutive samples";

        var once = AlertDetailText.WithSampleAge(handBuilt, context);
        var twice = AlertDetailText.WithSampleAge(once, context);

        Assert.Equal(once, twice);
        Assert.Single(Regex.Matches(once!, "Newest CPU sample"));
        Assert.EndsWith("Newest CPU sample: 15:43:29 UTC, 5m 20s old", once, StringComparison.Ordinal);
    }

    /// <summary>
    /// Source shape, RED at the base commit (each of these four fires passed a context with no sample, or none at all):
    /// every family hands the stamp its own instant. High CPU's context was null.
    /// </summary>
    [Fact]
    public void EveryFamilyWhoseReadReturnsAnInstant_StampsItAtFireTime()
    {
        var source = ServerConnectBackoffTests.ReadWorkerSource();
        Assert.Contains("Context: AlertSampleAge.Stamp(null, \"CPU\", reading.SampleTimeUtc, now)", source, StringComparison.Ordinal);
        Assert.Contains("\"deadlock\", AlertSampleAge.Newest(rows.Select(r => r.OccurredAtUtc)), now)", source, StringComparison.Ordinal);
        Assert.Contains("\"blocking capture\", AlertSampleAge.Newest(worstPerRoot.Select(r => r.CapturedAt)), now)", source, StringComparison.Ordinal);
        Assert.Contains("newestCollectionBySubject.TryGetValue(finding.Subject, out var newestWaitCollection)", source, StringComparison.Ordinal);
        /* 8 since #7082: the six above plus the two digest rows it adds (High CPU held at the ceiling, and the held Poison Wait row). */
        Assert.Equal(8, Regex.Matches(source, @"AlertSampleAge\.Stamp\(\s*(?:null|new AlertContext)").Count);
    }

    // ---- the Tier 0 trio and Long-Running Query (#5642, ruling 6) ----

    public static IEnumerable<object[]> Tier0AndLongRunningReads() => new[]
    {
        new object[] { "WraparoundSql", DarlingPostgresAlertReadAdapter.WraparoundSql },
        new object[] { "XminSql", DarlingPostgresAlertReadAdapter.XminSql },
        new object[] { "SlotSql", DarlingPostgresAlertReadAdapter.SlotSql },
        new object[] { "CurrentLongRunningSessionsSqlTemplate", DarlingPgSessionStatesReader.CurrentLongRunningSessionsSqlTemplate },
    };

    /// <summary>
    /// Shape pin: each read ends in the collection-time column. A column added in the middle of a select list moves
    /// the ordinal the reader takes, so the age line would read another column.
    /// </summary>
    [Theory]
    [MemberData(nameof(Tier0AndLongRunningReads))]
    public void TheSql_EndsInTheNewestSampleColumn(string name, string sql)
    {
        var columns = DarlingAlertReadAdapterNewestSampleTests.FinalSelectColumns(sql);
        Assert.True(columns.Count >= 2, $"{name}: the select list was not parsed");
        Assert.EndsWith("newest_sample_utc", columns[^1], StringComparison.Ordinal);    }

    [Fact]
    public void TheReaders_TakeTheLastColumn_ByOrdinal_AsUtc()
    {
        var adapter = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingPostgresAlertReadAdapter.cs");
        var ordinals = Regex.Matches(adapter, @"AsUtc\(reader, (\d+)\)")
            .Select(m => int.Parse(m.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture))
            .ToList();
        Assert.Equal(
            new[]
            {
                DarlingAlertReadAdapterNewestSampleTests.FinalSelectColumns(DarlingPostgresAlertReadAdapter.WraparoundSql).Count - 1,
                DarlingAlertReadAdapterNewestSampleTests.FinalSelectColumns(DarlingPostgresAlertReadAdapter.XminSql).Count - 1,
                DarlingAlertReadAdapterNewestSampleTests.FinalSelectColumns(DarlingPostgresAlertReadAdapter.SlotSql).Count - 1,
            },
            ordinals);
        Assert.Contains("DateTime.SpecifyKind(reader.GetDateTime(ordinal), DateTimeKind.Utc)", adapter, StringComparison.Ordinal);

        var sessions = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "DarlingPgSessionStatesReader.cs");
        var lastOrdinal = DarlingAlertReadAdapterNewestSampleTests.FinalSelectColumns(
            DarlingPgSessionStatesReader.CurrentLongRunningSessionsSqlTemplate).Count - 1;
        Assert.Contains(
            $"reader.IsDBNull({lastOrdinal}) ? null : DateTime.SpecifyKind(reader.GetDateTime({lastOrdinal}), DateTimeKind.Utc)",
            sessions, StringComparison.Ordinal);
    }

    private static Finding MakeFinding(string metric, string subject) =>
        new(metric, AlertSeverityLevel.Warning, subject, "", "", "", 0, 0);

    [Fact]
    public void ATier0Finding_IsAgedFromItsOwnRow_ByMetricAndSubject()
    {
        var older = Now.AddMinutes(-9);
        var newer = Now.AddSeconds(-90);
        var wraparound = new[]
        {
            new PostgresWraparoundAlertInfo("db_a", 1, 0, 1, 1, NewestSampleUtc: older),
            new PostgresWraparoundAlertInfo("db_b", 1, 0, 1, 1, NewestSampleUtc: newer),
        };
        var xmin = new PostgresXminHorizonAlertInfo("session", "1", 1, 1, 1, null, NewestSampleUtc: Now.AddMinutes(-4));
        var slots = new[]
        {
            new PostgresSlotAlertInfo("slot_a", "lost", false, 1, 0, null, Now.AddMinutes(-2)),
        };

        Assert.Equal(older, DarlingWorker.Tier0NewestSample(MakeFinding(PostgresAlertEvaluator.WraparoundMetric, "db_a"), wraparound, xmin, slots));
        Assert.Equal(newer, DarlingWorker.Tier0NewestSample(MakeFinding(PostgresAlertEvaluator.WraparoundMetric, "db_b"), wraparound, xmin, slots));
        Assert.Equal(xmin.NewestSampleUtc, DarlingWorker.Tier0NewestSample(MakeFinding(PostgresAlertEvaluator.XminHorizonMetric, "rotating holders"), wraparound, xmin, slots));
        Assert.Equal(Now.AddMinutes(-2), DarlingWorker.Tier0NewestSample(MakeFinding(PostgresAlertEvaluator.SlotRetentionMetric, "slot_a"), wraparound, xmin, slots));
    }

    [Fact]
    public void ATier0ReadWithNoTime_GivesNoLine()
    {
        var wraparound = new[] { new PostgresWraparoundAlertInfo("db_a", 1, 0, 1, 1) };
        var slots = new[] { new PostgresSlotAlertInfo("slot_a", "lost", false, 1, 0, null) };
        var xmin = new PostgresXminHorizonAlertInfo("session", "1", 1, 1, 1, null);

        Assert.Null(DarlingWorker.Tier0NewestSample(MakeFinding(PostgresAlertEvaluator.WraparoundMetric, "db_a"), wraparound, xmin, slots));
        Assert.Null(DarlingWorker.Tier0NewestSample(MakeFinding(PostgresAlertEvaluator.XminHorizonMetric, "x"), wraparound, xmin, slots));
        Assert.Null(DarlingWorker.Tier0NewestSample(MakeFinding(PostgresAlertEvaluator.SlotRetentionMetric, "slot_a"), wraparound, xmin, slots));
        Assert.Null(DarlingWorker.Tier0NewestSample(MakeFinding(PostgresAlertEvaluator.SlotRetentionMetric, "unknown_slot"), wraparound, xmin, slots));
        Assert.Null(DarlingWorker.Tier0NewestSample(MakeFinding(PostgresAlertEvaluator.WraparoundMetric, "db_a"), null, null, null));

        var context = AlertSampleAge.Stamp(
            new AlertContext(), "collection",
            DarlingWorker.Tier0NewestSample(MakeFinding(PostgresAlertEvaluator.SlotRetentionMetric, "slot_a"), wraparound, xmin, slots), Now);
        Assert.Null(context!.NewestSample);
        Assert.Null(AlertDetailText.SampleAgeLineFor(context, null));
    }

    [Fact]
    public void ATier0Fire_CarriesTheLineOnce_FromItsRowsTime()
    {
        var context = AlertSampleAge.Stamp(new AlertContext(), "collection", Now.AddMinutes(-4), Now);
        var text = AlertDetailText.WithSampleAge("  XID age 1,000", context);
        Assert.Single(Regex.Matches(text!, "Newest collection sample"));
        Assert.EndsWith("Newest collection sample: 15:44:49 UTC, 4m 0s old", text, StringComparison.Ordinal);
        Assert.Equal(text, AlertDetailText.WithSampleAge(text, context));
    }

    /// <summary>
    /// Source shape, RED at the base commit (the Tier 0 fire passed a context with no sample, and so did
    /// Long-Running Query): both fires hand the stamp the instant their own read returned.
    /// </summary>
    [Fact]
    public void TheTier0TrioAndLongRunningQuery_StampTheirReadsInstantAtFireTime()
    {
        var source = ServerConnectBackoffTests.ReadWorkerSource();
        Assert.Contains("Tier0NewestSample(finding, wraparound, xmin, slots), now)", source, StringComparison.Ordinal);
        Assert.Contains("\"query snapshot\", AlertSampleAge.Newest(rows.Select(r => r.NewestSampleUtc ?? default)), now)", source, StringComparison.Ordinal);
    }
}
