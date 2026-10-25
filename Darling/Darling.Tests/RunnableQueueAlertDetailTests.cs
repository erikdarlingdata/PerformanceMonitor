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
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Runnable Queue alert's detail block (#5641): CPU percent for the same five minutes the queue average covers, and
/// the heaviest requests in the newest query snapshot. The block rides in <c>AlertContext.Details</c>, so every channel
/// that renders another alert's details (both email bodies, the webhook, the history row) renders it; these tests pin
/// the content, the degrade-not-suppress rule when the request read fails, and the two email bodies.
/// </summary>
public sealed class RunnableQueueAlertDetailTests
{
    private const string Key = "101";
    private static readonly DateTime Collected = new(2026, 10, 9, 14, 0, 0, DateTimeKind.Unspecified);
    private static readonly AlertBranding Branding = new("Test Edition", null);

    /* #5641 Lane 1c: five collections a minute apart, the newest at Collected, every one at the same average. */
    private static List<RunnableQueueSample> Samples(double average, long total) =>
        Enumerable.Range(0, 5).Select(i => new RunnableQueueSample(Collected.AddMinutes(-i), average, total, 4)).ToList();

    private static AlertEngineTests.Harness Armed()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.RunnableQueueEnabled = true;
        h.Adapter.RunnableQueue = Samples(20.0, 80);
        return h;
    }

    private static LongRunningQueryInfo Request(int spid, long cpuMs, long elapsedSeconds = 30, string text = "SELECT 1") => new()
    {
        SessionId = spid,
        DatabaseName = "StackOverflow",
        ProgramName = "App",
        QueryText = text,
        ElapsedSeconds = elapsedSeconds,
        CpuTimeMs = cpuMs,
        Reads = 1000,
        Writes = 5,
        WaitType = "SOS_SCHEDULER_YIELD",
    };

    private static AlertServerSnapshot WithCpuSamples(AlertEngineTests.Harness h, params (int MinutesAgo, double Sql, double Total)[] samples) =>
        AlertEngineTests.Harness.Snapshot(sqlCpu: 50, totalCpu: 55) with
        {
            RecentCpuSamples = samples
                .Select(s => new AlertCpuSample(h.Now.AddMinutes(-s.MinutesAgo), s.Sql, s.Total))
                .ToList(),
        };

    private static AlertDetailItem Item(AlertOutcome fired, string headingStart) =>
        Assert.Single(fired.Context!.Details, d => d.Heading.StartsWith(headingStart, StringComparison.Ordinal));

    private static string Field(AlertDetailItem item, string label) =>
        Assert.Single(item.Fields, f => f.Label == label).Value;

    [Fact]
    public async Task TheCard_CarriesTheCpuForTheSameFiveMinutes_AndTheTopRequestsByCpuTime()
    {
        var h = Armed();
        h.Adapter.LongRunning.Add(Request(51, cpuMs: 1_000));
        h.Adapter.LongRunning.Add(Request(52, cpuMs: 90_000, text: "SELECT * FROM dbo.Posts"));
        h.Adapter.LongRunning.Add(Request(53, cpuMs: 40_000));
        h.Adapter.LongRunning.Add(Request(54, cpuMs: 60_000));

        await h.Build().EvaluateServerAsync(WithCpuSamples(h, (1, 80, 90), (3, 60, 70), (5, 70, 80), (7, 10, 10)));

        var fired = Assert.Single(h.Deliverer.Outcomes);
        var cpu = Item(fired, "CPU over the last 5 minutes");
        Assert.Equal("70% average, 80% peak", Field(cpu, "SQL Server CPU"));
        Assert.Equal("80% average, 90% peak", Field(cpu, "Total CPU"));
        Assert.Equal("3", Field(cpu, "CPU samples"));

        var requests = fired.Context!.Details.Where(d => d.Heading.StartsWith("Top request:", StringComparison.Ordinal)).ToList();
        Assert.Equal(AlertContextBuilders.RunnableQueueRequestDisplayCap, requests.Count);
        Assert.Contains("session #52", requests[0].Heading, StringComparison.Ordinal);
        Assert.Contains("session #54", requests[1].Heading, StringComparison.Ordinal);
        Assert.Contains("session #53", requests[2].Heading, StringComparison.Ordinal);
        Assert.Equal("SELECT * FROM dbo.Posts", Field(requests[0], "Query"));
        Assert.Equal("90,000 ms", Field(requests[0], "CPU Time"));
        Assert.Equal("SOS_SCHEDULER_YIELD", Field(requests[0], "Wait Type"));

        var more = Item(fired, "More requests");
        Assert.Contains("1 more request", Field(more, "Not shown"), StringComparison.Ordinal);
        Assert.False(Assert.Single(h.Deliverer.Outcomes).Muted);
    }

    [Fact]
    public async Task TheRequestRead_IsTheUnfilteredThresholdZeroSnapshotRead()
    {
        var h = Armed();
        h.Adapter.LongRunning.Add(Request(51, cpuMs: 1_000));

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var args = h.Adapter.LastLrqArgs;
        Assert.NotNull(args);
        Assert.Equal(0, args!.Value.ThresholdMinutes);
        Assert.Equal(AlertEngine.ActiveMaintenanceProbeMaxRows, args.Value.MaxResults);
        Assert.False(args.Value.Diag);
        Assert.False(args.Value.WaitFor);
        Assert.False(args.Value.Backups);
        Assert.False(args.Value.Misc);
        Assert.False(args.Value.Cdc);
        Assert.Empty(args.Value.Excluded);
        Assert.Same(LongRunningQueryExclusions.None, h.Adapter.LastLrqExclusions);
    }

    [Fact]
    public async Task BelowTheBar_NoQuerySnapshotIsRead()
    {
        var h = Armed();
        h.Adapter.RunnableQueue = Samples(2.0, 8);
        h.Adapter.LongRunning.Add(Request(51, cpuMs: 1_000));

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Null(h.Adapter.LastLrqArgs);
    }

    [Fact]
    public async Task NoCpuSampleInTheWindow_FallsBackToTheNewestReading_AndSaysSo()
    {
        var h = Armed();

        await h.Build().EvaluateServerAsync(WithCpuSamples(h, (9, 10, 10), (12, 20, 20)));

        var cpu = Item(Assert.Single(h.Deliverer.Outcomes), "CPU (newest sample");
        Assert.Equal("50%", Field(cpu, "SQL Server CPU"));
        Assert.Equal("55%", Field(cpu, "Total CPU"));
    }

    [Fact]
    public async Task NoCpuAtAll_AndNoRequests_StillFires_WithoutDetailItems()
    {
        var h = Armed();

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(AlertSeverityLevel.Critical, fired.Severity);
        Assert.Empty(fired.Context!.Details);
    }

    [Fact]
    public async Task AFailedRequestRead_CostsTheCardItsRequests_NotTheAlert_AndIsCounted()
    {
        var counter = new AlertReadFailureCounter(() => new DateTime(2026, 10, 12, 8, 0, 0, DateTimeKind.Utc));
        var h = new AlertEngineTests.Harness { ReadFailures = counter };
        h.Settings.RunnableQueueEnabled = true;
        h.Adapter.RunnableQueue = Samples(20.0, 80);
        h.Adapter.ThrowOnLongRunningRead = true;

        await h.Build().EvaluateServerAsync(WithCpuSamples(h, (1, 80, 90), (2, 60, 70), (3, 70, 80)));

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.DoesNotContain(fired.Context!.Details, d => d.Heading.StartsWith("Top request:", StringComparison.Ordinal));
        Item(fired, "CPU over the last 5 minutes");
        Assert.Equal(1, counter.ReadFor(Key).ServerReadFailures);
    }

    [Fact]
    public async Task AMutedFire_StillCarriesTheDetail_SoTheHistoryRowMatchesTheCardThatWouldHaveBeenSent()
    {
        var h = Armed();
        h.IsMuted = _ => true;
        h.Adapter.LongRunning.Add(Request(51, cpuMs: 1_000));

        await h.Build().EvaluateServerAsync(WithCpuSamples(h, (1, 80, 90), (2, 60, 70), (3, 70, 80)));

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.True(fired.Muted);
        Item(fired, "CPU over the last 5 minutes");
        Item(fired, "Top request:");
    }

    [Fact]
    public async Task TheDetailRenders_InBothEmailBodies_AndInTheFlattenedText()
    {
        var h = Armed();
        h.Adapter.LongRunning.Add(Request(52, cpuMs: 90_000, text: "SELECT * FROM dbo.Posts"));

        await h.Build().EvaluateServerAsync(WithCpuSamples(h, (1, 80, 90), (2, 60, 70), (3, 70, 80)));

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Contains("CPU over the last 5 minutes", fired.DetailText, StringComparison.Ordinal);
        Assert.Contains("Top request: session #52", fired.DetailText, StringComparison.Ordinal);

        var (html, plain) = EmailTemplateBuilder.BuildAlertEmail(
            fired.MetricName, fired.ServerName, fired.CurrentValue, fired.ThresholdValue, 15, Branding,
            fired.Context, fired.DetailText);

        foreach (var body in new[] { html, plain })
        {
            Assert.Contains("CPU over the last 5 minutes", body, StringComparison.Ordinal);
            Assert.Contains("70% average, 80% peak", body, StringComparison.Ordinal);
            Assert.Contains("Top request: session #52", body, StringComparison.Ordinal);
            Assert.Contains("dbo.Posts", body, StringComparison.Ordinal);
        }
    }

    /* ---------------- the builders, without an engine ---------------- */

    [Fact]
    public void CpuItem_WindowBounds_IncludeTheFloor_ExcludeTheFuture_AndSkipMissingReadings()
    {
        var now = new DateTime(2026, 10, 12, 12, 0, 0, DateTimeKind.Utc);
        var samples = new List<AlertCpuSample>
        {
            new(now.AddMinutes(-5), 10, 20),                    /* exactly on the floor: in */
            new(now.AddMinutes(-5).AddTicks(-1), 99, 99),       /* just before it: out */
            new(now.AddSeconds(1), 99, 99),                     /* in the future: out */
            new(now.AddMinutes(-1), null, null),                /* no reading: counted in neither average */
            new(now.AddMinutes(-2), 30, 40),
        };

        var item = AlertContextBuilders.BuildRunnableQueueCpuItem(samples, 1, 2, now, 5);

        Assert.NotNull(item);
        Assert.Equal("20% average, 30% peak", Field(item!, "SQL Server CPU"));
        Assert.Equal("30% average, 40% peak", Field(item!, "Total CPU"));
        Assert.Equal("3", Field(item!, "CPU samples"));
    }

    [Fact]
    public void CpuItem_NothingKnown_IsNull_AndTheFallbackNamesItsSource()
    {
        var now = new DateTime(2026, 10, 12, 12, 0, 0, DateTimeKind.Utc);

        Assert.Null(AlertContextBuilders.BuildRunnableQueueCpuItem(null, null, null, now, 5));

        var fallback = AlertContextBuilders.BuildRunnableQueueCpuItem(null, 41.6, null, now, 5);
        Assert.NotNull(fallback);
        Assert.Contains("newest sample", fallback!.Heading, StringComparison.Ordinal);
        Assert.Equal("42%", Field(fallback, "SQL Server CPU"));
        Assert.DoesNotContain(fallback.Fields, f => f.Label == "Total CPU");
    }

    [Fact]
    public void RequestItems_NoSessions_IsEmpty_AndExactlyTheCap_HasNoOmissionItem()
    {
        Assert.Empty(AlertContextBuilders.BuildRunnableQueueRequestItems(Array.Empty<LongRunningQueryInfo>()));

        var atCap = Enumerable.Range(1, AlertContextBuilders.RunnableQueueRequestDisplayCap)
            .Select(i => Request(i, cpuMs: i))
            .ToList();
        var items = AlertContextBuilders.BuildRunnableQueueRequestItems(atCap);
        Assert.Equal(AlertContextBuilders.RunnableQueueRequestDisplayCap, items.Count);
        Assert.DoesNotContain(items, i => i.Heading == "More requests");
    }

    [Fact]
    public void RequestItems_TiesOnCpu_BreakOnElapsedThenSession_AndALongStatementIsTruncated()
    {
        var sessions = new List<LongRunningQueryInfo>
        {
            Request(9, cpuMs: 500, elapsedSeconds: 10),
            Request(8, cpuMs: 500, elapsedSeconds: 20, text: new string('x', 2000)),
            Request(7, cpuMs: 500, elapsedSeconds: 20),
        };

        var items = AlertContextBuilders.BuildRunnableQueueRequestItems(sessions);

        Assert.Contains("session #7", items[0].Heading, StringComparison.Ordinal);
        Assert.Contains("session #8", items[1].Heading, StringComparison.Ordinal);
        Assert.Contains("session #9", items[2].Heading, StringComparison.Ordinal);
        Assert.True(Field(items[1], "Query").Length < 2000);
    }
}
