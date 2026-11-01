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
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The 2026-10-09 incident replayed through the Runnable Queue rule (#5641). The rows are the <c>cpu_scheduler_stats</c>
/// rows of the incident window, copied as (collection time, avg_runnable_tasks_count, total_runnable_tasks_count), with
/// <c>scheduler_count</c> 4 on every row. Several collections hold SEVERAL rows each covering a subset of the
/// schedulers (the collector defect #5710; old rows stay split), and under load the collector ran late (gaps of 2.3, 4
/// and 5 minutes), which is what the per-collection fold and the 10-minute fallback are for. The same rows go through
/// the DuckDB read in <c>RunnableQueueWindowReadTests</c> (Lite.Tests).
/// </summary>
public sealed class RunnableQueueIncidentReplayTests
{
    private const int Schedulers = 4;

    /// <summary>One row of the table: time of day, then avg|total pairs (one pair per row of the collection).</summary>
    private static readonly (string Time, (double Avg, long Total)[] Rows)[] IncidentRows =
    {
        ("15:30:28.573", new[] { (0.00, 0L) }),
        ("15:31:30.741", new[] { (0.00, 0L) }),
        ("15:32:31.872", new[] { (2.25, 9L) }),
        ("15:33:28.181", new[] { (0.00, 0L) }),
        ("15:34:30.404", new[] { (0.00, 0L) }),
        ("15:35:28.451", new[] { (0.75, 3L) }),
        ("15:36:29.260", new[] { (0.00, 0L) }),
        ("15:37:29.873", new[] { (0.00, 0L) }),
        ("15:38:28.757", new[] { (0.00, 0L), (1.00, 1L), (0.00, 0L), (0.00, 0L) }),
        ("15:39:30.198", new[] { (0.00, 0L) }),
        ("15:40:29.006", new[] { (1.00, 4L) }),
        ("15:41:29.926", new[] { (12.75, 51L) }),
        ("15:42:43.814", new[] { (27.67, 83L), (18.00, 18L) }),
        ("15:45:00.871", new[] { (35.00, 35L), (32.00, 96L) }),
        ("15:49:01.703", new[] { (60.00, 60L), (50.50, 101L), (49.00, 49L) }),
        ("15:54:00.022", new[] { (10.00, 10L), (7.50, 15L), (10.00, 10L) }),
        ("15:55:01.517", new[] { (3.33, 10L), (8.00, 8L) }),
        ("15:55:29.287", new[] { (1.00, 1L), (0.00, 0L), (0.00, 0L) }),
        ("15:56:29.695", new[] { (0.50, 2L) }),
        ("15:57:31.491", new[] { (0.00, 0L) }),
        ("15:58:29.323", new[] { (0.25, 1L) }),
        ("15:59:30.398", new[] { (0.00, 0L) }),
    };

    private static DateTime At(string timeOfDay) =>
        DateTime.ParseExact("2026-10-09 " + timeOfDay, "yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture)
            .AddTicks(0);

    /// <summary>The read's per-collection fold, written out once more in C#: a lone row uses its own average, several
    /// rows use SUM(total) / MAX(scheduler_count).</summary>
    private static RunnableQueueSample Fold(string time, (double Avg, long Total)[] rows) =>
        new(At(time),
            rows.Length == 1 ? rows[0].Avg : (double)rows.Sum(r => r.Total) / Schedulers,
            rows.Sum(r => r.Total),
            Schedulers);

    private static List<RunnableQueueSample> AllCollections() =>
        IncidentRows.Select(r => Fold(r.Time, r.Rows)).ToList();

    /// <summary>What the read hands the engine at <paramref name="t"/>: collections already written and inside the
    /// 60-minute lookback, newest first.</summary>
    private static List<RunnableQueueSample> ReadAt(DateTime t) =>
        AllCollections()
            .Where(s => s.CollectionTime <= t && s.CollectionTime >= t.AddMinutes(-RunnableQueueEvaluator.ReadLookbackMinutes))
            .OrderByDescending(s => s.CollectionTime)
            .ToList();

    private static RunnableQueueEvaluator.Finding? Judge(string time, out RunnableQueueReading? reading)
    {
        var t = At(time);
        reading = RunnableQueueEvaluator.Read(ReadAt(t), t);
        return RunnableQueueEvaluator.Evaluate(reading, 8, 16);
    }

    [Fact]
    public void TheFold_GivesTheCollectionValuesNotTheRowAverages()
    {
        var byTime = AllCollections().ToDictionary(s => s.CollectionTime.ToString("HH:mm:ss", CultureInfo.InvariantCulture), s => s.PerScheduler);

        Assert.Equal(0.25, byTime["15:38:28"], 6);
        Assert.Equal(25.25, byTime["15:42:43"], 6);
        Assert.Equal(32.75, byTime["15:45:00"], 6);
        Assert.Equal(52.5, byTime["15:49:01"], 6);
        Assert.Equal(8.75, byTime["15:54:00"], 6);
        Assert.Equal(4.5, byTime["15:55:01"], 6);
        Assert.Equal(0.25, byTime["15:55:29"], 6);

        /* A lone row keeps its own average exactly, whatever total / scheduler_count says. */
        Assert.Equal(12.75, byTime["15:41:29"], 6);
        Assert.Equal(2.25, byTime["15:32:31"], 6);
    }

    [Theory]
    [InlineData("15:42:44.000", null, 7.85)]
    [InlineData("15:43:28.757", null, 7.85)]
    public void BeforeTheOldestZeroFallsOut_TheAverageIsUnderTheWarningBar(string time, AlertSeverityLevel? expected, double average)
    {
        var finding = Judge(time, out var reading);

        Assert.Equal(expected, finding?.Severity);
        Assert.Equal(average, reading!.AveragePerScheduler, 2);
    }

    [Fact]
    public void WarningFromTheFirstEvaluationAfterTheOldZeroFallsOut()
    {
        var finding = Judge("15:43:29.000", out var reading);

        Assert.Equal(AlertSeverityLevel.Warning, finding!.Severity);
        Assert.Equal(9.75, reading!.AveragePerScheduler, 2);
        Assert.Equal(4, reading.SampleCount);
    }

    [Fact]
    public void CriticalWhenTheSplitCollectionLandsAt_1500()
    {
        var finding = Judge("15:45:00.871", out var reading);

        Assert.Equal(AlertSeverityLevel.Critical, finding!.Severity);
        Assert.Equal(17.94, finding.AveragePerScheduler, 2);
        Assert.Equal(4, reading!.SampleCount);
    }

    [Theory]
    [InlineData("15:46:30.000", 23.58, 3, "Runnable tasks averaged 23.6 per scheduler over the last 3 collections (5 minutes, 4 schedulers).")]
    [InlineData("15:49:02.000", 36.83, 3, "Runnable tasks averaged 36.8 per scheduler over the last 3 collections (6 minutes, 4 schedulers).")]
    [InlineData("15:54:01.000", 31.33, 3, "Runnable tasks averaged 31.3 per scheduler over the last 3 collections (9 minutes, 4 schedulers).")]
    [InlineData("15:55:02.000", 21.92, 3, "Runnable tasks averaged 21.9 per scheduler over the last 3 collections (6 minutes, 4 schedulers).")]
    public void CriticalIsHeldThroughTheSparseStretchByTheTenMinuteFallback(string time, double average, int samples, string summary)
    {
        var finding = Judge(time, out var reading);

        Assert.Equal(AlertSeverityLevel.Critical, finding!.Severity);
        Assert.Equal(average, finding.AveragePerScheduler, 2);
        Assert.Equal(samples, reading!.SampleCount);
        Assert.Equal(summary, finding.Summary);
    }

    [Fact]
    public void NoEvaluationAt_1553_BecauseTheThirdNewestCollectionIsOlderThanTenMinutes()
    {
        var finding = Judge("15:53:00.000", out var reading);

        Assert.Null(reading);
        Assert.Null(finding);
    }

    [Fact]
    public void ClearsAtTheCollectionThatMakesThreeInFiveMinutes()
    {
        /* One tick before the collection lands the fallback still holds the alert; the collection itself brings the
           5-minute window to three and the average to 4.5. */
        var before = Judge("15:55:29.000", out _);
        var cleared = Judge("15:55:29.287", out var reading);

        Assert.Equal(AlertSeverityLevel.Critical, before!.Severity);
        Assert.Null(cleared);
        Assert.Equal(4.5, reading!.AveragePerScheduler, 2);
        Assert.Equal(3, reading.SampleCount);
    }

    [Fact]
    public void TheRowAverageRead_WouldHaveGotTheIncidentWrong()
    {
        /* The old read averaged ROWS: at 15:42:44 the nine rows in the 5 minutes are 0, 1, 0, 0 (15:38:28), 0, 1,
           12.75, 27.67 and 18.00, which mixes subsets of the schedulers as if each were all of them. */
        var rowAverages = IncidentRows
            .Where(r => At(r.Time) >= At("15:37:44.000") && At(r.Time) <= At("15:42:44.000"))
            .SelectMany(r => r.Rows.Select(x => x.Avg))
            .ToList();

        Assert.Equal(9, rowAverages.Count);
        Assert.Equal(6.71, rowAverages.Average(), 2);
        Assert.NotEqual(7.85, rowAverages.Average(), 2);
    }

    /* ---------------- the same replay through the engine ---------------- */

    [Fact]
    public async Task TheEngine_WarnsOnceTheOldZeroFallsOut_DeliversTheCriticalAtOnce_HoldsThroughTheSparseStretch_AndClearsOnce()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.RunnableQueueEnabled = true;
        var engine = h.Build();

        var warningHeldAt = default(DateTime);
        var criticalFiredAt = default(DateTime);
        var cleared = new List<DateTime>();
        var seenResolutions = 0;

        for (var t = At("15:30:15.000"); t <= At("15:57:00.000"); t = t.AddSeconds(30))
        {
            h.Now = DateTime.SpecifyKind(t, DateTimeKind.Utc);
            h.Adapter.RunnableQueue = ReadAt(t);
            var firedBefore = h.Deliverer.Outcomes.Count;
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

            if (h.Deliverer.Outcomes.Count > firedBefore && warningHeldAt == default)
            {
                warningHeldAt = t;
                Assert.Equal(AlertSeverityLevel.Warning, h.Deliverer.Outcomes[^1].Severity);
                Assert.NotNull(h.Deliverer.Outcomes[^1].Hold);
            }

            if (h.Deliverer.Outcomes.Count > firedBefore && criticalFiredAt == default
                && h.Deliverer.Outcomes[^1].Severity == AlertSeverityLevel.Critical)
            {
                criticalFiredAt = t;
            }

            if (h.Resolutions.Count > seenResolutions)
            {
                seenResolutions = h.Resolutions.Count;
                cleared.Add(t);
            }
        }

        /* #7086: the Warning at 15:43:45 is HELD (a digest row, nothing sent), so the first page is the Critical. */
        Assert.Equal(At("15:43:45.000"), warningHeldAt);

        /* #5641 (seat decision): the Critical is delivered on the FIRST evaluation at or after the 15:45:00.871 collection
           that took the average to 17.94, not four to five minutes later. Since #7086 nothing stamped the page cooldown
           before it (the held Warning leaves it alone), so it needs no bypass flag to go out at once. */
        Assert.Equal(At("15:45:15.000"), criticalFiredAt);
        Assert.True(criticalFiredAt - warningHeldAt < TimeSpan.FromMinutes(5));
        Assert.Equal(
            new[] { AlertSeverityLevel.Warning, AlertSeverityLevel.Critical },
            h.Deliverer.Outcomes.Take(2).Select(o => o.Severity!.Value).ToArray());
        Assert.NotNull(h.Deliverer.Outcomes[0].Hold);
        Assert.Null(h.Deliverer.Outcomes[1].Hold);
        Assert.False(h.Deliverer.Outcomes[1].Context!.EscalationBypassesCooldown);
        Assert.Equal(new[] { At("15:55:45.000") }, cleared);
        Assert.All(h.Deliverer.Outcomes, o => Assert.Equal("Runnable Queue", o.MetricName));
        Assert.Contains(h.Deliverer.Outcomes, o => o.Severity == AlertSeverityLevel.Critical);
    }
}
