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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// PostgreSQL 19 renamed the <c>BufferPin</c> wait type to <c>Buffer</c> (its <c>BufferPin</c> event is now
/// <c>BufferCleanup</c>) and removed <c>LWLock:BufferContent</c>: those waits are now <c>Buffer:BufferShared</c>,
/// <c>Buffer:BufferShareExclusive</c> and <c>Buffer:BufferExclusive</c> (#9131). The same synthetic samples written with
/// the 18 names and with the 19 names must give the same facts and the same score, and the LWLock advice must name
/// the events the target's own major reports.
/// </summary>
public sealed class PgTargetPg19BufferWaitTests
{
    private static readonly string LWLock = PgTargetFactKeys.WaitKey("LWLock", null);

    private static AnalysisContext Context() => new()
    {
        ServerId = 1,
        ServerName = "pg",
        TimeRangeStart = new DateTime(2026, 9, 18, 0, 0, 0, DateTimeKind.Unspecified),
        TimeRangeEnd = new DateTime(2026, 9, 18, 4, 0, 0, DateTimeKind.Unspecified),
        Coverage = new WindowCoverage { NominalMs = 14_400_000, ObservedMs = 14_400_000, SampleCount = 241, LargestGapMs = 60_000 },
    };

    /* 4 h window. The content-lock waits total 8,640 s = 0.6 of a backend: a LWLock rollup past its concerning bar. */
    private static List<PgTargetFactCollector.WaitProfileRow> Rows18() =>
    [
        new("lock", "relation", 720_000, 100),
        new("lwlock", "walwrite", 144_000, 500),
        new("lwlock", "buffercontent", 8_640_000, 90_000),
        new("lwlock", "lockmanager", 72_000, 40),
        new("io", "datafileread", 1_440_000, 9_000),
        new("bufferpin", "bufferpin", 288_000, 70),
        new("cpu", "cpu", 20_000_000, 0),
    ];

    private static List<PgTargetFactCollector.WaitProfileRow> Rows19() =>
    [
        new("lock", "relation", 720_000, 100),
        new("lwlock", "walwrite", 144_000, 500),
        new("buffer", "buffershared", 5_040_000, 50_000),
        new("buffer", "buffershareexclusive", 2_160_000, 25_000),
        new("buffer", "bufferexclusive", 1_440_000, 15_000),
        new("lwlock", "lockmanager", 72_000, 40),
        new("io", "datafileread", 1_440_000, 9_000),
        new("buffer", "buffercleanup", 288_000, 70),
        new("cpu", "cpu", 20_000_000, 0),
    ];

    private static List<Fact> Emit(List<PgTargetFactCollector.WaitProfileRow> rows, bool sampled)
    {
        var facts = new List<Fact>();
        PgTargetFactCollector.EmitWaitFacts(Context(), facts, rows, observedSec: 14_400, sampleCount: 240, collectionCount: 240,
            sampled: sampled ? (PeriodMs: 10, Resets: 0, IntervalSec: 14_400, SampledMsKnown: true) : null,
            extra: null);
        return facts;
    }

    private static Fact Registry(int major) => new()
    {
        Source = PgTargetSources.ConfigSource,
        Key = PgTargetFactKeys.ServerMajorVersion,
        Value = major,
        ServerId = 1,
    };

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void The19Names_GiveTheSameFactsAndScoreAsThe18Names(bool sampled)
    {
        var on18 = Emit(Rows18(), sampled);
        var on19 = Emit(Rows19(), sampled);

        Assert.Equal(on18.Select(f => f.Key).Order(StringComparer.Ordinal), on19.Select(f => f.Key).Order(StringComparer.Ordinal));
        foreach (var fact18 in on18)
        {
            var fact19 = on19.Single(f => f.Key == fact18.Key);
            Assert.Equal(fact18.ObjectName, fact19.ObjectName);
            Assert.Equal(fact18.Value, fact19.Value, precision: 9);
            Assert.Equal(fact18.Metadata.Keys.Order(StringComparer.Ordinal), fact19.Metadata.Keys.Order(StringComparer.Ordinal));
            foreach (var (name, value) in fact18.Metadata)
                Assert.Equal(value, fact19.Metadata[name], precision: 9);
            Assert.Equal(PgTargetScorer.ScoreBase(fact18), PgTargetScorer.ScoreBase(fact19), precision: 9);
        }

        /* The pin is not vacuous: the content-lock waits are in the LWLock rollup and carry it past its bar. */
        var rollup = on19.Single(f => f.Key == LWLock);
        Assert.Equal((144_000 + 8_640_000 + 72_000) / 14_400_000.0, rollup.Value, precision: 9);
        Assert.True(PgTargetScorer.ScoreBase(rollup) > 0);
        Assert.Equal(3, rollup.Metadata[PgTargetScorer.WaitEventsInTypeKey]);
    }

    [Fact]
    public void ABufferCleanupWait_IsReadTheWayABufferPinWaitIs()
    {
        var withPin = Emit([new("lock", "relation", 720_000, 100), new("bufferpin", "bufferpin", 288_000, 70)], sampled: false);
        var withCleanup = Emit([new("lock", "relation", 720_000, 100), new("buffer", "buffercleanup", 288_000, 70)], sampled: false);

        /* No fact of its own, but it is in the share denominator exactly as the pin wait was. */
        Assert.DoesNotContain(withCleanup, f => f.Key.Contains("BUFFER", StringComparison.OrdinalIgnoreCase));
        Assert.Equal(withPin.Count, withCleanup.Count);
        var relation = PgTargetFactKeys.WaitKey("Lock", "relation");
        var pinShare = withPin.Single(f => f.Key == relation).Metadata[PgTargetScorer.WaitShareOfWaitTimeKey];
        var cleanupShare = withCleanup.Single(f => f.Key == relation).Metadata[PgTargetScorer.WaitShareOfWaitTimeKey];
        Assert.Equal(720_000.0 / (720_000 + 288_000), cleanupShare, precision: 9);
        Assert.Equal(pinShare, cleanupShare, precision: 9);
    }

    [Fact]
    public void AWindowSpanningAnUpgrade_SumsBothSpellingsOfTheContentLockWaits()
    {
        var mixed = Emit(
        [
            new("lwlock", "buffercontent", 4_320_000, 40_000, PeakBackends: 3),
            new("buffer", "buffershared", 2_880_000, 30_000, PeakBackends: 5),
            new("buffer", "bufferexclusive", 1_440_000, 20_000, PeakBackends: 2),
        ], sampled: true);

        var rollup = Assert.Single(mixed);
        Assert.Equal(LWLock, rollup.Key);
        Assert.Equal(8_640_000 / 14_400_000.0, rollup.Value, precision: 9);
        Assert.Equal(90_000, rollup.Metadata[PgTargetScorer.WaitCountKey]);
        Assert.Equal(1, rollup.Metadata[PgTargetScorer.WaitEventsInTypeKey]);
        Assert.Equal(5, rollup.Metadata[PgTargetScorer.WaitPeakBackendsKey]);
    }

    [Theory]
    [InlineData(17, true, false)]
    [InlineData(18, true, false)]
    [InlineData(19, false, true)]
    [InlineData(20, false, true)]
    public void TheLWLockAdvice_NamesTheContentLockEventsTheTargetsMajorReports(int major, bool namesBufferContent, bool namesBufferEvents)
    {
        var rollup = Emit(Rows19(), sampled: false).Single(f => f.Key == LWLock);
        var advice = PgTargetAdvice.Compose(LWLock, new[] { rollup, Registry(major) }.ToFactLookup())!;

        Assert.Equal(namesBufferContent, advice.Remediation.Contains("BufferContent", StringComparison.Ordinal));
        Assert.Equal(namesBufferEvents, advice.Remediation.Contains("BufferShareExclusive", StringComparison.Ordinal));
    }

    [Fact]
    public void TheLWLockAdvice_NamesBothSpellings_WhenTheAnalysisHasNoMajor()
    {
        var rollup = Emit(Rows18(), sampled: false).Single(f => f.Key == LWLock);
        var advice = PgTargetAdvice.Compose(LWLock, new[] { rollup }.ToFactLookup())!;

        Assert.Contains("BufferContent", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("BufferShareExclusive", advice.Remediation, StringComparison.Ordinal);
    }
}
