/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The TempDB Log Space alert (#8881): judged on the log's used percent behind the same three-sample gate as tempdb Space, naming
/// the holder, and quiet where the log was never measured.
/// </summary>
public sealed class TempDbLogSpaceAlertTests
{
    private static async Task<DateTime> DriveAsync(
        AlertEngine engine, AlertEngineTests.Harness h, double? usedPercent, int samples, DateTime from, bool withHolder = true, string? holdup = "ACTIVE_TRANSACTION", string? reuseWait = "ACTIVE_TRANSACTION")
    {
        var at = from;
        for (var i = 0; i < samples; i++)
        {
            at = at.AddMinutes(1);
            h.Adapter.TempDb = new TempDbSpaceInfo
            {
                TotalReservedMb = 100,
                UnallocatedMb = 900,
                CollectionTimeUtc = at,
                LogTotalMb = 1000,
                LogUsedMb = usedPercent is { } p ? p * 10 : null,
                LogUsedPercent = usedPercent,
                LogReuseWaitDesc = reuseWait,
                LogTruncationHoldupReason = holdup,
                HolderSessionId = withHolder ? 57 : null,
                HolderLoginName = withHolder ? "etl_login" : null,
                HolderProgramName = withHolder ? "nightly load" : null,
                HolderBeginTimeUtc = withHolder ? h.Now.AddMinutes(-42) : null,
                HolderLogBytesUsed = withHolder ? 734003200 : null,
                HolderLogBytesReserved = withHolder ? 1048576000 : null,
            };
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        }

        return at;
    }

    [Fact]
    public async Task UnderTheBar_DoesNotFire()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.TempDbLogSpaceEnabled = true;
        var engine = h.Build();
        await DriveAsync(engine, h, 79, AlertEngine.TempDbLogSpaceBreachSamples * 3, AlertEngineTests.Harness.SampleBase);
        Assert.DoesNotContain(h.Deliverer.Outcomes, o => o.MetricName == AlertEngine.TempDbLogSpacePersistenceMetric);
    }

    [Fact]
    public async Task OverTheBarForTheGatesSamples_FiresOnceAndNamesTheHolder()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.TempDbLogSpaceEnabled = true;
        var engine = h.Build();
        var at = await DriveAsync(engine, h, 90, AlertEngine.TempDbLogSpaceBreachSamples - 1, AlertEngineTests.Harness.SampleBase);
        Assert.DoesNotContain(h.Deliverer.Outcomes, o => o.MetricName == AlertEngine.TempDbLogSpacePersistenceMetric);

        await DriveAsync(engine, h, 90, 1, at);
        var fired = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == AlertEngine.TempDbLogSpacePersistenceMetric);
        Assert.Equal(90d, fired.NumericCurrentValue!.Value, precision: 3);
        Assert.Equal("80%", fired.ThresholdValue);
        Assert.Contains("tempdb log at 90% of its size", fired.CurrentValue, StringComparison.Ordinal);
        Assert.Contains("ACTIVE_TRANSACTION", fired.CurrentValue, StringComparison.Ordinal);
        Assert.Contains("session 57", fired.CurrentValue, StringComparison.Ordinal);
        Assert.Contains("etl_login", fired.CurrentValue, StringComparison.Ordinal);
        Assert.Contains("nightly load", fired.CurrentValue, StringComparison.Ordinal);
        Assert.Contains("42 min", fired.CurrentValue, StringComparison.Ordinal);
        Assert.Contains("700 MB of log used", fired.CurrentValue, StringComparison.Ordinal);
    }

    [Fact]
    public async Task HoldupReasonNull_StillFires_AndSaysUnknown()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.TempDbLogSpaceEnabled = true;
        var engine = h.Build();
        await DriveAsync(engine, h, 95, AlertEngine.TempDbLogSpaceBreachSamples, AlertEngineTests.Harness.SampleBase, holdup: null);
        var fired = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == AlertEngine.TempDbLogSpacePersistenceMetric);
        Assert.Contains("holdup reason unknown", fired.CurrentValue, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NoLogMeasurement_FreezesTheGate_NeverFiresNorResolves()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.TempDbLogSpaceEnabled = true;
        var engine = h.Build();
        var at = await DriveAsync(engine, h, 90, AlertEngine.TempDbLogSpaceBreachSamples, AlertEngineTests.Harness.SampleBase);
        var firedBefore = h.Deliverer.Outcomes.Count(o => o.MetricName == AlertEngine.TempDbLogSpacePersistenceMetric);
        Assert.Equal(1, firedBefore);

        await DriveAsync(engine, h, null, 3, at);
        Assert.DoesNotContain(h.Resolutions, r => r.MetricName == AlertEngine.TempDbLogSpacePersistenceMetric);
        Assert.Equal(1, h.Deliverer.Outcomes.Count(o => o.MetricName == AlertEngine.TempDbLogSpacePersistenceMetric));
    }

    [Fact]
    public async Task BelowTheBarAfterAFire_Resolves_AndTheSwitchTurnsItOff()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.TempDbLogSpaceEnabled = true;
        var engine = h.Build();
        var at = await DriveAsync(engine, h, 90, AlertEngine.TempDbLogSpaceBreachSamples, AlertEngineTests.Harness.SampleBase);
        await DriveAsync(engine, h, 20, AlertEngine.TempDbLogSpaceClearSamples, at, withHolder: false);
        var resolution = Assert.Single(h.Resolutions, r => r.MetricName == AlertEngine.TempDbLogSpacePersistenceMetric);
        Assert.Equal("tempdb Log Space Resolved", resolution.Title);

        var off = new AlertEngineTests.Harness();
        var offEngine = off.Build();
        await DriveAsync(offEngine, off, 99, AlertEngine.TempDbLogSpaceBreachSamples * 2, AlertEngineTests.Harness.SampleBase);
        Assert.DoesNotContain(off.Deliverer.Outcomes, o => o.MetricName == AlertEngine.TempDbLogSpacePersistenceMetric);
    }

    [Fact]
    public async Task NothingHoldsTheLog_NeverFires_HoweverHighTheUsedPercent()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.TempDbLogSpaceEnabled = true;
        var engine = h.Build();
        await DriveAsync(engine, h, 90, AlertEngine.TempDbLogSpaceBreachSamples * 2, AlertEngineTests.Harness.SampleBase, withHolder: false, holdup: null, reuseWait: "NOTHING");
        Assert.DoesNotContain(h.Deliverer.Outcomes, o => o.MetricName == AlertEngine.TempDbLogSpacePersistenceMetric);
    }

    [Fact]
    public async Task NothingSampleBetweenBreaches_ResetsTheStreak()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.TempDbLogSpaceEnabled = true;
        var engine = h.Build();
        var at = await DriveAsync(engine, h, 90, AlertEngine.TempDbLogSpaceBreachSamples - 1, AlertEngineTests.Harness.SampleBase);
        at = await DriveAsync(engine, h, 90, 1, at, withHolder: false, holdup: null, reuseWait: "NOTHING");
        await DriveAsync(engine, h, 90, AlertEngine.TempDbLogSpaceBreachSamples - 1, at);
        Assert.DoesNotContain(h.Deliverer.Outcomes, o => o.MetricName == AlertEngine.TempDbLogSpacePersistenceMetric);
    }

    [Fact]
    public async Task ActiveTransactionWithAHolder_StillFires()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.TempDbLogSpaceEnabled = true;
        var engine = h.Build();
        await DriveAsync(engine, h, 90, AlertEngine.TempDbLogSpaceBreachSamples, AlertEngineTests.Harness.SampleBase);
        Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == AlertEngine.TempDbLogSpacePersistenceMetric);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task AnUnknownReuseWait_StillFiresOnThePercent(string? reuseWait)
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.TempDbLogSpaceEnabled = true;
        var engine = h.Build();
        await DriveAsync(engine, h, 90, AlertEngine.TempDbLogSpaceBreachSamples, AlertEngineTests.Harness.SampleBase, withHolder: false, holdup: null, reuseWait: reuseWait);
        Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == AlertEngine.TempDbLogSpacePersistenceMetric);
    }

    [Fact]
    public async Task NothingWithACapturedHolder_StillFires()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.TempDbLogSpaceEnabled = true;
        var engine = h.Build();
        await DriveAsync(engine, h, 90, AlertEngine.TempDbLogSpaceBreachSamples, AlertEngineTests.Harness.SampleBase, reuseWait: "NOTHING");
        Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == AlertEngine.TempDbLogSpacePersistenceMetric);
    }

    [Fact]
    public async Task AnOpenIncident_ResolvesOnANothingSampleWithNoHolder_EvenAtTheSamePercent()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.TempDbLogSpaceEnabled = true;
        var engine = h.Build();
        var at = await DriveAsync(engine, h, 90, AlertEngine.TempDbLogSpaceBreachSamples, AlertEngineTests.Harness.SampleBase);
        Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == AlertEngine.TempDbLogSpacePersistenceMetric);
        await DriveAsync(engine, h, 90, AlertEngine.TempDbLogSpaceClearSamples, at, withHolder: false, holdup: null, reuseWait: "NOTHING");
        var resolution = Assert.Single(h.Resolutions, r => r.MetricName == AlertEngine.TempDbLogSpacePersistenceMetric);
        Assert.Equal("tempdb Log Space Resolved", resolution.Title);
    }

    [Fact]
    public async Task AMutedServer_FiresMuted()
    {
        var h = new AlertEngineTests.Harness { Muted = true };
        h.Settings.TempDbLogSpaceEnabled = true;
        var engine = h.Build();
        await DriveAsync(engine, h, 90, AlertEngine.TempDbLogSpaceBreachSamples, AlertEngineTests.Harness.SampleBase);
        var fired = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == AlertEngine.TempDbLogSpacePersistenceMetric);
        Assert.True(fired.Muted);
    }

    private static TempDbSpaceInfo LogInfo(DateTime now, bool sessionless) => new()
    {
        TotalReservedMb = 100,
        UnallocatedMb = 900,
        CollectionTimeUtc = now,
        LogTotalMb = 1000,
        LogUsedMb = 900,
        LogUsedPercent = 90,
        LogReuseWaitDesc = "ACTIVE_TRANSACTION",
        LogTruncationHoldupReason = "ACTIVE_TRANSACTION",
        HolderBeginTimeUtc = sessionless ? now.AddMinutes(-42) : null,
        HolderLogBytesUsed = sessionless ? 734003200 : null,
    };

    [Fact]
    public void ASessionlessHolder_IsNamedAsASystemOrOrphanedTransaction_NotSaidToBeMissing()
    {
        var now = new DateTime(2026, 11, 16, 12, 0, 0, DateTimeKind.Utc);

        var message = AlertContextBuilders.TempDbLogSpaceMessage(LogInfo(now, sessionless: true), now);
        Assert.Contains("the oldest open tempdb transaction has no session (a system or orphaned transaction), open 42 min 0 s, 700 MB of log used", message, StringComparison.Ordinal);
        Assert.DoesNotContain("no holding transaction captured", message, StringComparison.Ordinal);

        var card = AlertContextBuilders.BuildTempDbLogSpaceContext(LogInfo(now, sessionless: true), now)!.Details.Single().Fields;
        Assert.Contains(card, f => f.Item1 == "Holder" && f.Item2.Contains("no session (a system or orphaned transaction)", StringComparison.Ordinal));
        Assert.DoesNotContain(card, f => f.Item2 == "None captured");
    }

    [Fact]
    public void NoHolderAtAll_StillSaysNoHoldingTransactionWasCaptured()
    {
        var now = new DateTime(2026, 11, 16, 12, 0, 0, DateTimeKind.Utc);

        Assert.EndsWith(", no holding transaction captured", AlertContextBuilders.TempDbLogSpaceMessage(LogInfo(now, sessionless: false), now), StringComparison.Ordinal);
        var card = AlertContextBuilders.BuildTempDbLogSpaceContext(LogInfo(now, sessionless: false), now)!.Details.Single().Fields;
        Assert.Contains(card, f => f.Item1 == "Holder" && f.Item2 == "None captured");
    }

    [Fact]
    public void NoHolderUnderTheReadBar_SaysTheHolderIsNotReadThere_NotThatNoneWasCaptured()
    {
        var now = new DateTime(2026, 11, 16, 12, 0, 0, DateTimeKind.Utc);
        var info = LogInfo(now, sessionless: false);
        info.LogUsedPercent = 30;
        info.LogUsedMb = 300;

        var message = AlertContextBuilders.TempDbLogSpaceMessage(info, now);
        Assert.EndsWith(", the holder is read at 50% log used and above", message, StringComparison.Ordinal);
        Assert.DoesNotContain("no holding transaction captured", message, StringComparison.Ordinal);
        var card = AlertContextBuilders.BuildTempDbLogSpaceContext(info, now)!.Details.Single().Fields;
        Assert.Contains(card, f => f.Item1 == "Holder" && f.Item2.Contains("the holder is read at 50% log used and above", StringComparison.Ordinal));
        Assert.DoesNotContain(card, f => f.Item2 == "None captured");

        info.LogUsedPercent = 50;
        Assert.EndsWith(", no holding transaction captured", AlertContextBuilders.TempDbLogSpaceMessage(info, now), StringComparison.Ordinal);
    }
}
