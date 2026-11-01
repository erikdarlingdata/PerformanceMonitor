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
using Xunit;
using static Darling.Tests.AlertEngineTests;

namespace Darling.Tests;

/// <summary>
/// #5910: every <see cref="AlertThresholdKey"/> resolves for the server it is asked about. For each key there is a
/// fixture that breaches (or grades) at the global value; an override for server A moves A's outcome on it, and
/// server B, holding the global value on the same data, is unchanged. The database-scope keys also move one
/// database and not the other on the same server.
/// </summary>
public sealed class AlertThresholdKeyBehaviourTests
{
    private const string ServerA = "101";
    private const string ServerB = "202";

    /// <summary>
    /// One key's fixture: <see cref="Arrange"/> makes the check breach at the global value, <see cref="Override"/> is a
    /// value that moves the bar, <see cref="Drive"/> evaluates one server (one sweep unless the check needs a streak),
    /// and <see cref="Probe"/> reads what that server's evaluation produced.
    /// </summary>
    private sealed record Fix(
        Action<Harness> Arrange,
        int Override,
        Func<AlertEngine, Harness, string, Task>? Drive = null,
        Func<Harness, string, string>? Probe = null,
        bool FailedJobsFetcher = false);

    private static AlertServerSnapshot SnapshotFor(string server, double? sqlCpu = null, double? totalCpu = null, DateTime? cpuSampleTime = null) =>
        Harness.Snapshot(sqlCpu: sqlCpu, totalCpu: totalCpu, cpuSampleTime: cpuSampleTime) with
        {
            ServerKey = server,
            ServerName = server == ServerA ? "SRV-A" : "SRV-B",
        };

    private static string Surface(Harness h, string server) =>
        string.Join(";", h.Deliverer.Outcomes
            .Where(o => o.ServerKey == server)
            .Select(o => $"{o.MetricName}|{o.Severity}|{o.CurrentValue}|{o.ThresholdValue}"));

    /// <summary>Everything an operator reads off the fired card: the lines and the rendered detail.</summary>
    private static string Card(Harness h, string server) =>
        string.Join(";", h.Deliverer.Outcomes
            .Where(o => o.ServerKey == server)
            .Select(o => $"{o.CurrentValue}|{o.ShortMessage}|{o.DetailText}"));

    private static Fix Fixture(AlertThresholdKey key) => key switch
    {
        AlertThresholdKey.CpuThresholdPercent => new Fix(
            h => h.Settings.CpuEnabled = true, 95,
            Drive: async (engine, _, server) =>
            {
                for (var i = 1; i <= AlertEngine.SqlCpuBreachSamples; i++)
                {
                    await engine.EvaluateServerAsync(SnapshotFor(server, 70, 90, Harness.SampleBase.AddMinutes(i)));
                }
            }),
        AlertThresholdKey.BlockingCountThreshold => new Fix(
            h =>
            {
                h.Settings.BlockingEnabled = true;
                h.Adapter.Blocking.Add(BlockingRow(55));
                h.Adapter.Blocking.Add(BlockingRow(56));
            }, 5),
        AlertThresholdKey.BlockingWaitSecondsThreshold => new Fix(
            h =>
            {
                h.Settings.BlockingEnabled = true;
                h.Settings.BlockingWaitSecondsThreshold = 60;
                /* One fresh snapshot at three times the bar fires at once; at an override of 100 it is over the bar
                   but under three times it, so a single snapshot is not yet an incident. */
                h.Adapter.BlockingWait = new CurrentBlockingWaitResult(new DateTime(2026, 7, 1, 11, 59, 0), 180_000, 145, true, 1);
            }, 100),
        AlertThresholdKey.DeadlockCountThreshold => new Fix(
            h =>
            {
                h.Settings.DeadlockEnabled = true;
                h.Adapter.Deadlocks.Add(DeadlockRow());
            }, 5),
        AlertThresholdKey.RunnableQueueWarningPerScheduler => new Fix(RunnableQueueArranged, 30),
        AlertThresholdKey.RunnableQueueCriticalPerScheduler => new Fix(RunnableQueueArranged, 40),
        AlertThresholdKey.ClientTimeoutsWarningPerMinute => new Fix(ClientTimeoutsArranged, 50),
        AlertThresholdKey.ClientTimeoutsCriticalPerMinute => new Fix(ClientTimeoutsArranged, 20),
        AlertThresholdKey.LongRunningQueryThresholdMinutes => new Fix(
            h => h.Settings.LongRunningQueryEnabled = true, 45,
            Probe: (h, server) => $"{h.Adapter.LastLrqArgs?.ThresholdMinutes}|{Surface(h, server)}"),
        AlertThresholdKey.TempDbSpaceThresholdPercent => new Fix(
            h => h.Settings.TempDbSpaceEnabled = true, 95,
            Drive: async (engine, h, server) =>
            {
                for (var i = 1; i <= AlertEngine.TempDbSpaceBreachSamples; i++)
                {
                    h.Adapter.TempDb = new TempDbSpaceInfo
                    {
                        TotalReservedMb = 900, UnallocatedMb = 100, CollectionTimeUtc = Harness.SampleBase.AddMinutes(i),
                    };
                    await engine.EvaluateServerAsync(SnapshotFor(server));
                }
            }),
        /* 8% / 8 GB free: under the 10% bar, over the 5 GB bar, over the critical bars (3% / 2 GB). */
        AlertThresholdKey.LowDiskThresholdPercent => new Fix(h => LowDiskArranged(h, totalMb: 102_400, freeMb: 8192), 5),
        /* 20% / 4 GB free: over the 10% bar, under the 5 GB bar. */
        AlertThresholdKey.LowDiskThresholdGb => new Fix(h => LowDiskArranged(h, totalMb: 20_480, freeMb: 4096), 3),
        /* The 8% volume is a Warning at the global 3% critical bar and Critical once that bar is 9%. */
        AlertThresholdKey.DiskCriticalFreePercent => new Fix(h => LowDiskArranged(h, totalMb: 102_400, freeMb: 8192), 9),
        AlertThresholdKey.DiskCriticalFreeGb => new Fix(h => LowDiskArranged(h, totalMb: 20_480, freeMb: 4096), 8),
        AlertThresholdKey.PvsThresholdPercent => new Fix(PvsArranged, 70),
        AlertThresholdKey.PvsFloorGb => new Fix(PvsArranged, 10),
        AlertThresholdKey.FileGrowthRiseMb => new Fix(
            h =>
            {
                h.Settings.FileGrowthEnabled = true;
                h.Adapter.Files.Add(RiseOnlyFile(null));
            }, 100_000),
        AlertThresholdKey.FileGrowthVolumePercent => new Fix(
            h =>
            {
                h.Settings.FileGrowthEnabled = true;
                h.Adapter.Files.Add(LevelOnlyFile(null));
            }, 95),
        AlertThresholdKey.FileGrowthLookbackMinutes => new Fix(
            h =>
            {
                h.Settings.FileGrowthEnabled = true;
                h.Adapter.Files.Add(RiseOnlyFile(null));
            }, 120,
            Probe: (h, server) => $"{h.Adapter.FileGrowthLookbackAsked}|{Surface(h, server)}"),
        AlertThresholdKey.LongRunningJobMultiplier => new Fix(
            h =>
            {
                h.Settings.LongRunningJobEnabled = true;
                h.Adapter.AnomalousJobs.Add(new AnomalousJobInfo
                {
                    JobName = "Nightly ETL", JobId = "job-1", StartTime = new DateTime(2026, 7, 1, 11, 0, 0),
                    CurrentDurationSeconds = 3600, AvgDurationSeconds = 900, PercentOfAverage = 400,
                });
            }, 10),
        AlertThresholdKey.FailedJobLookbackMinutes => new Fix(
            h =>
            {
                h.Settings.FailedJobEnabled = true;
                h.FailedJobs.Add(new FailedJobInfo { JobName = "Backup.Full", JobId = "j1", RunDateTime = h.Now.AddMinutes(-10), UtcOffsetMinutes = 0 });
            }, 45,
            /* The card's dedup key is per server; the window it states is what the lookback moves. */
            Probe: (h, server) => string.Join(";", h.Deliverer.Outcomes
                .Where(o => o.ServerKey == server)
                .SelectMany(o => (o.DetailText ?? "").Split('\n'))
                .Where(line => line.Contains(AlertContextBuilders.FailureWindowFromLabel, StringComparison.Ordinal))
                .Select(line => line.Trim())),
            FailedJobsFetcher: true),
        _ => throw new NotSupportedException($"{key} has no fixture: add one that breaches at the global value and moves with an override."),
    };

    private static void RunnableQueueArranged(Harness h)
    {
        h.Settings.RunnableQueueEnabled = true;
        /* 20 per scheduler: Critical at the global 8 / 16 pair. A Warning override of 30 lifts the Critical bar with it
           (the evaluator clamps the pair); a Critical override of 40 leaves the same reading a Warning. */
        h.Adapter.RunnableQueue = Enumerable.Range(0, 5)
            .Select(i => new RunnableQueueSample(h.Now.AddSeconds(-10).AddMinutes(-i), 20, 80, 4))
            .ToList();
    }

    private static void ClientTimeoutsArranged(Harness h)
    {
        h.Settings.ClientTimeoutsEnabled = true;
        /* 90 timeouts over three minutes is 30 a minute: a Warning at the global 10 / 60 pair. */
        h.Adapter.Attention = AttentionAlertTests.StormWindow(h.Now, 90);
    }

    private static void LowDiskArranged(Harness h, double totalMb, double freeMb)
    {
        h.Settings.LowDiskEnabled = true;
        h.Adapter.Volumes.Add(new VolumeFreeSpaceInfo { MountPoint = "D:\\", TotalMb = totalMb, FreeMb = freeMb });
    }

    private static void PvsArranged(Harness h)
    {
        h.Settings.PvsEnabled = true;
        /* shop is 60% and 6 GB, ledger 50% and 2 GB: both over the global 40% bar and the 1 GB floor. */
        h.Adapter.PvsDatabases.Add(new PvsPressureInfo { DatabaseName = "shop", PvsSizeMb = 6144, DatabaseDataSizeMb = 10240 });
        h.Adapter.PvsDatabases.Add(new PvsPressureInfo { DatabaseName = "ledger", PvsSizeMb = 2048, DatabaseDataSizeMb = 4096 });
    }

    private static async Task<(string A, string B)> RunAsync(Fix fix, IAlertThresholdOverrides? overrides)
    {
        var h = new Harness { Overrides = overrides };
        fix.Arrange(h);
        var engine = h.Build(withFailedJobsFetcher: fix.FailedJobsFetcher);
        var drive = fix.Drive ?? ((e, _, server) => e.EvaluateServerAsync(SnapshotFor(server)));
        var probe = fix.Probe ?? Surface;

        await drive(engine, h, ServerA);
        var a = probe(h, ServerA);
        await drive(engine, h, ServerB);
        return (a, probe(h, ServerB));
    }

    public static IEnumerable<object[]> AllKeys() => Enum.GetValues<AlertThresholdKey>().Select(k => new object[] { k });

    public static IEnumerable<object[]> DatabaseScopedKeys() =>
        Enum.GetValues<AlertThresholdKey>().Where(k => AlertThresholdKeys.ScopeOf(k) == AlertThresholdScope.ServerAndDatabase).Select(k => new object[] { k });

    [Theory]
    [MemberData(nameof(AllKeys))]
    public async Task ServerOverride_MovesThatServersOutcome_ForEveryKey_AndLeavesTheOther(AlertThresholdKey key)
    {
        var fix = Fixture(key);

        var baseline = await RunAsync(fix, overrides: null);
        /* The fixture breaches at the global value, the same on both servers: otherwise the test proves nothing. */
        Assert.NotEqual("", baseline.A.Replace("|", "").Replace(";", ""));
        Assert.Equal(baseline.A, baseline.B);

        var overrides = new AlertThresholdOverrideTests.MapOverrides();
        overrides.Values[(key, ServerA, null)] = fix.Override;
        var moved = await RunAsync(fix, overrides);

        Assert.NotEqual(baseline.A, moved.A);
        Assert.Equal(baseline.B, moved.B);
        Assert.Contains(overrides.Calls, c => c.Key == key && c.Server == ServerA);
    }

    private static Fix DatabaseFixture(AlertThresholdKey key) => key switch
    {
        AlertThresholdKey.PvsThresholdPercent => new Fix(PvsArranged, 70),
        AlertThresholdKey.PvsFloorGb => new Fix(PvsArranged, 10),
        AlertThresholdKey.FileGrowthRiseMb => new Fix(
            h =>
            {
                h.Settings.FileGrowthEnabled = true;
                foreach (var database in new[] { "shop", "ledger" })
                {
                    var f = RiseOnlyFile(null, fileName: database + "_data");
                    f.DatabaseName = database;
                    h.Adapter.Files.Add(f);
                }
            }, 100_000),
        AlertThresholdKey.FileGrowthVolumePercent => new Fix(
            h =>
            {
                h.Settings.FileGrowthEnabled = true;
                foreach (var database in new[] { "shop", "ledger" })
                {
                    var f = LevelOnlyFile(null);
                    f.DatabaseName = database;
                    f.FileName = database + "_data";
                    h.Adapter.Files.Add(f);
                }
            }, 95),
        _ => throw new NotSupportedException($"{key} has no database-scope fixture."),
    };

    /* The card names every breaching database; "shop" is the one an override for it moves, "ledger" the one it leaves. */
    [Theory]
    [MemberData(nameof(DatabaseScopedKeys))]
    public async Task DatabaseOverride_MovesThatDatabaseOnly_ForEveryDatabaseScopedKey(AlertThresholdKey key)
    {
        var fix = DatabaseFixture(key);

        var baselineHarness = new Harness();
        fix.Arrange(baselineHarness);
        await baselineHarness.Build().EvaluateServerAsync(SnapshotFor(ServerA));
        var baseline = Card(baselineHarness, ServerA);
        Assert.Contains("shop", baseline, StringComparison.Ordinal);
        Assert.Contains("ledger", baseline, StringComparison.Ordinal);

        var overrides = new AlertThresholdOverrideTests.MapOverrides();
        overrides.Values[(key, ServerA, "shop")] = fix.Override;
        var h = new Harness { Overrides = overrides };
        fix.Arrange(h);
        var engine = h.Build();
        await engine.EvaluateServerAsync(SnapshotFor(ServerA));
        var moved = Card(h, ServerA);

        Assert.DoesNotContain("shop", moved, StringComparison.Ordinal);
        Assert.Contains("ledger", moved, StringComparison.Ordinal);
        Assert.Contains(overrides.Calls, c => c.Key == key && c.Database == "shop");
        Assert.Contains(overrides.Calls, c => c.Key == key && c.Database == "ledger");

        /* The same database name on another server is another database: B still breaches on it. */
        await engine.EvaluateServerAsync(SnapshotFor(ServerB));
        Assert.Contains("shop", Card(h, ServerB), StringComparison.Ordinal);
    }
}

/// <summary>
/// #5910: the two engine paths where a database bar and a server bar meet. The PVS check is gated by the server's own
/// resolved percent; the file growth re-fire guard (#3636) uses the same per-database level bar as the breach list.
/// </summary>
public sealed class AlertThresholdDatabasePathTests
{
    private const string ServerA = "101";
    private const string ServerB = "202";

    private static AlertServerSnapshot SnapshotB() => Harness.Snapshot() with { ServerKey = ServerB, ServerName = "SRV-B" };

    private static Harness PvsHarness(IAlertThresholdOverrides overrides)
    {
        var h = new Harness { Overrides = overrides };
        h.Settings.PvsEnabled = true;
        h.Adapter.PvsDatabases.Add(new PvsPressureInfo { DatabaseName = "shop", PvsSizeMb = 6144, DatabaseDataSizeMb = 10240 });
        h.Adapter.PvsDatabases.Add(new PvsPressureInfo { DatabaseName = "ledger", PvsSizeMb = 2048, DatabaseDataSizeMb = 4096 });
        return h;
    }

    /// <summary>
    /// Zero on the server's own PVS percent is the switch for the whole server: a database row cannot turn the check
    /// back on, so nothing is asked about a database on A. Database overrides tune the bar; they do not switch the check.
    /// </summary>
    [Fact]
    public async Task PvsServerPercentOfZero_TurnsTheCheckOffForTheServer_ADatabaseOverrideCannotTurnItOn()
    {
        var overrides = new AlertThresholdOverrideTests.MapOverrides();
        overrides.Values[(AlertThresholdKey.PvsThresholdPercent, ServerA, null)] = 0;
        overrides.Values[(AlertThresholdKey.PvsThresholdPercent, ServerA, "shop")] = 10;
        var h = PvsHarness(overrides);
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Empty(h.Deliverer.Outcomes);
        Assert.DoesNotContain(overrides.Calls, c => c.Database is not null);

        /* B holds the global 40%: the same data fires there. */
        await engine.EvaluateServerAsync(SnapshotB());
        Assert.Single(h.Deliverer.Outcomes, o => o.ServerKey == ServerB && o.MetricName == "Version Store (PVS)");
    }

    /// <summary>With the server's bar on, a database whose percent resolves to 0 never breaches; the others still do.</summary>
    [Fact]
    public async Task PvsDatabasePercentOfZero_NeverBreaches_WhileTheServersBarIsOn()
    {
        var overrides = new AlertThresholdOverrideTests.MapOverrides();
        overrides.Values[(AlertThresholdKey.PvsThresholdPercent, ServerA, "shop")] = 0;
        var h = PvsHarness(overrides);

        await h.Build().EvaluateServerAsync(Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.StartsWith("ledger PVS 50%", fired.CurrentValue, StringComparison.Ordinal);
        Assert.DoesNotContain(fired.Context!.Details, d => d.Heading.StartsWith("shop", StringComparison.Ordinal));
    }

    /* #3636 on the per-database path. The file is rise-only (20 GB in the window, 2% of its volume). The GLOBAL level
       bar is 1%, which would make it a standing level breach that re-fires every cooldown by design; database "tempdb"
       carries an override of 90%, so for it the breach is the rise alone and the once-per-observation guard applies. */
    [Fact]
    public async Task FileGrowth_DatabaseLevelOverride_SameHourlyObservationAcrossTwelveCooldowns_FiresOnce_ThenResolves()
    {
        var overrides = new AlertThresholdOverrideTests.MapOverrides();
        overrides.Values[(AlertThresholdKey.FileGrowthVolumePercent, ServerA, "tempdb")] = 90;
        var h = new Harness { Overrides = overrides };
        h.Settings.FileGrowthEnabled = true;
        h.Settings.CooldownMinutes = 5;
        h.Settings.FileGrowthVolumePercent = 1;
        h.Now = HourlyCollection0.AddSeconds(40);
        h.Adapter.Files.Add(RiseOnlyFile(HourlyCollection0));
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Equal("Database File Growth", Assert.Single(h.Deliverer.Outcomes).MetricName);

        for (var pass = 1; pass <= 11; pass++)
        {
            h.Now = HourlyCollection0.AddMinutes(pass * 5).AddSeconds(40);
            await engine.EvaluateServerAsync(Harness.Snapshot());
        }

        Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(12, h.Adapter.FileGrowthFetches);

        h.Adapter.Files.Clear();
        h.Adapter.Files.Add(RiseOnlyFile(HourlyCollection1, growthMb: 0));
        h.Now = HourlyCollection1.AddSeconds(40);
        await engine.EvaluateServerAsync(Harness.Snapshot());

        Assert.Single(h.Deliverer.Outcomes);
        Assert.Single(h.Resolutions, r => r.MetricName == "Database File Growth");
    }

    /// <summary>The other direction: a database override that makes the file a level breach re-fires it on the cooldown, as the global bar does.</summary>
    [Fact]
    public async Task FileGrowth_DatabaseLevelOverride_ThatMakesTheFileALevelBreach_StillRefiresEveryCooldown()
    {
        var overrides = new AlertThresholdOverrideTests.MapOverrides();
        overrides.Values[(AlertThresholdKey.FileGrowthVolumePercent, ServerA, "tempdb")] = 1;
        var h = new Harness { Overrides = overrides };
        h.Settings.FileGrowthEnabled = true;
        h.Settings.CooldownMinutes = 5;
        h.Settings.FileGrowthVolumePercent = 90;
        h.Now = HourlyCollection0.AddSeconds(40);
        h.Adapter.Files.Add(RiseOnlyFile(HourlyCollection0));
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());
        h.Now = h.Now.AddMinutes(6);
        await engine.EvaluateServerAsync(Harness.Snapshot());

        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }
}
