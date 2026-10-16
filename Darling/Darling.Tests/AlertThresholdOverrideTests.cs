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
/// #5910: the alert engine reads every tunable threshold through <see cref="IAlertThresholdOverrides"/>. Pins the
/// issue's semantics: null overrides give today's results, an override for one server moves that server and not
/// another, and a database-scope override applies to that database only.
/// </summary>
public sealed class AlertThresholdOverrideTests
{
    private const string ServerA = "101";
    private const string ServerB = "202";

    /// <summary>An override map keyed by (key, server, database); a null database is the server-level row.</summary>
    internal sealed class MapOverrides : IAlertThresholdOverrides
    {
        public Dictionary<(AlertThresholdKey Key, string Server, string? Database), int> Values { get; } = new();
        public List<(AlertThresholdKey Key, string Server, string? Database, int Global)> Calls { get; } = new();

        public int Resolve(AlertThresholdKey key, string serverKey, string? database, int globalValue)
        {
            Calls.Add((key, serverKey, database, globalValue));
            if (database is not null && Values.TryGetValue((key, serverKey, database), out var databaseValue))
            {
                return databaseValue;
            }

            return Values.TryGetValue((key, serverKey, null), out var serverValue) ? serverValue : globalValue;
        }
    }

    private sealed class ThrowingOverrides : IAlertThresholdOverrides
    {
        public int Resolve(AlertThresholdKey key, string serverKey, string? database, int globalValue) =>
            throw new InvalidOperationException("override store is broken");
    }

    private static AlertServerSnapshot SnapshotB() => Harness.Snapshot() with { ServerKey = ServerB, ServerName = "SRV-B" };

    /* Blocking (2 rows against a bar of 1), deadlocks (1 against 1) and two ADR databases over the 40% PVS bar:
       three checks that fire on the first sweep. */
    private static Harness FiringHarness(IAlertThresholdOverrides? overrides)
    {
        var h = new Harness { Overrides = overrides };
        h.Settings.BlockingEnabled = true;
        h.Settings.DeadlockEnabled = true;
        h.Settings.PvsEnabled = true;
        h.Adapter.Blocking.Add(BlockingRow(55));
        h.Adapter.Blocking.Add(BlockingRow(56));
        h.Adapter.Deadlocks.Add(DeadlockRow());
        h.Adapter.PvsDatabases.Add(new PvsPressureInfo { DatabaseName = "shop", PvsSizeMb = 6144, DatabaseDataSizeMb = 10240 });
        h.Adapter.PvsDatabases.Add(new PvsPressureInfo { DatabaseName = "ledger", PvsSizeMb = 2048, DatabaseDataSizeMb = 4096 });
        return h;
    }

    private static string[] Rendered(Harness h) =>
        h.Deliverer.Outcomes
            .Select(o => $"{o.ServerKey}|{o.MetricName}|{o.CurrentValue}|{o.ThresholdValue}|{o.NumericThresholdValue}")
            .ToArray();

    [Fact]
    public async Task NullOverrides_GiveTheGlobalThresholds_AndAnOverrideThatHoldsNothingChangesNoResult()
    {
        var nullHarness = FiringHarness(overrides: null);
        await nullHarness.Build().EvaluateServerAsync(Harness.Snapshot());

        var empty = new MapOverrides();
        var emptyHarness = FiringHarness(empty);
        await emptyHarness.Build().EvaluateServerAsync(Harness.Snapshot());

        /* Null is today's engine: each check fires against the global bar. */
        var blocking = Assert.Single(nullHarness.Deliverer.Outcomes, o => o.MetricName == "Blocking Detected");
        Assert.Equal("1", blocking.ThresholdValue);
        var pvs = Assert.Single(nullHarness.Deliverer.Outcomes, o => o.MetricName == "Version Store (PVS)");
        Assert.Equal(40d, pvs.NumericThresholdValue);
        Assert.Contains(nullHarness.Deliverer.Outcomes, o => o.MetricName == "Deadlocks Detected");

        /* An overrides object that holds nothing is asked, and answers the global value everywhere. */
        Assert.Equal(Rendered(nullHarness), Rendered(emptyHarness));
        Assert.Contains(empty.Calls, c => c.Key == AlertThresholdKey.BlockingCountThreshold && c.Server == ServerA && c.Global == 1);
        Assert.Contains(empty.Calls, c => c.Key == AlertThresholdKey.DeadlockCountThreshold);
        Assert.Contains(empty.Calls, c => c.Key == AlertThresholdKey.PvsThresholdPercent && c.Database == "shop");
    }

    [Fact]
    public async Task ServerOverride_MovesThatServer_AndLeavesTheOther()
    {
        var overrides = new MapOverrides();
        overrides.Values[(AlertThresholdKey.BlockingCountThreshold, ServerA, null)] = 5;
        overrides.Values[(AlertThresholdKey.LongRunningQueryThresholdMinutes, ServerA, null)] = 45;
        var h = FiringHarness(overrides);
        h.Settings.LongRunningQueryEnabled = true;
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());
        /* A's bar is 5 and it has 2 blocked sessions: no blocking alert. Its other checks are untouched. */
        Assert.DoesNotContain(h.Deliverer.Outcomes, o => o.ServerKey == ServerA && o.MetricName == "Blocking Detected");
        Assert.Contains(h.Deliverer.Outcomes, o => o.ServerKey == ServerA && o.MetricName == "Deadlocks Detected");
        /* A threshold the read filters on travels to A's read. */
        Assert.Equal(45, h.Adapter.LastLrqArgs!.Value.ThresholdMinutes);

        await engine.EvaluateServerAsync(SnapshotB());
        /* B, same data, holds the global bar of 1: it fires, and states that bar. */
        var blockingB = Assert.Single(h.Deliverer.Outcomes, o => o.ServerKey == ServerB && o.MetricName == "Blocking Detected");
        Assert.Equal("1", blockingB.ThresholdValue);
        Assert.Equal(1d, blockingB.NumericThresholdValue);
        Assert.Equal(h.Settings.LongRunningQueryThresholdMinutes, h.Adapter.LastLrqArgs!.Value.ThresholdMinutes);
    }

    [Fact]
    public async Task ServerOverride_ThatLowersTheBar_FiresThatServerOnly_AndTheCardStatesItsBar()
    {
        var overrides = new MapOverrides();
        overrides.Values[(AlertThresholdKey.PvsThresholdPercent, ServerA, null)] = 50;
        var h = FiringHarness(overrides);
        h.Settings.BlockingEnabled = false;
        h.Settings.DeadlockEnabled = false;
        h.Settings.PvsThresholdPercent = 70;
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());
        await engine.EvaluateServerAsync(SnapshotB());

        /* shop is at 60%: over A's 50, under the global 70 that B holds. */
        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(ServerA, fired.ServerKey);
        Assert.StartsWith("shop PVS 60%", fired.CurrentValue, StringComparison.Ordinal);
        Assert.StartsWith("50% of database", fired.ThresholdValue, StringComparison.Ordinal);
        Assert.Equal(50d, fired.NumericThresholdValue);
    }

    [Fact]
    public async Task DatabaseOverride_AppliesToThatDatabaseOnly()
    {
        var overrides = new MapOverrides();
        /* Raise shop's bar on A to 70: shop (60%) stops breaching there, ledger (50%) still breaches the global 40. */
        overrides.Values[(AlertThresholdKey.PvsThresholdPercent, ServerA, "shop")] = 70;
        var h = FiringHarness(overrides);
        h.Settings.BlockingEnabled = false;
        h.Settings.DeadlockEnabled = false;
        var engine = h.Build();

        await engine.EvaluateServerAsync(Harness.Snapshot());
        var firedA = Assert.Single(h.Deliverer.Outcomes);
        Assert.StartsWith("ledger PVS 50%", firedA.CurrentValue, StringComparison.Ordinal);
        Assert.Equal(40d, firedA.NumericThresholdValue);
        Assert.DoesNotContain(firedA.Context!.Details, d => d.Heading.StartsWith("shop", StringComparison.Ordinal));

        /* The same database name on B is a different database: B's shop still breaches and names the alert. */
        await engine.EvaluateServerAsync(SnapshotB());
        var firedB = Assert.Single(h.Deliverer.Outcomes, o => o.ServerKey == ServerB);
        Assert.StartsWith("shop PVS 60%", firedB.CurrentValue, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DatabaseOverride_OnFileGrowth_HoldsEachFileToItsOwnDatabase()
    {
        var overrides = new MapOverrides();
        overrides.Values[(AlertThresholdKey.FileGrowthVolumePercent, ServerA, "sales")] = 50;
        overrides.Values[(AlertThresholdKey.FileGrowthLookbackMinutes, ServerA, null)] = 120;
        var h = new Harness { Overrides = overrides };
        h.Settings.FileGrowthEnabled = true;
        /* Both files fill 70% of their volume and did not grow: under the global level bar of 80. */
        h.Settings.FileGrowthVolumePercent = 80;
        foreach (var database in new[] { "sales", "archive" })
        {
            h.Adapter.Files.Add(new DatabaseFileGrowthInfo
            {
                DatabaseName = database, FileName = database + "_data", FileTypeDesc = "ROWS",
                TotalSizeMb = 700, VolumeTotalMb = 1000, VolumeFreeMb = 300, GrowthMb = 0, GrowthWindowMinutes = 60,
            });
        }

        await h.Build().EvaluateServerAsync(Harness.Snapshot());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Database File Growth", fired.MetricName);
        Assert.Contains("sales", fired.CurrentValue, StringComparison.Ordinal);
        Assert.DoesNotContain("archive", fired.CurrentValue, StringComparison.Ordinal);
        Assert.Equal(50d, fired.NumericThresholdValue);
        Assert.Contains("120 min", fired.ThresholdValue, StringComparison.Ordinal);
        /* The lookback shapes the read, so it is a server-level value and travels to the read. */
        Assert.Equal(120, h.Adapter.FileGrowthLookbackAsked);
    }

    [Fact]
    public async Task ServerScopedKeys_AreNeverHandedADatabase()
    {
        var overrides = new MapOverrides();
        /* A database row for a server-scoped key must move nothing: blocking still fires at the global bar of 1. */
        overrides.Values[(AlertThresholdKey.BlockingCountThreshold, ServerA, "StackOverflow")] = 99;
        var h = FiringHarness(overrides);
        h.Settings.FileGrowthEnabled = true;
        h.Settings.TempDbSpaceEnabled = true;
        h.Settings.LowDiskEnabled = true;
        h.Settings.LongRunningQueryEnabled = true;

        await h.Build().EvaluateServerAsync(Harness.Snapshot());

        Assert.Contains(h.Deliverer.Outcomes, o => o.MetricName == "Blocking Detected" && o.ThresholdValue == "1");
        Assert.All(
            overrides.Calls.Where(c => c.Database is not null),
            c => Assert.Equal(AlertThresholdScope.ServerAndDatabase, AlertThresholdKeys.ScopeOf(c.Key)));
        Assert.Contains(overrides.Calls, c => c.Key == AlertThresholdKey.BlockingCountThreshold && c.Database is null);
        Assert.Contains(overrides.Calls, c => c.Key == AlertThresholdKey.PvsFloorGb && c.Database == "ledger");
    }

    /// <summary>
    /// An override of one bar of a Warning/Critical pair can cross the other. The evaluator's own clamp puts the
    /// resolved pair back in order: a Warning of 30 over the global Critical of 16 lifts Critical to 30, so a reading
    /// of 20 fires nothing on A, while B, on the global 8 / 16, pages for the same reading.
    /// </summary>
    [Fact]
    public async Task APairedBarOverride_IsHeldInOrderByTheEvaluatorsClamp()
    {
        var overrides = new MapOverrides();
        overrides.Values[(AlertThresholdKey.RunnableQueueWarningPerScheduler, ServerA, null)] = 30;
        var h = new Harness { Overrides = overrides };
        h.Settings.RunnableQueueEnabled = true;
        var engine = h.Build();
        h.Adapter.RunnableQueue = Enumerable.Range(0, 5)
            .Select(i => new RunnableQueueSample(h.Now.AddSeconds(-10).AddMinutes(-i), 20, 80, 4))
            .ToList();

        await engine.EvaluateServerAsync(Harness.Snapshot());
        Assert.Empty(h.Deliverer.Outcomes);

        /* B holds the global pair (8 / 16): the same reading fires there. */
        await engine.EvaluateServerAsync(SnapshotB());
        Assert.Single(h.Deliverer.Outcomes, o => o.ServerKey == ServerB);
    }

    [Fact]
    public async Task AThrowingOverride_FallsBackToTheGlobalValue()
    {
        var h = FiringHarness(new ThrowingOverrides());

        await h.Build().EvaluateServerAsync(Harness.Snapshot());

        var blocking = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == "Blocking Detected");
        Assert.Equal("1", blocking.ThresholdValue);
        Assert.Contains(h.Deliverer.Outcomes, o => o.MetricName == "Deadlocks Detected");
        Assert.Contains(h.Deliverer.Outcomes, o => o.MetricName == "Version Store (PVS)");
    }
}
