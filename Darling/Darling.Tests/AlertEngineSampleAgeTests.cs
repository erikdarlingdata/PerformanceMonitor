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
using PerformanceMonitor.Notifications;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5642: every alert the engine fires carries the newest sample's instant, its source label and its age, set
/// ONCE at fire time against the engine's clock (pinned at 2026-07-01 12:00 UTC) and persisted with the alert's
/// context. An alert whose evidence has no usable instant (null, a local-kind pre-V134 stamp, the MinValue
/// sentinel) carries no age. The failed-job alert is left out on purpose: its evidence is a live msdb read.
/// </summary>
public sealed class AlertEngineSampleAgeTests
{
    private static readonly DateTime Noon = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime FiveMinutesAgo = Noon.AddMinutes(-5);

    private static void AssertAge(AlertOutcome fired, string source, DateTime instant)
    {
        var sample = fired.Context?.NewestSample;
        Assert.NotNull(sample);
        Assert.Equal(source, sample!.Source);
        Assert.Equal(instant, sample.InstantUtc);
        Assert.Equal(DateTimeKind.Utc, sample.InstantUtc.Kind);
        Assert.Equal(Noon - instant, sample.Age);
    }

    [Fact]
    public void StampNewestSample_NullLocalAndMinValue_GiveNoAge_AndLeaveTheContextAlone()
    {
        Assert.Null(AlertEngine.StampNewestSample(null, "x", null, Noon));
        Assert.Null(AlertEngine.StampNewestSample(null, "x", DateTime.MinValue, Noon));
        Assert.Null(AlertEngine.StampNewestSample(null, "x", new DateTime(2026, 7, 1, 11, 0, 0, DateTimeKind.Local), Noon));

        var existing = new AlertContext();
        Assert.Same(existing, AlertEngine.StampNewestSample(existing, "x", null, Noon));
        Assert.Null(existing.NewestSample);
    }

    [Fact]
    public void StampNewestSample_ASampleAheadOfTheClock_AgesAsZero_AndTheContextIsCreatedOnlyForAnAge()
    {
        var stamped = AlertEngine.StampNewestSample(null, "x", Noon.AddMinutes(3), Noon);
        Assert.NotNull(stamped);
        Assert.Equal(TimeSpan.Zero, stamped!.NewestSample!.Age);

        /* An unspecified-kind instant (the store's naive UTC) is read as UTC. */
        var naive = AlertEngine.StampNewestSample(null, "x", new DateTime(2026, 7, 1, 11, 55, 0, DateTimeKind.Unspecified), Noon);
        Assert.Equal(DateTimeKind.Utc, naive!.NewestSample!.InstantUtc.Kind);
        Assert.Equal(TimeSpan.FromMinutes(5), naive.NewestSample.Age);
    }

    [Fact]
    public async Task Deadlock_CarriesTheAge_FromTheRowsInstant()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.DeadlockEnabled = true;
        h.Adapter.Deadlocks.Add(new DeadlockAlertRow
        {
            VictimProcessId = "process1", VictimSqlText = "UPDATE Users SET Reputation = 1",
            DeadlockGraphXml = "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list><process-list><process id=\"process1\" spid=\"55\" currentdbname=\"Db\"/></process-list></deadlock>",
            NewestSampleUtc = FiveMinutesAgo
        });
        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        AssertAge(Assert.Single(h.Deliverer.Outcomes), "deadlock", FiveMinutesAgo);
    }

    [Fact]
    public async Task Deadlock_WithNoInstant_CarriesNoAge()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.DeadlockEnabled = true;
        h.Adapter.Deadlocks.Add(new DeadlockAlertRow
        {
            VictimProcessId = "process1", VictimSqlText = "UPDATE Users SET Reputation = 1",
            DeadlockGraphXml = "<deadlock><victim-list><victimProcess id=\"process1\"/></victim-list><process-list><process id=\"process1\" spid=\"55\" currentdbname=\"Db\"/></process-list></deadlock>",
            NewestSampleUtc = new DateTime(2026, 7, 1, 11, 55, 0, DateTimeKind.Local)
        });
        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        Assert.Null(Assert.Single(h.Deliverer.Outcomes).Context?.NewestSample);
    }

    [Fact]
    public async Task Blocking_CarriesTheAge_FromTheNewestReport()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.BlockingEnabled = true;
        h.Adapter.Blocking.Add(AlertEngineTests.BlockingRow(55));
        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        var fired = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == "Blocking Detected");
        AssertAge(fired, "blocked process", new DateTime(2026, 7, 1, 11, 55, 0, DateTimeKind.Utc));
    }

    [Fact]
    public async Task BlockingWait_CarriesTheAge_FromTheSnapshot()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.BlockingEnabled = true;
        h.Settings.BlockingWaitSecondsThreshold = 600;
        h.Adapter.Blocking.Add(AlertEngineTests.BlockingRow(55));
        /* Three times the bar in one snapshot fires at once. */
        h.Adapter.BlockingWait = new CurrentBlockingWaitResult(FiveMinutesAgo, 1_900_000, 2, true);
        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        var fired = Assert.Single(h.Deliverer.Outcomes, o => o.MetricName == "Blocking Wait Time");
        AssertAge(fired, "blocking wait", FiveMinutesAgo);
    }

    [Fact]
    public async Task PoisonWait_CarriesTheAge_FromTheNewestCollection()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.PoisonWaitEnabled = true;
        h.Settings.PoisonWaitThresholdMs = 500;
        h.Adapter.PoisonWaits.Add(new PoisonWaitAccumulation("THREADPOOL", 600_000, 300_000, 10, FiveMinutesAgo));
        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        AssertAge(Assert.Single(h.Deliverer.Outcomes), "wait stats", FiveMinutesAgo);
    }

    [Fact]
    public async Task LongRunningQuery_CarriesTheAge_FromTheSnapshot()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.LongRunningQueryEnabled = true;
        h.Adapter.LongRunning.Add(new LongRunningQueryInfo
        {
            SessionId = 71, DatabaseName = "Db", QueryText = "SELECT 1", ElapsedSeconds = 2159, CpuTimeMs = 1000,
            QueryHash = "0x9AAF0129E4E9AD07", NewestSampleUtc = FiveMinutesAgo
        });
        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        AssertAge(Assert.Single(h.Deliverer.Outcomes), "query snapshot", FiveMinutesAgo);
    }

    [Fact]
    public async Task TempDb_CarriesTheAge_FromTheCollectionTime()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.TempDbSpaceEnabled = true;
        var engine = h.Build();
        var at = FiveMinutesAgo.AddMinutes(-3);
        for (var i = 0; i < AlertEngine.TempDbSpaceBreachSamples; i++)
        {
            at = at.AddMinutes(1);
            h.Adapter.TempDb = new TempDbSpaceInfo { TotalReservedMb = 800, UnallocatedMb = 200, CollectionTimeUtc = at };
            await engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        }

        AssertAge(Assert.Single(h.Deliverer.Outcomes), "tempdb", at);
    }

    [Fact]
    public async Task Volume_CarriesTheAge_FromTheNewestSnapshot()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.LowDiskEnabled = true;
        h.Adapter.Volumes.Add(new VolumeFreeSpaceInfo { MountPoint = "D:\\", TotalMb = 102400, FreeMb = 8192, NewestSampleUtc = FiveMinutesAgo });
        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        AssertAge(Assert.Single(h.Deliverer.Outcomes), "volume", FiveMinutesAgo);
    }

    [Fact]
    public async Task Pvs_CarriesTheAge_FromTheNewestSnapshot()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.PvsEnabled = true;
        h.Adapter.PvsDatabases.Add(new PvsPressureInfo
        {
            DatabaseName = "shop", PvsSizeMb = 6144, DatabaseDataSizeMb = 10240, NewestSampleUtc = FiveMinutesAgo
        });
        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        AssertAge(Assert.Single(h.Deliverer.Outcomes), "PVS", FiveMinutesAgo);
    }

    [Fact]
    public async Task FileGrowth_CarriesTheAge_FromTheObservation()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.FileGrowthEnabled = true;
        h.Adapter.Files.Add(new DatabaseFileGrowthInfo
        {
            DatabaseName = "tempdb", FileName = "tempdev", PhysicalName = @"D:\data\tempdev.mdf", FileTypeDesc = "ROWS",
            TotalSizeMb = 90_000, GrowthMb = 20_480, GrowthWindowMinutes = 60,
            VolumeMountPoint = @"D:\", VolumeTotalMb = 4_000_000, VolumeFreeMb = 3_000_000,
            ObservedAtUtc = FiveMinutesAgo
        });
        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        AssertAge(Assert.Single(h.Deliverer.Outcomes), "file growth", FiveMinutesAgo);
    }

    [Fact]
    public async Task RunningJob_CarriesTheAge_FromTheFreshnessSnapshot()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.LongRunningJobEnabled = true;
        h.Adapter.AnomalousJobs.Add(new AnomalousJobInfo
        {
            JobName = "Nightly ETL", JobId = "job-1", StartTime = new DateTime(2026, 7, 1, 11, 0, 0),
            CurrentDurationSeconds = 3600, AvgDurationSeconds = 900, PercentOfAverage = 400,
            NewestSampleUtc = FiveMinutesAgo
        });
        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        AssertAge(Assert.Single(h.Deliverer.Outcomes), "running job", FiveMinutesAgo);
    }

    [Fact]
    public async Task DatabaseState_CarriesTheAge_FromTheNewestCollection()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.DatabaseStateEnabled = true;
        h.Adapter.DatabaseStates.Add(new DatabaseStateInfo
        {
            DatabaseName = "Archive", StateDesc = "OFFLINE", ExpectedState = "ONLINE", LastAlertedState = "",
            NewestSampleUtc = FiveMinutesAgo
        });
        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        AssertAge(Assert.Single(h.Deliverer.Outcomes), "database state", FiveMinutesAgo);
    }

    [Fact]
    public async Task ForcedPlan_CarriesTheAge_FromTheObservation()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.ForcePlanFailureEnabled = true;
        h.Adapter.ForcePlanFailures.Add(new ForcePlanFailureInfo
        {
            DatabaseName = "Sales", QueryId = 11, PlanId = 22, ForcingType = "MANUAL", FailureReason = "NO_INDEX",
            FailureDelta = 4, ObservedAtUtc = FiveMinutesAgo
        });
        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        AssertAge(Assert.Single(h.Deliverer.Outcomes), "query store", FiveMinutesAgo);
    }

    /// <summary>
    /// The base-RED half (ruling 8): these tests read <c>NewestSample</c> and <c>NewestSampleUtc</c>, members this
    /// change adds, so they cannot compile at the base commit. The engine stamps every fired type at its fire
    /// site; the failed-job alert stays unstamped because its evidence is a live msdb read.
    /// </summary>
    [Fact]
    public void EveryFiringSite_StampsTheAge_ExceptTheFailedJobs()
    {
        var source = ReadRepoFile("PerformanceMonitor.Alerting", "AlertEngine.cs");
        foreach (var label in new[]
                 {
                     "\"CPU\"", "\"blocked process\"", "\"blocking wait\"", "\"deadlock\"", "\"wait stats\"", "\"query snapshot\"",
                     "\"tempdb\"", "\"volume\"", "\"PVS\"", "\"file growth\"", "\"running job\"", "\"database state\"", "\"query store\""
                 })
        {
            Assert.Contains("StampNewestSample(", source, StringComparison.Ordinal);
            Assert.True(
                source.Split(new[] { "StampNewestSample(" }, StringSplitOptions.None).Skip(1)
                    .Any(part => part.Contains(label, StringComparison.Ordinal) && part.IndexOf(label, StringComparison.Ordinal) < 160),
                $"no fire site stamps the age with the source label {label}");
        }
    }
}
