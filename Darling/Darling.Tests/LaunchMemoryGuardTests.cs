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
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5479: the memory launch guard releases itself. A tripped guard launches no new collection bodies, the in-flight
/// ones finish, and a drained process allocates nothing, so no garbage collection ran and the figure stayed over the
/// line for 25 hours. These tests drive <see cref="LaunchMemoryGuard"/> through an injected sampler, collection and
/// clock, so they need no real memory and no particular host OS.
/// </summary>
public sealed class LaunchMemoryGuardTests
{
    private const long Mb = 1024L * 1024;
    private const long Limit = 1536 * Mb;

    /* The line is 0.80 of the limit: 1228.8MB. */
    private static LaunchMemoryReading Over(string metric = "private bytes", string source = "the GC memory budget")
        => new(1426 * Mb, Limit, metric, source);

    private static LaunchMemoryReading Under() => new(815 * Mb, Limit, "private bytes", "the GC memory budget");

    /// <summary>A guard over scripted memory: <see cref="Reading"/> is what the sampler returns, <see cref="Collections"/>
    /// counts the collections run, <see cref="Now"/> is the clock, and <see cref="OnCollect"/> lets a test say what a
    /// collection does to the memory.</summary>
    private sealed class Rig
    {
        public LaunchMemoryReading Reading = Over();
        public int Collections;
        public TimeSpan Now = TimeSpan.FromHours(3);
        public Action? OnCollect;
        public int Samples;
        public Exception? SamplerFault;
        public CapturingTestLogger Logger { get; } = new();
        public LaunchMemoryGuard Guard { get; }

        public Rig()
        {
            Guard = new LaunchMemoryGuard(
                () =>
                {
                    Samples++;
                    if (SamplerFault is not null)
                    {
                        throw SamplerFault;
                    }

                    return Reading;
                },
                () =>
                {
                    Collections++;
                    OnCollect?.Invoke();
                },
                () => Now,
                Logger);
        }

        public bool Pass(int inFlight = 0) => Guard.MayLaunch(inFlight);

        public bool PassAfter(TimeSpan advance, int inFlight = 0)
        {
            Now += advance;
            return Guard.MayLaunch(inFlight);
        }

        public int Count(LogLevel level, string containing)
            => Logger.Lines.Count(l => l.StartsWith(level + ":", StringComparison.Ordinal) && l.Contains(containing, StringComparison.Ordinal));
    }

    /// <summary>
    /// The latch (#5479). The sampler reads over the line at the trip and keeps reading over until the collection has
    /// run, then reads under. The in-flight bodies finish with no allocation, so nothing else ever frees the memory.
    /// RED on the base behavior (the same test with the collection planted out): the guard holds forever.
    /// </summary>
    [Fact]
    public void AGuardWhoseMemoryOnlyFallsAfterACollection_ReleasesAfterIt()
    {
        var rig = new Rig();
        rig.OnCollect = () => rig.Reading = Under();

        Assert.True(rig.Pass(inFlight: 2));

        Assert.Equal(1, rig.Collections);
        Assert.False(rig.Guard.IsHolding);

        /* The next 25 hours of 15-second passes: the guard stays released and runs nothing more. */
        for (var i = 0; i < 6000; i++)
        {
            Assert.True(rig.PassAfter(TimeSpan.FromSeconds(15)));
        }

        Assert.Equal(1, rig.Collections);
        Assert.Equal(1, rig.Logger.CountAtLevel(LogLevel.Critical));
    }

    /// <summary>The same latch with the release one pass LATER: the collection frees the memory, but the first
    /// reading after it still reads over (the runtime returns memory to the OS a moment after the collection); the
    /// next pass reads under and releases. No new collection is needed for that, and none is run.</summary>
    [Fact]
    public void AGuardWhoseMemoryFallsJustAfterTheCollection_ReleasesOnTheNextPass()
    {
        var rig = new Rig();

        Assert.False(rig.Pass());
        Assert.Equal(1, rig.Collections);
        Assert.True(rig.Guard.IsHolding);

        rig.Reading = Under();
        Assert.True(rig.PassAfter(TimeSpan.FromSeconds(15)));
        Assert.False(rig.Guard.IsHolding);
        Assert.Equal(1, rig.Collections);
        Assert.Equal(1, rig.Count(LogLevel.Information, "resuming collection-body launches"));
    }

    /// <summary>
    /// Cadence: memory the collection cannot free keeps the hold, and the collection repeats no sooner than a
    /// minute after the last one. The hold Warning comes every five minutes. The trip, each collection and the release
    /// log exactly once per event. The guard never releases on its own while the figure stays over.
    /// </summary>
    [Fact]
    public void AGuardStillOverAfterTheCollection_RunsItAgainEachMinuteAndWarnsEveryFiveMinutes()
    {
        var rig = new Rig();

        Assert.False(rig.Pass(inFlight: 3));
        Assert.Equal(1, rig.Collections);

        rig.Now += TimeSpan.FromSeconds(45);
        Assert.False(rig.Guard.MayLaunch(3));
        Assert.Equal(1, rig.Collections);

        rig.Now += TimeSpan.FromSeconds(15);
        Assert.False(rig.Guard.MayLaunch(3));
        Assert.Equal(2, rig.Collections);

        /* Fifteen-second passes up to eleven minutes after the trip: a collection at 0, 60, ... 660 seconds. */
        for (var elapsed = 75; elapsed <= 660; elapsed += 15)
        {
            rig.Now = TimeSpan.FromHours(3) + TimeSpan.FromSeconds(elapsed);
            Assert.False(rig.Guard.MayLaunch(3));
        }

        Assert.Equal(12, rig.Collections);
        Assert.Equal(12, rig.Count(LogLevel.Information, "ran a full garbage collection"));
        Assert.Equal(1, rig.Logger.CountAtLevel(LogLevel.Critical));

        /* Warnings at 5 and 10 minutes; each names the hold, the figure and the bodies still in flight. */
        Assert.Equal(2, rig.Logger.CountAtLevel(LogLevel.Warning));
        var warning = rig.Logger.Lines.First(l => l.StartsWith("Warning:", StringComparison.Ordinal));
        Assert.Contains("5m 00s", warning);
        Assert.Contains("1426MB", warning);
        Assert.Contains("3 collection bodies still in flight", warning);

        Assert.Equal(0, rig.Count(LogLevel.Information, "resuming collection-body launches"));

        /* The memory finally drops (a body ended and freed it): one release, with how long it held. */
        rig.Reading = Under();
        rig.Now += TimeSpan.FromSeconds(15);
        Assert.True(rig.Guard.MayLaunch(0));
        Assert.Equal(1, rig.Count(LogLevel.Information, "resuming collection-body launches"));
        var release = rig.Logger.Lines.Last(l => l.Contains("resuming collection-body launches", StringComparison.Ordinal));
        Assert.Contains("11m 15s", release);
        Assert.Contains("815MB", release);
    }

    /// <summary>A pass under the line while not holding does nothing: no log, no collection.</summary>
    [Fact]
    public void AGuardUnderTheLine_LogsNothingAndRunsNothing()
    {
        var rig = new Rig { Reading = Under() };

        for (var i = 0; i < 50; i++)
        {
            Assert.True(rig.PassAfter(TimeSpan.FromSeconds(15)));
        }

        Assert.Empty(rig.Logger.Lines);
        Assert.Equal(0, rig.Collections);
    }

    /// <summary>The trip line names the metric, the figure, the limit and where the limit came from, and the
    /// collection line gives the figure before and after.</summary>
    [Fact]
    public void TheTripAndTheCollectionLogTheMetricTheFigureTheLimitAndItsSource()
    {
        var rig = new Rig { Reading = Over("resident memory", "cgroup memory limit") };
        rig.OnCollect = () => rig.Reading = new LaunchMemoryReading(900 * Mb, Limit, "resident memory", "cgroup memory limit");

        rig.Pass();

        var critical = rig.Logger.Lines.Single(l => l.StartsWith("Critical:", StringComparison.Ordinal));
        Assert.Contains("resident memory 1426MB", critical);
        Assert.Contains("80", critical);
        Assert.Contains("1536MB limit (cgroup memory limit)", critical);

        var collection = rig.Logger.Lines.Single(l => l.Contains("ran a full garbage collection", StringComparison.Ordinal));
        Assert.StartsWith("Information:", collection, StringComparison.Ordinal);
        Assert.Contains("resident memory 1426MB before, 900MB after", collection);
    }

    /// <summary>A release and then a fresh climb is a new episode: a second Critical and a second collection.</summary>
    [Fact]
    public void AReleasedGuardThatTripsAgain_StartsANewEpisode()
    {
        var rig = new Rig();
        rig.OnCollect = () => rig.Reading = Under();

        Assert.True(rig.Pass());
        rig.Reading = Over();
        Assert.True(rig.PassAfter(TimeSpan.FromSeconds(15)));

        Assert.Equal(2, rig.Collections);
        Assert.Equal(2, rig.Logger.CountAtLevel(LogLevel.Critical));
        Assert.Equal(2, rig.Count(LogLevel.Information, "resuming collection-body launches"));
    }

    /// <summary>A guard that cannot read memory fails open, with one Warning, and keeps asking each pass.</summary>
    [Fact]
    public void AGuardThatCannotReadMemory_LetsCollectionLaunchAndWarnsOnce()
    {
        var rig = new Rig { SamplerFault = new InvalidOperationException("no /proc") };

        for (var i = 0; i < 5; i++)
        {
            Assert.True(rig.PassAfter(TimeSpan.FromSeconds(15)));
        }

        Assert.Equal(5, rig.Samples);
        Assert.Equal(1, rig.Logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, rig.Collections);
    }

    /// <summary>A collection that throws does not end the hold or the loop: the hold continues and the failure is
    /// logged.</summary>
    [Fact]
    public void ACollectionThatFails_KeepsTheHoldAndLogsIt()
    {
        var rig = new Rig();
        rig.OnCollect = () => throw new InvalidOperationException("collection refused");

        Assert.False(rig.Pass());

        Assert.True(rig.Guard.IsHolding);
        Assert.Equal(1, rig.Count(LogLevel.Warning, "collection refused"));
    }

    [Theory]
    [InlineData(0, "0s")]
    [InlineData(42, "42s")]
    [InlineData(300, "5m 00s")]
    [InlineData(425, "7m 05s")]
    [InlineData(3600, "1h 00m")]
    [InlineData(90240, "25h 04m")]
    public void TheHoldLengthReadsAtBothEnds(int seconds, string expected)
        => Assert.Equal(expected, LaunchMemoryGuard.FormatHeld(TimeSpan.FromSeconds(seconds)));

    /// <summary>The 0.80 line itself is unchanged and the Windows metric is private bytes against the GC's budget,
    /// the way it was: the guard passes the sampler's two figures straight to the threshold rule.</summary>
    [Fact]
    public void TheThresholdRuleIsTheSameForAnySampler()
    {
        var rig = new Rig { Reading = new LaunchMemoryReading((long)(0.79 * Limit), Limit, "private bytes", "the GC memory budget") };
        Assert.True(rig.Pass());

        rig.Reading = new LaunchMemoryReading((long)(0.81 * Limit), Limit, "private bytes", "the GC memory budget");
        Assert.False(rig.PassAfter(TimeSpan.FromSeconds(15)));

        /* An unknown limit never blocks. */
        rig.Reading = new LaunchMemoryReading(long.MaxValue, 0, "private bytes", "the GC memory budget");
        Assert.True(rig.PassAfter(TimeSpan.FromMinutes(2)));
    }

    /// <summary>
    /// The real collection, on this process: garbage that nothing references still counts in the process's private
    /// bytes until a collection decommits it, and the guard's collection gives it back. This is the premise the whole
    /// release rests on (a drained process allocates nothing, so the figure never falls by itself), measured rather
    /// than assumed. The garbage is made in its own method so no local keeps it alive.
    /// </summary>
    [Fact]
    public void TheRealCollection_GivesDeadGarbageBackToTheProcess()
    {
        MakeGarbage();
        var before = PrivateMegabytes();
        Assert.True(before > 150, $"the test garbage should show in private bytes, read {before}MB");

        LaunchMemoryGuard.CollectAndDecommit();

        var after = PrivateMegabytes();
        Assert.True(before - after >= 100, $"the collection should decommit the garbage: {before}MB before, {after}MB after");
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static void MakeGarbage()
    {
        var blocks = new List<byte[]>();
        for (var i = 0; i < 20; i++)
        {
            var block = new byte[10_000_000];
            Array.Fill(block, (byte)1);
            blocks.Add(block);
        }

        GC.KeepAlive(blocks);
    }

    private static long PrivateMegabytes()
    {
        using var process = System.Diagnostics.Process.GetCurrentProcess();
        return process.PrivateMemorySize64 / (1024 * 1024);
    }

    /// <summary>
    /// Wiring pin: the collection loop asks the guard once per pass, the old latch field and the direct
    /// <c>PrivateMemorySize64</c> read are gone from the worker, and the worker hands a test the guard to replace.
    /// </summary>
    [Fact]
    public void TheCollectionLoopAsksTheGuardOncePerPass()
    {
        var worker = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

        Assert.Equal(1, CountOf(worker, "_launchMemoryGuard.MayLaunch("));
        Assert.Equal(0, CountOf(worker, "_memoryGuardTrippedThisEpisode"));
        Assert.Equal(0, CountOf(worker, "ShouldLaunchSweeps(workingSetBytes"));
        Assert.Equal(0, CountOf(worker, "= currentProcess.PrivateMemorySize64"));
        Assert.Contains("internal LaunchMemoryGuard LaunchGuard", worker, StringComparison.Ordinal);
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        var at = 0;
        while ((at = text.IndexOf(needle, at, StringComparison.Ordinal)) >= 0)
        {
            count++;
            at += needle.Length;
        }

        return count;
    }

    /* ========================== the Linux metric: resident memory against the real limit ========================== */

    private const string StatusFromTheIssue =
        "Name:\tdotnet\nVmPeak:\t 9000000 kB\nVmSize:\t 8000000 kB\nVmData:\t 1460224 kB\nVmRSS:\t  829440 kB\nRssAnon:\t  700000 kB\nThreads:\t42\n";

    private const string MeminfoSixteenGb = "MemTotal:       16777216 kB\nMemFree:         1000000 kB\n";

    private sealed class FakeFiles
    {
        public Dictionary<string, string?> Files { get; } = new();
        public Dictionary<string, int> Reads { get; } = new();

        public string? Read(string path)
        {
            Reads[path] = Reads.GetValueOrDefault(path) + 1;
            return Files.GetValueOrDefault(path);
        }
    }

    private static (LinuxLaunchMemorySampler Sampler, FakeFiles Files, CapturingTestLogger Logger) LinuxRig(
        string? status, string? meminfo, string? v2, string? v1, long workingSet = 700 * Mb, long gcBudget = Limit)
    {
        var files = new FakeFiles();
        files.Files[LinuxLaunchMemorySampler.ProcStatusPath] = status;
        files.Files[DarlingStoreHostProfile.ProcMeminfoPath] = meminfo;
        files.Files[DarlingStoreHostProfile.CgroupV2MemoryMaxPath] = v2;
        files.Files[DarlingStoreHostProfile.CgroupV1MemoryLimitPath] = v1;
        var logger = new CapturingTestLogger();
        return (new LinuxLaunchMemorySampler(files.Read, () => workingSet, () => gcBudget, logger), files, logger);
    }

    /// <summary>
    /// The issue's numbers: <c>VmData</c> 1426MB, <c>VmRSS</c> 810MB, cgroup v2 <c>memory.max</c> 2147483648 (2Gi, the
    /// runtime's GC budget 1536MB). The kernel kills at 2048MB of resident memory; the guard's line is 1638MB. The
    /// figure is 810MB, so the guard launches. RED on the base metric (<c>VmData</c> against the 1536MB GC budget,
    /// line 1229MB): 1426MB is over, so it holds.
    /// </summary>
    [Fact]
    public void Linux_TheIssuesNumbers_Launch()
    {
        var (sampler, _, logger) = LinuxRig(StatusFromTheIssue, MeminfoSixteenGb, "2147483648\n", null);

        var reading = sampler.Sample();

        Assert.Equal("resident memory", reading.Metric);
        Assert.Equal("cgroup memory limit", reading.LimitSource);
        Assert.Equal(829440L * 1024, reading.Bytes);
        Assert.Equal(2147483648L, reading.LimitBytes);
        Assert.True(DarlingWorker.ShouldLaunchSweeps(reading.Bytes, reading.LimitBytes));
        Assert.Empty(logger.Lines);

        /* The base metric, for the record: it held at the same moment. */
        var vmData = LinuxLaunchMemorySampler.ParseProcStatusBytes(StatusFromTheIssue, "VmData");
        Assert.Equal(1460224L * 1024, vmData);
        Assert.False(DarlingWorker.ShouldLaunchSweeps(vmData!.Value, Limit));
    }

    /// <summary>Resident memory over 80% of the cgroup limit still holds: the guard backs away from the kill, not from
    /// a figure that never mattered.</summary>
    [Fact]
    public void Linux_ResidentMemoryOverTheCgroupLine_Holds()
    {
        var status = "VmData:\t 1460224 kB\nVmRSS:\t 1800000 kB\n";
        var (sampler, _, _) = LinuxRig(status, MeminfoSixteenGb, "2147483648", null);

        var reading = sampler.Sample();

        Assert.False(DarlingWorker.ShouldLaunchSweeps(reading.Bytes, reading.LimitBytes));
    }

    [Fact]
    public void Linux_CgroupV1Limit_IsUsedWhenV2IsAbsent()
    {
        var (sampler, _, _) = LinuxRig(StatusFromTheIssue, MeminfoSixteenGb, null, "1073741824\n");

        var reading = sampler.Sample();

        Assert.Equal(1073741824L, reading.LimitBytes);
        Assert.Equal("cgroup memory limit", reading.LimitSource);
    }

    /// <summary>cgroup v2 <c>max</c>, and v1's near-<c>long.MaxValue</c> sentinel, both mean no limit: the limit is
    /// total RAM.</summary>
    [Theory]
    [InlineData("max\n", null)]
    [InlineData(null, "9223372036854771712\n")]
    [InlineData("max", "9223372036854771712")]
    public void Linux_NoCgroupLimit_FallsBackToTotalRam(string? v2, string? v1)
    {
        var (sampler, _, logger) = LinuxRig(StatusFromTheIssue, MeminfoSixteenGb, v2, v1);

        var reading = sampler.Sample();

        Assert.Equal(16777216L * 1024, reading.LimitBytes);
        Assert.Equal("total RAM", reading.LimitSource);
        Assert.Empty(logger.Lines);
    }

    /// <summary>A cgroup limit above total RAM is not a limit: RAM is smaller.</summary>
    [Fact]
    public void Linux_ACgroupLimitAboveRam_IsRam()
    {
        var (sampler, _, _) = LinuxRig(StatusFromTheIssue, MeminfoSixteenGb, "68719476736", null);

        var reading = sampler.Sample();

        Assert.Equal(16777216L * 1024, reading.LimitBytes);
        Assert.Equal("total RAM", reading.LimitSource);
    }

    /// <summary>A readable cgroup limit with no readable <c>/proc/meminfo</c> is still a real limit.</summary>
    [Fact]
    public void Linux_CgroupLimitWithoutMeminfo_StillTheLimit()
    {
        var (sampler, _, _) = LinuxRig(StatusFromTheIssue, null, "2147483648", null);

        var reading = sampler.Sample();

        Assert.Equal(2147483648L, reading.LimitBytes);
        Assert.Equal("cgroup memory limit", reading.LimitSource);
    }

    /// <summary>No limit readable at all: the sampler falls back to the runtime's working set against the GC's
    /// budget, and says so once, however many samples follow.</summary>
    [Fact]
    public void Linux_UnreadableLimitFiles_FallBackAndLogOnce()
    {
        var (sampler, _, logger) = LinuxRig(StatusFromTheIssue, null, null, null, workingSet: 700 * Mb, gcBudget: 1536 * Mb);

        for (var i = 0; i < 4; i++)
        {
            var reading = sampler.Sample();
            Assert.Equal(700 * Mb, reading.Bytes);
            Assert.Equal(1536 * Mb, reading.LimitBytes);
            Assert.Equal("the GC memory budget", reading.LimitSource);
            Assert.Equal("resident memory", reading.Metric);
        }

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Contains("falls back", logger.Joined);
    }

    /// <summary>A limit that reads fine but a status file with no <c>VmRSS</c>: the same fallback, logged once.</summary>
    [Fact]
    public void Linux_UnreadableStatus_FallsBackAndLogsOnce()
    {
        var (sampler, _, logger) = LinuxRig("Name:\tdotnet\nVmData:\t 1460224 kB\n", MeminfoSixteenGb, "2147483648", null);

        for (var i = 0; i < 3; i++)
        {
            var reading = sampler.Sample();
            Assert.Equal(700 * Mb, reading.Bytes);
            Assert.Equal("the GC memory budget", reading.LimitSource);
        }

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Contains("VmRSS", logger.Joined);
    }

    /// <summary>The limit does not change while the process runs: its three files are read once, the figure's file
    /// on every sample.</summary>
    [Fact]
    public void Linux_TheLimitIsReadOnceAndTheFigureEverySample()
    {
        var (sampler, files, _) = LinuxRig(StatusFromTheIssue, MeminfoSixteenGb, "2147483648", null);

        for (var i = 0; i < 5; i++)
        {
            sampler.Sample();
        }

        Assert.Equal(1, files.Reads[DarlingStoreHostProfile.ProcMeminfoPath]);
        Assert.Equal(1, files.Reads[DarlingStoreHostProfile.CgroupV2MemoryMaxPath]);
        Assert.Equal(1, files.Reads[DarlingStoreHostProfile.CgroupV1MemoryLimitPath]);
        Assert.Equal(5, files.Reads[LinuxLaunchMemorySampler.ProcStatusPath]);
    }

    [Theory]
    [InlineData("VmRSS:\t  829440 kB\n", "VmRSS", 829440L * 1024)]
    [InlineData("VmData:\t 1 kB\r\nVmRSS:   2 kB\r\n", "VmRSS", 2048L)]
    [InlineData("VmRSS:\t  0 kB\n", "VmRSS", 0L)]
    [InlineData("VmRSS:\t  abc kB\n", "VmRSS", null)]
    [InlineData("RssAnon:\t 5 kB\n", "VmRSS", null)]
    [InlineData("", "VmRSS", null)]
    [InlineData(null, "VmRSS", null)]
    public void ParseProcStatusBytes_ReadsOneKeyAsBytes(string? text, string key, long? expected)
        => Assert.Equal(expected, LinuxLaunchMemorySampler.ParseProcStatusBytes(text, key));

    /// <summary>The same Linux rig, end to end through the guard: the issue's numbers launch with no log at all.</summary>
    [Fact]
    public void Linux_TheIssuesNumbers_ThroughTheGuard_NeverTrip()
    {
        var (sampler, _, logger) = LinuxRig(StatusFromTheIssue, MeminfoSixteenGb, "2147483648", null);
        var guard = new LaunchMemoryGuard(sampler.Sample, () => throw new InvalidOperationException("no collection expected"), () => TimeSpan.Zero, logger);

        Assert.True(guard.MayLaunch(0));
        Assert.Empty(logger.Lines);
    }
}
