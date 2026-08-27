/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Identity pins for the Phase-5 slice-A extraction: the six pure alert-context builders (and their
/// pure helpers) moved verbatim from this app's <c>MainWindow.AlertEngine.cs</c> into the shared
/// <see cref="AlertContextBuilders"/>. Every expectation here is derived from the pre-extraction
/// code, so a behavioral drift in the shared copy fails a pin. The ONE documented reconciliation:
/// <see cref="AlertContextBuilders.BuildLongRunningQueryContext"/> adopts the Dashboard's version,
/// so Lite's long-running-query alerts now render the ("Program", ...) field — pinned as NEW
/// behavior in <see cref="BuildLongRunningQueryContext_RendersProgramField_TheDocumentedChange"/>.
/// </summary>
public class AlertContextBuildersTests
{
    private const string Server = "SQL2022";

    /* ---------------- poison waits ---------------- */

    [Fact]
    public void BuildPoisonWaitContext_EmptyList_ReturnsNull()
    {
        Assert.Null(AlertContextBuilders.BuildPoisonWaitContext(new List<PoisonWaitDelta>()));
    }

    [Fact]
    public void BuildPoisonWaitContext_RendersOneDetailPerWait_NoIncidents()
    {
        var context = AlertContextBuilders.BuildPoisonWaitContext(new List<PoisonWaitDelta>
        {
            new() { WaitType = "RESOURCE_SEMAPHORE", AvgMsPerWait = 2000.5, DeltaMs = 100000, DeltaTasks = 50 },
            new() { WaitType = "THREADPOOL", AvgMsPerWait = 750.0, DeltaMs = 1500, DeltaTasks = 2 }
        });

        Assert.NotNull(context);
        Assert.Equal(2, context!.Details.Count);
        Assert.Equal("RESOURCE_SEMAPHORE", context.Details[0].Heading);
        Assert.Equal(
            new List<(string, string)>
            {
                ("Avg ms/wait", $"{2000.5:F1}"),
                ("Delta wait ms", $"{100000L:N0}"),
                ("Delta tasks", $"{50L:N0}")
            },
            context.Details[0].Fields);
        Assert.Equal("THREADPOOL", context.Details[1].Heading);
        /* The poison-wait builder never attached #1140 incidents pre-extraction. */
        Assert.Null(context.Incidents);
    }

    /* ---------------- long-running queries ---------------- */

    private static LongRunningQueryInfo Lrq(
        int sessionId, long elapsedSeconds, string db = "", string program = "", string query = "",
        string? waitType = null, int? blockedBy = null, string? hash = null) => new()
    {
        SessionId = sessionId,
        ElapsedSeconds = elapsedSeconds,
        DatabaseName = db,
        ProgramName = program,
        QueryText = query,
        CpuTimeMs = 1234,
        Reads = 10,
        Writes = 2,
        WaitType = waitType,
        BlockingSessionId = blockedBy,
        QueryHash = hash
    };

    [Fact]
    public void BuildLongRunningQueryContext_EmptyList_ReturnsNull()
    {
        Assert.Null(AlertContextBuilders.BuildLongRunningQueryContext(Server, new List<LongRunningQueryInfo>()));
    }

    [Fact]
    public void BuildLongRunningQueryContext_CapsAtThreeQueries_AppendsIncidents()
    {
        var queries = new List<LongRunningQueryInfo>
        {
            Lrq(71, 314, db: "StackOverflow", query: "SELECT 1", hash: "0x9AAF0129E4E9AD07"),
            Lrq(72, 200, db: "StackOverflow", query: "SELECT 2", hash: "0x1111111111111111"),
            Lrq(73, 150, db: "StackOverflow", query: "SELECT 3", hash: "0x2222222222222222"),
            Lrq(74, 100, db: "StackOverflow", query: "SELECT 4", hash: "0x3333333333333333")
        };

        var context = AlertContextBuilders.BuildLongRunningQueryContext(Server, queries);

        Assert.NotNull(context);
        /* 3 session items (capped) + 3 appended incident items (one per hashed query). */
        Assert.Equal(6, context!.Details.Count);
        Assert.Equal("Session #71 — 5m 14s", context.Details[0].Heading);
        Assert.Equal("Incident 1 of 3", context.Details[3].Heading);
        Assert.NotNull(context.Incidents);
        Assert.Equal(3, context.Incidents!.Count);
        /* Dedup key = query_hash fingerprint, scoped by server + incident type (#1140). */
        var expected = AlertFingerprint.ForKey(Server, AlertFingerprint.Query, "0x9AAF0129E4E9AD07", new[] { "StackOverflow" });
        Assert.Equal(expected!.DedupKey, context.Incidents[0].DedupKey);
    }

    [Fact]
    public void BuildLongRunningQueryContext_RendersProgramField_TheDocumentedChange()
    {
        /* Phase-5 slice A's ONE behavior reconciliation: Lite's pre-extraction builder had no
           ("Program", ...) item; the shared builder adopts the Dashboard's version, so Lite's
           long-running-query alerts GAIN this field (Lite's LRQ read now populates ProgramName). */
        var context = AlertContextBuilders.BuildLongRunningQueryContext(Server, new List<LongRunningQueryInfo>
        {
            Lrq(71, 314, db: "StackOverflow", program: "HammerDB", query: "SELECT 1", waitType: "CXPACKET", blockedBy: 55)
        });

        Assert.NotNull(context);
        Assert.Equal(
            new List<(string, string)>
            {
                ("Database", "StackOverflow"),
                ("Program", "HammerDB"),
                ("Query", "SELECT 1"),
                ("CPU Time", $"{1234L:N0} ms"),
                ("Reads", $"{10L:N0}"),
                ("Writes", $"{2L:N0}"),
                ("Wait Type", "CXPACKET"),
                ("Blocked By", "Session #55")
            },
            context!.Details[0].Fields);
    }

    [Fact]
    public void BuildLongRunningQueryContext_EmptyProgramName_OmitsProgramField()
    {
        /* Rows without a program (or pre-upgrade Lite archive rows where the column is NULL)
           render exactly as before the extraction — the field is conditional. */
        var context = AlertContextBuilders.BuildLongRunningQueryContext(Server, new List<LongRunningQueryInfo>
        {
            Lrq(71, 61, db: "StackOverflow", query: "SELECT 1")
        });

        Assert.NotNull(context);
        Assert.Equal("Session #71 — 1m 1s", context!.Details[0].Heading);
        Assert.DoesNotContain(context.Details[0].Fields, f => f.Label == "Program");
    }

    [Fact]
    public void BuildLongRunningQueryContext_NullQueryHash_EmitsNoIncident()
    {
        var context = AlertContextBuilders.BuildLongRunningQueryContext(Server, new List<LongRunningQueryInfo>
        {
            Lrq(71, 90, db: "StackOverflow", query: "SELECT 1", hash: null)
        });

        Assert.NotNull(context);
        Assert.Single(context!.Details);
        Assert.Null(context.Incidents);
    }

    /* ---------------- low disk ---------------- */

    private static VolumeFreeSpaceInfo Volume(string mount, double totalMb, double freeMb) =>
        new() { MountPoint = mount, TotalMb = totalMb, FreeMb = freeMb };

    [Fact]
    public void GetBreachedVolumes_FiltersOnEitherDimension_WorstFirst()
    {
        var volumes = new List<VolumeFreeSpaceInfo>
        {
            Volume("C:\\", 102400, 8192),     /* 8% free, 8 GB    -> breaches percent */
            Volume("D:\\", 1048576, 524288),  /* 50% free, 512 GB -> healthy          */
            Volume("E:\\", 10240, 3072)       /* 30% free, 3 GB   -> breaches GB      */
        };

        var breached = AlertContextBuilders.GetBreachedVolumes(volumes, thresholdPercent: 10, thresholdGb: 5);

        Assert.Equal(2, breached.Count);
        /* Ordered by free percent ascending — the tightest volume first. */
        Assert.Equal("C:\\", breached[0].MountPoint);
        Assert.Equal("E:\\", breached[1].MountPoint);
    }

    [Fact]
    public void GetBreachedVolumes_ZeroThresholdDisablesThatDimension()
    {
        var lowPercent = new List<VolumeFreeSpaceInfo> { Volume("C:\\", 102400, 8192) };  /* 8%, 8 GB */
        var lowGb = new List<VolumeFreeSpaceInfo> { Volume("E:\\", 10240, 3072) };        /* 30%, 3 GB */

        Assert.Empty(AlertContextBuilders.GetBreachedVolumes(lowPercent, thresholdPercent: 0, thresholdGb: 5));
        Assert.Empty(AlertContextBuilders.GetBreachedVolumes(lowGb, thresholdPercent: 10, thresholdGb: 0));
        Assert.Empty(AlertContextBuilders.GetBreachedVolumes(lowPercent, thresholdPercent: 0, thresholdGb: 0));
    }

    [Theory]
    [InlineData(10, 5, "10% / 5 GB")]
    [InlineData(10, 0, "10%")]
    [InlineData(0, 5, "5 GB")]
    [InlineData(0, 0, "—")]
    public void FormatLowDiskThreshold_RendersConfiguredDimensions(double pct, double gb, string expected)
    {
        Assert.Equal(expected, AlertContextBuilders.FormatLowDiskThreshold(pct, gb));
    }

    [Fact]
    public void BuildVolumeFreeSpaceContext_RendersVolume_WithPerVolumeIncident()
    {
        var context = AlertContextBuilders.BuildVolumeFreeSpaceContext(Server, new List<VolumeFreeSpaceInfo>
        {
            Volume("C:\\", 102400, 5120) /* 5% free, 5 GB free, 100 GB total */
        });

        Assert.NotNull(context);
        /* 1 volume item + 1 appended incident item. */
        Assert.Equal(2, context!.Details.Count);
        Assert.Equal("C:\\ — 5% Free", context.Details[0].Heading);
        Assert.Equal(
            new List<(string, string)>
            {
                ("Free Space", $"{5.0:F1} GB"),
                ("Total Size", $"{100.0:F1} GB"),
                ("Used", $"{95.0:F1} GB")
            },
            context.Details[0].Fields);

        Assert.NotNull(context.Incidents);
        var expected = AlertFingerprint.ForKey(Server, AlertFingerprint.Disk, "C:\\", new[] { "C:\\" });
        Assert.Equal(expected!.DedupKey, Assert.Single(context.Incidents!).DedupKey);
    }

    [Fact]
    public void BuildVolumeFreeSpaceContext_CapsAtFiveVolumes()
    {
        var volumes = new List<VolumeFreeSpaceInfo>();
        for (int i = 0; i < 6; i++)
            volumes.Add(Volume($"{(char)('C' + i)}:\\", 102400, 1024 * (i + 1)));

        var context = AlertContextBuilders.BuildVolumeFreeSpaceContext(Server, volumes);

        Assert.NotNull(context);
        /* 5 shown volumes + 5 incident items. */
        Assert.Equal(10, context!.Details.Count);
        Assert.Equal(5, context.Incidents!.Count);
    }

    /* ---------------- tempdb ---------------- */

    [Fact]
    public void BuildTempDbSpaceContext_RendersAllFields_WithTopConsumer()
    {
        var context = AlertContextBuilders.BuildTempDbSpaceContext(new TempDbSpaceInfo
        {
            TotalReservedMb = 800,
            UnallocatedMb = 200,
            UserObjectReservedMb = 500,
            InternalObjectReservedMb = 250,
            VersionStoreReservedMb = 50,
            TopConsumerSessionId = 55,
            TopConsumerMb = 123.4
        });

        Assert.NotNull(context);
        var item = Assert.Single(context!.Details);
        Assert.Equal("tempdb — 80% Used", item.Heading);
        Assert.Equal(
            new List<(string, string)>
            {
                ("Total Reserved", $"{800d:F0} MB"),
                ("Unallocated", $"{200d:F0} MB"),
                /* #2515: no MaxSizeMb on this fixture, so the ceiling was never measured and the
                   percentage above is still against the allocation — which the detail now says out loud
                   rather than leaving the reader to assume a denominator. */
                ("Max Size", "Unknown"),
                ("User Objects", $"{500d:F0} MB"),
                ("Internal Objects", $"{250d:F0} MB"),
                ("Version Store", $"{50d:F0} MB"),
                ("Top Consumer", $"Session #55 ({123.4:F0} MB)")
            },
            item.Fields);
    }

    /// <summary>
    /// #2515: the three states of the ceiling render as three different words, because they are three
    /// different facts. "Unlimited" and "Unknown" take the same denominator but they do not mean the same
    /// thing, and printing either as a number would claim a measurement that was never taken.
    /// </summary>
    [Theory]
    [InlineData(65536d, "65536 MB")]
    [InlineData(-1d, "Unlimited")]
    [InlineData(0d, "Unknown")]
    public void BuildTempDbSpaceContext_RendersTheCeilingsThreeStates(double maxSizeMb, string expected)
    {
        var context = AlertContextBuilders.BuildTempDbSpaceContext(new TempDbSpaceInfo
        {
            TotalReservedMb = 800,
            UnallocatedMb = 200,
            MaxSizeMb = maxSizeMb
        });

        Assert.Equal(("Max Size", expected), context!.Details[0].Fields[2]);
    }

    [Fact]
    public void BuildTempDbSpaceContext_NoTopConsumer_RendersNone()
    {
        var context = AlertContextBuilders.BuildTempDbSpaceContext(new TempDbSpaceInfo
        {
            TotalReservedMb = 100,
            UnallocatedMb = 900,
            TopConsumerSessionId = 0
        });

        Assert.Equal(("Top Consumer", "None"), context!.Details[0].Fields[6]);
    }

    /* ---------------- anomalous jobs ---------------- */

    [Fact]
    public void BuildAnomalousJobContext_RendersDurationsAndIncidents()
    {
        var jobs = new List<AnomalousJobInfo>
        {
            new()
            {
                JobName = "Nightly ETL",
                CurrentDurationSeconds = 3661,  /* -> 1h 1m   */
                AvgDurationSeconds = 90,        /* -> 1m 30s  */
                P95DurationSeconds = 45,        /* -> 45s     */
                PercentOfAverage = 350,
                StartTime = new DateTime(2026, 6, 19, 14, 30, 15)
            }
        };

        var context = AlertContextBuilders.BuildAnomalousJobContext(Server, jobs);

        Assert.NotNull(context);
        /* 1 job item + 1 appended incident item. */
        Assert.Equal(2, context!.Details.Count);
        Assert.Equal("Nightly ETL", context.Details[0].Heading);
        Assert.Equal(
            new List<(string, string)>
            {
                ("Current Duration", "1h 1m"),
                ("Avg Duration", "1m 30s"),
                ("P95 Duration", "45s"),
                ("% of Average", $"{350m:F0}%"),
                ("Started", "2026-06-19 14:30:15")
            },
            context.Details[0].Fields);

        var expected = AlertFingerprint.ForKey(Server, AlertFingerprint.Job, "Nightly ETL", new[] { "Nightly ETL" });
        Assert.Equal(expected!.DedupKey, Assert.Single(context.Incidents!).DedupKey);
    }

    [Fact]
    public void BuildAnomalousJobContext_NullPercentOfAverage_RendersNA_AndCapsAtThree()
    {
        var jobs = new List<AnomalousJobInfo>();
        for (int i = 0; i < 4; i++)
            jobs.Add(new AnomalousJobInfo { JobName = $"Job {i}", PercentOfAverage = null, StartTime = DateTime.UtcNow });

        var context = AlertContextBuilders.BuildAnomalousJobContext(Server, jobs);

        Assert.NotNull(context);
        /* 3 shown jobs (capped) + 3 incident items. */
        Assert.Equal(6, context!.Details.Count);
        Assert.Equal(("% of Average", "N/A"), context.Details[0].Fields[3]);
        Assert.Null(AlertContextBuilders.BuildAnomalousJobContext(Server, new List<AnomalousJobInfo>()));
    }

    /* ---------------- failed jobs ---------------- */

    [Fact]
    public void BuildFailedJobContext_RendersStepAndTruncatedMessage()
    {
        var runTime = new DateTime(2026, 6, 19, 3, 15, 0);
        var context = AlertContextBuilders.BuildFailedJobContext(Server, new List<FailedJobInfo>
        {
            new()
            {
                JobName = "Backup Job",
                RunDateTime = runTime,
                StepId = 2,
                StepName = "Backup databases",
                Message = new string('x', 350)
            }
        });

        Assert.NotNull(context);
        Assert.Equal(2, context!.Details.Count); /* 1 job item + 1 incident item */
        Assert.Equal("Backup Job", context.Details[0].Heading);
        Assert.Equal(
            new List<(string, string)>
            {
                ("Job", "Backup Job"),
                ("Failed At", runTime.ToString("yyyy-MM-dd HH:mm:ss")),
                ("Step", "2 — Backup databases"),
                ("Message", new string('x', 300) + "...")
            },
            context.Details[0].Fields);

        var expected = AlertFingerprint.ForKey(Server, AlertFingerprint.Job, "Backup Job", new[] { "Backup Job" });
        Assert.Equal(expected!.DedupKey, Assert.Single(context.Incidents!).DedupKey);
    }

    [Fact]
    public void BuildFailedJobContext_OutcomeRowWithoutStep_OmitsStepField_AndCapsAtFive()
    {
        var jobs = new List<FailedJobInfo>();
        for (int i = 0; i < 6; i++)
            jobs.Add(new FailedJobInfo { JobName = $"Job {i}", RunDateTime = DateTime.UtcNow, StepId = 0 });

        var context = AlertContextBuilders.BuildFailedJobContext(Server, jobs);

        Assert.NotNull(context);
        /* 5 shown jobs (capped) + 5 incident items. */
        Assert.Equal(10, context!.Details.Count);
        Assert.DoesNotContain(context.Details[0].Fields, f => f.Label == "Step");
        Assert.Null(AlertContextBuilders.BuildFailedJobContext(Server, new List<FailedJobInfo>()));
    }

    /* ---------------- helpers ---------------- */

    [Fact]
    public void ContextToDetailText_NullOrEmptyContext_ReturnsNull()
    {
        Assert.Null(AlertContextBuilders.ContextToDetailText(null));
        Assert.Null(AlertContextBuilders.ContextToDetailText(new AlertContext()));
    }

    [Fact]
    public void ContextToDetailText_RendersHeadingsAndIndentedFields()
    {
        var context = new AlertContext();
        context.Details.Add(new AlertDetailItem { Heading = "H1", Fields = new() { ("A", "1") } });
        context.Details.Add(new AlertDetailItem { Heading = "H2", Fields = new() { ("B", "2") } });

        var expected = string.Join(Environment.NewLine, "H1", "  A: 1", "", "H2", "  B: 2");
        Assert.Equal(expected, AlertContextBuilders.ContextToDetailText(context));
    }

    [Fact]
    public void TruncateText_CollapsesNewlines_TruncatesWithEllipsis()
    {
        Assert.Equal("", AlertContextBuilders.TruncateText(""));
        /* '\r' and '\n' each become a space (historical behavior — not collapsed to one). */
        Assert.Equal("a  b", AlertContextBuilders.TruncateText("a\r\nb"));
        Assert.Equal("abc", AlertContextBuilders.TruncateText("  abc  "));
        Assert.Equal(new string('x', 300) + "...", AlertContextBuilders.TruncateText(new string('x', 350)));
        Assert.Equal("ab...", AlertContextBuilders.TruncateText("abcdef", 2));
    }

    /* ---------------- A0: the shared row types are the app's row types ---------------- */

    [Fact]
    public void RowTypes_ResolveToAlertingNamespace_ThroughAppApiSurface()
    {
        /* The global using aliases in Lite's GlobalUsings.cs point the old bare type names at
           PerformanceMonitor.Alerting — so the app's public API must now surface the SHARED types. */
        Assert.Equal("PerformanceMonitor.Alerting", typeof(FailedJobInfo).Namespace);

        Assert.Equal(typeof(Task<List<PoisonWaitDelta>>),
            typeof(LocalDataService).GetMethod("GetLatestPoisonWaitAvgsAsync")!.ReturnType);
        Assert.Equal(typeof(Task<List<LongRunningQueryInfo>>),
            typeof(LocalDataService).GetMethod("GetLongRunningQueriesAsync")!.ReturnType);
        Assert.Equal(typeof(Task<List<VolumeFreeSpaceInfo>>),
            typeof(LocalDataService).GetMethod("GetVolumeFreeSpaceAsync")!.ReturnType);
        Assert.Equal(typeof(Task<TempDbSpaceInfo?>),
            typeof(LocalDataService).GetMethod("GetLatestTempDbSpaceAsync")!.ReturnType);
        Assert.Equal(typeof(Task<List<AnomalousJobInfo>>),
            typeof(LocalDataService).GetMethod("GetAnomalousJobsAsync")!.ReturnType);
        Assert.Equal(typeof(Task<List<FailedJobInfo>>),
            typeof(RemoteCollectorService).GetMethod("GetRecentlyFailedJobsAsync")!.ReturnType);
    }
}
