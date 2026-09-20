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

    /* ---------------- #3497: the Agent-job annotation ---------------- */

    /// <summary>msdb job_id AB6D9F63-3B01-4E15-9F34-B0A0F0B355A2, step 3, spelled the way SQL Agent
    /// spells it: the job_id's binary(16) hex (Data1–Data3 little-endian — the byte-order pin lives in
    /// AgentJobStepQueryTests).</summary>
    private const string AgentProgramName = "SQLAgent - TSQL JobStep (Job 0x639F6DAB013B154E9F34B0A0F0B355A2 : Step 3)";
    private static readonly Guid AgentJobId = Guid.Parse("AB6D9F63-3B01-4E15-9F34-B0A0F0B355A2");

    [Fact]
    public void BuildLongRunningQueryContext_NullAgentMap_RendersNoAnnotation_TheByteIdenticalArm()
    {
        /* The regression posture: NULL (the default, every pre-#3497 caller) means no resolution was
           attempted, and an Agent session's card renders exactly as before — raw Program field only. */
        var context = AlertContextBuilders.BuildLongRunningQueryContext(Server, new List<LongRunningQueryInfo>
        {
            Lrq(71, 314, db: "StackOverflow", program: AgentProgramName, query: "ALTER INDEX IX_Users_Rep ON dbo.Users REORGANIZE")
        });

        Assert.NotNull(context);
        Assert.DoesNotContain(context!.Details[0].Fields, f => f.Label == "Running under Agent job");
        Assert.Contains(("Program", AgentProgramName), context.Details[0].Fields);
    }

    [Fact]
    public void BuildLongRunningQueryContext_ResolvedAgentJob_NamesTheJobAndStep_DirectlyUnderProgram()
    {
        var names = new Dictionary<AgentJobStepKey, AgentJobStepNames>
        {
            [new AgentJobStepKey(AgentJobId, 3)] = new("nightly index maintenance", "Reorganize fragmented indexes")
        };

        var context = AlertContextBuilders.BuildLongRunningQueryContext(Server, new List<LongRunningQueryInfo>
        {
            Lrq(71, 314, db: "StackOverflow", program: AgentProgramName, query: "ALTER INDEX IX_Users_Rep ON dbo.Users REORGANIZE")
        }, agentJobNames: names);

        Assert.NotNull(context);
        var fields = context!.Details[0].Fields;
        var programIndex = fields.FindIndex(f => f.Label == "Program");
        Assert.Equal(programIndex + 1, fields.FindIndex(f => f.Label == "Running under Agent job"));
        Assert.Contains(("Running under Agent job", "nightly index maintenance, step 3 (Reorganize fragmented indexes)"), fields);
    }

    [Fact]
    public void BuildLongRunningQueryContext_ResolvedJobWithNoStepRow_NamesTheJob_WithoutInventingAStepName()
    {
        /* sysjobsteps had no row (deleted or renumbered step) — the LEFT JOIN arm: the JOB is the fact
           that closes the triage, so it still renders; only the parenthetical step name is omitted. */
        var names = new Dictionary<AgentJobStepKey, AgentJobStepNames>
        {
            [new AgentJobStepKey(AgentJobId, 3)] = new("nightly index maintenance", null)
        };

        var context = AlertContextBuilders.BuildLongRunningQueryContext(Server, new List<LongRunningQueryInfo>
        {
            Lrq(71, 314, db: "StackOverflow", program: AgentProgramName, query: "SELECT 1")
        }, agentJobNames: names);

        Assert.Contains(("Running under Agent job", "nightly index maintenance, step 3"), context!.Details[0].Fields);
    }

    [Fact]
    public void BuildLongRunningQueryContext_UnresolvedAgentJob_StatesTheFact_AndCarriesTheRawMarker()
    {
        /* A NON-null EMPTY map says a resolution ran and answered nothing (msdb denied, transient fault,
           job deleted): the card still states the Agent-job fact the parse alone establishes, in the
           unresolved form whose hex matches the Program field's spelling byte for byte — never an error,
           never a fabricated name. */
        var context = AlertContextBuilders.BuildLongRunningQueryContext(Server, new List<LongRunningQueryInfo>
        {
            Lrq(71, 314, db: "StackOverflow", program: AgentProgramName, query: "SELECT 1")
        }, agentJobNames: new Dictionary<AgentJobStepKey, AgentJobStepNames>());

        Assert.Contains(
            ("Running under Agent job", "(name unresolved) Job 0x639F6DAB013B154E9F34B0A0F0B355A2, step 3"),
            context!.Details[0].Fields);
    }

    [Fact]
    public void BuildLongRunningQueryContext_NonAgentSession_WithAMapSupplied_RendersByteIdentically()
    {
        /* The other regression pin: an application connection's card is untouched even when a resolution
           ran for a sibling session — the annotation keys off the parse, not off the map's presence. */
        var withMap = AlertContextBuilders.BuildLongRunningQueryContext(Server, new List<LongRunningQueryInfo>
        {
            Lrq(71, 314, db: "StackOverflow", program: "HammerDB", query: "SELECT 1", hash: "0x9AAF0129E4E9AD07")
        }, agentJobNames: new Dictionary<AgentJobStepKey, AgentJobStepNames>());
        var withoutMap = AlertContextBuilders.BuildLongRunningQueryContext(Server, new List<LongRunningQueryInfo>
        {
            Lrq(71, 314, db: "StackOverflow", program: "HammerDB", query: "SELECT 1", hash: "0x9AAF0129E4E9AD07")
        });

        Assert.Equal(
            AlertContextBuilders.ContextToDetailText(withoutMap),
            AlertContextBuilders.ContextToDetailText(withMap));
    }

    [Fact]
    public void BuildLongRunningQueryContext_TheAnnotationIsFingerprintInert()
    {
        /* #1140's dedup key hashes (server, type, query_hash) and the annotation is a FIELD — so a card
           that re-fires with the job name freshly resolved (or freshly unresolvable) folds into the same
           incident. Pinned rather than merely stated: same rows, three annotation states, one DedupKey. */
        List<LongRunningQueryInfo> Rows() => new()
        {
            Lrq(71, 314, db: "StackOverflow", program: AgentProgramName, query: "SELECT 1", hash: "0x9AAF0129E4E9AD07")
        };
        var resolved = AlertContextBuilders.BuildLongRunningQueryContext(Server, Rows(),
            agentJobNames: new Dictionary<AgentJobStepKey, AgentJobStepNames>
            {
                [new AgentJobStepKey(AgentJobId, 3)] = new("nightly index maintenance", "step name")
            });
        var unresolved = AlertContextBuilders.BuildLongRunningQueryContext(Server, Rows(),
            agentJobNames: new Dictionary<AgentJobStepKey, AgentJobStepNames>());
        var unannotated = AlertContextBuilders.BuildLongRunningQueryContext(Server, Rows());

        var key = Assert.Single(unannotated!.Incidents!).DedupKey;
        Assert.Equal(key, Assert.Single(resolved!.Incidents!).DedupKey);
        Assert.Equal(key, Assert.Single(unresolved!.Incidents!).DedupKey);
    }

    /* ---------------- #3742: the excluded-databases receipt beside the knob's ---------------- */

    [Fact]
    public void BuildLongRunningQueryExcludedDatabasesItem_IsItsOwnItem_WithTheCountAndTheList()
    {
        /* #3742 moved excludedDatabases INTO both SQL Server reads ahead of the cap (it was dropped client-side
           after LIMIT, so an excluded database's sessions could consume the whole page), and the card now says how
           many it removed. Its own item, not fields on the knob's: the knob item's heading says "by the opt-out
           knob" and its Excluded Count has always been the knob's two arms — a database count inside it would make
           the heading false and the count ambiguous. Singular/plural heading, the count under the shared label
           constant, the list as the operator spelled it. */
        var six = AlertContextBuilders.BuildLongRunningQueryExcludedDatabasesItem(new[] { "ReportingDb", "Staging" }, 6);
        Assert.Equal("6 sessions over the threshold were in excluded databases", six.Heading);
        Assert.Equal("6", Assert.Single(six.Fields, f => f.Label == AlertContextBuilders.LongRunningQueryExcludedByDatabaseLabel).Value);
        Assert.Equal("ReportingDb, Staging", Assert.Single(six.Fields, f => f.Label == "Excluded Databases").Value);
        Assert.Equal(2, six.Fields.Count);

        var one = AlertContextBuilders.BuildLongRunningQueryExcludedDatabasesItem(new[] { "ReportingDb" }, 1);
        Assert.Equal("1 session over the threshold was in an excluded database", one.Heading);

        /* "0" is a receipt too — the engine renders the item whenever the list is set (and never when it is empty). */
        var none = AlertContextBuilders.BuildLongRunningQueryExcludedDatabasesItem(new[] { "ReportingDb" }, 0);
        Assert.Equal("0 sessions over the threshold were in excluded databases", none.Heading);
        Assert.Equal("0", Assert.Single(none.Fields, f => f.Label == AlertContextBuilders.LongRunningQueryExcludedByDatabaseLabel).Value);

        /* The label is distinct from the knob's three so a reader of context_json cannot confuse the two receipts. */
        Assert.Equal("Excluded By Database", AlertContextBuilders.LongRunningQueryExcludedByDatabaseLabel);
        Assert.NotEqual(AlertContextBuilders.LongRunningQueryExcludedCountLabel, AlertContextBuilders.LongRunningQueryExcludedByDatabaseLabel);
        Assert.NotEqual(AlertContextBuilders.LongRunningQueryExcludedByLoginLabel, AlertContextBuilders.LongRunningQueryExcludedByDatabaseLabel);

        Assert.Throws<ArgumentNullException>(() => AlertContextBuilders.BuildLongRunningQueryExcludedDatabasesItem(null!, 0));
    }

    /* ---------------- #3495: the High CPU active-maintenance annotation ---------------- */

    [Fact]
    public void BuildActiveMaintenanceDetail_NoMaintenanceSessions_ReturnsEmpty_TheByteIdenticalArm()
    {
        /* The regression pin's builder half: the caller string-appends this, so "" leaves the High CPU
           card byte-identical to the pre-#3495 render. An ordinary busy session is not maintenance. */
        Assert.Equal("", AlertContextBuilders.BuildActiveMaintenanceDetail(new List<LongRunningQueryInfo>()));
        Assert.Equal("", AlertContextBuilders.BuildActiveMaintenanceDetail(new List<LongRunningQueryInfo>
        {
            Lrq(71, 314, db: "StackOverflow", program: "HammerDB", query: "SELECT COUNT_BIG(*) FROM dbo.Users", waitType: "CXPACKET")
        }));
    }

    [Fact]
    public void BuildActiveMaintenanceDetail_NamesTheBackup_InTheIssuesOneLineForm()
    {
        /* #3495's acceptance shape: session kind, program, elapsed, wait — what IS, never a verdict. */
        var detail = AlertContextBuilders.BuildActiveMaintenanceDetail(new List<LongRunningQueryInfo>
        {
            Lrq(120, 1034, db: "StackOverflow", program: "RdsAdminService",
                query: "BACKUP DATABASE [StackOverflow] TO VIRTUAL_DEVICE = 'x' WITH COMPRESSION",
                waitType: "ASYNC_IO_COMPLETION")
        });

        Assert.Equal("\n  Active maintenance: BACKUP DATABASE (RdsAdminService), 17m 14s elapsed, ASYNC_IO_COMPLETION", detail);
    }

    [Fact]
    public void BuildActiveMaintenanceDetail_OmitsAnEmptyProgramAndAnAbsentWait_RatherThanRenderingBlanks()
    {
        var detail = AlertContextBuilders.BuildActiveMaintenanceDetail(new List<LongRunningQueryInfo>
        {
            Lrq(120, 45, query: "ALTER INDEX IX_Users_Rep ON dbo.Users REBUILD")
        });

        Assert.Equal("\n  Active maintenance: ALTER INDEX, 45s elapsed", detail);
    }

    [Fact]
    public void BuildActiveMaintenanceDetail_MatchesTheStatementHead_NotALiteralMention()
    {
        /* The under-annotate direction is chosen deliberately: a head match can miss maintenance buried
           mid-batch (costing only the annotation), while a contains-anywhere match could NAME maintenance
           on a card where none runs — someone's dynamic-SQL builder mentioning the phrase in a literal. */
        Assert.Null(AlertContextBuilders.TryGetMaintenanceStatementHead(
            "SELECT command = N'BACKUP DATABASE ' + QUOTENAME(d.name) FROM sys.databases AS d"));
        /* Leading whitespace and casing are presentation, not identity — both still match. */
        Assert.Equal("BACKUP DATABASE", AlertContextBuilders.TryGetMaintenanceStatementHead("  \n backup database [x] TO DISK = 'y'"));
        Assert.Equal("RESTORE LOG", AlertContextBuilders.TryGetMaintenanceStatementHead("RESTORE LOG [x] FROM DISK = 'y'"));
        Assert.Null(AlertContextBuilders.TryGetMaintenanceStatementHead(null));
        Assert.Null(AlertContextBuilders.TryGetMaintenanceStatementHead(""));
    }

    [Fact]
    public void BuildActiveMaintenanceDetail_CapsTheLines_AndStatesTheOmission()
    {
        /* The #3494 discipline: whole lines that fit, then a stated omission — never a silent cut. Five
           concurrent maintenance sessions render three lines (the input's own order — the read returns
           elapsed DESC, so the longest-running leads) and one line counting the other two. */
        var sessions = new List<LongRunningQueryInfo>();
        for (int i = 0; i < 5; i++)
        {
            sessions.Add(Lrq(100 + i, 600 - (i * 60), program: "RdsAdminService",
                query: $"BACKUP DATABASE [db{i}] TO VIRTUAL_DEVICE = 'x'", waitType: "ASYNC_IO_COMPLETION"));
        }

        var detail = AlertContextBuilders.BuildActiveMaintenanceDetail(sessions);

        /* Every emitted line leads with "\n", so the split's first element is empty and the line count
           is Length - 1: the cap's three named sessions plus the one stated-omission line. */
        Assert.Equal(AlertContextBuilders.ActiveMaintenanceMaxLines + 2, detail.Split('\n').Length);
        Assert.Equal(AlertContextBuilders.ActiveMaintenanceMaxLines,
            System.Text.RegularExpressions.Regex.Matches(detail, "Active maintenance: BACKUP DATABASE").Count);
        Assert.EndsWith("Active maintenance: 2 more maintenance session(s) not shown", detail, StringComparison.Ordinal);
        /* The leader (longest elapsed) is named first — the likeliest pin leads the reader's eye. */
        Assert.Contains("10m 0s elapsed", detail.Split('\n')[1], StringComparison.Ordinal);
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
        Assert.Equal("tempdb — 80% Reserved", item.Heading);
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
                /* #3421: the run start is the monitored server's own clock, and the offset rides with the
                   row so the body can state it in UTC. -240 is the measured fleet value. */
                StartTime = new DateTime(2026, 6, 19, 14, 30, 15),
                UtcOffsetMinutes = -240
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
                ("Started", "2026-06-19 18:30:15Z")
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
                UtcOffsetMinutes = -240,
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
                ("Failed At", "2026-06-19 07:15:00Z"),
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

        /* #3297: the implementation moved to the Notifications project, because the delivery channels there
           decide whether an alert's prose adds anything over its structured context by comparing against
           this exact text — and they cannot reference this project. One implementation, so the comparison
           cannot drift away from the text it compares to. */
        Assert.Equal(expected, AlertDetailText.Flatten(context));
    }

    /// <summary>
    /// #3297's gate, which is what keeps the fix from printing every engine alert twice. Every engine
    /// alert's detail text IS <see cref="AlertDetailText.Flatten"/> of its own context —
    /// <c>AlertEngine</c> builds it that way at every fire site — so the channels suppress prose on that
    /// equality and the alerts that already read correctly (blocking, deadlocks) are unchanged.
    /// </summary>
    [Fact]
    public void ProseForDelivery_SuppressesAFlattenedContext_AndKeepsIndependentProse()
    {
        var context = new AlertContext();
        context.Details.Add(new AlertDetailItem { Heading = "H1", Fields = new() { ("A", "1") } });

        /* The engine path: detail text derived from the context. Nothing to add. */
        var flattened = AlertContextBuilders.ContextToDetailText(context)!;
        Assert.Null(AlertDetailText.ProseForDelivery(flattened, context));
        Assert.Null(AlertDetailText.ProseForDelivery(flattened + "\r\n  ", context));

        /* The #2109 AG-database path: a structured context AND hand-written prose naming the remedy.
           Suppressing whenever a context is present would drop exactly this. */
        Assert.Equal(
            "Resume it with ALTER DATABASE SET HADR RESUME.",
            AlertDetailText.ProseForDelivery("Resume it with ALTER DATABASE SET HADR RESUME.", context));

        /* The self-alert path: prose, no context at all. */
        Assert.Equal("do the thing", AlertDetailText.ProseForDelivery("do the thing", null));

        /* Nothing is nothing, however it is spelled. */
        Assert.Null(AlertDetailText.ProseForDelivery(null, context));
        Assert.Null(AlertDetailText.ProseForDelivery("   ", context));
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

        /* #3539 A4: the alert's DuckDB read is the window accumulation; PoisonWaitDelta itself stays
           shared for the deprecated Dashboard and BuildPoisonWaitContext. */
        Assert.Equal(typeof(Task<List<PoisonWaitAccumulation>>),
            typeof(LocalDataService).GetMethod("GetPoisonWaitAccumulationAsync")!.ReturnType);
        Assert.Equal("PerformanceMonitor.Alerting", typeof(PoisonWaitDelta).Namespace);
        /* #3653 (A5, Q5): the read returns the shared result record (sessions + the opt-out knob's excluded
           count) — still the Alerting namespace's type, which is what this pin is about. */
        Assert.Equal(typeof(Task<LongRunningQueryReadResult>),
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
