/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4821: four Lite reads turn a SQL Server's local wall-clock time into UTC, and each used ONE offset, the
/// newest <c>server_properties.utc_offset_minutes</c>. A row stamped before the last daylight-saving change
/// came out an hour off. They now follow the server's zone (<c>time_zone_id</c>, SQL Server 2022 and later)
/// through <see cref="ServerClock"/>, the way the viewer does (#4783), and keep the fixed offset for a server
/// that reports no zone.
///
/// <para>Every arm plants the SAME situation: the newest properties row was collected in summer (offset -240,
/// zone Eastern) and the rows under test were stamped in winter, when Eastern was at -300. The old single
/// offset put a winter row an hour late (or early); the zone puts it right. A summer row, a server with an
/// offset and no zone, and a server with no properties row at all are the controls that must not move.</para>
///
/// <para>The mirror (<see cref="Clock.EasternZoneWinterSnapshot"/>) is the newest snapshot in WINTER and the rows in
/// summer: <c>server_properties</c> is collected when the server connects, so its newest row can be months old. There
/// the newest offset reads a plan compiled just before the window an hour late, and the SQL's one-hour margin
/// (<c>PlanCreationClock.RoughBound</c>) is what keeps it for the exact test. The caps the reads used to apply in SQL
/// (twenty for the fact, five for the drill-down) are pinned against plans that pass the rough filter and fail the
/// exact test, which a cap applied first would let fill every slot.</para>
///
/// <para>The four sites are read through their real entry points: the PARAMETER_SENSITIVITY fact
/// (<c>DuckDbFactCollector</c>), the two drill-downs that carry the same plan-creation test
/// (<c>DrillDownCollector</c>), the default-trace anchor for a configuration change
/// (<c>AnalysisService.ResolveTraceAnchorAsync</c>) and the long-running job read
/// (<c>LocalDataService.GetAnomalousJobsAsync</c>).</para>
/// </summary>
public sealed class ServerClockDstReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 48210;
    private const string ServerName = "DstSrv";
    private const string Db = "DstDb";
    private const string EasternWindowsId = "Eastern Standard Time";
    private const string WesternEuropeWindowsId = "W. Europe Standard Time";
    private const string Maxdop = "max degree of parallelism";
    private const string CostThreshold = "cost threshold for parallelism";
    private const string ServerMemory = "max server memory (MB)";
    private const string BackupCompression = "backup compression default";

    /// <summary>How the server reports its clock.</summary>
    public enum Clock
    {
        /// <summary>Newest properties row: offset -240 (summer) and the Eastern zone.</summary>
        EasternZone,

        /// <summary>Newest properties row: offset -240 and NO zone (a pre-2022 engine).</summary>
        OffsetOnly,

        /// <summary>No properties row at all: UTC.</summary>
        NoRow,

        /// <summary>
        /// Newest properties row: offset -300 (winter) and the Eastern zone, collected in January. The mirror of
        /// <see cref="EasternZone"/>: <c>server_properties</c> is collected when the server connects, so the newest
        /// row can be months old, and the rows under test are then the SUMMER ones.
        /// </summary>
        EasternZoneWinterSnapshot,
    }

    /// <summary>What the two reads applied in SQL, as a LIMIT, before the exact creation-time test moved to the reader.</summary>
    private const int ParameterSensitivityFactCap = 20;
    private const int ParameterSensitiveDrillDownCap = 5;

    /* A real offender's worker times (ratio 1,000) and a decoy's (ratio 25,000, so a decoy sorts ahead of every real one). */
    private const long RealMinWorkerUs = 20_000;
    private const long RealMaxWorkerUs = 20_000_000;
    private const long DecoyMinWorkerUs = 10_000;
    private const long DecoyMaxWorkerUs = 250_000_000;

    private static readonly DateTime NewestPropertiesTime = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WinterSnapshotTime = new(2026, 1, 2, 0, 0, 0, DateTimeKind.Unspecified);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public ServerClockDstReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>The instant the arms hang their rows on, naive UTC: mid-January (Eastern -300) or mid-July (-240).</summary>
    private static DateTime Anchor(bool winter) =>
        winter ? new DateTime(2026, 1, 15, 12, 0, 0, DateTimeKind.Unspecified) : new DateTime(2026, 7, 15, 12, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The offset the SERVER's clock was really at when the rows were stamped.</summary>
    private static int StampedOffset(bool winter, Clock clock) => clock switch
    {
        Clock.EasternZone or Clock.EasternZoneWinterSnapshot => winter ? -300 : -240,
        Clock.OffsetOnly => -240,
        _ => 0,
    };

    private static AnalysisContext Context(bool winter) => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        TimeRangeStart = Anchor(winter),
        TimeRangeEnd = Anchor(winter).AddHours(4),
    };

    /* ── Site 3: the PARAMETER_SENSITIVITY fact ── */

    [Theory]
    [InlineData(true, Clock.EasternZone)]
    [InlineData(false, Clock.EasternZone)]
    [InlineData(false, Clock.EasternZoneWinterSnapshot)]
    [InlineData(true, Clock.OffsetOnly)]
    [InlineData(true, Clock.NoRow)]
    public async Task TheParameterSensitivityFact_ConvertsEachPlansCreationTime_WithTheOffsetInForceThen(bool winter, Clock clock)
    {
        await SeedClockAsync(clock);
        await SeedPlansAsync(winter, clock);

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context(winter));

        var fact = Assert.Single(facts, f => f.Key == "PARAMETER_SENSITIVITY");
        Assert.Equal(2, fact.Metadata["offender_count"]);
    }

    /* ── Site 2 (first read): the parameter-sensitive drill-down ── */

    [Theory]
    [InlineData(true, Clock.EasternZone)]
    [InlineData(false, Clock.EasternZone)]
    [InlineData(false, Clock.EasternZoneWinterSnapshot)]
    [InlineData(true, Clock.OffsetOnly)]
    [InlineData(true, Clock.NoRow)]
    public async Task TheParameterSensitiveDrillDown_ConvertsEachPlansCreationTime_WithTheOffsetInForceThen(bool winter, Clock clock)
    {
        await SeedClockAsync(clock);
        await SeedPlansAsync(winter, clock);

        var finding = new AnalysisFinding
        {
            RootFactKey = "PARAMETER_SENSITIVITY",
            StoryPath = "PARAMETER_SENSITIVITY",
            PathKeys = ["PARAMETER_SENSITIVITY"],
            Severity = 1.0,
        };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], Context(winter));

        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown!.TryGetValue("parameter_sensitive_queries", out var raw));
        Assert.Equal(2, JsonSerializer.SerializeToElement(raw).GetArrayLength());
    }

    /* ── Site 2 (second read): the regressed-queries drill-down's co-fired flag ── */

    [Theory]
    [InlineData(true, Clock.EasternZone)]
    [InlineData(false, Clock.EasternZone)]
    [InlineData(false, Clock.EasternZoneWinterSnapshot)]
    [InlineData(true, Clock.OffsetOnly)]
    [InlineData(true, Clock.NoRow)]
    public async Task TheRegressedQueriesCoFiredFlag_ConvertsTheCreationTime_WithTheOffsetInForceThen(bool winter, Clock clock)
    {
        await SeedClockAsync(clock);
        var t = Anchor(winter);
        var offset = StampedOffset(winter, clock);

        /* Query 1 regressed and its plan-cache plan was compiled one minute AFTER the window opened: the
           detector does not count it, so the flag stays false. Query 2's plan was compiled half an hour
           BEFORE the window: it does, so the flag is true. */
        await SeedRegressionAsync(1, t);
        await SeedRegressionAsync(2, t);
        await SeedPlanCacheRowAsync("0xQH1", t.AddMinutes(1).AddMinutes(offset), t, 1);
        await SeedPlanCacheRowAsync("0xQH2", t.AddMinutes(-30).AddMinutes(offset), t, 2);

        var finding = new AnalysisFinding
        {
            RootFactKey = "PLAN_REGRESSION",
            StoryPath = "PLAN_REGRESSION",
            PathKeys = ["PLAN_REGRESSION"],
            Severity = 1.0,
        };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], Context(winter));

        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown!.TryGetValue("regressed_queries", out var raw));
        var cofired = JsonSerializer.SerializeToElement(raw).EnumerateArray()
            .ToDictionary(r => r.GetProperty("query_id").GetInt64(), r => r.GetProperty("parameter_sensitivity_cofired").GetBoolean());
        Assert.Equal(2, cofired.Count);
        Assert.False(cofired[1], "a plan compiled after the window opened must not count as co-fired");
        Assert.True(cofired[2], "a plan compiled before the window opened must count as co-fired");
    }

    /* ── Site 4: the long-running job alert ── */

    [Theory]
    [InlineData(true, Clock.EasternZone, -300)]
    [InlineData(false, Clock.EasternZone, -240)]
    [InlineData(false, Clock.EasternZoneWinterSnapshot, -240)]
    [InlineData(true, Clock.OffsetOnly, -240)]
    [InlineData(true, Clock.NoRow, null)]
    public async Task TheAnomalousJobRead_CarriesTheOffsetInForceAtTheJobsStartTime(bool winter, Clock clock, int? expectedOffset)
    {
        await SeedClockAsync(clock);
        var startUtc = Anchor(winter);
        await SeedRunningJobAsync("Nightly ETL", startUtc.AddMinutes(StampedOffset(winter, clock)));

        var jobs = await new LocalDataService(_duckDb).GetAnomalousJobsAsync(ServerId, 3);

        var job = Assert.Single(jobs);
        Assert.Equal(expectedOffset, job.UtcOffsetMinutes);
        if (expectedOffset.HasValue)
        {
            Assert.Equal(startUtc, job.StartTimeUtc);
        }
        else
        {
            Assert.Null(job.StartTimeUtc);
        }
    }

    [Fact]
    public async Task TheAnomalousJobRead_ConvertsAWinterJobAndASummerJob_EachWithItsOwnOffset()
    {
        await SeedClockAsync(Clock.EasternZone);
        await SeedRunningJobAsync("Winter job", Anchor(true).AddMinutes(-300));
        await SeedRunningJobAsync("Summer job", Anchor(false).AddMinutes(-240));

        var jobs = await new LocalDataService(_duckDb).GetAnomalousJobsAsync(ServerId, 3);

        Assert.Equal(2, jobs.Count);
        var winter = jobs.Single(j => j.JobName == "Winter job");
        var summer = jobs.Single(j => j.JobName == "Summer job");
        Assert.Equal(-300, winter.UtcOffsetMinutes);
        Assert.Equal(Anchor(true), winter.StartTimeUtc);
        Assert.Equal(-240, summer.UtcOffsetMinutes);
        Assert.Equal(Anchor(false), summer.StartTimeUtc);
    }

    /* ── Site 1: the default-trace anchor of a configuration change ── */

    [Theory]
    [InlineData(true, Clock.EasternZone)]
    [InlineData(false, Clock.EasternZone)]
    [InlineData(false, Clock.EasternZoneWinterSnapshot)]
    [InlineData(true, Clock.OffsetOnly)]
    [InlineData(true, Clock.NoRow)]
    public async Task TheTraceAnchor_KeepsTheLineInsideTheExactSpan_AndDropsTheOnesJustOutside(bool winter, Clock clock)
    {
        await SeedClockAsync(clock);
        var previousCapture = Anchor(winter);
        var changeTime = previousCapture.AddHours(4);
        var offset = StampedOffset(winter, clock);

        /* One line a minute inside the span's upper edge, one half an hour after it and one half an hour
           before its lower edge. Both outside lines pass the widened first filter in SQL; only the exact
           span, applied after the conversion, drops them. */
        var insideUtc = changeTime.AddMinutes(-1);
        await SeedReconfigureLineAsync(insideUtc, offset, Maxdop, 0, 8);
        await SeedReconfigureLineAsync(changeTime.AddMinutes(30), offset, CostThreshold, 5, 50);
        await SeedReconfigureLineAsync(previousCapture.AddMinutes(-30), offset, ServerMemory, 1024, 2048);

        var change = new ConfigChangeAttribution.ChangeEvent(changeTime, previousCapture,
        [
            new ConfigChangeAttribution.SettingChange(Maxdop, 0, 8, 0, 8, false),
            new ConfigChangeAttribution.SettingChange(CostThreshold, 5, 50, 5, 50, false),
            new ConfigChangeAttribution.SettingChange(ServerMemory, 1024, 2048, 1024, 2048, false),
        ]);

        var anchor = await new AnalysisService(_duckDb).ResolveTraceAnchorAsync(Context(winter), change);

        Assert.NotNull(anchor);
        Assert.Equal([Maxdop], anchor!.Matched.Keys.ToArray());
        Assert.Equal(insideUtc, anchor.ChangedAtUtc);
    }

    /* ── The caps that moved from SQL into the reader ── */

    /// <summary>
    /// The fact's cap of twenty used to be a LIMIT in SQL, after the compiled-before-the-window test, so it kept the
    /// twenty worst plans that test admitted. The exact test now runs in the reader, so the cap has to run after it
    /// too. Twenty plans that pass the SQL's rough first filter (winter plans, read against a summer newest offset),
    /// fail the exact test (they were compiled 30 minutes AFTER the window opened) and outrank every real offender
    /// would take every slot of a cap applied first and leave the fact empty; the real offenders must still fill it.
    /// </summary>
    [Fact]
    public async Task TheParameterSensitivityFact_StillFillsItsCapWithRealOffenders_WhenPlansTheExactTestRejectsOutrankThem()
    {
        await SeedClockAsync(Clock.EasternZone);
        await SeedCapBoundaryPlansAsync(winter: true, decoys: ParameterSensitivityFactCap, realOffenders: ParameterSensitivityFactCap + 2);

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context(winter: true));

        var fact = Assert.Single(facts, f => f.Key == "PARAMETER_SENSITIVITY");
        Assert.Equal(ParameterSensitivityFactCap, fact.Metadata["offender_count"]);
        Assert.Equal((double)RealMaxWorkerUs / RealMinWorkerUs, fact.Metadata["worst_ratio"]);
        Assert.Equal(RealMinWorkerUs, fact.Metadata["worst_min_worker_us"]);
        Assert.Equal(RealMaxWorkerUs, fact.Metadata["worst_max_worker_us"]);
    }

    /// <summary>The same boundary for the drill-down's cap of five, which was also a LIMIT in SQL.</summary>
    [Fact]
    public async Task TheParameterSensitiveDrillDown_StillFillsItsCapWithRealOffenders_WhenPlansTheExactTestRejectsOutrankThem()
    {
        await SeedClockAsync(Clock.EasternZone);
        await SeedCapBoundaryPlansAsync(winter: true, decoys: ParameterSensitiveDrillDownCap, realOffenders: ParameterSensitiveDrillDownCap + 2);

        var finding = new AnalysisFinding
        {
            RootFactKey = "PARAMETER_SENSITIVITY",
            StoryPath = "PARAMETER_SENSITIVITY",
            PathKeys = ["PARAMETER_SENSITIVITY"],
            Severity = 1.0,
        };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], Context(winter: true));

        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown!.TryGetValue("parameter_sensitive_queries", out var raw));
        var hashes = JsonSerializer.SerializeToElement(raw).EnumerateArray()
            .Select(r => r.GetProperty("query_hash").GetString())
            .ToArray();
        Assert.Equal(ParameterSensitiveDrillDownCap, hashes.Length);
        Assert.All(hashes, h => Assert.StartsWith("0xREAL_", h, StringComparison.Ordinal));
    }

    /* ── The one-hour margin on the SQL first filter ── */

    [Theory]
    [InlineData(2026, 1, 15)]
    [InlineData(2026, 7, 15)]
    public void TheRoughFilterBound_IsTheWindowStartPlusExactlyOneHour(int year, int month, int day)
    {
        var start = new DateTime(year, month, day, 12, 0, 0, DateTimeKind.Unspecified);

        Assert.Equal(60, PlanCreationClock.RoughFilterMarginMinutes);
        Assert.Equal(start.AddMinutes(60), PlanCreationClock.RoughBound(start));
    }

    /// <summary>
    /// The margin's own boundary, in the direction the winter-rows arms cannot reach: the newest properties row is
    /// WINTER (-300, collected in January) and the plan is a SUMMER row stored at -240. The newest offset reads a plan
    /// compiled at the very instant the window opens a whole hour late, on the first filter's edge; a margin of even
    /// 59 minutes loses a plan the exact test keeps.
    /// </summary>
    [Fact]
    public async Task TheFirstFilterMargin_KeepsAPlanCompiledAtTheWindowStart_WhenTheNewestSnapshotIsWinterAndThePlanIsSummer()
    {
        await SeedClockAsync(Clock.EasternZoneWinterSnapshot);
        var t = Anchor(winter: false);
        await SeedPlanCacheRowAsync("0xQH_at_0m", t.AddMinutes(StampedOffset(winter: false, Clock.EasternZoneWinterSnapshot)), t, 0);

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context(winter: false));

        var fact = Assert.Single(facts, f => f.Key == "PARAMETER_SENSITIVITY");
        Assert.Equal(1, fact.Metadata["offender_count"]);
    }

    /* ── Site 1 again: a zone east of UTC, across the autumn change ── */

    /// <summary>
    /// W. Europe falls back at 01:00Z on 2026-10-25, so local 02:00 to 02:59 happens twice, and the span below holds the
    /// change itself. A stored server-local time cannot say which occurrence it was, so the conversion takes the FIRST
    /// (+120), as <see cref="ServerClock.ToUtc"/> does for the viewer: the two repeated-hour lines read 00:15Z and
    /// 00:45Z and both stay in the span. Two lines just outside it pass the SQL's rough first filter and only the
    /// exact span drops them; one of them (03:10 local, +60 after the change) is INSIDE the span on the summer offset
    /// the newest snapshot holds.
    /// </summary>
    [Fact]
    public async Task TheTraceAnchor_InAZoneEastOfUtc_ReadsALineInTheRepeatedHourAsItsFirstOccurrence()
    {
        await SeedPropertiesAsync(NewestPropertiesTime, 120, WesternEuropeWindowsId);
        var previousCapture = new DateTime(2026, 10, 24, 23, 30, 0, DateTimeKind.Unspecified);
        var changeTime = new DateTime(2026, 10, 25, 1, 15, 0, DateTimeKind.Unspecified);
        var repeatedHour = new DateTime(2026, 10, 25, 2, 0, 0, DateTimeKind.Unspecified);

        await SeedReconfigureLineAtLocalAsync(repeatedHour.AddMinutes(15), changeTime, Maxdop, 0, 8);
        await SeedReconfigureLineAtLocalAsync(repeatedHour.AddMinutes(45), changeTime, CostThreshold, 5, 50);
        await SeedReconfigureLineAtLocalAsync(repeatedHour.AddMinutes(70), changeTime, ServerMemory, 1024, 2048);
        await SeedReconfigureLineAtLocalAsync(repeatedHour.AddMinutes(-75), changeTime, BackupCompression, 0, 1);

        var change = new ConfigChangeAttribution.ChangeEvent(changeTime, previousCapture,
        [
            new ConfigChangeAttribution.SettingChange(Maxdop, 0, 8, 0, 8, false),
            new ConfigChangeAttribution.SettingChange(CostThreshold, 5, 50, 5, 50, false),
            new ConfigChangeAttribution.SettingChange(ServerMemory, 1024, 2048, 1024, 2048, false),
            new ConfigChangeAttribution.SettingChange(BackupCompression, 0, 1, 0, 1, false),
        ]);

        var anchor = await new AnalysisService(_duckDb).ResolveTraceAnchorAsync(Context(winter: true), change);

        Assert.NotNull(anchor);
        Assert.Equal([CostThreshold, Maxdop], anchor!.Matched.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray());
        Assert.Equal(new DateTime(2026, 10, 25, 0, 15, 0, DateTimeKind.Unspecified), anchor.Matched[Maxdop].ChangedAtUtc);
        Assert.Equal(new DateTime(2026, 10, 25, 0, 45, 0, DateTimeKind.Unspecified), anchor.Matched[CostThreshold].ChangedAtUtc);
        Assert.Equal(new DateTime(2026, 10, 25, 0, 45, 0, DateTimeKind.Unspecified), anchor.ChangedAtUtc);
    }

    /* ── The SQL: the exact conversion is in C#, not in the projection ── */

    [Fact]
    public void TheThreeCreationTimeReads_ReturnTheZoneBesideTheRawColumn_AndConvertInCSharp()
    {
        var fact = File.ReadAllText(RepoPath("Lite/Analysis/DuckDbFactCollector.QueryPerf.cs"));
        var drill = File.ReadAllText(RepoPath("Lite/Analysis/DrillDownCollector.Queries.cs"));

        /* The svr subquery reads the zone from the SAME newest row as the offset: BaselineProvider.ServerClockSql's WHERE and ORDER BY. */
        var zoneRead = new Regex(
            @"SELECT\s+utc_offset_minutes,\s*time_zone_id\s+FROM\s+v_server_properties\s+WHERE\s+server_id\s*=\s*\$1\s+AND\s+utc_offset_minutes\s+IS\s+NOT\s+NULL\s+ORDER\s+BY\s+collection_time\s+DESC\s+LIMIT\s+1",
            RegexOptions.IgnoreCase);
        Assert.Single(zoneRead.Matches(fact));
        Assert.Equal(2, zoneRead.Matches(drill).Count);

        /* The subtraction stays only as the rough first filter (its bound opened by an hour); what the reads
           RETURN is the raw creation_time and the zone, and the exact test is the clock's, in C#. */
        foreach (var source in new[] { fact, drill })
        {
            Assert.Contains("PlanCreationClock.RoughBound(", source, StringComparison.Ordinal);
            Assert.Contains("PlanCreationClock.CompiledBeforeWindow(", source, StringComparison.Ordinal);
        }

        var planCreation = File.ReadAllText(RepoPath("Lite/Analysis/PlanCreationClock.cs"));
        Assert.Contains("ServerClock.Resolve(", planCreation, StringComparison.Ordinal);

        var jobs = File.ReadAllText(RepoPath("Lite/Services/LocalDataService.RunningJobs.cs"));
        Assert.Matches(zoneRead, jobs);
        Assert.Contains("ServerClock.Resolve(", jobs, StringComparison.Ordinal);

        var analysis = File.ReadAllText(RepoPath("Lite/Analysis/AnalysisService.cs"));
        Assert.DoesNotContain("ServerUtcOffsetForAttributionSql", analysis, StringComparison.Ordinal);
        Assert.Contains("BaselineProvider.ReadServerClockAsync(", analysis, StringComparison.Ordinal);
    }

    /* ── plants ── */

    private static string RepoPath(string relative, [CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", relative));

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task ExecuteAsync(string sql, params object?[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var value in values)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = value ?? DBNull.Value });
        }
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// The server's newest properties row: collected in SUMMER (offset -240) after every row the arms plant, or, for
    /// <see cref="Clock.EasternZoneWinterSnapshot"/>, in WINTER (offset -300) months before the rows the arm plants.
    /// The zone rides on it for <see cref="Clock.EasternZone"/> and <see cref="Clock.EasternZoneWinterSnapshot"/>;
    /// <see cref="Clock.NoRow"/> plants nothing.
    /// </summary>
    private async Task SeedClockAsync(Clock clock)
    {
        if (clock == Clock.NoRow)
        {
            return;
        }

        var winterSnapshot = clock == Clock.EasternZoneWinterSnapshot;
        await SeedPropertiesAsync(
            winterSnapshot ? WinterSnapshotTime : NewestPropertiesTime,
            winterSnapshot ? -300 : -240,
            clock is Clock.EasternZone or Clock.EasternZoneWinterSnapshot ? EasternWindowsId : null);
    }

    /// <summary>One <c>server_properties</c> row: the offset the server reported and, from SQL Server 2022, its zone.</summary>
    private async Task SeedPropertiesAsync(DateTime collectionTime, int utcOffsetMinutes, string? timeZoneId)
    {
        await ExecuteAsync(@"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level,
     engine_edition, utc_offset_minutes, time_zone_id)
VALUES ($1, $2, $3, $4, 'Enterprise Edition', '16.0.4085.2', 'RTM', 3, $5, $6)",
            _nextId++, collectionTime, ServerId, ServerName, utcOffsetMinutes, timeZoneId);
    }

    /// <summary>
    /// Four plans that clear every floor of the detector, each compiled at a UTC instant relative to the
    /// window start and stored at the server's local clock as it really read then. Two compile before the
    /// window (1 and 30 minutes) and two after it (1 and 30 minutes): a conversion an hour off flips the
    /// in-window ones into the count.
    /// </summary>
    private async Task SeedPlansAsync(bool winter, Clock clock)
    {
        var t = Anchor(winter);
        var offset = StampedOffset(winter, clock);
        var plans = new (string Label, int MinutesFromWindowStart)[] { ("pre_30m", -30), ("pre_1m", -1), ("in_1m", 1), ("in_30m", 30) };
        for (var i = 0; i < plans.Length; i++)
        {
            await SeedPlanCacheRowAsync("0xQH_" + plans[i].Label, t.AddMinutes(plans[i].MinutesFromWindowStart).AddMinutes(offset), t, i);
        }
    }

    private async Task SeedPlanCacheRowAsync(
        string queryHash, DateTime creationLocal, DateTime windowStart, int index,
        long minWorkerTime = RealMinWorkerUs, long maxWorkerTime = RealMaxWorkerUs)
    {
        await ExecuteAsync(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     creation_time, execution_count, min_worker_time, max_worker_time, min_grant_kb, max_grant_kb,
     min_spills, max_spills, query_text, delta_execution_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 5000, $10, $11, 1024, 1048576, 0, 50, $9, 500)",
            _nextId++, windowStart.AddMinutes(45 + index), ServerId, ServerName, Db, queryHash, "0xPH_" + queryHash,
            creationLocal, "SELECT * FROM dbo.Synth_" + queryHash, minWorkerTime, maxWorkerTime);
    }

    /// <summary>
    /// The plans of a cap arm, stored at the server's local clock as it really read then. Each of the
    /// <paramref name="realOffenders"/> was compiled five hours before the window opened; each of the
    /// <paramref name="decoys"/> was compiled 30 minutes AFTER it opened and has a higher worker ratio, so it sorts
    /// first. Against a newest properties row from the other side of a daylight saving change a decoy reads as
    /// compiled 30 minutes BEFORE the window, which passes the SQL's rough first filter; only the exact test rejects it.
    /// </summary>
    private async Task SeedCapBoundaryPlansAsync(bool winter, int decoys, int realOffenders)
    {
        var t = Anchor(winter);
        var offset = StampedOffset(winter, Clock.EasternZone);
        for (var i = 0; i < decoys; i++)
        {
            await SeedPlanCacheRowAsync($"0xDECOY_{i:D2}", t.AddMinutes(30).AddMinutes(offset), t, i, DecoyMinWorkerUs, DecoyMaxWorkerUs);
        }

        for (var i = 0; i < realOffenders; i++)
        {
            await SeedPlanCacheRowAsync($"0xREAL_{i:D2}", t.AddHours(-5).AddMinutes(offset), t, decoys + i);
        }
    }

    /// <summary>
    /// One regressed query (id <paramref name="queryId"/>, hash <c>0xQH{id}</c>): a cheap plan that ran five days
    /// before the window and a plan three times as costly still running at its end, two intervals each.
    /// </summary>
    private async Task SeedRegressionAsync(long queryId, DateTime windowStart)
    {
        var windowEnd = windowStart.AddHours(4);
        await SeedQueryStorePlanAsync(queryId, queryId * 10 + 1, "0xCHEAP" + queryId, 100_000, 1, windowStart.AddDays(-5), windowEnd);
        await SeedQueryStorePlanAsync(queryId, queryId * 10 + 2, "0xCOSTLY" + queryId, 300_000, 11, windowEnd, windowEnd);
    }

    private async Task SeedQueryStorePlanAsync(
        long queryId, long planId, string planHash, long cpuUs, long firstIntervalId, DateTime lastExec, DateTime windowEnd)
    {
        for (var interval = 0; interval < 2; interval++)
        {
            var firstExec = lastExec.AddHours(-(2 - interval));
            for (var collection = 1; collection <= 2; collection++)
            {
                await ExecuteAsync(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     query_plan_hash, is_forced_plan, force_failure_count,
     runtime_stats_interval_id, interval_start_time_utc, replica_role)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'Regular', $8, $9, $10, $11, $12, $13, $14, false, 0, $15, $16, NULL)",
                    _nextId++, windowEnd.AddMinutes(-10 + collection), ServerId, ServerName, Db, queryId, planId,
                    firstExec, lastExec.AddHours(-(1 - interval)), "0xQH" + queryId, 50L * collection, cpuUs, cpuUs + 20_000,
                    planHash, firstIntervalId + interval, firstExec);
            }
        }
    }

    private async Task SeedRunningJobAsync(string jobName, DateTime startLocal)
    {
        await ExecuteAsync(@"
INSERT INTO running_jobs (collection_time, server_id, server_name, job_name, job_id, job_enabled, start_time,
    current_duration_seconds, avg_duration_seconds, p95_duration_seconds, successful_run_count, is_running_long, percent_of_average)
VALUES ($1, $2, $3, $4, $5, true, $6, 3600, 900, 1200, 42, true, 400.0)",
            NewestPropertiesTime, ServerId, ServerName, jobName, "job-" + jobName, startLocal);
    }

    /// <summary>
    /// One stored msg 15457 line as the collector writes it, its <c>event_time</c> at the SERVER's local wall
    /// clock: the UTC instant plus the offset the server was really at then.
    /// </summary>
    private async Task SeedReconfigureLineAsync(DateTime changedAtUtc, int stampedOffset, string option, long oldValue, long newValue)
    {
        var local = changedAtUtc.AddMinutes(stampedOffset);
        await SeedReconfigureLineAtLocalAsync(local, changedAtUtc.AddMinutes(1), option, oldValue, newValue);
    }

    /// <summary>The same line at a server wall-clock time given as it is stored (a repeated hour has no single UTC instant to start from).</summary>
    private async Task SeedReconfigureLineAtLocalAsync(DateTime local, DateTime collectionTime, string option, long oldValue, long newValue)
    {
        await ExecuteAsync(@"
INSERT INTO default_trace_events
    (default_trace_event_id, collection_time, server_id, server_name,
     event_time, event_name, event_class, spid, database_id, database_name, error_number, severity, text_data)
VALUES ($1, $2, $3, $4, $5, 'ErrorLog', 22, 95, 1, 'master', 15457, 10, $6)",
            _nextId++, collectionTime, ServerId, ServerName, local,
            $"{local:yyyy-MM-dd HH:mm:ss.ff} spid95      Configuration option '{option}' changed from {oldValue} to {newValue}. Run the RECONFIGURE statement to install.");
    }
}
