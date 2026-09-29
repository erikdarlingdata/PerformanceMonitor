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
    private const string Maxdop = "max degree of parallelism";
    private const string CostThreshold = "cost threshold for parallelism";
    private const string ServerMemory = "max server memory (MB)";

    /// <summary>How the server reports its clock.</summary>
    public enum Clock
    {
        /// <summary>Newest properties row: offset -240 (summer) and the Eastern zone.</summary>
        EasternZone,

        /// <summary>Newest properties row: offset -240 and NO zone (a pre-2022 engine).</summary>
        OffsetOnly,

        /// <summary>No properties row at all: UTC.</summary>
        NoRow,
    }

    private static readonly DateTime NewestPropertiesTime = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified);

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
        Clock.EasternZone => winter ? -300 : -240,
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
    /// The server's newest properties row, collected in SUMMER (offset -240) after every row the arms plant.
    /// The zone rides on it only for <see cref="Clock.EasternZone"/>; <see cref="Clock.NoRow"/> plants nothing.
    /// </summary>
    private async Task SeedClockAsync(Clock clock)
    {
        if (clock == Clock.NoRow)
        {
            return;
        }

        await ExecuteAsync(@"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level,
     engine_edition, utc_offset_minutes, time_zone_id)
VALUES ($1, $2, $3, $4, 'Enterprise Edition', '16.0.4085.2', 'RTM', 3, -240, $5)",
            _nextId++, NewestPropertiesTime, ServerId, ServerName, clock == Clock.EasternZone ? EasternWindowsId : null);
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

    private async Task SeedPlanCacheRowAsync(string queryHash, DateTime creationLocal, DateTime windowStart, int index)
    {
        await ExecuteAsync(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     creation_time, execution_count, min_worker_time, max_worker_time, min_grant_kb, max_grant_kb,
     min_spills, max_spills, query_text, delta_execution_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 5000, 20000, 20000000, 1024, 1048576, 0, 50, $9, 500)",
            _nextId++, windowStart.AddMinutes(45 + index), ServerId, ServerName, Db, queryHash, "0xPH_" + queryHash,
            creationLocal, "SELECT * FROM dbo.Synth_" + queryHash);
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
        await ExecuteAsync(@"
INSERT INTO default_trace_events
    (default_trace_event_id, collection_time, server_id, server_name,
     event_time, event_name, event_class, spid, database_id, database_name, error_number, severity, text_data)
VALUES ($1, $2, $3, $4, $5, 'ErrorLog', 22, 95, 1, 'master', 15457, 10, $6)",
            _nextId++, changedAtUtc.AddMinutes(1), ServerId, ServerName, local,
            $"{local:yyyy-MM-dd HH:mm:ss.ff} spid95      Configuration option '{option}' changed from {oldValue} to {newValue}. Run the RECONFIGURE statement to install.");
    }
}
