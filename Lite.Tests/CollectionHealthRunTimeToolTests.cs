/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.DependencyInjection;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4938: <c>get_collection_health</c> shows a collector's run time. A collector with a run time carries
/// <c>run_at</c> (24-hour <c>HH:MM</c> on the monitored server's clock) and <c>next_run_utc</c> (the next time it is due,
/// UTC, from the shared rules). A collector without one carries null for both on a full row, and a compact row leaves
/// the keys out. The health band is read from the shipped cadence and a run time does not move it, so a day that was
/// skipped reads STALE before the next run. A server with no clock yet reads the run time as UTC. These are the same
/// two fields Darling's tool publishes. A day that was skipped also carries a <c>run_time_note</c> that says so, in Lite's
/// terms (it collects only while it is open): a full row always has the key, a partial row has it only for a collector with a
/// run time, and a compact row never does. The app's own registration hands the tool the schedule that holds the run times.
/// </summary>
[Trait("Reads", "Darling")]
public sealed class CollectionHealthRunTimeToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "RunTimeHealthSrv";
    private const string Daily = "index_object_stats";
    private const string Frequent = "wait_stats";
    private const string FiveMinutes = "running_jobs";
    private const int TwoAm = 120;

    /* Noon UTC. A daily slot at 02:00 plus a spread under an hour has passed by ten hours, outside its grace. */
    private static readonly DateTime Now = new(2026, 6, 10, 12, 0, 0, DateTimeKind.Utc);

    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly ServerConnection _server;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public CollectionHealthRunTimeToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-collhealthruntime-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);
        _server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = ServerName, IsEnabled = true };
        _serverManager.AddServer(_server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(_server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    /* A run time half a day away from now, on UTC (the test server has no clock row), so the slot is never inside the
       hour the collector would read as due now, whatever hour the test runs in. */
    private static (int Minute, string Text) RunTimeAwayFromNow()
    {
        var minute = (DateTime.UtcNow.Hour + 12) % 24 * 60;
        return (minute, CollectorRunTime.Format(minute));
    }

    private McpCollectorRunTimes RunTimes(params (string Name, int Frequency, string? RunAt, bool Enabled)[] collectors)
    {
        var scheduler = new ScheduleManager(_configDir);
        scheduler.SetScheduleForServer(_server.Id, collectors.Select(c => new CollectorSchedule
        {
            Name = c.Name,
            Enabled = c.Enabled,
            FrequencyMinutes = c.Frequency,
            RetentionDays = 30,
            RunAt = c.RunAt,
        }).ToList());
        return new McpCollectorRunTimes(scheduler, _serverManager);
    }

    private async Task<(Dictionary<string, JsonElement> Rows, JsonElement Root)> CallAsync(McpCollectorRunTimes? runTimes, bool fullDetail)
    {
        var json = await McpHealthTools.GetCollectionHealth(
            new LocalDataService(_duckDb), _serverManager, ServerName, full_detail: fullDetail, runTimes: runTimes);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement.Clone();
        var rows = root.GetProperty("collectors").EnumerateArray()
            .ToDictionary(e => e.GetProperty("collector").GetString()!, e => e.Clone());
        return (rows, root);
    }

    private static DateTime ParseUtc(JsonElement value) =>
        DateTime.Parse(value.GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

    [Fact]
    public async Task ACollectorWithARunTime_ShowsRunAtAndNextRunUtc_AndACollectorWithoutOneShowsNullForBoth()
    {
        var now = DateTime.UtcNow;
        var (minute, text) = RunTimeAwayFromNow();
        await SeedLogAsync(Daily, now.AddMinutes(-1), "SUCCESS", 5000, 10);
        await SeedLogAsync(Frequent, now.AddMinutes(-1), "SUCCESS", 80, 40);
        var runTimes = RunTimes((Daily, 1440, text, true), (Frequent, 1, null, true));

        var (rows, root) = await CallAsync(runTimes, fullDetail: true);

        var daily = rows[Daily];
        Assert.Equal(text, daily.GetProperty("run_at").GetString());
        var next = ParseUtc(daily.GetProperty("next_run_utc"));
        Assert.True(next > now, $"the next run {next:o} must be ahead of {now:o}");
        Assert.True(next <= now + CollectorRunTime.MaxStampAhead(1440), $"the next run {next:o} is more than a day and the spread away");

        /* No server_properties row, so the clock is unknown and the run time reads as UTC: the time plus the server's fixed spread. */
        Assert.Equal(TimeSpan.FromTicks((TimeSpan.FromMinutes(minute) + CollectorRunTime.Spread(_serverId)).Ticks % TimeSpan.TicksPerDay), next.TimeOfDay);

        Assert.Equal(JsonValueKind.Null, rows[Frequent].GetProperty("run_at").ValueKind);
        Assert.Equal(JsonValueKind.Null, rows[Frequent].GetProperty("next_run_utc").ValueKind);

        /* A full row always carries the note, null here: nothing was skipped, and a collector with no run time cannot skip a day. */
        Assert.Equal(JsonValueKind.Null, daily.GetProperty("run_time_note").ValueKind);
        Assert.Equal(JsonValueKind.Null, rows[Frequent].GetProperty("run_time_note").ValueKind);

        /* The heaviest-collectors list names the run time beside the shipped cadence, and null for a collector without one. */
        var heaviest = root.GetProperty("sweep_pressure").GetProperty("heaviest_collectors").EnumerateArray()
            .ToDictionary(e => e.GetProperty("collector").GetString()!, e => e.Clone());
        Assert.Equal(text, heaviest[Daily].GetProperty("run_at").GetString());
        Assert.Equal(JsonValueKind.Null, heaviest[Frequent].GetProperty("run_at").ValueKind);
    }

    [Fact]
    public void TheRunTimeIsOnTheServersClock_AndAServerWithNoClockReadsItAsUtc()
    {
        var settings = new[] { new ScheduleManager.RunTimeSetting(Daily, 120, 1440, true) };
        var neverRan = Array.Empty<CollectorHealthRow>();
        var now = new DateTime(2026, 7, 15, 12, 0, 0, DateTimeKind.Utc);

        var plain = McpCollectorRunTimes.Compute(_serverId, settings, neverRan, clock: null, now)[Daily];
        var fiveBehind = McpCollectorRunTimes.Compute(_serverId, settings, neverRan, ServerClock.Resolve(null, -300), now)[Daily];

        Assert.Equal("02:00", plain.RunAt);
        Assert.Equal("02:00", fiveBehind.RunAt);
        Assert.Equal(TimeSpan.FromHours(2) + CollectorRunTime.Spread(_serverId), plain.NextRunUtc!.Value.TimeOfDay);

        /* Five hours behind UTC, a fixed offset: its 02:00 is 07:00 UTC. */
        Assert.Equal(TimeSpan.FromHours(7) + CollectorRunTime.Spread(_serverId), fiveBehind.NextRunUtc!.Value.TimeOfDay);
    }

    [Fact]
    public async Task ADisabledCollectorWithARunTime_ShowsItsRunTime_AndNoNextRun()
    {
        var (_, text) = RunTimeAwayFromNow();
        await SeedLogAsync(Daily, DateTime.UtcNow.AddMinutes(-1), "SUCCESS", 5000, 10);

        var (rows, _) = await CallAsync(RunTimes((Daily, 1440, text, false)), fullDetail: true);

        Assert.Equal(text, rows[Daily].GetProperty("run_at").GetString());
        Assert.Equal(JsonValueKind.Null, rows[Daily].GetProperty("next_run_utc").ValueKind);
        Assert.Equal(JsonValueKind.Null, rows[Daily].GetProperty("run_time_note").ValueKind);
    }

    [Fact]
    public async Task ACompactRow_CarriesTheRunTimeOnlyWhenTheCollectorHasOne()
    {
        var now = DateTime.UtcNow;
        var (_, text) = RunTimeAwayFromNow();
        await SeedLogAsync(Daily, now.AddMinutes(-30), "SUCCESS", 5000, 10);
        await SeedLogAsync(Frequent, now.AddMinutes(-1), "SUCCESS", 80, 40);

        var (rows, _) = await CallAsync(RunTimes((Daily, 1440, text, true), (Frequent, 1, null, true)), fullDetail: false);

        Assert.True(rows[Daily].GetProperty("compact").GetBoolean());
        Assert.Equal(text, rows[Daily].GetProperty("run_at").GetString());
        Assert.Equal(JsonValueKind.String, rows[Daily].GetProperty("next_run_utc").ValueKind);

        Assert.True(rows[Frequent].GetProperty("compact").GetBoolean());
        Assert.False(rows[Frequent].TryGetProperty("run_at", out _), "a compact row without a run time stays seven fields");
        Assert.False(rows[Frequent].TryGetProperty("next_run_utc", out _));

        /* A skipped day reads STALE or worse, so a compact row never carries the note, with a run time or without one. */
        Assert.False(rows[Daily].TryGetProperty("run_time_note", out _));
        Assert.False(rows[Frequent].TryGetProperty("run_time_note", out _));
    }

    [Fact]
    public async Task ASkippedDay_CrossesTheStaleLineBeforeTheNextRun_AndTheBandIsTheSameWithoutARunTime()
    {
        var now = DateTime.UtcNow;
        var (_, text) = RunTimeAwayFromNow();

        /* A daily collector that last ran 40 hours ago is past the 36-hour stale line: a day was skipped. The next run is
           still hours away, so the row reads STALE before it. The run time changes nothing about the band. */
        await SeedLogAsync(Daily, now.AddHours(-40), "SUCCESS", 5000, 10);

        var (withRunTime, _) = await CallAsync(RunTimes((Daily, 1440, text, true)), fullDetail: true);
        var (without, _) = await CallAsync(runTimes: null, fullDetail: true);

        Assert.Equal("STALE", withRunTime[Daily].GetProperty("status").GetString());
        Assert.Equal(without[Daily].GetProperty("status").GetString(), withRunTime[Daily].GetProperty("status").GetString());
        Assert.Equal(JsonValueKind.Null, without[Daily].GetProperty("run_at").ValueKind);
        Assert.True(ParseUtc(withRunTime[Daily].GetProperty("next_run_utc")) > now.AddHours(1),
            "the skipped day's collector waits for its next slot, not for the read");

        /* The default shape of a stale row is the partial one: it carries the run time when the collector has one and
           leaves the keys out when it has none. */
        var (partial, _) = await CallAsync(RunTimes((Daily, 1440, text, true)), fullDetail: false);
        Assert.True(partial[Daily].GetProperty("partial_detail").GetBoolean());
        Assert.Equal(text, partial[Daily].GetProperty("run_at").GetString());
        Assert.Equal(JsonValueKind.String, partial[Daily].GetProperty("next_run_utc").ValueKind);

        var (partialWithout, _) = await CallAsync(runTimes: null, fullDetail: false);
        Assert.False(partialWithout[Daily].TryGetProperty("run_at", out _));
        Assert.False(partialWithout[Daily].TryGetProperty("next_run_utc", out _));
    }

    [Fact]
    public async Task ASkippedDay_CarriesANoteThatSaysWhyAndWhenTheNextRunIs_OnAFullRowAndAPartialOne()
    {
        var now = DateTime.UtcNow;
        var (_, text) = RunTimeAwayFromNow();

        /* The daily collector last ran 40 hours ago, so it missed a day and is past its 36-hour stale line, and its next slot
           is half a day away. The second collector, also on a run time, ran a minute ago: nothing was skipped there. */
        await SeedLogAsync(Daily, now.AddHours(-40), "SUCCESS", 5000, 10);
        await SeedLogAsync(FiveMinutes, now.AddMinutes(-1), "SUCCESS", 80, 40);
        var runTimes = RunTimes((Daily, 1440, text, true), (FiveMinutes, 1440, text, true));

        var (full, _) = await CallAsync(runTimes, fullDetail: true);
        var (partial, _) = await CallAsync(runTimes, fullDetail: false);

        Assert.True(partial[Daily].GetProperty("partial_detail").GetBoolean());
        foreach (var row in new[] { full[Daily], partial[Daily] })
        {
            Assert.Equal("STALE", row.GetProperty("status").GetString());
            var next = ParseUtc(row.GetProperty("next_run_utc"));
            Assert.True(next > now, "the next slot is still ahead of the stale reading");
            var note = row.GetProperty("run_time_note").GetString();
            Assert.NotNull(note);
            Assert.Contains("Skipped day", note, StringComparison.Ordinal);
            Assert.Contains("36-hour stale line", note, StringComparison.Ordinal);
            Assert.Contains(next.ToString("u", CultureInfo.InvariantCulture), note, StringComparison.Ordinal);
            Assert.Contains("not replayed", note, StringComparison.Ordinal);

            /* In Lite's terms: it collects only while it is open. The slot is named the way the row's run_at is. */
            Assert.Contains("only while it is open", note, StringComparison.Ordinal);
            Assert.Contains($"{text} slot", note, StringComparison.Ordinal);
        }

        /* It ran a minute ago: a null note on a full row, and the compact row it takes by default never carries one. */
        Assert.Equal(JsonValueKind.Null, full[FiveMinutes].GetProperty("run_time_note").ValueKind);
        Assert.True(partial[FiveMinutes].GetProperty("compact").GetBoolean());
        Assert.False(partial[FiveMinutes].TryGetProperty("run_time_note", out _));
    }

    [Fact]
    public async Task APartialRowCarriesTheRunTimeAndTheNoteOnlyWhenTheCollectorHasARunTime()
    {
        var now = DateTime.UtcNow;
        var (_, text) = RunTimeAwayFromNow();

        /* A success and then a failure for each collector: an error in the window fails the compact test, so both take the
           partial shape. */
        foreach (var collector in new[] { Daily, Frequent })
        {
            await SeedLogAsync(collector, now.AddMinutes(-2), "SUCCESS", 80, 40);
            await SeedLogAsync(collector, now.AddMinutes(-1), "ERROR", 80, 0);
        }

        var (rows, _) = await CallAsync(RunTimes((Daily, 1440, text, true), (Frequent, 1, null, true)), fullDetail: false);

        /* The partial row of a collector with a run time carries all three keys, the note null because nothing was skipped. A
           partial row for a collector with none has no key at all, so the shape costs a collector without a run time no bytes. */
        var daily = rows[Daily];
        Assert.True(daily.GetProperty("partial_detail").GetBoolean());
        Assert.Equal(text, daily.GetProperty("run_at").GetString());
        Assert.Equal(JsonValueKind.String, daily.GetProperty("next_run_utc").ValueKind);
        Assert.Equal(JsonValueKind.Null, daily.GetProperty("run_time_note").ValueKind);

        var minute = rows[Frequent];
        Assert.True(minute.GetProperty("partial_detail").GetBoolean());
        Assert.False(minute.TryGetProperty("run_at", out _));
        Assert.False(minute.TryGetProperty("next_run_utc", out _));
        Assert.False(minute.TryGetProperty("run_time_note", out _));
    }

    private static CollectorHealthRow HealthRow(string collector, DateTime? lastRun) => new()
    {
        CollectorName = collector,
        TotalRuns = 1,
        SuccessCount = 1,
        LastRunTime = lastRun,
        LastSuccessTime = lastRun,
    };

    private static DateTime Ago(double hours) => DateTime.SpecifyKind(Now.AddHours(-hours), DateTimeKind.Unspecified);

    private CollectorRunTimeReading ReadingAtNoon(string collector, int runAtMinute, int intervalMinutes, bool enabled, CollectorHealthRow row) =>
        McpCollectorRunTimes.Compute(
            _serverId, new[] { new ScheduleManager.RunTimeSetting(collector, runAtMinute, intervalMinutes, enabled) },
            new[] { row }, clock: null, Now)[collector];

    [Theory]
    [InlineData(1, false)]    // ran this morning: on time
    [InlineData(20, false)]   // ran yesterday: on time
    [InlineData(30, false)]   // a day late but still under the 36-hour stale line: not stale yet, so no note
    [InlineData(40, true)]    // a skipped day past the 36-hour stale line, with the next slot ahead
    [InlineData(60, true)]    // two days
    public void ASkippedDayCarriesANoteOnlyOncePastTheStaleLineWithTheNextSlotAhead(double hoursSinceLastRun, bool expectNote)
    {
        var reading = ReadingAtNoon(Daily, TwoAm, 1440, enabled: true, HealthRow(Daily, Ago(hoursSinceLastRun)));

        Assert.True(reading.NextRunUtc > Now, "the next slot is ahead");
        if (!expectNote)
        {
            Assert.Null(reading.SkippedDayNote);
            return;
        }

        Assert.NotNull(reading.SkippedDayNote);
        Assert.Contains("Skipped day", reading.SkippedDayNote, StringComparison.Ordinal);
        Assert.Contains("36-hour stale line", reading.SkippedDayNote, StringComparison.Ordinal);
        Assert.Contains($"no run for {hoursSinceLastRun:0.#} hours", reading.SkippedDayNote, StringComparison.Ordinal);
        Assert.Contains(reading.NextRunUtc!.Value.ToString("u", CultureInfo.InvariantCulture), reading.SkippedDayNote, StringComparison.Ordinal);
        Assert.Contains("not replayed", reading.SkippedDayNote, StringComparison.Ordinal);
        Assert.Contains("Lite collects only while it is open", reading.SkippedDayNote, StringComparison.Ordinal);
        Assert.Contains($"{CollectorRunTime.Format(TwoAm)} slot", reading.SkippedDayNote, StringComparison.Ordinal);
    }

    [Fact]
    public void ACollectorDueNowHasNoNote_BecauseItsRunIsAMomentAway()
    {
        /* A run time whose slot for this server passed about ten minutes ago: noon is inside its 60-minute grace. */
        var spreadMinutes = (int)CollectorRunTime.Spread(_serverId).TotalMinutes;
        var runAt = 12 * 60 - spreadMinutes - 10;

        var reading = ReadingAtNoon(Daily, runAt, 1440, enabled: true, HealthRow(Daily, Ago(40)));

        Assert.Equal(Now, reading.NextRunUtc);
        Assert.Null(reading.SkippedDayNote);
    }

    [Fact]
    public void ADisabledCollectorHasNoNote_BecauseNothingIsScheduled()
    {
        var reading = ReadingAtNoon(Daily, TwoAm, 1440, enabled: false, HealthRow(Daily, Ago(40)));

        Assert.Null(reading.NextRunUtc);
        Assert.Null(reading.SkippedDayNote);
    }

    [Fact]
    public void ADailyIntervalChosenOnAFasterShippedCadence_IsNotASkippedDayOnAnOrdinaryDay()
    {
        /* running_jobs ships on a five-minute cadence, so the health band calls it stale after four hours. Moved to a daily
           run time, ten hours since its last run is just a day's wait, not a skipped slot. */
        var ordinary = ReadingAtNoon(FiveMinutes, TwoAm, 1440, enabled: true, HealthRow(FiveMinutes, Ago(10)));
        Assert.Null(ordinary.SkippedDayNote);

        /* A day and a quarter is past one interval plus its grace, so a slot really was missed. */
        var skipped = ReadingAtNoon(FiveMinutes, TwoAm, 1440, enabled: true, HealthRow(FiveMinutes, Ago(30)));
        Assert.NotNull(skipped.SkippedDayNote);
    }

    [Fact]
    public void TheRunTimeIsNeverABandInput()
    {
        /* HealthStatus reads the real clock, so this row and the call use it too. */
        var realNow = DateTime.UtcNow;
        var row = HealthRow(Daily, DateTime.SpecifyKind(realNow.AddHours(-40), DateTimeKind.Unspecified));
        var bandBefore = row.HealthStatus;

        _ = McpCollectorRunTimes.Compute(
            _serverId, new[] { new ScheduleManager.RunTimeSetting(Daily, TwoAm, 1440, true) }, new[] { row }, clock: null, realNow);

        Assert.Equal("STALE", bandBefore);
        Assert.Equal(bandBefore, row.HealthStatus);
    }

    /* #4938: the tool takes its run-time source as an optional service parameter, so a host that registers nothing does not
       fail: every collector just reads as having no run time. Everything above hands the source in by hand. This one goes
       through the registration the host itself runs at start-up. */
    [Fact]
    public async Task TheHostsOwnRegistration_HandsTheToolTheSchedule_SoARunTimeSetThereReachesTheDueRule()
    {
        var now = DateTime.UtcNow;
        var (minute, text) = RunTimeAwayFromNow();
        var lastRun = now.AddMinutes(-1);
        await SeedLogAsync(Daily, lastRun, "SUCCESS", 5000, 10);

        /* The schedule the app holds, with a run time on the daily collector. */
        var schedules = new ScheduleManager(_configDir);
        schedules.SetScheduleForServer(_server.Id, new List<CollectorSchedule>
        {
            new() { Name = Daily, Enabled = true, FrequencyMinutes = 1440, RetentionDays = 30, RunAt = text },
        });

        var services = new ServiceCollection();
        McpHostService.RegisterCollectorRunTimes(services, schedules, _serverManager);
        using var provider = services.BuildServiceProvider();
        var runTimes = provider.GetRequiredService<McpCollectorRunTimes>();

        var (rows, _) = await CallAsync(runTimes, fullDetail: true);

        Assert.Equal(text, rows[Daily].GetProperty("run_at").GetString());
        var expectedNext = CollectorRunTime.NextDue(now, lastRun, minute, 1440, _serverId, CollectorRunTime.LocalIsUtc);
        Assert.Equal(expectedNext, ParseUtc(rows[Daily].GetProperty("next_run_utc")));
    }

    /* The two links the runtime test above cannot reach: the host calls that one registration method (and nothing else
       builds the service), and the app hands the host the schedule it holds. */
    [Fact]
    public void TheHostRegistersTheRunTimeServiceOnlyThroughItsRegistrationMethod_AndTheAppHandsTheHostItsSchedule()
    {
        var host = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Mcp", "McpHostService.cs"));
        Assert.Contains("RegisterCollectorRunTimes(builder.Services, _scheduleManager, _serverManager);", host, StringComparison.Ordinal);
        Assert.Equal(1, Regex.Count(host, @"new McpCollectorRunTimes\("));

        var app = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "MainWindow.xaml.cs"));
        Assert.Matches(@"new McpHostService\([^;]*,\s*_scheduleManager\)", app);
    }

    /* The shared run-time paragraph names run_time_note for both apps: it is no longer scoped to Darling, and the row-shape
       rule is the one both tools follow. */
    [Fact]
    public void BothAppsDescriptions_NameTheNoteForBothApps_AndGiveTheSameRowShapeRule()
    {
        var files = new[]
        {
            Path.Combine(RepoRoot(), "Lite", "Mcp", "McpHealthTools.cs"),
            Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs"),
        };

        foreach (var file in files)
        {
            var source = File.ReadAllText(file);
            Assert.Contains("run_time_note says so: how long the collector has not run, the stale line it is past, and when the next run is due.", source, StringComparison.Ordinal);
            Assert.Contains("On Lite it also names the slot, in the form run_at uses, and says Lite collects only while it is open.", source, StringComparison.Ordinal);
            Assert.Contains("A full row always carries run_at, next_run_utc and run_time_note, each null when the collector has no run time.", source, StringComparison.Ordinal);
            Assert.Contains("A partial row carries run_time_note under the same condition, and a compact row never carries it, because a skipped day is not healthy.", source, StringComparison.Ordinal);
            Assert.DoesNotContain("Lite has no such note", source, StringComparison.Ordinal);
            Assert.DoesNotContain("Darling also says so in run_time_note", source, StringComparison.Ordinal);
            Assert.DoesNotContain("On Darling a full row always carries run_time_note", source, StringComparison.Ordinal);
        }
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));

    private async Task SeedLogAsync(string collector, DateTime collectionTimeUtc, string status, double durationMs, int? rowsCollected)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, $8, $9, $10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = durationMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rowsCollected ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = durationMs * 0.8 });
        cmd.Parameters.Add(new DuckDBParameter { Value = durationMs * 0.2 });
        await cmd.ExecuteNonQueryAsync();
    }
}
