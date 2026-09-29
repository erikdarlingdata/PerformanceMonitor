/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The live tests here go through
   CollectionHealthAggregateTests.OpenStoreAsync, which reaches DARLING_TEST_PG only to CREATE and DROP its own
   database through ScratchPostgres and stops that database's TimescaleDB scheduler. */

/// <summary>
/// #4812: the fleet overview, <c>get_fleet_overview</c> and the Overview cards band a collector that lost half
/// or more of its databases WARNING, the way its own Collection Health tab does. The per-server reads pass the
/// newest run's note (<c>PartialDatabaseFailureNote</c>) to the shared banding; the fleet reads read the hourly
/// rollup, which kept no note, so they banded the same collector Healthy. The rollup now keeps the newest run's
/// note per hour, both raw fleet reads select it, the composer re-aggregates it, and both fleet mappers carry
/// it into the banding.
/// </summary>
public class FleetPartialDatabaseFailureBandTests
{
    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));

    private static string Source(string relative) => File.ReadAllText(Path.Combine(RepoRoot(), relative));

    private static string Flat(string sql) => Regex.Replace(Regex.Replace(sql, @"--[^\r\n]*", string.Empty), @"\s+", " ");

    private static string Note(int failed, int total) =>
        string.Format(CultureInfo.InvariantCulture, PartialDatabaseFailureNote.Format, failed, total, "a, b, c", "boom");

    /* ───────────────────────────── the SQL and source shape ───────────────────────────── */

    [Fact]
    public void RollupDefinition_KeepsTheNewestRunNote_AsLastOfNoteByCollectionTime()
    {
        var cagg = Flat(TimescaleSupport.CreateCollectionHealthHourlySql);

        Assert.Contains(
            $"last(CASE WHEN status = 'SUCCESS' AND error_message LIKE '%{PartialDatabaseFailureNote.Marker}%' THEN error_message END, collection_time) AS latest_run_note",
            cagg, StringComparison.Ordinal);
        Assert.DoesNotContain("array_agg", cagg, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("DarlingFleetReader.FleetCollectionHealthSql")]
    [InlineData("ViewerDataService.FleetCollectionHealthByServerSql")]
    public void BothRawFleetReads_SelectTheNewestRunNote_WithPlainAggregatesOnly(string which)
    {
        var sql = which == "DarlingFleetReader.FleetCollectionHealthSql"
            ? DarlingFleetReader.FleetCollectionHealthSql
            : ViewerDataService.FleetCollectionHealthByServerSql;
        var flat = Flat(sql);

        Assert.Contains(Flat(CollectionHealthRollupSupport.LatestRunNoteRawSql), flat, StringComparison.Ordinal);
        Assert.Contains("AS latest_run_note", flat, StringComparison.Ordinal);
        /* An ordered aggregate cannot be hashed or run as a partial aggregate: it would put a serial sort of a
           week of collection_log in front of the fleet GROUP BY (the cost FleetCollectionHealthSql's own comment
           refuses), and the raw read is what answers while the rollup is being rebuilt. */
        Assert.DoesNotContain("array_agg", flat, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("ROW_NUMBER", flat, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("ORDER BY collection_time", flat, StringComparison.Ordinal);

        /* The note is the LAST column: the reader is positional and ordinal 13 is the fourteenth. */
        Assert.True(flat.IndexOf("AS last_zero_row_streak_break_time", StringComparison.Ordinal)
                    < flat.IndexOf("AS latest_run_note", StringComparison.Ordinal));
        Assert.EndsWith(" FROM v_collection_log WHERE collection_time >= $1 AND server_id <> 0 GROUP BY server_id, collector_name",
            flat.Replace("\"", string.Empty), StringComparison.Ordinal);
    }

    [Fact]
    public void ComposeFleetSql_TakesTheNoteOfTheRowWithTheGreatestLastRunTime()
    {
        foreach (var raw in new[] { DarlingFleetReader.FleetCollectionHealthSql, ViewerDataService.FleetCollectionHealthByServerSql })
        {
            var noHoles = Flat(CollectionHealthRollupSupport.ComposeFleetSql(raw));
            var withHoles = Flat(CollectionHealthRollupSupport.ComposeFleetSql(raw, [new DateTime(2026, 9, 1, 3, 0, 0)]));

            /* The note rides every part (the rollup's buckets, and the hole hours when there are any), and the
               raw head slice is the raw statement itself, which already selects it. */
            Assert.Single(Regex.Matches(noHoles, @"last_zero_row_streak_break_time, latest_run_note FROM collect\.collection_health_hourly"));
            Assert.Single(Regex.Matches(withHoles, @"last_zero_row_streak_break_time, latest_run_note FROM unnest"));

            /* Re-aggregated by NEWEST time, not SUM or MAX of the text: the note of the part whose last_run_time
               is the greatest, and only while that part is the one carrying a note. */
            var expected = Flat(CollectionHealthRollupSupport.LatestRunNoteComposedSql);
            Assert.Contains(expected, noHoles, StringComparison.Ordinal);
            Assert.Contains(expected, withHoles, StringComparison.Ordinal);
            Assert.Contains("= MAX(last_run_time)", expected, StringComparison.Ordinal);
            Assert.Contains("TO_CHAR(last_run_time, 'YYYYMMDDHH24MISSUS') || latest_run_note", expected, StringComparison.Ordinal);
            Assert.DoesNotContain("MAX(latest_run_note)", noHoles, StringComparison.Ordinal);
            Assert.DoesNotContain("array_agg", noHoles, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void RollupProbe_RequiresTheNoteColumn_SoAStoreNotYetRebuiltAnswersFromRaw()
    {
        var probe = CollectionHealthRollupSupport.RollupProbeSql;

        Assert.Contains("to_regclass('collect.collection_health_hourly')", probe, StringComparison.Ordinal);
        Assert.Contains("attname = 'latest_run_note'", probe, StringComparison.Ordinal);
    }

    [Fact]
    public void ReshapeList_RebuildsAViewWithoutTheNoteColumn()
    {
        var source = Source("Darling/PerformanceMonitor.Darling.Storage/TimescaleSupport.cs");
        var start = source.IndexOf("public static async Task<int> DropStaleContinuousAggregatesAsync(", StringComparison.Ordinal);
        Assert.True(start > 0);
        var body = source[start..source.IndexOf("foreach (var (view, staleCheck) in reshapes)", start, StringComparison.Ordinal)];

        Assert.Contains("(View: CollectionHealthHourlyView", body, StringComparison.Ordinal);
        /* Stale iff the view EXISTS and lacks the column, the query_stats_hourly entry's shape: a fresh store
           (no view yet) matches nothing, and a rebuilt view matches nothing. */
        Assert.Matches(
            @"table_name = 'collection_health_hourly'\) AND NOT EXISTS \(SELECT 1 FROM information_schema\.columns WHERE table_schema = 'collect' AND table_name = 'collection_health_hourly' AND column_name = 'latest_run_note'\)\)",
            body);
    }

    [Fact]
    public void WorkerLaunchesTheOneTimeRollupRefresh_AfterTheEnsureSweep_AndNeverAwaitsItOnTheStartPath()
    {
        var worker = Source("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs");

        const string launch = "collectionHealthWarm = RunCollectionHealthRollupWarmAsync(postgres, stoppingToken);";
        Assert.Single(Regex.Matches(worker, Regex.Escape(launch)));
        Assert.DoesNotContain("await RunCollectionHealthRollupWarmAsync", worker, StringComparison.Ordinal);

        /* After the segment that runs the reshape and the ensure (the "Timescale" stage), beside the other
           launched-not-awaited bulk work, and drained (not dropped) at shutdown. */
        var ensureSegment = worker.IndexOf("timescaleConnection, StoreObjectConvergenceStage.Timescale, startupConvergence", StringComparison.Ordinal);
        var holeRepair = worker.IndexOf("holeRepair = RunMaterializationHoleRepairAsync(postgres, stoppingToken);", StringComparison.Ordinal);
        var warm = worker.IndexOf(launch, StringComparison.Ordinal);
        Assert.True(ensureSegment > 0 && ensureSegment < holeRepair && holeRepair < warm, "the refresh must launch after the ensure sweep");
        Assert.Contains("await collectionHealthWarm;", worker, StringComparison.Ordinal);

        /* The refresh itself is called from exactly one place: the background method on its own connection. */
        Assert.Single(Regex.Matches(worker, Regex.Escape("TimescaleSupport.WarmCollectionHealthHourlyAsync(connection")));
        var method = worker[worker.IndexOf("private async Task RunCollectionHealthRollupWarmAsync(", StringComparison.Ordinal)..];
        Assert.Contains("await using var connection = await postgres.OpenConnectionAsync(stoppingToken);", method[..600], StringComparison.Ordinal);
    }

    [Fact]
    public void RollupRefresh_UsesThePoliciesOwnWindow_AndRunsOnlyWhileTheViewHoldsNothing()
    {
        var source = Source("Darling/PerformanceMonitor.Darling.Storage/TimescaleSupport.cs");
        var start = source.IndexOf("public static async Task<bool> WarmCollectionHealthHourlyAsync(", StringComparison.Ordinal);
        Assert.True(start > 0);
        var body = source[start..source.IndexOf("Continuous aggregates <see cref=\"EnsureContinuousAggregatesAsync\"/> creates", start, StringComparison.Ordinal)];

        Assert.Contains("CollectionHealthRollupSupport.RollupProbeSql", body, StringComparison.Ordinal);
        Assert.Contains("CollectionHealthRollupSupport.WatermarkSql", body, StringComparison.Ordinal);
        Assert.Contains("RollupBackfill.RefreshSliceSql(CollectionHealthHourlyView, force: false, withOptions: false)", body, StringComparison.Ordinal);
        Assert.Contains("nowUtc - CollectionHealthRefreshStartSpan", body, StringComparison.Ordinal);
        Assert.Contains("nowUtc - CollectionHealthRefreshScheduleSpan", body, StringComparison.Ordinal);
        Assert.Equal(TimeSpan.FromDays(8), TimescaleSupport.CollectionHealthRefreshStartSpan);
        Assert.Equal(TimeSpan.FromHours(1), TimescaleSupport.CollectionHealthRefreshScheduleSpan);
    }

    /* ───────────────────────────── the band, through both fleet mappers ───────────────────────────── */

    /// <summary>One row in the fourteen-column shape both fleet reads produce (server_id first), healthy on every
    /// count and fresh on every instant, so the only thing that can move the band is the note.</summary>
    private static DataTableReader FleetRow(string? note)
    {
        var table = new DataTable();
        table.Columns.Add("server_id", typeof(int));
        table.Columns.Add("collector_name", typeof(string));
        foreach (var name in new[] { "total_runs", "success_count", "error_count" })
        {
            table.Columns.Add(name, typeof(long));
        }

        table.Columns.Add("last_success_time", typeof(DateTime));
        table.Columns.Add("permission_denied_count", typeof(long));
        table.Columns.Add("last_run_time", typeof(DateTime));
        table.Columns.Add("abandoned_count", typeof(long));
        table.Columns.Add("extension_missing_count", typeof(long));
        table.Columns.Add("last_non_skip_time", typeof(DateTime));
        table.Columns.Add("last_productive_time", typeof(DateTime));
        table.Columns.Add("last_zero_row_streak_break_time", typeof(DateTime));
        table.Columns.Add("latest_run_note", typeof(string));

        var fresh = DateTime.UtcNow.AddMinutes(-2);
        table.Rows.Add(1, "wait_stats", 100L, 100L, 0L, fresh, 0L, fresh, 0L, 0L, fresh, fresh, fresh, (object?)note ?? DBNull.Value);
        var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        return reader;
    }

    public static TheoryData<int, int, string> Bands => new()
    {
        { 3, 4, "WARNING" },
        { 2, 4, "WARNING" },
        { 4, 4, "WARNING" },
        { 1, 4, "HEALTHY" },
        { 1, 10, "HEALTHY" },
    };

    [Theory]
    [MemberData(nameof(Bands))]
    public void FleetOverviewRow_BandsByTheNoteOfItsNewestRun(int failed, int total, string expected)
    {
        using var reader = FleetRow(Note(failed, total));

        Assert.Equal(expected, DarlingFleetReader.MapFleetHealthRow(reader).HealthStatus);
    }

    [Theory]
    [MemberData(nameof(Bands))]
    public void OverviewCardRow_BandsByTheNoteOfItsNewestRun(int failed, int total, string expected)
    {
        using var reader = FleetRow(Note(failed, total));

        Assert.Equal(expected, ViewerDataService.MapFleetByServerRow(reader).HealthStatus);
    }

    [Fact]
    public void FleetRowWithNoNote_BandsAsItAlwaysDid()
    {
        using (var reader = FleetRow(null))
        {
            Assert.Equal("HEALTHY", DarlingFleetReader.MapFleetHealthRow(reader).HealthStatus);
        }

        using (var reader = FleetRow(null))
        {
            Assert.Equal("HEALTHY", ViewerDataService.MapFleetByServerRow(reader).HealthStatus);
        }

        /* A note that is not the partial-failure sentence (an ordinary informational note) never bands. */
        using (var reader = FleetRow("0 item(s) enumerated"))
        {
            Assert.Equal("HEALTHY", DarlingFleetReader.MapFleetHealthRow(reader).HealthStatus);
        }
    }

    /* ───────────────────────────── live PostgreSQL (CI) ───────────────────────────── */

    /// <summary>Half-hour runs seeded per collector: 384, eight days. The read window starts seven days back, so the
    /// seed has to reach past it: then every hour from the window's first whole hour to the newest run holds a run,
    /// and so a bucket. The continuity guard reads an hour with no bucket below the watermark as a hole, and more
    /// than <see cref="CollectionHealthRollupSupport.MaxRepairableHoleHours"/> of them send the fleet read to raw.
    /// Six runs left about 160 empty hours in the window, so the rollup was never usable and the "rollup" reads
    /// of these tests would have been raw reads.</summary>
    private const int SeededRuns = 384;

    /// <summary>The newest seeded run (g = 0): the :40 mark of the hour two hours back, 80 to 140 minutes ago.
    /// Anchored on the hour so the minute of every run is fixed whatever the clock reads when a test runs: runs
    /// fall at :40 (g even) and :10 (g odd), an hour holds exactly two of them, and the LATER run of an hour is the
    /// even one. The newest run's whole hour is below the refresh policy's watermark (the hour boundary at or below
    /// now minus one hour), not in the real-time tail, and it is never near STALE (the floor is four hours).</summary>
    private const string NewestRunSql = "date_trunc('hour', now() AT TIME ZONE 'UTC') - INTERVAL '80 minutes'";

    /// <summary>One seeded collector: <paramref name="Note"/> rides the run <paramref name="NoteRun"/> steps back
    /// from the newest (0 = the newest run), and no other run carries a note. A null note seeds clean history.</summary>
    private readonly record struct SeededCollector(string Name, string? Note = null, int NoteRun = 0)
    {
        /// <summary>The hour buckets whose LAST run carries the note, which is what the rollup's
        /// <c>last(CASE ... END, collection_time)</c> keeps: an even run is the :40 run, the later of its hour.</summary>
        public long NotedBuckets => Note is not null && NoteRun % 2 == 0 ? 1 : 0;
    }

    private static async Task SeedRunsAsync(NpgsqlConnection connection, SeededCollector[] collectors, CancellationToken ct)
    {
        /* Same age, same cadence, same clean history (SUCCESS with rows, every 30 minutes for eight days) for every
           collector; only the run that carries the note differs. */
        var values = string.Join(", ", collectors.Select((_, i) => $"(@name{i}, CAST(@note{i} AS text), CAST(@run{i} AS integer))"));
        await using var plant = new NpgsqlCommand($@"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected, error_message)
SELECT 30000000 + row_number() OVER (), 1, 'srv-1', c.name, {NewestRunSql} - g * INTERVAL '30 minutes',
       1, 'SUCCESS', 5, CASE WHEN g = c.note_run THEN c.note END
FROM (VALUES {values}) AS c(name, note, note_run)
CROSS JOIN generate_series(0, {SeededRuns - 1}) AS g", connection);
        for (var i = 0; i < collectors.Length; i++)
        {
            plant.Parameters.AddWithValue($"name{i}", collectors[i].Name);
            plant.Parameters.AddWithValue($"note{i}", (object?)collectors[i].Note ?? DBNull.Value);
            plant.Parameters.AddWithValue($"run{i}", collectors[i].NoteRun);
        }

        await plant.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The seed where only the NEWEST run's note differs: the first collector's newest run lost 3 of 4
    /// databases (WARNING in every read), the second's lost 1 of 4 (HEALTHY), the third never carried a note.</summary>
    private static Task SeedThreeCollectorsAsync(NpgsqlConnection connection, string[] collectors, CancellationToken ct) =>
        SeedRunsAsync(connection, [new(collectors[0], Note(3, 4)), new(collectors[1], Note(1, 4)), new(collectors[2])], ct);

    /// <summary>The three slowest-cadence collectors, so a newest run up to 140 minutes old is nowhere near STALE
    /// (the floor is four hours) and the band the test reads is the note's.</summary>
    private static string[] SlowCollectors() => CollectorScheduleDefaults.All
        .OrderByDescending(kv => CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(kv.Value.FrequencyMinutes))
        .ThenBy(kv => kv.Key, StringComparer.Ordinal)
        .Select(kv => kv.Key).Take(3).ToArray();

    private static async Task<Dictionary<string, string>> ViewerBandsAsync(NpgsqlDataSource postgres, string sql, bool composed, DateTime windowStart, CancellationToken ct)
    {
        await using var command = postgres.CreateCommand(sql);
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });
        if (composed)
        {
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = CollectionHealthRollupSupport.CeilingHour(windowStart) });
        }

        var bands = new Dictionary<string, string>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            bands[reader.GetString(1)] = ViewerDataService.MapFleetByServerRow(reader).HealthStatus;
        }

        return bands;
    }

    [Fact]
    public async Task NewestRunNoteInAnOlderHour_BandsWarning_FromRawAndRollupReadsOfBothFleetPaths_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await CollectionHealthAggregateTests.OpenStoreAsync(ct);
        Assert.SkipWhen(store is null, "Set DARLING_TEST_PG to a Postgres connection string with TimescaleDB to run the live fleet-note test.");
        var (scratch, connection, jobId) = store!.Value;
        await using var _s = scratch;
        await using var _c = connection;
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var collectors = SlowCollectors();
        var seeded = new SeededCollector[] { new(collectors[0], Note(3, 4)), new(collectors[1], Note(1, 4)), new(collectors[2]) };
        await SeedRunsAsync(connection, seeded, ct);

        /* The Overview cards' read: the collector whose newest run lost 3 of 4 databases bands WARNING, the one that
           lost 1 of 4 and the one with no note stay HEALTHY, whether the rollup or raw answers. */
        var expectedBands = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [collectors[0]] = "WARNING",
            [collectors[1]] = "HEALTHY",
            [collectors[2]] = "HEALTHY",
        };
        await AssertBandsThroughRollupAndRawAsync(connection, postgres, jobId, seeded, expectedBands, ct);
    }

    /// <summary>
    /// The policy's one run materializes the seed (the scratch database's scheduler is stopped, so it is the only
    /// run); then the same rows are read every way a fleet surface reads them, and each must band the collectors as
    /// <paramref name="expectedBands"/> says: the Overview cards' read composed from the rollup and then raw, and the
    /// fleet overview's read through the service's own chooser, first through the rollup and then, once the view is
    /// dropped, through raw. Before any of that, the rollup itself must hold the note in exactly the buckets the
    /// seed put it (<see cref="SeededCollector.NotedBuckets"/>) and the window must be usable, so a HEALTHY band
    /// below is the guards' answer and not an absent note, and the "rollup" reads are not quietly raw reads.
    /// </summary>
    private static async Task AssertBandsThroughRollupAndRawAsync(
        NpgsqlConnection connection, NpgsqlDataSource postgres, int jobId, SeededCollector[] seeded,
        IReadOnlyDictionary<string, string> expectedBands, CancellationToken ct)
    {
        await CollectionHealthAggregateTests.RunPolicyAsync(connection, jobId, ct);

        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var windowStart = now.AddDays(-7);
        var headEnd = CollectionHealthRollupSupport.CeilingHour(windowStart);

        /* The newest run's whole hour is below the watermark, so it is a materialized bucket and the composed reads
           below take it from the rollup, not from the view's real-time tail. */
        await using (var newestRun = new NpgsqlCommand("SELECT max(collection_time) FROM collect.collection_log", connection))
        await using (var mark = new NpgsqlCommand(CollectionHealthRollupSupport.WatermarkSql, connection))
        {
            var newest = Assert.IsType<DateTime>(await newestRun.ExecuteScalarAsync(ct));
            var watermark = Assert.IsType<DateTime>(await mark.ExecuteScalarAsync(ct));
            Assert.True(
                watermark >= CollectionHealthRollupSupport.CeilingHour(newest),
                $"watermark {watermark:O} has not passed the hour of the newest seeded run {newest:O}");
        }

        /* Every hour from the window's first whole hour to the newest run holds a run, so the continuity guard finds
           no more than the hour or two between the newest run and the watermark to patch. */
        var plan = await CollectionHealthRollupSupport.RollupPlanAsync(postgres, headEnd, ct);
        Assert.True(plan.Usable, $"the rollup is not usable for the window {headEnd:O}; the composed reads would be raw reads");

        foreach (var collector in seeded)
        {
            await using var noted = new NpgsqlCommand(
                $"SELECT count(*) FROM collect.{TimescaleSupport.CollectionHealthHourlyView} WHERE collector_name = @c AND latest_run_note IS NOT NULL", connection);
            noted.Parameters.AddWithValue("c", collector.Name);
            var buckets = Assert.IsType<long>(await noted.ExecuteScalarAsync(ct));
            Assert.True(
                buckets == collector.NotedBuckets,
                $"{collector.Name}: the rollup holds the note in {buckets} hour bucket(s), expected {collector.NotedBuckets}");
        }

        var composedSql = CollectionHealthRollupSupport.ComposeFleetSql(ViewerDataService.FleetCollectionHealthByServerSql);
        Assert.Equal(expectedBands.OrderBy(kv => kv.Key), (await ViewerBandsAsync(postgres, composedSql, true, windowStart, ct)).OrderBy(kv => kv.Key));
        Assert.Equal(expectedBands.OrderBy(kv => kv.Key), (await ViewerBandsAsync(postgres, ViewerDataService.FleetCollectionHealthByServerSql, false, windowStart, ct)).OrderBy(kv => kv.Key));

        /* The fleet overview's read, through the service's own chooser: the HEALTHY collectors are counted (before
           #4812 a collector that lost half its databases was one of them), through the rollup and then through raw. */
        var read = typeof(DarlingFleetReader).GetMethod("ReadFailingCollectorCountsAsync", BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException("ReadFailingCollectorCountsAsync not found");
        async Task<DarlingFleetReader.CollectorCounts> BandedAsync() =>
            (await (Task<Dictionary<int, DarlingFleetReader.CollectorCounts>>)read.Invoke(null, [postgres, now, ct])!)[1];

        var expectedCounts = (
            expectedBands.Count(kv => kv.Value == "HEALTHY"),
            expectedBands.Count(kv => kv.Value == "FAILING"),
            expectedBands.Count);

        var viaRollup = await BandedAsync();
        Assert.Equal(expectedCounts, (viaRollup.Healthy, viaRollup.Failing, viaRollup.Total));

        await using (var drop = new NpgsqlCommand($"DROP MATERIALIZED VIEW collect.{TimescaleSupport.CollectionHealthHourlyView} CASCADE", connection))
        {
            await drop.ExecuteNonQueryAsync(ct);
        }

        Assert.False(await CollectionHealthRollupSupport.RollupUsableAsync(postgres, headEnd, ct));
        var viaRaw = await BandedAsync();
        Assert.Equal(expectedCounts, (viaRaw.Healthy, viaRaw.Failing, viaRaw.Total));
    }

    /* A clean run AFTER a partial-failure run reads Healthy: the newest run's note is the note only while it IS the
       newest run. Two guards make that so, one per shape, and each case below is red without one of them:
         - the raw read's `MAX(partial run time) = MAX(collection_time)` (LatestRunNoteRawSql), which both cases reach
           through the raw reads: without it the raw read keeps the older partial-failure run's note;
         - the composed read's `MAX(last_run_time of a part with a note) = MAX(last_run_time)`
           (LatestRunNoteComposedSql), which only the older-hour case reaches: that hour's bucket holds the note
           (its last run is the partial one) and a newer bucket holds none, so without the guard the composed read
           returns the older bucket's note. In the same-hour case the bucket's own note is already NULL (its last run
           is the clean one), so no bucket carries a note for the composed guard to pass or drop.
       Each seeds a control beside the case: a collector whose newest run carries the note, WARNING through every
       read, so a HEALTHY answer cannot come from a note the reads never see. */

    [Fact]
    public async Task CleanRunInANewerHourAfterAPartialFailureRun_BandsHealthy_FromRawAndRollupReadsOfBothFleetPaths_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await CollectionHealthAggregateTests.OpenStoreAsync(ct);
        Assert.SkipWhen(store is null, "Set DARLING_TEST_PG to a Postgres connection string with TimescaleDB to run the live fleet-note test.");
        var (scratch, connection, jobId) = store!.Value;
        await using var _s = scratch;
        await using var _c = connection;
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var collectors = SlowCollectors();
        var seeded = new SeededCollector[]
        {
            /* Run 2 is the :40 run of the hour before the newest run's hour: the LAST run of that hour, so that
               hour's bucket carries the note. Runs 0 and 1 (the newest hour) are clean and newer. */
            new(collectors[0], Note(3, 4), NoteRun: 2),
            new(collectors[1], Note(3, 4)),
            new(collectors[2]),
        };
        await SeedRunsAsync(connection, seeded, ct);

        var expectedBands = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [collectors[0]] = "HEALTHY",
            [collectors[1]] = "WARNING",
            [collectors[2]] = "HEALTHY",
        };
        await AssertBandsThroughRollupAndRawAsync(connection, postgres, jobId, seeded, expectedBands, ct);
    }

    [Fact]
    public async Task CleanRunLaterInTheSameHourAsAPartialFailureRun_BandsHealthy_FromRawAndRollupReadsOfBothFleetPaths_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await CollectionHealthAggregateTests.OpenStoreAsync(ct);
        Assert.SkipWhen(store is null, "Set DARLING_TEST_PG to a Postgres connection string with TimescaleDB to run the live fleet-note test.");
        var (scratch, connection, jobId) = store!.Value;
        await using var _s = scratch;
        await using var _c = connection;
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var collectors = SlowCollectors();
        var seeded = new SeededCollector[]
        {
            /* Run 1 is the :10 run of the newest run's own hour and run 0 the :40 run after it, clean: one bucket
               holds both, and the bucket's newest run is the clean one, so it holds no note. */
            new(collectors[0], Note(3, 4), NoteRun: 1),
            new(collectors[1], Note(3, 4)),
            new(collectors[2]),
        };
        await SeedRunsAsync(connection, seeded, ct);

        var expectedBands = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [collectors[0]] = "HEALTHY",
            [collectors[1]] = "WARNING",
            [collectors[2]] = "HEALTHY",
        };
        await AssertBandsThroughRollupAndRawAsync(connection, postgres, jobId, seeded, expectedBands, ct);
    }

    [Fact]
    public async Task StoreWhoseRollupPredatesTheNoteColumn_IsRebuiltAndRefreshedOnce_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await CollectionHealthAggregateTests.OpenStoreAsync(ct);
        Assert.SkipWhen(store is null, "Set DARLING_TEST_PG to a Postgres connection string with TimescaleDB to run the live rebuild test.");
        var (scratch, connection, jobId) = store!.Value;
        await using var _s = scratch;
        await using var _c = connection;
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* Put the store back in the shape it had before #4812: the same view without the note column. */
        await using (var drop = new NpgsqlCommand($"DROP MATERIALIZED VIEW collect.{TimescaleSupport.CollectionHealthHourlyView} CASCADE", connection))
        {
            await drop.ExecuteNonQueryAsync(ct);
        }

        var oldShape = Regex.Replace(TimescaleSupport.CreateCollectionHealthHourlySql, @",\s*last\(CASE WHEN .*? AS latest_run_note", string.Empty, RegexOptions.Singleline);
        Assert.DoesNotContain("latest_run_note", oldShape, StringComparison.Ordinal);
        await using (var create = new NpgsqlCommand(oldShape, connection))
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        var collectors = SlowCollectors();
        await SeedThreeCollectorsAsync(connection, collectors, ct);
        var windowStart = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-7), DateTimeKind.Unspecified);
        var headEnd = CollectionHealthRollupSupport.CeilingHour(windowStart);

        /* Old shape: the composer names a column this view lacks, so the guard must send the read to raw. */
        Assert.False(await CollectionHealthRollupSupport.RollupUsableAsync(postgres, headEnd, ct));

        /* The reshape sweep drops it (and only it: nothing else on a fresh store is stale), the ensure sweep
           recreates it WITH NO DATA in the new shape. */
        Assert.Equal(1, await TimescaleSupport.DropStaleContinuousAggregatesAsync(connection, null, ct));
        await using (var gone = new NpgsqlCommand($"SELECT to_regclass('collect.{TimescaleSupport.CollectionHealthHourlyView}') IS NULL", connection))
        {
            Assert.True((bool)(await gone.ExecuteScalarAsync(ct))!);
        }

        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        Assert.True(await CollectionHealthRollupSupport.RollupUsableAsync(postgres, headEnd, ct));

        /* Empty and unmaterialized: the reads are exact from the view's own real-time branch, note included. */
        var emptyBands = await ViewerBandsAsync(postgres, CollectionHealthRollupSupport.ComposeFleetSql(ViewerDataService.FleetCollectionHealthByServerSql), true, windowStart, ct);
        Assert.Equal("WARNING", emptyBands[collectors[0]]);

        /* ONE refresh of the eight-day window materializes it; a second call finds it materialized and does nothing. */
        Assert.True(await TimescaleSupport.WarmCollectionHealthHourlyAsync(connection, null, DateTime.UtcNow, ct));
        await using (var mark = new NpgsqlCommand(CollectionHealthRollupSupport.WatermarkSql, connection))
        {
            Assert.IsType<DateTime>(await mark.ExecuteScalarAsync(ct));
        }

        Assert.False(await TimescaleSupport.WarmCollectionHealthHourlyAsync(connection, null, DateTime.UtcNow, ct));

        var warmedBands = await ViewerBandsAsync(postgres, CollectionHealthRollupSupport.ComposeFleetSql(ViewerDataService.FleetCollectionHealthByServerSql), true, windowStart, ct);
        Assert.Equal("WARNING", warmedBands[collectors[0]]);
        Assert.Equal("HEALTHY", warmedBands[collectors[1]]);
        Assert.Equal("HEALTHY", warmedBands[collectors[2]]);
        _ = jobId;
    }
}
