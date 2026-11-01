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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5810 rollup part 1: V186's <c>collect.collection_log_hourly</c> and the hourly pass that keeps it
/// (<see cref="CollectionLogHourly.RollUpAsync"/>). The rung applies on an empty store and on a store at V185, rewinds, and
/// the viewer probe maps it; a planted hour rolls up to exactly the counts, sums, maxima, instants and histogram its raw rows
/// give (computed here in C#, not by a second copy of the SQL); a late run is counted by the next pass; the first pass
/// backfills the window, a gap is caught up, and retention drops the hours that age out.
/// </summary>
/* #1776 own-store: each live test mints its own scratch database, so no other class's rows shape what the pass rolls up or
   which hour it starts from. */
[Collection("live-postgres")]
public sealed class CollectionLogHourlyLiveTests
{
    private const int ServerId = -5810101;
    private const string ServerName = "collection-log-hourly";

    /// <summary>The pass's clock in every test: half past noon, so 12:00 is the open hour and 11:00 the newest closed one.</summary>
    private static readonly DateTime Now = new(2026, 10, 1, 12, 30, 0, DateTimeKind.Utc);

    private static readonly DateTime CurrentHour = new(2026, 10, 1, 12, 0, 0, DateTimeKind.Unspecified);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed record Run(string Collector, DateTime Time, string Status, int? DurationMs, int? Rows = null, int? SlowestMs = null, string? Error = null);

    [Fact]
    public void TheBounds_AreFortyLogSpacedIntegers_FromOneMillisecondToOneHour_AndSlotOfCountsTheBoundsBelow()
    {
        var bounds = CollectionLogHourly.DurationBucketBounds;
        Assert.Equal(40, bounds.Length);
        Assert.Equal(1, bounds[0]);
        Assert.Equal(3_600_000, bounds[^1]);
        for (var i = 1; i < bounds.Length; i++)
        {
            Assert.True(bounds[i] > bounds[i - 1], $"bound {i} ({bounds[i]}) is not above bound {i - 1} ({bounds[i - 1]})");
            /* Log-spaced: no step is more than 1.5x (plus the ceiling's one millisecond). */
            Assert.True(bounds[i] <= (bounds[i - 1] * 1.5) + 1, $"bound {i} ({bounds[i]}) is more than 1.5x bound {i - 1}");
        }

        Assert.Equal(0, CollectionLogHourly.SlotOf(0));
        Assert.Equal(0, CollectionLogHourly.SlotOf(1));
        Assert.Equal(1, CollectionLogHourly.SlotOf(2));
        Assert.Equal(39, CollectionLogHourly.SlotOf(3_600_000));
        Assert.Equal(40, CollectionLogHourly.SlotOf(3_600_001));
        Assert.Equal(3_600_000, CollectionLogHourly.UpperBoundOfSlot(39));
        Assert.Null(CollectionLogHourly.UpperBoundOfSlot(40));
        for (var s = 0; s < 40; s++)
        {
            Assert.Equal(s, CollectionLogHourly.SlotOf(bounds[s]));
            Assert.Equal(s + 1, CollectionLogHourly.SlotOf(bounds[s] + 1L));
        }
    }

    [Fact]
    public async Task AnEmptyStore_TakesV186_AndAStoreAtV185_TakesItAgain_AndTheProbeMapsEach_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the V186 rung test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TableExistsAsync(connection, ct));
        Assert.Equal(new[] { "server_id", "collector_name", "hour_start" }, await PrimaryKeyColumnsAsync(connection, ct));
        Assert.Equal(StorageVersion.SchemaVersion, await ProbeAsync(scratch.ConnectionString, ct));

        /* Back to V185: the table gone and the ladder at 185. The probe must say 185, not 186. */
        /* V190 (#5857) and V189 (#5867) sit above this rung too, so their sentinels go first. */
        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 185, ct);
        await ExecAsync(connection, ct, "DELETE FROM darling_schema_version WHERE version >= 186;");
        Assert.False(await TableExistsAsync(connection, ct));
        Assert.Equal(185, await ProbeAsync(scratch.ConnectionString, ct));

        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TableExistsAsync(connection, ct));
        Assert.Equal(StorageVersion.SchemaVersion, await ProbeAsync(scratch.ConnectionString, ct));
        Assert.Equal(StorageVersion.SchemaVersion, await ScalarAsync<int>(connection, ct, "SELECT max(version) FROM darling_schema_version;"));

        /* Idempotent: the rung re-applied over its own table (the stamp rewound, the table kept) changes nothing. */
        await ExecAsync(connection, ct, "DELETE FROM darling_schema_version WHERE version >= 186;");
        await PgMigrations.MigrateAsync(connection, ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.Equal(1L, await ScalarAsync<long>(connection, ct,
            "SELECT count(*) FROM pg_indexes WHERE schemaname = 'collect' AND indexname = 'idx_collection_log_hourly_hour';"));
        Assert.Equal(StorageVersion.SchemaVersion, await ProbeAsync(scratch.ConnectionString, ct));
    }

    [Fact]
    public async Task APlantedHour_RollsUpToExactlyWhatItsRawRowsGive_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the hourly rollup test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var h10 = CurrentHour.AddHours(-2);
            var h11 = CurrentHour.AddHours(-1);
            var abandonedNote = "wall-clock budget (30s) reached; cycle abandoned";
            var runs = new List<Run>
            {
                /* Every status the aggregates split on, each bound edge, NULL and zero durations. */
                new("alpha", h10.AddMinutes(1), "SUCCESS", 0, 5, 3),
                new("alpha", h10.AddMinutes(2), "SUCCESS", 1, 0),
                new("alpha", h10.AddMinutes(3), "SUCCESS", 2, 7, 9),
                new("alpha", h10.AddMinutes(4), "SUCCESS", 3_600_000, 1),
                new("alpha", h10.AddMinutes(5), "SUCCESS", 3_600_001, 2, 40),
                new("alpha", h10.AddMinutes(6), "SUCCESS", null, 4),
                new("alpha", h10.AddMinutes(7), "ERROR", 250, null, null, "boom"),
                new("alpha", h10.AddMinutes(8), "PERMISSIONS", 12, null, null, "denied"),
                new("alpha", h10.AddMinutes(9), "YIELDED", 30),
                new("alpha", h10.AddMinutes(10), "SKIPPED", null),
                new("alpha", h10.AddMinutes(11), "EXTENSION_MISSING", 4, 0),
                new("alpha", h10.AddMinutes(12), "SESSION_MISSING", 5),
                new("alpha", h10.AddMinutes(13), EnumeratedCollectorDriver.AbandonedStatus, 900, 0),
                new("alpha", h10.AddMinutes(14), "SUCCESS", 70, 3, 11, "a note"),
                new("alpha", h10.AddMinutes(15), "SUCCESS", 80, 0, null, abandonedNote),
                new("alpha", h10.AddMinutes(16), "SUCCESS", 90, 0),
                new("alpha", h11.AddMinutes(20), "SUCCESS", 15, 1),
                /* Excluded: the cycle row and the open hour. */
                new(CollectionCycleRow.CollectorName, h10.AddMinutes(30), "SUCCESS", 5_000, 1),
                new("alpha", CurrentHour.AddMinutes(5), "ERROR", 123),
            };
            var random = new Random(5810);
            for (var i = 0; i < 300; i++)
            {
                int? duration = i % 37 == 0 ? null : (int)Math.Round(Math.Exp(random.NextDouble() * 12));
                runs.Add(new Run("beta", h11.AddSeconds(i * 11), i % 50 == 0 ? "ERROR" : "SUCCESS", duration, i % 3, i % 5 == 0 ? i : null));
            }

            await PlantAsync(connection, runs, ct);
            var upserted = await CollectionLogHourly.RollUpAsync(connection, Now, null, ct);

            var expected = ExpectedRows(runs);
            Assert.Equal(expected.Count, upserted);
            var actual = await ReadHourlyAsync(connection, ct);
            Assert.Equal(expected.Keys.OrderBy(k => k), actual.Keys.OrderBy(k => k));
            foreach (var (key, row) in expected)
            {
                Assert.Equal(row, actual[key]);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task ALateRunInARolledHour_IsCountedByTheNextPass_AndAGapIsCaughtUp_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the hourly rollup test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var h10 = CurrentHour.AddHours(-2);
            var h11 = CurrentHour.AddHours(-1);
            var runs = new List<Run>
            {
                new("alpha", h10.AddMinutes(5), "SUCCESS", 10, 1),
                new("alpha", h11.AddMinutes(5), "SUCCESS", 20, 1),
            };
            await PlantAsync(connection, runs, ct);
            await CollectionLogHourly.RollUpAsync(connection, Now, null, ct);
            Assert.Equal(1, (await ReadHourlyAsync(connection, ct))[("alpha", h11)].TotalRuns);

            /* Two runs written late into both rolled hours, after the pass. The next pass, in the same hour, counts both. */
            var late = new List<Run>
            {
                new("alpha", h10.AddMinutes(50), "ERROR", 5_000, null, null, "late"),
                new("alpha", h11.AddMinutes(50), "ERROR", 6_000, null, null, "late"),
            };
            await PlantAsync(connection, late, ct);
            runs.AddRange(late);
            await CollectionLogHourly.RollUpAsync(connection, Now, null, ct);
            var afterLate = await ReadHourlyAsync(connection, ct);
            Assert.Equal(ExpectedRows(runs)[("alpha", h10)], afterLate[("alpha", h10)]);
            Assert.Equal(ExpectedRows(runs)[("alpha", h11)], afterLate[("alpha", h11)]);
            Assert.Equal(2, afterLate[("alpha", h11)].TotalRuns);
            Assert.Equal(1, afterLate[("alpha", h11)].ErrorCount);

            /* Five hours later with nothing rolled in between: every closed hour after the newest rolled one is caught up. */
            var gap = new List<Run>
            {
                new("alpha", CurrentHour.AddMinutes(10), "SUCCESS", 30, 1),
                new("alpha", CurrentHour.AddHours(2).AddMinutes(10), "SUCCESS", 40, 1),
                new("alpha", CurrentHour.AddHours(4).AddMinutes(10), "SUCCESS", 50, 1),
                new("alpha", CurrentHour.AddHours(5).AddMinutes(10), "SUCCESS", 60, 1),
            };
            await PlantAsync(connection, gap, ct);
            runs.AddRange(gap);
            await CollectionLogHourly.RollUpAsync(connection, Now.AddHours(5), null, ct);
            var afterGap = await ReadHourlyAsync(connection, ct);
            var expectedAfterGap = ExpectedRows(runs, CurrentHour.AddHours(5));
            Assert.Equal(expectedAfterGap.Keys.OrderBy(k => k), afterGap.Keys.OrderBy(k => k));
            Assert.Equal(new[] { h10, h11, CurrentHour, CurrentHour.AddHours(2), CurrentHour.AddHours(4) },
                afterGap.Keys.Select(k => k.Hour).OrderBy(h => h));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task TheFirstPass_BackfillsTheWindow_AndRetentionDropsTheHoursThatAgeOut_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the hourly rollup test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var oldestKept = CurrentHour.AddHours(-CollectionLogHourly.RetentionHours);
            Assert.True(oldestKept < CurrentHour.AddDays(-7), "retention must keep every closed hour of the 7-day window");
            var runs = new List<Run>
            {
                new("alpha", oldestKept.AddHours(-1).AddMinutes(5), "SUCCESS", 10, 1),
                new("alpha", oldestKept.AddMinutes(5), "SUCCESS", 10, 1),
                new("alpha", oldestKept.AddHours(1).AddMinutes(5), "SUCCESS", 10, 1),
                new("alpha", CurrentHour.AddDays(-3).AddMinutes(5), "SUCCESS", 10, 1),
                new("alpha", CurrentHour.AddHours(-1).AddMinutes(5), "SUCCESS", 10, 1),
            };
            await PlantAsync(connection, runs, ct);

            /* The first pass on an empty table rolls every closed hour of the kept window, and nothing older. */
            await CollectionLogHourly.RollUpAsync(connection, Now, null, ct);
            Assert.Equal(
                new[] { oldestKept, oldestKept.AddHours(1), CurrentHour.AddDays(-3), CurrentHour.AddHours(-1) },
                (await ReadHourlyAsync(connection, ct)).Keys.Select(k => k.Hour).OrderBy(h => h));

            /* Two hours on, the two oldest hours are older than the window kept and are deleted; the rest stay. */
            await CollectionLogHourly.RollUpAsync(connection, Now.AddHours(2), null, ct);
            Assert.Equal(
                new[] { CurrentHour.AddDays(-3), CurrentHour.AddHours(-1) },
                (await ReadHourlyAsync(connection, ct)).Keys.Select(k => k.Hour).OrderBy(h => h));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task ABacklog_IsCaughtUpADayPerPass_NewestFirst_AndTheRecomputedHoursAlways_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the hourly rollup test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            /* One run in each of the 60 closed hours before the current one, plus an empty 6-hour outage that must not use
               up the cap: hours with no raw row are not missing. */
            var runs = Enumerable.Range(1, 60)
                .Where(back => back is < 30 or > 35)
                .Select(back => new Run("alpha", CurrentHour.AddHours(-back).AddMinutes(7), "SUCCESS", back, 1))
                .ToList();
            await PlantAsync(connection, runs, ct);
            var withRows = runs.Select(r => r.Time.AddMinutes(-7)).OrderByDescending(h => h).ToList();

            /* Pass 1: the two recomputed hours plus the 24 newest missing hours that hold rows (the ruled cap, spelled out). */
            await CollectionLogHourly.RollUpAsync(connection, Now, null, ct);
            var expected = withRows.Take(2 + 24).OrderBy(h => h).ToList();
            Assert.Equal(expected, (await ReadHourlyAsync(connection, ct)).Keys.Select(k => k.Hour).OrderBy(h => h));

            /* Pass 2, same hour: the next 24 missing hours, newest first, skipping the outage. */
            await CollectionLogHourly.RollUpAsync(connection, Now, null, ct);
            expected = withRows.Take(2 + 2 * 24).OrderBy(h => h).ToList();
            Assert.Equal(expected, (await ReadHourlyAsync(connection, ct)).Keys.Select(k => k.Hour).OrderBy(h => h));

            /* Pass 3: the rest, and every rolled hour holds exactly what its raw rows give. */
            await CollectionLogHourly.RollUpAsync(connection, Now, null, ct);
            var rolled = await ReadHourlyAsync(connection, ct);
            Assert.Equal(withRows.OrderBy(h => h), rolled.Keys.Select(k => k.Hour).OrderBy(h => h));
            foreach (var (key, row) in ExpectedRows(runs))
            {
                Assert.Equal(row, rolled[key]);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, DeleteRowsAsync);
        }
    }

    private sealed record HourRow(
        int TotalRuns, int SuccessCount, int ErrorCount, int PermissionDeniedCount, int YieldCount, int AbandonedCount,
        int ExtensionMissingCount, int SessionMissingCount, int NoteCount, int RunsWithRows, long RowsStored,
        long DurationSum, int DurationCount, int? MaxDurationMs, int? MaxSlowestItemMs,
        DateTime? LastSuccess, DateTime LastRun, DateTime? LastError, DateTime? LastFailureText, DateTime? LastNote,
        DateTime? LastDenied, DateTime? LastNonSkip, DateTime? LastProductive, DateTime? LastStreakBreak, string Histogram);

    /// <summary>What each closed hour's row must hold, computed from the planted runs by the definitions the read's
    /// <c>health</c> aggregate states, and the histogram from <see cref="CollectionLogHourly.SlotOf"/>.</summary>
    private static Dictionary<(string Collector, DateTime Hour), HourRow> ExpectedRows(IEnumerable<Run> runs, DateTime? currentHour = null)
    {
        var end = currentHour ?? CurrentHour;
        static DateTime? Last(IEnumerable<Run> rows, Func<Run, bool> where) =>
            rows.Where(where).Select(r => (DateTime?)r.Time).DefaultIfEmpty(null).Max();
        static bool Failure(Run r) => r.Status is "ERROR" or "PERMISSIONS" or "EXTENSION_MISSING";
        static bool AbandonedByNote(Run r) => r.Rows == 0 && (r.Error ?? "").EndsWith("s) reached; cycle abandoned", StringComparison.Ordinal)
            && (r.Error ?? "").StartsWith("wall-clock budget (", StringComparison.Ordinal);

        return runs
            .Where(r => r.Time < end && r.Collector != CollectionCycleRow.CollectorName && r.Collector != AnalysisPassRow.CollectorName)
            .GroupBy(r => (r.Collector, Hour: new DateTime(r.Time.Year, r.Time.Month, r.Time.Day, r.Time.Hour, 0, 0)))
            .ToDictionary(g => g.Key, g =>
            {
                var rows = g.ToList();
                var slots = new int[CollectionLogHourly.SlotCount];
                foreach (var r in rows.Where(r => r.DurationMs.HasValue))
                {
                    slots[CollectionLogHourly.SlotOf(r.DurationMs!.Value)]++;
                }

                return new HourRow(
                    rows.Count,
                    rows.Count(r => r.Status == "SUCCESS" && !AbandonedByNote(r)),
                    rows.Count(r => r.Status == "ERROR"),
                    rows.Count(r => r.Status == "PERMISSIONS"),
                    rows.Count(r => r.Status == "YIELDED"),
                    rows.Count(r => r.Status == EnumeratedCollectorDriver.AbandonedStatus || AbandonedByNote(r)),
                    rows.Count(r => r.Status == "EXTENSION_MISSING"),
                    rows.Count(r => r.Status == "SESSION_MISSING"),
                    rows.Count(r => r.Status == "SUCCESS" && r.Error is not null),
                    rows.Count(r => r.Rows > 0),
                    rows.Sum(r => (long)(r.Rows ?? 0)),
                    rows.Sum(r => (long)(r.DurationMs ?? 0)),
                    rows.Count(r => r.DurationMs.HasValue),
                    rows.Max(r => r.DurationMs),
                    rows.Max(r => r.SlowestMs),
                    Last(rows, r => r.Status is "SUCCESS" or "SKIPPED"),
                    rows.Max(r => r.Time),
                    Last(rows, Failure),
                    Last(rows, r => Failure(r) && r.Error is not null),
                    Last(rows, r => r.Status == "SUCCESS" && r.Error is not null),
                    Last(rows, r => r.Status == "PERMISSIONS"),
                    Last(rows, r => !CollectorRuntimePrecondition.NamedSkipStatuses.Contains(r.Status)),
                    Last(rows, r => r.Rows > 0),
                    Last(rows, r => !(r.Status == "SUCCESS" && (r.Rows ?? 0) == 0 && !AbandonedByNote(r))),
                    string.Join(",", slots));
            });
    }

    private static async Task<Dictionary<(string Collector, DateTime Hour), HourRow>> ReadHourlyAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
SELECT collector_name, hour_start, total_runs, success_count, error_count, permission_denied_count, yield_count,
       abandoned_count, extension_missing_count, session_missing_count, note_count, runs_with_rows, rows_stored,
       duration_sum, duration_count, max_duration_ms, max_slowest_item_ms,
       last_success_time, last_run_time, last_error_time, last_failure_text_time, last_note_time, last_denied_time,
       last_non_skip_time, last_productive_time, last_streak_break_time, array_to_string(duration_histogram, ',')
FROM collect.collection_log_hourly
WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(ServerId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = new Dictionary<(string, DateTime), HourRow>();
        DateTime? Time(int i) => reader.IsDBNull(i) ? null : reader.GetDateTime(i);
        int? Int(int i) => reader.IsDBNull(i) ? null : reader.GetInt32(i);
        while (await reader.ReadAsync(ct))
        {
            result[(reader.GetString(0), reader.GetDateTime(1))] = new HourRow(
                reader.GetInt32(2), reader.GetInt32(3), reader.GetInt32(4), reader.GetInt32(5), reader.GetInt32(6),
                reader.GetInt32(7), reader.GetInt32(8), reader.GetInt32(9), reader.GetInt32(10), reader.GetInt32(11),
                reader.GetInt64(12), reader.GetInt64(13), reader.GetInt32(14), Int(15), Int(16),
                Time(17), reader.GetDateTime(18), Time(19), Time(20), Time(21), Time(22), Time(23), Time(24), Time(25),
                reader.GetString(26));
        }

        return result;
    }

    private static async Task PlantAsync(NpgsqlConnection connection, IReadOnlyList<Run> runs, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, slowest_item_ms)
SELECT $1 + u.ord, $2, $3, u.collector_name, u.collection_time, u.duration_ms, u.status, u.error_message, u.rows_collected, u.slowest_item_ms
FROM unnest($4::text[], $5::timestamp[], $6::integer[], $7::text[], $8::text[], $9::integer[], $10::integer[])
     WITH ORDINALITY AS u (collector_name, collection_time, duration_ms, status, error_message, rows_collected, slowest_item_ms, ord)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(runs.Select(r => r.Collector).ToArray());
        command.Parameters.AddWithValue(runs.Select(r => DateTime.SpecifyKind(r.Time, DateTimeKind.Unspecified)).ToArray());
        command.Parameters.AddWithValue(runs.Select(r => r.DurationMs).ToArray());
        command.Parameters.AddWithValue(runs.Select(r => r.Status).ToArray());
        command.Parameters.AddWithValue(runs.Select(r => r.Error).ToArray());
        command.Parameters.AddWithValue(runs.Select(r => r.Rows).ToArray());
        command.Parameters.AddWithValue(runs.Select(r => r.SlowestMs).ToArray());
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<int?> ProbeAsync(string connectionString, CancellationToken ct)
    {
        await using var service = new ViewerDataService(connectionString);
        return await service.GetStoreSchemaVersionAsync(ct);
    }

    private static async Task<bool> TableExistsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await ScalarAsync<bool>(connection, ct, "SELECT to_regclass('collect.collection_log_hourly') IS NOT NULL;");

    private static async Task<string[]> PrimaryKeyColumnsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
SELECT a.attname
FROM pg_index AS i
CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k (attnum, ord)
JOIN pg_attribute AS a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
WHERE i.indrelid = to_regclass('collect.collection_log_hourly')
AND   i.indisprimary
ORDER BY k.ord", connection);
        var columns = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            columns.Add(reader.GetString(0));
        }

        return columns.ToArray();
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, CancellationToken ct, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log_hourly WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
