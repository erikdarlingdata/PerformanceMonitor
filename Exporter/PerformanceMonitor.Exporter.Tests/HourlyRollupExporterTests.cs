using System.Data.Common;
using DuckDB.NET.Data;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// Exercises <see cref="HourlyRollupExporter"/> against a memory_grant_stats-shaped fake reader (design doc
/// §2.3.3): 12 planted rows across two (server, key) pairs and two hours, so the rollup's avg/max/last, the
/// "no raw part" rule, a missing-column failure, and the manifest fields (§2.3.5) are all checked directly
/// against the exporter, not just against <see cref="RollupSql.Build"/>'s pure SQL.
/// </summary>
public class HourlyRollupExporterTests
{
    private static string NewOutDir() => Directory.CreateTempSubdirectory("darling-export-hourly-").FullName;

    private static readonly string[] MemoryGrantColumns =
    [
        "server_name", "collection_time", "resource_semaphore_id", "pool_id",
        "target_memory_mb", "max_target_memory_mb", "total_memory_mb", "granted_memory_mb", "used_memory_mb",
        "grantee_count", "waiter_count", "available_memory_mb",
        "timeout_error_count_delta", "forced_grant_count_delta", "timeout_error_count", "forced_grant_count",
        "sample_interval_seconds",
    ];

    private static readonly string[] MemoryGrantPgTypes =
    [
        "varchar", "timestamp", "int4", "int4",
        "int4", "int4", "int4", "int4", "int4",
        "int4", "int4", "int4",
        "int4", "int4", "int4", "int4",
        "int4",
    ];

    /// <summary>12 planted rows: two (resource_semaphore_id, pool_id) keys, two hours each, three samples per
    /// (key, hour). server_name is appended separately by the exporter/reader factory pairing below.</summary>
    private static List<object[]> PlantedRows(DateTime hour0, DateTime hour1)
    {
        List<object[]> Row(DateTime t, int semaphoreId, int poolId, int granted, int available, int timeoutDelta, int timeoutTotal) =>
        [
            ["server1", t, semaphoreId, poolId, 100, 100, 100, granted, granted, 1, 0, available, timeoutDelta, 0, timeoutTotal, 0, 300],
        ];

        var rows = new List<List<object[]>>
        {
            // key (1,1), hour0: granted 10,20,30 -> avg 20, max 30, last (arg_max by time) = the last row's value.
            Row(hour0, 1, 1, 10, 90, 1, 1),
            Row(hour0.AddMinutes(5), 1, 1, 20, 80, 1, 2),
            Row(hour0.AddMinutes(10), 1, 1, 30, 70, 1, 3),
            // key (1,1), hour1.
            Row(hour1, 1, 1, 40, 60, 1, 4),
            Row(hour1.AddMinutes(5), 1, 1, 50, 50, 1, 5),
            Row(hour1.AddMinutes(10), 1, 1, 60, 40, 1, 6),
            // key (2,2), hour0.
            Row(hour0, 2, 2, 5, 95, 0, 0),
            Row(hour0.AddMinutes(5), 2, 2, 15, 85, 0, 0),
            Row(hour0.AddMinutes(10), 2, 2, 25, 75, 0, 0),
            // key (2,2), hour1.
            Row(hour1, 2, 2, 35, 65, 0, 0),
            Row(hour1.AddMinutes(5), 2, 2, 45, 55, 0, 0),
            Row(hour1.AddMinutes(10), 2, 2, 55, 45, 0, 0),
        };

        return rows.SelectMany(r => r).ToList();
    }

    private static FakeDbDataReader NewReader(List<object[]> rows) =>
        new(MemoryGrantColumns, MemoryGrantPgTypes, rows);

    private static async Task<object?> Scalar(string absoluteParquetPath, string selectExpr, string whereClause)
    {
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT {selectExpr} FROM read_parquet('{absoluteParquetPath}') WHERE {whereClause}";
        return await command.ExecuteScalarAsync();
    }

    [Fact]
    public async Task Twelve_planted_rows_give_one_row_per_key_per_hour_with_exact_avg_max_last()
    {
        var hour0 = new DateTime(2026, 9, 27, 3, 0, 0, DateTimeKind.Unspecified);
        var hour1 = hour0.AddHours(1);
        var reader = NewReader(PlantedRows(hour0, hour1));
        var outDir = NewOutDir();
        var spec = RollupSql.Specs["memory_grant_stats_hourly"];

        var result = await HourlyRollupExporter.ExportAsync(
            spec, "store_a", outDir, hour0, hour0.AddHours(2),
            _ => Task.FromResult<DbDataReader>(reader), CancellationToken.None);

        Assert.Equal(12, result.RawRowCount);
        Assert.Equal(4, result.RolledRowCount); // 2 keys x 2 hours

        var absolute = Path.Combine(outDir, "store_a", result.Part.RelativePath.Replace('/', Path.DirectorySeparatorChar));

        const string key1Hour0 = "resource_semaphore_id = 1 AND pool_id = 1 AND hour = TIMESTAMP '2026-09-27 03:00:00'";
        const string key2Hour1 = "resource_semaphore_id = 2 AND pool_id = 2 AND hour = TIMESTAMP '2026-09-27 04:00:00'";

        var avgKey1Hour0 = await Scalar(absolute, "granted_memory_mb_avg", key1Hour0);
        Assert.Equal(20.0, Convert.ToDouble(avgKey1Hour0));

        var maxKey1Hour0 = await Scalar(absolute, "granted_memory_mb_max", key1Hour0);
        Assert.Equal(30L, Convert.ToInt64(maxKey1Hour0));

        var lastKey1Hour0 = await Scalar(absolute, "granted_memory_mb_last", key1Hour0);
        Assert.Equal(30L, Convert.ToInt64(lastKey1Hour0));

        var avgKey2Hour1 = await Scalar(absolute, "granted_memory_mb_avg", key2Hour1);
        Assert.Equal(45.0, Convert.ToDouble(avgKey2Hour1));
    }

    [Fact]
    public async Task Every_rolled_row_carries_the_store_in_source_store()
    {
        var hour0 = new DateTime(2026, 9, 27, 3, 0, 0, DateTimeKind.Unspecified);
        var reader = NewReader(PlantedRows(hour0, hour0.AddHours(1)));
        var outDir = NewOutDir();
        var spec = RollupSql.Specs["memory_grant_stats_hourly"];

        var result = await HourlyRollupExporter.ExportAsync(
            spec, "store_a", outDir, hour0, hour0.AddHours(2),
            _ => Task.FromResult<DbDataReader>(reader), CancellationToken.None);

        var absolute = Path.Combine(outDir, "store_a", result.Part.RelativePath.Replace('/', Path.DirectorySeparatorChar));
        var total = await Scalar(absolute, "count(*)", "true");
        var labeled = await Scalar(absolute, "count(*)", "source_store = 'store_a'");
        Assert.Equal(4L, Convert.ToInt64(total));
        Assert.Equal(4L, Convert.ToInt64(labeled)); // all four rows, none NULL
    }

    [Fact]
    public async Task No_raw_part_is_written_only_one_rollup_part()
    {
        var hour0 = new DateTime(2026, 9, 27, 3, 0, 0, DateTimeKind.Unspecified);
        var reader = NewReader(PlantedRows(hour0, hour0.AddHours(1)));
        var outDir = NewOutDir();
        var spec = RollupSql.Specs["memory_grant_stats_hourly"];

        var result = await HourlyRollupExporter.ExportAsync(
            spec, "store_a", outDir, hour0, hour0.AddHours(2),
            _ => Task.FromResult<DbDataReader>(reader), CancellationToken.None);

        var tableDir = Path.Combine(outDir, "store_a", "memory_grant_stats_hourly");
        var files = Directory.Exists(tableDir) ? Directory.GetFiles(tableDir, "*.parquet", SearchOption.AllDirectories) : [];
        Assert.Single(files);
        Assert.Contains("memory_grant_stats_hourly", result.Part.RelativePath);
        Assert.DoesNotContain("raw", Path.GetFileName(result.Part.RelativePath), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Missing_rule_column_fails_the_table()
    {
        // Drop granted_memory_mb (a Gauge rule column the spec requires) from both the column names and every row.
        var hour0 = new DateTime(2026, 9, 27, 3, 0, 0, DateTimeKind.Unspecified);
        var grantedIndex = Array.IndexOf(MemoryGrantColumns, "granted_memory_mb");
        var names = MemoryGrantColumns.Where((_, i) => i != grantedIndex).ToArray();
        var types = MemoryGrantPgTypes.Where((_, i) => i != grantedIndex).ToArray();
        var rows = PlantedRows(hour0, hour0.AddHours(1))
            .Select(r => r.Where((_, i) => i != grantedIndex).ToArray())
            .ToList();

        var reader = new FakeDbDataReader(names, types, rows);
        var outDir = NewOutDir();
        var spec = RollupSql.Specs["memory_grant_stats_hourly"];

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => HourlyRollupExporter.ExportAsync(
            spec, "store_a", outDir, hour0, hour0.AddHours(2),
            _ => Task.FromResult<DbDataReader>(reader), CancellationToken.None));

        Assert.Contains("granted_memory_mb", ex.Message);
    }

    // ---- counters across the hour boundary (pg_database_stats_hourly) ----

    private static readonly string[] DatabaseStatsColumns =
    [
        "server_name", "collection_time", "database_name",
        "xact_commit", "xact_rollback", "blks_read", "blks_hit", "temp_files", "temp_bytes", "deadlocks",
        "numbackends", "stats_reset",
    ];

    private static readonly string[] DatabaseStatsPgTypes =
    [
        "varchar", "timestamp", "varchar",
        "int8", "int8", "int8", "int8", "int8", "int8", "int8",
        "int4", "timestamp",
    ];

    /// <summary>One sample a minute through the hour, the counter up by one each minute, with an optional sample
    /// one minute before the window: the previous hour's last sample.</summary>
    private static List<object[]> CounterRows(DateTime hour, bool withSampleBeforeWindow)
    {
        var rows = new List<object[]>();
        var reset = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified);
        if (withSampleBeforeWindow)
        {
            rows.Add(["server1", hour.AddMinutes(-1), "db1", 1000L, 0L, 0L, 0L, 0L, 0L, 0L, 1, reset]);
        }

        for (var minute = 0; minute < 60; minute++)
        {
            rows.Add(["server1", hour.AddMinutes(minute), "db1", 1001L + minute, 0L, 0L, 0L, 0L, 0L, 0L, 1, reset]);
        }

        return rows;
    }

    [Fact]
    public async Task A_sample_before_the_window_bridges_the_counter_into_the_hour_and_is_not_itself_counted()
    {
        var hour = new DateTime(2026, 9, 27, 3, 0, 0, DateTimeKind.Unspecified);
        var reader = new FakeDbDataReader(DatabaseStatsColumns, DatabaseStatsPgTypes, CounterRows(hour, withSampleBeforeWindow: true));
        var outDir = NewOutDir();
        var spec = RollupSql.Specs["pg_database_stats_hourly"];

        var result = await HourlyRollupExporter.ExportAsync(
            spec, "store_a", outDir, hour, hour.AddHours(1),
            _ => Task.FromResult<DbDataReader>(reader), CancellationToken.None);

        Assert.Equal(1, result.RolledRowCount);
        var absolute = Path.Combine(outDir, "store_a", result.Part.RelativePath.Replace('/', Path.DirectorySeparatorChar));
        const string theHour = "database_name = 'db1' AND hour = TIMESTAMP '2026-09-27 03:00:00'";

        // The sample before the window feeds the first in-hour step and nothing else.
        Assert.Equal(60L, Convert.ToInt64(await Scalar(absolute, "sample_count", theHour)));
        Assert.Equal(1001L, Convert.ToInt64(await Scalar(absolute, "xact_commit_first", theHour)));
        Assert.Equal(1060L, Convert.ToInt64(await Scalar(absolute, "xact_commit_last", theHour)));
        // Unchanged: the steps between the hour's own samples.
        Assert.Equal(59L, Convert.ToInt64(await Scalar(absolute, "xact_commit_delta_in_hour", theHour)));
        // New: the step from the previous hour's last sample into this hour is counted too.
        Assert.Equal(60L, Convert.ToInt64(await Scalar(absolute, "xact_commit_delta_hour_bridged", theHour)));
    }

    [Fact]
    public async Task Without_a_sample_before_the_window_the_bridged_delta_equals_the_in_hour_delta()
    {
        var hour = new DateTime(2026, 9, 27, 3, 0, 0, DateTimeKind.Unspecified);
        var reader = new FakeDbDataReader(DatabaseStatsColumns, DatabaseStatsPgTypes, CounterRows(hour, withSampleBeforeWindow: false));
        var outDir = NewOutDir();
        var spec = RollupSql.Specs["pg_database_stats_hourly"];

        var result = await HourlyRollupExporter.ExportAsync(
            spec, "store_a", outDir, hour, hour.AddHours(1),
            _ => Task.FromResult<DbDataReader>(reader), CancellationToken.None);

        var absolute = Path.Combine(outDir, "store_a", result.Part.RelativePath.Replace('/', Path.DirectorySeparatorChar));
        const string theHour = "database_name = 'db1' AND hour = TIMESTAMP '2026-09-27 03:00:00'";

        using (var connection = new DuckDBConnection("Data Source=:memory:"))
        {
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{absolute}'))";
            using var describeReader = await command.ExecuteReaderAsync();
            var names = new List<string>();
            while (await describeReader.ReadAsync())
            {
                names.Add(describeReader.GetString(0));
            }

            Assert.Contains("xact_commit_delta_in_hour", names);
            Assert.Contains("xact_commit_delta_hour_bridged", names);
        }

        Assert.Equal(59L, Convert.ToInt64(await Scalar(absolute, "xact_commit_delta_in_hour", theHour)));
        Assert.Equal(59L, Convert.ToInt64(await Scalar(absolute, "xact_commit_delta_hour_bridged", theHour)));
    }

    [Fact]
    public async Task Unknown_spec_name_is_not_a_valid_call()
    {
        // HourlyRollupExporter always takes a resolved RollupSpec (the caller resolves it via RollupSql.Specs);
        // this checks that RollupSql itself still rejects an unknown name, the seam the exporter relies on.
        Assert.Throws<ArgumentException>(() => RollupSql.Build("not_a_real_spec"));
    }
}
