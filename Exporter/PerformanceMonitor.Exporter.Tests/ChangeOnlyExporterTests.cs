using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// ChangeOnlyExporter tests, on the server_config shape (docs/optional-grains.md §2.2.10):
/// key = configuration_name (+ server_name), tracked column = value_configured. Uses the same
/// FakeDbDataReader shape OptionalTableExporterTests uses.
/// </summary>
public class ChangeOnlyExporterTests
{
    private static readonly OptionalTable ServerConfigTable = OptionalTables.TryGet("server_config")!;

    private static string NewOutDir() => Directory.CreateTempSubdirectory("darling-export-change-out-").FullName;
    private static string NewStateDir() => Path.Combine(Path.GetTempPath(), $"darling-export-change-state-{Guid.NewGuid():N}");

    private static List<DiscoveredColumn> ServerConfigColumns() => new()
    {
        new("server_name", "text", "text"),
        new("capture_time", "timestamp", "timestamp"),
        new("configuration_name", "text", "text"),
        new("value_configured", "int4", "int4"),
    };

    private static FakeDbDataReader Reader(params (string Server, DateTime Capture, string Config, int Value)[] rows)
    {
        var data = rows.Select(r => new object[] { r.Server, r.Capture, r.Config, r.Value }).ToList();
        return new FakeDbDataReader(
            ["server_name", "capture_time", "configuration_name", "value_configured"],
            ["text", "timestamp", "text", "int4"],
            data);
    }

    private static async Task<List<(string Kind, string Server, string Config, int Value)>> ReadEmitted(string outDir, string store, string table, DateTime fromUtc, DateTime toUtc)
    {
        var relative = OutputPaths.RelativeParquetPath(store, table, fromUtc, toUtc);
        var absolute = Path.Combine(outDir, store, relative.Replace('/', Path.DirectorySeparatorChar));
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT change_kind, server_name, configuration_name, value_configured FROM read_parquet('{absolute}') ORDER BY server_name, configuration_name";
        using var reader = command.ExecuteReader();
        var results = new List<(string, string, string, int)>();
        while (reader.Read())
        {
            results.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetInt32(3)));
        }
        return results;
    }

    private static Task<ChangeOnlyExportResult> Run(
        FakeDbDataReader reader, string outDir, ChangeStateStore? store, DateTime fromUtc, DateTime toUtc, bool forceBaseline = false) =>
        ChangeOnlyExporter.ExportAsync(
            ServerConfigTable, ServerConfigColumns(), "store_a", outDir,
            _ => Task.FromResult<DbDataReader>(reader), fromUtc, toUtc, toUtc, store, forceBaseline, CancellationToken.None);

    [Fact]
    public void Sha256_exists_in_the_pinned_DuckDB_NET()
    {
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT sha256(CAST('hello' AS VARCHAR))";
        var result = (string?)command.ExecuteScalar();
        Assert.False(string.IsNullOrEmpty(result));
        Assert.Equal(64, result!.Length);
    }

    // The real server_config shape carries config_id: Darling assigns a fresh one on every collection (a plain
    // bigint NOT NULL, so column discovery keeps it). A re-collection after a Darling restart has the same values
    // and new ids, and must emit nothing (2026-10-01: store_a emitted 4,753 "changed" rows with identical values).
    private static List<DiscoveredColumn> ServerConfigColumnsWithId() => new()
    {
        new("config_id", "bigint", "int8"),
        new("server_name", "text", "text"),
        new("capture_time", "timestamp", "timestamp"),
        new("configuration_name", "text", "text"),
        new("value_configured", "int4", "int4"),
    };

    private static FakeDbDataReader ReaderWithId(params (long Id, string Server, DateTime Capture, string Config, int Value)[] rows) =>
        new(
            ["config_id", "server_name", "capture_time", "configuration_name", "value_configured"],
            ["int8", "text", "timestamp", "text", "int4"],
            rows.Select(r => new object[] { r.Id, r.Server, r.Capture, r.Config, r.Value }).ToList());

    [Theory]
    [InlineData("server_config")]
    [InlineData("database_config")]
    [InlineData("database_scoped_config")]
    [InlineData("trace_flags")]
    [InlineData("query_store_health")]
    public void Config_tables_do_not_compare_the_darling_assigned_config_id(string tableName)
    {
        var table = OptionalTables.TryGet(tableName)!;
        Assert.Equal(ExportGrain.Change, table.Grain);
        Assert.Contains("config_id", table.Change!.Ignore);
    }

    [Fact]
    public async Task Recollection_with_new_config_ids_and_same_values_gives_zero_rows()
    {
        var outDir = NewOutDir();
        var stateDir = NewStateDir();
        try
        {
            var w1From = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Utc);
            var w1To = new DateTime(2026, 1, 1, 2, 0, 0, DateTimeKind.Utc);
            var w2From = w1To;
            var w2To = new DateTime(2026, 1, 1, 3, 0, 0, DateTimeKind.Utc);
            using var store = ChangeStateStore.Open(stateDir);

            Task<ChangeOnlyExportResult> RunWithId(FakeDbDataReader reader, DateTime from, DateTime to) =>
                ChangeOnlyExporter.ExportAsync(
                    ServerConfigTable, ServerConfigColumnsWithId(), "store_a", outDir,
                    _ => Task.FromResult<DbDataReader>(reader), from, to, to, store, false, CancellationToken.None);

            var w1 = await RunWithId(ReaderWithId(
                (101, "srv-1", w1From, "max degree of parallelism", 0),
                (102, "srv-1", w1From, "cost threshold for parallelism", 5)), w1From, w1To);
            Assert.Equal(2, w1.RowCount);

            // Darling restarts and re-collects: new config_ids, same values; one value really changes.
            var w2 = await RunWithId(ReaderWithId(
                (201, "srv-1", w2From.AddMinutes(5), "max degree of parallelism", 0),
                (202, "srv-1", w2From.AddMinutes(5), "cost threshold for parallelism", 50)), w2From, w2To);
            Assert.Equal(1, w2.RowCount);

            var rows = await ReadEmitted(outDir, "store_a", "server_config", w2From, w2To);
            var only = Assert.Single(rows);
            Assert.Equal(("changed", "srv-1", "cost threshold for parallelism", 50), only);
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
            if (Directory.Exists(stateDir)) Directory.Delete(stateDir, recursive: true);
        }
    }

    [Fact]
    public async Task W1_three_keys_are_all_new()
    {
        var outDir = NewOutDir();
        var stateDir = NewStateDir();
        try
        {
            var w1From = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Utc);
            var w1To = new DateTime(2026, 1, 1, 2, 0, 0, DateTimeKind.Utc);
            using var store = ChangeStateStore.Open(stateDir);

            var reader = Reader(
                ("srv-1", w1From, "max degree of parallelism", 0),
                ("srv-1", w1From, "cost threshold for parallelism", 5),
                ("srv-2", w1From, "max degree of parallelism", 4));

            var result = await Run(reader, outDir, store, w1From, w1To);
            Assert.Equal(3, result.RowCount);

            var rows = await ReadEmitted(outDir, "store_a", "server_config", w1From, w1To);
            Assert.Equal(3, rows.Count);
            Assert.All(rows, r => Assert.Equal("new", r.Kind));
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
            if (Directory.Exists(stateDir)) Directory.Delete(stateDir, recursive: true);
        }
    }

    [Fact]
    public async Task W2_same_values_sampled_twice_gives_zero_rows()
    {
        var outDir = NewOutDir();
        var stateDir = NewStateDir();
        try
        {
            var w1From = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Utc);
            var w1To = new DateTime(2026, 1, 1, 2, 0, 0, DateTimeKind.Utc);
            var w2From = w1To;
            var w2To = new DateTime(2026, 1, 1, 3, 0, 0, DateTimeKind.Utc);
            using var store = ChangeStateStore.Open(stateDir);

            await Run(Reader(("srv-1", w1From, "max degree of parallelism", 0)), outDir, store, w1From, w1To);

            var w2Reader = Reader(
                ("srv-1", w2From.AddMinutes(10), "max degree of parallelism", 0),
                ("srv-1", w2From.AddMinutes(40), "max degree of parallelism", 0));
            var result = await Run(w2Reader, outDir, store, w2From, w2To);

            Assert.Equal(0, result.RowCount);
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
            if (Directory.Exists(stateDir)) Directory.Delete(stateDir, recursive: true);
        }
    }

    [Fact]
    public async Task W3_change_then_change_back_gives_two_changed_rows()
    {
        var outDir = NewOutDir();
        var stateDir = NewStateDir();
        try
        {
            var w1From = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Utc);
            var w1To = new DateTime(2026, 1, 1, 2, 0, 0, DateTimeKind.Utc);
            var w3From = new DateTime(2026, 1, 1, 3, 0, 0, DateTimeKind.Utc);
            var w3To = new DateTime(2026, 1, 1, 4, 0, 0, DateTimeKind.Utc);
            using var store = ChangeStateStore.Open(stateDir);

            await Run(Reader(("srv-1", w1From, "max degree of parallelism", 0)), outDir, store, w1From, w1To);

            var w3Reader = Reader(
                ("srv-1", w3From.AddMinutes(10), "max degree of parallelism", 4),
                ("srv-1", w3From.AddMinutes(40), "max degree of parallelism", 0));
            var result = await Run(w3Reader, outDir, store, w3From, w3To);

            Assert.Equal(2, result.RowCount);
            var rows = await ReadEmitted(outDir, "store_a", "server_config", w3From, w3To);
            Assert.All(rows, r => Assert.Equal("changed", r.Kind));
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
            if (Directory.Exists(stateDir)) Directory.Delete(stateDir, recursive: true);
        }
    }

    [Fact]
    public async Task Change_to_ignore_only_column_gives_zero_rows()
    {
        // query_store_health: Key = [server_name, database_name], Ignore = [current_storage_size_mb].
        var outDir = NewOutDir();
        var stateDir = NewStateDir();
        try
        {
            var table = OptionalTables.TryGet("query_store_health")!;
            var columns = new List<DiscoveredColumn>
            {
                new("server_name", "text", "text"),
                new("capture_time", "timestamp", "timestamp"),
                new("database_name", "text", "text"),
                new("current_storage_size_mb", "int4", "int4"),
            };

            var w1From = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Utc);
            var w1To = new DateTime(2026, 1, 1, 2, 0, 0, DateTimeKind.Utc);
            var w2From = w1To;
            var w2To = new DateTime(2026, 1, 1, 3, 0, 0, DateTimeKind.Utc);

            using var store = ChangeStateStore.Open(stateDir);

            static FakeDbDataReader R(string server, DateTime capture, string db, int size) => new(
                ["server_name", "capture_time", "database_name", "current_storage_size_mb"],
                ["text", "timestamp", "text", "int4"],
                [[server, capture, db, size]]);

            await ChangeOnlyExporter.ExportAsync(
                table, columns, "store_a", outDir, _ => Task.FromResult<DbDataReader>(R("srv-1", w1From, "db1", 100)),
                w1From, w1To, w1To, store, forceBaseline: false, CancellationToken.None);

            var result = await ChangeOnlyExporter.ExportAsync(
                table, columns, "store_a", outDir, _ => Task.FromResult<DbDataReader>(R("srv-1", w2From.AddMinutes(5), "db1", 999)),
                w2From, w2To, w2To, store, forceBaseline: false, CancellationToken.None);

            Assert.Equal(0, result.RowCount);
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
            if (Directory.Exists(stateDir)) Directory.Delete(stateDir, recursive: true);
        }
    }

    [Fact]
    public async Task New_server_is_new()
    {
        var outDir = NewOutDir();
        var stateDir = NewStateDir();
        try
        {
            var w1From = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Utc);
            var w1To = new DateTime(2026, 1, 1, 2, 0, 0, DateTimeKind.Utc);
            var w2From = w1To;
            var w2To = new DateTime(2026, 1, 1, 3, 0, 0, DateTimeKind.Utc);
            using var store = ChangeStateStore.Open(stateDir);

            await Run(Reader(("srv-1", w1From, "max degree of parallelism", 0)), outDir, store, w1From, w1To);
            var result = await Run(Reader(("srv-2", w2From.AddMinutes(5), "max degree of parallelism", 0)), outDir, store, w2From, w2To);

            Assert.Equal(1, result.RowCount);
            var rows = await ReadEmitted(outDir, "store_a", "server_config", w2From, w2To);
            Assert.Equal("new", rows.Single().Kind);
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
            if (Directory.Exists(stateDir)) Directory.Delete(stateDir, recursive: true);
        }
    }

    [Fact]
    public async Task Midnight_window_baselines_every_present_key()
    {
        var outDir = NewOutDir();
        var stateDir = NewStateDir();
        try
        {
            var w1From = new DateTime(2026, 1, 1, 23, 0, 0, DateTimeKind.Utc);
            var w1To = new DateTime(2026, 1, 2, 0, 0, 0, DateTimeKind.Utc);
            var w2From = w1To;
            var w2To = new DateTime(2026, 1, 2, 1, 0, 0, DateTimeKind.Utc);
            using var store = ChangeStateStore.Open(stateDir);

            await Run(Reader(("srv-1", w1From, "max degree of parallelism", 0)), outDir, store, w1From, w1To);

            // w2 is not the baseline window (2026-01-02 00:00 -> 01:00 is not itself midnight for its `to`).
            // Use the window ending exactly at the NEXT midnight to exercise the baseline path.
            var baselineFrom = new DateTime(2026, 1, 2, 23, 0, 0, DateTimeKind.Utc);
            var baselineTo = new DateTime(2026, 1, 3, 0, 0, 0, DateTimeKind.Utc);
            var result = await Run(Reader(("srv-1", baselineFrom.AddMinutes(5), "max degree of parallelism", 0)), outDir, store, baselineFrom, baselineTo);

            Assert.True(result.Baseline);
            Assert.Equal(1, result.RowCount);
            var rows = await ReadEmitted(outDir, "store_a", "server_config", baselineFrom, baselineTo);
            Assert.Equal("baseline", rows.Single().Kind);
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
            if (Directory.Exists(stateDir)) Directory.Delete(stateDir, recursive: true);
        }
    }

    [Fact]
    public async Task Changed_tracked_set_forces_a_baseline()
    {
        var outDir = NewOutDir();
        var stateDir = NewStateDir();
        try
        {
            var w1From = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Utc);
            var w1To = new DateTime(2026, 1, 1, 2, 0, 0, DateTimeKind.Utc);
            var w2From = w1To;
            var w2To = new DateTime(2026, 1, 1, 3, 0, 0, DateTimeKind.Utc);
            using var store = ChangeStateStore.Open(stateDir);

            await Run(Reader(("srv-1", w1From, "max degree of parallelism", 0)), outDir, store, w1From, w1To);

            // A stale, wrong tracked-set hash forces the change detector to treat the tracked set as changed.
            store.SetTrackedSetHash("store_a", "server_config", "stale-hash-not-matching-anything");

            var result = await Run(Reader(("srv-1", w2From.AddMinutes(5), "max degree of parallelism", 0)), outDir, store, w2From, w2To);

            Assert.True(result.Baseline);
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
            if (Directory.Exists(stateDir)) Directory.Delete(stateDir, recursive: true);
        }
    }

    [Fact]
    public async Task Deleted_state_file_gives_all_new()
    {
        var outDir = NewOutDir();
        var stateDir = NewStateDir();
        try
        {
            var w1From = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Utc);
            var w1To = new DateTime(2026, 1, 1, 2, 0, 0, DateTimeKind.Utc);
            var w2From = w1To;
            var w2To = new DateTime(2026, 1, 1, 3, 0, 0, DateTimeKind.Utc);

            using (var store = ChangeStateStore.Open(stateDir))
            {
                await Run(Reader(("srv-1", w1From, "max degree of parallelism", 0)), outDir, store, w1From, w1To);
            }

            File.Delete(Path.Combine(stateDir, "change_state.duckdb"));

            // A missing (not corrupt) file is simply recreated empty (WasRecreated is only set for a corrupt
            // recovery, per ChangeStateStore.Open's own docs) -- the empty prev set is what makes every key 'new'.
            using var recreated = ChangeStateStore.Open(stateDir);

            var result = await Run(Reader(("srv-1", w2From.AddMinutes(5), "max degree of parallelism", 0)), outDir, recreated, w2From, w2To);
            var rows = await ReadEmitted(outDir, "store_a", "server_config", w2From, w2To);
            Assert.All(rows, r => Assert.Equal("new", r.Kind));
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
            if (Directory.Exists(stateDir)) Directory.Delete(stateDir, recursive: true);
        }
    }

    [Fact]
    public async Task Rerunning_w2_gives_byte_identical_output_and_state()
    {
        var outDir = NewOutDir();
        var stateDir = NewStateDir();
        try
        {
            var w1From = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Utc);
            var w1To = new DateTime(2026, 1, 1, 2, 0, 0, DateTimeKind.Utc);
            var w2From = w1To;
            var w2To = new DateTime(2026, 1, 1, 3, 0, 0, DateTimeKind.Utc);
            using var store = ChangeStateStore.Open(stateDir);

            await Run(Reader(("srv-1", w1From, "max degree of parallelism", 0)), outDir, store, w1From, w1To);

            var w2Reader1 = Reader(("srv-1", w2From.AddMinutes(5), "max degree of parallelism", 4));
            var result1 = await Run(w2Reader1, outDir, store, w2From, w2To);
            var sha1 = result1.Parts.Single().Sha256;

            var w2Reader2 = Reader(("srv-1", w2From.AddMinutes(5), "max degree of parallelism", 4));
            var result2 = await Run(w2Reader2, outDir, store, w2From, w2To);
            var sha2 = result2.Parts.Single().Sha256;

            Assert.Equal(sha1, sha2);
            Assert.Equal(result1.RowCount, result2.RowCount);
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
            if (Directory.Exists(stateDir)) Directory.Delete(stateDir, recursive: true);
        }
    }

    /// <summary>
    /// docs/optional-grains.md §2.2.7's `group_id` note, exercised generically: `ag_replica_states`' Change.Key
    /// is [server_name, ag_name, replica_server_name, group_id], but the discovered columns below (a pre-V151
    /// store) never include group_id. ExportAsync must drop it from the key instead of throwing, and the
    /// returned ResolvedKeyColumns must say so.
    /// </summary>
    [Fact]
    public async Task Key_column_not_discovered_is_dropped_from_the_key()
    {
        var outDir = NewOutDir();
        var stateDir = NewStateDir();
        try
        {
            var table = OptionalTables.TryGet("ag_replica_states")!;
            Assert.Equal(["server_name", "ag_name", "replica_server_name", "group_id"], table.Change!.Key);

            var columns = new List<DiscoveredColumn>
            {
                new("server_name", "text", "text"),
                new("collection_time", "timestamp", "timestamp"),
                new("ag_name", "text", "text"),
                new("replica_server_name", "text", "text"),
                new("role_desc", "text", "text"),
                // no group_id: a pre-V151 store's discovery never returns it.
            };

            var w1From = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Utc);
            var w1To = new DateTime(2026, 1, 1, 2, 0, 0, DateTimeKind.Utc);

            static FakeDbDataReader R() => new(
                ["server_name", "collection_time", "ag_name", "replica_server_name", "role_desc"],
                ["text", "timestamp", "text", "text", "text"],
                [["srv-1", new DateTime(2026, 1, 1, 1, 5, 0, DateTimeKind.Utc), "ag1", "srv-2", "PRIMARY"]]);

            using var store = ChangeStateStore.Open(stateDir);
            var result = await ChangeOnlyExporter.ExportAsync(
                table, columns, "store_a", outDir, _ => Task.FromResult<DbDataReader>(R()),
                w1From, w1To, w1To, store, forceBaseline: false, CancellationToken.None);

            Assert.Equal(["server_name", "ag_name", "replica_server_name"], result.ResolvedKeyColumns);
            Assert.Equal(1, result.RowCount);
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
            if (Directory.Exists(stateDir)) Directory.Delete(stateDir, recursive: true);
        }
    }

    /// <summary>
    /// docs/optional-grains.md §2.3.1's one exception: ag_database_replica_states is Grain.Change AND carries a
    /// RollupName, so ONE ExportAsync call over ONE reader (never re-invoked) must produce both the Change
    /// Parquet part and the ag_database_replica_states_hourly rollup part.
    /// </summary>
    [Fact]
    public async Task Dual_output_change_and_rollup_from_one_read()
    {
        var outDir = NewOutDir();
        var stateDir = NewStateDir();
        try
        {
            var table = OptionalTables.TryGet("ag_database_replica_states")!;
            Assert.NotNull(table.RollupName);

            var columns = new List<DiscoveredColumn>
            {
                new("server_name", "text", "text"),
                new("collection_time", "timestamp", "timestamp"),
                new("ag_name", "text", "text"),
                new("database_name", "text", "text"),
                new("replica_server_name", "text", "text"),
                new("group_id", "text", "text"),
                new("synchronization_state_desc", "text", "text"),
                new("log_send_queue_size", "int8", "int8"),
                new("redo_queue_size", "int8", "int8"),
                new("secondary_lag_seconds", "int8", "int8"),
                new("log_send_rate", "int8", "int8"),
                new("redo_rate", "int8", "int8"),
                new("est_redo_completion_time_min", "float8", "float8"),
                new("est_send_drain_time_min", "float8", "float8"),
                new("last_commit_time", "timestamp", "timestamp"),
                new("last_hardened_time", "timestamp", "timestamp"),
                new("last_redone_time", "timestamp", "timestamp"),
                new("last_received_time", "timestamp", "timestamp"),
                new("last_hardened_lsn", "text", "text"),
                new("last_commit_lsn", "text", "text"),
            };

            var names = columns.Select(c => c.Name).ToArray();
            var pgTypes = columns.Select(c => c.DataType).ToArray();

            object[] Row(DateTime t, string syncState, long lag) =>
            [
                "srv-1", t, "ag1", "db1", "srv-2", "grp-1", syncState,
                (long)100, (long)200, lag, (long)10, (long)20,
                (double)1.5, (double)2.5,
                t, t, t, t, "0x01", "0x02",
            ];

            var w1From = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Utc);
            var w1To = new DateTime(2026, 1, 1, 2, 0, 0, DateTimeKind.Utc);
            var reader = new FakeDbDataReader(names, pgTypes,
            [
                Row(w1From.AddMinutes(5), "SYNCHRONIZED", 5),
                Row(w1From.AddMinutes(35), "SYNCHRONIZED", 7),
            ]);

            using var store = ChangeStateStore.Open(stateDir);
            var result = await ChangeOnlyExporter.ExportAsync(
                table, columns, "store_a", outDir, _ => Task.FromResult<DbDataReader>(reader),
                w1From, w1To, w1To, store, forceBaseline: false, CancellationToken.None);

            Assert.NotNull(result.RollupPart);
            Assert.Equal(1, result.RollupRowCount);
            Assert.True(result.RollupPart!.RowCount > 0);

            var relative = OutputPaths.RelativeParquetPath("store_a", "ag_database_replica_states_hourly", w1From, w1To);
            var absolute = Path.Combine(outDir, "store_a", relative.Replace('/', Path.DirectorySeparatorChar));
            Assert.True(File.Exists(absolute));

            using var connection = new DuckDBConnection("Data Source=:memory:");
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT log_send_queue_size_avg, secondary_lag_seconds_last FROM read_parquet('{absolute}')";
            using var rollupReader = command.ExecuteReader();
            Assert.True(rollupReader.Read());
            Assert.Equal(100.0, rollupReader.GetDouble(0));
            Assert.Equal(7L, rollupReader.GetInt64(1));
            rollupReader.Close();

            command.CommandText = $"SELECT count(*) FILTER (WHERE source_store = 'store_a'), count(*) FROM read_parquet('{absolute}')";
            using var labelReader = command.ExecuteReader();
            Assert.True(labelReader.Read());
            Assert.Equal(labelReader.GetInt64(1), labelReader.GetInt64(0)); // every rollup row labeled, none NULL
            Assert.True(labelReader.GetInt64(0) > 0);
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
            if (Directory.Exists(stateDir)) Directory.Delete(stateDir, recursive: true);
        }
    }
}
