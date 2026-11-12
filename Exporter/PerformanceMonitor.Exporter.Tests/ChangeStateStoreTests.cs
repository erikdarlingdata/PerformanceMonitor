using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using DuckDB.NET.Data;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class ChangeStateStoreTests
{
    private static string NewStateDir() =>
        Path.Combine(Path.GetTempPath(), $"darling-export-changestate-test-{Guid.NewGuid():N}");

    private static Dictionary<string, string> ReadTempTable(DuckDBConnection connection, string tempTable)
    {
        var result = new Dictionary<string, string>();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT key_hash, value_hash FROM {tempTable}";
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            result[reader.GetString(0)] = reader.GetString(1);
        }
        return result;
    }

    [Fact]
    public void LoadPrevAsOf_reflects_the_latest_hash_per_key_across_three_windows()
    {
        var dir = NewStateDir();
        using var store = ChangeStateStore.Open(dir);

        var w1 = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Utc);
        var w2 = new DateTime(2026, 1, 1, 2, 0, 0, DateTimeKind.Utc);
        var w3 = new DateTime(2026, 1, 1, 3, 0, 0, DateTimeKind.Utc);

        // key "a" changes in w1 and again in w3; key "b" only ever appears in w1.
        store.Record("store_a", "server_config", [("a", "hash-a1")], w1);
        store.Record("store_a", "server_config", [("b", "hash-b1")], w1);
        store.Record("store_a", "server_config", [("a", "hash-a3")], w3);

        using var target = new DuckDBConnection("Data Source=:memory:");
        target.Open();

        var asOfAfterW1 = store.LoadPrevAsOf("store_a", "server_config", w2, target);
        var afterW1 = ReadTempTable(target, asOfAfterW1);
        Assert.Equal("hash-a1", afterW1["a"]);
        Assert.Equal("hash-b1", afterW1["b"]);

        var asOfAfterW3 = store.LoadPrevAsOf("store_a", "server_config", w3.AddSeconds(1), target);
        var afterW3 = ReadTempTable(target, asOfAfterW3);
        Assert.Equal("hash-a3", afterW3["a"]);
        Assert.Equal("hash-b1", afterW3["b"]);
    }

    [Fact]
    public void DeleteWindow_then_Record_replaces_only_that_windows_rows()
    {
        var dir = NewStateDir();
        using var store = ChangeStateStore.Open(dir);

        var w1 = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Utc);
        var w2 = new DateTime(2026, 1, 1, 2, 0, 0, DateTimeKind.Utc);
        var w3 = new DateTime(2026, 1, 1, 3, 0, 0, DateTimeKind.Utc);

        store.Record("store_a", "server_config", [("a", "hash-w1")], w1);
        store.Record("store_a", "server_config", [("a", "hash-w2-bad")], w2);
        store.Record("store_a", "server_config", [("a", "hash-w3")], w3);

        // Re-run the middle window: the same output must replace only w2's row.
        store.DeleteWindow("store_a", "server_config", w2);
        store.Record("store_a", "server_config", [("a", "hash-w2-good")], w2);

        using var target = new DuckDBConnection("Data Source=:memory:");
        target.Open();

        var asOfAfterW2 = store.LoadPrevAsOf("store_a", "server_config", w2.AddSeconds(1), target);
        var afterW2 = ReadTempTable(target, asOfAfterW2);
        Assert.Equal("hash-w2-good", afterW2["a"]);

        var asOfAfterW3 = store.LoadPrevAsOf("store_a", "server_config", w3.AddSeconds(1), target);
        var afterW3 = ReadTempTable(target, asOfAfterW3);
        Assert.Equal("hash-w3", afterW3["a"]);
    }

    [Fact]
    public void Prune_leaves_the_asof_answer_unchanged_for_from_at_or_after_the_cutoff()
    {
        var dir = NewStateDir();
        using var store = ChangeStateStore.Open(dir);

        var now = new DateTime(2026, 1, 10, 0, 0, 0, DateTimeKind.Utc);
        var w1 = now.AddDays(-20);
        var w2 = now.AddDays(-15);
        var w3 = now.AddDays(-10);
        var w4 = now.AddDays(-1);

        store.Record("store_a", "server_config", [("a", "hash-w1")], w1);
        store.Record("store_a", "server_config", [("a", "hash-w2")], w2);
        store.Record("store_a", "server_config", [("a", "hash-w3")], w3);
        store.Record("store_a", "server_config", [("a", "hash-w4")], w4);

        using var target = new DuckDBConnection("Data Source=:memory:");
        target.Open();

        var cutoff = now.AddDays(-7);
        var before = ReadTempTable(target, store.LoadPrevAsOf("store_a", "server_config", cutoff.AddSeconds(1), target));

        store.Prune(now, keepDays: 7);

        var after = ReadTempTable(target, store.LoadPrevAsOf("store_a", "server_config", cutoff.AddSeconds(1), target));
        Assert.Equal(before["a"], after["a"]);

        var afterAtW4 = ReadTempTable(target, store.LoadPrevAsOf("store_a", "server_config", w4.AddSeconds(1), target));
        Assert.Equal("hash-w4", afterAtW4["a"]);
    }

    [Fact]
    public void TrackedSetHash_round_trips()
    {
        var dir = NewStateDir();
        using var store = ChangeStateStore.Open(dir);

        Assert.Null(store.GetTrackedSetHash("store_a", "server_config"));

        store.SetTrackedSetHash("store_a", "server_config", "hash-v1");
        Assert.Equal("hash-v1", store.GetTrackedSetHash("store_a", "server_config"));

        store.SetTrackedSetHash("store_a", "server_config", "hash-v2");
        Assert.Equal("hash-v2", store.GetTrackedSetHash("store_a", "server_config"));
    }

    [Fact]
    public void Open_recreates_a_corrupt_file_and_renames_the_original()
    {
        var dir = NewStateDir();
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, "change_state.duckdb");
        File.WriteAllBytes(path, [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);

        using var store = ChangeStateStore.Open(dir);

        Assert.True(store.WasRecreated);
        Assert.True(File.Exists(path));
        var corruptFiles = Directory.GetFiles(dir, "change_state.duckdb.corrupt-*");
        Assert.Single(corruptFiles);

        // The fresh file is usable.
        store.SetTrackedSetHash("store_a", "server_config", "hash-v1");
        Assert.Equal("hash-v1", store.GetTrackedSetHash("store_a", "server_config"));
    }

    [Fact]
    public void MinWindowTo_returns_the_earliest_recorded_window_and_null_when_nothing_is_recorded()
    {
        var dir = NewStateDir();
        using var store = ChangeStateStore.Open(dir);

        Assert.Null(store.MinWindowTo("store_a", "server_config"));

        var w1 = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Utc);
        var w2 = new DateTime(2026, 1, 1, 2, 0, 0, DateTimeKind.Utc);
        store.Record("store_a", "server_config", [("a", "hash-w2")], w2);
        store.Record("store_a", "server_config", [("a", "hash-w1")], w1);

        Assert.Equal(w1, store.MinWindowTo("store_a", "server_config"));
    }

    [Fact]
    public void State_is_isolated_across_stores_and_tables()
    {
        var dir = NewStateDir();
        using var store = ChangeStateStore.Open(dir);

        var w1 = new DateTime(2026, 1, 1, 1, 0, 0, DateTimeKind.Utc);

        store.Record("store_a", "server_config", [("a", "store_a-server_config-a")], w1);
        store.Record("store_a", "database_config", [("a", "store_a-database_config-a")], w1);
        store.Record("store_b", "server_config", [("a", "store_b-server_config-a")], w1);

        using var target = new DuckDBConnection("Data Source=:memory:");
        target.Open();

        var asOf = w1.AddSeconds(1);
        Assert.Equal("store_a-server_config-a",
            ReadTempTable(target, store.LoadPrevAsOf("store_a", "server_config", asOf, target))["a"]);
        Assert.Equal("store_a-database_config-a",
            ReadTempTable(target, store.LoadPrevAsOf("store_a", "database_config", asOf, target))["a"]);
        Assert.Equal("store_b-server_config-a",
            ReadTempTable(target, store.LoadPrevAsOf("store_b", "server_config", asOf, target))["a"]);
    }
}
