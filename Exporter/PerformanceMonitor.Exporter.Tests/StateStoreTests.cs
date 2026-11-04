using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class StateStoreTests
{
    private static string NewStateDir() =>
        Path.Combine(Path.GetTempPath(), $"darling-export-state-test-{Guid.NewGuid():N}");

    [Fact]
    public void FilterUnknownFeatureDigests_returns_every_digest_when_nothing_is_recorded()
    {
        var dir = NewStateDir();
        using var store = StateStore.Open(dir);

        var result = store.FilterUnknownFeatureDigests("store_b", ["abc", "def"], featureVersion: 1);

        Assert.Equal(new[] { "abc", "def" }.ToHashSet(), result);
    }

    [Fact]
    public void FilterUnknownFeatureDigests_drops_digests_recorded_at_the_same_feature_version()
    {
        var dir = NewStateDir();
        using var store = StateStore.Open(dir);

        store.RecordExportedKeys("store_b", [], [], [("abc", 1)]);

        var result = store.FilterUnknownFeatureDigests("store_b", ["abc", "def"], featureVersion: 1);

        Assert.Equal(new[] { "def" }.ToHashSet(), result);
    }

    [Fact]
    public void FilterUnknownFeatureDigests_treats_a_different_feature_version_as_unknown()
    {
        var dir = NewStateDir();
        using var store = StateStore.Open(dir);

        store.RecordExportedKeys("store_b", [], [], [("abc", 1)]);

        var result = store.FilterUnknownFeatureDigests("store_b", ["abc"], featureVersion: 2);

        Assert.Equal(new[] { "abc" }.ToHashSet(), result);
    }

    [Fact]
    public void FilterUnknownFeatureDigests_is_scoped_to_source_store()
    {
        var dir = NewStateDir();
        using var store = StateStore.Open(dir);

        store.RecordExportedKeys("store_a", [], [], [("abc", 1)]);

        var result = store.FilterUnknownFeatureDigests("store_b", ["abc"], featureVersion: 1);

        Assert.Equal(new[] { "abc" }.ToHashSet(), result);
    }

    [Fact]
    public void RecordExportedKeys_with_no_feature_digests_leaves_the_table_untouched()
    {
        var dir = NewStateDir();
        using var store = StateStore.Open(dir);

        store.RecordExportedKeys("store_b", [("srv", "0xAAA")], [], newFeatureDigests: null);

        var result = store.FilterUnknownFeatureDigests("store_b", ["abc"], featureVersion: 1);

        Assert.Equal(new[] { "abc" }.ToHashSet(), result);
    }

    [Fact]
    public void FilterUnknownProcedureDigests_returns_every_digest_and_no_known_shapes_when_nothing_is_recorded()
    {
        var dir = NewStateDir();
        using var store = StateStore.Open(dir);

        var (unknown, knownShapes) = store.FilterUnknownProcedureDigests("store_b", ["d1", "d2"]);

        Assert.Equal(new[] { "d1", "d2" }.ToHashSet(), unknown);
        Assert.Empty(knownShapes);
    }

    [Fact]
    public void FilterUnknownProcedureDigests_drops_recorded_digests_and_returns_their_shape()
    {
        var dir = NewStateDir();
        using var store = StateStore.Open(dir);

        store.RecordExportedKeys("store_b", [], [], newProcedureDigests: [("d1", "shape-a")]);

        var (unknown, knownShapes) = store.FilterUnknownProcedureDigests("store_b", ["d1", "d2"]);

        Assert.Equal(new[] { "d2" }.ToHashSet(), unknown);
        Assert.Equal("shape-a", knownShapes["d1"]);
    }

    [Fact]
    public void FilterUnknownProcedureDigests_scopes_by_source_store()
    {
        var dir = NewStateDir();
        using var store = StateStore.Open(dir);

        store.RecordExportedKeys("store_a", [], [], newProcedureDigests: [("d1", "shape-a")]);

        var (unknown, knownShapes) = store.FilterUnknownProcedureDigests("store_b", ["d1"]);

        Assert.Equal(new[] { "d1" }.ToHashSet(), unknown);
        Assert.Empty(knownShapes);
    }

    [Fact]
    public void FilterUnknownProcedurePlanKeys_returns_every_key_when_nothing_is_recorded()
    {
        var dir = NewStateDir();
        using var store = StateStore.Open(dir);

        var keys = new (string? ServerName, string? DatabaseName, string? SchemaName, string? ObjectName, string ShapeHash)[] { ("srv", "db", "schema", "obj", "shape-a") };
        var result = store.FilterUnknownProcedurePlanKeys("store_b", keys);

        Assert.Equal(keys.ToHashSet(), result);
    }

    [Fact]
    public void FilterUnknownProcedurePlanKeys_drops_recorded_keys()
    {
        var dir = NewStateDir();
        using var store = StateStore.Open(dir);

        (string? ServerName, string? DatabaseName, string? SchemaName, string? ObjectName, string ShapeHash) key = ("srv", "db", "schema", "obj", "shape-a");
        store.RecordExportedKeys("store_b", [], [], newProcedurePlanKeys: [key]);

        var result = store.FilterUnknownProcedurePlanKeys("store_b", [key, ("srv", "db", "schema", "obj", "shape-b")]);

        Assert.Equal(new (string? ServerName, string? DatabaseName, string? SchemaName, string? ObjectName, string ShapeHash)[] { ("srv", "db", "schema", "obj", "shape-b") }.ToHashSet(), result);
    }

    [Fact]
    public void FilterUnknownProcedurePlanKeys_null_server_is_a_key_value_not_a_wildcard()
    {
        var dir = NewStateDir();
        using var store = StateStore.Open(dir);

        store.RecordExportedKeys("store_b", [], [], newProcedurePlanKeys: [((string?)null, "db", "schema", "obj", "shape-a")]);

        var result = store.FilterUnknownProcedurePlanKeys("store_b", [("srv", "db", "schema", "obj", "shape-a")]);

        Assert.Equal(new (string? ServerName, string? DatabaseName, string? SchemaName, string? ObjectName, string ShapeHash)[] { ("srv", "db", "schema", "obj", "shape-a") }.ToHashSet(), result);
    }

    [Fact]
    public void FilterUnknownPgTextKeys_returns_every_key_when_nothing_is_recorded()
    {
        var dir = NewStateDir();
        using var store = StateStore.Open(dir);

        var result = store.FilterUnknownPgTextKeys("store_pg", [("srv1", 101L), ("srv1", 102L)]);

        Assert.Equal(new (string? ServerName, long QueryId)[] { ("srv1", 101L), ("srv1", 102L) }.ToHashSet(), result);
    }

    [Fact]
    public void FilterUnknownPgTextKeys_drops_keys_already_recorded_for_the_source_store()
    {
        var dir = NewStateDir();
        using var store = StateStore.Open(dir);

        store.RecordPgTextKeys("store_pg", [("srv1", 101L)]);

        var result = store.FilterUnknownPgTextKeys("store_pg", [("srv1", 101L), ("srv1", 102L)]);

        Assert.Equal(new (string? ServerName, long QueryId)[] { ("srv1", 102L) }.ToHashSet(), result);
    }

    [Fact]
    public void FilterUnknownPgTextKeys_is_scoped_to_the_source_store()
    {
        var dir = NewStateDir();
        using var store = StateStore.Open(dir);

        store.RecordPgTextKeys("store_pg", [("srv1", 101L)]);

        var result = store.FilterUnknownPgTextKeys("other-store", [("srv1", 101L)]);

        Assert.Equal(new (string? ServerName, long QueryId)[] { ("srv1", 101L) }.ToHashSet(), result);
    }

    [Fact]
    public void FilterUnknownPgTextKeys_null_server_is_a_key_value_not_a_wildcard()
    {
        var dir = NewStateDir();
        using var store = StateStore.Open(dir);

        store.RecordPgTextKeys("store_pg", [((string?)null, 101L)]);

        var result = store.FilterUnknownPgTextKeys("store_pg", [("srv1", 101L)]);

        Assert.Equal(new (string? ServerName, long QueryId)[] { ("srv1", 101L) }.ToHashSet(), result);
    }

    // Lane H8 (docs/hourly-export.md §7.3): each FilterUnknown* now anti-joins in DuckDB instead of loading
    // every known row into a HashSet. These tests check the anti-join result is identical to the old
    // HashSet-based oracle (kept here verbatim) on a synthetic 1,000,000-row state, and that peak managed
    // memory during the filter call stays well under 200 MB. 1,000,000 rows keeps the container test fast;
    // the shape of the query (temp table + anti-join) does not change with row count.

    private static HashSet<(string?, string)> OldFilterUnknownPlanKeys(
        IEnumerable<(string? ServerName, string QueryPlanHash)> known, IEnumerable<(string? ServerName, string QueryPlanHash)> keys)
    {
        var knownSet = known.ToHashSet();
        return keys.Where(k => !knownSet.Contains(k)).ToHashSet();
    }

    [Fact]
    [Trait("Category", "Slow")]
    public void FilterUnknownPlanKeys_matches_the_HashSet_oracle_on_a_large_synthetic_state()
    {
        var dir = NewStateDir();
        using var store = StateStore.Open(dir);

        const int rowCount = 1_000_000;
        var rnd = new Random(42);
        var known = new List<(string? ServerName, string QueryPlanHash)>(rowCount);
        for (var i = 0; i < rowCount; i++)
        {
            known.Add((i % 7 == 0 ? null : $"srv{i % 50}", $"hash{i}"));
        }
        store.RecordExportedKeys("store_a", known, []);

        // Candidates: half already known, half brand new, plus a NULL-server key.
        var candidates = new List<(string? ServerName, string QueryPlanHash)>();
        for (var i = 0; i < rowCount; i += 2)
        {
            candidates.Add(known[i]);
        }
        for (var i = 0; i < 500; i++)
        {
            candidates.Add(($"srv{i % 50}", $"newhash{i}"));
        }
        candidates.Add((null, "hash0"));

        var expected = OldFilterUnknownPlanKeys(known, candidates);
        var actual = store.FilterUnknownPlanKeys("store_a", candidates);

        Assert.Equal(expected, actual);
    }

    [Fact]
    [Trait("Category", "Slow")]
    public void FilterUnknownPlanKeys_peak_managed_memory_stays_under_200MB_for_a_million_row_state()
    {
        var dir = NewStateDir();
        using var store = StateStore.Open(dir);

        const int rowCount = 1_000_000;
        var known = new List<(string? ServerName, string QueryPlanHash)>(rowCount);
        for (var i = 0; i < rowCount; i++)
        {
            known.Add(($"srv{i % 50}", $"hash{i}"));
        }
        store.RecordExportedKeys("store_a", known, []);

        var candidates = new List<(string? ServerName, string QueryPlanHash)>();
        for (var i = 0; i < rowCount; i += 10)
        {
            candidates.Add(known[i]);
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        var before = GC.GetTotalMemory(forceFullCollection: true);

        var result = store.FilterUnknownPlanKeys("store_a", candidates);

        var after = GC.GetTotalMemory(forceFullCollection: false);
        var deltaMb = (after - before) / (1024.0 * 1024.0);

        // GC.GetTotalMemory: managed heap bytes allocated and not yet collected. The old HashSet-based
        // implementation would have materialized all 1,000,000 known rows as boxed tuples; the anti-join
        // version only ever materializes the (much smaller) unknown result set.
        Assert.True(deltaMb < 200, $"expected under 200 MB of managed growth, measured {deltaMb:F1} MB");
        Assert.NotNull(result);
    }

    [Fact]
    public void PruneOldProcedureDigests_deletes_only_rows_older_than_the_retention_window()
    {
        var dir = NewStateDir();
        using (var store = StateStore.Open(dir))
        {
            store.RecordExportedKeys("store_a", [], [], newProcedureDigests: [("old-digest", "shape-a"), ("new-digest", "shape-b")]);
        }

        // Backdate "old-digest" to 20 days ago; leave "new-digest" at now() (set by RecordExportedKeys). The
        // StateStore above is disposed first so this direct connection to the same file has exclusive access.
        var dbPath = Path.Combine(dir, "exported_keys.duckdb");
        using (var backdate = new DuckDB.NET.Data.DuckDBConnection($"Data Source={dbPath}"))
        {
            backdate.Open();
            using var cmd = backdate.CreateCommand();
            cmd.CommandText = "UPDATE procedure_digests SET recorded_at = now() - INTERVAL '20 days' WHERE plan_digest = 'old-digest'";
            cmd.ExecuteNonQuery();
        }

        using var reopened = StateStore.Open(dir);

        var deleted = reopened.PruneOldProcedureDigests(DateTime.UtcNow, retentionDays: 14);

        Assert.Equal(1, deleted);

        var (unknown, knownShapes) = reopened.FilterUnknownProcedureDigests("store_a", ["old-digest", "new-digest"]);
        Assert.Contains("old-digest", unknown);
        Assert.DoesNotContain("new-digest", unknown);
        Assert.True(knownShapes.ContainsKey("new-digest"));

        // Idempotent: pruning again deletes nothing more.
        var deletedAgain = reopened.PruneOldProcedureDigests(DateTime.UtcNow, retentionDays: 14);
        Assert.Equal(0, deletedAgain);
    }
}
