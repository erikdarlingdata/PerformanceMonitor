using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class DimensionOriginTests
{
    private static readonly DateTime Now = new(2026, 9, 26, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime From = new(2026, 9, 26, 8, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime To = new(2026, 9, 26, 9, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void Validate_defaults_dimension_origin_to_both()
    {
        var options = ExportOptions.Validate(
            "store_a", "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", "/tmp/out", null,
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now);

        Assert.Equal(DimensionOrigins.Both, options.DimensionOrigin);
    }

    [Theory]
    [InlineData(DimensionOrigins.QueryStats)]
    [InlineData(DimensionOrigins.QueryStore)]
    [InlineData(DimensionOrigins.Both)]
    public void Validate_accepts_every_valid_dimension_origin(string origin)
    {
        var options = ExportOptions.Validate(
            "store_a", "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", "/tmp/out", null,
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now,
            dimensionOrigin: origin);

        Assert.Equal(origin, options.DimensionOrigin);
    }

    [Fact]
    public void Validate_rejects_an_unknown_dimension_origin()
    {
        Assert.Throws<ArgumentException>(() =>
            ExportOptions.Validate(
                "store_a", "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", "/tmp/out", null,
                "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now,
                dimensionOrigin: "not_a_real_origin"));
    }

    [Fact]
    public void PartFor_suffixes_only_the_query_store_only_origin()
    {
        Assert.Equal("qs", DimensionOrigins.PartFor(DimensionOrigins.QueryStore));
        Assert.Null(DimensionOrigins.PartFor(DimensionOrigins.QueryStats));
        Assert.Null(DimensionOrigins.PartFor(DimensionOrigins.Both));
    }

    /// <summary>
    /// Enumerates every (table, origin, part) path for one window's two hourly calls (a fresh
    /// call with --dimension-origin query_stats over every table except query_store_stats, and
    /// a Query Store call with --dimension-origin query_store over query_store_stats plus
    /// plan_shape/query_text) and asserts no two of them share a file path. This is the
    /// collision the brief calls out: both calls close the same window, so their outputs must
    /// never overwrite each other in S3.
    /// </summary>
    [Fact]
    public void No_two_table_origin_part_combinations_for_one_window_share_a_file_path()
    {
        var paths = new List<string>();

        void AddPath(string table, string? part) =>
            paths.Add(OutputPaths.ParquetFilePath("/out", "store_a", table, From, To, part));

        // Fresh call: every raw/rollup/system_health/procedure table, plus plan_shape/query_text
        // and their plan_features companions from query_stats (unsuffixed).
        foreach (var table in RawTables.All.Concat(HourlyTables.All).Concat(SystemHealthTables.All))
        {
            AddPath(table, part: null);
        }
        AddPath(DimensionTables.ProcedurePlan, part: null);
        AddPath(PlanFeatureTables.PlanFeatures, part: "procedure"); // procedure_plan's own companions
        AddPath(PlanFeatureTables.PlanWarnings, part: "procedure");
        AddPath(PlanFeatureTables.PlanMissingIndexes, part: "procedure");
        AddPath(DimensionTables.PlanShape, DimensionOrigins.PartFor(DimensionOrigins.QueryStats));
        AddPath(DimensionTables.QueryText, DimensionOrigins.PartFor(DimensionOrigins.QueryStats));
        AddPath(PlanFeatureTables.PlanFeatures, DimensionOrigins.PartFor(DimensionOrigins.QueryStats));
        AddPath(PlanFeatureTables.PlanWarnings, DimensionOrigins.PartFor(DimensionOrigins.QueryStats));
        AddPath(PlanFeatureTables.PlanMissingIndexes, DimensionOrigins.PartFor(DimensionOrigins.QueryStats));

        // Query Store call: query_store_stats, plus plan_shape/query_text (and companions) from
        // Query Store (suffixed "qs").
        AddPath(QueryStoreTables.QueryStoreStats, part: null);
        AddPath(DimensionTables.PlanShape, DimensionOrigins.PartFor(DimensionOrigins.QueryStore));
        AddPath(DimensionTables.QueryText, DimensionOrigins.PartFor(DimensionOrigins.QueryStore));
        AddPath(PlanFeatureTables.PlanFeatures, DimensionOrigins.PartFor(DimensionOrigins.QueryStore));
        AddPath(PlanFeatureTables.PlanWarnings, DimensionOrigins.PartFor(DimensionOrigins.QueryStore));
        AddPath(PlanFeatureTables.PlanMissingIndexes, DimensionOrigins.PartFor(DimensionOrigins.QueryStore));

        // Optional query_snapshots run (7b): plan_shape/plan_features/plan_warnings/plan_missing_indexes
        // under origin/part "qsnap" (docs/optional-tables.md §7) must not collide with any of the above.
        AddPath("query_snapshots", part: null);
        AddPath(DimensionTables.PlanShape, QuerySnapshotsExporter.Part);
        AddPath(PlanFeatureTables.PlanFeatures, QuerySnapshotsExporter.Part);
        AddPath(PlanFeatureTables.PlanWarnings, QuerySnapshotsExporter.Part);
        AddPath(PlanFeatureTables.PlanMissingIndexes, QuerySnapshotsExporter.Part);

        var distinct = paths.Distinct().ToList();
        Assert.Equal(paths.Count, distinct.Count);
    }

    [Fact]
    public void QuerySnapshotsExporter_part_and_origin_are_unique_against_the_other_dimension_origins()
    {
        Assert.Equal("qsnap", QuerySnapshotsExporter.Part);
        Assert.Equal("qsnap", QuerySnapshotsExporter.Origin);
        Assert.NotEqual(QuerySnapshotsExporter.Part, DimensionOrigins.PartFor(DimensionOrigins.QueryStore));
        Assert.NotEqual(QuerySnapshotsExporter.Part, "procedure");
    }

    [Fact]
    public void ManifestFilePath_differs_between_the_fresh_and_query_store_calls_even_with_an_overlapping_table_set()
    {
        // Both calls could in principle request {plan_shape, query_text} -- the fresh call under
        // query_stats, the Query Store call under query_store -- so the origin must be part of
        // the manifest tag even when the table set is identical.
        var fresh = OutputPaths.ManifestFilePath("out", "store_a", From, To, [DimensionTables.PlanShape, DimensionTables.QueryText], DimensionOrigins.QueryStats);
        var queryStore = OutputPaths.ManifestFilePath("out", "store_a", From, To, [DimensionTables.PlanShape, DimensionTables.QueryText], DimensionOrigins.QueryStore);

        Assert.NotEqual(fresh, queryStore);
    }

    [Fact]
    public void ManifestFilePath_with_both_origin_and_the_full_table_set_keeps_the_plain_name()
    {
        var path = OutputPaths.ManifestFilePath("out", "store_a", From, To, ExportOptions.AllTables, DimensionOrigins.Both);
        Assert.Equal("store_a__20260926T0800Z__20260926T0900Z.json", Path.GetFileName(path));
    }

    [Fact]
    public void ManifestFilePath_with_a_non_both_origin_and_the_full_table_set_gets_its_own_name()
    {
        var both = OutputPaths.ManifestFilePath("out", "store_a", From, To, ExportOptions.AllTables, DimensionOrigins.Both);
        var queryStatsOnly = OutputPaths.ManifestFilePath("out", "store_a", From, To, ExportOptions.AllTables, DimensionOrigins.QueryStats);

        Assert.NotEqual(both, queryStatsOnly);
    }
}
