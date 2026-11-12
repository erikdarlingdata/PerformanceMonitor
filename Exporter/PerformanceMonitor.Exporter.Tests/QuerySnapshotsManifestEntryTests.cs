using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// A `query_snapshots` run's manifest entries name their files the way every other entry does: `table/file`,
/// relative to the store folder, with `/`.
/// </summary>
public class QuerySnapshotsManifestEntryTests
{
    private static readonly OptionalTable Table = OptionalTables.TryGet("query_snapshots")!;

    [Fact]
    public void The_fact_entry_names_its_file_under_its_table_folder()
    {
        var outputPath = OutputPaths.ParquetFilePath(
            Path.GetTempPath(), "store_a", "query_snapshots",
            new DateTime(2026, 10, 22, 11, 0, 0, DateTimeKind.Utc), new DateTime(2026, 10, 22, 12, 0, 0, DateTimeKind.Utc));

        var entry = OptionalTableRunner.BuildQuerySnapshotsEntry(
            Table, "query_snapshots", outputPath, rowCount: 0, part: null, timeColumn: "collection_time", kind: "window", unparsedPlanRows: 0);

        Assert.Equal("query_snapshots/query_snapshots__store_a__20261022T1100Z__20261022T1200Z.parquet", entry.OutputFile);
    }

    [Fact]
    public void A_companion_entry_names_its_file_under_the_companion_table_folder()
    {
        var outputPath = OutputPaths.ParquetFilePath(
            Path.GetTempPath(), "store_a", DimensionTables.PlanShape,
            new DateTime(2026, 10, 22, 11, 0, 0, DateTimeKind.Utc), new DateTime(2026, 10, 22, 12, 0, 0, DateTimeKind.Utc), QuerySnapshotsExporter.Part);

        var entry = OptionalTableRunner.BuildQuerySnapshotsEntry(
            Table, DimensionTables.PlanShape, outputPath, rowCount: 0, QuerySnapshotsExporter.Part, timeColumn: "last_seen", kind: "dimension", unparsedPlanRows: null);

        Assert.Equal($"plan_shape/plan_shape__store_a__20261022T1100Z__20261022T1200Z__{QuerySnapshotsExporter.Part}.parquet", entry.OutputFile);
        Assert.DoesNotContain('\\', entry.OutputFile);
    }
}
