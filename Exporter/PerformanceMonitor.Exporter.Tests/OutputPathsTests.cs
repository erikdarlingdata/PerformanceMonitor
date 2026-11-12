using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class OutputPathsTests
{
    private static readonly DateTime From = new(2026, 9, 26, 8, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime To = new(2026, 9, 26, 9, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void ParquetFilePath_matches_the_documented_layout()
    {
        var path = OutputPaths.ParquetFilePath("/out", "store_a", "perfmon_stats", From, To);

        Assert.Equal(
            Path.Combine("/out", "store_a", "perfmon_stats", "perfmon_stats__store_a__20260926T0800Z__20260926T0900Z.parquet"),
            path);
    }

    [Fact]
    public void ManifestFilePath_matches_the_documented_layout()
    {
        var path = OutputPaths.ManifestFilePath("/out", "store_a", From, To);

        Assert.Equal(
            Path.Combine("/out", "store_a", "_manifests", "store_a__20260926T0800Z__20260926T0900Z.json"),
            path);
    }

    [Fact]
    public void FormatTimestamp_uses_a_sortable_utc_compact_form()
    {
        Assert.Equal("20260926T0800Z", OutputPaths.FormatTimestamp(From));
    }

    [Fact]
    public void RelativeParquetPath_always_uses_forward_slash_even_on_windows()
    {
        var path = OutputPaths.RelativeParquetPath("store_a", "perfmon_stats", From, To);

        Assert.Equal("perfmon_stats/perfmon_stats__store_a__20260926T0800Z__20260926T0900Z.parquet", path);
        Assert.DoesNotContain('\\', path);
    }

    [Fact]
    public void ParquetFilePath_appends_the_part_suffix_before_the_extension()
    {
        var path = OutputPaths.ParquetFilePath("/out", "store_b", "plan_features", From, To, part: "procedure");

        Assert.Equal(
            Path.Combine("/out", "store_b", "plan_features", "plan_features__store_b__20260926T0800Z__20260926T0900Z__procedure.parquet"),
            path);
    }

    [Fact]
    public void ParquetFilePath_with_no_part_matches_the_no_arg_overload()
    {
        var withPart = OutputPaths.ParquetFilePath("/out", "store_a", "perfmon_stats", From, To, part: null);
        var withoutPart = OutputPaths.ParquetFilePath("/out", "store_a", "perfmon_stats", From, To);

        Assert.Equal(withoutPart, withPart);
    }

    [Fact]
    public void RelativeParquetPath_appends_the_part_suffix_before_the_extension()
    {
        var path = OutputPaths.RelativeParquetPath("store_b", "plan_features", From, To, part: "procedure");

        Assert.Equal("plan_features/plan_features__store_b__20260926T0800Z__20260926T0900Z__procedure.parquet", path);
    }


    [Fact]
    public void A_full_run_keeps_the_plain_manifest_name()
    {
        var path = OutputPaths.ManifestFilePath("out", "store_a", From, To, ExportOptions.AllTables);
        Assert.Equal("store_a__20260926T0800Z__20260926T0900Z.json", Path.GetFileName(path));
        Assert.Equal(path, OutputPaths.ManifestFilePath("out", "store_a", From, To));
    }

    [Fact]
    public void A_subset_run_gets_its_own_manifest_name_regardless_of_order()
    {
        var a = OutputPaths.ManifestFilePath("out", "store_a", From, To, ["system_health_cpu", "system_health_io"]);
        var b = OutputPaths.ManifestFilePath("out", "store_a", From, To, ["system_health_io", "system_health_cpu"]);
        var other = OutputPaths.ManifestFilePath("out", "store_a", From, To, ["perfmon_stats"]);
        Assert.Equal(a, b);
        Assert.NotEqual(a, other);
        Assert.Matches(@"^store_a__20260926T0800Z__20260926T0900Z__t[0-9a-f]{8}\.json$", Path.GetFileName(a));
        Assert.NotEqual(OutputPaths.ManifestFilePath("out", "store_a", From, To), a);
    }
}
