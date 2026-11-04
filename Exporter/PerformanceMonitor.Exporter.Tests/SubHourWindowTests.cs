using PerformanceMonitor.Exporter;
using Npgsql;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// Sub-hour windows: --from/--to on a whole UTC minute, allowed only for tables whose store read is a plain
/// collection_time range (<see cref="SubHourTables"/>). Whole-hour windows are covered by the existing tests and
/// must be untouched.
/// </summary>
public class SubHourWindowTests
{
    private static readonly DateTime Now = new(2026, 9, 26, 12, 0, 0, DateTimeKind.Utc);

    private static ExportOptions Validate(
        string from, string to, string? tables, string? also = null, string? dimensionOrigin = null) =>
        ExportOptions.Validate(
            "store_a", from, to, "/tmp/out", tables,
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now,
            dimensionOrigin: dimensionOrigin, alsoCsv: also);

    [Theory]
    [InlineData("2026-09-26T10:10:00Z", "2026-09-26T10:20:00Z")] // inside one hour
    [InlineData("2026-09-26T10:00:00Z", "2026-09-26T10:10:00Z")] // on the hour, ends inside it
    [InlineData("2026-09-26T10:50:00Z", "2026-09-26T11:10:00Z")] // crosses an hour boundary
    public void Raw_only_table_set_accepts_a_sub_hour_window(string from, string to)
    {
        var options = Validate(from, to, "perfmon_stats,wait_stats");

        Assert.Equal(DateTime.Parse(from, null, System.Globalization.DateTimeStyles.AdjustToUniversal), options.FromUtc);
        Assert.Equal(DateTime.Parse(to, null, System.Globalization.DateTimeStyles.AdjustToUniversal), options.ToUtc);
        Assert.True(SubHourTables.IsSubHourWindow(options.FromUtc, options.ToUtc));
    }

    [Fact]
    public void System_health_tables_accept_a_sub_hour_window()
    {
        var options = Validate("2026-09-26T10:10:00Z", "2026-09-26T10:20:00Z", string.Join(",", SystemHealthTables.All));
        Assert.Equal(SystemHealthTables.All, options.Tables);
    }

    [Fact]
    public void Every_raw_table_is_accepted_over_a_sub_hour_window()
    {
        var options = Validate("2026-09-26T10:10:00Z", "2026-09-26T10:20:00Z", string.Join(",", RawTables.All));
        Assert.Equal(RawTables.All, options.Tables);
    }

    [Theory]
    [InlineData("query_stats_hourly")]
    [InlineData("procedure_stats_hourly")]
    [InlineData("query_store_stats")]
    [InlineData("plan_shape")]
    [InlineData("query_text")]
    [InlineData("procedure_plan")]
    public void Hourly_only_table_refuses_a_sub_hour_window_and_names_the_table(string table)
    {
        var ex = Assert.Throws<ArgumentException>(() =>
            Validate("2026-09-26T08:10:00Z", "2026-09-26T08:20:00Z", $"perfmon_stats,{table}"));
        Assert.Contains($"{table} needs a whole-hour window", ex.Message);
    }

    [Fact]
    public void Refusal_names_the_first_hourly_only_table_in_request_order()
    {
        var ex = Assert.Throws<ArgumentException>(() =>
            Validate("2026-09-26T08:10:00Z", "2026-09-26T08:20:00Z", "wait_stats,query_stats_hourly,plan_shape"));
        Assert.Contains("query_stats_hourly needs a whole-hour window", ex.Message);
    }

    [Fact]
    public void An_all_tables_run_refuses_a_sub_hour_window()
    {
        var ex = Assert.Throws<ArgumentException>(() =>
            Validate("2026-09-26T08:10:00Z", "2026-09-26T08:20:00Z", null));
        Assert.Contains("needs a whole-hour window", ex.Message);
    }

    [Fact]
    public void An_optional_table_named_in_tables_refuses_a_sub_hour_window()
    {
        var optional = OptionalTables.All.First().Name;
        var ex = Assert.Throws<ArgumentException>(() =>
            Validate("2026-09-26T08:10:00Z", "2026-09-26T08:20:00Z", optional));
        Assert.Contains($"{optional} needs a whole-hour window", ex.Message);
    }

    [Fact]
    public void Also_refuses_a_sub_hour_window()
    {
        var optional = OptionalTables.All.First().Name;
        var ex = Assert.Throws<ArgumentException>(() =>
            Validate("2026-09-26T08:10:00Z", "2026-09-26T08:20:00Z", "perfmon_stats", also: optional));
        Assert.Contains("--also", ex.Message);
    }

    [Theory]
    [InlineData(DimensionOrigins.Both)]
    [InlineData(DimensionOrigins.QueryStats)]
    [InlineData(DimensionOrigins.QueryStore)]
    public void Dimension_origin_refuses_a_sub_hour_window(string origin)
    {
        var ex = Assert.Throws<ArgumentException>(() =>
            Validate("2026-09-26T08:10:00Z", "2026-09-26T08:20:00Z", "perfmon_stats", dimensionOrigin: origin));
        Assert.Contains("--dimension-origin", ex.Message);
    }

    [Theory]
    [InlineData("2026-09-26T10:10:30Z", "2026-09-26T10:20:00Z")]
    [InlineData("2026-09-26T10:10:00Z", "2026-09-26T10:20:30Z")]
    [InlineData("2026-09-26T10:10:00.500Z", "2026-09-26T10:20:00Z")]
    [InlineData("2026-09-26T10:10:00Z", "2026-09-26T10:20:00.0000001Z")]
    public void Non_minute_boundaries_are_refused(string from, string to)
    {
        Assert.Throws<ArgumentException>(() => Validate(from, to, "perfmon_stats"));
    }

    [Fact]
    public void A_sub_hour_window_must_still_have_to_after_from()
    {
        Assert.Throws<ArgumentException>(() => Validate("2026-09-26T10:20:00Z", "2026-09-26T10:20:00Z", "perfmon_stats"));
        Assert.Throws<ArgumentException>(() => Validate("2026-09-26T10:20:00Z", "2026-09-26T10:10:00Z", "perfmon_stats"));
    }

    [Fact]
    public void The_fresh_family_margin_still_applies_to_a_sub_hour_window()
    {
        // Now is 12:00Z; to = 11:50Z is only 10 minutes back.
        Assert.Throws<ArgumentException>(() => Validate("2026-09-26T11:40:00Z", "2026-09-26T11:50:00Z", "perfmon_stats"));
        // 11:45Z is exactly 15 minutes back.
        Assert.Equal(
            new DateTime(2026, 9, 26, 11, 45, 0, DateTimeKind.Utc),
            Validate("2026-09-26T11:35:00Z", "2026-09-26T11:45:00Z", "perfmon_stats").ToUtc);
    }

    [Fact]
    public void Whole_hour_windows_still_accept_every_table_and_option()
    {
        var optional = OptionalTables.All.First().Name;
        var options = Validate(
            "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", "perfmon_stats,query_stats_hourly,plan_shape",
            also: optional, dimensionOrigin: DimensionOrigins.QueryStats);
        Assert.False(SubHourTables.IsSubHourWindow(options.FromUtc, options.ToUtc));
    }

    [Fact]
    public void Sub_hour_safe_list_is_exactly_raw_plus_system_health_and_excludes_everything_hourly_only()
    {
        Assert.Equal([.. RawTables.All, .. SystemHealthTables.All], SubHourTables.All);
        foreach (var t in HourlyTables.All.Concat(QueryStoreTables.All).Concat(DimensionTables.All))
        {
            Assert.DoesNotContain(t, SubHourTables.All);
        }
    }

    [Fact]
    public void Window_parameters_carry_the_exact_sub_hour_bounds_as_unspecified_kind()
    {
        var from = new DateTime(2026, 9, 26, 10, 10, 0, DateTimeKind.Utc);
        var to = new DateTime(2026, 9, 26, 10, 20, 0, DateTimeKind.Utc);

        var parameters = SqlBuilder.WindowParameters(from, to);

        Assert.Equal(["from", "to"], parameters.Select(p => p.ParameterName));
        Assert.All(parameters, p => Assert.Equal(NpgsqlTypes.NpgsqlDbType.Timestamp, p.NpgsqlDbType));
        var boundFrom = Assert.IsType<DateTime>(parameters[0].Value);
        var boundTo = Assert.IsType<DateTime>(parameters[1].Value);
        Assert.Equal(from.Ticks, boundFrom.Ticks);
        Assert.Equal(to.Ticks, boundTo.Ticks);
        Assert.Equal(DateTimeKind.Unspecified, boundFrom.Kind);
        Assert.Equal(DateTimeKind.Unspecified, boundTo.Kind);
    }

    [Fact]
    public void Output_file_and_manifest_names_carry_the_minutes_for_a_sub_hour_window()
    {
        var from = new DateTime(2026, 9, 26, 10, 10, 0, DateTimeKind.Utc);
        var to = new DateTime(2026, 9, 26, 10, 20, 0, DateTimeKind.Utc);

        Assert.Equal(
            Path.Combine("/out", "store_a", "perfmon_stats", "perfmon_stats__store_a__20260926T1010Z__20260926T1020Z.parquet"),
            OutputPaths.ParquetFilePath("/out", "store_a", "perfmon_stats", from, to));
        Assert.Equal(
            "perfmon_stats/perfmon_stats__store_a__20260926T1010Z__20260926T1020Z.parquet",
            OutputPaths.RelativeParquetPath("store_a", "perfmon_stats", from, to));

        var manifest = OutputPaths.ManifestFilePath("/out", "store_a", from, to, ["perfmon_stats"]);
        Assert.StartsWith("store_a__20260926T1010Z__20260926T1020Z__t", Path.GetFileName(manifest));

        // Adjacent slices of the same table set never share a manifest, and a different set never shares one.
        var next = OutputPaths.ManifestFilePath("/out", "store_a", to, to.AddMinutes(10), ["perfmon_stats"]);
        var other = OutputPaths.ManifestFilePath("/out", "store_a", from, to, ["wait_stats"]);
        Assert.NotEqual(manifest, next);
        Assert.NotEqual(manifest, other);
    }

    [Fact]
    public async Task Manifest_window_for_a_sub_hour_window_is_the_exact_bounds()
    {
        var from = new DateTime(2026, 9, 26, 10, 50, 0, DateTimeKind.Utc);
        var to = new DateTime(2026, 9, 26, 11, 10, 0, DateTimeKind.Utc);
        var path = Path.Combine(Path.GetTempPath(), $"subhour-{Guid.NewGuid():N}", "m.json");
        try
        {
            await ManifestWriter.WriteAsync(
                new Manifest
                {
                    Store = "store_a",
                    FromUtc = from,
                    ToUtc = to,
                    ExporterVersion = "t",
                    DarlingSchemaVersion = null,
                    Tables = [],
                },
                path,
                CancellationToken.None);

            using var doc = System.Text.Json.JsonDocument.Parse(await File.ReadAllTextAsync(path));
            var window = doc.RootElement.GetProperty("window");
            Assert.Equal("2026-09-26T10:50:00.0000000Z", window.GetProperty("from_utc").GetString());
            Assert.Equal("2026-09-26T11:10:00.0000000Z", window.GetProperty("to_utc").GetString());
        }
        finally
        {
            Directory.Delete(Path.GetDirectoryName(path)!, recursive: true);
        }
    }

    [Fact]
    public void Whole_hour_names_are_exactly_as_before()
    {
        var from = new DateTime(2026, 9, 26, 8, 0, 0, DateTimeKind.Utc);
        var to = new DateTime(2026, 9, 26, 9, 0, 0, DateTimeKind.Utc);

        Assert.Equal(
            "perfmon_stats/perfmon_stats__store_a__20260926T0800Z__20260926T0900Z.parquet",
            OutputPaths.RelativeParquetPath("store_a", "perfmon_stats", from, to));
        Assert.Equal(
            "store_a__20260926T0800Z__20260926T0900Z.json",
            Path.GetFileName(OutputPaths.ManifestFilePath("/out", "store_a", from, to, ExportOptions.AllTables)));
    }

    [Fact]
    public async Task Runner_refuses_a_sub_hour_window_for_options_built_around_Validate()
    {
        var options = new ExportOptions
        {
            Store = "store_a",
            FromUtc = new DateTime(2026, 9, 26, 8, 10, 0, DateTimeKind.Utc),
            ToUtc = new DateTime(2026, 9, 26, 8, 20, 0, DateTimeKind.Utc),
            OutDir = "/tmp/out",
            Tables = ["perfmon_stats", "query_stats_hourly"],
            DryRun = true,
        };

        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            ExportRunner.RunAsync(options, new StringWriter(), CancellationToken.None));
        Assert.Contains("query_stats_hourly needs a whole-hour window", ex.Message);
    }
}
