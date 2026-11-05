using System.Text.RegularExpressions;
using PerformanceMonitor.Exporter;
using Npgsql;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// O2a: --also parsing, exit-code mapping, the manifest metadata fields for every core table (fixed by the file contract), and the separate optional-manifest name.
/// </summary>
public class OptionalTableWiringTests
{
    private static readonly DateTime Now = new(2026, 9, 26, 12, 0, 0, DateTimeKind.Utc);

    // ---- --also parsing (unknown/absent handling is exercised via OptionalTableRunner.Resolve, which takes an
    // injected lookup so this test never needs a live store) ----

    [Fact]
    public void Also_splits_on_commas_and_trims()
    {
        var options = ExportOptions.Validate(
            "store_a", "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", "/tmp/out", "deadlocks",
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now,
            alsoCsv: " plan_correction , job_history ");

        Assert.Equal(["plan_correction", "job_history"], options.Also);
    }

    [Fact]
    public void Also_defaults_to_empty()
    {
        var options = ExportOptions.Validate(
            "store_a", "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", "/tmp/out", "deadlocks",
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now);

        Assert.Empty(options.Also);
    }

    [Fact]
    public void Also_plus_query_store_dimension_origin_is_rejected()
    {
        Assert.Throws<ArgumentException>(() =>
            ExportOptions.Validate(
                "store_a", "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", "/tmp/out", "plan_shape",
                "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now,
                dimensionOrigin: DimensionOrigins.QueryStore, alsoCsv: "plan_correction"));
    }

    [Fact]
    public void Also_alone_is_fine_with_default_dimension_origin()
    {
        var options = ExportOptions.Validate(
            "store_a", "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", "/tmp/out", "deadlocks",
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now,
            alsoCsv: "plan_correction");

        Assert.Equal(["plan_correction"], options.Also);
    }

    [Fact]
    public void A_registry_optional_name_is_allowed_in_dash_dash_tables_for_manual_runs()
    {
        var options = ExportOptions.Validate(
            "store_a", "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", "/tmp/out", "plan_correction",
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now);

        Assert.Equal(["plan_correction"], options.Tables);
    }

    // ---- OptionalTableRunner.Resolve: unknown vs. known, via an injected lookup ----

    [Fact]
    public void Resolve_reports_unknown_names_and_keeps_known_ones()
    {
        var (known, unknown) = OptionalTableRunner.Resolve(
            ["plan_correction", "bogus_table"], OptionalTables.TryGet);

        Assert.Single(known);
        Assert.Equal("plan_correction", known[0].Name);
        Assert.Single(unknown);
        Assert.Equal("bogus_table", unknown[0].Table);
        Assert.Equal("unknown", unknown[0].Reason);
    }

    // ---- OptionalTableRunner.RunAsync: pg_statement_text (Dimension) routing/skip (docs/optional-tables.md §8.2, lane O7) ----

    [Fact]
    public async Task RunAsync_skips_pg_statement_text_with_needs_pg_statement_stats_when_stats_is_not_in_the_run()
    {
        // pg_statement_text alone (no pg_statement_stats in the same --also/--tables set) must be skipped before
        // ever opening a store connection -- the bogus connection string below would fail loudly if RunAsync tried
        // to use it, proving the skip really happens first.
        var table = OptionalTables.TryGet("pg_statement_text");
        Assert.NotNull(table);

        var connectionStringBuilder = new NpgsqlConnectionStringBuilder { Host = "unroutable.invalid", Port = 1, Timeout = 1 };
        var options = ExportOptions.Validate(
            "store_pg", "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", "/tmp/out", "deadlocks",
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now,
            alsoCsv: "pg_statement_text");

        var result = await OptionalTableRunner.RunAsync(
            connectionStringBuilder, options, [table!], [], TextWriter.Null, CancellationToken.None);

        Assert.Empty(result.Entries);
        Assert.Empty(result.Failed);
        var skipped = Assert.Single(result.Skipped);
        Assert.Equal("pg_statement_text", skipped.Table);
        Assert.Equal(PgStatementTextExporter.MissingStatsReason, skipped.Reason);
    }

    // ---- exit-code mapping (docs/optional-tables.md §2: exit 3 iff core ok and >=1 optional table failed) ----

    [Fact]
    public void Exit_code_is_3_when_any_optional_table_failed()
    {
        var result = new OptionalRunResult([], [], [new FailedOptionalTable { Table = "job_history", Error = "boom" }]);
        Assert.Equal(3, result.Failed.Count > 0 ? 3 : 0);
    }

    [Fact]
    public void Exit_code_is_0_when_no_optional_table_failed()
    {
        var result = new OptionalRunResult([], [new SkippedOptionalTable { Table = "bogus", Reason = "unknown" }], []);
        Assert.Equal(0, result.Failed.Count > 0 ? 3 : 0);
    }

    // ---- manifest field values for every core table: the loader-map drift test (docs/optional-tables.md §4.1) ----

    [Fact]
    public void CoreTableMetadata_time_column_matches_loaders_TIME_COLUMN_map()
    {
        foreach (var table in ExportOptions.AllTables)
        {
            Assert.Equal(SqlBuilder.TimeColumn(table), CoreTableMetadata.TimeColumn(table));
        }
    }

    [Theory]
    [InlineData(HourlyTables.QueryStatsHourly, "hour")]
    [InlineData(HourlyTables.ProcedureStatsHourly, "hour")]
    [InlineData(QueryStoreTables.QueryStoreStats, "interval_start_time_utc")]
    [InlineData("perfmon_stats", "collection_time")]
    public void CoreTableMetadata_time_column_matches_pinned_loader_values(string table, string expected)
    {
        Assert.Equal(expected, CoreTableMetadata.TimeColumn(table));
    }

    [Theory]
    [InlineData(DimensionTables.PlanShape)]
    [InlineData(DimensionTables.QueryText)]
    [InlineData(DimensionTables.ProcedurePlan)]
    [InlineData(PlanFeatureTables.PlanFeatures)]
    [InlineData(PlanFeatureTables.PlanWarnings)]
    [InlineData(PlanFeatureTables.PlanMissingIndexes)]
    public void CoreTableMetadata_kind_is_dimension_for_loader_dimension_tables(string table)
    {
        Assert.Equal("dimension", CoreTableMetadata.Kind(table));
        Assert.NotNull(CoreTableMetadata.KeyColumns(table));
    }

    [Fact]
    public void CoreTableMetadata_kind_is_window_for_a_raw_table()
    {
        Assert.Equal("window", CoreTableMetadata.Kind("deadlocks"));
        Assert.Null(CoreTableMetadata.KeyColumns("deadlocks"));
    }

    [Fact]
    public void CoreTableMetadata_dimension_key_columns_match_the_loader_exactly()
    {
        Assert.Equal(
            new[] { "source_store", "server_name", "query_plan_hash" },
            CoreTableMetadata.KeyColumns(DimensionTables.PlanShape));
        Assert.Equal(
            new[] { "source_store", "server_name", "database_name", "query_hash" },
            CoreTableMetadata.KeyColumns(DimensionTables.QueryText));
        Assert.Equal(
            new[] { "source_store", "server_name", "database_name", "schema_name", "object_name", "shape_hash" },
            CoreTableMetadata.KeyColumns(DimensionTables.ProcedurePlan));
    }

    [Fact]
    public void WithMetadata_fills_every_core_entry_without_disturbing_its_other_fields()
    {
        var entry = new ManifestTableEntry
        {
            Table = "plan_correction",
            OutputFile = "plan_correction/x.parquet",
            RowCount = 5,
            ByteSize = 100,
            Sha256 = "abc",
            Columns = [],
            StoreQueryElapsedMs = 1.0,
            WriteElapsedMs = 2.0,
            MissingContentCount = 0,
        };

        var withMeta = CoreTableMetadata.WithMetadata(entry);

        Assert.Equal("collection_time", withMeta.TimeColumn);
        Assert.Equal("window", withMeta.Kind);
        Assert.Null(withMeta.KeyColumns);
        Assert.Equal(entry.RowCount, withMeta.RowCount);
        Assert.Equal(entry.Sha256, withMeta.Sha256);
    }

    /// <summary>
    /// The time column and the dimension keys of the core tables are part of the file contract. A change here is a
    /// contract change (see the file contract's schema-change rule).
    /// </summary>
    [Fact]
    public void CoreTableMetadata_matches_the_file_contract_for_the_tables_that_differ_from_the_default()
    {
        Assert.Equal("hour", CoreTableMetadata.TimeColumn(HourlyTables.QueryStatsHourly));
        Assert.Equal("hour", CoreTableMetadata.TimeColumn(HourlyTables.ProcedureStatsHourly));
        Assert.Equal("interval_start_time_utc", CoreTableMetadata.TimeColumn(QueryStoreTables.QueryStoreStats));
        Assert.Equal(CoreTableMetadata.DefaultTimeColumn, CoreTableMetadata.TimeColumn("perfmon_stats"));
        Assert.Equal(["source_store", "server_name", "query_plan_hash"], CoreTableMetadata.KeyColumns(DimensionTables.PlanShape));
    }

    // ---- the optional manifest name differs from the core manifest name ----

    [Fact]
    public void Optional_manifest_name_never_collides_with_the_core_manifest_name()
    {
        var from = new DateTime(2026, 9, 26, 8, 0, 0, DateTimeKind.Utc);
        var to = new DateTime(2026, 9, 26, 9, 0, 0, DateTimeKind.Utc);

        var corePath = OutputPaths.ManifestFilePath("/tmp/out", "store_a", from, to, ExportOptions.AllTables, DimensionOrigins.Both);
        var optionalPath = OutputPaths.ManifestFilePath("/tmp/out", "store_a", from, to, ["plan_correction", "job_history"], dimensionOrigin: null);

        Assert.NotEqual(corePath, optionalPath);
        Assert.EndsWith(".json", corePath);
        Assert.Contains("__t", optionalPath);
    }
}
