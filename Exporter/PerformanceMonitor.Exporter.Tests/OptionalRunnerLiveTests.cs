using DuckDB.NET.Data;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// <see cref="OptionalTableRunner.RunAsync"/> against a throwaway PostgreSQL (Docker) laid out like a store: the
/// manifest entries it folds, the tables it skips, and when a daily table runs. With no Docker each test returns
/// without asserting, like the other Docker-backed suites.
/// </summary>
[Trait("Category", "Slow")]
public sealed class OptionalRunnerLiveTests : IClassFixture<OptionalRunnerLiveTests.StoreFixture>
{
    private static readonly DateTime From = new(2026, 10, 22, 11, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Midnight = new(2026, 10, 22, 0, 0, 0, DateTimeKind.Utc);

    public sealed class StoreFixture : IDisposable
    {
        public GenericRawTableEquivalenceTests.GenericPgFixture Pg { get; } = new();
        public string Root { get; } = Directory.CreateTempSubdirectory("darling-export-optional-live-").FullName;

        public StoreFixture()
        {
            if (Pg.Builder is null) { return; }
            // Present: an Event table with a policy, an Hourly table, a Daily table. Absent: pg_wait_stats (Hourly)
            // and database_states (Change). The chunk guard reads the TimescaleDB catalog view, which a plain
            // PostgreSQL lacks, so an empty stand-in stands in for it.
            var result = Pg.Psql("""
                CREATE TABLE collect.darling_schema_version (version int);
                INSERT INTO collect.darling_schema_version VALUES (150);
                CREATE TABLE collect.analysis_findings (server_name text, collection_time timestamp, analysis_time timestamp, finding_id bigint, severity text);
                INSERT INTO collect.analysis_findings VALUES ('srv1', '2026-10-22 11:30:00', '2026-10-22 11:30:00', 1, 'high');
                CREATE TABLE collect.pg_cpu_utilization (server_name text, collection_time timestamp,
                  cpu_percent double precision, acu_utilization_percent double precision, serverless_capacity_acu double precision,
                  memory_active_bytes bigint, memory_cached_bytes bigint, memory_buffers_bytes bigint, memory_free_bytes bigint,
                  max_configured_acu double precision, memory_total_bytes bigint, configured_memory_bytes bigint);
                INSERT INTO collect.pg_cpu_utilization VALUES ('srv1', '2026-10-22 11:30:00', 12.5, 1.0, 2.0, 100, 200, 300, 400, 8.0, 1000, 900);
                CREATE TABLE collect.pg_database_size_stats (server_name text, collection_time timestamp, database_name text, size_bytes bigint);
                INSERT INTO collect.pg_database_size_stats VALUES ('srv1', '2026-10-21 23:00:00', 'db1', 100), ('srv1', '2026-10-21 23:30:00', 'db1', 110);
                CREATE SCHEMA timescaledb_information;
                CREATE VIEW timescaledb_information.chunks AS
                  SELECT NULL::text AS chunk_name, NULL::boolean AS is_compressed, NULL::text AS hypertable_schema,
                         NULL::text AS hypertable_name, NULL::timestamp AS range_start, NULL::timestamp AS range_end
                  WHERE false;
                """);
            Assert.True(result.code == 0, result.output);
        }

        public ExportOptions Options(string outDir, DateTime fromUtc) => new()
        {
            Store = "store_a",
            FromUtc = fromUtc,
            ToUtc = fromUtc.AddHours(1),
            OutDir = outDir,
            Tables = [],
            StateDir = Path.Combine(Root, "state"),
        };

        public void Dispose()
        {
            Pg.Dispose();
            try { Directory.Delete(Root, recursive: true); } catch (IOException) { }
        }
    }

    private readonly StoreFixture _store;

    public OptionalRunnerLiveTests(StoreFixture store) => _store = store;

    private static string NewOutDir() => Directory.CreateTempSubdirectory("darling-export-optional-live-out-").FullName;

    private static List<OptionalTable> Registry(params string[] names) => [.. names.Select(n => OptionalTables.TryGet(n)!)];

    private Task<OptionalRunResult> RunAsync(ExportOptions options, params string[] names) =>
        OptionalTableRunner.RunAsync(_store.Pg.Builder!, options, Registry(names), [], TextWriter.Null, CancellationToken.None, darlingSchemaVersion: 150);

    private static List<(string Name, string Type)> Describe(string outDir, string relativePath)
    {
        var absolute = Path.Combine(outDir, "store_a", relativePath.Replace('/', Path.DirectorySeparatorChar));
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('{absolute.Replace("'", "''")}'))";
        using var reader = command.ExecuteReader();
        var columns = new List<(string, string)>();
        while (reader.Read())
        {
            columns.Add((reader.GetString(0), reader.GetString(1)));
        }

        return columns;
    }

    [Fact]
    public async Task Every_optional_entry_lists_the_columns_of_its_file()
    {
        if (_store.Pg.Builder is null) { return; }
        var outDir = NewOutDir();

        var result = await RunAsync(_store.Options(outDir, From), "analysis_findings", "pg_cpu_utilization");

        Assert.Empty(result.Failed);
        Assert.Equal(2, result.Entries.Count);
        foreach (var entry in result.Entries)
        {
            var described = Describe(outDir, entry.OutputFile);
            Assert.NotEmpty(described);
            Assert.Equal(described, entry.Columns.Select(c => (c.Name, c.DuckDbType)).ToList());
        }
    }

    [Fact]
    public async Task An_absent_hourly_or_change_table_is_listed_under_skipped_optional()
    {
        if (_store.Pg.Builder is null) { return; }

        var result = await RunAsync(_store.Options(NewOutDir(), From), "pg_wait_stats", "database_states");

        Assert.Empty(result.Failed);
        Assert.Empty(result.Entries);
        Assert.Equal(new HashSet<string> { "pg_wait_stats", "database_states" }, result.Skipped.Select(s => s.Table).ToHashSet());
        Assert.All(result.Skipped, s => Assert.Equal("absent", s.Reason));
    }

    [Fact]
    public async Task A_daily_table_runs_once_in_a_later_window_of_the_same_day()
    {
        if (_store.Pg.Builder is null) { return; }
        var outDir = NewOutDir();

        // The 00:00 window never ran for this store (this fixture's state is empty), so the 01:00 window produces
        // the day's snapshot, reading up to midnight; the 02:00 window then has nothing to add.
        var first = await RunAsync(_store.Options(outDir, Midnight.AddHours(1)), "pg_database_size_stats");
        Assert.Empty(first.Failed);
        var entry = Assert.Single(first.Entries);
        Assert.Equal("pg_database_size_stats", entry.Table);
        Assert.Equal<DateTime?>(Midnight, entry.ReadTo);
        Assert.Equal<DateTime?>(Midnight.AddHours(-2), entry.ReadFrom);
        Assert.Equal(1, entry.RowCount);

        var second = await RunAsync(_store.Options(outDir, Midnight.AddHours(2)), "pg_database_size_stats");
        Assert.Empty(second.Failed);
        Assert.Empty(second.Entries);
        var skipped = Assert.Single(second.Skipped);
        Assert.Equal("pg_database_size_stats", skipped.Table);
    }
}
