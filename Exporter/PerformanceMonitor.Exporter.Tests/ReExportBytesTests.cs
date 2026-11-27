using System.Data.Common;
using System.Security.Cryptography;
using Npgsql;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// A re-export of the same data writes the same bytes. The store can hand the same rows back in another order (a
/// rewritten heap page, a compressed chunk, a different plan), and rows that tie on the documented sort columns used to
/// land in the order they arrived, so the Parquet file and its sha256 changed with nothing in the data having changed.
/// Each test feeds the same rows in three different read orders and compares the bytes of every file written.
/// </summary>
public sealed class ReExportBytesTests
{
    private static readonly DateTime Hour0 = new(2026, 10, 22, 11, 0, 0, DateTimeKind.Unspecified);

    private static string NewOutDir() => Directory.CreateTempSubdirectory("darling-export-rerun-").FullName;

    private static string Sha256(string path) => Convert.ToHexStringLower(SHA256.HashData(File.ReadAllBytes(path)));

    /// <summary>The same rows in three orders: as given, reversed and interleaved.</summary>
    private static IEnumerable<List<object[]>> ReadOrders(List<object[]> rows)
    {
        yield return [.. rows];
        yield return [.. rows.AsEnumerable().Reverse()];
        yield return [.. rows.Where((_, i) => i % 2 == 0), .. rows.Where((_, i) => i % 2 == 1).Reverse()];
    }

    [Fact]
    public async Task An_hourly_rollup_with_several_keys_in_one_hour_writes_the_same_bytes_in_any_read_order()
    {
        string[] names =
        [
            "server_name", "collection_time", "resource_semaphore_id", "pool_id",
            "target_memory_mb", "max_target_memory_mb", "total_memory_mb", "granted_memory_mb", "used_memory_mb",
            "grantee_count", "waiter_count", "available_memory_mb",
            "timeout_error_count_delta", "forced_grant_count_delta", "timeout_error_count", "forced_grant_count",
            "sample_interval_seconds",
        ];
        string[] types =
        [
            "varchar", "timestamp", "int4", "int4",
            "int4", "int4", "int4", "int4", "int4",
            "int4", "int4", "int4",
            "int4", "int4", "int4", "int4",
            "int4",
        ];

        // Eight (semaphore, pool) keys under one server and one hour: every output row ties on (server_name, hour).
        var rows = new List<object[]>();
        for (var key = 0; key < 8; key++)
        {
            for (var sample = 0; sample < 3; sample++)
            {
                rows.Add(["server1", Hour0.AddMinutes(sample * 5), key / 2, key % 2, 100, 100, 100, 10 * key + sample, 10, 1, 0, 90, 1, 0, sample, 0, 300]);
            }
        }

        var spec = RollupSql.Specs["memory_grant_stats_hourly"];
        var hashes = new List<string>();
        foreach (var order in ReadOrders(rows))
        {
            var outDir = NewOutDir();
            try
            {
                var result = await HourlyRollupExporter.ExportAsync(
                    spec, "store_a", outDir, Hour0, Hour0.AddHours(1),
                    _ => Task.FromResult<DbDataReader>(new FakeDbDataReader(names, types, order)), CancellationToken.None);

                Assert.Equal(8, result.RolledRowCount);
                var path = Path.Combine(outDir, "store_a", result.Part.RelativePath.Replace('/', Path.DirectorySeparatorChar));
                Assert.Equal(result.Part.Sha256, Sha256(path));
                hashes.Add(result.Part.Sha256);
            }
            finally
            {
                Directory.Delete(outDir, recursive: true);
            }
        }

        Assert.Single(hashes.Distinct());
    }

    [Fact]
    public async Task An_event_table_with_rows_tied_on_server_and_time_writes_the_same_bytes_in_any_read_order()
    {
        string[] names = ["server_name", "collection_time", "counter_name", "cntr_value"];
        string[] types = ["varchar", "timestamp", "varchar", "int8"];
        var rows = new List<object[]>();
        for (var i = 0; i < 40; i++)
        {
            // Four distinct times, ten rows on each, all under one server.
            rows.Add(["server1", Hour0.AddMinutes(i / 10), $"counter {i % 10}", (long)(i * 7)]);
        }

        var request = new OptionalExportRequest("tied_opt", "collection_time", IsSnapshot: false, Hour0, Hour0.AddHours(1));
        var hashes = new List<string>();
        foreach (var order in ReadOrders(rows))
        {
            var outDir = NewOutDir();
            try
            {
                var result = await OptionalTableExporter.ExportAsync(
                    request, "store_a", outDir, _ => Task.FromResult<DbDataReader>(new FakeDbDataReader(names, types, order)), CancellationToken.None);

                Assert.Equal(40, result.RowCount);
                var part = Assert.Single(result.Parts);
                var path = Path.Combine(outDir, "store_a", part.RelativePath.Replace('/', Path.DirectorySeparatorChar));
                Assert.Equal(part.Sha256, Sha256(path));
                hashes.Add(part.Sha256);
            }
            finally
            {
                Directory.Delete(outDir, recursive: true);
            }
        }

        Assert.Single(hashes.Distinct());
    }

    [Fact]
    public async Task A_table_with_no_server_or_time_column_writes_the_same_bytes_in_any_read_order()
    {
        string[] names = ["name", "value"];
        string[] types = ["varchar", "int8"];
        var rows = Enumerable.Range(0, 30).Select(i => new object[] { $"name {i % 6}", (long)(i * 3) }).ToList();

        var request = new OptionalExportRequest("unkeyed_opt", TimeColumn: null, IsSnapshot: false, Hour0, Hour0.AddHours(1));
        var hashes = new List<string>();
        foreach (var order in ReadOrders(rows))
        {
            var outDir = NewOutDir();
            try
            {
                var result = await OptionalTableExporter.ExportAsync(
                    request, "store_a", outDir, _ => Task.FromResult<DbDataReader>(new FakeDbDataReader(names, types, order)), CancellationToken.None);

                var part = Assert.Single(result.Parts);
                hashes.Add(part.Sha256);
            }
            finally
            {
                Directory.Delete(outDir, recursive: true);
            }
        }

        Assert.Single(hashes.Distinct());
    }
}

/// <summary>
/// The same check for a core table read from a real PostgreSQL: the heap is rewritten in the opposite order between two
/// exports, so the store hands the rows back in another order, and the bytes must not change. With no Docker the test
/// returns without asserting, like the other Docker-backed suites.
/// </summary>
[Trait("Category", "Slow")]
public sealed class ReExportLiveTests : IClassFixture<GenericRawTableEquivalenceTests.GenericPgFixture>
{
    private static readonly DateTime From = new(2026, 10, 22, 11, 0, 0, DateTimeKind.Utc);

    private readonly GenericRawTableEquivalenceTests.GenericPgFixture _pg;

    public ReExportLiveTests(GenericRawTableEquivalenceTests.GenericPgFixture pg) => _pg = pg;

    private static IReadOnlyList<string> RawColumns(NpgsqlConnectionStringBuilder builder, string table)
    {
        using var connection = new NpgsqlConnection(builder.ConnectionString);
        connection.Open();
        using var command = new NpgsqlCommand(SqlBuilder.RawColumnDiscoverySql, connection);
        command.Parameters.AddWithValue("table", table);
        using var reader = command.ExecuteReader();
        var columns = new List<string>();
        while (reader.Read()) { columns.Add(reader.GetString(0)); }
        return columns;
    }

    private async Task<string> ExportAsync(string table)
    {
        var outDir = Directory.CreateTempSubdirectory("darling-export-rerun-live-").FullName;
        try
        {
            var options = new ExportOptions { Store = "bench", FromUtc = From, ToUtc = From.AddHours(1), OutDir = outDir, Tables = [] };
            var entry = await TableExporter.ExportAsync(_pg.Builder!, options, table, RawColumns(_pg.Builder!, table), default);
            var path = OutputPaths.ParquetFilePath(outDir, "bench", table, From, From.AddHours(1));
            Assert.Equal(entry.Sha256, Convert.ToHexStringLower(SHA256.HashData(File.ReadAllBytes(path))));
            return entry.Sha256;
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
        }
    }

    [Fact]
    public async Task A_core_table_with_rows_tied_on_server_and_time_writes_the_same_bytes_after_the_heap_is_rewritten()
    {
        if (_pg.Builder is null) { return; }

        // Sixty rows, ten per collection_time, all under one server: every row ties with nine others on (server_name, collection_time).
        var seeded = _pg.Psql("""
            TRUNCATE collect.perfmon_probe;
            INSERT INTO collect.perfmon_probe
            SELECT i, timestamp '2026-10-22 11:00:00' + (i / 10) * interval '1 minute', 1, 'srv1', 'SQLServer:Buffer Manager',
                   'counter ' || (i % 10), 'inst', i * 13, 65792, i % 1000, 60, (i % 1000) / 60.0
            FROM generate_series(1::bigint, 60::bigint) i;
            """);
        Assert.True(seeded.code == 0, seeded.output);

        var first = await ExportAsync("perfmon_probe");

        var rewritten = _pg.Psql("""
            CREATE TEMP TABLE reversed AS SELECT * FROM collect.perfmon_probe ORDER BY collection_id DESC;
            TRUNCATE collect.perfmon_probe;
            INSERT INTO collect.perfmon_probe SELECT * FROM reversed;
            """);
        Assert.True(rewritten.code == 0, rewritten.output);

        Assert.Equal(first, await ExportAsync("perfmon_probe"));
    }
}
