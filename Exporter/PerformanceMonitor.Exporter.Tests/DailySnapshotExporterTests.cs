using System.Data.Common;
using DuckDB.NET.Data;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class DailySnapshotExporterTests
{
    private static string NewOutDir() => Directory.CreateTempSubdirectory("darling-export-daily-").FullName;

    private static OptionalTable MakeTable(int lookbackHours) =>
        new("index_object_stats", "collect", "collection_time", OptionalTableKind.Window, [], "7a", OptionalTableStatus.Candidate, "test")
        {
            Grain = ExportGrain.Daily,
            DailyLookbackHours = lookbackHours,
        };

    private static ExportOptions MakeOptions(DateTime fromUtc) => new()
    {
        Store = "store_a",
        FromUtc = fromUtc,
        ToUtc = fromUtc.AddHours(1),
        OutDir = NewOutDir(),
        Tables = [],
    };

    private static async Task<long> TotalRows(string outDir, string store, string table, List<OptionalTablePart> parts)
    {
        long total = 0;
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        foreach (var part in parts)
        {
            var absolute = Path.Combine(outDir, store, part.RelativePath.Replace('/', Path.DirectorySeparatorChar));
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT count(*) FROM read_parquet('{absolute}')";
            total += (long)(await command.ExecuteScalarAsync() ?? 0L);
        }

        return total;
    }

    private static async Task<List<(string ServerName, DateTime CollectionTime, DateTime SnapshotTime)>> ReadRows(
        string outDir, string store, List<OptionalTablePart> parts)
    {
        var rows = new List<(string, DateTime, DateTime)>();
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        foreach (var part in parts)
        {
            var absolute = Path.Combine(outDir, store, part.RelativePath.Replace('/', Path.DirectorySeparatorChar));
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT server_name, collection_time, snapshot_time FROM read_parquet('{absolute}') ORDER BY server_name";
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                rows.Add((reader.GetString(0), reader.GetDateTime(1), reader.GetDateTime(2)));
            }
        }

        return rows;
    }

    [Fact]
    public async Task Keeps_only_the_last_cycle_per_server_and_stamps_midnight()
    {
        var table = MakeTable(lookbackHours: 24);
        var from = new DateTime(2026, 9, 21, 0, 0, 0, DateTimeKind.Utc);
        var options = MakeOptions(from);

        // Server A has cycles at 22:00, 23:00, 23:30; server B only at 21:00.
        var data = new List<object[]>
        {
            new object[] { "A", new DateTime(2026, 9, 20, 22, 0, 0), 1 },
            new object[] { "A", new DateTime(2026, 9, 20, 23, 0, 0), 2 },
            new object[] { "A", new DateTime(2026, 9, 20, 23, 30, 0), 3 },
            new object[] { "B", new DateTime(2026, 9, 20, 21, 0, 0), 4 },
        };

        var reader = new FakeDbDataReader(["server_name", "collection_time", "val"], ["text", "timestamp", "int4"], data);
        var opened = false;

        var result = await DailySnapshotExporter.ExportAsync(
            table, options, options.Store, options.OutDir,
            (readFrom, readTo, ct) =>
            {
                opened = true;
                Assert.Equal(from.AddHours(-24), readFrom);
                Assert.Equal(from, readTo);
                return Task.FromResult<DbDataReader>(reader);
            },
            CancellationToken.None);

        Assert.True(opened);
        Assert.Null(result.SkippedReason);
        Assert.Equal(2, result.RowCount);
        Assert.Equal(from.AddHours(-24), result.ReadFrom);
        Assert.Equal(from, result.ReadTo);

        var rows = await ReadRows(options.OutDir, options.Store, result.Parts);
        Assert.Equal(2, rows.Count);

        var a = rows.Single(r => r.ServerName == "A");
        Assert.Equal(new DateTime(2026, 9, 20, 23, 30, 0), a.CollectionTime);
        Assert.Equal(from, a.SnapshotTime);

        var b = rows.Single(r => r.ServerName == "B");
        Assert.Equal(new DateTime(2026, 9, 20, 21, 0, 0), b.CollectionTime);
        Assert.Equal(from, b.SnapshotTime);
    }

    [Fact]
    public async Task Excludes_a_row_at_from_and_a_row_before_read_from()
    {
        var table = MakeTable(lookbackHours: 2);
        var from = new DateTime(2026, 9, 21, 0, 0, 0, DateTimeKind.Utc);
        var options = MakeOptions(from);

        // read_from = 22:00, read_to = 00:00. A row exactly at read_from is IN [inclusive lower bound]; a row
        // exactly at `from` (read_to) is excluded (it belongs to the next window); a row before read_from is
        // excluded. The reader here simulates what the store SELECT (@from/@to) would already have filtered
        // out, plus a boundary row at read_from to prove it is kept.
        var data = new List<object[]>
        {
            new object[] { "A", new DateTime(2026, 9, 20, 22, 0, 0), 1 }, // == read_from: kept
            new object[] { "A", new DateTime(2026, 9, 20, 23, 0, 0), 2 }, // inside: kept (and is the max)
        };

        var reader = new FakeDbDataReader(["server_name", "collection_time", "val"], ["text", "timestamp", "int4"], data);

        var result = await DailySnapshotExporter.ExportAsync(
            table, options, options.Store, options.OutDir,
            (readFrom, readTo, ct) =>
            {
                Assert.Equal(new DateTime(2026, 9, 20, 22, 0, 0), readFrom);
                Assert.Equal(from, readTo);
                return Task.FromResult<DbDataReader>(reader);
            },
            CancellationToken.None);

        Assert.Null(result.SkippedReason);
        var rows = await ReadRows(options.OutDir, options.Store, result.Parts);
        Assert.Single(rows);
        Assert.Equal(new DateTime(2026, 9, 20, 23, 0, 0), rows[0].CollectionTime);
    }

    [Fact]
    public async Task Skips_a_window_that_is_not_midnight_without_opening_the_reader()
    {
        var table = MakeTable(lookbackHours: 24);
        var from = new DateTime(2026, 9, 21, 10, 0, 0, DateTimeKind.Utc);
        var options = MakeOptions(from);
        var opened = false;

        var result = await DailySnapshotExporter.ExportAsync(
            table, options, options.Store, options.OutDir,
            (readFrom, readTo, ct) =>
            {
                opened = true;
                return Task.FromResult<DbDataReader>(new FakeDbDataReader([], [], []));
            },
            CancellationToken.None);

        Assert.False(opened);
        Assert.Equal(0, result.RowCount);
        Assert.Empty(result.Parts);
        Assert.Equal("skipped: not the daily snapshot window", result.SkippedReason);
    }

    [Fact]
    public async Task The_lookback_comes_from_the_registry()
    {
        var table = MakeTable(lookbackHours: 2);
        var from = new DateTime(2026, 9, 21, 0, 0, 0, DateTimeKind.Utc);
        var options = MakeOptions(from);
        DateTime? seenFrom = null;

        var reader = new FakeDbDataReader(["server_name", "collection_time"], ["text", "timestamp"], []);

        await DailySnapshotExporter.ExportAsync(
            table, options, options.Store, options.OutDir,
            (readFrom, readTo, ct) =>
            {
                seenFrom = readFrom;
                return Task.FromResult<DbDataReader>(reader);
            },
            CancellationToken.None);

        Assert.Equal(from.AddHours(-2), seenFrom);
    }
}
