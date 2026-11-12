using System.Data.Common;
using DuckDB.NET.Data;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// A day's daily snapshot is not tied to the 00:00 window alone: when nothing says the day was exported, a later
/// window of the same day produces it, with the same read range and the same snapshot time the midnight window would
/// have written.
/// </summary>
public class DailyRetryTests
{
    private static readonly DateTime Midnight = new(2026, 10, 22, 0, 0, 0, DateTimeKind.Utc);

    private static string NewDir(string prefix) => Directory.CreateTempSubdirectory(prefix).FullName;

    private static OptionalTable DailyTable() =>
        new("pg_database_size_stats", "collect", "collection_time", OptionalTableKind.Window, [], "7c", OptionalTableStatus.Candidate, "test")
        {
            Grain = ExportGrain.Daily,
            DailyLookbackHours = 2,
        };

    private static ExportOptions OptionsAt(DateTime fromUtc, string outDir) => new()
    {
        Store = "store_a",
        FromUtc = fromUtc,
        ToUtc = fromUtc.AddHours(1),
        OutDir = outDir,
        Tables = [],
        StateDir = NewDir("darling-export-daily-retry-state-"),
    };

    private static async Task<(long Rows, DateTime SnapshotTime)> ReadSnapshot(string outDir, string relativePath)
    {
        var absolute = Path.Combine(outDir, "store_a", relativePath.Replace('/', Path.DirectorySeparatorChar));
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT count(*), min(snapshot_time) FROM read_parquet('{absolute.Replace("'", "''")}')";
        using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return (reader.GetInt64(0), reader.GetDateTime(1));
    }

    [Fact]
    public async Task A_daily_window_table_runs_in_a_later_window_of_the_day_and_reads_up_to_midnight()
    {
        var outDir = NewDir("darling-export-daily-retry-");
        var options = OptionsAt(Midnight.AddHours(1), outDir);
        var rows = new List<object[]>
        {
            new object[] { "A", new DateTime(2026, 10, 21, 23, 0, 0), 4L },
            new object[] { "A", new DateTime(2026, 10, 21, 23, 30, 0), 5L },
        };
        DateTime? readFrom = null, readTo = null;

        var result = await DailySnapshotExporter.ExportAsync(
            DailyTable(), options, "store_a", outDir,
            (from, to, _) =>
            {
                readFrom = from;
                readTo = to;
                return Task.FromResult<DbDataReader>(new FakeDbDataReader(["server_name", "collection_time", "size_bytes"], ["varchar", "timestamp", "int8"], rows));
            },
            CancellationToken.None);

        Assert.Null(result.SkippedReason);
        Assert.Equal(Midnight, readTo);
        Assert.Equal(Midnight.AddHours(-2), readFrom);
        var part = Assert.Single(result.Parts);
        var (snapRows, snapshotTime) = await ReadSnapshot(outDir, part.RelativePath);
        Assert.Equal(1, snapRows);
        Assert.Equal(new DateTime(2026, 10, 22, 0, 0, 0), snapshotTime);
    }

    [Fact]
    public async Task The_servers_snapshot_runs_in_a_later_window_of_the_day_and_stamps_that_day()
    {
        var outDir = NewDir("darling-export-servers-retry-");
        var request = new OptionalExportRequest("servers", null, IsSnapshot: true, Midnight.AddHours(1), Midnight.AddHours(2));
        var reader = new FakeDbDataReader(["server_name"], ["varchar"], [new object[] { "A" }, new object[] { "B" }]);

        var result = await OptionalTableExporter.ExportAsync(
            request, "store_a", outDir, _ => Task.FromResult<DbDataReader>(reader), CancellationToken.None);

        Assert.Null(result.SkippedReason);
        var part = Assert.Single(result.Parts);
        var (snapRows, snapshotTime) = await ReadSnapshot(outDir, part.RelativePath);
        Assert.Equal(2, snapRows);
        Assert.Equal(new DateTime(2026, 10, 22, 0, 0, 0), snapshotTime);
    }
}
