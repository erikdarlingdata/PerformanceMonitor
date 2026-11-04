using System;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7386: archiving moves a server's oldest collection_log rows out of the hot table, so the hot table's oldest row is the
/// server's first collection only when the archive holds nothing at or after the window's start. Without that guard, a server
/// whose oldest rows in the window were archived would get its chart axis trimmed to the hot boundary. Its own store, not the
/// shared fixture, because it writes a Parquet file and rebuilds the archive views.
/// </summary>
public sealed class ServerDataStartArchiveGuardTests : IDisposable
{
    private const int ServerId = 7386;

    private static readonly DateTime WindowStart = new(2026, 6, 10, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime HotOldest = new(2026, 6, 20, 8, 30, 0, DateTimeKind.Utc);

    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
    private DuckDbInitializer? _initializer;

    public ServerDataStartArchiveGuardTests() => Directory.CreateDirectory(Path.Combine(_tempDir, "archive"));

    public void Dispose()
    {
        _initializer?.Dispose();
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    private static string Row(long id, DateTime collectionTime) =>
        $"({id}, {ServerId}, 'DATA-START', 'wait_stats', TIMESTAMP '{collectionTime:yyyy-MM-dd HH:mm:ss}', 'SUCCESS')";

    private static async Task ExecuteAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>A store whose hot table starts at <see cref="HotOldest"/>, with one archived row when <paramref name="archived"/> is given.</summary>
    private async Task<DuckDbInitializer> StoreAsync(DateTime? archived)
    {
        const string columns = "(log_id, server_id, server_name, collector_name, collection_time, status)";
        var dbPath = Path.Combine(_tempDir, "test.duckdb");
        _initializer = new DuckDbInitializer(dbPath);
        await _initializer.InitializeFromTemplateAsync();

        using (var connection = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            if (archived is DateTime archivedTime)
            {
                await ExecuteAsync(connection, $"INSERT INTO collection_log {columns} VALUES {Row(1, archivedTime)}");
                var parquet = Path.Combine(_tempDir, "archive", "20260601_0000_collection_log.parquet").Replace("\\", "/");
                await ExecuteAsync(connection, $"COPY collection_log TO '{parquet}' (FORMAT PARQUET)");
                await ExecuteAsync(connection, "DELETE FROM collection_log");
            }

            await ExecuteAsync(connection, $"INSERT INTO collection_log {columns} VALUES {Row(2, HotOldest)}, {Row(3, HotOldest.AddHours(1))}");
        }

        await _initializer.CreateArchiveViewsAsync();
        return _initializer;
    }

    [Theory]
    [InlineData(12)]
    [InlineData(10)]
    public async Task AnArchiveHoldingARowFromTheWindowStartOn_AnswersNull_NotTheHotBoundary(int archivedDay)
    {
        var initializer = await StoreAsync(new DateTime(2026, 6, archivedDay, 0, 0, 0, DateTimeKind.Utc));

        var start = await new LocalDataService(initializer).GetServerDataStartAsync(ServerId, WindowStart, TestContext.Current.CancellationToken);

        Assert.Null(start);
    }

    [Fact]
    public async Task AnArchiveThatEndsBeforeTheWindow_AnswersTheHotTablesOldestRow()
    {
        var initializer = await StoreAsync(new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc));

        var start = await new LocalDataService(initializer).GetServerDataStartAsync(ServerId, WindowStart, TestContext.Current.CancellationToken);

        Assert.Equal(HotOldest, start);
        Assert.Equal(DateTimeKind.Utc, start!.Value.Kind);
    }

    [Fact]
    public async Task NoArchiveAtAll_AnswersTheHotTablesOldestRow()
    {
        var initializer = await StoreAsync(archived: null);

        var start = await new LocalDataService(initializer).GetServerDataStartAsync(ServerId, WindowStart, TestContext.Current.CancellationToken);

        Assert.Equal(HotOldest, start);
    }
}
