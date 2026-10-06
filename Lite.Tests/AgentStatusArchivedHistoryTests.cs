using System;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// <c>ever_seen_running</c> asks about every stored agent_status row, and archival moves rows to Parquet: after 7
/// days, or all of them at the 512 MB reset. Here the row that saw Agent running is archived and the hot table
/// holds only the newest, stopped row. The reader must still call it a stopped Agent, not one that never ran.
/// Its own store, not the shared fixture, because it writes a Parquet file and rebuilds the archive views.
/// </summary>
public sealed class AgentStatusArchivedHistoryTests : IDisposable
{
    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);

    public AgentStatusArchivedHistoryTests() => Directory.CreateDirectory(Path.Combine(_tempDir, "archive"));

    public void Dispose()
    {
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

    private static string Row(long id, DateTime collectionTime, bool running) =>
        $"({id}, TIMESTAMP '{collectionTime:yyyy-MM-dd HH:mm:ss}', 5201, 'ARCHIVED-HISTORY', {(running ? "true" : "false")}, "
        + $"'{(running ? "Running" : "Stopped")}', 'Automatic', NULL)";

    private static async Task ExecuteAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task ARunningRowInTheArchive_StillCounts_WhenTheHotTableHoldsOnlyTheNewestStoppedRow()
    {
        var dbPath = Path.Combine(_tempDir, "test.duckdb");
        using var initializer = new DuckDbInitializer(dbPath);
        await initializer.InitializeAsync();
        const string columns = "(collection_id, collection_time, server_id, server_name, agent_running, agent_status_desc, agent_startup_desc, next_scheduled_run)";
        var now = DateTime.UtcNow;

        using (var connection = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await ExecuteAsync(connection, $"INSERT INTO agent_status {columns} VALUES {Row(1, now.AddDays(-10), running: true)}");
            var parquet = Path.Combine(_tempDir, "archive", "20260601_0000_agent_status.parquet").Replace("\\", "/");
            await ExecuteAsync(connection, $"COPY agent_status TO '{parquet}' (FORMAT PARQUET)");
            await ExecuteAsync(connection, "DELETE FROM agent_status");
            await ExecuteAsync(connection, $"INSERT INTO agent_status {columns} VALUES {Row(2, now.AddMinutes(-3), running: false)}");
        }

        await initializer.CreateArchiveViewsAsync();

        var row = Assert.Single(await new LocalDataService(initializer).GetAgentStatusAsync(5201));

        Assert.False(row.AgentRunning);
        Assert.True(row.EverSeenRunning);
        Assert.False(row.IsStale);
        Assert.True(row.IsAgentProblem);
    }
}
