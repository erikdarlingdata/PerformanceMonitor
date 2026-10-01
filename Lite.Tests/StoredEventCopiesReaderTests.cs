using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The readers with no other test that read the event tables through <see cref="StoredEventCopies"/>. Each event
/// here was first stored 40 minutes ago and is now in the archive, and a later batch stored it again 35 minutes
/// ago into the hot table, the way a cycle after the 512 MB reset did. Every reader counts it once, and a window
/// that starts between the two copies shows neither. Its own store, not the shared fixture, because it writes
/// Parquet files and rebuilds the archive views.
/// </summary>
public sealed class StoredEventCopiesReaderTests : IDisposable
{
    private const int ServerId = 5301;
    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);

    public StoredEventCopiesReaderTests() => Directory.CreateDirectory(Path.Combine(_tempDir, "archive"));

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

    private static string Ts(DateTime value) => $"TIMESTAMP '{value:yyyy-MM-dd HH:mm:ss.ffffff}'";

    private static async Task ExecuteAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /* One blocked process report, one long query completion and one system_health event, each stored by the batch
       at firstStored and archived, then stored again by the batch at copyStored. */
    private async Task<LocalDataService> StageAsync(DateTime firstStored, DateTime copyStored)
    {
        var dbPath = Path.Combine(_tempDir, "test.duckdb");
        var initializer = new DuckDbInitializer(dbPath);
        await initializer.InitializeAsync();
        var eventTime = firstStored.AddMinutes(-2);

        string Bpr(int id, DateTime stored) =>
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, "
            + "database_name, blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_status, blocking_status, "
            + $"blocked_process_report_xml) VALUES ({id}, {Ts(stored)}, {ServerId}, 'COPIES', {Ts(eventTime)}, 'db1', 55, 66, "
            + "5000, 'X', 'suspended', 'sleeping', '<blocked-process-report monitorLoop=\"1\"/>')";
        string Lqc(int id, DateTime stored) =>
            "INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, "
            + "event_type, database_name, duration_microseconds, session_id, event_sequence, statement_text) "
            + $"VALUES ({id}, {Ts(stored)}, {ServerId}, 'COPIES', {Ts(eventTime)}, 'sql_batch_completed', 'db1', 9000000, 55, 7, 'EXEC p')";
        string She(int id, DateTime stored) =>
            "INSERT INTO system_health_events (system_health_event_id, collection_time, server_id, server_name, event_time, "
            + $"event_type, event_xml) VALUES ({id}, {Ts(stored)}, {ServerId}, 'COPIES', {Ts(eventTime)}, 'wait_info', '<event name=\"wait_info\"/>')";

        using (var connection = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await ExecuteAsync(connection, Bpr(1, firstStored));
            await ExecuteAsync(connection, Lqc(1, firstStored));
            await ExecuteAsync(connection, She(1, firstStored));
            foreach (var table in new[] { "blocked_process_reports", "long_query_completions", "system_health_events" })
            {
                var parquet = Path.Combine(_tempDir, "archive", $"20260601_0000_{table}.parquet").Replace("\\", "/");
                await ExecuteAsync(connection, $"COPY {table} TO '{parquet}' (FORMAT PARQUET)");
                await ExecuteAsync(connection, $"DELETE FROM {table}");
            }

            await ExecuteAsync(connection, Bpr(2, copyStored));
            await ExecuteAsync(connection, Lqc(2, copyStored));
            await ExecuteAsync(connection, She(2, copyStored));
        }

        await initializer.CreateArchiveViewsAsync();
        return new LocalDataService(initializer);
    }

    [Fact]
    public async Task EachReader_CountsAnEventThatALaterBatchStoredAgain_Once()
    {
        var now = DateTime.UtcNow;
        var service = await StageAsync(now.AddMinutes(-40), now.AddMinutes(-35));
        var from = now.AddHours(-1);

        Assert.Single(await service.GetRecentBlockedProcessReportsAsync(ServerId, 1, from, now));
        Assert.Single(await service.GetRecentBlockedProcessReportsAsync(ServerId, 1, from, now, windowOnCollectionTime: true));
        Assert.Single(await service.GetBlockingPairRowsAsync(ServerId, from, now));
        Assert.Equal(1, (await service.GetBlockingSlicerDataAsync(ServerId, 1, from, now)).Sum(b => b.SessionCount));
        Assert.Single(await service.GetRecentLongQueryCompletionsAsync(ServerId, 1, from, now));
        Assert.Single(await service.GetSlowestLongQueryCompletionsAsync(ServerId, 1, now, 10));
        Assert.Equal(1, await service.CountSystemHealthEventsAsync(ServerId, "wait_info", 1, now));
    }

    /* The window starts after the event happened and was first stored, but before the later batch stored it again.
       A read on event_time leaves both copies out by the event time they share; a read on collection_time (the alert
       engine's, and the long query completions') finds the first copy in its look-back and drops the later one. */
    [Fact]
    public async Task AWindowStartingBetweenTheTwoCopies_ShowsNeither()
    {
        var now = DateTime.UtcNow;
        var service = await StageAsync(now.AddMinutes(-40), now.AddMinutes(-35));
        var from = now.AddMinutes(-37);

        Assert.Empty(await service.GetRecentBlockedProcessReportsAsync(ServerId, 1, from, now));
        Assert.Empty(await service.GetRecentBlockedProcessReportsAsync(ServerId, 1, from, now, windowOnCollectionTime: true));
        Assert.Equal(0, (await service.GetBlockingSlicerDataAsync(ServerId, 1, from, now)).Sum(b => b.SessionCount));
        Assert.Empty(await service.GetRecentLongQueryCompletionsAsync(ServerId, 1, from, now));
    }
}
