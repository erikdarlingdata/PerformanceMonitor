using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A watermark read that fails still returns its "no watermark" value, so the collector reads its fallback
/// window that cycle and can store again events it already holds. Each of the four reads logs one WARN naming
/// the read, the table, the server and the exception type, so that cycle can be traced. The failure is planted
/// as a read of a table that does not exist, which DuckDB refuses.
///
/// <para>Collection <c>app-logger-statics</c>: the log assertions drain the process-wide buffer, which the other
/// tests that do the same share.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class WatermarkReadFailureLogTests : IDisposable
{
    private const string Missing = "no_such_table";

    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);

    private readonly List<DuckDbInitializer> _stores = [];

    public WatermarkReadFailureLogTests() => Directory.CreateDirectory(_tempDir);

    public void Dispose()
    {
        foreach (var duckDb in _stores)
        {
            duckDb.Dispose();
        }

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

    /// <summary>Exposes the runner's four watermark reads.</summary>
    private sealed class Reads(DuckDbInitializer duckDb, ServerManager? serverManager)
        : RemoteCollectorService(duckDb, serverManager: serverManager!, scheduleManager: null!)
    {
        /// <summary>Runs each read against the missing table; true when every one came back "no watermark".</summary>
        public async Task<bool> EachReturnsNoWatermarkAsync(int serverId)
        {
            var time = await GetLastCollectedTimeAsync(serverId, Missing, "event_time", CancellationToken.None);
            var frame = await GetLastCollectedTimeWithFrameAsync(serverId, Missing, "sample_time", "sample_time_utc", CancellationToken.None);
            var database = await GetLastCollectedTimeForDatabaseAsync(
                serverId, Missing, "event_time", "database_name", "db1", CancellationToken.None);
            var instance = await GetLastCollectedInstanceIdAsync(serverId, Missing, "instance_id", CancellationToken.None);
            return time is null && frame.Value is null && !frame.FromUtcColumn && database is null && instance is null;
        }

        /// <summary>The plain read on a real, empty table: a first run, not a failure.</summary>
        public Task<DateTime?> FirstRunTimeAsync(int serverId) =>
            GetLastCollectedTimeAsync(serverId, "blocked_process_reports", "event_time", CancellationToken.None);
    }

    private async Task<DuckDbInitializer> StoreAsync()
    {
        var duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _stores.Add(duckDb);
        await duckDb.InitializeAsync();
        return duckDb;
    }

    private static List<string> WatermarkLines(IEnumerable<string> log) =>
        log.Where(line => line.Contains("[Watermark]", StringComparison.Ordinal)).ToList();

    [Fact]
    public async Task EachFailedRead_LogsOneWarn_NamingTheRead_TheTable_TheServer_AndTheExceptionType()
    {
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);
        var serverManager = new ServerManager(configDir);
        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = "wm-fail",
            DisplayName = "wm-fail (display)",
        };
        serverManager.AddServer(server);
        var serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
        var reads = new Reads(await StoreAsync(), serverManager);

        AppLogger.DrainBufferedLines();
        var noWatermark = await reads.EachReturnsNoWatermarkAsync(serverId);
        var lines = WatermarkLines(AppLogger.DrainBufferedLines());

        Assert.True(noWatermark);
        Assert.Equal(4, lines.Count);
        Assert.All(lines, line =>
        {
            Assert.Contains("[WARN ]", line, StringComparison.Ordinal);
            Assert.Contains($"[{server.DisplayName}]", line, StringComparison.Ordinal);
            Assert.Contains($"on {Missing} failed (DuckDBException)", line, StringComparison.Ordinal);
            Assert.Contains("reads the collector's fallback window", line, StringComparison.Ordinal);
        });
        Assert.Single(lines, line => line.Contains("] Watermark read on ", StringComparison.Ordinal));
        Assert.Single(lines, line => line.Contains("] Watermark read with its UTC twin on ", StringComparison.Ordinal));
        Assert.Single(lines, line => line.Contains("] Watermark read for database db1 on ", StringComparison.Ordinal));
        Assert.Single(lines, line => line.Contains("] Instance-id watermark read on ", StringComparison.Ordinal));
    }

    /// <summary>No server in the list carries the id (or there is no list): the line names the id instead.</summary>
    [Fact]
    public async Task AFailedReadForAnUnknownServer_NamesTheServerId()
    {
        var reads = new Reads(await StoreAsync(), serverManager: null);

        AppLogger.DrainBufferedLines();
        var noWatermark = await reads.EachReturnsNoWatermarkAsync(424242);
        var lines = WatermarkLines(AppLogger.DrainBufferedLines());

        Assert.True(noWatermark);
        Assert.Equal(4, lines.Count);
        Assert.All(lines, line => Assert.Contains("[server_id 424242]", line, StringComparison.Ordinal));
    }

    /// <summary>A read that succeeds logs nothing, even when it finds no rows (a true first run).</summary>
    [Fact]
    public async Task AReadThatSucceeds_LogsNothing()
    {
        var reads = new Reads(await StoreAsync(), serverManager: null);

        AppLogger.DrainBufferedLines();
        var watermark = await reads.FirstRunTimeAsync(424242);
        var lines = WatermarkLines(AppLogger.DrainBufferedLines());

        Assert.Null(watermark);
        Assert.Empty(lines);
    }
}
