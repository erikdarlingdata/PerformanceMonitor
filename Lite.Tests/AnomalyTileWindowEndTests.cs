using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4731: the anomaly detector reads its analysis window as [start, end). A sample stamped exactly at the end
/// belongs to the next window, so this window does not count it: the blocking and deadlock counts, and the
/// tiles that feed a latency or rate gate, all leave it out. Eight reads used a closed end (<c>collection_time</c>
/// up to and including the end) beside three that were already open, so a sample on the boundary moved the event
/// counts and a tile's peak and mean while the CPU and wait figures ignored it.
///
/// <para>Each test runs the real detector. The counts are asserted exactly (five blocking events or three
/// deadlocks inside the window, one more on the boundary), so each test also proves the in-window samples are
/// counted. The I/O tile is the one tiled family covered here: the same rows one minute before the
/// end fire, the same rows on the end do not. The source pin over both products' detector files, which holds
/// every window read to the open end, is <c>AnomalyTileWindowEndTests</c> in Darling.Tests.</para>
/// </summary>
public class AnomalyTileWindowEndTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -4731_03;

    /// <summary>A four-hour window on a Wednesday, aligned to the hour so the seeded baseline days share its hour and weekday.</summary>
    private static readonly DateTime WindowStart = new(2026, 3, 4, 10, 0, 0);

    private static readonly DateTime WindowEnd = WindowStart.AddHours(4);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _conn;
    private long _nextId = -1;

    public AnomalyTileWindowEndTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _conn?.Dispose();

    /* ───────────────────────── seeding ───────────────────────── */

    private async Task ExecAsync(string sql, params object[] args)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_conn is null)
        {
            _conn = _duckDb.CreateConnection();
            await _conn.OpenAsync();
        }
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var arg in args)
            cmd.Parameters.Add(new DuckDBParameter { Value = arg });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>The one row that lets the detector run at all: it skips a server with nothing collected in the 30 days before the window end.</summary>
    private Task SeedCollectedHistoryAsync() => ExecAsync(
        @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time,
            sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, 'TestServer', $2, 10, 2)",
        _nextId--, WindowEnd.AddDays(-1), ServerId);

    private Task SeedBlockedProcessReportAsync(DateTime at) => ExecAsync(
        "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, wait_time_ms) VALUES ($1,$2,$3,'TestServer',1000)",
        _nextId--, at, ServerId);

    private Task SeedDeadlockAsync(DateTime at) => ExecAsync(
        "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name) VALUES ($1,$2,$3,'TestServer')",
        _nextId--, at, ServerId);

    private Task SeedDmvBlockingSnapshotAsync(DateTime at) => ExecAsync(
        "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, monitor_loop) VALUES ($1,$2,$3,'TestServer',1)",
        _nextId--, at, ServerId);

    /// <summary><paramref name="count"/> timestamps spread inside the window, none near either edge.</summary>
    private static DateTime[] InsideTheWindow(int count) =>
        Enumerable.Range(0, count).Select(i => WindowStart.AddMinutes(30 + (i * 30))).ToArray();

    /// <summary>One read-bearing file row: (stall, reads) is the sample's read latency in milliseconds.</summary>
    private Task SeedFileIoAsync(DateTime at, string fileName, long stallReadMs, long reads) => ExecAsync(
        @"INSERT INTO file_io_stats
            (collection_id, collection_time, server_id, server_name,
             database_name, file_name, file_type, physical_name, size_mb,
             delta_reads, delta_writes, delta_read_bytes, delta_write_bytes,
             delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
          VALUES ($1, $2, $3, 'TestServer', 'AppDb', $4, 'ROWS', 'D:\AppDb.mdf', 100,
                  $5, 0, 81920, 0, $6, 0, 60)",
        _nextId--, at, ServerId, fileName, reads, stallReadMs);

    /// <summary>Fourteen days of 2 ms reads in the window's own hour, the shape the detector's I/O tests use for a trustworthy baseline.</summary>
    private async Task SeedQuietIoBaselineAsync()
    {
        await ExecAsync("BEGIN TRANSACTION");
        for (var day = 1; day <= 14; day++)
        {
            var baseDay = WindowStart.AddDays(-day);
            for (var i = 0; i < 4; i++)
                await SeedFileIoAsync(baseDay.AddMinutes(i * 3), "AppDb_data", stallReadMs: 20, reads: 10);
        }
        await ExecAsync("COMMIT");
    }

    /// <summary>Four files sampled in one collection, each 40 ms per read, all stamped <paramref name="at"/>.</summary>
    private async Task SeedSlowCollectionAsync(DateTime at)
    {
        for (var file = 0; file < 4; file++)
            await SeedFileIoAsync(at, $"AppDb_data{file}", stallReadMs: 400, reads: 10);
    }

    private async Task<List<Fact>> DetectAsync() =>
        await new AnomalyDetector(_duckDb, new BaselineProvider(_duckDb)).DetectAnomaliesAsync(new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = "TestServer",
            TimeRangeStart = WindowStart,
            TimeRangeEnd = WindowEnd,
        });

    /* ───────────────────────── event counts ───────────────────────── */

    [Fact]
    public async Task BlockedProcessReportCount_LeavesOutASampleStampedAtTheWindowEnd()
    {
        await SeedCollectedHistoryAsync();
        foreach (var at in InsideTheWindow(5))
            await SeedBlockedProcessReportAsync(at);
        await SeedBlockedProcessReportAsync(WindowEnd);

        var facts = await DetectAsync();

        Assert.Equal(5.0, Assert.Single(facts, f => f.Key == "ANOMALY_BLOCKING_SPIKE").Value);
    }

    [Fact]
    public async Task DeadlockCount_LeavesOutASampleStampedAtTheWindowEnd()
    {
        await SeedCollectedHistoryAsync();
        foreach (var at in InsideTheWindow(3))
            await SeedDeadlockAsync(at);
        await SeedDeadlockAsync(WindowEnd);

        var facts = await DetectAsync();

        Assert.Equal(3.0, Assert.Single(facts, f => f.Key == "ANOMALY_DEADLOCK_SPIKE").Value);
    }

    [Fact]
    public async Task DmvBlockingSnapshotCount_LeavesOutASampleStampedAtTheWindowEnd()
    {
        /* No blocked process report in the window, so the count falls back to the always-on DMV snapshots. */
        await SeedCollectedHistoryAsync();
        foreach (var at in InsideTheWindow(5))
            await SeedDmvBlockingSnapshotAsync(at);
        await SeedDmvBlockingSnapshotAsync(WindowEnd);

        var facts = await DetectAsync();

        Assert.Equal(5.0, Assert.Single(facts, f => f.Key == "ANOMALY_BLOCKING_SPIKE").Value);
    }

    /* ───────────────────────── a tiled family ───────────────────────── */

    [Fact]
    public async Task IoLatencyTile_LeavesOutRowsStampedAtTheWindowEnd()
    {
        await SeedCollectedHistoryAsync();
        await SeedQuietIoBaselineAsync();
        await SeedSlowCollectionAsync(WindowEnd);

        var facts = await DetectAsync();

        Assert.DoesNotContain(facts, f => f.Key == "ANOMALY_READ_LATENCY");
    }

    [Fact]
    public async Task IoLatencyTile_StillReadsRowsStampedJustBeforeTheWindowEnd()
    {
        /* The same rows as above, one minute earlier: the boundary is the only difference between the two tests. */
        await SeedCollectedHistoryAsync();
        await SeedQuietIoBaselineAsync();
        await SeedSlowCollectionAsync(WindowEnd.AddMinutes(-1));

        var facts = await DetectAsync();

        Assert.Equal(40.0, Assert.Single(facts, f => f.Key == "ANOMALY_READ_LATENCY").Value);
    }
}
