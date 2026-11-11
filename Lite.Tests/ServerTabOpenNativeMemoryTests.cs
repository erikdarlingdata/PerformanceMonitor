/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8535: a field walk of Lite showed about 800 MB of native growth that the gc heap did not explain, a 60 to 160 MB step on
/// every server tab open, and nothing handed back on close. This reproduces the data path of a server tab's open, with no
/// monitored server and no window: the reads the Overview runs (<c>RefreshServerDataStartAsync</c> and
/// <c>CorrelatedTimelineLanesControl.RefreshAsync</c>), over a seeded store whose older days sit in parquet archive files
/// and whose newer days are hot. Each step opens a fresh <see cref="LocalDataService"/>, runs the reads, drops it, and
/// records what DuckDB says it holds (<c>duckdb_memory()</c> on Lite's own instance) next to the process's private bytes and
/// the gc heap, so the split between DuckDB's buffer pool, DuckDB's other allocations and everything else is read directly.
///
/// <para>Set <c>LITE_8535_ROWS</c> (wait_stats rows per server per day, default 2,000) and <c>LITE_8535_STEPS</c> (default
/// 6) for a bigger run, and <c>LITE_8535_OUT</c> to a file path to receive the table. The defaults keep the class to a few
/// seconds in CI.</para>
/// </summary>
[Collection("CollectionResetGate")]
public sealed class ServerTabOpenNativeMemoryTests : IDisposable
{
    private const int ServerCount = 5;
    private const int SeededDays = 14;

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;
    private readonly List<DuckDbInitializer> _initializers = [];

    public ServerTabOpenNativeMemoryTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
    }

    public void Dispose()
    {
        foreach (var initializer in _initializers)
            initializer.Dispose();

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

    private static int EnvInt(string name, int fallback) =>
        int.TryParse(Environment.GetEnvironmentVariable(name), NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) && v > 0 ? v : fallback;

    private async Task<DuckDbInitializer> SeedAsync(int waitRowsPerServerPerDay)
    {
        var initializer = new DuckDbInitializer(_dbPath);
        _initializers.Add(initializer);
        await initializer.InitializeFromTemplateAsync();

        using (var connection = initializer.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);

            /* One sample a minute per server for CPU, memory and file IO, and waitRowsPerServerPerDay wait rows a day. The
               timestamps run from SeededDays ago to now, so the oldest seven days are archived and the newest seven stay hot. */
            var minutes = SeededDays * 1440;
            var start = "(date_trunc('minute', now()::TIMESTAMP) - INTERVAL (" + minutes + ") MINUTE)";
            var statements = new[]
            {
                $@"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sample_time_utc, sqlserver_cpu_utilization, other_process_cpu_utilization)
                   SELECT -(s * 100000000 + i), {start} + INTERVAL (i) MINUTE, s, 'S' || s, {start} + INTERVAL (i) MINUTE, {start} + INTERVAL (i) MINUTE, (i % 90)::INTEGER, (i % 7)::INTEGER
                   FROM range(1, {ServerCount + 1}) a(s), range(0, {minutes}) b(i)",
                $@"INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb, target_server_memory_mb, total_server_memory_mb, buffer_pool_mb)
                   SELECT -(s * 100000000 + i), {start} + INTERVAL (i) MINUTE, s, 'S' || s, 65536, 20000 + (i % 500), 49152, 40000 + (i % 900), 30000 + (i % 700)
                   FROM range(1, {ServerCount + 1}) a(s), range(0, {minutes}) b(i)",
                $@"INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb, delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
                   SELECT -(s * 100000000 + i * 10 + f), {start} + INTERVAL (i) MINUTE, s, 'S' || s, 'Db' || f, 'Db' || f || '_data', 'ROWS', 'D:\Db' || f || '.mdf', 100, 100 + (i % 50), 10, 819200, 81920, (i % 30), 3, 60
                   FROM range(1, {ServerCount + 1}) a(s), range(0, {minutes}) b(i), range(0, 4) c(f)",
                $@"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
                   SELECT -(s * 1000000000 + d * 10000000 + w), {start} + INTERVAL (d * 1440 + (w % 1440)) MINUTE, s, 'S' || s, 'WAIT_' || (w % 40), 5000 + w, 9000 + w, 10, 50, 100 + (w % 70), 5
                   FROM range(1, {ServerCount + 1}) a(s), range(0, {SeededDays}) b(d), range(0, {waitRowsPerServerPerDay}) c(w)",
            };

            foreach (var sql in statements)
            {
                using var cmd = connection.CreateCommand();
                cmd.CommandText = sql;
                cmd.CommandTimeout = 600;
                await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }
        }

        var archived = await new ArchiveService(initializer, _archiveDir).ArchiveOldDataAsync(hotDataDays: 7);
        Assert.True(archived);
        Assert.NotEmpty(Directory.GetFiles(_archiveDir, "*.parquet"));

        /* The app starts with a cold buffer pool: a store that was just seeded still holds its rows in memory, which a tab
           opened after a start does not. Close the instance and open the file again, so the reads run from disk. */
        if (Environment.GetEnvironmentVariable("LITE_8535_HOT") == "1")
            return initializer;

        initializer.Dispose();
        _initializers.Remove(initializer);
        var reopened = new DuckDbInitializer(_dbPath);
        _initializers.Add(reopened);
        await reopened.InitializeAsync();
        return reopened;
    }

    /// <summary>The reads a server tab's Overview runs on open, in the tab's own shape: the lane reads run together.</summary>
    private static async Task RunTabOpenReadsAsync(LocalDataService data, int serverId)
    {
        const int hoursBack = 168;
        var now = DateTime.UtcNow;
        var from = now.AddHours(-hoursBack);

        await data.GetServerDataStartAsync(serverId, from);
        await data.GetOldestRetainedAsync(serverId);
        await data.GetSqlEngineEditionAsync(serverId);

        var cpu = Task.Run(() => data.GetCpuUtilizationAsync(serverId, hoursBack, null, null, frame: CpuTimeFrame.Utc));
        var wait = Task.Run(() => data.GetTotalWaitTrendAsync(serverId, hoursBack, null, null));
        var blocking = Task.Run(() => data.GetBlockingTrendAsync(serverId, hoursBack, null, null));
        var deadlock = Task.Run(() => data.GetDeadlockTrendAsync(serverId, hoursBack, null, null));
        var memory = Task.Run(() => data.GetMemoryTrendAsync(serverId, hoursBack, null, null));
        var fileIo = Task.Run(() => data.GetFileIoOverviewLatencyTrendAsync(serverId, hoursBack, null, null));
        await Task.WhenAll(cpu, wait, blocking, deadlock, memory, fileIo);

        /* The comparison lane: the same hours one day earlier. */
        var refFrom = from.AddDays(-1);
        var refTo = now.AddDays(-1);
        await Task.WhenAll(
            Task.Run(() => data.GetCpuUtilizationAsync(serverId, 0, refFrom, refTo, frame: CpuTimeFrame.Utc)),
            Task.Run(() => data.GetTotalWaitTrendAsync(serverId, 0, refFrom, refTo)),
            Task.Run(() => data.GetMemoryTrendAsync(serverId, 0, refFrom, refTo)),
            Task.Run(() => data.GetFileIoOverviewLatencyTrendAsync(serverId, 0, refFrom, refTo)));

        Assert.NotEmpty(cpu.Result);
        Assert.NotEmpty(wait.Result);
        Assert.NotEmpty(memory.Result);
    }

    private static async Task<(Dictionary<string, (long Memory, long Temp)> Tags, long PrivateBytes, long GcBytes)> SampleAsync(DuckDbInitializer initializer)
    {
        var tags = new Dictionary<string, (long Memory, long Temp)>(StringComparer.Ordinal);
        using (var connection = initializer.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT tag, memory_usage_bytes::BIGINT, temporary_storage_bytes::BIGINT FROM duckdb_memory()";
            using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            while (await reader.ReadAsync(TestContext.Current.CancellationToken))
                tags[reader.GetString(0)] = (reader.GetInt64(1), reader.GetInt64(2));
        }

        using var process = Process.GetCurrentProcess();
        return (tags, process.PrivateMemorySize64, GC.GetTotalMemory(forceFullCollection: false));
    }

    private static string Mb(long bytes) => (bytes / (1024.0 * 1024.0)).ToString("F1", CultureInfo.InvariantCulture);

    [Fact]
    public async Task TabOpenReads_RepeatedOpens_LeaveNoLockHeld_AndDuckDbStaysInsideItsLimit()
    {
        var waitRows = EnvInt("LITE_8535_ROWS", 2000);
        var steps = EnvInt("LITE_8535_STEPS", 6);
        var initializer = await SeedAsync(waitRows);

        var table = new StringBuilder();
        table.AppendLine("| step | event | duckdb total MB | top tags (MB) | temp MB | private MB | gc heap MB |");
        table.AppendLine("|---|---|---|---|---|---|---|");

        async Task Record(string label, string eventName)
        {
            var (tags, priv, gc) = await SampleAsync(initializer);
            var total = tags.Values.Sum(t => t.Memory);
            var temp = tags.Values.Sum(t => t.Temp);
            var top = string.Join(", ", tags.Where(t => t.Value.Memory > 0).OrderByDescending(t => t.Value.Memory).Take(5)
                .Select(t => t.Key + " " + Mb(t.Value.Memory)));
            table.AppendLine($"| {label} | {eventName} | {Mb(total)} | {top} | {Mb(temp)} | {Mb(priv)} | {Mb(gc)} |");
        }

        await Record("0", "seeded and archived");

        var holdersBefore = StoreLockHolders.Snapshot().Count;
        for (var step = 1; step <= steps; step++)
        {
            var serverId = (step - 1) % ServerCount + 1;

            /* A tab opening: its own data service, the Overview reads, then the tab is gone. */
            var data = new LocalDataService(initializer);
            await RunTabOpenReadsAsync(data, serverId);
            await Record(step.ToString(CultureInfo.InvariantCulture), $"opened server {serverId}");

            data = null;
            GC.Collect();
            GC.WaitForPendingFinalizers();
            await Record(step.ToString(CultureInfo.InvariantCulture), "closed, after GC.Collect");
        }

        var tracked = (await SampleAsync(initializer)).Tags.Values.Sum(t => t.Memory);
        var holdersAfter = StoreLockHolders.Snapshot().Count;

        /* What a trim would give back, read on the same store: one trim cycle with the threshold at zero, the cycle the 60 s
           timer runs once the sentinel's total is over 256 MB. */
        var originalThreshold = DuckDbInitializer.TrimThresholdBytes;
        DuckDbInitializer.TrimThresholdBytes = 0;
        initializer.TrimLockWaitForTests = TimeSpan.FromSeconds(30);
        try
        {
            initializer.RunMemoryTrimCycle();
        }
        finally
        {
            DuckDbInitializer.TrimThresholdBytes = originalThreshold;
        }

        Assert.Equal(1, initializer.CompletedTrimCycleCount);
        await Record("trim", "after one trim cycle");
        GC.Collect();
        await Record("trim", "after the trim and GC.Collect");

        var outPath = Environment.GetEnvironmentVariable("LITE_8535_OUT");
        if (!string.IsNullOrWhiteSpace(outPath))
            File.WriteAllText(outPath, table.ToString());

        /* No read connection or read lock is left behind by a tab's data path. */
        Assert.Equal(holdersBefore, holdersAfter);

        /* DuckDB's own account stays inside its memory_limit. */
        Assert.True(tracked <= DuckDbInitializer.ConfiguredMemoryLimitGb * 1024L * 1024 * 1024,
            $"duckdb_memory() holds {Mb(tracked)} MB, over the {DuckDbInitializer.ConfiguredMemoryLimitGb} GB limit");
    }
}
