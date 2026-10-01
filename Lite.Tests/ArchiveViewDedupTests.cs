using System;
using System.IO;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Guards the natural-key dedup on the archive views that UNION the hot table with the parquet archive.
/// The 512MB emergency reset (ArchiveService.ArchiveAllAndResetAsync) archives all hot data to parquet and
/// wipes collection_log, so the next cycle re-collects recent history into the hot store while the parquet
/// tier still holds it. The local surrogate prefix id is a per-process counter, so the re-collected rows get
/// brand-new ids — only the SQL-Server-side natural key (job_history: server_id+instance_id;
/// default_trace_events: server_id+event_time+event_sequence) identifies a logical event. Without the QUALIFY
/// dedup those events would appear twice in v_job_history / v_default_trace_events.
/// </summary>
public class ArchiveViewDedupTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archivePath;

    public ArchiveViewDedupTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archivePath = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archivePath);
    }

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

    private async Task<DuckDBConnection> OpenAsync()
    {
        var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        return connection;
    }

    private async Task ExecuteAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task<T> ScalarAsync<T>(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        var result = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);
        return (T)Convert.ChangeType(result!, typeof(T));
    }

    /// <summary>
    /// Simulates archive → reset → re-collect: writes an "archived" set to parquet, empties the hot table,
    /// then inserts a "re-collected" set that overlaps the archived set on the natural key (with new surrogate
    /// ids and a newer collection_time). Copies the whole hot table to a *_{table}.parquet file so the archive
    /// view's glob picks it up, exactly like ArchiveService does.
    /// </summary>
    private async Task StageArchiveAndRecollectAsync(
        DuckDBConnection connection, string table, string archivedInsert, string recollectedInsert)
    {
        await ExecuteAsync(connection, archivedInsert);
        var parquetPath = Path.Combine(_archivePath, $"20260101_0000_{table}.parquet").Replace("\\", "/");
        await ExecuteAsync(connection, $"COPY {table} TO '{parquetPath}' (FORMAT PARQUET)");
        await ExecuteAsync(connection, $"DELETE FROM {table}");
        await ExecuteAsync(connection, recollectedInsert);
    }

    [Fact]
    public async Task VJobHistory_DedupsReCollectedRowsOnServerAndInstanceId()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        using (var connection = await OpenAsync())
        {
            /* Archived (parquet) copy, collected before the reset. */
            var archived = @"
INSERT INTO job_history
    (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name,
     job_enabled, step_id, run_status, run_duration_seconds, retries_attempted, message)
VALUES
    (1, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', 100, 'j', 'Job', true, 0, 1, 10, 0, 'archived-A'),
    (2, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', 200, 'j', 'Job', true, 0, 1, 10, 0, 'archived-B'),
    (3, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', 300, 'j', 'Job', true, 0, 1, 10, 0, 'archived-C')";
            /* Re-collected (hot) copy after the reset: instance_id 200/300 reappear with new surrogate ids and
               a newer collection_time; 400 is genuinely new. */
            var recollected = @"
INSERT INTO job_history
    (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name,
     job_enabled, step_id, run_status, run_duration_seconds, retries_attempted, message)
VALUES
    (11, TIMESTAMP '2026-06-01 00:00:00', 1, 'S1', 200, 'j', 'Job', true, 0, 1, 10, 0, 'hot-B'),
    (12, TIMESTAMP '2026-06-01 00:00:00', 1, 'S1', 300, 'j', 'Job', true, 0, 1, 10, 0, 'hot-C'),
    (13, TIMESTAMP '2026-06-01 00:00:00', 1, 'S1', 400, 'j', 'Job', true, 0, 1, 10, 0, 'hot-D')";
            await StageArchiveAndRecollectAsync(connection, "job_history", archived, recollected);
        }

        await initializer.CreateArchiveViewsAsync();

        using (var connection = await OpenAsync())
        {
            /* Four distinct logical events, not six: the two re-collected duplicates collapse. */
            Assert.Equal(4, await ScalarAsync<int>(connection, "SELECT COUNT(*) FROM v_job_history"));
            /* No instance_id survives twice. */
            Assert.Equal(1, await ScalarAsync<int>(connection,
                "SELECT MAX(c) FROM (SELECT COUNT(*) AS c FROM v_job_history GROUP BY server_id, instance_id)"));
            /* The archive-only row is still present (the union really does reach the parquet tier). */
            Assert.Equal("archived-A", await ScalarAsync<string>(connection,
                "SELECT message FROM v_job_history WHERE instance_id = 100"));
            /* For the overlapping keys the newest-collected (hot) copy wins. */
            Assert.Equal("hot-B", await ScalarAsync<string>(connection,
                "SELECT message FROM v_job_history WHERE instance_id = 200"));
            Assert.Equal("hot-C", await ScalarAsync<string>(connection,
                "SELECT message FROM v_job_history WHERE instance_id = 300"));
        }
    }

    [Fact]
    public async Task VDefaultTraceEvents_DedupsReCollectedRowsOnEventTimeAndSequence()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        using (var connection = await OpenAsync())
        {
            var archived = @"
INSERT INTO default_trace_events
    (default_trace_event_id, collection_time, server_id, server_name, event_time, event_name, event_sequence)
VALUES
    (1, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:01', 'archived-A', 1),
    (2, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:02', 'archived-B', 2),
    (3, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:03', 'archived-C', 3)";
            var recollected = @"
INSERT INTO default_trace_events
    (default_trace_event_id, collection_time, server_id, server_name, event_time, event_name, event_sequence)
VALUES
    (11, TIMESTAMP '2026-06-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:02', 'hot-B', 2),
    (12, TIMESTAMP '2026-06-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:03', 'hot-C', 3),
    (13, TIMESTAMP '2026-06-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:04', 'hot-D', 4)";
            await StageArchiveAndRecollectAsync(connection, "default_trace_events", archived, recollected);
        }

        await initializer.CreateArchiveViewsAsync();

        using (var connection = await OpenAsync())
        {
            Assert.Equal(4, await ScalarAsync<int>(connection, "SELECT COUNT(*) FROM v_default_trace_events"));
            Assert.Equal(1, await ScalarAsync<int>(connection,
                "SELECT MAX(c) FROM (SELECT COUNT(*) AS c FROM v_default_trace_events GROUP BY server_id, event_time, event_sequence)"));
            Assert.Equal("archived-A", await ScalarAsync<string>(connection,
                "SELECT event_name FROM v_default_trace_events WHERE event_sequence = 1"));
            Assert.Equal("hot-B", await ScalarAsync<string>(connection,
                "SELECT event_name FROM v_default_trace_events WHERE event_sequence = 2"));
            Assert.Equal("hot-C", await ScalarAsync<string>(connection,
                "SELECT event_name FROM v_default_trace_events WHERE event_sequence = 3"));
        }
    }

    private const string DeadlockInsertColumns =
        "(deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)";

    private async Task<int> StageDeadlocksAsync(string archived, string live)
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();
        using (var connection = await OpenAsync())
        {
            await StageArchiveAndRecollectAsync(connection, "deadlocks", archived, live);
        }
        await initializer.CreateArchiveViewsAsync();
        using (var connection = await OpenAsync())
        {
            return await ScalarAsync<int>(connection, "SELECT COUNT(*) FROM v_deadlocks");
        }
    }

    [Fact]
    public async Task VDeadlocks_ReadsAnArchivedCopyOfAStoredDeadlockOnce()
    {
        var archived = $@"INSERT INTO deadlocks {DeadlockInsertColumns} VALUES
    (1, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:00', 'p1', 'q', '<deadlock>A</deadlock>'),
    (2, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 11:00:00', 'p1', 'q', '<deadlock>B</deadlock>')";
        var live = $@"INSERT INTO deadlocks {DeadlockInsertColumns} VALUES
    (11, TIMESTAMP '2026-06-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:00', 'p1', 'q', '<deadlock>A</deadlock>')";
        Assert.Equal(2, await StageDeadlocksAsync(archived, live));
    }

    [Fact]
    public async Task VDeadlocks_ReadsTwoDifferentGraphsAtTheSameTimeBoth()
    {
        var archived = $@"INSERT INTO deadlocks {DeadlockInsertColumns} VALUES
    (1, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:00', 'p1', 'q', '<deadlock>A</deadlock>')";
        var live = $@"INSERT INTO deadlocks {DeadlockInsertColumns} VALUES
    (11, TIMESTAMP '2026-06-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:00', 'p1', 'q', '<deadlock>B</deadlock>')";
        Assert.Equal(2, await StageDeadlocksAsync(archived, live));
    }

    [Fact]
    public async Task VDeadlocks_NeverCollapsesRowsWithoutAGraph()
    {
        var archived = $@"INSERT INTO deadlocks {DeadlockInsertColumns} VALUES
    (1, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:00', 'p1', 'q', NULL),
    (2, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:00', 'p1', 'q', NULL)";
        var live = $@"INSERT INTO deadlocks {DeadlockInsertColumns} VALUES
    (11, TIMESTAMP '2026-06-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:00', 'p1', 'q', NULL),
    (12, TIMESTAMP '2026-06-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:00', 'p1', 'q', '')";
        Assert.Equal(4, await StageDeadlocksAsync(archived, live));
    }
}
