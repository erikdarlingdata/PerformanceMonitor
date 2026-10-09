using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
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
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();

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

    /// <summary>
    /// #5457: run_datetime is part of the job_history dedup key, so an archived copy and a live copy of one run,
    /// which carry the same non-NULL run_datetime (it is decoded from the msdb row's own run_date and run_time),
    /// still collapse to one row, and the live copy wins. Rows with a NULL run_datetime still group together
    /// (the older test above inserts none).
    /// </summary>
    [Fact]
    public async Task VJobHistory_CollapsesAnArchivedAndALiveCopyOfOneRunWithTheSameRunDatetime()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();

        const string Columns = @"
    (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name,
     job_enabled, step_id, run_status, run_datetime, run_duration_seconds, retries_attempted, message)";

        using (var connection = await OpenAsync())
        {
            var archived = "INSERT INTO job_history" + Columns + @"
VALUES
    (1, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', 100, 'j', 'Job', true, 0, 1, TIMESTAMP '2025-12-31 23:10:00', 10, 0, 'archived-A'),
    (2, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', 200, 'j', 'Job', true, 0, 1, TIMESTAMP '2025-12-31 23:20:00', 10, 0, 'archived-B'),
    (3, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', 300, 'j', 'Job', true, 0, 1, NULL, 10, 0, 'archived-NULL')";
            var recollected = "INSERT INTO job_history" + Columns + @"
VALUES
    (11, TIMESTAMP '2026-06-01 00:00:00', 1, 'S1', 200, 'j', 'Job', true, 0, 1, TIMESTAMP '2025-12-31 23:20:00', 10, 0, 'hot-B'),
    (12, TIMESTAMP '2026-06-01 00:00:00', 1, 'S1', 300, 'j', 'Job', true, 0, 1, NULL, 10, 0, 'hot-NULL'),
    (13, TIMESTAMP '2026-06-01 00:00:00', 1, 'S1', 400, 'j', 'Job', true, 0, 1, TIMESTAMP '2026-05-31 23:50:00', 10, 0, 'hot-D')";
            await StageArchiveAndRecollectAsync(connection, "job_history", archived, recollected);
        }

        await initializer.CreateArchiveViewsAsync();

        using (var connection = await OpenAsync())
        {
            /* Four logical runs, not six: 200 (same non-NULL run_datetime) and 300 (NULL run_datetime) collapse. */
            Assert.Equal(4, await ScalarAsync<int>(connection, "SELECT COUNT(*) FROM v_job_history"));
            Assert.Equal(1, await ScalarAsync<int>(connection,
                "SELECT MAX(c) FROM (SELECT COUNT(*) AS c FROM v_job_history GROUP BY server_id, instance_id)"));
            Assert.Equal("hot-B", await ScalarAsync<string>(connection,
                "SELECT message FROM v_job_history WHERE instance_id = 200"));
            Assert.Equal("hot-NULL", await ScalarAsync<string>(connection,
                "SELECT message FROM v_job_history WHERE instance_id = 300"));
            Assert.Equal("archived-A", await ScalarAsync<string>(connection,
                "SELECT message FROM v_job_history WHERE instance_id = 100"));
        }
    }

    /// <summary>
    /// #5457: DuckDB moves a filter below a window only when the filter reads nothing but PARTITION BY columns.
    /// With the dedup keyed on (server_id, instance_id) alone, a Job History read's run_datetime filter stayed
    /// ABOVE the window, so every read loaded and sorted the server's whole archive (message text included)
    /// before it could drop a row: 11 s and ~0.7 GB per server read on a 30-server store, past Lite's memory
    /// cap on a larger one. run_datetime in the key lets the filter reach the table and parquet scans. This pins
    /// the plan shape of the predicate pair the Job History readers use.
    /// </summary>
    [Fact]
    public async Task VJobHistory_RunDatetimeFilterRunsBelowTheDedupAndReachesBothScans()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();

        const string Columns = @"
    (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name,
     job_enabled, step_id, run_status, run_datetime, run_duration_seconds, retries_attempted, message)";

        using (var connection = await OpenAsync())
        {
            var archived = "INSERT INTO job_history" + Columns + @"
VALUES
    (1, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', 100, 'j', 'Job', true, 0, 1, TIMESTAMP '2025-12-31 23:10:00', 10, 0, 'archived-A'),
    (2, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', 101, 'j', 'Job', true, 0, 1, TIMESTAMP '2025-06-01 00:00:00', 10, 0, 'archived-old')";
            var recollected = "INSERT INTO job_history" + Columns + @"
VALUES
    (11, TIMESTAMP '2026-06-01 00:00:00', 1, 'S1', 400, 'j', 'Job', true, 0, 1, TIMESTAMP '2026-05-31 23:50:00', 10, 0, 'hot-D'),
    (12, TIMESTAMP '2026-06-01 00:00:00', 1, 'S1', 401, 'j', 'Job', true, 0, 1, TIMESTAMP '2025-11-15 00:00:00', 10, 0, 'hot-old')";
            await StageArchiveAndRecollectAsync(connection, "job_history", archived, recollected);
        }

        await initializer.CreateArchiveViewsAsync();

        using (var connection = await OpenAsync())
        {
            /* Each side holds one row before the cutoff and one after it, so DuckDB can neither prove from the file or
               table statistics that nothing matches (the scan becomes an EMPTY_RESULT node with no filter to pin)
               nor that everything does (the filter is dropped from the scan). */
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"EXPLAIN (FORMAT JSON)
SELECT * FROM v_job_history WHERE run_datetime >= TIMESTAMP '2025-12-01 00:00:00' AND server_id = 1";
            string planJson;
            using (var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken))
            {
                Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
                planJson = reader.GetString(1);
            }

            var filtersOnRunDatetime = new List<string>();
            var scansWithRunDatetime = new List<string>();
            var scansWithoutRunDatetime = new List<string>();
            using var doc = JsonDocument.Parse(planJson);
            foreach (var root in doc.RootElement.EnumerateArray())
                WalkPlan(root, filtersOnRunDatetime, scansWithRunDatetime, scansWithoutRunDatetime);

            Assert.True(filtersOnRunDatetime.Count == 0,
                "run_datetime must be pushed below the dedup window, but a FILTER node still reads it above the dedup: "
                + string.Join(" | ", filtersOnRunDatetime) + "\n" + planJson);
            /* Both the hot table scan and the parquet scan carry it. */
            Assert.True(scansWithoutRunDatetime.Count == 0,
                "a scan under the dedup does not carry the run_datetime filter: "
                + string.Join(" | ", scansWithoutRunDatetime) + "\n" + planJson);
            Assert.True(scansWithRunDatetime.Count >= 2,
                "expected the table scan and the parquet scan to both carry the run_datetime filter, saw "
                + scansWithRunDatetime.Count + "\n" + planJson);
        }
    }

    private static void WalkPlan(
        JsonElement node, List<string> filtersOnRunDatetime, List<string> scansWith, List<string> scansWithout)
    {
        var name = node.TryGetProperty("name", out var n) ? n.GetString() ?? "" : "";
        string extra = node.TryGetProperty("extra_info", out var e) ? e.GetRawText() : "";
        if (name == "FILTER" && extra.Contains("run_datetime", StringComparison.Ordinal))
            filtersOnRunDatetime.Add(extra);
        if (name is "SEQ_SCAN" or "READ_PARQUET" or "TABLE_SCAN")
        {
            if (extra.Contains("run_datetime>=", StringComparison.Ordinal))
                scansWith.Add(name);
            else
                scansWithout.Add(name + " " + extra);
        }
        if (node.TryGetProperty("children", out var children))
            foreach (var child in children.EnumerateArray())
                WalkPlan(child, filtersOnRunDatetime, scansWith, scansWithout);
    }

    [Fact]
    public async Task VDefaultTraceEvents_DedupsReCollectedRowsOnEventTimeAndSequence()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();

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
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();
        using (var connection = await OpenAsync())
        {
            await StageArchiveAndRecollectAsync(connection, "deadlocks", archived, live);
        }
        await initializer.CreateArchiveViewsAsync();
        using (var connection = await OpenAsync())
        {
            return await ScalarAsync<int>(connection, "SELECT COUNT(*) FROM " + StoredEventCopies.Deadlocks("TRUE") + " AS dl");
        }
    }

    [Fact]
    public async Task VDeadlocks_IsAPlainUnion_ReadingEveryCopy()
    {
        var archived = $@"INSERT INTO deadlocks {DeadlockInsertColumns} VALUES
    (1, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:00', 'p1', 'q', '<deadlock>A</deadlock>')";
        var live = $@"INSERT INTO deadlocks {DeadlockInsertColumns} VALUES
    (11, TIMESTAMP '2026-06-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:00', 'p1', 'q', '<deadlock>A</deadlock>')";
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();
        using (var connection = await OpenAsync())
        {
            await StageArchiveAndRecollectAsync(connection, "deadlocks", archived, live);
        }
        await initializer.CreateArchiveViewsAsync();
        using (var connection = await OpenAsync())
        {
            /* The view keeps both copies of the stored deadlock; each read drops the copies it must not count. */
            Assert.Equal(2, await ScalarAsync<int>(connection, "SELECT COUNT(*) FROM v_deadlocks"));
        }
    }

    [Fact]
    public async Task DeadlockRows_ReadsAnArchivedCopyOfAStoredDeadlockOnce()
    {
        var archived = $@"INSERT INTO deadlocks {DeadlockInsertColumns} VALUES
    (1, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:00', 'p1', 'q', '<deadlock>A</deadlock>'),
    (2, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 11:00:00', 'p1', 'q', '<deadlock>B</deadlock>')";
        var live = $@"INSERT INTO deadlocks {DeadlockInsertColumns} VALUES
    (11, TIMESTAMP '2026-06-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:00', 'p1', 'q', '<deadlock>A</deadlock>')";
        Assert.Equal(2, await StageDeadlocksAsync(archived, live));
    }

    [Fact]
    public async Task DeadlockRows_ReadsTwoDifferentGraphsAtTheSameTimeBoth()
    {
        var archived = $@"INSERT INTO deadlocks {DeadlockInsertColumns} VALUES
    (1, TIMESTAMP '2026-01-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:00', 'p1', 'q', '<deadlock>A</deadlock>')";
        var live = $@"INSERT INTO deadlocks {DeadlockInsertColumns} VALUES
    (11, TIMESTAMP '2026-06-01 00:00:00', 1, 'S1', TIMESTAMP '2026-01-01 10:00:00', 'p1', 'q', '<deadlock>B</deadlock>')";
        Assert.Equal(2, await StageDeadlocksAsync(archived, live));
    }

    [Fact]
    public async Task DeadlockRows_NeverCollapsesRowsWithoutAGraph()
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
