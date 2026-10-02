using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// <see cref="StoredEventCopies.Deadlocks"/>: a stored deadlock reads once, as the copy the startup cleanup keeps (the
/// earliest collection_time, the lowest deadlock_id breaking a tie), even when both copies come from one batch.
/// The other three tables keep a whole first batch, so these rules are not part of their shared tests.
/// </summary>
public class StoredEventCopiesDeadlockTests : IDisposable
{
    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
    private readonly string _dbPath;

    public StoredEventCopiesDeadlockTests()
    {
        Directory.CreateDirectory(Path.Combine(_tempDir, "archive"));
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
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

    private const string Columns = "(deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, deadlock_graph_xml)";
    private const string Archived = "2026-01-01 00:00:00";
    private const string Batch = "2026-06-01 00:00:00";
    private const string D1 = "2026-05-31 23:51:00";
    private const string D2 = "2026-05-31 23:54:00";
    private const string D3 = "2026-05-31 22:30:00";

    private static string Row(int id, string collected, string? time, string? graph) =>
        $"({id}, TIMESTAMP '{collected}', 1, 'S1', {(time is null ? "NULL" : $"TIMESTAMP '{time}'")}, 'p1', "
        + $"{(graph is null ? "NULL" : $"'{graph}'")})";

    /* The archived rows go to a parquet file the view reads and the hot table is emptied; the hot rows are stored
       after that, the way a cycle after the 512 MB reset stores them. */
    private async Task<DuckDBConnection> StageAsync(string[] archived, string[] hot)
    {
        await new DuckDbInitializer(_dbPath).InitializeAsync();
        var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        if (archived.Length > 0)
        {
            await ExecuteAsync(connection, $"INSERT INTO deadlocks {Columns} VALUES {string.Join(", ", archived)}");
            var parquet = Path.Combine(_tempDir, "archive", "20260601_0000_deadlocks.parquet").Replace("\\", "/");
            await ExecuteAsync(connection, $"COPY deadlocks TO '{parquet}' (FORMAT PARQUET)");
            await ExecuteAsync(connection, "DELETE FROM deadlocks");
        }

        if (hot.Length > 0)
            await ExecuteAsync(connection, $"INSERT INTO deadlocks {Columns} VALUES {string.Join(", ", hot)}");
        await new DuckDbInitializer(_dbPath).CreateArchiveViewsAsync();
        return connection;
    }

    private static async Task ExecuteAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task<long[]> IdsAsync(DuckDBConnection connection, string? collectedFrom = null, string where = "server_id = 1")
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT deadlock_id FROM {StoredEventCopies.Deadlocks(where, collectedFrom)} AS r ORDER BY deadlock_id";
        var ids = new System.Collections.Generic.List<long>();
        using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
            ids.Add(Convert.ToInt64(reader.GetValue(0)));
        return ids.ToArray();
    }

    [Fact]
    public async Task ATelemetryCopyAndARingBufferCopyFromOneBatch_ReadAsTheLowestId()
    {
        using var connection = await StageAsync([], [Row(8, Batch, D1, "<d>A</d>"), Row(5, Batch, D1, "<d>A</d>"), Row(9, Batch, D2, "<d>B</d>")]);

        Assert.Equal(new long[] { 5L, 9L }, await IdsAsync(connection));
    }

    [Fact]
    public async Task AnArchivedFirstCopyAndAHotLaterCopy_ReadAsTheArchivedOne()
    {
        using var connection = await StageAsync([Row(1, Archived, D1, "<d>A</d>")], [Row(11, Batch, D1, "<d>A</d>"), Row(12, Batch, D2, "<d>B</d>")]);

        Assert.Equal(new long[] { 1L, 12L }, await IdsAsync(connection));
    }

    [Fact]
    public async Task GraphsThatDifferOnlyInText_BothStay()
    {
        using var connection = await StageAsync([], [Row(1, Batch, D1, "<d>A</d>"), Row(2, Batch, D1, "<d>a</d>"), Row(3, Batch, D1, "<d>A </d>")]);

        Assert.Equal(new long[] { 1L, 2L, 3L }, await IdsAsync(connection));
    }

    [Fact]
    public async Task RowsWithNoGraphOrNoTime_AreNeverCollapsed()
    {
        using var connection = await StageAsync(
            [Row(1, Archived, D1, null), Row(2, Archived, D1, null), Row(3, Archived, null, "<d>A</d>")],
            [Row(11, Batch, D1, null), Row(12, Batch, D1, ""), Row(13, Batch, null, "<d>A</d>"), Row(14, Batch, null, "<d>A</d>")]);

        Assert.Equal(new long[] { 1L, 2L, 3L, 11L, 12L, 13L, 14L }, await IdsAsync(connection));
    }

    [Fact]
    public async Task AReadFromACollectionTime_DropsACopyWhoseFirstCopyWasStoredJustBeforeIt()
    {
        const string firstStored = "2026-05-31 23:57:00";
        const string windowStart = "TIMESTAMP '2026-05-31 23:59:00'";
        using var connection = await StageAsync([Row(1, firstStored, D1, "<d>A</d>")], [Row(11, Batch, D1, "<d>A</d>"), Row(12, Batch, D2, "<d>B</d>")]);

        /* Only the deadlock first stored inside the window. */
        Assert.Equal(new long[] { 12L }, await IdsAsync(connection, windowStart));
        Assert.Equal(new long[] { 1L, 12L }, await IdsAsync(connection));
    }

    private static async Task<long> ScalarAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    /* A count reads the plain union with the shared identity, after its own filter. */
    private static Task<long> CountAsync(DuckDBConnection connection, string where = "server_id = 1") =>
        ScalarAsync(connection, $"SELECT {StoredEventCopies.DeadlockDistinctCount} FROM v_deadlocks AS dl WHERE {where}");

    [Fact]
    public async Task ACountWithCopies_CountsEachStoredDeadlockOnce()
    {
        /* An archived first copy plus a hot later copy, and a same-batch telemetry + ring pair. */
        using var connection = await StageAsync(
            [Row(1, Archived, D1, "<d>A</d>")],
            [Row(11, Batch, D1, "<d>A</d>"), Row(12, Batch, D2, "<d>B</d>"), Row(13, Batch, D2, "<d>B</d>")]);

        Assert.Equal(2L, await CountAsync(connection));
        Assert.Equal((long)(await IdsAsync(connection)).Length, await CountAsync(connection));
    }

    [Fact]
    public async Task ACountOfRowsWithNoGraphOrNoTime_CountsEachOne()
    {
        using var connection = await StageAsync(
            [Row(1, Archived, D1, null), Row(2, Archived, D1, null), Row(3, Archived, null, "<d>A</d>")],
            [Row(11, Batch, D1, null), Row(12, Batch, D1, ""), Row(13, Batch, null, "<d>A</d>"), Row(14, Batch, null, "<d>A</d>")]);

        Assert.Equal(7L, await CountAsync(connection));
    }

    [Fact]
    public async Task ABucketedCountWithCopies_CountsOncePerBucket()
    {
        using var connection = await StageAsync(
            [Row(1, Archived, D1, "<d>A</d>"), Row(2, Archived, D2, "<d>B</d>"), Row(3, Archived, D3, "<d>D</d>")],
            [Row(11, Batch, D1, "<d>A</d>"), Row(12, Batch, D2, "<d>B</d>"), Row(13, Batch, D2, "<d>C</d>"), Row(14, Batch, D3, "<d>D</d>")]);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT date_trunc('hour', deadlock_time) AS bucket, {StoredEventCopies.DeadlockDistinctCount} "
            + "FROM v_deadlocks AS dl WHERE server_id = 1 GROUP BY date_trunc('hour', deadlock_time) ORDER BY bucket";
        var counts = new System.Collections.Generic.List<long>();
        using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
            counts.Add(Convert.ToInt64(reader.GetValue(1)));

        /* A, B and C fall in the 23:00 hour, each once; D falls in the 22:00 hour with an archived and a hot copy, once. */
        Assert.Equal(new long[] { 1L, 3L }, counts.ToArray());
    }

    [Fact]
    public async Task AMaxOfDeadlockTime_IsTheSameWithCopiesPresent()
    {
        using var connection = await StageAsync(
            [Row(1, Archived, D1, "<d>A</d>")],
            [Row(11, Batch, D1, "<d>A</d>"), Row(12, Batch, D2, "<d>B</d>"), Row(13, Batch, D2, "<d>B</d>")]);

        var plain = await ScalarAsync(connection, "SELECT epoch(MAX(deadlock_time)) FROM v_deadlocks WHERE server_id = 1");
        var helper = await ScalarAsync(connection, $"SELECT epoch(MAX(deadlock_time)) FROM {StoredEventCopies.Deadlocks("server_id = 1")} AS dl");
        Assert.Equal(helper, plain);
    }

    /* The count SQL holds the shared identity once, joins nothing back, and reads the graph only through hash(). */
    [Fact]
    public void TheCountSql_UsesTheSharedIdentity_WithNoJoinBack_AndNeverSelectsTheGraph()
    {
        var count = StoredEventCopies.DeadlockDistinctCount;
        Assert.StartsWith("COUNT(DISTINCT ", count, StringComparison.Ordinal);
        Assert.Contains(StoredEventCopies.DeadlockIdentityTuple, count, StringComparison.Ordinal);
        Assert.DoesNotContain("JOIN (", count, StringComparison.Ordinal);

        /* Every mention of the graph column outside hash(…) and the NULL/'' tests of the never-collapse parts. */
        var bare = count.Replace("hash(deadlock_graph_xml)", "", StringComparison.Ordinal)
            .Replace("deadlock_graph_xml IS NULL OR deadlock_graph_xml = ''", "", StringComparison.Ordinal);
        Assert.DoesNotContain("deadlock_graph_xml", bare, StringComparison.Ordinal);

        /* The helper's grouped side keys by the same parts. */
        var helper = StoredEventCopies.Deadlocks("server_id = 1");
        foreach (var part in new[] { "server_id", "deadlock_time", "hash(deadlock_graph_xml)" })
            Assert.Contains(part, helper, StringComparison.Ordinal);
    }

    /* The three other tables send the SQL they sent before deadlocks joined the helper, byte for byte. */
    [Fact]
    public void TheOtherThreeTables_KeepTheirSql()
    {
        Assert.Equal("(SELECT v.* FROM v_blocked_process_reports AS v JOIN (SELECT k0, k1, k2, k3, k4, MIN(ct) AS first_ct FROM (SELECT server_id AS k0, event_time AS k1, hash(blocked_process_report_xml) AS k2, CASE WHEN blocked_process_report_xml IS NULL OR blocked_process_report_xml = '' OR event_time IS NULL THEN blocked_report_id END AS k3, CASE WHEN blocked_process_report_xml IS NULL OR blocked_process_report_xml = '' OR event_time IS NULL THEN collection_time END AS k4, collection_time AS ct FROM v_blocked_process_reports WHERE (server_id = 1) AND collection_time >= CAST($2 AS TIMESTAMP) - INTERVAL 600 SECOND) GROUP BY k0, k1, k2, k3, k4) AS g ON (server_id) IS NOT DISTINCT FROM g.k0 AND (event_time) IS NOT DISTINCT FROM g.k1 AND (hash(blocked_process_report_xml)) IS NOT DISTINCT FROM g.k2 AND (CASE WHEN blocked_process_report_xml IS NULL OR blocked_process_report_xml = '' OR event_time IS NULL THEN blocked_report_id END) IS NOT DISTINCT FROM g.k3 AND (CASE WHEN blocked_process_report_xml IS NULL OR blocked_process_report_xml = '' OR event_time IS NULL THEN collection_time END) IS NOT DISTINCT FROM g.k4 AND v.collection_time = g.first_ct WHERE (server_id = 1) AND collection_time >= $2)", StoredEventCopies.BlockedProcessReports("server_id = 1", "$2"));
        Assert.Equal("(SELECT v.* FROM v_long_query_completions AS v JOIN (SELECT k0, k1, k2, k3, k4, k5, k6, k7, MIN(ct) AS first_ct FROM (SELECT server_id AS k0, database_name AS k1, event_time AS k2, statement_text AS k3, session_id AS k4, event_sequence AS k5, CASE WHEN statement_text IS NULL OR statement_text = '' OR event_time IS NULL THEN long_query_completion_id END AS k6, CASE WHEN statement_text IS NULL OR statement_text = '' OR event_time IS NULL THEN collection_time END AS k7, collection_time AS ct FROM v_long_query_completions WHERE (server_id = 1)) GROUP BY k0, k1, k2, k3, k4, k5, k6, k7) AS g ON (server_id) IS NOT DISTINCT FROM g.k0 AND (database_name) IS NOT DISTINCT FROM g.k1 AND (event_time) IS NOT DISTINCT FROM g.k2 AND (statement_text) IS NOT DISTINCT FROM g.k3 AND (session_id) IS NOT DISTINCT FROM g.k4 AND (event_sequence) IS NOT DISTINCT FROM g.k5 AND (CASE WHEN statement_text IS NULL OR statement_text = '' OR event_time IS NULL THEN long_query_completion_id END) IS NOT DISTINCT FROM g.k6 AND (CASE WHEN statement_text IS NULL OR statement_text = '' OR event_time IS NULL THEN collection_time END) IS NOT DISTINCT FROM g.k7 AND v.collection_time = g.first_ct WHERE (server_id = 1))", StoredEventCopies.LongQueryCompletions("server_id = 1"));
        Assert.Equal("(SELECT v.* FROM v_system_health_events AS v JOIN (SELECT k0, k1, k2, k3, k4, MIN(ct) AS first_ct FROM (SELECT server_id AS k0, event_time AS k1, hash(event_xml) AS k2, CASE WHEN event_xml IS NULL OR event_xml = '' OR event_time IS NULL THEN system_health_event_id END AS k3, CASE WHEN event_xml IS NULL OR event_xml = '' OR event_time IS NULL THEN collection_time END AS k4, collection_time AS ct FROM v_system_health_events WHERE (server_id = 1) AND collection_time >= CAST($2 AS TIMESTAMP) - INTERVAL 600 SECOND) GROUP BY k0, k1, k2, k3, k4) AS g ON (server_id) IS NOT DISTINCT FROM g.k0 AND (event_time) IS NOT DISTINCT FROM g.k1 AND (hash(event_xml)) IS NOT DISTINCT FROM g.k2 AND (CASE WHEN event_xml IS NULL OR event_xml = '' OR event_time IS NULL THEN system_health_event_id END) IS NOT DISTINCT FROM g.k3 AND (CASE WHEN event_xml IS NULL OR event_xml = '' OR event_time IS NULL THEN collection_time END) IS NOT DISTINCT FROM g.k4 AND v.collection_time = g.first_ct WHERE (server_id = 1) AND collection_time >= $2)", StoredEventCopies.SystemHealthEvents("server_id = 1", "$2"));
    }

    /* One generated read, for the record: the grouped side holds the keys and an exact (collection_time,
       deadlock_id) minimum, and the join back matches it on both. */
    [Fact]
    public void ADeadlockRead_TakesTheExactMinimumPerIdentity()
    {
        var sql = StoredEventCopies.Deadlocks("server_id = 1", "$2");
        Assert.Contains("MIN(struct_pack(ct := collection_time, id := deadlock_id)) AS first_row", sql, StringComparison.Ordinal);
        Assert.Contains("v.collection_time IS NOT DISTINCT FROM g.first_row.ct AND v.deadlock_id IS NOT DISTINCT FROM g.first_row.id", sql, StringComparison.Ordinal);
        Assert.Contains("hash(deadlock_graph_xml)", sql, StringComparison.Ordinal);
        Assert.Contains("INTERVAL 600 SECOND", sql, StringComparison.Ordinal);
    }
}
