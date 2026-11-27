/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9233: the High Impact read picked each hash's grant with a per-hash LATERAL that sorted the whole window, and its sample
/// text with a ROW_NUMBER sort that carried the text of every row of the kept hashes. Both are now one grouped pass. The new
/// statement must return the same rows. This runs the statement the app ships and the one it shipped before (kept here as the
/// oracle) over one seeded window and compares every column of every row. The seed holds the cases where a NULL sort key
/// decides the pick: a NULL creation_time and a NULL delta_worker_time (the newest row's grant), a NULL count (the sample
/// text), ties on collection_time, hashes with several plans and several databases, an empty and a NULL text.
/// </summary>
public sealed class HighImpactReadEqualityTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = -9233;
    private const string ServerName = "lite-high-impact-equality";

    private readonly DuckDbInitializer _duckDb;

    public HighImpactReadEqualityTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    /// <summary>The statement as it shipped before #9233: the grant from a per-hash LATERAL, the sample text from a ROW_NUMBER over the rows.</summary>
    private const string OracleSql = @"
WITH win AS MATERIALIZED
(
    SELECT query_hash, database_name, delta_execution_count, delta_worker_time, delta_elapsed_time,
           delta_logical_reads, delta_logical_writes, max_grant_kb, collection_time, creation_time
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   query_hash IS NOT NULL AND query_hash != ''
),
per_database AS
(
    SELECT query_hash, database_name,
           SUM(delta_execution_count) AS executions,
           SUM(delta_worker_time) AS worker_time,
           SUM(delta_elapsed_time) AS elapsed_time,
           SUM(delta_logical_reads) AS logical_reads,
           SUM(delta_logical_writes) AS logical_writes
    FROM win
    WHERE delta_execution_count > 0
    GROUP BY query_hash, database_name
),
per_hash AS
(
    SELECT
        query_hash,
        (ARRAY_AGG(database_name ORDER BY worker_time DESC NULLS LAST, LOWER(database_name), database_name)
            FILTER (WHERE database_name IS NOT NULL))[1] AS database_name,
        SUM(executions) AS total_executions,
        SUM(worker_time) AS total_worker_time,
        SUM(elapsed_time) AS total_elapsed_time,
        SUM(logical_reads) AS total_reads,
        SUM(logical_writes) AS total_writes,
        COUNT(DISTINCT database_name) AS database_count
    FROM per_database
    GROUP BY query_hash
    HAVING SUM(executions) > 0
),
ranked AS
(
    SELECT h.*, COALESCE(g.max_grant_kb, 0) AS peak_grant_kb,
           ROW_NUMBER() OVER (ORDER BY COALESCE(h.total_worker_time, 0) DESC, h.total_worker_time DESC, h.query_hash) AS cpu_rank,
           ROW_NUMBER() OVER (ORDER BY COALESCE(h.total_elapsed_time, 0) DESC, h.total_worker_time DESC, h.query_hash) AS duration_rank,
           ROW_NUMBER() OVER (ORDER BY COALESCE(h.total_reads, 0) DESC, h.total_worker_time DESC, h.query_hash) AS reads_rank,
           ROW_NUMBER() OVER (ORDER BY COALESCE(h.total_writes, 0) DESC, h.total_worker_time DESC, h.query_hash) AS writes_rank,
           ROW_NUMBER() OVER (ORDER BY COALESCE(g.max_grant_kb, 0) DESC, h.total_worker_time DESC, h.query_hash) AS memory_rank,
           ROW_NUMBER() OVER (ORDER BY h.total_executions DESC, h.total_worker_time DESC, h.query_hash) AS executions_rank
    FROM per_hash AS h
    LEFT JOIN LATERAL
    (
        SELECT qs.max_grant_kb
        FROM win AS qs
        WHERE qs.query_hash = h.query_hash
        AND   qs.delta_execution_count > 0
        ORDER BY qs.collection_time DESC, qs.creation_time DESC NULLS LAST, qs.delta_worker_time DESC NULLS LAST
        LIMIT 1
    ) AS g ON true
),
kept AS
(
    SELECT *
    FROM ranked
    WHERE cpu_rank <= $3 OR duration_rank <= $3 OR reads_rank <= $3
    OR    writes_rank <= $3 OR memory_rank <= $3 OR executions_rank <= $3
),
sample_text AS
(
    SELECT qs2.query_hash, qs2.query_text
    FROM v_query_stats AS qs2
    JOIN kept AS kk ON kk.query_hash = qs2.query_hash
    WHERE qs2.server_id = $1
    AND   qs2.collection_time >= $2
    AND   qs2.query_text IS NOT NULL AND qs2.query_text != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY qs2.query_hash ORDER BY qs2.delta_execution_count DESC NULLS LAST, qs2.collection_time DESC, qs2.creation_time DESC NULLS LAST, qs2.query_text) = 1
)
SELECT
    k.query_hash,
    k.database_name,
    k.total_executions,
    k.total_worker_time / 1000.0 AS total_cpu_ms,
    k.total_elapsed_time / 1000.0 AS total_duration_ms,
    k.total_reads,
    k.total_writes,
    k.peak_grant_kb / 1024.0 AS peak_memory_mb,
    k.database_count,
    LEFT(t.query_text, 200) AS sample_query_text,
    t.query_text AS full_query_text
FROM kept AS k
LEFT JOIN sample_text AS t ON t.query_hash = k.query_hash
ORDER BY k.total_worker_time DESC, k.query_hash";

    private static readonly DateTime Now = DateTime.UtcNow;
    private static readonly DateTime T1 = Now.AddHours(-5);
    private static readonly DateTime Plan1 = Now.AddDays(-9);
    private static readonly DateTime Plan2 = Now.AddDays(-4);

    private static readonly object Null = DBNull.Value;

    private async Task SeedAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        long id = -9_233_000;

        async Task Row(string hash, string db, object? text, DateTime at, object creation, object count, object cpu, object grant)
        {
            using var command = connection.CreateCommand();
            command.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text, creation_time,
     delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes, max_grant_kb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)";
            foreach (var value in new object[] { id--, at, ServerId, ServerName, db, hash, text ?? Null, creation, count, cpu, 1_000L, 10L, 1L, grant })
            {
                command.Parameters.Add(new DuckDBParameter { Value = value });
            }

            await command.ExecuteNonQueryAsync();
        }

        /* Grant: the newest collection, then the newest plan creation (a NULL creation last), then the most worker time (a NULL last). */
        await Row("0xA", "Db1", "SELECT a", T1, Null, 5L, 100L, 100L);              // a NULL creation_time loses to a real one: 200
        await Row("0xA", "Db1", "SELECT a", T1, Plan1, 5L, 100L, 200L);
        await Row("0xB", "Db1", "SELECT b", T1, Plan1, 5L, Null, 300L);             // a NULL delta_worker_time loses to a real one: 400
        await Row("0xB", "Db1", "SELECT b", T1, Plan1, 5L, 7L, 400L);
        await Row("0xC", "Db1", "SELECT c", T1, Plan2, 5L, 100L, 900L);             // the newer collection wins whatever the grant: 50
        await Row("0xC", "Db1", "SELECT c", T1.AddMinutes(1), Plan1, 5L, 1L, 50L);
        await Row("0xD", "Db1", "SELECT d", T1, Plan1, 5L, 100L, 600L);             // a newest row with no executions is no reading: 600
        await Row("0xD", "Db1", "SELECT d", T1.AddMinutes(1), Plan2, 0L, 100L, 9_000L);
        await Row("0xE", "Db1", "SELECT e", T1, Plan1, 5L, 100L, 700L);             // the newest row's grant is NULL: 0
        await Row("0xE", "Db1", "SELECT e", T1.AddMinutes(1), Plan2, 5L, 100L, Null);
        await Row("0xF", "Db1", "SELECT f", T1, Null, 5L, Null, 11L);               // both keys NULL on one row, a real key on the other: 22
        await Row("0xF", "Db1", "SELECT f", T1, Plan2, 5L, 3L, 22L);
        await Row("0xG", "Db2", "SELECT g", T1, Plan1, 5L, 100L, 33L);              // the same figures in two databases: the same grant either way
        await Row("0xG", "Db3", "SELECT g", T1, Plan1, 5L, 100L, 33L);

        /* Sample text: the most executions (a NULL count last), then the newest collection, then the newest creation (a NULL last), then the smallest text. */
        await Row("0xH", "Db1", "SELECT h-null-count", T1.AddMinutes(5), Plan2, Null, 10L, 1L);   // a NULL count ranks last: "SELECT h-few"
        await Row("0xH", "Db1", "SELECT h-few", T1, Plan1, 1L, 10L, 1L);
        await Row("0xI", "Db1", "SELECT i-null-creation", T1, Null, 4L, 10L, 1L);                 // a NULL creation ranks last: "SELECT i-real"
        await Row("0xI", "Db1", "SELECT i-real", T1, Plan1, 4L, 10L, 1L);
        await Row("0xJ", "Db1", "SELECT j-b", T1, Plan1, 4L, 10L, 1L);                            // a full tie: the smaller text, "SELECT j-a"
        await Row("0xJ", "Db2", "SELECT j-a", T1, Plan1, 4L, 10L, 1L);
        await Row("0xK", "Db1", "SELECT k-old", T1, Plan1, 4L, 10L, 1L);                          // the newer collection wins a count tie: "SELECT k-new"
        await Row("0xK", "Db1", "SELECT k-new", T1.AddMinutes(1), Plan1, 4L, 10L, 1L);
        await Row("0xL", "Db1", "", T1.AddMinutes(1), Plan1, 9L, 10L, 1L);                        // an empty and a NULL text are no sample: "SELECT l"
        await Row("0xL", "Db1", null, T1.AddMinutes(2), Plan1, 9L, 10L, 1L);
        await Row("0xL", "Db1", "SELECT l", T1, Plan1, 1L, 10L, 1L);
        await Row("0xM", "Db1", null, T1, Plan1, 3L, 10L, 1L);                                    // no text anywhere: a NULL sample

        /* Filler, so the keep count cuts the window: each measure puts a different hash on top. */
        for (var i = 0; i < 40; i++)
        {
            await Row("0xZ" + i.ToString("D2", CultureInfo.InvariantCulture), "Db" + (i % 3), "SELECT z" + i, T1.AddMinutes(i % 4),
                i % 5 == 0 ? Null : Plan1, (i * 7) % 13 + 1L, i % 6 == 0 ? Null : (i * 11) % 17 * 100L, (i * 13) % 19 * 1024L);
        }

        /* A row outside the window is not read. */
        await Row("0xOLD", "Db1", "SELECT old", Now.AddDays(-3), Plan1, 99L, 999L, 99_999L);
    }

    private async Task<List<string>> RunAsync(string sql, int keep)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = sql.Replace("v_query_stats", "query_stats", StringComparison.Ordinal);
        command.Parameters.Add(new DuckDBParameter { Value = ServerId });
        command.Parameters.Add(new DuckDBParameter { Value = Now.AddHours(-24) });
        command.Parameters.Add(new DuckDBParameter { Value = keep });

        var rows = new List<string>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var cells = new string[reader.FieldCount];
            for (var c = 0; c < cells.Length; c++)
            {
                cells[c] = reader.IsDBNull(c) ? "<null>" : Convert.ToString(reader.GetValue(c), CultureInfo.InvariantCulture) ?? "";
            }

            rows.Add(string.Join("|", cells));
        }

        return rows;
    }

    [Theory]
    [InlineData(1000)]
    [InlineData(10)]
    [InlineData(3)]
    public async Task TheGroupedPasses_ReturnTheRowsTheLateralAndTheRowNumberSortReturned(int keep)
    {
        await SeedAsync();

        var oracle = await RunAsync(OracleSql, keep);
        var shipped = await RunAsync(LocalDataService.HighImpactQueriesSql, keep);

        Assert.True(oracle.Count >= 3, $"the seed must keep a spread of hashes, the oracle read {oracle.Count}");
        Assert.Equal(oracle, shipped);
    }

    /// <summary>The picks the seed was built around, read from the shipped statement, so a seed that stopped exercising a NULL key fails here and not silently.</summary>
    [Fact]
    public async Task TheShippedStatement_PicksTheGrantAndTheSampleTheNullKeysCallFor()
    {
        await SeedAsync();

        var shipped = await RunAsync(LocalDataService.HighImpactQueriesSql, 1000);
        string Cell(string hash, int column) => shipped.Single(r => r.StartsWith(hash + "|", StringComparison.Ordinal)).Split('|')[column];

        /* Column 7 is the peak memory in MB, the grant in KB over 1024. */
        Assert.Equal(200.0 / 1024, double.Parse(Cell("0xA", 7), CultureInfo.InvariantCulture), 1e-9);
        Assert.Equal(400.0 / 1024, double.Parse(Cell("0xB", 7), CultureInfo.InvariantCulture), 1e-9);
        Assert.Equal(50.0 / 1024, double.Parse(Cell("0xC", 7), CultureInfo.InvariantCulture), 1e-9);
        Assert.Equal(600.0 / 1024, double.Parse(Cell("0xD", 7), CultureInfo.InvariantCulture), 1e-9);
        Assert.Equal(0.0, double.Parse(Cell("0xE", 7), CultureInfo.InvariantCulture), 1e-9);
        Assert.Equal(22.0 / 1024, double.Parse(Cell("0xF", 7), CultureInfo.InvariantCulture), 1e-9);

        /* Column 10 is the full sample text. */
        Assert.Equal("SELECT h-few", Cell("0xH", 10));
        Assert.Equal("SELECT i-real", Cell("0xI", 10));
        Assert.Equal("SELECT j-a", Cell("0xJ", 10));
        Assert.Equal("SELECT k-new", Cell("0xK", 10));
        Assert.Equal("SELECT l", Cell("0xL", 10));
        Assert.Equal("<null>", Cell("0xM", 10));
    }
}
