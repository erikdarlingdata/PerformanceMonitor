/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8570: the plan fetch of the plan advisory fact and its drill-down ranks thin rows and reads the ten winning plans afterwards
/// (<see cref="DuckDbFactCollector.PlanAdvisorySql"/>), where it used to carry every in-window row's plan XML through the window's sort. This
/// holds the new statement to the old one's exact rows: the same ten plans in the same order, over a window with several snapshots of each
/// plan, two databases sharing query hashes, rows with no plan, hashless rows that rank by plan handle, and plans older or newer than the window.
/// The Darling twin is <c>PlanAdvisoryReadLiveTests</c>.
/// </summary>
public sealed class PlanAdvisoryFetchRowsTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 857_001;

    private static readonly DateTime WindowEnd = DateTime.SpecifyKind(
        new DateTime(DateTime.UtcNow.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);

    /// <summary>The statement as it was before #8570, kept here so the new read is held to its rows.</summary>
    private const string OldSql = @"
SELECT query_plan_xml
FROM
(
    SELECT
        query_plan_xml,
        delta_worker_time,
        ROW_NUMBER() OVER (PARTITION BY database_name, query_hash, COALESCE(query_plan_hash, plan_handle) ORDER BY delta_worker_time DESC) AS plan_rank
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   query_plan_xml IS NOT NULL
) AS ranked
WHERE plan_rank = 1
ORDER BY delta_worker_time DESC
LIMIT 10";

    private readonly DuckDbInitializer _duckDb;

    public PlanAdvisoryFetchRowsTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public async Task ThePlanFetch_ReturnsTheOldStatementsTenPlansInTheSameOrder()
    {
        await SeedAsync();

        var expected = await ReadAsync(OldSql);
        var actual = await ReadAsync(DuckDbFactCollector.PlanAdvisorySql);

        Assert.Equal(10, expected.Count);
        Assert.Equal(expected, actual);
        /* Ten distinct plans: the rank keeps one row per plan, and the fetch by collection_id must not bring a plan back twice. */
        Assert.Equal(10, new HashSet<string>(actual).Count);
    }

    /// <summary>
    /// Plans 1 to 30 in database A and plans 1 to 30 again in database B (the same query hashes, a plan of its own each), four snapshots each, the worker time
    /// distinct for every row. Plans 28 to 30 of each database carry no plan XML (never in the result), plans 20 to 24 carry no plan hash (they rank by plan handle),
    /// and plan 31 is the costliest of all but sits one hour after the window's end. The plan text is padded so a plan is far wider than the thin columns.
    /// </summary>
    private async Task SeedAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, plan_handle,
     creation_time, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_spills,
     query_text, query_plan_xml)
SELECT
    $1 + row_number() OVER (),
    CASE WHEN q = 31 THEN $2 + INTERVAL 1 HOUR ELSE $2 - s * INTERVAL 7 MINUTE END,
    $3, 'FetchRowsSrv', d, 'QH' || q,
    CASE WHEN q BETWEEN 20 AND 24 THEN NULL ELSE 'PH' || q END,
    'HANDLE' || d || q,
    $2 - INTERVAL 1 DAY, 10,
    (CASE WHEN q = 31 THEN 5000000 ELSE 100000 + q * 1000 + (CASE WHEN d = 'DbA' THEN 0 ELSE 500 END) END) - s * 3,
    1000000, 1000, 0, 'SELECT 1',
    CASE WHEN q BETWEEN 28 AND 30 THEN NULL ELSE '<plan db=""' || d || '"" q=""' || q || '"">' || repeat('<RelOp NodeId=""1""/>', 200) || '</plan>' END
FROM (VALUES ('DbA'), ('DbB')) AS dbs(d)
CROSS JOIN range(1, 32) AS qs(q)
CROSS JOIN range(0, 4) AS ss(s)";
        command.Parameters.Add(new DuckDBParameter { Value = 8_570_000_000L });
        command.Parameters.Add(new DuckDBParameter { Value = WindowEnd });
        command.Parameters.Add(new DuckDBParameter { Value = ServerId });
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// #9195: the plan's costliest row is now picked by one hash aggregate in place of a window function ordered by worker time, so its edges are held
    /// here against the old statement. Plan 1 has a NULL worker time on its newest snapshot and a real one on the older ones, and its plan text
    /// differs by snapshot, so the text read says which snapshot stood for it (the costliest real one, never the NULL one). Plan 2 has a NULL worker
    /// time on every snapshot and still stands for itself, after the plans that have one. Plans 3 to 5 repeat their costliest value across snapshots
    /// that carry the same text, since the old statement's pick among equal rows was arbitrary and only the text it reads can be compared.
    /// </summary>
    [Fact]
    public async Task ThePlanFetch_NullAndRepeatedWorkerTimes_GiveTheOldStatementsPlans()
    {
        const int server = 857_002;
        using (var readLock = _duckDb.AcquireReadLock())
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync();
            using var command = connection.CreateCommand();
            command.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, plan_handle,
     creation_time, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_spills,
     query_text, query_plan_xml)
SELECT
    $1 + row_number() OVER (),
    $2 - s * INTERVAL 7 MINUTE,
    $3, 'FetchRowsSrv2', 'DbA', 'QH' || q, 'PH' || q, 'HANDLE' || q,
    $2 - INTERVAL 1 DAY, 10,
    CASE
        WHEN q = 1 THEN (CASE WHEN s = 0 THEN NULL ELSE 9000 - s * 10 END)
        WHEN q = 2 THEN NULL
        WHEN q = 3 THEN (CASE WHEN s IN (1, 2) THEN 7000 ELSE 100 END)
        ELSE 1000 * q
    END,
    1000000, 1000, 0, 'SELECT 1',
    '<plan q=""' || q || '"" s=""' || (CASE WHEN q = 1 THEN s ELSE 0 END) || '""/>'
FROM range(1, 6) AS qs(q)
CROSS JOIN range(0, 4) AS ss(s)";
            command.Parameters.Add(new DuckDBParameter { Value = 9_195_000_000L });
            command.Parameters.Add(new DuckDBParameter { Value = WindowEnd });
            command.Parameters.Add(new DuckDBParameter { Value = server });
            await command.ExecuteNonQueryAsync();
        }

        var expected = await ReadAsync(OldSql, server);
        var actual = await ReadAsync(DuckDbFactCollector.PlanAdvisorySql, server);

        Assert.Equal(5, expected.Count);
        Assert.Equal(expected, actual);
        /* Not only equal to the old statement: the order is the costliest real worker time first and the all-NULL plan last. */
        Assert.Equal(
            ["<plan q=\"1\" s=\"1\"/>", "<plan q=\"3\" s=\"0\"/>", "<plan q=\"5\" s=\"0\"/>", "<plan q=\"4\" s=\"0\"/>", "<plan q=\"2\" s=\"0\"/>"],
            actual);
    }

    private async Task<List<string>> ReadAsync(string sql, int serverId = ServerId)
    {
        var rows = new List<string>();
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = WindowEnd.AddHours(-4) });
        command.Parameters.Add(new DuckDBParameter { Value = WindowEnd });
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(reader.GetString(0));
        }

        return rows;
    }
}
