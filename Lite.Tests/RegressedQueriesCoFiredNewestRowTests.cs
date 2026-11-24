/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9195: the regressed-queries drill-down flags a query whose plan cache rows also carry the parameter sensitivity signature. It finds the
/// signature on each plan's newest row, taken by one hash aggregate in place of a ROW_NUMBER() over the analysis window. These cases hold the
/// answer to what the window function gave, on a store with ties: a tie on collection_time goes to the larger collection_id whichever row was
/// written first, the newest row decides over an older one in both directions, a row with no executions in its snapshot never counts, a row
/// after the window end is ignored, and a query with two plans is flagged when either plan fires. The expected flags are written out and are
/// also computed by the ROW_NUMBER() form of the read, so the drill-down gives the same rows and the same JSON as it did.
/// </summary>
public sealed class RegressedQueriesCoFiredNewestRowTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 919_502;

    private static readonly DateTime WindowEnd = DateTime.SpecifyKind(
        new DateTime(DateTime.UtcNow.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);
    private static readonly DateTime WindowStart = WindowEnd.AddHours(-4);
    private static readonly DateTime Newest = WindowEnd.AddMinutes(-30);

    private readonly DuckDbInitializer _duckDb;
    private long _nextStoreId = 1;

    public RegressedQueriesCoFiredNewestRowTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private static AnalysisContext Context() => new()
    {
        ServerId = ServerId,
        ServerName = "CoFiredSrv",
        TimeRangeStart = WindowStart,
        TimeRangeEnd = WindowEnd,
    };

    /// <summary>(database, query_id, query_hash, expected flag). At most five regressed queries per database come back, so two databases hold the eight cases.</summary>
    private static readonly (string Database, long QueryId, string Hash, bool CoFired)[] Cases =
    [
        ("DbA", 1, "A_TIE_WIN", true),
        ("DbA", 2, "B_TIE_LOSE", false),
        ("DbA", 3, "D_OLDER_QUIET", true),
        ("DbA", 4, "E_NEWER_QUIET", false),
        ("DbB", 5, "Z_ZERO_DELTA", true),
        ("DbB", 6, "F_AFTER_END", false),
        ("DbB", 7, "G_TWO_PLANS", true),
        ("DbB", 8, "H_NO_PLAN_HASH", true),
    ];

    private async Task SeedAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();

        async Task Execute(string sql, params object?[] values)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            foreach (var v in values)
                cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
            await cmd.ExecuteNonQueryAsync();
        }

        /* The plan cache row: ids are not in time order and the winners are not written last, so neither the insert order nor the id alone picks the row. */
        async Task Cache(string database, long collectionId, DateTime at, string hash, string? planHash, bool firing, long execs, long deltaExecs)
        {
            await Execute(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     creation_time, execution_count, min_worker_time, max_worker_time, min_grant_kb, max_grant_kb,
     min_spills, max_spills, query_text, delta_execution_count)
VALUES ($1, $2, $3, 'CoFiredSrv', $4, $5, $6, $7, $8, 20000, $9, 1024, 1048576, 0, 50, 'cache text', $10)",
                collectionId, at, ServerId, database, hash, planHash, WindowStart.AddDays(-3), execs, firing ? 20_000_000L : 20_000L, deltaExecs);
        }

        /* The Query Store side: a cheap plan a few days back and a costly newest plan, 500 runs each, so every query is a regression the floors keep. */
        async Task Store(string database, long query, string hash, double costlyCpu)
        {
            foreach (var (plan, planHash, cpu, last) in new[]
            {
                (query * 10 + 1, "0xCHEAP" + query, 20_000.0, WindowEnd.AddDays(-5)),
                (query * 10 + 2, "0xCOSTLY" + query, costlyCpu, WindowEnd.AddHours(-1)),
            })
            {
                await Execute(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc,
     first_execution_time, last_execution_time, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     query_plan_hash, is_forced_plan, force_failure_count, runtime_stats_interval_id, interval_start_time_utc, avg_rowcount)
VALUES ($1, $2, $3, 'CoFiredSrv', $4, $5, $6, 'Regular', $7, $8, $9, 500, $10, $11, $12, false, 0, $13, $7, 10)",
                    _nextStoreId++, last.AddMinutes(1), ServerId, database, query, plan, last.AddHours(-1), last, hash, cpu, cpu + 20_000,
                    planHash, plan);
            }
        }

        long id = 100;
        await Cache("DbA", id + 21, Newest, "A_TIE_WIN", "PH1", true, 5021, 500);
        await Cache("DbA", id + 20, Newest, "A_TIE_WIN", "PH1", false, 5020, 500);
        await Cache("DbA", id + 10, Newest, "B_TIE_LOSE", "PH2", true, 5010, 500);
        await Cache("DbA", id + 11, Newest, "B_TIE_LOSE", "PH2", false, 5011, 500);
        await Cache("DbA", id + 31, Newest.AddMinutes(-20), "D_OLDER_QUIET", "PH3", false, 5031, 500);
        await Cache("DbA", id + 30, Newest, "D_OLDER_QUIET", "PH3", true, 5030, 500);
        await Cache("DbA", id + 40, Newest.AddMinutes(-20), "E_NEWER_QUIET", "PH4", true, 5040, 500);
        await Cache("DbA", id + 41, Newest, "E_NEWER_QUIET", "PH4", false, 5041, 500);
        await Cache("DbB", id + 50, Newest.AddMinutes(-20), "Z_ZERO_DELTA", "PH5", true, 5050, 500);
        await Cache("DbB", id + 51, Newest, "Z_ZERO_DELTA", "PH5", false, 5051, 0);
        await Cache("DbB", id + 70, Newest.AddMinutes(-20), "F_AFTER_END", "PH7", false, 5070, 500);
        await Cache("DbB", id + 71, WindowEnd.AddHours(1), "F_AFTER_END", "PH7", true, 5071, 500);
        /* Two plans of one query: one quiet, one firing. */
        await Cache("DbB", id + 81, Newest, "G_TWO_PLANS", "PH8A", false, 5081, 500);
        await Cache("DbB", id + 82, Newest, "G_TWO_PLANS", "PH8B", true, 5082, 500);
        /* No plan hash: its own group, a quiet older row and a firing newest row. */
        await Cache("DbB", id + 90, Newest.AddMinutes(-20), "H_NO_PLAN_HASH", null, false, 5090, 500);
        await Cache("DbB", id + 91, Newest, "H_NO_PLAN_HASH", null, true, 5091, 500);

        foreach (var (database, query, hash, _) in Cases)
            await Store(database, query, hash, 200_000 + query * 10_000);
    }

    /// <summary>The ROW_NUMBER() form of the signature: the (database, query_hash) pairs it reports, with the earliest creation_time.</summary>
    private async Task<Dictionary<(string, string), DateTime>> ReferenceSignatureAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SELECT database_name, query_hash, MIN(creation_time) AS creation_time
FROM
(
    SELECT
        database_name, query_hash, query_plan_hash, execution_count, creation_time,
        creation_time AS creation_time_utc, min_worker_time, max_worker_time,
        ROW_NUMBER() OVER (PARTITION BY database_name, query_hash, query_plan_hash ORDER BY collection_time DESC, collection_id DESC) AS rn
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   delta_execution_count > 0
) AS latest_cache
WHERE rn = 1
" + ParameterSensitivitySignature.PredicateSql("creation_time_utc <= $4") + @"
GROUP BY database_name, query_hash";
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = WindowStart });
        cmd.Parameters.Add(new DuckDBParameter { Value = WindowEnd });
        cmd.Parameters.Add(new DuckDBParameter { Value = WindowStart });
        var found = new Dictionary<(string, string), DateTime>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            found[(reader.GetString(0), reader.GetString(1))] = Convert.ToDateTime(reader.GetValue(2));
        return found;
    }

    [Fact]
    public async Task TheRegressedQueriesDrillDown_FlagsTheQueriesTheRowNumberFormFlags_OnAStoreWithTies()
    {
        await SeedAsync();
        var context = Context();
        var plan = await _duckDb.PlanArchiveReadAsync(
            System.Threading.CancellationToken.None,
            new ArchiveWindow("query_stats", "collection_time", context.TimeRangeStart),
            new ArchiveWindow("query_store_stats", "last_execution_time", context.TimeRangeStart.AddDays(-14)));

        var rows = new List<JsonElement>();
        foreach (var database in new[] { "DbA", "DbB" })
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();
            rows.AddRange((await DrillDownCollector.ReadRegressedQueriesAsync(connection, context, plan, new[] { database }))
                .Select(r => JsonSerializer.SerializeToElement(r.Item)));
        }

        var flags = rows.ToDictionary(r => r.GetProperty("query_id").GetInt64(), r => r.GetProperty("parameter_sensitivity_cofired").GetBoolean());

        /* Every case is a regression, and the flag is the one the newest rows decide. */
        Assert.Equal(Cases.Select(c => c.QueryId).Order(), flags.Keys.Order());
        foreach (var c in Cases)
            Assert.True(c.CoFired == flags[c.QueryId], $"{c.Hash}: expected the flag {c.CoFired}");

        /* The same flags the ROW_NUMBER() form of the read gives. */
        var reference = await ReferenceSignatureAsync();
        foreach (var c in Cases)
            Assert.Equal(reference.ContainsKey((c.Database, c.Hash)), flags[c.QueryId]);
    }
}
