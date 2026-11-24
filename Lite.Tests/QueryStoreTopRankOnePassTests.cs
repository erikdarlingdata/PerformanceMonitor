/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9195: the Query Store top rank read takes each interval's newest snapshot in ONE pass (a max of a struct per interval key).
/// It used to take the newest row's id by an arg_max, then scan the same rows again and join back to it. These tests run the
/// old head and the shipped head against the same seeded store, under the same ranking tail, and require the same rows.
/// </summary>
public sealed class QueryStoreTopRankOnePassTests : IDisposable
{
    private static readonly DateTime Now = DateTime.SpecifyKind(new DateTime(2026, 6, 10, 12, 0, 0), DateTimeKind.Unspecified);
    private static readonly DateTime WindowStart = Now.AddHours(-48);

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly List<(long Id, string Sql, bool Archive)> _rows = new();
    private long _nextId = 1;

    public QueryStoreTopRankOnePassTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(Path.Combine(_tempDir, "archive"));
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _duckDb.InitializeFromTemplateAsync().GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _duckDb.Dispose();
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

    /* The statement before #9195, kept as the reference: the newest id per interval by arg_max, then a second wide scan joined to it. */
    private const string OldHead = @"
WITH latest AS (
    SELECT
        arg_max(collection_id, (collection_time, COALESCE(execution_count, -1))) AS latest_id,
        MAX(collection_time) AS latest_time
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   COALESCE(interval_start_time_utc, collection_time) >= $2
    AND   COALESCE(interval_start_time_utc, collection_time) < $3
    AND   collection_time >= $2 - INTERVAL '1 day'
    AND   collection_time <= $3 + INTERVAL '30 days'{EXEC}
    GROUP BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
),
deduped AS (
    SELECT
        database_name, query_id, plan_id, query_hash, replica_role, module_name, execution_type_desc,
        runtime_stats_interval_id, first_execution_time, last_execution_time, collection_time,
        execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes,
        avg_physical_io_reads, avg_rowcount, min_dop, max_dop, query_plan_hash, is_forced_plan,
        plan_forcing_type, avg_clr_time_us, avg_tempdb_space_used, avg_log_bytes_used, plan_type,
        force_failure_count, last_force_failure_reason, compatibility_level, min_duration_us,
        max_duration_us, min_cpu_time_us, max_cpu_time_us, min_logical_io_reads, max_logical_io_reads,
        min_logical_io_writes, max_logical_io_writes, min_physical_io_reads, max_physical_io_reads,
        min_clr_time_us, max_clr_time_us, min_rowcount, max_rowcount, min_log_bytes_used,
        max_log_bytes_used, min_tempdb_space_used, max_tempdb_space_used, avg_query_max_used_memory,
        min_query_max_used_memory, max_query_max_used_memory, avg_num_physical_io_reads,
        min_num_physical_io_reads, max_num_physical_io_reads
    FROM v_query_store_stats
    INNER JOIN latest ON latest.latest_id = collection_id AND latest.latest_time = collection_time
    WHERE server_id = $1
    AND   COALESCE(interval_start_time_utc, collection_time) >= $2
    AND   COALESCE(interval_start_time_utc, collection_time) < $3
    AND   collection_time >= $2 - INTERVAL '1 day'
    AND   collection_time <= $3 + INTERVAL '30 days'{EXEC}
),
";

    private static readonly string[] MinMaxColumns =
    {
        "duration_us", "cpu_time_us", "logical_io_reads", "logical_io_writes", "physical_io_reads", "clr_time_us", "rowcount",
        "log_bytes_used", "tempdb_space_used", "query_max_used_memory", "num_physical_io_reads",
    };

    private static string Ts(DateTime t) => $"TIMESTAMP '{t:yyyy-MM-dd HH:mm:ss}'";

    private static string Lit(string? s) => s is null ? "NULL" : "'" + s.Replace("'", "''") + "'";

    private static string Num(long? v) => v is null ? "NULL" : v.Value.ToString(CultureInfo.InvariantCulture);

    /// <summary>One snapshot row. Every numeric is a whole number, so the sums do not depend on the order a plan adds them in.</summary>
    private void Add(bool archive, int server, long id, DateTime collected, long query, long plan, long? interval, DateTime? intervalStart,
        long? execs, long pay, string database = "Db1", string execType = "Regular", string? role = null, string? module = null, bool nulls = false)
    {
        var first = intervalStart ?? collected.AddHours(-1);
        string N(long v) => nulls && v % 3 == 0 ? "NULL" : Num(v);
        var minMax = string.Join(", ", MinMaxColumns.SelectMany(c => new[] { N(pay + c.Length), N(pay * 3 + c.Length) }));
        var minMaxNames = string.Join(", ", MinMaxColumns.SelectMany(c => new[] { "min_" + c, "max_" + c }));
        _rows.Add((id, $@"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
    execution_type_desc, first_execution_time, last_execution_time, query_hash, module_name, execution_count, avg_duration_us, avg_cpu_time_us,
    avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, avg_rowcount, min_dop, max_dop, query_plan_hash, is_forced_plan,
    plan_forcing_type, avg_clr_time_us, avg_tempdb_space_used, avg_log_bytes_used, plan_type, force_failure_count, last_force_failure_reason,
    compatibility_level, avg_query_max_used_memory, avg_num_physical_io_reads, runtime_stats_interval_id, interval_start_time_utc, replica_role, {minMaxNames})
VALUES ({id}, {Ts(collected)}, {server}, 'S{server}', {Lit(database)}, {query}, {plan}, {Lit(execType)}, {Ts(first)}, {Ts(collected)},
    {(nulls ? "NULL" : Lit("0xQ" + query))}, {Lit(module)}, {Num(execs)}, {N(pay * 1000)}, {N(pay * 700)}, {N(pay * 5)}, {N(pay * 2)}, {N(pay)}, {N(pay % 11)},
    {N(1 + pay % 3)}, {N(4 + pay % 3)}, {Lit("0xP" + plan)}, {(pay % 2 == 0 ? "true" : "false")}, {(nulls ? "NULL" : Lit("PLAN"))}, {N(pay * 9)},
    {N(pay * 4)}, {N(pay * 6)}, {(nulls ? "NULL" : Lit("Compiled"))}, {N(pay % 4)}, {(nulls ? "NULL" : Lit("none"))}, {N(150 + pay % 4)}, {N(pay * 8)}, {N(pay * 3)},
    {Num(interval)}, {(intervalStart is null ? "NULL" : Ts(intervalStart.Value))}, {Lit(role)}, {minMax})", archive));
    }

    private long Id(long query) => query % 2 == 0 ? 1_000_000 - _nextId++ : _nextId++;

    /// <summary>The standard shape for one server: repeated snapshots of each interval with a growing count, a counter that restarts,
    /// NULL counts, NULL payload, a legacy row with no interval, outcomes, replica roles, modules, and rows on the window's edges.
    /// <paramref name="archived"/> says which snapshots sit in the archive.</summary>
    private void SeedServer(int server, Func<DateTime, bool> archived)
    {
        for (long q = 1; q <= 7; q++)
        {
            foreach (var plan in q % 2 == 0 ? new[] { q, q + 10 } : new[] { q })
            {
                for (var i = 0; i < 6; i++)
                {
                    var start = WindowStart.AddHours(2 + i * 7);
                    long? interval = q == 6 ? null : 100 + i;
                    DateTime? intervalStart = q == 6 ? null : start;
                    for (var s = 0; s < 3; s++)
                    {
                        var collected = start.AddMinutes(10 + s * 20 + (q == 5 ? 1 : 0));
                        long? execs = 10 * (s + 1) + q + plan;
                        if (q == 2 && i == 1 && s == 1) execs = null;                    /* a NULL count mid-interval */
                        if (q == 3 && i == 2 && s == 2) execs = 5;                        /* the counter restarted: the newest row holds the lower count */
                        var pay = q * 13 + plan + i * 5 + s;
                        Add(archived(collected), server, Id(q), collected, q, plan, interval, intervalStart, execs, pay,
                            role: q == 5 ? (i % 2 == 0 ? "PRIMARY" : "SECONDARY") : null,
                            module: q == 1 ? "modA" : (q == 3 ? "modB" : null),
                            nulls: q == 7 && s == 2);
                        if (q == 4 && s == 2)
                            Add(archived(collected), server, Id(q), collected, q, plan, interval, intervalStart, 7, pay + 1, execType: "Aborted");
                    }
                }
            }
        }
        /* A NULL count and a count on one stamp: the count wins, the NULL sorts lowest. */
        var edge = WindowStart.AddHours(30);
        Add(archived(edge), server, Id(9), edge, 9, 9, 500, edge.AddHours(-1), null, 3);
        Add(archived(edge), server, Id(9), edge, 9, 9, 500, edge.AddHours(-1), 6, 4);
        /* Edges: an interval that started before the window (out), one collected after the window's end but started inside it (in), one at the end (out). */
        var before = WindowStart.AddHours(-2);
        Add(archived(before), server, Id(10), before, 10, 10, 600, before, 50, 8);
        var late = Now.AddMinutes(30);
        Add(archived(late), server, Id(11), late, 11, 11, 601, Now.AddMinutes(-5), 60, 9);
        Add(archived(late), server, Id(12), late, 12, 12, 602, Now, 70, 10);
    }

    private async Task LoadAsync()
    {
        Execute(_rows.Select(r => r.Sql).ToArray());
        var archived = _rows.Where(r => r.Archive).Select(r => r.Id).ToArray();
        if (archived.Length > 0)
        {
            /* The archive's order: the rows are in the hot table, copied to a Parquet file, then deleted from it. */
            var ids = string.Join(", ", archived);
            var path = Path.Combine(_tempDir, "archive", "20260601_000000_query_store_stats.parquet").Replace("\\", "/");
            Execute(
                $"COPY (SELECT * FROM query_store_stats WHERE collection_id IN ({ids})) TO '{DuckDbInitializer.EscapeSqlPath(path)}' (FORMAT PARQUET)",
                $"DELETE FROM query_store_stats WHERE collection_id IN ({ids})");
        }
        await _duckDb.CreateArchiveViewsAsync();
    }

    private void Execute(params string[] statements)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        connection.Open();
        foreach (var sql in statements)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }
    }

    private List<string> Run(bool oldHead, int server, string? executionType = null, string? module = null)
    {
        var execIndex = 4;
        var execClause = executionType is null ? "" : $" AND execution_type_desc = ${execIndex}";
        var moduleIndex = execIndex + (executionType is null ? 0 : 1);
        var moduleClause = module is null ? "" : $" AND module_name = ${moduleIndex}";
        var head = oldHead ? OldHead.Replace("{EXEC}", execClause) : LocalDataService.QueryStoreTopRankHeadSql("", execClause);
        var sql = head + LocalDataService.QueryStoreTopRankTailSql(moduleClause, 1000);

        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add(new DuckDBParameter { Value = server });
        command.Parameters.Add(new DuckDBParameter { Value = WindowStart });
        command.Parameters.Add(new DuckDBParameter { Value = Now });
        if (executionType is not null) command.Parameters.Add(new DuckDBParameter { Value = executionType });
        if (module is not null) command.Parameters.Add(new DuckDBParameter { Value = module });
        var result = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var cells = new string[reader.FieldCount];
            for (var c = 0; c < cells.Length; c++)
                cells[c] = reader.IsDBNull(c) ? "<null>" : Convert.ToString(reader.GetValue(c), CultureInfo.InvariantCulture) ?? "<null>";
            result.Add(string.Join("|", cells));
        }
        return result;
    }

    [Fact]
    public async Task TheOnePassRead_ReturnsTheRowsTheTwoPassReadReturned_OnHotSplitAndArchiveOnlyServers()
    {
        SeedServer(1, _ => false);                       /* hot only */
        SeedServer(2, t => t < WindowStart.AddHours(22)); /* split: the early snapshots are archived, the later ones hot, and intervals straddle the cut */
        SeedServer(3, _ => true);                         /* archive only */
        await LoadAsync();

        var shapes = new (string? ExecutionType, string? Module)[] { (null, null), ("Regular", null), ("Aborted", null), (null, "modA"), ("Regular", "modB") };
        foreach (var server in new[] { 1, 2, 3 })
        {
            foreach (var (executionType, module) in shapes)
            {
                var reference = Run(oldHead: true, server, executionType, module);
                var shipped = Run(oldHead: false, server, executionType, module);
                Assert.NotEmpty(reference);
                Assert.Equal(reference, shipped);
            }
        }

        /* The three servers hold one shape, so one must read as another: the comparison is not between empty or arbitrary sets. */
        var unfiltered = Run(oldHead: false, 1);
        Assert.True(unfiltered.Count >= 10, "the seed ranks at least ten groups");
        Assert.Equal(unfiltered, Run(oldHead: false, 2));
        Assert.Equal(unfiltered, Run(oldHead: false, 3));
        Assert.Contains(unfiltered, r => r.Split('|')[3] == "<null>");
    }

    [Fact]
    public async Task TheOnePassRead_PicksTheHigherCollectionId_WhenTwoRowsOfAnIntervalShareTheirTimeAndCount()
    {
        var t = WindowStart.AddHours(5);
        /* The lower id is inserted second in one key and first in the other, so the pick is not the insertion order. */
        Add(false, 5, 7, t, 1, 1, 100, t.AddHours(-1), 40, pay: 1);
        Add(false, 5, 9, t, 1, 1, 100, t.AddHours(-1), 40, pay: 9);
        Add(false, 5, 20, t, 2, 2, 100, t.AddHours(-1), 40, pay: 2);
        Add(false, 5, 15, t, 2, 2, 100, t.AddHours(-1), 40, pay: 7);
        await LoadAsync();

        var rows = Run(oldHead: false, 5);
        Assert.Equal(2, rows.Count);
        /* avg_duration_ms is the eighth cell: the winner row's pay (9 and 2), not the other row's. */
        var byQuery = rows.ToDictionary(r => r.Split('|')[1], r => r.Split('|')[7]);
        Assert.Equal("9", byQuery["1"]);
        Assert.Equal("2", byQuery["2"]);
    }

    [Fact]
    public async Task TheOnePassRead_CountsARowOnce_WhenTheHotTableAndTheArchiveBothHoldIt()
    {
        var t = WindowStart.AddHours(5);
        Add(false, 6, 1, t, 1, 1, 100, t.AddHours(-1), 10, pay: 4);
        Add(false, 6, 2, t.AddMinutes(20), 1, 1, 100, t.AddHours(-1), 30, pay: 4);
        await LoadAsync();
        /* The archival copied the newest row to a Parquet file and had not yet deleted it from the hot table. */
        var path = Path.Combine(_tempDir, "archive", "20260601_000001_query_store_stats.parquet").Replace("\\", "/");
        Execute($"COPY (SELECT * FROM query_store_stats WHERE collection_id = 2) TO '{DuckDbInitializer.EscapeSqlPath(path)}' (FORMAT PARQUET)");
        await _duckDb.CreateArchiveViewsAsync();

        var shipped = Run(oldHead: false, 6);
        var reference = Run(oldHead: true, 6);
        Assert.Single(shipped);
        Assert.Single(reference);
        /* total_executions is the seventh cell: the interval's newest count once, where the join returned the row twice. */
        Assert.Equal("30", shipped[0].Split('|')[6]);
        Assert.Equal("60", reference[0].Split('|')[6]);
    }
}
