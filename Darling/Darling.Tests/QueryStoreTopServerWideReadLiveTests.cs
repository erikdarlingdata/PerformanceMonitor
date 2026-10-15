/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5815: the server-wide 24 h Query Store top read (<see cref="DarlingDataReader.QueryStoreTopTableSql"/>, no database
/// filter) on a planted busy server, several databases, interleaved with eleven other servers the way a collection cycle writes
/// them. Pins three things: (1) one run at <see cref="TopFill.FinalCandidates"/> returns exactly the rows, in the order,
/// that the old fill rounds return, on a window whose WAITFOR shells make the rounds run more than once (the reader and the
/// viewer used to re-read the window from the wide table once per round); (2) the executed plan reads the wide table
/// through the <c>(server_id, first_execution_time)</c> background index and touches no other server's row; (3) the
/// page looks up text only until it is full, not for every candidate of the final limit.
/// </summary>
/* #1776 own-store: reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and then
   works entirely inside it, the same shape as QueryStoreWideUpperBoundLiveTests. */
public sealed class QueryStoreTopServerWideReadLiveTests
{
    private const int Busy = 1;
    private const int Servers = 12;
    private const int Databases = 4;
    private const int Plans = 60;
    private const int Top = 25;

    /* 48 hourly intervals, the window is the last 24 of them. */
    private static readonly DateTime FirstHour = new(2026, 10, 12, 12, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowStart = new(2026, 10, 13, 12, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowEnd = new(2026, 10, 14, 12, 0, 0, DateTimeKind.Unspecified);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheServerWideRead_RunsOnce_ReturnsTheRoundsAnswer_AndReadsOnlyTheServersRows()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5815 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await SeedAsync(connection, ct);

        /* (1) One run at the final limit equals the rounds, and the rounds really ran more than once here. */
        var sql = DarlingDataReader.QueryStoreTopTableSql;
        var rounds = 0;
        var roundRows = await TopFill.RunAsync(Top, async candidates =>
        {
            rounds++;
            return await RunPageAsync(connection, sql, candidates, ct);
        });
        Assert.True(rounds > 1, $"the WAITFOR shells should have made the fill run more than one round (ran {rounds})");
        var (onceRows, _) = await RunPageAsync(connection, sql, TopFill.FinalCandidates(Top), ct);
        Assert.Equal(Top, onceRows.Count);
        Assert.Equal(roundRows, onceRows);
        Assert.DoesNotContain(onceRows, r => r.Text is not null && r.Text.StartsWith("WAITFOR", StringComparison.Ordinal));

        /* The viewer's table read takes the same path: one run at the final limit equals its own rounds. */
        var viewerSql = ViewerDataService.QueryStoreTopTableSql;
        var viewerRounds = await TopFill.RunAsync(Top, candidates => RunViewerPageAsync(connection, viewerSql, candidates, ct));
        var (viewerOnce, _) = await RunViewerPageAsync(connection, viewerSql, TopFill.FinalCandidates(Top), ct);
        Assert.Equal(viewerRounds, viewerOnce);

        /* (2) and (3) from the executed plan's JSON nodes. */
        var serverIndex = await ScalarAsync<string>(connection, """
            SELECT c.relname
            FROM pg_index AS i
            JOIN pg_class AS c ON c.oid = i.indexrelid
            WHERE i.indrelid = 'collect.query_store_interval_wide_legacy'::regclass
            AND   pg_get_indexdef(i.indexrelid) LIKE '%(server_id, first_execution_time)'
            """, ct);
        var busyRowsInRange = await ScalarAsync<long>(connection, $"""
            SELECT COUNT(*)
            FROM collect.query_store_interval_wide
            WHERE server_id = {Busy}
            AND   first_execution_time >= timestamp '{WindowStart:yyyy-MM-dd HH:mm}' - {QueryStoreIntervalWide.PurgeEdgeMarginSql}
            AND   first_execution_time <= timestamp '{WindowEnd:yyyy-MM-dd HH:mm}' + {QueryStoreIntervalWide.FirstExecUpperSlackSql}
            """, ct);
        var busyRowsInWindow = await ScalarAsync<long>(connection, $"""
            SELECT COUNT(*)
            FROM collect.query_store_interval_wide
            WHERE server_id = {Busy}
            AND   collection_time >= timestamp '{WindowStart:yyyy-MM-dd HH:mm}'
            AND   collection_time <= timestamp '{WindowEnd:yyyy-MM-dd HH:mm}'
            """, ct);

        var plan = await ExplainAsync(connection, sql, TopFill.FinalCandidates(Top), ct);
        var nodes = Flatten(plan).ToList();
        var wideNodes = nodes.Where(n => Str(n, "Relation Name")?.StartsWith("query_store_interval_wide", StringComparison.Ordinal) == true).ToList();
        Assert.NotEmpty(wideNodes);
        Assert.Contains(nodes, n => Str(n, "Index Name") == serverIndex);
        Assert.DoesNotContain(wideNodes, n => Str(n, "Node Type") == "Seq Scan");
        var touched = wideNodes.Sum(n => (Num(n, "Actual Rows") + Num(n, "Rows Removed by Filter") + Num(n, "Rows Removed by Index Recheck")) * Math.Max(1, Num(n, "Actual Loops")));
        Assert.Equal(busyRowsInWindow, (long)Math.Round(wideNodes.Sum(n => Num(n, "Actual Rows") * Math.Max(1, Num(n, "Actual Loops")))));
        Assert.True(touched <= busyRowsInRange, $"the read touched {touched} wide rows; the busy server holds {busyRowsInRange} in the first_execution_time range, and no other server's row may be read");

        /* The page's sorted candidates feed the text lateral lazily: it pulls only the rows it needs to fill the page,
           far fewer than the final candidate limit. */
        var candidateSort = nodes.Single(n => Str(n, "Node Type") == "Sort"
            && n.TryGetProperty("Sort Key", out var keys) && keys.EnumerateArray().Any(k => k.GetString()!.Contains("k.database_name", StringComparison.Ordinal)));
        var pulled = Num(candidateSort, "Actual Rows");
        Assert.InRange(pulled, Top, TopFill.FinalCandidates(Top) - 1);
    }

    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        /* Every (server, database, plan) has one row per hourly interval, collected two minutes after it closes, written
           hour by hour with the servers interleaved (a collection cycle's order). One query in eight per database is a
           WAITFOR shell with a long duration, so the top of the ranking is full of them and the first round's
           top + 5 candidates leave the page short. */
        await using (var insert = new NpgsqlCommand($"""
            INSERT INTO collect.query_store_interval_wide
                (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
                 last_execution_time, query_hash, query_plan_hash, execution_count, avg_duration_us, avg_cpu_time_us,
                 avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, avg_rowcount, runtime_stats_interval_id)
            SELECT
                t.h + interval '62 minutes', s.s, 'db' || d.d, p.p, p.p, 'Regular', t.h + (p.p % 50) * interval '1 minute',
                t.h + interval '55 minutes', 'qh' || p.p, 'ph' || p.p, 10 + p.p % 7,
                CASE WHEN p.p % 8 = 0 THEN 50000000 + p.p * 1000 + d.d ELSE 1000 * p.p + d.d END,
                100 * p.p, p.p, 0, 0, 1, CAST(extract(epoch FROM t.h) AS bigint) / 3600
            FROM generate_series(timestamp '{FirstHour:yyyy-MM-dd HH:mm}', timestamp '{FirstHour.AddHours(47):yyyy-MM-dd HH:mm}', interval '1 hour') AS t(h)
            CROSS JOIN generate_series(1, {Servers}) AS s(s)
            CROSS JOIN generate_series(1, {Databases}) AS d(d)
            CROSS JOIN generate_series(1, {Plans}) AS p(p)
            ORDER BY t.h, p.p, s.s, d.d
            """, connection))
        {
            await insert.ExecuteNonQueryAsync(ct);
        }

        await using (var text = new NpgsqlCommand($"""
            INSERT INTO collect.query_store_text (server_id, database_name, query_id, last_seen, query_sql_text)
            SELECT s.s, 'db' || d.d, p.p, timestamp '{WindowEnd:yyyy-MM-dd HH:mm}',
                   CASE WHEN p.p % 8 = 0 THEN 'WAITFOR DELAY ''00:01:00''' ELSE 'SELECT ' || p.p END
            FROM generate_series(1, 2) AS s(s)
            CROSS JOIN generate_series(1, {Databases}) AS d(d)
            CROSS JOIN generate_series(1, {Plans}) AS p(p)
            """, connection))
        {
            await text.ExecuteNonQueryAsync(ct);
        }

        /* The background (server_id, first_execution_time) index the service builds after the rungs (#4952); a plain
           CREATE INDEX on the parent builds it on the partition. */
        foreach (var statement in new[]
        {
            "CREATE INDEX IF NOT EXISTS ix_query_store_interval_wide_server_first_exec ON collect.query_store_interval_wide (server_id, first_execution_time)",
            "VACUUM ANALYZE collect.query_store_interval_wide",
            "ANALYZE collect.query_store_text",
        })
        {
            await using var command = new NpgsqlCommand(statement, connection);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private sealed record Row(string Key, string? Text, long PageOrd);

    private static void AddMcpParameters(NpgsqlCommand command, int candidates)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = Busy });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = WindowStart, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = WindowEnd, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = Top });
        command.Parameters.Add(DatabaseFilter.All.Parameter());
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = candidates });
    }

    private static async Task<(List<Row> Rows, int CandidateCount)> RunPageAsync(NpgsqlConnection connection, string sql, int candidates, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        AddMcpParameters(command, candidates);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await TopFill.ReadPageAsync(reader, reader.GetOrdinal("page_ord"), r => new Row(
            string.Join('|', r.GetString(r.GetOrdinal("database_name")), r.GetInt64(r.GetOrdinal("query_id")), r.GetInt64(r.GetOrdinal("plan_id")),
                r.GetDouble(r.GetOrdinal("avg_duration_ms")), r.GetInt64(r.GetOrdinal("total_executions"))),
            r.IsDBNull(r.GetOrdinal("query_text")) ? null : r.GetString(r.GetOrdinal("query_text")),
            r.GetInt64(r.GetOrdinal("page_ord"))), ct);
    }

    private static async Task<(List<Row> Rows, int CandidateCount)> RunViewerPageAsync(NpgsqlConnection connection, string sql, int candidates, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = Busy });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = WindowStart, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = WindowEnd, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = Top });
        command.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = candidates });
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await TopFill.ReadPageAsync(reader, reader.GetOrdinal("page_ord"), r => new Row(
            string.Join('|', r.GetString(r.GetOrdinal("database_name")), r.GetInt64(r.GetOrdinal("query_id")), r.GetInt64(r.GetOrdinal("plan_id")),
                r.GetDouble(r.GetOrdinal("avg_duration_ms")), r.GetInt64(r.GetOrdinal("total_executions"))),
            r.IsDBNull(r.GetOrdinal("query_text")) ? null : r.GetString(r.GetOrdinal("query_text")),
            r.GetInt64(r.GetOrdinal("page_ord"))), ct);
    }

    private static async Task<JsonElement> ExplainAsync(NpgsqlConnection connection, string sql, int candidates, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, connection);
        AddMcpParameters(command, candidates);
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(json);
        return document.RootElement[0].GetProperty("Plan").Clone();
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }

    private static IEnumerable<JsonElement> Flatten(JsonElement node)
    {
        yield return node;
        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                foreach (var descendant in Flatten(child))
                {
                    yield return descendant;
                }
            }
        }
    }

    private static string? Str(JsonElement node, string name) =>
        node.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    private static double Num(JsonElement node, string name) =>
        node.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Number ? value.GetDouble() : 0;
}
