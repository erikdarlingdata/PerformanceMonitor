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
/// Pins the text lookup in the tail of the Query Store top read (#8570), in the MCP reader's raw read and in the Viewer's
/// twin: the lookup runs once for each candidate the page's LIMIT reaches, and the page leaves the WAITFOR statements out.
/// The lookups are counted from the statement's own plan under EXPLAIN ANALYZE, as the rows its scans of
/// <c>query_store_text</c> return. Every statement seeded has one row in that table, so one row returned is one lookup. The
/// seed is at a fixed past date, the window is passed explicitly, and each live fact mints its own scratch database.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every live fact here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and then works entirely inside it, so it cannot race live
   collection. */
public sealed class QueryStoreTopTextLookupLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the Query Store top read's text lookup pins (each mints its own scratch database).";
    private const int ServerId = 1;
    private const int Top = 20;
    private const int WaitforStatements = 5;
    private const int Statements = 60;
    private const string WaitforText = "WAITFOR DELAY '00:00:05'";

    private static readonly DateTime Start = new(2026, 1, 14, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime End = new(2026, 1, 14, 6, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime IntervalStart = new(2026, 1, 14, 3, 0, 0, DateTimeKind.Unspecified);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task RunLiveAsync(Func<NpgsqlConnection, string, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, ct);
            await body(connection, scratch.ConnectionString, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>One raw row each for statements 1 through <see cref="Statements"/> in one database, and each statement's
    /// stored text. A lower query id runs longer and ranks first, so the <see cref="WaitforStatements"/> WAITFOR statements
    /// (1 through 5) rank ahead of every ordinary one.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, $"INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES ({ServerId}, 'srv{ServerId}', TRUE)", ct);
        await using (var rows = new NpgsqlCommand($@"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc,
     first_execution_time, last_execution_time, query_hash, execution_count, avg_duration_us, avg_cpu_time_us,
     is_forced_plan, runtime_stats_interval_id, interval_start_time_utc, query_text)
SELECT 1, @started + interval '5 minutes', {ServerId}, 'srv{ServerId}', 'db0', q, q, 'Regular',
       @started, @started + interval '5 minutes', 'h' || q, 10, 100000 - q, 5,
       FALSE, 1000, @started, NULL
FROM generate_series(1, {Statements}) AS q", connection))
        {
            rows.Parameters.Add(new NpgsqlParameter("started", NpgsqlDbType.Timestamp) { Value = IntervalStart });
            await rows.ExecuteNonQueryAsync(ct);
        }

        await using var text = new NpgsqlCommand($@"
INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen)
SELECT {ServerId}, 'db0', q, CASE WHEN q <= {WaitforStatements} THEN @waitfor ELSE 'SELECT ' || q || ' FROM dbo.t' END,
       TIMESTAMP '2026-01-14 03:00:00'
FROM generate_series(1, {Statements}) AS q", connection);
        text.Parameters.Add(new NpgsqlParameter("waitfor", NpgsqlDbType.Text) { Value = WaitforText });
        await text.ExecuteNonQueryAsync(ct);
    }

    private static void AddWindow(NpgsqlCommand command)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Start });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = End });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Top });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = DBNull.Value });
    }

    /// <summary>The text lookups of the MCP reader's raw read: $1 server, $2/$3 window, $4 top, $5 databases (all), $6 execution
    /// type and $7 module (no filter), $8 the candidate limit.</summary>
    private static async Task<long> McpTextLookupsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + DarlingDataReader.QueryStoreTopSql, connection);
        AddWindow(explain);
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = TopFill.FinalCandidates(Top) });
        return ExplainBuffers.RowsRead(await explain.ExecuteScalarAsync(ct), new HashSet<string>(StringComparer.Ordinal) { "query_store_text" });
    }

    /// <summary>The text lookups of the Viewer's raw read: $1 server, $2/$3 window, $4 top, $5 databases (all), $6 the
    /// candidate limit.</summary>
    private static async Task<long> ViewerTextLookupsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + ViewerDataService.QueryStoreTopSql, connection);
        AddWindow(explain);
        explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = TopFill.FinalCandidates(Top) });
        return ExplainBuffers.RowsRead(await explain.ExecuteScalarAsync(ct), new HashSet<string>(StringComparer.Ordinal) { "query_store_text" });
    }

    /// <summary>The page is the <see cref="Top"/> ordinary statements that rank first, in rank order, each with its stored text.</summary>
    private static void AssertPage(IReadOnlyList<(long QueryId, string Text)> page, string what)
    {
        var expected = Enumerable.Range(WaitforStatements + 1, Top)
            .Select(q => ((long)q, "SELECT " + q + " FROM dbo.t"))
            .ToList();
        Assert.True(expected.SequenceEqual(page), $"{what}: the page is not the {Top} ordinary statements that rank first, with their text");
    }

    [Fact]
    public async Task TheMcpRawRead_LooksTextUpOncePerCandidateTheLimitReaches()
    {
        await RunLiveAsync(async (connection, connectionString, ct) =>
        {
            await using var source = NpgsqlDataSource.Create(connectionString);
            var read = await DarlingDataReader.GetQueryStoreTopWithReachAsync(source, ServerId, Start, End, Top, DatabaseFilter.All, null, null, ct);
            Assert.Null(read.Table);
            AssertPage(read.Rows.Select(r => (r.QueryId, r.QueryText)).ToList(), "the MCP raw read");

            Assert.Equal(WaitforStatements + Top, await McpTextLookupsAsync(connection, ct));
        });
    }

    [Fact]
    public async Task TheViewerRawRead_LooksTextUpOncePerCandidateTheLimitReaches()
    {
        await RunLiveAsync(async (connection, connectionString, ct) =>
        {
            await using var viewer = new ViewerDataService(connectionString);
            var read = await viewer.GetQueryStoreTopQueriesWithReachAsync(ServerId, Start, End, Top, cancellationToken: ct);
            Assert.Null(read.Plan);
            AssertPage(read.Rows.Select(r => (r.QueryId, r.QueryText)).ToList(), "the Viewer raw read");

            Assert.Equal(WaitforStatements + Top, await ViewerTextLookupsAsync(connection, ct));
        });
    }
}
