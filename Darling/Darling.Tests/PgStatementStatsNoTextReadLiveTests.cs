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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch databases through ScratchPostgres and never touches the shared store. */

/// <summary>
/// #7783: the statement statistics collector reads <c>pg_stat_statements</c> without its query text. The extension's view is
/// <c>SELECT * FROM pg_stat_statements(true)</c>, and <c>showtext = true</c> loads the whole query-text file from disk on every
/// call, whether or not the caller selects <c>query</c>. The collector selects no text (the hourly statement-text fetch does), so
/// its read calls the function with <c>showtext = false</c>, as the Aurora flavor already does with <c>aurora_stat_statements(false)</c>.
/// <para>The two forms return the same rows on every column the collector projects, so the result set cannot tell them apart.
/// What can is the plan the server builds for the shipped text: a Function Scan whose call is <c>pg_stat_statements(false)</c>,
/// not the view's <c>pg_stat_statements(true)</c>. The second fact shows the rows are unaffected: every entry the collector
/// returns for the target database is an entry the view holds, under the same <c>(queryid, dbid, userid, toplevel)</c> key.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgStatementStatsNoTextReadLiveTests
{
    private const int ServerId = -778301;

    [Fact]
    public async Task TheStockRead_CallsTheFunctionWithShowtextOff_AndReturnsTheViewsEntries()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the statement collector showtext test.");

        var ct = TestContext.Current.CancellationToken;

        await using var target = await ScratchPostgres.CreateAsync(adminConnectionString!, ct, emptyStore: true);

        /* pg_stat_statements is cluster-wide: the entries of every database are read through this one. */
        await ExecAsync(target.ConnectionString, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements", ct);

        /* A statement of this database's own, so the collector has at least one entry to return for it. */
        for (var run = 0; run < 3; run++)
        {
            await ExecAsync(target.ConnectionString, "SELECT 778300 + 1 AS pm7783_no_text_read", ct);
        }

        await using var connection = new NpgsqlConnection(target.ConnectionString);
        await connection.OpenAsync(ct);

        var targetOid = await ScalarAsync<long>(connection, "SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname = current_database()", ct);

        var context = new CollectorContext
        {
            ServerId = ServerId,
            ServerName = "pm7783-target",
            CollectionTime = LiveClock.Now(),
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = connection.PostgreSqlVersion.Major },
        };
        Assert.True(await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(
            context, PgStatementStatsCollector.Instance, connection, null, ct));

        var shipped = PgStatementStatsCollector.Instance.BuildQuery(context).Text;

        /* The server's own account of what the shipped text runs. EXPLAIN plans without executing, so the epoch
           sub-selects cost nothing here, and VERBOSE prints each Function Scan's call expression. */
        var calls = await FunctionScanCallsAsync(connection, shipped, "pg_stat_statements", ct);
        var call = Assert.Single(calls);
        Assert.EndsWith("pg_stat_statements(false)", call, StringComparison.Ordinal);

        List<PgStatementStatsCollector.Row> rows;
        await using (var command = new NpgsqlCommand(shipped, connection))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            rows = await PgStatementStatsCollector.Instance.ReadAsync(reader, context, ct);
        }

        var ofTarget = rows.Where(r => r.DatabaseId == targetOid).ToList();
        Assert.NotEmpty(ofTarget);
        Assert.All(ofTarget, r =>
        {
            Assert.True(r.Calls > 0, $"queryid {r.QueryId}: calls {r.Calls}");
            Assert.Equal(target.DatabaseName, r.DatabaseName);
        });

        /* The view, read after the collector: entries are only ever added or evicted under pressure, so every key the
           collector returned for this database is still in the view. The same keys from a text-free read and the view. */
        var viewKeys = await ViewKeysAsync(connection, context.PgExtensionSchema("pg_stat_statements"), targetOid, ct);
        Assert.All(ofTarget, r => Assert.Contains((r.QueryId, r.DatabaseId, r.UserId, r.TopLevel), viewKeys));
    }

    private static async Task<List<string>> FunctionScanCallsAsync(NpgsqlConnection connection, string sql, string functionName, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (VERBOSE, COSTS OFF, FORMAT JSON) " + sql, connection);
        var planJson = (string)(await command.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(planJson);
        var calls = new List<string>();
        void Walk(JsonElement node)
        {
            if (node.TryGetProperty("Node Type", out var nodeType) && nodeType.GetString() == "Function Scan"
                && node.TryGetProperty("Function Name", out var name) && name.GetString() == functionName
                && node.TryGetProperty("Function Call", out var functionCall))
            {
                calls.Add(functionCall.GetString()!);
            }

            if (node.TryGetProperty("Plans", out var children))
            {
                foreach (var child in children.EnumerateArray())
                {
                    Walk(child);
                }
            }
        }

        Walk(document.RootElement[0].GetProperty("Plan"));
        return calls;
    }

    private static async Task<HashSet<(long QueryId, long DatabaseId, long UserId, bool TopLevel)>> ViewKeysAsync(
        NpgsqlConnection connection, string schema, long databaseId, CancellationToken ct)
    {
        /* toplevel arrived in pg_stat_statements 1.9; before that every entry is top level, which is what the collector reads too. */
        var hasToplevel = await ScalarAsync<bool>(connection,
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid = pg_catalog.to_regclass($1) AND attname = 'toplevel' AND NOT attisdropped)",
            ct, schema + ".pg_stat_statements");
        var topLevel = hasToplevel ? "s.toplevel" : "true";

        var keys = new HashSet<(long, long, long, bool)>();
        await using var command = new NpgsqlCommand(
            $"SELECT s.queryid::bigint, s.dbid::bigint, s.userid::bigint, {topLevel} FROM {schema}.pg_stat_statements AS s WHERE s.dbid = $1", connection);
        command.Parameters.AddWithValue(databaseId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            keys.Add((reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetBoolean(3)));
        }

        return keys;
    }

    private static async Task ExecAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var parameter in parameters)
        {
            command.Parameters.AddWithValue(parameter);
        }

        return (T)(await command.ExecuteScalarAsync(ct))!;
    }
}
