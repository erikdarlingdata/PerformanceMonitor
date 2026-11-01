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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #6160, V201: the four variant and plan-guide columns on <c>collect.query_store_forced_plans</c>. A row written through the product's own
/// binary COPY writer (positional, so column ORDER is the contract) reads back all four on a store built fresh and on a store rewound to V200 and
/// migrated up, and both stores end with the four columns LAST, in the same order and with the same types.
/// </summary>
public sealed class ForcedPlanVariantRungTests
{
    private static readonly string[] NewColumns = ["variant_plan_ids", "plan_guide_name", "plan_guide_enabled", "plan_guide_scope"];

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AFreshStore_AndOneUpgradedFromV200_StoreAndReadBackTheColumns_LastAndWithTheSameTypes()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the forced-plan rung test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var bodySucceeded = false;
        try
        {
            await PgMigrations.MigrateAsync(connection, ct);
            var fresh = await ColumnsAsync(connection, ct);
            Assert.Equal(NewColumns, fresh.TakeLast(4).Select(c => c.Name).ToArray());
            Assert.Equal(["text", "text", "boolean", "text"], fresh.TakeLast(4).Select(c => c.Type).ToArray());
            await WriteAndAssertAsync(connection, ct);

            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 200, ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 201", ct);
            await ExecAsync(connection, "DELETE FROM collect.query_store_forced_plans", ct);
            Assert.Empty((await ColumnsAsync(connection, ct)).Select(c => c.Name).Intersect(NewColumns));

            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(fresh, await ColumnsAsync(connection, ct));
            await WriteAndAssertAsync(connection, ct);

            /* A row stored before the rung reads NULL in all four, not a default. */
            await ExecAsync(connection,
                "INSERT INTO collect.query_store_forced_plans (collection_id, collection_time, server_id, server_name, database_name, plan_id, query_id) "
                + "VALUES (" + CollectionIdGenerator.Next() + ", timestamp '2026-10-19 10:00:00', -6160, 'rung-test', 'old', 1, 1)", ct);
            Assert.Equal("<null>|<null>|<null>|<null>", (string)(await ScalarAsync(connection,
                "SELECT COALESCE(variant_plan_ids, '<null>') || '|' || COALESCE(plan_guide_name, '<null>') || '|' || COALESCE(plan_guide_enabled::text, '<null>') || '|' "
                + "|| COALESCE(plan_guide_scope, '<null>') FROM collect.query_store_forced_plans WHERE database_name = 'old'", ct))!);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task WriteAndAssertAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var definition = QueryStoreForcedPlansCollector.Instance;
        var runTime = new DateTime(2026, 10, 19, 12, 0, 0, DateTimeKind.Unspecified);
        var row = new QueryStoreForcedPlansCollector.Row
        {
            DbName = "rungdb", PlanId = 6, QueryId = 6, QueryPlanHash = "0x01", PlanForcingType = "MANUAL", ForceFailureCount = 0,
            VariantPlanIds = "12,15", PlanGuideName = "pg_one", PlanGuideEnabled = true, PlanGuideScope = "OBJECT",
        };
        var context = new CollectorContext { ServerId = -6160, ServerName = "rung-test", CollectionTime = runTime, Deltas = null! };
        var writer = new PgCollectorRowWriter();
        await using (var importer = await connection.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct))
        {
            writer.Importer = importer;
            await importer.StartRowAsync(ct);
            writer.Value(CollectionIdGenerator.Next());
            writer.Value(runTime).Value(-6160).Value("rung-test");
            writer.BeginPayload();
            definition.WritePayload(row, writer, context);
            writer.EndPayload(definition.PayloadColumns.Count);
            await importer.CompleteAsync(ct);
        }

        Assert.Equal("MANUAL|12,15|pg_one|true|OBJECT", (string)(await ScalarAsync(connection,
            "SELECT plan_forcing_type || '|' || variant_plan_ids || '|' || plan_guide_name || '|' || plan_guide_enabled::text || '|' || plan_guide_scope "
            + "FROM collect.query_store_forced_plans WHERE database_name = 'rungdb'", ct))!);
    }

    private static async Task<List<(string Name, string Type)>> ColumnsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT attname, format_type(atttypid, atttypmod) FROM pg_catalog.pg_attribute WHERE attrelid = 'collect.query_store_forced_plans'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum", connection)
        { CommandTimeout = 60 };
        var columns = new List<(string, string)>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) columns.Add((reader.GetString(0), reader.GetString(1)));
        return columns;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return await command.ExecuteScalarAsync(ct);
    }
}
