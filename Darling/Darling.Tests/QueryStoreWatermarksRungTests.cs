/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// Pins the rung that adds the Query Store watermark side tables (V178, #5684): its place in the ladder below V179 (#5640) and V180 (#5654), the shape of its
/// script (guarded creates, the one coverage insert, nothing moved or dropped), the schema-version probe's sentinel and its mapping arm.
/// V176 (#5638) and V177 (#5641) are on dev below this rung, so V176-V180 are contiguous and the dense-ladder pins hold. The probe
/// ordinal below counts the probe's columns; it moves up by one for each rung that lands below this one.
/// </summary>
public sealed class QueryStoreWatermarksRungTests
{
    public const string RungName = "query-store-watermarks";

    private const int ProbeOrdinal = 153;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    private static string Statements() =>
        Regex.Replace(Rung.Sql, @"/\*.*?\*/", " ", RegexOptions.Singleline).Replace("\r\n", "\n", StringComparison.Ordinal);

    [Fact]
    public void TheRungIsRegisteredOnce_BelowTheTopOfTheLadder_AtTheVersionTheTablesNameThemselves()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(178, QueryStoreWatermarks.RungVersion);
        Assert.Equal(QueryStoreWatermarks.RungVersion, Rung.Version);
        Assert.Single(PgMigrations.Scripts, m => m.Name == RungName);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Equal(StorageVersion.SchemaVersion, versions[^1]);
        /* V179 (#5640) and every later rung sit above this one with no gap, whatever the top is when a rung lands (#5825). */
        Assert.Equal(
            Enumerable.Range(QueryStoreWatermarks.RungVersion, StorageVersion.SchemaVersion - QueryStoreWatermarks.RungVersion + 1).ToArray(),
            versions.Where(v => v >= QueryStoreWatermarks.RungVersion).ToArray());
        Assert.Equal(QueryStoreWatermarks.CreateSql, Rung.Sql);
    }

    [Fact]
    public void TheScript_IsGuardedCreates_AndOneCoverageInsertThatKeepsTheFirstValue_WithNothingMovedOrDropped()
    {
        var sql = Statements();

        Assert.Equal(2, Regex.Matches(sql, @"CREATE\s+TABLE\b", RegexOptions.IgnoreCase).Count);
        Assert.Equal(2, Regex.Matches(sql, @"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\b", RegexOptions.IgnoreCase).Count);
        Assert.Single(Regex.Matches(sql, @"CREATE\s+INDEX\b", RegexOptions.IgnoreCase));
        Assert.Contains("CREATE INDEX IF NOT EXISTS ix_query_store_watermarks_time", sql, StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT pk_query_store_watermarks PRIMARY KEY (server_id, database_name, collection_time)", sql, StringComparison.Ordinal);

        /* Types mirror query_store_stats: naive timestamps and bigint ids. */
        Assert.Contains("collection_time timestamp NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("batch_max timestamp,", sql, StringComparison.Ordinal);
        Assert.Contains("max_query_id bigint,", sql, StringComparison.Ordinal);
        Assert.Contains("max_plan_id bigint,", sql, StringComparison.Ordinal);
        Assert.Contains("database_name text NOT NULL", sql, StringComparison.Ordinal);

        /* The coverage start is inserted once and a re-run keeps the first value, in the store's own UTC clock. */
        Assert.Single(Regex.Matches(sql, @"\bINSERT\b"));
        Assert.Contains("ON CONFLICT (singleton) DO NOTHING", sql, StringComparison.Ordinal);
        Assert.Contains("now() AT TIME ZONE 'UTC'", sql, StringComparison.Ordinal);

        foreach (var verb in new[] { "UPDATE", "DELETE", "GRANT", "REVOKE", "TRUNCATE", "DROP", "ALTER" })
        {
            Assert.DoesNotMatch(@"(?m)^\s*" + verb + @"\b", sql);
        }
    }

    [Fact]
    public async Task OnAFreshStore_TheProbeMapsToTheNewestRung_AndWithoutTheTableToTheOneBelow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the watermark rung's live probe pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct)));
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));

            /* Every sentinel above this rung's is removed; this rung's table is the newest sentinel left. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, QueryStoreWatermarks.RungVersion, ct);
            Assert.Equal(QueryStoreWatermarks.RungVersion, await ProbedVersionAsync(connection, ct));

            await ExecAsync(connection, "DROP TABLE collect.query_store_watermarks, collect.query_store_watermarks_coverage", ct);
            Assert.Equal(177, await ProbedVersionAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<int> ProbedVersionAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));

        var sentinels = new object[arity];
        for (var i = 0; i < arity; i++)
        {
            sentinels[i] = reader.GetBoolean(i);
        }

        return (int)method.Invoke(null, sentinels)!;
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
