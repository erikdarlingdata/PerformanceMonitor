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
/// Pins the rung that adds the Query Store watermark side tables (V178, #5684): its place at the top of the ladder, the shape of its
/// script (guarded creates, the one coverage insert, nothing moved or dropped), the schema-version probe's sentinel and its mapping arm.
/// The ladder is deliberately NOT asserted dense here: V176 and V177 are separate open changes, and the dense-ladder pins elsewhere stay
/// red until they merge. The probe ordinal below counts the probe's columns; it moves up by one for each rung that lands below this one.
/// </summary>
public sealed class QueryStoreWatermarksRungTests
{
    public const string RungName = "query-store-watermarks";

    private const int ProbeOrdinal = 151;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    private static string Statements() =>
        Regex.Replace(Rung.Sql, @"/\*.*?\*/", " ", RegexOptions.Singleline).Replace("\r\n", "\n", StringComparison.Ordinal);

    [Fact]
    public void TheRungIsRegisteredOnce_AtTheTopOfTheLadder_AtTheVersionTheTablesNameThemselves()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(178, QueryStoreWatermarks.RungVersion);
        Assert.Equal(QueryStoreWatermarks.RungVersion, Rung.Version);
        Assert.Single(PgMigrations.Scripts, m => m.Name == RungName);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Equal(StorageVersion.SchemaVersion, versions[^1]);
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
    public void TheProbeNamesTheTable_AndMapsAFullyMigratedStoreToThisTopRung()
    {
        Assert.Contains("to_regclass('collect.query_store_watermarks')", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        Assert.Equal("hasQueryStoreWatermarks", parameters[ProbeOrdinal].Name);
        Assert.Equal("hasPlanRegressionDailyRowcount", parameters[ProbeOrdinal - 1].Name);
        Assert.Equal(ProbeOrdinal, parameters.Length - 1);

        /* Every sentinel present maps to exactly this build's version; this sentinel absent maps one rung back. */
        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);
        Assert.Equal(QueryStoreWatermarks.RungVersion, (int)method.Invoke(null, all)!);

        var behind = (object[])all.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(175, (int)method.Invoke(null, behind)!);

        /* In the source the arm sits ABOVE V175's and returns this rung's version. */
        var thisArm = viewer.IndexOf("if (hasQueryStoreWatermarks)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasPlanRegressionDailyRowcount)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0 && previousArm > thisArm, "the V178 arm sits above V175's");
        Assert.Contains("return " + QueryStoreWatermarks.RungVersion + ";", viewer[thisArm..previousArm], StringComparison.Ordinal);
    }

    [Fact]
    public async Task OnAFreshStore_TheProbeMapsToTheNewestRung_AndWithoutTheTableToTheOneBelow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the watermark rung's live probe pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct)));
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));

            await ExecAsync(connection, "DROP TABLE collect.query_store_watermarks, collect.query_store_watermarks_coverage", ct);
            Assert.Equal(175, await ProbedVersionAsync(connection, ct));

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
