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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Darling rung that adds <c>collect.slow_reads</c> (#5097), the record of reads that ran long or ended in a
/// timeout, error or limit. Internal self-telemetry: one plain table and one index, every statement guarded by
/// IF NOT EXISTS, nothing that alters or removes existing objects. Every fact finds the rung by NAME. The live facts run
/// when <c>DARLING_TEST_PG</c> is set, each on its own scratch store.
/// </summary>
[Collection("live-postgres")]
public sealed class SlowReadsRungTests
{
    public const string RungName = "slow-reads";

    private const string Table = "slow_reads";

    public static int RungVersion => Rung.Version;

    /// <summary>This rung's sentinel; the ordinal is a fact of the probe's shape, and a newer rung's sentinel follows it.</summary>
    private const int ProbeOrdinal = 137;

    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the slow-read table's live pins (each mints its own scratch database).";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    private static string Statements() => Rung.Sql.Replace("\r\n", "\n", StringComparison.Ordinal);

    [Fact]
    public void TheRungIsRegisteredInADenseLadder_AtVersion162()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(162, Rung.Version);
        /* No longer the top rung: V163 (the store's statement history) landed above it. */
        Assert.True(Rung.Version < StorageVersion.SchemaVersion);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Contains(Rung.Version - 1, versions);
    }

    [Fact]
    public void TheSql_CreatesTheTableAndTheTimeIndex_AndOnlyCreatesWithGuards()
    {
        var sql = Statements();

        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.slow_reads\n", sql, StringComparison.Ordinal);
        Assert.Contains("    error_class text,\n    row_count bigint\n);", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_slow_reads_time\n    ON collect.slow_reads(read_time, slow_read_id);", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("create_hypertable", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CHECK", sql, StringComparison.Ordinal);

        foreach (var create in Regex.Matches(sql, @"CREATE\s+(?:UNIQUE\s+)?(TABLE|INDEX)\s+(?!IF NOT EXISTS)", RegexOptions.IgnoreCase).Cast<Match>())
        {
            Assert.Fail("an unguarded CREATE: " + create.Value);
        }

        foreach (var verb in new[] { "ALTER ", "UPDATE ", "DELETE ", "GRANT ", "DROP ", "INSERT " })
        {
            Assert.DoesNotContain(verb, sql, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void TheProbeCarriesTheTable_AndMapsAStoreThroughThisRungToThisRung()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var arm = $"to_regclass('collect.{Table}') IS NOT NULL";
        Assert.Contains(arm, probe, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.True(ProbeOrdinal < arity - 1, "a newer rung's sentinel follows this one");
        Assert.Equal("hasSlowReads", method.GetParameters()[ProbeOrdinal].Name);

        var all = Enumerable.Range(0, arity).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(Rung.Version, (int)method.Invoke(null, all)!);
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var behind = (object[])all.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(Rung.Version - 1, (int)method.Invoke(null, behind)!);

        var thisArm = viewer.IndexOf("if (hasSlowReads)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasQueryStoreTopDaily)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "no sentinel arm: a fully-migrated store would map one rung short");
        Assert.True(thisArm < previousArm, "this arm sits above the previous rung's, so a current store maps one rung short");
        Assert.Contains($"return {Rung.Version};", viewer[thisArm..previousArm], StringComparison.Ordinal);

        /* The V71 finding: the table is named in the probe line and nowhere in the arm's prose. */
        var armProseStart = viewer.LastIndexOf("/* V162 (#5097)", thisArm, StringComparison.Ordinal);
        Assert.True(armProseStart >= 0, "the arm has no comment block saying why it exists");
        Assert.DoesNotContain(Table, viewer[armProseStart..thisArm], StringComparison.Ordinal);
    }

    [Fact]
    public async Task MigratingAFreshStore_CreatesThePlainTableAndTheIndex_AndRerunningTheRungChangesNothing()
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
            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct)));
            Assert.Equal(1L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'collect' AND c.relname = 'slow_reads' AND c.relkind = 'r'", ct));
            Assert.Equal(1L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_indexes WHERE schemaname = 'collect' AND tablename = 'slow_reads' AND indexname = 'idx_slow_reads_time'", ct));

            await ExecAsync(connection,
                "INSERT INTO collect.slow_reads (read_time, surface, route, outcome, total_ms, arguments, arguments_truncated, statements, statement_count, statements_truncated) VALUES (TIMESTAMP '2026-01-01 00:00:00', 'mcp', 'r', 'ok', 5000, '{}'::jsonb, FALSE, '[]'::jsonb, 0, FALSE)", ct);
            var shape = "SELECT string_agg(column_name || ':' || data_type, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'slow_reads'";
            var before = await ScalarAsync(connection, shape, ct);

            /* A store stamped below the rung migrates again: the stamp goes, the SQL runs a second time. */
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 162", ct);
            await PgMigrations.MigrateAsync(connection, ct);

            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct)));
            Assert.Equal(before, await ScalarAsync(connection, shape, ct));
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM collect.slow_reads", ct));
            Assert.Equal(2L, await ScalarAsync(connection, "SELECT count(*) FROM pg_indexes WHERE schemaname = 'collect' AND tablename = 'slow_reads'", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
