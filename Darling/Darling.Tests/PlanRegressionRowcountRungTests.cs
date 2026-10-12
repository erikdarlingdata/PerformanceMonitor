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
/// Pins the rung that adds <c>rowcount_sum</c> to <c>collect.plan_regression_daily</c> (V175, #5631): the rung's place in the
/// ladder, the shape of its script (one guarded ADD COLUMN and one stale-marking UPDATE, run only when the column is added),
/// the schema-version probe's sentinel and its mapping arm, and the behaviour on a fresh store, on a store one rung behind
/// that already has built days, and on a re-run.
/// </summary>
public sealed class PlanRegressionRowcountRungTests
{
    public const string RungName = "plan-regression-daily-rowcount";

    private const int RungVersion = 175;

    private const int ProbeOrdinal = 150;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    private static string Statements() =>
        Regex.Replace(Rung.Sql, @"/\*.*?\*/", " ", RegexOptions.Singleline).Replace("\r\n", "\n", StringComparison.Ordinal);

    [Fact]
    public void TheRungIsRegisteredOnce_InADenseLadder_AboveTheServiceVersionRung()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(RungVersion, Rung.Version);
        Assert.Equal(1, PgMigrations.Scripts.Count(m => m.Name == RungName));
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Contains(RungVersion - 1, versions);
        Assert.True(RungVersion <= StorageVersion.SchemaVersion);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);
    }

    [Fact]
    public void TheScript_AddsTheColumnOnce_AndMarksBuiltDaysStaleOnlyWhenItDoes()
    {
        var sql = Statements();

        /* The column is a nullable numeric with no default: catalog-only, and NULL means "not known". */
        Assert.Contains("ALTER TABLE collect.plan_regression_daily ADD COLUMN rowcount_sum numeric;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("DEFAULT", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT NULL", sql, StringComparison.Ordinal);

        /* Every built day is bumped, so reads take it from the interval table until the hourly builder rebuilds it. */
        Assert.Contains("UPDATE collect.plan_regression_daily_built SET late_seq = late_seq + 1;", sql, StringComparison.Ordinal);

        /* Both statements sit inside the guard, so a second run changes nothing. */
        var guard = sql.IndexOf("IF NOT EXISTS", StringComparison.Ordinal);
        var alter = sql.IndexOf("ALTER TABLE", StringComparison.Ordinal);
        var update = sql.IndexOf("UPDATE collect.", StringComparison.Ordinal);
        var end = sql.IndexOf("END IF;", StringComparison.Ordinal);
        Assert.True(guard >= 0 && guard < alter && alter < update && update < end, "the ALTER and the UPDATE are inside the column-missing guard");
        Assert.Contains("column_name = 'rowcount_sum'", sql, StringComparison.Ordinal);

        /* No data is rewritten, no row is dropped, and no GRANT is needed. */
        foreach (var verb in new[] { "DELETE", "INSERT", "GRANT", "REVOKE", "TRUNCATE", "DROP" })
        {
            Assert.DoesNotMatch(@"(?m)^\s*" + verb + @"\b", sql);
        }
    }

    [Fact]
    public void TheProbeNamesTheColumn_AndMapsAFullyMigratedStoreToThisTopRung()
    {
        Assert.Contains("attname = 'rowcount_sum'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        Assert.Equal("hasPlanRegressionDailyRowcount", parameters[ProbeOrdinal].Name);
        Assert.Equal("hasServiceVersion", parameters[ProbeOrdinal - 1].Name);
        Assert.Equal(ProbeOrdinal, parameters.Length - 1);

        /* Every sentinel present maps to exactly this build's version; this sentinel absent maps one rung back. */
        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);
        Assert.Equal(RungVersion, (int)method.Invoke(null, all)!);

        var behind = (object[])all.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(RungVersion - 1, (int)method.Invoke(null, behind)!);

        /* In the source the arm sits ABOVE V174's and returns this rung's version. */
        var thisArm = viewer.IndexOf("if (hasPlanRegressionDailyRowcount)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasServiceVersion)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0 && previousArm > thisArm, "the V175 arm sits above V174's");
        Assert.Contains("return " + RungVersion + ";", viewer[thisArm..previousArm], StringComparison.Ordinal);
    }

    [Fact]
    public async Task MigratingAFreshStore_AddsTheColumn_AndAnUpgradeMarksBuiltDaysStale_AndARerunChangesNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the rowcount_sum rung's live pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            /* A fresh install: the column is there, nullable numeric, and the probe maps the store to the newest rung. */
            Assert.Equal(
                "numeric:YES",
                await ScalarAsync(connection,
                    "SELECT data_type || ':' || is_nullable FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'plan_regression_daily' AND column_name = 'rowcount_sum'", ct));
            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));

            /* An upgrade: one rung behind, with two days built and valid (built_seq = late_seq) and one never built. */
            await ExecAsync(connection, "ALTER TABLE collect.plan_regression_daily DROP COLUMN rowcount_sum", ct);
            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= " + RungVersion, ct);
            await ExecAsync(connection,
                "INSERT INTO collect.plan_regression_daily_built (server_id, day, late_seq, built_seq, built_at, source_rows) VALUES "
                + "(1, '2026-09-01', 3, 3, '2026-09-05 01:00:00', 10), (1, '2026-09-02', 0, 0, '2026-09-05 01:00:00', 4), (2, '2026-09-01', 7, NULL, NULL, NULL)", ct);

            await PgMigrations.MigrateAsync(connection, ct);

            Assert.Equal(StorageVersion.SchemaVersion, await ProbedVersionAsync(connection, ct));
            Assert.Equal(
                "numeric:YES",
                await ScalarAsync(connection,
                    "SELECT data_type || ':' || is_nullable FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'plan_regression_daily' AND column_name = 'rowcount_sum'", ct));

            /* Every built day now reads as stale, so a read takes it from the interval table. */
            Assert.Equal(0L, await ScalarAsync(connection,
                "SELECT count(*) FROM collect.plan_regression_daily_built WHERE built_seq = late_seq", ct));
            Assert.Equal("4,1,8", await ScalarAsync(connection,
                "SELECT string_agg(late_seq::text, ',' ORDER BY server_id, day) FROM collect.plan_regression_daily_built", ct));

            /* The rung's text run again: the column exists, so nothing moves. */
            await ExecAsync(connection, Rung.Sql, ct);
            Assert.Equal("4,1,8", await ScalarAsync(connection,
                "SELECT string_agg(late_seq::text, ',' ORDER BY server_id, day) FROM collect.plan_regression_daily_built", ct));

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

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }
}
