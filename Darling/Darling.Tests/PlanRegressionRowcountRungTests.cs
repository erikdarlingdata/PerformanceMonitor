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
    public void TheScript_AddsTheIntervalTablesRowcountColumn_InItsOwnGuard_WithRawsType()
    {
        var sql = Statements();

        /* collect.query_store_interval_latest never had avg_rowcount (V143), yet the interval-table read and the daily build both
           read it: the rung adds it as a nullable bigint, the type of collect.query_store_stats.avg_rowcount, with no default and
           no backfill (an older interval row keeps NULL, so its rows per call is unknown). */
        Assert.Contains("ALTER TABLE collect.query_store_interval_latest ADD COLUMN avg_rowcount bigint;", sql, StringComparison.Ordinal);

        /* Its own column check, so each column add is idempotent on its own: the rowcount_sum guard cannot skip it. */
        var ownGuard = sql.IndexOf("column_name = 'avg_rowcount'", StringComparison.Ordinal);
        var alter = sql.IndexOf("ALTER TABLE collect.query_store_interval_latest", StringComparison.Ordinal);
        var firstEnd = sql.IndexOf("END IF;", StringComparison.Ordinal);
        Assert.True(firstEnd >= 0 && firstEnd < ownGuard && ownGuard < alter, "the avg_rowcount add sits in its own IF NOT EXISTS, after the rowcount_sum one closes");
        Assert.Contains("table_name = 'query_store_interval_latest'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheIntervalUpsert_WritesAvgRowcount_OnInsertAndOnConflict()
    {
        var sql = QueryStoreIntervalLatest.UpsertSql;

        Assert.Contains($"force_failure_count,{Environment.NewLine}        avg_rowcount,{Environment.NewLine}        query_text", sql.ReplaceLineEndings(), StringComparison.Ordinal);
        Assert.Contains("s.avg_rowcount,", sql, StringComparison.Ordinal);
        Assert.Contains("avg_rowcount = EXCLUDED.avg_rowcount,", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeNamesTheColumn_AndMapsToThisRungWhenNoLaterSentinelIsPresent()
    {
        Assert.Contains("attname = 'rowcount_sum'", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        /* V176's (#5638), V177's (#5641), V178's (#5684), V179's (#5640) and V180's (#5654) sentinels are appended after this one, so this rung is no longer the top. */

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        Assert.Equal("hasPlanRegressionDailyRowcount", parameters[ProbeOrdinal].Name);
        Assert.Equal("hasServiceVersion", parameters[ProbeOrdinal - 1].Name);
        Assert.Equal("hasProcedureStatsTotals", parameters[ProbeOrdinal + 1].Name);
        Assert.Equal("hasRunnableQueueSettings", parameters[ProbeOrdinal + 2].Name);
        Assert.Equal("hasQueryStoreWatermarks", parameters[ProbeOrdinal + 3].Name);
        Assert.Equal("hasClientTimeoutSettings", parameters[ProbeOrdinal + 4].Name);
        Assert.Equal("hasExternalAlerts", parameters[ProbeOrdinal + 5].Name);
        Assert.Equal("hasNotificationRouteScope", parameters[ProbeOrdinal + 6].Name);
        Assert.Equal("hasReadLatencyCapacityFields", parameters[ProbeOrdinal + 7].Name);
        Assert.Equal("hasReadCaller", parameters[ProbeOrdinal + 8].Name);
        Assert.Equal("hasInvestigations", parameters[ProbeOrdinal + 9].Name);
        Assert.Equal("hasPlanRegressionPartialDays", parameters[ProbeOrdinal + 10].Name);

        /* Every sentinel present maps to exactly this build's version. With the later sentinels absent this rung is the
           answer; with this one absent too, one rung back. */
        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);
        /* V176 (#5638) sits above this rung: with only this sentinel and those below it the store maps to this rung. */
        var withoutLater = (object[])all.Clone();
        withoutLater[ProbeOrdinal + 1] = false;
        /* V177 (#5641), V178 (#5684) and V179 (#5640) sit above V176 and are appended after it, so they go absent too. */
        withoutLater[ProbeOrdinal + 2] = false;
        withoutLater[ProbeOrdinal + 3] = false;
        withoutLater[ProbeOrdinal + 4] = false;
        /* V180 (#5654) sits above V179 and goes absent too. */
        withoutLater[ProbeOrdinal + 5] = false;
        /* V181 (#5750) sits above V180 and goes absent too. */
        withoutLater[ProbeOrdinal + 6] = false;
        /* V182 (#5506) sits above V181 and goes absent too. */
        withoutLater[ProbeOrdinal + 7] = false;
        /* V183 (#5793) sits above V182 and goes absent too. */
        withoutLater[ProbeOrdinal + 8] = false;
        /* V184 (#5632) and V185 (#5812) sit above V183 and go absent too. */
        withoutLater[ProbeOrdinal + 9] = false;
        withoutLater[ProbeOrdinal + 10] = false;
        /* V187 (#5825) and any later rung's sentinel go absent too. */
        for (var i = ProbeOrdinal + 11; i < withoutLater.Length; i++)
        {
            withoutLater[i] = false;
        }

        Assert.Equal(RungVersion, (int)method.Invoke(null, withoutLater)!);

        var behind = (object[])withoutLater.Clone();
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

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct, emptyStore: true);
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

            /* The interval table has the raw table's rows-per-call column too (V143 created it without one). */
            const string IntervalColumnSql =
                "SELECT data_type || ':' || is_nullable FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'query_store_interval_latest' AND column_name = 'avg_rowcount'";
            Assert.Equal("bigint:YES", await ScalarAsync(connection, IntervalColumnSql, ct));

            /* An upgrade: one rung behind, with two days built and valid (built_seq = late_seq) and one never built. */
            await ExecAsync(connection, "ALTER TABLE collect.plan_regression_daily DROP COLUMN rowcount_sum", ct);
            await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest DROP COLUMN avg_rowcount", ct);
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

            Assert.Equal("bigint:YES", await ScalarAsync(connection, IntervalColumnSql, ct));

            /* Every built day now reads as stale, so a read takes it from the interval table. */
            Assert.Equal(0L, await ScalarAsync(connection,
                "SELECT count(*) FROM collect.plan_regression_daily_built WHERE built_seq = late_seq", ct));
            Assert.Equal("4,1,8", await ScalarAsync(connection,
                "SELECT string_agg(late_seq::text, ',' ORDER BY server_id, day) FROM collect.plan_regression_daily_built", ct));

            /* The rung's text run again: the columns exist, so nothing moves. */
            await ExecAsync(connection, Rung.Sql, ct);
            Assert.Equal("bigint:YES", await ScalarAsync(connection, IntervalColumnSql, ct));

            /* The two column adds are independent: with only avg_rowcount missing, running the text adds it and leaves the days alone. */
            await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest DROP COLUMN avg_rowcount", ct);
            await ExecAsync(connection, Rung.Sql, ct);
            Assert.Equal("bigint:YES", await ScalarAsync(connection, IntervalColumnSql, ct));
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
