/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
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
/// Pins the Darling rung that adds the per-day per-plan totals PLAN_REGRESSION reads for closed days (#5448):
/// <c>collect.plan_regression_daily</c>, <c>collect.plan_regression_daily_built</c>, the function
/// <c>collect.plan_regression_daily_mark_late</c> and the trigger that calls it from
/// <c>collect.query_store_interval_latest</c>. Both tables are plain tables (a day is deleted and rebuilt whole), the
/// unique index is <c>NULLS NOT DISTINCT</c> because most key columns can be NULL, the function is SECURITY INVOKER
/// with a pinned <c>search_path</c>, and the rung adds no index on the interval table. The rung only creates the
/// objects; no builder or reader uses them yet. Every fact finds the rung by NAME, so a renumber is one edit to the
/// registration, the constant and the viewer's <c>return</c>. The live facts run when <c>DARLING_TEST_PG</c> is set,
/// each on its own scratch store.
///
/// <para>This file carries the "I am the top rung" claims that moved off <see cref="PagerDutyAutoResolveRungTests"/>
/// (V166) when this rung landed: a fully-migrated store must map to EXACTLY this version, or the viewer's connect-time
/// gate refuses a store that is actually current.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PlanRegressionDailyRungTests
{
    public const string RungName = "plan-regression-daily";

    private const int RungVersion = 168;

    private const int PreviousVersion = 167;

    /// <summary>This rung's sentinel ordinal in the viewer probe, the newest and so the last argument.</summary>
    private const int ProbeOrdinal = 143;

    private const string Daily = "plan_regression_daily";

    private const string Built = "plan_regression_daily_built";

    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the PLAN_REGRESSION per-day totals' live pins (each mints its own scratch database).";

    private static readonly string[] DailyColumns =
    {
        "server_id", "day", "database_name", "query_id", "plan_id", "replica_role", "query_plan_hash",
        "execs", "cpu_us_sum", "dur_us_sum", "last_exec", "is_forced_plan", "force_failure_count",
    };

    private static readonly string[] KeyColumns =
    {
        "server_id", "day", "database_name", "query_id", "plan_id", "replica_role", "query_plan_hash",
    };

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    /// <summary>The rung's SQL with its comments removed, so a pin reads statements and not the prose around them.</summary>
    private static string Statements() =>
        Regex.Replace(Rung.Sql, @"/\*.*?\*/", " ", RegexOptions.Singleline).Replace("\r\n", "\n", StringComparison.Ordinal);

    /// <summary>The columns of one <c>CREATE TABLE</c> in a script, name to type text, in order.</summary>
    private static List<(string Name, string Type)> ColumnsOf(string table)
    {
        var stripped = Statements();
        var header = "CREATE TABLE IF NOT EXISTS " + table + "\n(";
        var start = stripped.IndexOf(header, StringComparison.Ordinal);
        Assert.True(start >= 0, "no CREATE TABLE for " + table);
        var end = stripped.IndexOf("\n)", start, StringComparison.Ordinal);
        var columns = new List<(string, string)>();
        foreach (var raw in stripped[(start + header.Length)..end].Split('\n'))
        {
            var line = raw.Trim().TrimEnd(',');
            if (line.Length == 0 || line.StartsWith("CONSTRAINT", StringComparison.Ordinal))
            {
                continue;
            }

            var space = line.IndexOf(' ', StringComparison.Ordinal);
            columns.Add((line[..space], line[(space + 1)..].Trim()));
        }

        return columns;
    }

    [Fact]
    public void TheRungIsRegisteredInADenseLadder_BelowTheCurrentTop()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(RungName, Rung.Name);
        Assert.Equal(RungVersion, Rung.Version);
        /* No longer the top rung: the per-server AWS role rung (V169) landed above it. */
        Assert.True(RungVersion < StorageVersion.SchemaVersion);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Contains(RungVersion + 1, versions);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Contains(PreviousVersion, versions);
    }

    [Fact]
    public void BothTablesArePlainSchemaQualifiedTables_NotHypertables()
    {
        var sql = Statements();

        Assert.Contains("CREATE TABLE IF NOT EXISTS collect." + Daily + "\n", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect." + Built + "\n", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("create_hypertable", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("timescaledb", sql, StringComparison.OrdinalIgnoreCase);

        /* The rung never uses a bare name: the migrate session's search_path puts collect first, but the function's does not. */
        foreach (var match in Regex.Matches(sql, @"(?:CREATE TABLE IF NOT EXISTS|CREATE OR REPLACE FUNCTION|INSERT INTO|DROP TRIGGER IF EXISTS \S+ ON|EXECUTE FUNCTION|AFTER INSERT OR UPDATE ON)\s+(\S+)").Cast<Match>())
        {
            Assert.StartsWith("collect.", match.Groups[1].Value, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheDailyTable_HasTheThirteenColumns_InOrder_WithTheReadsTypes()
    {
        var columns = ColumnsOf("collect." + Daily);
        Assert.Equal(DailyColumns, columns.Select(c => c.Name).ToArray());

        var types = columns.ToDictionary(c => c.Name, c => c.Type);
        Assert.Equal("integer NOT NULL", types["server_id"]);
        Assert.Equal("date NOT NULL", types["day"]);
        Assert.Equal("numeric", types["execs"]);
        Assert.Equal("numeric", types["cpu_us_sum"]);
        Assert.Equal("numeric", types["dur_us_sum"]);
        Assert.Equal("timestamp", types["last_exec"]);
        Assert.Equal("boolean", types["is_forced_plan"]);
        Assert.Equal("bigint", types["force_failure_count"]);
        Assert.Equal("text", types["query_plan_hash"]);
        Assert.Equal("text", types["replica_role"]);
    }

    [Fact]
    public void TheUniqueIndex_IsNullsNotDistinct_OnExactlyTheIdentityKey()
    {
        var index = Regex.Match(Statements(), @"CREATE UNIQUE INDEX IF NOT EXISTS ux_plan_regression_daily\s+ON collect\." + Daily + @"\s*\((?<cols>[^)]*)\)\s*(?<tail>[^;]*);");

        Assert.True(index.Success, "the rung has no unique index named ux_plan_regression_daily on the totals table");
        Assert.Equal(KeyColumns, index.Groups["cols"].Value.Split(',').Select(c => c.Trim()).Where(c => c.Length > 0).ToArray());
        Assert.Equal("NULLS NOT DISTINCT", index.Groups["tail"].Value.Trim());
    }

    [Fact]
    public void TheBuiltTable_IsTheValidityRecord_OneRowPerServerAndDay()
    {
        var columns = ColumnsOf("collect." + Built);

        Assert.Equal(
            new[]
            {
                ("server_id", "integer NOT NULL"), ("day", "date NOT NULL"), ("late_seq", "bigint NOT NULL DEFAULT 0"),
                ("built_seq", "bigint"), ("built_at", "timestamp"), ("source_rows", "bigint"),
            },
            columns.ToArray());
        Assert.Contains("CONSTRAINT pk_plan_regression_daily_built PRIMARY KEY (server_id, day)", Statements(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheRungNeedsNoGrant_BecauseTheCollectSchemaGrantsCoverANewTable()
    {
        var sql = Statements();

        Assert.DoesNotContain("GRANT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("REVOKE", sql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The function runs as whoever writes <c>query_store_interval_latest</c>, so it is SECURITY INVOKER (the default,
    /// and never DEFINER here) and pins its own <c>search_path</c>: a function that inherits the caller's could be made
    /// to resolve <c>date_trunc</c> or the bookkeeping table to an object of the caller's choosing.
    /// </summary>
    [Fact]
    public void TheMarkFunction_PinsItsSearchPath_AndIsSecurityInvoker()
    {
        var sql = Statements();
        var function = Regex.Match(sql, @"CREATE OR REPLACE FUNCTION collect\.plan_regression_daily_mark_late\(\) RETURNS trigger\s+LANGUAGE plpgsql\s+SET search_path = pg_catalog, pg_temp\s+AS \$f\$.*?\$f\$;", RegexOptions.Singleline);

        Assert.True(function.Success, "the function has no `SET search_path = pg_catalog, pg_temp` clause");
        Assert.DoesNotContain("SECURITY DEFINER", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("INSERT INTO collect.plan_regression_daily_built AS b", function.Value, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (server_id, day) DO UPDATE SET late_seq = b.late_seq + 1", function.Value, StringComparison.Ordinal);
        /* Marks the day of first_execution_time and the day after it (last_execution_time can cross midnight). */
        Assert.Contains("(NEW.first_execution_time::date, new0), (NEW.first_execution_time::date + 1, new1)", function.Value, StringComparison.Ordinal);
    }

    /// <summary>
    /// #5448: the function bumps a (server, day) once per transaction. The apply is one statement, so a bump per
    /// late row updates the same built tuple tens of thousands of times inside one transaction, and each conflict check
    /// after the first walks the whole version chain (quadratic: 17 s for 50,000 late rows). The pairs already bumped are
    /// kept in a transaction-local setting, and a row whose days are both in it returns before any write. A pin on the
    /// shape, because the live tests need a server: the setting is read, the guard returns early, only the unmarked days
    /// are written, and the setting is written with is_local true (false would leak the marks to the next transaction of
    /// the session and silently drop its bumps).
    /// </summary>
    [Fact]
    public void TheMarkFunction_BumpsAPairOncePerTransaction_ThroughATransactionLocalSetting()
    {
        var sql = Statements();
        var function = Regex.Match(sql, @"CREATE OR REPLACE FUNCTION collect\.plan_regression_daily_mark_late\(\).*?\$f\$;", RegexOptions.Singleline).Value;

        Assert.Contains("current_setting('darling.plan_regression_marked', true)", function, StringComparison.Ordinal);
        Assert.Contains("nullif(current_setting('darling.plan_regression_marked', true), '')", function, StringComparison.Ordinal);
        Assert.Contains("IF NOT (new0 OR new1) THEN", function, StringComparison.Ordinal);
        Assert.Contains("WHERE v.fresh", function, StringComparison.Ordinal);
        Assert.Matches(@"set_config\('darling\.plan_regression_marked',[^;]*, true\);", function);
        Assert.DoesNotContain(", false)", function, StringComparison.Ordinal);

        /* First executions run T-17 through T-2, and the day after each is marked too: at most 17 pairs per server. The
           function body (comment included) is stored in pg_proc.prosrc, so the number it states has to be the true one. */
        Assert.Matches(@"keeps the list to at most\s+17 pairs per server", Rung.Sql);
        Assert.DoesNotContain("19 pairs", Rung.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheTrigger_IsAfterInsertOrUpdate_PerRow_ClampedToTheWindow_AndTheRungAddsNoIndexOnTheIntervalTable()
    {
        var sql = Statements();
        var trigger = Regex.Match(sql, @"CREATE TRIGGER trg_plan_regression_daily_late\s+AFTER INSERT OR UPDATE ON collect\.query_store_interval_latest\s+FOR EACH ROW\s+WHEN \((?<when>.*?)\)\s+EXECUTE FUNCTION collect\.plan_regression_daily_mark_late\(\);", RegexOptions.Singleline);

        Assert.True(trigger.Success, "the rung has no row trigger named trg_plan_regression_daily_late on the interval table");
        Assert.Contains("DROP TRIGGER IF EXISTS trg_plan_regression_daily_late ON collect.query_store_interval_latest;", sql, StringComparison.Ordinal);

        /* The WHEN condition reads NEW only (PostgreSQL forbids a subquery there), at least a day old and no earlier than the start of the day 17 back. */
        var condition = Regex.Replace(trigger.Groups["when"].Value, @"\s+", " ");
        Assert.Contains("NEW.first_execution_time < date_trunc('day', now() AT TIME ZONE 'UTC') - interval '1 day'", condition, StringComparison.Ordinal);
        /* A whole-day edge, one day wider than the cleanup's 16 days, so every day the cleanup keeps can be marked: a late row
           for T-16 has its first execution on T-17, and a clamp at now() minus 16 days would miss it between midnight and
           the time of day. */
        Assert.Contains("NEW.first_execution_time >= date_trunc('day', now() AT TIME ZONE 'UTC') - interval '17 days'", condition, StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT", condition, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(PlanRegressionDaily.MarkLateWindowDays, 16);

        /* No index on the interval table in this rung: V153's first_execution_time index serves the per-day builds. */
        Assert.DoesNotContain("CREATE INDEX", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TheRungAndTheBuilderCarryNoProcessWording_BecauseTheFunctionBodyIsStoredInEveryStore()
    {
        /* The function body is kept in pg_proc.prosrc on every store, and the doc comments ship with the product: neither
           names a work lane or a hand-over step. */
        var storage = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PlanRegressionDaily.cs");
        foreach (var text in new[] { Rung.Sql, storage })
        {
            Assert.DoesNotMatch(@"\blane\b", text);
            Assert.DoesNotMatch(@"big-store EXPLAIN", text);
        }
    }

    [Fact]
    public void TheCleanup_IsDrivenFromTheSmallBuiltTable_SoTheTotalsDeleteIsAnIndexProbeByKey()
    {
        var built = Regex.Replace(PlanRegressionDaily.GcBuiltSql, @"\s+", " ");
        Assert.StartsWith("DELETE FROM collect.plan_regression_daily_built", built.TrimStart(), StringComparison.Ordinal);
        Assert.Contains("RETURNING server_id, day", built, StringComparison.Ordinal);

        /* The totals delete is the two leading columns of the unique index with constants: no OR of a day test and a NOT IN
           on the totals table, which could not use that index and would read every totals row every hour. */
        var totals = Regex.Replace(PlanRegressionDaily.GcTotalsSql, @"\s+", " ");
        Assert.Contains("DELETE FROM collect.plan_regression_daily WHERE server_id = $1::integer AND day = $2::date;", totals, StringComparison.Ordinal);
        Assert.DoesNotContain(" OR ", totals, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT IN", totals, StringComparison.Ordinal);
    }

    [Fact]
    public void TheClosedDayArithmetic_IsOnePastOneSkewAndOneSpan()
    {
        Assert.Equal(14, PlanRegressionDaily.WindowDays);
        Assert.Equal(1, PlanRegressionDaily.PlanRegressionSkewMarginDays);
        Assert.Equal(3, PlanRegressionDaily.ClosedDayLagDays);
    }

    [Fact]
    public void BuildDaySql_InsertsTheTablesColumnsInOrder_FromTheIntervalTable_WithTheDaysBounds()
    {
        var sql = Regex.Replace(PlanRegressionDaily.BuildDaySql, @"\s+", " ");

        var insert = Regex.Match(sql, @"INSERT INTO collect\.plan_regression_daily \((?<cols>[^)]*)\)");
        Assert.True(insert.Success);
        Assert.Equal(DailyColumns, insert.Groups["cols"].Value.Split(',').Select(c => c.Trim()).ToArray());

        Assert.Contains("FROM collect.query_store_interval_latest AS l", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE l.server_id = $1::integer AND l.first_execution_time >= $4::timestamp AND l.first_execution_time < $3::timestamp AND l.last_execution_time >= $2::date AND l.last_execution_time < $3::timestamp AND l.collection_time >= $4::timestamp", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY l.database_name, l.query_id, l.plan_id, l.replica_role, l.query_plan_hash", sql, StringComparison.Ordinal);

        /* The same aggregates as PLAN_REGRESSION's plan_agg step, summed rather than divided. */
        Assert.Contains("SUM(l.execution_count)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(l.avg_cpu_time_us::numeric * l.execution_count)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(l.avg_duration_us::numeric * l.execution_count)", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(l.last_execution_time)", sql, StringComparison.Ordinal);
        Assert.Contains("bool_or(l.is_forced_plan)", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(l.force_failure_count)", sql, StringComparison.Ordinal);

        /* Four parameters, each cast where it is used, so each has one type. */
        Assert.Equal(new[] { "$1", "$2", "$3", "$4" }, Regex.Matches(sql, @"\$\d").Select(m => m.Value).Distinct().OrderBy(v => v).ToArray());
        Assert.DoesNotContain("$5", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheProbeCarriesTheBuiltTableAtItsOwnOrdinal_AndMapsItsRung()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var arm = $"to_regclass('collect.{Built}') IS NOT NULL";
        Assert.Contains(arm, probe, StringComparison.Ordinal);
        Assert.True(probe.IndexOf(arm, StringComparison.Ordinal) > probe.IndexOf("legacy_secret_pin_candidate", StringComparison.Ordinal),
            "this rung's sentinel follows V167's");

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal + 2})", viewer, StringComparison.Ordinal);
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal + 3})", viewer, StringComparison.Ordinal);
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal + 4})", viewer, StringComparison.Ordinal);
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal + 5})", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({ProbeOrdinal + 6})", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();

        /* A newer rung's sentinel (V173's, #5582) is the last argument. */
        Assert.Equal(ProbeOrdinal + 5, parameters.Length - 1);
        Assert.Equal("hasPlanRegressionDaily", parameters[ProbeOrdinal].Name);

        /* Every sentinel true is a fully-migrated store, which maps to exactly this build's version. */
        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        /* A store that stopped at this rung answers this rung; the same store without its sentinel answers the one before. */
        var atThisRung = Enumerable.Range(0, parameters.Length).Select(i => (object)(i <= ProbeOrdinal)).ToArray();
        Assert.Equal(RungVersion, (int)method.Invoke(null, atThisRung)!);
        var behind = (object[])atThisRung.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, behind)!);

        /* The arm sits ABOVE V167's (newest-first is the contract of that method) and returns this rung's version. */
        var thisArm = viewer.IndexOf("if (hasPlanRegressionDaily)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasLegacyPinCandidates)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "the viewer has no V168 sentinel arm: a fully-migrated store would map to 167");
        Assert.True(previousArm >= 0, "the V167 arm is gone, so this pin is comparing against nothing");
        Assert.True(thisArm < previousArm, "this arm sits below V167's, so a current store maps one rung low");
        Assert.Contains("return " + RungVersion.ToString(CultureInfo.InvariantCulture) + ";", viewer[thisArm..previousArm], StringComparison.Ordinal);

        /* The table is named in the probe line and nowhere in the arm's prose (the V71 finding). */
        var proseStart = viewer.LastIndexOf("/* V168 (#5448)", thisArm, StringComparison.Ordinal);
        Assert.True(proseStart >= 0, "the arm has no comment block saying why it exists");
        Assert.DoesNotContain(Built, viewer[proseStart..thisArm], StringComparison.Ordinal);
        Assert.DoesNotContain(Daily, viewer[proseStart..thisArm], StringComparison.Ordinal);
    }

    private static async Task<NpgsqlConnection> MigratedAsync(ScratchPostgres scratch, System.Threading.CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
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

    [Fact]
    public async Task MigratingAFreshStore_CreatesTheObjects_AsPlainTables_WithAPinnedFunctionAndAnEnabledTrigger()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        // #1776 own-store: a scratch database of its own, so no other class's migrations race it.
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct), CultureInfo.InvariantCulture));
            Assert.True(await ScalarAsync(connection, "SELECT to_regclass('collect.plan_regression_daily') IS NOT NULL AND to_regclass('collect.plan_regression_daily_built') IS NOT NULL", ct) is true);

            /* Plain relations (relkind 'r'). */
            Assert.Equal(2L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'collect' AND c.relname IN ('plan_regression_daily', 'plan_regression_daily_built') AND c.relkind = 'r'", ct));

            /* The unique index is unique, NULLS NOT DISTINCT, on the key. */
            var indexDef = (string?)await ScalarAsync(connection,
                "SELECT indexdef FROM pg_indexes WHERE schemaname = 'collect' AND tablename = 'plan_regression_daily' AND indexname = 'ux_plan_regression_daily'", ct);
            Assert.NotNull(indexDef);
            Assert.Contains("UNIQUE INDEX", indexDef, StringComparison.Ordinal);
            Assert.Contains("NULLS NOT DISTINCT", indexDef, StringComparison.Ordinal);
            Assert.Contains("(server_id, day, database_name, query_id, plan_id, replica_role, query_plan_hash)", indexDef, StringComparison.Ordinal);

            /* The function: plpgsql, SECURITY INVOKER, and the pinned search_path is stored on the function itself. */
            Assert.Equal("plpgsql|false|{\"search_path=pg_catalog, pg_temp\"}", await ScalarAsync(connection,
                "SELECT l.lanname || '|' || p.prosecdef::text || '|' || p.proconfig::text FROM pg_proc p JOIN pg_language l ON l.oid = p.prolang WHERE p.pronamespace = 'collect'::regnamespace AND p.proname = 'plan_regression_daily_mark_late'", ct));

            /* The trigger is on the interval table, row-level, after insert or update, and enabled. */
            Assert.Equal("O|true", await ScalarAsync(connection,
                "SELECT t.tgenabled::text || '|' || ((t.tgtype & 1) = 1)::text FROM pg_trigger t WHERE t.tgrelid = 'collect.query_store_interval_latest'::regclass AND t.tgname = 'trg_plan_regression_daily_late' AND NOT t.tgisinternal", ct));

            /* NULLS NOT DISTINCT in effect: two rows whose nullable key columns are all NULL are one group. */
            const string Insert = "INSERT INTO collect.plan_regression_daily (server_id, day) VALUES (1, DATE '2026-01-01')";
            await ExecAsync(connection, Insert, ct);
            var duplicate = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection, Insert, ct));
            Assert.Equal("23505", duplicate.SqlState);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task MigratingTwice_ChangesNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        // #1776 own-store: a scratch database of its own, so no other class's migrations race it.
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            /* Re-run just this rung's script on a store that already has it: every statement is idempotent. */
            await ExecAsync(connection, Rung.Sql, ct);
            await ExecAsync(connection, Rung.Sql, ct);
            Assert.Equal(1L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'collect.query_store_interval_latest'::regclass AND tgname = 'trg_plan_regression_daily_late'", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// The day builder's SQL runs against the migrated schema and returns the same per-plan totals the read's
    /// <c>plan_agg</c> step would sum: two plans on a closed day, one row of another day left out, one NULL-keyed row.
    /// </summary>
    [Fact]
    public async Task BuildDaySql_SumsTheDaysRowsPerPlan_AndLeavesOtherDaysAlone()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        // #1776 own-store: a scratch database of its own, so no other class's migrations race it.
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            /* The trigger's WHEN clause is not under test here, and an old-dated insert would mark days: disable it. */
            await ExecAsync(connection, "ALTER TABLE collect.query_store_interval_latest DISABLE TRIGGER trg_plan_regression_daily_late", ct);

            var d = new DateTime(2026, 3, 10, 0, 0, 0, DateTimeKind.Unspecified);
            const string Insert =
                "INSERT INTO collect.query_store_interval_latest (server_id, database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time, collection_time, query_plan_hash, execution_count, avg_cpu_time_us, avg_duration_us, last_execution_time, is_forced_plan, force_failure_count) "
                + "VALUES (7, $1, 10, $2, NULL, $3, $4, $4 + interval '1 hour', 'h' || $2, $5, $6, $7, $4 + interval '30 minutes', $8, 0)";
            async Task RowAsync(string? db, long plan, long interval, DateTime first, long execs, long cpu, long dur, bool forced)
            {
                await using var cmd = new NpgsqlCommand(Insert, connection);
                cmd.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Text, (object?)db ?? DBNull.Value);
                cmd.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Bigint, plan);
                cmd.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Bigint, interval);
                cmd.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, first);
                cmd.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Bigint, execs);
                cmd.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Bigint, cpu);
                cmd.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Bigint, dur);
                cmd.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Boolean, forced);
                await cmd.ExecuteNonQueryAsync(ct);
            }

            /* Plan 1: two intervals on day D (10 x 100 + 30 x 200 = 7000 cpu, 10 x 1000 + 30 x 2000 = 70000 dur). */
            await RowAsync("db1", 1, 1, d.AddHours(2), 10, 100, 1000, false);
            await RowAsync("db1", 1, 2, d.AddHours(5), 30, 200, 2000, true);
            /* Plan 2, NULL database. */
            await RowAsync(null, 2, 3, d.AddHours(3), 5, 50, 500, false);
            /* Another day's row for plan 1, which must not land in D. */
            await RowAsync("db1", 1, 4, d.AddDays(1).AddHours(2), 99, 1, 1, false);

            await using (var build = new NpgsqlCommand(PlanRegressionDaily.BuildDaySql, connection))
            {
                build.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Integer, 7);
                build.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Date, d.Date);
                build.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, d.AddDays(1));
                build.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, d.AddDays(-1));
                Assert.Equal(2L, await build.ExecuteScalarAsync(ct));
            }

            Assert.Equal("db1|1|40|7000|70000|true;<null>|2|5|250|2500|false", await ScalarAsync(connection,
                "SELECT string_agg(COALESCE(database_name, '<null>') || '|' || plan_id || '|' || execs || '|' || cpu_us_sum || '|' || dur_us_sum || '|' || is_forced_plan::text, ';' ORDER BY plan_id) FROM collect.plan_regression_daily WHERE server_id = 7 AND day = DATE '2026-03-10'", ct));
            Assert.Equal(2L, await ScalarAsync(connection, "SELECT count(*) FROM collect.plan_regression_daily", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
