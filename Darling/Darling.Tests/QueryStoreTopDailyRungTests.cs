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
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Darling rung that adds the daily summary tables for <c>get_query_store_top</c>'s long windows (#5094):
/// <c>collect.query_store_top_daily</c> and <c>collect.query_store_top_daily_built</c>. Both are plain tables (a day is
/// deleted and rebuilt whole, which a hypertable chunk would complicate), the summary's key columns copy the types of
/// <c>collect.query_store_interval_wide</c> and its unique index is <c>NULLS NOT DISTINCT</c> because most key columns
/// are NULL for most rows. The summary is approximate by design; the rung only creates the tables, and no reader or
/// writer uses them yet. Every fact finds the rung by NAME, so a renumber is one edit to the registration, the constant
/// and the viewer's <c>return</c>. The live facts run when <c>DARLING_TEST_PG</c> is set, each on its own scratch store.
/// </summary>
[Collection("live-postgres")]
public sealed class QueryStoreTopDailyRungTests
{
    public const string RungName = "query-store-top-daily";

    private const string Daily = "query_store_top_daily";

    private const string Built = "query_store_top_daily_built";

    /// <summary>This rung's sentinel; the ordinal is a fact of the probe's shape, and it is the probe's last.</summary>
    private const int ProbeOrdinal = 136;

    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the daily summary tables' live pins (each mints its own scratch database).";

    private static readonly string[] KeyColumns =
    {
        "server_id", "day", "database_name", "query_id", "plan_id", "query_hash", "execution_type_desc", "replica_role", "module_name",
    };

    private static readonly string[] AveragedColumns =
    {
        "avg_duration_us", "avg_cpu_time_us", "avg_logical_io_reads", "avg_logical_io_writes", "avg_physical_io_reads", "avg_rowcount",
    };

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The rung's version, read off the migration registered under <see cref="RungName"/>.</summary>
    public static int RungVersion => Rung.Version;

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    /// <summary>The rung's SQL with its comments removed, so a pin reads statements and not the prose around them.</summary>
    private static string Statements() =>
        Regex.Replace(Rung.Sql, @"/\*.*?\*/", " ", RegexOptions.Singleline).Replace("\r\n", "\n", StringComparison.Ordinal);

    /// <summary>The columns of one <c>CREATE TABLE</c> in a script, name to type text, in order.</summary>
    private static List<(string Name, string Type)> ColumnsOf(string sql, string table)
    {
        var stripped = Regex.Replace(sql, @"/\*.*?\*/", " ", RegexOptions.Singleline).Replace("\r\n", "\n", StringComparison.Ordinal);
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
            var type = line[(space + 1)..].Replace(" NOT NULL", string.Empty, StringComparison.Ordinal).Trim();
            columns.Add((line[..space], type));
        }

        return columns;
    }

    private static string WideSql() =>
        PgMigrations.Scripts.Single(m => m.Sql.Contains("CREATE TABLE IF NOT EXISTS collect.query_store_interval_wide\n", StringComparison.Ordinal)
            || m.Sql.Contains("CREATE TABLE IF NOT EXISTS collect.query_store_interval_wide\r\n", StringComparison.Ordinal)).Sql;

    [Fact]
    public void TheRungIsRegisteredInADenseLadder_AndIsTheTopRung()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(RungName, Rung.Name);
        Assert.Equal(161, Rung.Version);
        Assert.Equal(StorageVersion.SchemaVersion, Rung.Version);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Contains(Rung.Version - 1, versions);
    }

    [Fact]
    public void BothTablesArePlainSchemaQualifiedTables_NotHypertables()
    {
        var sql = Statements();

        Assert.Contains("CREATE TABLE IF NOT EXISTS collect." + Daily + "\n", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect." + Built + "\n", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("create_hypertable", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("timescaledb", sql, StringComparison.OrdinalIgnoreCase);

        /* Every object is schema-qualified: the migrate session's search_path puts collect first, so a bare name would
           still land in collect, but the next reader should not have to know that. */
        foreach (var match in Regex.Matches(sql, @"(?:CREATE TABLE IF NOT EXISTS|ON)\s+(\S+)").Cast<Match>())
        {
            Assert.StartsWith("collect.", match.Groups[1].Value, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheUniqueIndex_IsNullsNotDistinct_OnExactlyTheIdentityKey()
    {
        var sql = Statements();
        var index = Regex.Match(sql, @"CREATE UNIQUE INDEX IF NOT EXISTS ux_query_store_top_daily\s+ON collect\." + Daily + @"\s*\((?<cols>[^)]*)\)\s*(?<tail>[^;]*);");

        Assert.True(index.Success, "the rung has no unique index named ux_query_store_top_daily on the summary table");
        var columns = index.Groups["cols"].Value.Split(',').Select(c => c.Trim()).Where(c => c.Length > 0).ToArray();
        Assert.Equal(KeyColumns, columns);
        Assert.Equal("NULLS NOT DISTINCT", index.Groups["tail"].Value.Trim());
    }

    [Fact]
    public void TheSummaryTable_HoldsTheExactlyRecombinableParts_AndTheRest()
    {
        var columns = ColumnsOf(Rung.Sql, "collect." + Daily);
        var names = columns.Select(c => c.Name).ToList();

        var expected = new List<string>(KeyColumns) { "interval_rows", "execution_count_sum" };
        foreach (var column in AveragedColumns)
        {
            expected.Add(column + "_sum");
            expected.Add(column + "_n");
        }

        expected.AddRange(new[] { "last_execution_time_max", "query_plan_hash_max", "first_execution_time_min" });
        Assert.Equal(expected, names);

        var types = columns.ToDictionary(c => c.Name, c => c.Type);
        Assert.Equal("bigint", types["interval_rows"]);
        Assert.Equal("numeric", types["execution_count_sum"]);
        foreach (var column in AveragedColumns)
        {
            Assert.Equal("numeric", types[column + "_sum"]);
            Assert.Equal("bigint", types[column + "_n"]);
        }

        /* Counts are NOT NULL (a day with no value has a zero count), sums are nullable (no value, no sum). */
        var sql = Statements();
        Assert.Contains("interval_rows bigint NOT NULL", sql, StringComparison.Ordinal);
        foreach (var column in AveragedColumns)
        {
            Assert.Contains(column + "_n bigint NOT NULL", sql, StringComparison.Ordinal);
            Assert.DoesNotContain(column + "_sum numeric NOT NULL", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void EverySharedColumn_HasTheWideTablesType()
    {
        var wide = ColumnsOf(WideSql(), "collect.query_store_interval_wide").ToDictionary(c => c.Name, c => c.Type);
        var daily = ColumnsOf(Rung.Sql, "collect." + Daily).ToDictionary(c => c.Name, c => c.Type);

        /* The key columns that the wide table also has, by name; day is the one new date. */
        foreach (var column in KeyColumns.Where(c => c != "day"))
        {
            Assert.True(wide.ContainsKey(column), column + " is not a wide-table column");
            Assert.Equal(wide[column], daily[column]);
        }

        /* The three aggregates over a wide column take that column's type. */
        Assert.Equal(wide["query_plan_hash"], daily["query_plan_hash_max"]);
        Assert.Equal(wide["last_execution_time"], daily["last_execution_time_max"]);
        Assert.Equal(wide["first_execution_time"], daily["first_execution_time_min"]);
        Assert.Equal("date", daily["day"]);
    }

    [Fact]
    public void TheBuiltTable_RecordsOneRowPerServerAndDay()
    {
        var columns = ColumnsOf(Rung.Sql, "collect." + Built);

        Assert.Equal(
            new[] { ("server_id", "integer"), ("day", "date"), ("pass", "smallint"), ("built_at", "timestamp"), ("source_rows", "bigint") },
            columns.ToArray());
        Assert.Matches(@"PRIMARY KEY \(server_id, day\)", Statements());
        Assert.Contains("pass smallint NOT NULL", Statements(), StringComparison.Ordinal);
        Assert.Contains("built_at timestamp NOT NULL", Statements(), StringComparison.Ordinal);
        Assert.Contains("source_rows bigint NOT NULL", Statements(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheRungNeedsNoGrant_BecauseTheCollectSchemaGrantsCoverANewTable()
    {
        var sql = Statements();

        Assert.DoesNotContain("GRANT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("REVOKE", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TheProbeCarriesTheTableAsItsLastArm_AndMapsFullyMigratedToTheTopRung()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var arm = $"to_regclass('collect.{Daily}') IS NOT NULL";
        Assert.Contains(arm, probe, StringComparison.Ordinal);
        Assert.True(probe.LastIndexOf("EXISTS", StringComparison.Ordinal) < probe.IndexOf(arm, StringComparison.Ordinal),
            "the new arm is the probe's last EXISTS, so it reads at the next ordinal");

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;
        Assert.Equal(ProbeOrdinal, arity - 1);
        Assert.Equal("hasQueryStoreTopDaily", method.GetParameters()[ProbeOrdinal].Name);

        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(Rung.Version, (int)method.Invoke(null, all)!);
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var behind = (object[])all.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(Rung.Version - 1, (int)method.Invoke(null, behind)!);

        var thisArm = viewer.IndexOf("if (hasQueryStoreTopDaily)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasCollectorRunAt)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0, "no sentinel arm: a fully-migrated store would map one rung short");
        Assert.True(thisArm < previousArm, "this arm sits above the previous rung's, so a current store maps one rung short");
        Assert.Contains($"return {Rung.Version};", viewer[thisArm..previousArm], StringComparison.Ordinal);

        /* The V71 finding: the table is named in the probe line and nowhere in the arm's prose. The comment block sits
           ABOVE the `if`, and this probe line has no information_schema in it, so the prose must not repeat the name. */
        var armProseStart = viewer.LastIndexOf("/* V161 (#5094)", thisArm, StringComparison.Ordinal);
        Assert.True(armProseStart >= 0, "the arm has no comment block saying why it exists");
        Assert.DoesNotContain(Daily, viewer[armProseStart..thisArm], StringComparison.Ordinal);
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

    /// <summary>Everything that describes the two tables, as one string, so a test can say "unchanged" in one assertion.</summary>
    private const string ShapeSql =
        "SELECT (SELECT string_agg(table_name || '.' || column_name || ':' || data_type || ':' || is_nullable, ',' ORDER BY table_name, ordinal_position) FROM information_schema.columns WHERE table_schema = 'collect' AND table_name IN ('query_store_top_daily', 'query_store_top_daily_built'))"
        + " || '|' || (SELECT string_agg(indexname || ':' || indexdef, ';' ORDER BY indexname) FROM pg_indexes WHERE schemaname = 'collect' AND tablename IN ('query_store_top_daily', 'query_store_top_daily_built'))"
        + " || '|' || (SELECT count(*) FROM pg_trigger WHERE tgrelid IN ('collect.query_store_top_daily'::regclass, 'collect.query_store_top_daily_built'::regclass) AND NOT tgisinternal)";

    [Fact]
    public async Task MigratingAFreshStore_CreatesBothTablesAndTheUniqueIndex_AsPlainTables()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct)));
            Assert.True(await ScalarAsync(connection, "SELECT to_regclass('collect.query_store_top_daily') IS NOT NULL AND to_regclass('collect.query_store_top_daily_built') IS NOT NULL", ct) is true);

            /* Plain relations (relkind 'r'), not hypertables, in the collect schema. */
            Assert.Equal(2L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'collect' AND c.relname IN ('query_store_top_daily', 'query_store_top_daily_built') AND c.relkind = 'r'", ct));
            /* Plain-ness is asked of the catalog only when the store has no timescaledb: the hypertables view does not exist
               there, and a relation in an OR still fails the parse. With the extension, neither table may be a hypertable. */
            var hasTimescale = await ScalarAsync(connection, "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')", ct) is true;
            if (hasTimescale)
            {
                Assert.True(await ScalarAsync(connection,
                    "SELECT NOT EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_schema = 'collect' AND hypertable_name IN ('query_store_top_daily', 'query_store_top_daily_built'))", ct) is true);
            }
            else
            {
                Assert.True(await ScalarAsync(connection,
                    "SELECT NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhparent IN ('collect.query_store_top_daily'::regclass, 'collect.query_store_top_daily_built'::regclass) OR i.inhrelid IN ('collect.query_store_top_daily'::regclass, 'collect.query_store_top_daily_built'::regclass))", ct) is true);
            }

            /* The unique index exists, is unique, and is NULLS NOT DISTINCT on the key. */
            var indexDef = (string?)await ScalarAsync(connection,
                "SELECT indexdef FROM pg_indexes WHERE schemaname = 'collect' AND tablename = 'query_store_top_daily' AND indexname = 'ux_query_store_top_daily'", ct);
            Assert.NotNull(indexDef);
            Assert.Contains("UNIQUE INDEX", indexDef, StringComparison.Ordinal);
            Assert.Contains("NULLS NOT DISTINCT", indexDef, StringComparison.Ordinal);
            Assert.Contains("(server_id, day, database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role, module_name)", indexDef, StringComparison.Ordinal);

            /* The built table's key is one row per server and day. */
            Assert.Equal("server_id,day", await ScalarAsync(connection,
                "SELECT string_agg(a.attname, ',' ORDER BY k.ord) FROM pg_constraint c CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord) JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum WHERE c.conrelid = 'collect.query_store_top_daily_built'::regclass AND c.contype = 'p'", ct));

            /* The live column types equal the wide table's for every shared column. */
            Assert.Equal(0L, await ScalarAsync(connection,
                "SELECT count(*) FROM information_schema.columns d JOIN information_schema.columns w ON w.table_schema = 'collect' AND w.table_name = 'query_store_interval_wide' AND w.column_name = d.column_name WHERE d.table_schema = 'collect' AND d.table_name = 'query_store_top_daily' AND d.data_type <> w.data_type", ct));

            /* NULLS NOT DISTINCT in effect: two rows whose nullable key columns are all NULL are one group. */
            const string Insert =
                "INSERT INTO collect.query_store_top_daily (server_id, day, interval_rows, avg_duration_us_n, avg_cpu_time_us_n, avg_logical_io_reads_n, avg_logical_io_writes_n, avg_physical_io_reads_n, avg_rowcount_n) VALUES (1, DATE '2026-01-02', 1, 0, 0, 0, 0, 0, 0)";
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
    public async Task TheReadOnlyRoles_CanSelectBothTables_ThroughTheCollectSchemaGrants()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);

        /* Roles are cluster-wide, so each run mints its own pair. */
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var roles = new[] { "qstd_viewer_" + suffix, "qstd_mcp_" + suffix };

        var bodySucceeded = false;
        try
        {
            /* The provisioning grant model for the read-only roles (DarlingManagedRoles sections 2 and 6): SELECT on
               every table in collect, as it is re-run after each migration pass. The rung adds no grant of its own. */
            foreach (var role in roles)
            {
                await ExecAsync(connection,
                    $"CREATE ROLE {role} NOLOGIN NOSUPERUSER; GRANT USAGE ON SCHEMA collect TO {role}; GRANT SELECT ON ALL TABLES IN SCHEMA collect TO {role};", ct);
            }

            foreach (var role in roles)
            {
                await ExecAsync(connection, $"SET ROLE {role}", ct);
                var roleBodySucceeded = false;
                try
                {
                    Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_store_top_daily", ct));
                    Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_store_top_daily_built", ct));

                    var refused = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(connection,
                        "DELETE FROM collect.query_store_top_daily", ct));
                    Assert.Equal("42501", refused.SqlState);
                    roleBodySucceeded = true;
                }
                finally
                {
                    /* RESET ROLE is a session setting: it has to run on the very session that took SET ROLE. */
                    await LiveStoreCleanup.RunOwnedAsync(roleBodySucceeded, async () =>
                        await ExecAsync(connection, "RESET ROLE", ct));
                }
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                foreach (var role in roles)
                {
                    await ExecAsync(cleanup, $"DROP OWNED BY {role}", cleanupCt);
                    await ExecAsync(cleanup, $"DROP ROLE IF EXISTS {role}", cleanupCt);
                }
            });
        }
    }

    [Fact]
    public async Task ASecondMigrateRun_IsANoOp_AndTheRungsSqlRunAgain_ChangesNothing_AndKeepsItsRows()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = await MigratedAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            await ExecAsync(connection,
                "INSERT INTO collect.query_store_top_daily (server_id, day, database_name, query_id, interval_rows, avg_duration_us_n, avg_cpu_time_us_n, avg_logical_io_reads_n, avg_logical_io_writes_n, avg_physical_io_reads_n, avg_rowcount_n) VALUES (1, DATE '2026-01-02', 'db_a', 7, 3, 3, 3, 3, 3, 3, 3)", ct);
            await ExecAsync(connection,
                "INSERT INTO collect.query_store_top_daily_built (server_id, day, pass, built_at, source_rows) VALUES (1, DATE '2026-01-02', 1, TIMESTAMP '2026-01-03 02:00:00', 3)", ct);

            var shapeBefore = await ScalarAsync(connection, ShapeSql, ct);
            var versionsBefore = await ScalarAsync(connection, "SELECT string_agg(version::text, ',' ORDER BY version) FROM darling_schema_version", ct);

            await PgMigrations.MigrateAsync(connection, ct);
            await ExecAsync(connection, Rung.Sql, ct);
            await ExecAsync(connection, Rung.Sql, ct);

            Assert.Equal(shapeBefore, await ScalarAsync(connection, ShapeSql, ct));
            Assert.Equal(versionsBefore, await ScalarAsync(connection, "SELECT string_agg(version::text, ',' ORDER BY version) FROM darling_schema_version", ct));
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_store_top_daily", ct));
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_store_top_daily_built", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
