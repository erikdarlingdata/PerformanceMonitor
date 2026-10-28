/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6143: monitoring connections to a PostgreSQL target pin search_path to pg_catalog at sign-in, and the collectors
/// name extension objects by the schema the extension was created in.
/// <para>The fixture is a database owned by a role that is not a superuser. A database setting, a role setting or a
/// role-in-database setting puts a schema of the owner's first in the search_path. That schema holds an <c>=</c>
/// operator for (integer, integer) and for (oid, oid), and same-named copies of functions the collectors call; each one
/// records its call. The test first proves the copies do run on a connection without the pin, then runs the collectors
/// through the service's own connection string and asserts they return rows and the copies never ran.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgCollectorSearchPathLiveTests
{
    private const string Database = "pm6143_owned";
    private const string HypoDatabase = "pm6143_hypo";
    private const string AuroraDatabase = "pm6143_aurora";
    private const string InfoDatabase = "pm6143_info";
    private const string TextDatabase = "pm6143_text";
    private const string Owner = "pm6143_owner";
    private const string Monitor = "pm6143_monitor";
    private const int ServerId = 6143;

    /* A schema name that has to be quoted: a space and a double quote. */
    private const string ExtSchema = "\"Ext \"\"q\"\"\"";

    /* Same-named copies of built-in functions, each with the built-in's exact signature, so a search_path that lists
       the owner's schema first resolves an unqualified call to the copy. public.pg_qualstats() stands where the
       extension's function would be if it were created in public; pg_qualstats is not created in this database. */
    private const string SetupSql = """"
        CREATE SCHEMA lookalike AUTHORIZATION pm6143_owner;
        SET ROLE pm6143_owner;
        CREATE TABLE lookalike.calls (what text NOT NULL);
        GRANT USAGE ON SCHEMA lookalike TO PUBLIC;
        GRANT INSERT ON lookalike.calls TO PUBLIC;

        CREATE FUNCTION lookalike.int4_eq(integer, integer) RETURNS boolean LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO lookalike.calls VALUES ('=(int4,int4)'); RETURN $1 OPERATOR(pg_catalog.=) $2; END $$;
        CREATE OPERATOR lookalike.= (LEFTARG = integer, RIGHTARG = integer, FUNCTION = lookalike.int4_eq);

        CREATE FUNCTION lookalike.oid_eq(oid, oid) RETURNS boolean LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO lookalike.calls VALUES ('=(oid,oid)'); RETURN $1 OPERATOR(pg_catalog.=) $2; END $$;
        CREATE OPERATOR lookalike.= (LEFTARG = oid, RIGHTARG = oid, FUNCTION = lookalike.oid_eq);

        CREATE FUNCTION lookalike.current_database() RETURNS name LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO lookalike.calls VALUES ('current_database'); RETURN pg_catalog.current_database(); END $$;
        CREATE FUNCTION lookalike.now() RETURNS timestamptz LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO lookalike.calls VALUES ('now'); RETURN pg_catalog.now(); END $$;
        CREATE FUNCTION lookalike.pg_relation_size(regclass) RETURNS bigint LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO lookalike.calls VALUES ('pg_relation_size'); RETURN pg_catalog.pg_relation_size($1); END $$;
        CREATE FUNCTION lookalike.pg_total_relation_size(regclass) RETURNS bigint LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO lookalike.calls VALUES ('pg_total_relation_size'); RETURN pg_catalog.pg_total_relation_size($1); END $$;
        CREATE FUNCTION lookalike.current_setting(text, boolean) RETURNS text LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO lookalike.calls VALUES ('current_setting'); RETURN pg_catalog.current_setting($1, $2); END $$;
        CREATE FUNCTION lookalike.quote_ident(text) RETURNS text LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO lookalike.calls VALUES ('quote_ident'); RETURN pg_catalog.quote_ident($1); END $$;

        CREATE FUNCTION public.pg_qualstats() RETURNS SETOF record LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO lookalike.calls VALUES ('pg_qualstats'); RETURN; END $$;

        CREATE TABLE public.pm6143_rows (id integer PRIMARY KEY, label text NOT NULL);
        INSERT INTO public.pm6143_rows SELECT g, 'row ' || g FROM pg_catalog.generate_series(1, 40000) AS g; -- over the bloat collectors' 1 MB floor
        CREATE INDEX pm6143_rows_label ON public.pm6143_rows (label);
        RESET ROLE;

        CREATE SCHEMA "Ext ""q""";
        CREATE EXTENSION pg_buffercache SCHEMA "Ext ""q""";
        CREATE EXTENSION pg_stat_statements SCHEMA "Ext ""q""";
        ANALYZE public.pm6143_rows;
        SELECT pg_catalog.count(*) FROM public.pm6143_rows;
        """";

    /// <summary>
    /// The text the same-named operators raise in the type-loading test. Npgsql's type-loading joins cannot hash or
    /// merge on these operators, so they run as nested loops and call the operator very many times. Raising on the
    /// first call ends that query at once on any machine, where a counting copy ran for as long as the machine took.
    /// </summary>
    private const string OperatorRanMarker = "pm6143 same-named operator ran";

    private const string RaisingOperatorsSql = $$"""
        CREATE OR REPLACE FUNCTION lookalike.int4_eq(integer, integer) RETURNS boolean LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION '{{OperatorRanMarker}}'; END $$;
        CREATE OR REPLACE FUNCTION lookalike.oid_eq(oid, oid) RETURNS boolean LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION '{{OperatorRanMarker}}'; END $$;
        """;

    [Theory]
    [InlineData("database")]
    [InlineData("role")]
    [InlineData("role in database")]
    public async Task CollectorsOnADatabaseWithItsOwnSearchPath_ReturnRows_AndNeverRunItsObjects(string setting)
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the search_path live test.");

        var ct = TestContext.Current.CancellationToken;
        var admin = new NpgsqlConnectionStringBuilder(adminConnectionString) { Pooling = false };
        var ownedAdmin = new NpgsqlConnectionStringBuilder(admin.ConnectionString) { Database = Database };
        /* No type loading: its catalog query compares oids, and on this unpinned connection each comparison runs the copy. */
        var ownedMonitor = new NpgsqlConnectionStringBuilder(ownedAdmin.ConnectionString) { Username = Monitor };
        ownedMonitor["Server Compatibility Mode"] = "NoTypeLoading";   /* the keyword, not the obsolete property (CS0618) */

        var bodySucceeded = false;
        try
        {
            await ResetAsync(admin.ConnectionString, ct);
            await ExecAsync(admin.ConnectionString, $"CREATE ROLE {Owner} LOGIN NOSUPERUSER", ct);
            await ExecAsync(admin.ConnectionString, $"CREATE ROLE {Monitor} LOGIN SUPERUSER", ct);
            await ExecAsync(admin.ConnectionString, $"CREATE DATABASE {Database} OWNER {Owner}", ct);
            await ExecAsync(ownedAdmin.ConnectionString, SetupSql, ct);

            const string Path = "lookalike, pg_catalog, public";
            await ExecAsync(
                setting == "database" ? new NpgsqlConnectionStringBuilder(ownedAdmin.ConnectionString) { Username = Owner }.ConnectionString : admin.ConnectionString,
                setting switch
                {
                    "database" => $"ALTER DATABASE {Database} SET search_path = {Path}",
                    "role" => $"ALTER ROLE {Monitor} SET search_path = {Path}",
                    _ => $"ALTER ROLE {Monitor} IN DATABASE {Database} SET search_path = {Path}",
                },
                ct);

            /* The fixture is live: on the monitoring role's connection without the pin, an unqualified = and
               current_database() run the copies. */
            await ExecAsync(ownedMonitor.ConnectionString, "SELECT 1 = 1, current_database()", ct);
            var unpinnedCalls = await ScalarLongAsync(ownedAdmin.ConnectionString, "SELECT pg_catalog.count(*) FROM lookalike.calls", ct);
            Assert.True(unpinnedCalls >= 2, $"the copies did not run on an unpinned connection ({unpinnedCalls} calls)");
            await ExecAsync(ownedAdmin.ConnectionString, "TRUNCATE lookalike.calls", ct);

            var runtime = await RuntimeAsync(admin, Database, Monitor, ct);
            Assert.Equal("pg_catalog,pg_temp", await ScalarTextAsync(runtime.ConnectionString, "SHOW search_path", ct));

            await using var store = NpgsqlDataSource.Create(adminConnectionString!);
            var runner = new DarlingCollectorRunner(
                store, new CollectorDeltaCalculator(), NullLogger<DarlingCollectorRunner>.Instance,
                databaseScope: (_, _) => new[] { Database });

            var counts = new Dictionary<string, int>(StringComparer.Ordinal)
            {
                /* Per-database collectors, read here from the configured database (the owned one). */
                ["pg_index_usage_stats"] = (await runner.FetchRowsAsync(PgIndexUsageStatsCollector.Instance, runtime, 60, ct)).Count,
                ["pg_autovacuum_stats"] = (await runner.FetchRowsAsync(PgAutovacuumStatsCollector.Instance, runtime, 60, ct)).Count,
                ["pg_table_bloat_stats"] = (await runner.FetchRowsAsync(PgTableBloatStatsCollector.Instance, runtime, 60, ct)).Count,
                ["pg_index_bloat"] = (await runner.FetchRowsAsync(PgIndexBloatCollector.Instance, runtime, 60, ct)).Count,
                ["pg_column_stats"] = (await runner.FetchRowsAsync(PgColumnStatsCollector.Instance, runtime, 60, ct)).Count,
                ["pg_extension_availability"] = (await runner.FetchRowsAsync(PgExtensionAvailabilityCollector.Instance, runtime, 60, ct)).Count,
                /* Server-wide collectors, read from the configured database. */
                ["pg_database_stats"] = (await runner.FetchRowsAsync(PgDatabaseStatsCollector.Instance, runtime, 60, ct)).Count,
                ["pg_database_size_stats"] = (await runner.FetchRowsAsync(PgDatabaseSizeStatsCollector.Instance, runtime, 60, ct)).Count,
                ["pg_wraparound_stats"] = (await runner.FetchRowsAsync(PgWraparoundStatsCollector.Instance, runtime, 60, ct)).Count,
                ["pg_server_config"] = (await runner.FetchRowsAsync(PgServerConfigCollector.Instance, runtime, 60, ct)).Count,
                /* Extensions created in a schema whose name needs quoting: named by that schema. */
                ["pg_buffer_usage"] = (await runner.FetchRowsAsync(PgBufferUsageCollector.Instance, runtime, 60, ct)).Count,
                ["pg_statement_stats"] = (await runner.FetchRowsAsync(PgStatementStatsCollector.Instance, runtime, 60, ct)).Count,
                /* The scheduled path: per database (the query rebuilt on each database's connection), then server-scoped. */
                ["run: pg_index_usage_stats"] = (await runner.RunAsync(PgIndexUsageStatsCollector.Instance, runtime, ct)).Rows,
                ["run: pg_buffer_usage"] = (await runner.RunAsync(PgBufferUsageCollector.Instance, runtime, ct)).Rows,
            };

            foreach (var (collector, count) in counts)
            {
                Assert.True(count > 0, $"{collector} returned no rows");
            }

            /* pg_qualstats is not created here, and public holds a same-named function. The host reads the extension
               list, finds it missing and records the missing extension without sending the query (#6888): 42P01 is the
               fault it raises. A 42883 here would be the query's own failure, so the gate was skipped. The database is
               skipped; the function in public never runs. */
            PostgresException? qualstatsFailure = null;
            try
            {
                var qualstats = await runner.RunAsync(PgPredicateStatsCollector.Instance, runtime, ct);
                Assert.Equal(0, qualstats.Rows);
            }
            catch (PostgresException ex)
            {
                qualstatsFailure = ex;
            }

            Assert.True(qualstatsFailure is null || qualstatsFailure.SqlState == PostgresErrorCodes.UndefinedTable,
                $"pg_predicate_stats failed with {qualstatsFailure?.SqlState}: {qualstatsFailure?.MessageText}");
            Assert.Equal(0L, await ScalarLongAsync(ownedAdmin.ConnectionString, "SELECT pg_catalog.count(*) FROM lookalike.calls", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(admin.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                foreach (var table in new[] { PgIndexUsageStatsCollector.Instance.TargetTable, PgBufferUsageCollector.Instance.TargetTable })
                {
                    await using var rows = new NpgsqlCommand($"DELETE FROM collect.{table} WHERE server_id = {ServerId}", cleanup);
                    await rows.ExecuteNonQueryAsync(cleanupCt);
                }

                await DropAsync(admin.ConnectionString, cleanup, cleanupCt);
            });
        }
    }

    /// <summary>
    /// H2: the hypothetical-index experiment on a pinned connection. The database's search_path puts a schema first
    /// that holds same-named set_config, current_setting, <c>||</c> and hypopg functions; none of them runs, and the
    /// application statement still resolves its unqualified table through the database's own search_path.
    /// <para>hypopg is not in every runtime. Without it the experiment fails at <c>pg_catalog.hypopg_create_index</c>
    /// with 42883, which it only reaches after the first EXPLAIN planned the statement.</para>
    /// </summary>
    [Theory]
    [InlineData("database")]
    [InlineData("role in database")]
    public async Task HypotheticalIndexExperiment_OnAPinnedConnection_PlansTheApplicationTable_AndNeverRunsSameNamedFunctions(string setting)
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the search_path live test.");

        var ct = TestContext.Current.CancellationToken;
        var admin = new NpgsqlConnectionStringBuilder(adminConnectionString) { Pooling = false };
        var hypoAdmin = new NpgsqlConnectionStringBuilder(admin.ConnectionString) { Database = HypoDatabase };

        var bodySucceeded = false;
        try
        {
            await ResetAsync(admin.ConnectionString, ct);
            await ExecAsync(admin.ConnectionString, $"CREATE ROLE {Monitor} LOGIN SUPERUSER", ct);
            await ExecAsync(admin.ConnectionString, $"CREATE DATABASE {HypoDatabase}", ct);
            var major = (int)(await ScalarLongAsync(hypoAdmin.ConnectionString, "SELECT pg_catalog.current_setting('server_version_num')::int / 10000", ct));
            Assert.SkipWhen(major < HypotheticalIndexExperiment.MinimumPostgresMajorForGenericPlan, "EXPLAIN (GENERIC_PLAN) needs PostgreSQL 16.");

            var hypopg = await ScalarLongAsync(hypoAdmin.ConnectionString,
                "SELECT pg_catalog.count(*) FROM pg_catalog.pg_available_extensions WHERE name = 'hypopg'", ct) > 0;

            /* A sequence counts the calls: nextval is not rolled back with the experiment's transaction. */
            await ExecAsync(hypoAdmin.ConnectionString, $"""
                CREATE SCHEMA shadow;
                CREATE SEQUENCE shadow.calls;
                CREATE FUNCTION shadow.set_config(text, text, boolean) RETURNS text LANGUAGE plpgsql AS $$
                BEGIN PERFORM pg_catalog.nextval('shadow.calls'); RETURN pg_catalog.set_config($1, $2, $3); END $$;
                CREATE FUNCTION shadow.current_setting(text) RETURNS text LANGUAGE plpgsql AS $$
                BEGIN PERFORM pg_catalog.nextval('shadow.calls'); RETURN pg_catalog.current_setting($1); END $$;
                CREATE FUNCTION shadow.current_setting(text, boolean) RETURNS text LANGUAGE plpgsql AS $$
                BEGIN PERFORM pg_catalog.nextval('shadow.calls'); RETURN pg_catalog.current_setting($1, $2); END $$;
                CREATE FUNCTION shadow.cat(text, text) RETURNS text LANGUAGE plpgsql AS $$
                BEGIN PERFORM pg_catalog.nextval('shadow.calls'); RETURN pg_catalog.textcat($1, $2); END $$;
                CREATE OPERATOR shadow.|| (LEFTARG = text, RIGHTARG = text, FUNCTION = shadow.cat);
                CREATE FUNCTION shadow.hypopg_create_index(text, OUT indexrelid oid, OUT indexname text) RETURNS SETOF record LANGUAGE plpgsql AS $$
                BEGIN PERFORM pg_catalog.nextval('shadow.calls'); RETURN; END $$;
                CREATE FUNCTION shadow.hypopg_reset() RETURNS void LANGUAGE plpgsql AS $$
                BEGIN PERFORM pg_catalog.nextval('shadow.calls'); END $$;
                CREATE SCHEMA app;
                CREATE TABLE app.orders (id integer PRIMARY KEY, amount numeric NOT NULL);
                INSERT INTO app.orders SELECT g, g FROM pg_catalog.generate_series(1, 5000) AS g;
                ANALYZE app.orders;
                {(hypopg ? $"CREATE SCHEMA {ExtSchema}; CREATE EXTENSION hypopg SCHEMA {ExtSchema};" : string.Empty)}
                """, ct);

            const string Path = "shadow, pg_catalog, app";
            await ExecAsync(admin.ConnectionString,
                setting == "database"
                    ? $"ALTER DATABASE {HypoDatabase} SET search_path = {Path}"
                    : $"ALTER ROLE {Monitor} IN DATABASE {HypoDatabase} SET search_path = {Path}",
                ct);

            /* The fixture is live: without the pin, an unqualified current_setting runs the copy. */
            var unpinned = new NpgsqlConnectionStringBuilder(hypoAdmin.ConnectionString) { Username = Monitor }.ConnectionString;
            await ExecAsync(unpinned, "SELECT current_setting('search_path')", ct);
            Assert.True(await CallsAsync(hypoAdmin.ConnectionString, ct) > 0, "the copies did not run on an unpinned connection");
            await ExecAsync(hypoAdmin.ConnectionString, "SELECT pg_catalog.setval('shadow.calls', 1, false)", ct);

            var runtime = await RuntimeAsync(admin, HypoDatabase, Monitor, ct);
            await using var connection = new NpgsqlConnection(runtime.ConnectionString);
            await connection.OpenAsync(ct);

            HypotheticalIndexExperiment.Result? result = null;
            PostgresException? failure = null;
            try
            {
                result = await HypotheticalIndexExperiment.RunAsync(
                    connection, "SELECT amount FROM orders WHERE id = $1", "CREATE INDEX ON app.orders (amount)", major, ct);
            }
            catch (PostgresException ex)
            {
                failure = ex;
            }

            if (hypopg)
            {
                Assert.Null(failure);
                Assert.Contains("orders", result.GetValueOrDefault().PlanBeforeJson, StringComparison.Ordinal);
            }
            else
            {
                /* Reached only after the first EXPLAIN planned the unqualified table: a failure to resolve it is 42P01. */
                Assert.NotNull(failure);
                Assert.Equal(PostgresErrorCodes.UndefinedFunction, failure!.SqlState);
                Assert.Contains("pg_catalog.hypopg_create_index", failure.MessageText, StringComparison.Ordinal);
            }

            Assert.Equal(0L, await CallsAsync(hypoAdmin.ConnectionString, ct));

            /* The pin is back once the experiment is over. */
            await using (var show = new NpgsqlCommand("SHOW search_path", connection))
            {
                Assert.Equal("pg_catalog,pg_temp", (string?)await show.ExecuteScalarAsync(ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(admin.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await DropAsync(admin.ConnectionString, cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// M3: an Aurora function is called by its schema in pg_catalog or in pg_stat_statements' schema, never by a
    /// same-named function in any other schema, wherever the search_path puts that schema.
    /// </summary>
    [Fact]
    public async Task AuroraFunctionSchemas_AcceptOnlyPgCatalogAndTheStatementsExtensionSchema()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the search_path live test.");

        var ct = TestContext.Current.CancellationToken;
        var admin = new NpgsqlConnectionStringBuilder(adminConnectionString) { Pooling = false };
        var auroraAdmin = new NpgsqlConnectionStringBuilder(admin.ConnectionString) { Database = AuroraDatabase };

        var bodySucceeded = false;
        try
        {
            await ResetAsync(admin.ConnectionString, ct);
            await ExecAsync(admin.ConnectionString, $"CREATE DATABASE {AuroraDatabase}", ct);
            await ExecAsync(auroraAdmin.ConnectionString, $"""
                CREATE SCHEMA other;
                CREATE SCHEMA {ExtSchema};
                CREATE EXTENSION pg_stat_statements SCHEMA {ExtSchema};
                CREATE FUNCTION other.aurora_stat_system_waits() RETURNS SETOF record LANGUAGE sql AS 'SELECT NULL::record WHERE false';
                CREATE FUNCTION other.aurora_stat_wait_type() RETURNS SETOF record LANGUAGE sql AS 'SELECT NULL::record WHERE false';
                CREATE FUNCTION {ExtSchema}.aurora_stat_wait_type() RETURNS SETOF record LANGUAGE sql AS 'SELECT NULL::record WHERE false';
                CREATE FUNCTION other.aurora_stat_wait_event() RETURNS SETOF record LANGUAGE sql AS 'SELECT NULL::record WHERE false';
                CREATE FUNCTION {ExtSchema}.aurora_stat_wait_event() RETURNS SETOF record LANGUAGE sql AS 'SELECT NULL::record WHERE false';
                CREATE FUNCTION pg_catalog.aurora_stat_wait_event() RETURNS SETOF record LANGUAGE sql AS 'SELECT NULL::record WHERE false';
                CREATE FUNCTION other.aurora_version() RETURNS text LANGUAGE sql AS 'SELECT ''other''';
                ALTER DATABASE {AuroraDatabase} SET search_path = other, pg_catalog, public;
                """, ct);

            var runtime = await RuntimeAsync(admin, AuroraDatabase, admin.Username!, ct);
            await using var connection = new NpgsqlConnection(runtime.ConnectionString);
            await connection.OpenAsync(ct);

            var context = new CollectorContext
            {
                ServerId = ServerId,
                ServerName = "pm6143-target",
                CollectionTime = LiveClock.Now(),
                Deltas = new CollectorDeltaCalculator(),
                Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17, IsAurora = true },
            };
            Assert.True(await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(
                context, PgWaitStatsCollector.Instance, connection, null, ct));

            /* Only in other: not found, so pg_catalog. In other and the extension's schema: the extension does not ship
               it and it is not a member there, so pg_catalog too. In all three: pg_catalog. */
            var sql = PgWaitStatsCollector.Instance.BuildQuery(context).Text;
            Assert.Contains("FROM pg_catalog.aurora_stat_system_waits()", sql, StringComparison.Ordinal);
            Assert.Contains("JOIN pg_catalog.aurora_stat_wait_type()", sql, StringComparison.Ordinal);
            Assert.DoesNotContain(ExtSchema + ".", sql, StringComparison.Ordinal);
            Assert.Contains("JOIN \"pg_catalog\".aurora_stat_wait_event()", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("other.", sql, StringComparison.Ordinal);

            /* The connect-time probe: aurora_version exists only in other, so it is called in pg_catalog. */
            Assert.Equal("pg_catalog", await DarlingServerConnector.ResolveAuroraFunctionSchemaAsync(connection, "aurora_version", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(admin.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await DropAsync(admin.ConnectionString, cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// A stock PostgreSQL target where pg_stat_statements is created in public and someone with CREATE there made
    /// IMMUTABLE public.aurora_version() and public.aurora_stat_statements(boolean). PostgreSQL evaluates an IMMUTABLE
    /// call with constant arguments while it plans, so planning such a call (EXPLAIN) runs it. Neither the connect-time
    /// Aurora probe nor a collector cycle ever runs them, and the target is not Aurora. Once
    /// aurora_stat_statements(boolean) is a member of the extension it is found there without being run, while
    /// aurora_version, which the extension does not ship, still resolves in pg_catalog or nowhere.
    /// </summary>
    [Fact]
    public async Task AuroraNamedFunctionsInTheExtensionSchema_ThatAreNotMembers_AreNeverPlannedOrRun()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the search_path live test.");

        var ct = TestContext.Current.CancellationToken;
        var admin = new NpgsqlConnectionStringBuilder(adminConnectionString) { Pooling = false };
        var auroraAdmin = new NpgsqlConnectionStringBuilder(admin.ConnectionString) { Database = AuroraDatabase };

        var bodySucceeded = false;
        try
        {
            await ResetAsync(admin.ConnectionString, ct);
            await ExecAsync(admin.ConnectionString, $"CREATE DATABASE {AuroraDatabase}", ct);
            /* A sequence counts each call: nextval is allowed in an IMMUTABLE function and is not undone by a rollback. */
            await ExecAsync(auroraAdmin.ConnectionString, $"""
                CREATE EXTENSION pg_stat_statements SCHEMA public;
                CREATE SEQUENCE public.version_calls;
                CREATE SEQUENCE public.statements_calls;
                CREATE FUNCTION public.aurora_version() RETURNS text IMMUTABLE LANGUAGE plpgsql AS $$
                BEGIN PERFORM pg_catalog.nextval('public.version_calls'); RETURN 'x'; END $$;
                CREATE FUNCTION public.aurora_stat_statements(boolean) RETURNS text IMMUTABLE LANGUAGE plpgsql AS $$
                BEGIN PERFORM pg_catalog.nextval('public.statements_calls'); RETURN 'x'; END $$;
                ALTER DATABASE {AuroraDatabase} SET search_path = public, pg_catalog;
                """, ct);

            /* The premise: planning the call runs it. */
            await ExecAsync(auroraAdmin.ConnectionString, "EXPLAIN (COSTS OFF) SELECT * FROM public.aurora_version()", ct);
            Assert.Equal(1, await CountAsync(auroraAdmin.ConnectionString, "public.version_calls", ct));
            await ExecAsync(auroraAdmin.ConnectionString, "SELECT pg_catalog.setval('public.version_calls', 1, false)", ct);

            var runtime = await RuntimeAsync(admin, AuroraDatabase, admin.Username!, ct);
            await using var connection = new NpgsqlConnection(runtime.ConnectionString);
            await connection.OpenAsync(ct);

            /* The connect-time probe: not Aurora. */
            Assert.False(await DarlingServerConnector.ProbeAuroraAsync(connection, ct));
            Assert.Equal("pg_catalog", await DarlingServerConnector.ResolveAuroraFunctionSchemaAsync(connection, "aurora_version", ct));

            /* A collector cycle that reads Aurora functions (the host gates it on IsAurora; forced here so the lookup
               runs): nothing outside pg_catalog is chosen. */
            var aurora = NewContext(new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = connection.PostgreSqlVersion.Major, IsAurora = true,
            });
            Assert.True(await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(
                aurora, PgStatementStatsCollector.Instance, connection, null, ct));
            Assert.Empty(aurora.PgFunctionSchemas!);
            Assert.Contains("FROM pg_catalog.aurora_stat_statements(false)", PgStatementStatsCollector.Instance.BuildQuery(aurora).Text, StringComparison.Ordinal);
            Assert.True(await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(
                aurora, PgWaitStatsCollector.Instance, connection, null, ct));
            Assert.Empty(aurora.PgFunctionSchemas!);

            /* The cycle a stock target really gets: the statement collector reads pg_stat_statements. */
            var stock = NewContext(new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = connection.PostgreSqlVersion.Major });
            Assert.True(await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(
                stock, PgStatementStatsCollector.Instance, connection, null, ct));
            await using (var command = new NpgsqlCommand(PgStatementStatsCollector.Instance.BuildQuery(stock).Text, connection))
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                await PgStatementStatsCollector.Instance.ReadAsync(reader, stock, ct);
            }

            Assert.Equal(0, await CountAsync(auroraAdmin.ConnectionString, "public.version_calls", ct));
            Assert.Equal(0, await CountAsync(auroraAdmin.ConnectionString, "public.statements_calls", ct));

            /* Members of the extension: aurora_stat_statements(boolean) is found in the extension's schema by the catalog
               read, without being planned or run; aurora_version still is not. */
            await ExecAsync(auroraAdmin.ConnectionString, """
                ALTER EXTENSION pg_stat_statements ADD FUNCTION public.aurora_stat_statements(boolean);
                ALTER EXTENSION pg_stat_statements ADD FUNCTION public.aurora_version();
                """, ct);
            Assert.True(await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(
                aurora, PgStatementStatsCollector.Instance, connection, null, ct));
            Assert.Equal("public", aurora.PgFunctionSchemas!["aurora_stat_statements"]);
            Assert.False(aurora.PgFunctionSchemas.ContainsKey("aurora_version"));
            Assert.False(await DarlingServerConnector.ProbeAuroraAsync(connection, ct));

            Assert.Equal(0, await CountAsync(auroraAdmin.ConnectionString, "public.version_calls", ct));
            Assert.Equal(0, await CountAsync(auroraAdmin.ConnectionString, "public.statements_calls", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(admin.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await DropAsync(admin.ConnectionString, cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// pg_stat_statements below 1.9 has no pg_stat_statements_info view, so another role with CREATE on the extension's
    /// schema can create a view of that name there. The statements epoch and eviction reads give NULL for it and never
    /// read it: the function under the view never runs. Once the extension's own view exists (1.9), both are read.
    /// </summary>
    [Fact]
    public async Task StatementsInfoView_ThatIsNotAMemberOfTheExtension_IsNeverRead()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the search_path live test.");

        var ct = TestContext.Current.CancellationToken;
        var admin = new NpgsqlConnectionStringBuilder(adminConnectionString) { Pooling = false };
        var infoAdmin = new NpgsqlConnectionStringBuilder(admin.ConnectionString) { Database = InfoDatabase };
        var epochAndDealloc = $"SELECT {PgStatementStatsCollector.StatementsEpochSql} AS epoch, {PgStatementStatsCollector.StatementsDeallocSql} AS dealloc";

        var bodySucceeded = false;
        try
        {
            await ResetAsync(admin.ConnectionString, ct);
            await ExecAsync(admin.ConnectionString, $"CREATE ROLE {Owner} LOGIN NOSUPERUSER", ct);
            await ExecAsync(admin.ConnectionString, $"CREATE DATABASE {InfoDatabase}", ct);
            await ExecAsync(infoAdmin.ConnectionString, $"""
                CREATE SCHEMA stmts;
                CREATE EXTENSION pg_stat_statements SCHEMA stmts VERSION '1.8';
                GRANT USAGE, CREATE ON SCHEMA stmts TO {Owner};
                SET ROLE {Owner};
                CREATE SEQUENCE stmts.info_calls;
                GRANT USAGE ON SEQUENCE stmts.info_calls TO PUBLIC;
                CREATE FUNCTION stmts.info_row() RETURNS TABLE (dealloc bigint, stats_reset timestamptz) LANGUAGE plpgsql AS $$
                BEGIN PERFORM pg_catalog.nextval('stmts.info_calls'); RETURN QUERY SELECT 7::bigint, pg_catalog.now(); END $$;
                CREATE VIEW stmts.pg_stat_statements_info AS SELECT * FROM stmts.info_row();
                RESET ROLE;
                """, ct);

            /* The fixture is live: reading the view runs the function. */
            await ExecAsync(infoAdmin.ConnectionString, "SELECT * FROM stmts.pg_stat_statements_info", ct);
            Assert.Equal(1, await CountAsync(infoAdmin.ConnectionString, "stmts.info_calls", ct));
            await ExecAsync(infoAdmin.ConnectionString, "SELECT pg_catalog.setval('stmts.info_calls', 1, false)", ct);

            var runtime = await RuntimeAsync(admin, InfoDatabase, admin.Username!, ct);
            await using (var connection = new NpgsqlConnection(runtime.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await using var command = new NpgsqlCommand(epochAndDealloc, connection);
                await using var reader = await command.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.True(reader.IsDBNull(0));
                Assert.True(reader.IsDBNull(1));
            }

            Assert.Equal(0, await CountAsync(infoAdmin.ConnectionString, "stmts.info_calls", ct));

            /* The extension's own view, at 1.9, is a member: both are read. */
            await ExecAsync(infoAdmin.ConnectionString, """
                DROP VIEW stmts.pg_stat_statements_info;
                ALTER EXTENSION pg_stat_statements UPDATE TO '1.9';
                """, ct);
            await using (var connection = new NpgsqlConnection(runtime.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await using var command = new NpgsqlCommand(epochAndDealloc, connection);
                await using var reader = await command.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.False(reader.IsDBNull(0));
                Assert.False(reader.IsDBNull(1));
            }

            Assert.Equal(0, await CountAsync(infoAdmin.ConnectionString, "stmts.info_calls", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(admin.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await DropAsync(admin.ConnectionString, cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The hourly statement-text fetch reads pg_stat_statements from the schema the extension was created in. With the
    /// extension in its own schema and a view named pg_stat_statements in public, made by a role with CREATE there, the
    /// fetch never reads the view: the function under it never runs.
    /// </summary>
    [Fact]
    public async Task StatementTextFetch_ReadsTheExtensionSchema_NeverASameNamedViewInPublic()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the search_path live test.");

        var ct = TestContext.Current.CancellationToken;
        var admin = new NpgsqlConnectionStringBuilder(adminConnectionString) { Pooling = false };
        var textAdmin = new NpgsqlConnectionStringBuilder(admin.ConnectionString) { Database = TextDatabase };

        var bodySucceeded = false;
        try
        {
            await ResetAsync(admin.ConnectionString, ct);
            await ExecAsync(admin.ConnectionString, $"CREATE ROLE {Owner} LOGIN NOSUPERUSER", ct);
            await ExecAsync(admin.ConnectionString, $"CREATE DATABASE {TextDatabase}", ct);
            await ExecAsync(textAdmin.ConnectionString, $"""
                CREATE SCHEMA stmts;
                CREATE EXTENSION pg_stat_statements SCHEMA stmts;
                GRANT USAGE, CREATE ON SCHEMA public TO {Owner};
                SET ROLE {Owner};
                CREATE SEQUENCE public.view_calls;
                GRANT USAGE ON SEQUENCE public.view_calls TO PUBLIC;
                CREATE FUNCTION public.statement_rows()
                RETURNS TABLE (queryid bigint, query text, total_exec_time float8, toplevel boolean) LANGUAGE plpgsql AS $$
                BEGIN PERFORM pg_catalog.nextval('public.view_calls'); RETURN QUERY SELECT 6143::bigint, 'SELECT 6143'::text, 1::float8, true; END $$;
                CREATE VIEW public.pg_stat_statements AS SELECT * FROM public.statement_rows();
                RESET ROLE;
                SELECT pg_catalog.count(*) FROM pg_catalog.pg_class;
                """, ct);

            /* The fixture is live: reading the view runs the function. */
            await ExecAsync(textAdmin.ConnectionString, "SELECT * FROM public.pg_stat_statements", ct);
            Assert.Equal(1, await CountAsync(textAdmin.ConnectionString, "public.view_calls", ct));
            await ExecAsync(textAdmin.ConnectionString, "SELECT pg_catalog.setval('public.view_calls', 1, false)", ct);

            var runtime = await RuntimeAsync(admin, TextDatabase, admin.Username!, ct);
            var (queryIds, _) = await DarlingWorker.ReadPgStatementTextAsync(runtime, ct);

            Assert.DoesNotContain(6143L, queryIds);
            Assert.NotEmpty(queryIds);
            Assert.Equal(0, await CountAsync(textAdmin.ConnectionString, "public.view_calls", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(admin.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await DropAsync(admin.ConnectionString, cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// A session that lost the pin on the way in, on a database whose own search_path puts another role's schema
    /// first. The connect-time detection refuses it, and nothing but the detection query ran on it first: Npgsql's
    /// type-loading query compares oids, and the = in that schema would have run.
    /// </summary>
    [Fact]
    public async Task ConnectWithoutThePin_RunsNothingBeforeTheCheck()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the search_path live test.");

        var ct = TestContext.Current.CancellationToken;
        var admin = new NpgsqlConnectionStringBuilder(adminConnectionString) { Pooling = false };
        var ownedAdmin = new NpgsqlConnectionStringBuilder(admin.ConnectionString) { Database = Database };

        var bodySucceeded = false;
        try
        {
            await ResetAsync(admin.ConnectionString, ct);
            await ExecAsync(admin.ConnectionString, $"CREATE ROLE {Owner} LOGIN NOSUPERUSER", ct);
            await ExecAsync(admin.ConnectionString, $"CREATE ROLE {Monitor} LOGIN SUPERUSER", ct);
            await ExecAsync(admin.ConnectionString, $"CREATE DATABASE {Database} OWNER {Owner}", ct);
            await ExecAsync(ownedAdmin.ConnectionString, SetupSql, ct);
            await ExecAsync(
                new NpgsqlConnectionStringBuilder(ownedAdmin.ConnectionString) { Username = Owner }.ConnectionString,
                $"ALTER DATABASE {Database} SET search_path = lookalike, pg_catalog, public", ct);

            var runtime = await RuntimeAsync(admin, Database, Monitor, ct);
            var stripped = new NpgsqlConnectionStringBuilder(runtime.ConnectionString) { Options = null }.ConnectionString;

            var refused = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                DarlingServerConnector.ConnectPostgresAsync(runtime.Config, stripped, null, ct));
            Assert.StartsWith("precondition:", refused.Message, StringComparison.Ordinal);
            Assert.Contains("lookalike", refused.Message, StringComparison.Ordinal);
            Assert.Equal(0L, await ScalarLongAsync(ownedAdmin.ConnectionString, "SELECT pg_catalog.count(*) FROM lookalike.calls", ct));

            /* With the pin the same database connects, and nothing of that schema ran either. */
            var connected = await DarlingServerConnector.ConnectPostgresAsync(runtime.Config, runtime.ConnectionString, null, ct);
            Assert.Equal(Database, connected.ConnectedDatabase);
            Assert.Equal(0L, await ScalarLongAsync(ownedAdmin.ConnectionString, "SELECT pg_catalog.count(*) FROM lookalike.calls", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(admin.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await DropAsync(admin.ConnectionString, cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// Every new physical connection a monitoring connection string opens reads its search_path before any caller's
    /// statement. Npgsql's own Search Path keyword is applied after the startup options and replaces the pin, which
    /// stands in for a session that lost it: that session is refused. The pinned string opens.
    /// </summary>
    [Fact]
    public async Task PhysicalConnection_WithoutThePin_IsRefusedAtOpen()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the search_path live test.");

        var ct = TestContext.Current.CancellationToken;
        var admin = new NpgsqlConnectionStringBuilder(adminConnectionString) { Pooling = false };
        var runtime = await RuntimeAsync(admin, admin.Database!, admin.Username!, ct);

        await using (var pinned = PostgresTargetProvider.CreatePinnedConnection(runtime.ConnectionString))
        {
            await pinned.OpenAsync(ct);
            await using var command = new NpgsqlCommand("SHOW search_path", pinned);
            Assert.Equal("pg_catalog,pg_temp", (string?)await command.ExecuteScalarAsync(ct));
        }

        var replaced = new NpgsqlConnectionStringBuilder(runtime.ConnectionString) { SearchPath = "public" }.ConnectionString;
        await using var unpinned = PostgresTargetProvider.CreatePinnedConnection(replaced);
        var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => unpinned.OpenAsync(ct));
        Assert.StartsWith("precondition:", refused.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The first statement on a target connection is the caller's own. The database's search_path puts a schema first
    /// that holds a same-named <c>=</c> for oid and integer, which is what Npgsql's type-loading query would run on the
    /// first connection of a data source. A string without the pin (so the server's own setting applies) and the
    /// service's pinned string both open and answer, and none of that schema runs.
    /// </summary>
    [Fact]
    public async Task TargetConnection_FirstStatementIsTheCallers_AndNoSameNamedOperatorRuns()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the search_path live test.");

        var ct = TestContext.Current.CancellationToken;
        var admin = new NpgsqlConnectionStringBuilder(adminConnectionString) { Pooling = false };
        var ownedAdmin = new NpgsqlConnectionStringBuilder(admin.ConnectionString) { Database = Database };

        var bodySucceeded = false;
        try
        {
            await ResetAsync(admin.ConnectionString, ct);
            await ExecAsync(admin.ConnectionString, $"CREATE ROLE {Owner} LOGIN NOSUPERUSER", ct);
            await ExecAsync(admin.ConnectionString, $"CREATE ROLE {Monitor} LOGIN SUPERUSER", ct);
            await ExecAsync(admin.ConnectionString, $"CREATE DATABASE {Database} OWNER {Owner}", ct);
            await ExecAsync(ownedAdmin.ConnectionString, SetupSql, ct);
            await ExecAsync(ownedAdmin.ConnectionString, RaisingOperatorsSql, ct);
            await ExecAsync(
                new NpgsqlConnectionStringBuilder(ownedAdmin.ConnectionString) { Username = Owner }.ConnectionString,
                $"ALTER DATABASE {Database} SET search_path = lookalike, pg_catalog, public", ct);

            var runtime = await RuntimeAsync(admin, Database, Monitor, ct);
            /* Both data sources are cached by connection string for the life of the process (ClearPools empties their
               pools, not the cache), so a fresh GUID in each string makes every run build new ones and open a first
               physical connection. */
            var runId = Guid.NewGuid().ToString("N");

            /* Control: a plain, unpinned connection with Npgsql's type loading on. Its pg_type read runs under the
               database's search_path and reaches the same-named operator, which raises at once. The open failing with
               that text proves this setup catches type loading. */
            var controlString = new NpgsqlConnectionStringBuilder(runtime.ConnectionString) { Options = null, ApplicationName = $"pm6143-control-{runId}" }.ConnectionString;
            var control = await Assert.ThrowsAnyAsync<NpgsqlException>(async () =>
            {
                await using var plain = new NpgsqlConnection(controlString);
                await plain.OpenAsync(ct);
            });
            Assert.Contains(OperatorRanMarker, control.Message, StringComparison.Ordinal);

            var withoutPin = new NpgsqlConnectionStringBuilder(runtime.ConnectionString) { Options = null, ApplicationName = $"pm6143-first-statement-{runId}" }.ConnectionString;

            await using (var unpinned = PostgresTargetProvider.CreatePinnedConnection(withoutPin))
            {
                await unpinned.OpenAsync(ct);
                await using var command = new NpgsqlCommand("SHOW search_path", unpinned);
                Assert.Contains("lookalike", (string?)await command.ExecuteScalarAsync(ct), StringComparison.Ordinal);
            }


            var pinnedString = new NpgsqlConnectionStringBuilder(runtime.ConnectionString) { ApplicationName = $"pm6143-first-statement-pinned-{runId}" }.ConnectionString;
            await using (var pinned = PostgresTargetProvider.CreatePinnedConnection(pinnedString))
            {
                await pinned.OpenAsync(ct);
                await using var command = new NpgsqlCommand("SHOW search_path", pinned);
                Assert.Equal("pg_catalog,pg_temp", (string?)await command.ExecuteScalarAsync(ct));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(admin.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await DropAsync(admin.ConnectionString, cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The search_path check ignores only the whitespace PostgreSQL skips between schema names. A no-break space or
    /// another Unicode space is part of a schema name to PostgreSQL, so a search_path holding one is refused. What the
    /// server reported reaches the message without control characters and capped in length.
    /// </summary>
    [Theory]
    [InlineData("pg_catalog,pg_temp", true)]
    [InlineData("pg_catalog, pg_temp", true)]
    [InlineData(" pg_catalog,\tpg_temp\r\n\f", true)]
    [InlineData("pg_catalog,\u00A0pg_temp", false)]
    [InlineData("\u2003pg_catalog,pg_temp", false)]
    [InlineData("pg_catalog,pg_temp\u3000", false)]
    [InlineData("pg_catalog\u2009,pg_temp", false)]
    [InlineData("pg_catalog,\u200Bpg_temp", false)]
    [InlineData("pg_catalog,pg_temp,public", false)]
    public void SearchPathCheck_IgnoresOnlyPostgresWhitespace(string observed, bool accepted)
    {
        if (accepted)
        {
            DarlingServerConnector.EnsureSearchPathPinned(observed, "pm6143-target");
            return;
        }

        var refused = Assert.Throws<InvalidOperationException>(() => DarlingServerConnector.EnsureSearchPathPinned(observed, "pm6143-target"));
        Assert.StartsWith("precondition:", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ServerTextInMessages_HasNoControlCharacters_AndIsCapped()
    {
        var observed = "lookalike\r\n2026-10-19 [ERROR] another line\u0007" + new string('x', 5000);
        var refused = Assert.Throws<InvalidOperationException>(() => DarlingServerConnector.EnsureSearchPathPinned(observed, "pm6143-target"));
        Assert.DoesNotContain(refused.Message, char.IsControl);
        Assert.True(refused.Message.Length < 1000, $"message is {refused.Message.Length} characters");

        var pooler = DarlingServerConnector.PoolerPrecondition(
            "pm6143-target",
            "it refused the sign-in: " + DarlingServerConnector.CleanServerText("bad\nparameter\u001b[31m" + new string('y', 5000), DarlingServerConnector.MaxPoolerTextLength));
        Assert.DoesNotContain(pooler.Message, char.IsControl);
        Assert.True(pooler.Message.Length < 1000, $"message is {pooler.Message.Length} characters");
    }

    private static async Task<long> CountAsync(string connectionString, string sequence, CancellationToken ct) =>
        await ScalarLongAsync(connectionString, $"SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM {sequence}", ct);

    /// <summary>
    /// PgBouncer refuses the startup 'options' parameter with 08P01 "unsupported startup parameter: options". That
    /// error at sign-in becomes a precondition that asks for a direct connection; other sign-in errors pass through.
    /// </summary>
    [Fact]
    public void PoolerRejectionAtSignIn_BecomesAPreconditionThatAsksForADirectConnection()
    {
        var pgBouncer = new PostgresException("unsupported startup parameter: options", "FATAL", "FATAL", "08P01");
        Assert.True(DarlingServerConnector.IsStartupParameterRejection(pgBouncer));
        Assert.False(DarlingServerConnector.IsStartupParameterRejection(
            new PostgresException("invalid length of startup packet", "FATAL", "FATAL", "08P01")));
        Assert.False(DarlingServerConnector.IsStartupParameterRejection(
            new PostgresException("password authentication failed for user \"m\"", "FATAL", "FATAL", "28P01")));

        var message = DarlingServerConnector.PoolerPrecondition("pm6143-target", "it refused the sign-in: " + pgBouncer.MessageText, pgBouncer).Message;
        Assert.StartsWith("precondition:", message, StringComparison.Ordinal);
        Assert.Contains("unsupported startup parameter: options", message, StringComparison.Ordinal);
        Assert.Contains("direct connection", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// M2: a session that lost the pin on the way in (a pooler that drops the startup option) is refused with a
    /// precondition that names the pooler setting; the pinned session passes.
    /// </summary>
    [Fact]
    public async Task DetectionQuery_RefusesASessionWithoutThePin()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the search_path live test.");

        var ct = TestContext.Current.CancellationToken;
        var admin = new NpgsqlConnectionStringBuilder(adminConnectionString) { Pooling = false };
        var runtime = await RuntimeAsync(admin, admin.Database!, admin.Username!, ct);
        var stripped = new NpgsqlConnectionStringBuilder(runtime.ConnectionString) { Options = null }.ConnectionString;

        DarlingServerConnector.EnsureSearchPathPinned(await DetectedSearchPathAsync(runtime.ConnectionString, ct), "pm6143-target");

        var refused = Assert.Throws<InvalidOperationException>(() =>
            DarlingServerConnector.EnsureSearchPathPinned(DetectedSearchPathAsync(stripped, ct).GetAwaiter().GetResult(), "pm6143-target"));
        Assert.StartsWith("precondition:", refused.Message, StringComparison.Ordinal);
        Assert.Contains("ignore_startup_parameters", refused.Message, StringComparison.Ordinal);
    }

    /// <summary>H1: when the extension lookup itself fails, the run fails with a message instead of guessing a schema.</summary>
    [Fact]
    public async Task ExtensionSchemaLookupFailure_FailsTheRun()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the search_path live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(adminConnectionString) { Pooling = false }.ConnectionString);
        await connection.OpenAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using (var fail = new NpgsqlCommand("SELECT 1 / 0", connection, transaction))
        {
            await Assert.ThrowsAsync<PostgresException>(() => fail.ExecuteScalarAsync(ct));
        }

        /* Every statement in an aborted transaction fails (25P02): the lookup cannot succeed. */
        var context = NewContext(new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17 });
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(context, PgBufferUsageCollector.Instance, connection, null, ct));
        Assert.Contains("pg_catalog.pg_extension", ex.Message, StringComparison.Ordinal);
        Assert.Null(context.PgExtensionSchemas);
    }

    /// <summary>
    /// Every extension and Aurora object the collectors name is named by the schema given, quoted, and pg_catalog when
    /// none was found; this also fails if a query's text no longer has the place the schema goes.
    /// </summary>
    [Fact]
    public void CollectorQueries_NameExtensionAndAuroraObjectsByTheirSchema()
    {
        var stock = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17, HasPgWaitSamplingExtension = true };
        var aurora = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17, IsAurora = true };
        var objects = new (ICollectorSchemaInfo Definition, CollectorTargetInfo Target, string Object)[]
        {
            (PgBufferUsageCollector.Instance, stock, ".pg_buffercache AS b"),
            (PgKernelStatsCollector.Instance, stock, ".pg_stat_kcache() AS k"),
            (PgPredicateStatsCollector.Instance, stock, ".pg_qualstats() AS q"),
            (PgWaitSamplingCollector.Instance, stock, ".pg_wait_sampling_profile AS p"),
            (PgStatementStatsCollector.Instance, stock, ".pg_stat_statements"),
            (PgStatementStatsCollector.Instance, aurora, ".aurora_stat_statements(false)"),
            (PgWaitStatsCollector.Instance, aurora, ".aurora_stat_system_waits()"),
            (PgWaitStatsCollector.Instance, aurora, ".aurora_stat_wait_type()"),
            (PgWaitStatsCollector.Instance, aurora, ".aurora_stat_wait_event()"),
        };

        var everything = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["pg_buffercache"] = "Ext \"q\"", ["pg_stat_kcache"] = "Ext \"q\"", ["pg_qualstats"] = "Ext \"q\"",
            ["pg_wait_sampling"] = "Ext \"q\"", ["pg_stat_statements"] = "Ext \"q\"",
        };
        /* Every function mapped to a schema that needs quoting, so each query shows where its schema goes. */
        var functions = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["aurora_stat_statements"] = "Ext \"q\"", ["aurora_stat_system_waits"] = "Ext \"q\"",
            ["aurora_stat_wait_type"] = "Ext \"q\"", ["aurora_stat_wait_event"] = "Ext \"q\"",
        };

        foreach (var (definition, target, name) in objects)
        {
            var found = NewContext(target);
            found.PgExtensionSchemas = everything;
            found.PgFunctionSchemas = functions;
            Assert.Contains(ExtSchema + name, Build(definition, found), StringComparison.Ordinal);

            var missing = NewContext(target);
            missing.PgExtensionSchemas = new Dictionary<string, string>(StringComparer.Ordinal);
            var sql = Build(definition, missing);
            Assert.Contains("pg_catalog" + name, sql, StringComparison.Ordinal);
            Assert.DoesNotContain("public" + name, sql, StringComparison.Ordinal);
        }

        /* A row from any schema other than pg_catalog and the statements extension's is never chosen. */
        Assert.Empty(CollectorContext.ChoosePgFunctionSchemas(new[] { ("aurora_version", "other") }, null));
        Assert.Equal("pg_catalog", CollectorContext.ChoosePgFunctionSchemas(
            new[] { ("aurora_version", "Ext \"q\""), ("aurora_version", "pg_catalog") }, "Ext \"q\"")["aurora_version"]);

        /* The extension's schema is accepted only for the function the extension ships. */
        var chosen = CollectorContext.ChoosePgFunctionSchemas(
            new[]
            {
                ("aurora_stat_statements", "Ext \"q\""), ("aurora_version", "Ext \"q\""),
                ("aurora_stat_system_waits", "Ext \"q\""), ("aurora_stat_wait_event", "other"),
            },
            "Ext \"q\"");
        Assert.Equal("Ext \"q\"", Assert.Single(chosen).Value);
        Assert.Equal("aurora_stat_statements", Assert.Single(chosen).Key);
    }

    [Fact]
    public void PinSearchPath_IsAlwaysTheLastOption_AndTheSearchPathKeywordIsCleared()
    {
        const string Pin = MonitoredServerConnection.PinnedSearchPathOption;
        Assert.Equal(Pin, MonitoredServerConnection.PinSearchPath(null));
        Assert.Equal("-c statement_timeout=5000 " + Pin, MonitoredServerConnection.PinSearchPath("-c statement_timeout=5000"));
        Assert.Equal(Pin, MonitoredServerConnection.PinSearchPath(Pin));
        Assert.Equal("-c a=b " + Pin, MonitoredServerConnection.PinSearchPath("-c a=b " + Pin));

        /* Containing the pin's text is not being the pin: these are overridden by a pin after them. */
        Assert.Equal("-c search_path=pg_catalog,pg_temp,public " + Pin,
            MonitoredServerConnection.PinSearchPath("-c search_path=pg_catalog,pg_temp,public"));
        Assert.Equal(Pin + " -c search_path=public " + Pin,
            MonitoredServerConnection.PinSearchPath(Pin + " -c search_path=public"));
        Assert.Equal("-c x=a\\ " + Pin + " " + Pin, MonitoredServerConnection.PinSearchPath("-c x=a\\ " + Pin));

        var monitoring = MonitoredServerConnection.BuildConnectionString(
            new MonitoredServer { Name = "pm6143", Host = "h", Database = "d", Engine = "postgres", Auth = "sql", Username = "u" }, "p");
        var builder = new NpgsqlConnectionStringBuilder(monitoring);
        Assert.Null(builder.SearchPath);
        Assert.EndsWith(Pin, builder.Options, StringComparison.Ordinal);
    }

    private static string Build(ICollectorSchemaInfo definition, CollectorContext context) =>
        definition switch
        {
            PgBufferUsageCollector d => d.BuildQuery(context).Text,
            PgKernelStatsCollector d => d.BuildQuery(context).Text,
            PgPredicateStatsCollector d => d.BuildQuery(context).Text,
            PgWaitSamplingCollector d => d.BuildQuery(context).Text,
            PgStatementStatsCollector d => d.BuildQuery(context).Text,
            PgWaitStatsCollector d => d.BuildQuery(context).Text,
            _ => throw new ArgumentOutOfRangeException(nameof(definition)),
        };

    private static CollectorContext NewContext(CollectorTargetInfo target) => new()
    {
        ServerId = ServerId,
        ServerName = "pm6143-target",
        CollectionTime = LiveClock.Now(),
        Deltas = new CollectorDeltaCalculator(),
        Target = target,
    };

    private static async Task<ServerRuntime> RuntimeAsync(NpgsqlConnectionStringBuilder admin, string database, string username, CancellationToken ct)
    {
        var config = new MonitoredServer
        {
            Name = "pm6143-target",
            Host = admin.Host ?? "127.0.0.1",
            Port = admin.Port,
            Database = database,
            Engine = "postgres",
            Auth = "sql",
            Username = username,
            EncryptMode = "Optional",
        };
        var monitoring = MonitoredServerConnection.BuildConnectionString(config, admin.Password ?? "unused");
        var major = (int)await ScalarLongAsync(monitoring, "SELECT pg_catalog.current_setting('server_version_num')::int / 10000", ct);
        return new ServerRuntime
        {
            Config = config,
            ConnectionString = monitoring,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = major,
                PostgresVersionNum = major * 10000,
            },
            StorageName = "pm6143-target",
            ServerId = ServerId,
        };
    }

    private static async Task<string?> DetectedSearchPathAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(DarlingServerConnector.PostgresDetectionQueryText, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return reader.GetString(reader.GetOrdinal("search_path"));
    }

    private static async Task<long> CallsAsync(string connectionString, CancellationToken ct) =>
        await ScalarLongAsync(connectionString, "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM shadow.calls", ct);

    private static async Task ResetAsync(string adminConnectionString, CancellationToken ct)
    {
        NpgsqlConnection.ClearAllPools();
        PostgresTargetProvider.ClearPools();
        await using var connection = new NpgsqlConnection(adminConnectionString);
        await connection.OpenAsync(ct);
        await DropAsync(adminConnectionString, connection, ct);
    }

    private static async Task DropAsync(string adminConnectionString, NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var database in new[] { Database, HypoDatabase, AuroraDatabase, InfoDatabase, TextDatabase })
        {
            await using (var exists = new NpgsqlCommand($"SELECT pg_catalog.count(*) FROM pg_catalog.pg_database WHERE datname = '{database}'", connection))
            {
                if (Convert.ToInt64(await exists.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture) > 0)
                {
                    await ScratchPostgres.QuiesceTimescaleJobsAsync(adminConnectionString, database, ct);
                }
            }

            await using var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS {database} WITH (FORCE)", connection);
            await drop.ExecuteNonQueryAsync(ct);
        }

        foreach (var role in new[] { Owner, Monitor })
        {
            await using var drop = new NpgsqlCommand($"DROP ROLE IF EXISTS {role}", connection);
            await drop.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task ExecAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ScalarLongAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<string?> ScalarTextAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(sql, connection);
        return (await command.ExecuteScalarAsync(ct))?.ToString();
    }
}
