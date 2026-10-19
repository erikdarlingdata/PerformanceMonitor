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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6143: monitoring connections to a PostgreSQL target pin search_path to pg_catalog at sign-in, and the collectors
/// name extension objects by the schema the extension was created in.
/// <para>The fixture is a database owned by a role that is not a superuser, whose database-level search_path puts a
/// schema of its own first. That schema holds an <c>=</c> operator for (integer, integer) and for (oid, oid), and
/// same-named copies of functions the collectors call; each one records its call in a table. The test first proves
/// the planted objects do run on a connection without the pin, then runs the collectors through the service's own
/// connection string and asserts they return rows and the planted objects never ran.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgCollectorSearchPathLiveTests
{
    private const string Database = "pm6143_owned";
    private const string Owner = "pm6143_owner";

    /* Same-named copies of built-in functions, each with the built-in's exact signature, so a search_path that lists
       the planted schema first resolves an unqualified call to the copy. */
    private const string PlantSql = """
        CREATE SCHEMA planted AUTHORIZATION pm6143_owner;
        SET ROLE pm6143_owner;
        CREATE TABLE planted.calls (what text NOT NULL);
        GRANT USAGE ON SCHEMA planted TO PUBLIC;
        GRANT INSERT ON planted.calls TO PUBLIC;

        CREATE FUNCTION planted.int4_eq(integer, integer) RETURNS boolean LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO planted.calls VALUES ('=(int4,int4)'); RETURN $1 OPERATOR(pg_catalog.=) $2; END $$;
        CREATE OPERATOR planted.= (LEFTARG = integer, RIGHTARG = integer, FUNCTION = planted.int4_eq);

        CREATE FUNCTION planted.oid_eq(oid, oid) RETURNS boolean LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO planted.calls VALUES ('=(oid,oid)'); RETURN $1 OPERATOR(pg_catalog.=) $2; END $$;
        CREATE OPERATOR planted.= (LEFTARG = oid, RIGHTARG = oid, FUNCTION = planted.oid_eq);

        CREATE FUNCTION planted.current_database() RETURNS name LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO planted.calls VALUES ('current_database'); RETURN pg_catalog.current_database(); END $$;
        CREATE FUNCTION planted.now() RETURNS timestamptz LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO planted.calls VALUES ('now'); RETURN pg_catalog.now(); END $$;
        CREATE FUNCTION planted.pg_relation_size(regclass) RETURNS bigint LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO planted.calls VALUES ('pg_relation_size'); RETURN pg_catalog.pg_relation_size($1); END $$;
        CREATE FUNCTION planted.pg_total_relation_size(regclass) RETURNS bigint LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO planted.calls VALUES ('pg_total_relation_size'); RETURN pg_catalog.pg_total_relation_size($1); END $$;
        CREATE FUNCTION planted.current_setting(text, boolean) RETURNS text LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO planted.calls VALUES ('current_setting'); RETURN pg_catalog.current_setting($1, $2); END $$;
        CREATE FUNCTION planted.quote_ident(text) RETURNS text LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO planted.calls VALUES ('quote_ident'); RETURN pg_catalog.quote_ident($1); END $$;

        CREATE TABLE public.pm6143_rows (id integer PRIMARY KEY, label text NOT NULL);
        INSERT INTO public.pm6143_rows SELECT g, 'row ' || g FROM pg_catalog.generate_series(1, 40000) AS g; -- over the bloat collectors' 1 MB floor
        CREATE INDEX pm6143_rows_label ON public.pm6143_rows (label);
        RESET ROLE;

        CREATE SCHEMA ext;
        CREATE EXTENSION pg_buffercache SCHEMA ext;
        CREATE EXTENSION pg_stat_statements SCHEMA ext;
        ANALYZE public.pm6143_rows;
        SELECT pg_catalog.count(*) FROM public.pm6143_rows;
        """;

    [Fact]
    public async Task CollectorsOnADatabaseWithItsOwnSearchPath_ReturnRows_AndNeverRunItsObjects()
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
            await ExecAsync(admin.ConnectionString, $"DROP DATABASE IF EXISTS {Database} WITH (FORCE)", ct);
            await ExecAsync(admin.ConnectionString, $"DROP ROLE IF EXISTS {Owner}", ct);
            await ExecAsync(admin.ConnectionString, $"CREATE ROLE {Owner} LOGIN NOSUPERUSER", ct);
            await ExecAsync(admin.ConnectionString, $"CREATE DATABASE {Database} OWNER {Owner}", ct);
            await ExecAsync(ownedAdmin.ConnectionString, PlantSql, ct);

            /* What the owner of a database can do without any superuser help. */
            await ExecAsync(
                new NpgsqlConnectionStringBuilder(ownedAdmin.ConnectionString) { Username = Owner }.ConnectionString,
                $"ALTER DATABASE {Database} SET search_path = planted, pg_catalog, public", ct);

            /* The fixture is live: on a connection without the pin, an unqualified = and current_database() run the
               planted copies. */
            await ExecAsync(ownedAdmin.ConnectionString, "SELECT 1 = 1, current_database()", ct);
            var unpinnedCalls = await ScalarLongAsync(ownedAdmin.ConnectionString, "SELECT pg_catalog.count(*) FROM planted.calls", ct);
            Assert.True(unpinnedCalls >= 2, $"the planted objects did not run on an unpinned connection ({unpinnedCalls} calls)");
            await ExecAsync(ownedAdmin.ConnectionString, "TRUNCATE planted.calls", ct);

            var config = new MonitoredServer
            {
                Name = "pm6143-target",
                Host = admin.Host ?? "127.0.0.1",
                Port = admin.Port,
                Database = Database,
                Engine = "postgres",
                Auth = "sql",
                Username = admin.Username,
                EncryptMode = "Optional",
            };
            var monitoring = MonitoredServerConnection.BuildConnectionString(config, admin.Password ?? "unused");

            Assert.Equal(
                "pg_catalog,pg_temp",
                await ScalarTextAsync(monitoring, "SHOW search_path", ct));

            var major = (int)(await ScalarLongAsync(monitoring, "SELECT current_setting('server_version_num')::int / 10000", ct));
            var runtime = new ServerRuntime
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
                ServerId = 6143,
            };

            await using var store = NpgsqlDataSource.Create(adminConnectionString!);
            var runner = new DarlingCollectorRunner(store, new CollectorDeltaCalculator(), NullLogger<DarlingCollectorRunner>.Instance);

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
                /* Extensions created in schema ext, not public: named by that schema. */
                ["pg_buffer_usage"] = (await runner.FetchRowsAsync(PgBufferUsageCollector.Instance, runtime, 60, ct)).Count,
                ["pg_statement_stats"] = (await runner.FetchRowsAsync(PgStatementStatsCollector.Instance, runtime, 60, ct)).Count,
            };

            foreach (var (collector, count) in counts)
            {
                Assert.True(count > 0, $"{collector} returned no rows");
            }

            Assert.Equal(0L, await ScalarLongAsync(ownedAdmin.ConnectionString, "SELECT pg_catalog.count(*) FROM planted.calls", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(admin.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using (var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS {Database} WITH (FORCE)", cleanup))
                {
                    await drop.ExecuteNonQueryAsync(cleanupCt);
                }

                await using var role = new NpgsqlCommand($"DROP ROLE IF EXISTS {Owner}", cleanup);
                await role.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    [Fact]
    public void PinSearchPath_KeepsOptionsAlreadySet_AndWithoutPinnedSearchPathRemovesOnlyThePin()
    {
        Assert.Equal(MonitoredServerConnection.PinnedSearchPathOption, MonitoredServerConnection.PinSearchPath(null));
        Assert.Equal(
            "-c statement_timeout=5000 " + MonitoredServerConnection.PinnedSearchPathOption,
            MonitoredServerConnection.PinSearchPath("-c statement_timeout=5000"));
        Assert.Equal(
            MonitoredServerConnection.PinnedSearchPathOption,
            MonitoredServerConnection.PinSearchPath(MonitoredServerConnection.PinnedSearchPathOption));

        var pinned = new NpgsqlConnectionStringBuilder("Host=h;Database=d")
        {
            Options = MonitoredServerConnection.PinSearchPath("-c statement_timeout=5000"),
        }.ConnectionString;
        Assert.Equal(
            "-c statement_timeout=5000",
            new NpgsqlConnectionStringBuilder(MonitoredServerConnection.WithoutPinnedSearchPath(pinned)).Options);
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
