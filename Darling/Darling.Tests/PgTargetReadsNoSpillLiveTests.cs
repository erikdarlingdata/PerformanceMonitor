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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the shared store. */

/// <summary>
/// #9062: the statement-statistics reads and the buffer usage read on a PostgreSQL target wrote temp files on every call. The
/// statement reads are a function in FROM, whose whole result a tuplestore holds under <c>work_mem</c>; the buffer read ran a
/// window over every row of the pool. These facts run the real statements on a real server.
/// <para>The spill facts lower the session's <c>work_mem</c> to its minimum, so a few hundred entries do what thousands do at
/// the default 4 MB, and each one first runs the stock statement to prove the fixture spills (a pass that could not fail proves
/// nothing). They read <c>temp_blks_written</c> off the statement's own entry in <c>pg_stat_statements</c>, which is what the
/// field measurement read.</para>
/// <para>pg_stat_statements is cluster-wide and its eviction passes would take the entries between the runs and the read, so the
/// spill facts run inside <see cref="PgStatementTable"/>'s bracket, in the collection that runs alone (the reason
/// <see cref="PgStatementStatsNoTextReadLiveTests"/> gives).</para>
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class PgTargetReadsNoSpillLiveTests
{
    private const int ServerId = -906201;

    /// <summary>The server's own minimum: PostgreSQL refuses a smaller work_mem.</summary>
    private const string LowWorkMem = "64kB";

    /// <summary>Entries the fixture adds. 64 kB holds about 140 of them, and the plain read needs a few hundred bytes each.</summary>
    private const int FillerStatements = 400;

    [Fact]
    public async Task TheStatementStatsRead_StaysInMemory_WhereTheStockReadSpills_AndLeavesTheSessionsWorkMemAlone()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the temp file test.");
        var ct = TestContext.Current.CancellationToken;

        await using var target = await ScratchPostgres.CreateAsync(adminConnectionString!, ct, emptyStore: true);
        await ExecAsync(target.ConnectionString, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements", ct);

        await using var connection = new NpgsqlConnection(LowWorkMemConnectionString(target.ConnectionString));
        await connection.OpenAsync(ct);
        Assert.Equal(LowWorkMem, await ShowWorkMemAsync(connection, ct));

        var targetOid = await ScalarAsync<long>(connection, "SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname = current_database()", ct);
        var context = new CollectorContext
        {
            ServerId = ServerId,
            ServerName = "pm9062-target",
            CollectionTime = LiveClock.Now(),
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = connection.PostgreSqlVersion.Major },
        };
        Assert.True(await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(
            context, PgStatementStatsCollector.Instance, connection, null, ct));
        var shipped = PgStatementStatsCollector.Instance.BuildQuery(context).Text;
        var schema = context.PgExtensionSchema("pg_stat_statements");

        long stockTemp = 0;
        long collectorTemp = 0;
        List<PgStatementStatsCollector.Row> rows = [];
        var bracket = await PgStatementTable.RunAsync(connection, ct, async pass =>
        {
            await FillAsync(connection, ct);
            await pass.BeforeReadAsync(ct);

            var before = await TempWrittenAsync(connection, schema, targetOid, "%AS stats_dealloc%", ct);

            /* The stock read: the shipped text on its own, as the host sent it before #9062. */
            await using (var stock = new NpgsqlCommand(PgOwnStatement.Mark(shipped), connection) { CommandTimeout = 60 })
            await using (var stockReader = await stock.ExecuteReaderAsync(ct))
            {
                while (await stockReader.ReadAsync(ct))
                {
                }
            }

            stockTemp = await TempWrittenAsync(connection, schema, targetOid, "%AS stats_dealloc%", ct) - before;

            /* The host's read: the same command through the helper the runner and the on-demand fetch both use. */
            await using (var command = new NpgsqlCommand(PgOwnStatement.Mark(shipped), connection) { CommandTimeout = 60 })
            {
                Assert.True(PgReadWorkMem.ApplyTo(command, PgStatementStatsCollector.Instance.ReadWorkMemBytesPerEntry));
                await using var reader = await command.ExecuteReaderAsync(ct);
                await PgReadWorkMem.SkipSettingResultAsync(reader, ct);
                rows = await PgStatementStatsCollector.Instance.ReadAsync(reader, context, ct);
            }

            collectorTemp = await TempWrittenAsync(connection, schema, targetOid, "%AS stats_dealloc%", ct) - before - stockTemp;
        });
        PgStatementTable.AssertNoPassInside(bracket);

        Assert.True(stockTemp > 0, $"the stock read wrote {stockTemp} temp blocks: the fixture does not spill, so this fact proves nothing");
        Assert.Equal(0, collectorTemp);
        Assert.NotEmpty(rows);

        /* SET LOCAL's reach is the read: the setting is back on the same connection the moment it ends. */
        Assert.Equal(LowWorkMem, await ShowWorkMemAsync(connection, ct));
    }

    /// <summary>
    /// #9074: runs as a login role of its own, with no superuser and no <c>pg_read_all_stats</c>. <c>pg_stat_statements(true)</c>
    /// gives a text only for the caller's own entries, so every other session's entry in the cluster comes back as a short
    /// placeholder and the volume the fetch reads is the filler's alone. As the cluster's superuser the same call loads the full
    /// text of every entry, the long statements other classes run in parallel included, and a fetch sized for 4 KB an entry
    /// wrote thousands of temp blocks in a busy CI cluster.
    /// </summary>
    [Fact]
    public async Task TheStatementTextFetch_StaysInMemory_WhereTheStockFetchSpills_AndStillReturnsTheTexts()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the temp file test.");
        var ct = TestContext.Current.CancellationToken;

        await using var target = await ScratchPostgres.CreateAsync(adminConnectionString!, ct, emptyStore: true);
        await ExecAsync(target.ConnectionString, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements", ct);

        /* The fetch opens its own connection, so the low setting is the database's: every new session of it starts at 64 kB. */
        var owner = new NpgsqlConnectionStringBuilder(target.ConnectionString) { Pooling = false };
        await ExecAsync(owner.ConnectionString, $"ALTER DATABASE \"{owner.Database}\" SET work_mem = '{LowWorkMem}'", ct);

        var roleName = "pm9074_" + Guid.NewGuid().ToString("N")[..12];
        var rolePassword = "Pm9074-" + Guid.NewGuid().ToString("N");
        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner.ConnectionString, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{rolePassword}'", ct);
            await ExecAsync(owner.ConnectionString, $"GRANT CONNECT ON DATABASE \"{owner.Database}\" TO {roleName}", ct);
            var reader = new NpgsqlConnectionStringBuilder(owner.ConnectionString) { Username = roleName, Password = rolePassword };

            var runtime = await RuntimeAsync(reader, ct);
            await using var connection = new NpgsqlConnection(reader.ConnectionString);
            await connection.OpenAsync(ct);
            Assert.Equal(LowWorkMem, await ShowWorkMemAsync(connection, ct));

            var targetOid = await ScalarAsync<long>(connection, "SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname = current_database()", ct);
            var schemas = new CollectorContext
            {
                ServerId = ServerId,
                ServerName = "pm9062-target",
                CollectionTime = LiveClock.Now(),
                Deltas = new CollectorDeltaCalculator(),
                Target = runtime.Target,
            };
            Assert.True(await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(
                schemas, PgStatementStatsCollector.Instance, connection, null, ct));
            var schema = schemas.PgExtensionSchema("pg_stat_statements");
            var fetchSql = PgOwnStatement.Mark(PgStatementText.FetchSqlFor(false, runtime.Target.PostgresMajorVersion, schemas));

            long stockTemp = 0;
            long fetchTemp = 0;
            List<long> queryIds = [];
            var bracket = await PgStatementTable.RunAsync(connection, ct, async pass =>
            {
                await FillAsync(connection, ct);
                await pass.BeforeReadAsync(ct);

                var before = await TempWrittenAsync(connection, schema, targetOid, "%AS candidate_entries%", ct);

                await using (var stock = new NpgsqlCommand(fetchSql, connection) { CommandTimeout = 60 })
                {
                    stock.Parameters.AddWithValue(10_000);
                    await using var stockReader = await stock.ExecuteReaderAsync(ct);
                    while (await stockReader.ReadAsync(ct))
                    {
                    }
                }

                stockTemp = await TempWrittenAsync(connection, schema, targetOid, "%AS candidate_entries%", ct) - before;

                (queryIds, _, _, _) = await DarlingWorker.ReadPgStatementTextAsync(runtime, ct);

                fetchTemp = await TempWrittenAsync(connection, schema, targetOid, "%AS candidate_entries%", ct) - before - stockTemp;
            });
            PgStatementTable.AssertNoPassInside(bracket);

            Assert.True(stockTemp > 0, $"the stock fetch wrote {stockTemp} temp blocks: the fixture does not spill, so this fact proves nothing");
            Assert.Equal(0, fetchTemp);
            Assert.NotEmpty(queryIds);
            bodySucceeded = true;
        }
        finally
        {
            /* A role belongs to the cluster, not to the scratch database that is dropped next, so it is dropped here. */
            await LiveStoreCleanup.RunAsync(target.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand($"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /// <summary>
    /// The buffer usage read returns the same rows as the read it replaced. Both shapes run over one frozen copy of the
    /// pool, in one statement, and <c>EXCEPT ALL</c> in each direction is empty. The copy is a table named
    /// <c>pg_buffercache</c> in a schema of its own, which the collector reaches the way it reaches the extension's view in a
    /// schema that is not public (#6143), so the shipped text runs as it is.
    /// <para>The copy carries the cases the grouping could change: a database other than the connected one (its relation is
    /// NULL), buffers with no relfilenode (the unused ones, in the pool total and out of the used count), and a group whose
    /// every usage count is NULL (the old average is NULL, and a division by the count of usage counts would raise).</para>
    /// </summary>
    [Fact]
    public async Task TheBufferUsageRead_ReturnsTheRowsTheWindowOverEveryBufferReturned_NullsIncluded()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the buffer usage test.");
        var ct = TestContext.Current.CancellationToken;

        await using var target = await ScratchPostgres.CreateAsync(adminConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(target.ConnectionString);
        await connection.OpenAsync(ct);

        await DarlingMcpTestData.ExecAsync(connection, ct, "CREATE EXTENSION IF NOT EXISTS pg_buffercache");
        await DarlingMcpTestData.ExecAsync(connection, ct, """
            CREATE TABLE pm9062_a AS SELECT g AS id, repeat('a', 200) AS pad FROM generate_series(1, 20000) AS g;
            CREATE TABLE pm9062_b AS SELECT g AS id, repeat('b', 200) AS pad FROM generate_series(1, 9000) AS g;
            CREATE INDEX pm9062_a_ix ON pm9062_a (id);
            UPDATE pm9062_b SET pad = repeat('c', 200) WHERE id % 3 = 0;
            SELECT count(*) FROM pm9062_a;
            SELECT count(*) FROM pm9062_a WHERE id < 100;
            """);

        await DarlingMcpTestData.ExecAsync(connection, ct, """
            CREATE SCHEMA pm9062_snapshot;
            CREATE TABLE pm9062_snapshot.pg_buffercache AS SELECT * FROM public.pg_buffercache;
            DELETE FROM pm9062_snapshot.pg_buffercache WHERE reldatabase = (SELECT oid FROM pg_database WHERE datname = 'template0');
            """);

        /* Another database's buffers, every usage count NULL: the group's average is NULL, and a division by the sum of the
           usage-count counts would raise on it. template0 is a database nothing connects to, so the group holds only these.
           Scratch databases are created from template0, so its own pages can sit in the pool during a parallel run (#9067), and the snapshot drops them above. */
        await DarlingMcpTestData.ExecAsync(connection, ct, """
            INSERT INTO pm9062_snapshot.pg_buffercache (bufferid, relfilenode, reldatabase, isdirty, usagecount)
            SELECT 900000 + g, 990001, (SELECT oid FROM pg_database WHERE datname = 'template0'), false, NULL
            FROM generate_series(1, 9) AS g;
            """);

        var context = new CollectorContext
        {
            ServerId = ServerId,
            ServerName = "pm9062-target",
            CollectionTime = LiveClock.Now(),
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = connection.PostgreSqlVersion.Major },
            PgExtensionSchemas = new Dictionary<string, string>(StringComparer.Ordinal) { ["pg_buffercache"] = "pm9062_snapshot" },
        };
        var shipped = PgBufferUsageCollector.Instance.BuildQuery(context).Text;

        const string OldShape = """
            WITH pool AS (
                SELECT
                    b.reldatabase,
                    b.relfilenode,
                    b.isdirty,
                    b.usagecount,
                    count(*) OVER ()                                        AS pool_total,
                    count(*) FILTER (WHERE b.relfilenode IS NOT NULL) OVER () AS pool_used
                FROM pm9062_snapshot.pg_buffercache AS b
            )
            SELECT
                d.datname::text                                             AS database_name,
                c.relname::text                                             AS relation_name,
                c.relkind::text                                             AS relation_kind,
                count(*)::bigint                                            AS buffers,
                count(*) FILTER (WHERE p.isdirty)::bigint                   AS dirty_buffers,
                avg(p.usagecount)::double precision                         AS avg_usage_count,
                max(p.pool_total)::bigint                                   AS pool_buffers_total,
                max(p.pool_used)::bigint                                    AS pool_buffers_used
            FROM pool AS p
            LEFT JOIN pg_catalog.pg_database AS d
              ON d.oid = p.reldatabase
            LEFT JOIN pg_catalog.pg_class AS c
              ON  p.relfilenode = pg_catalog.pg_relation_filenode(c.oid)
              AND p.reldatabase = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
            WHERE p.relfilenode IS NOT NULL
            GROUP BY d.datname, c.relname, c.relkind
            HAVING count(*) >= 8
            ORDER BY count(*) DESC
            """;

        var oldCount = await ScalarAsync<long>(connection, $"SELECT count(*) FROM ({OldShape}) AS o", ct);
        var newCount = await ScalarAsync<long>(connection, $"SELECT count(*) FROM ({shipped}) AS n", ct);
        Assert.True(oldCount >= 4, $"the old shape returned {oldCount} rows: the fixture does not hold the cases");
        Assert.Equal(oldCount, newCount);
        Assert.Equal(1, await ScalarAsync<long>(connection,
            $"SELECT count(*) FROM ({shipped}) AS n WHERE n.avg_usage_count IS NULL AND n.relation_name IS NULL AND n.database_name = 'template0'", ct));

        Assert.Equal(0, await ScalarAsync<long>(connection, $"SELECT count(*) FROM ((SELECT * FROM ({OldShape}) AS o) EXCEPT ALL (SELECT * FROM ({shipped}) AS n)) AS x", ct));
        Assert.Equal(0, await ScalarAsync<long>(connection, $"SELECT count(*) FROM ((SELECT * FROM ({shipped}) AS n) EXCEPT ALL (SELECT * FROM ({OldShape}) AS o)) AS x", ct));
    }

    /// <summary>
    /// Statements the module keeps an entry for each: the filler family of <see cref="PgStatementTable.Shape"/>, as many as it
    /// takes for the table to hold <see cref="FillerStatements"/> entries in all. The stock read spills when the module's
    /// function returns that many rows, and the rows are every entry in the cluster, so on a table other classes have already
    /// filled the body adds none and cannot overflow its headroom (#9106).
    /// </summary>
    private static async Task FillAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var toAdd = await PgStatementTable.EntriesToAddAsync(connection, ct, FillerStatements);
        var batch = new System.Text.StringBuilder();
        for (var n = 0; n < toAdd; n++)
        {
            batch.Append(PgStatementTable.Shape(n, 7)).Append(';');
            if ((n + 1) % 100 == 0 || n + 1 == toAdd)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, batch.ToString());
                batch.Clear();
            }
        }
    }

    /// <summary>
    /// Temp blocks the database's entries whose text has <paramref name="textPattern"/> have written, summed over the entries
    /// (the entry a statement's runs share; one per user). The pattern is a parameter, so the reading statement's own text
    /// never matches it.
    /// </summary>
    private static async Task<long> TempWrittenAsync(NpgsqlConnection connection, string schema, long databaseId, string textPattern, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT coalesce(sum(s.temp_blks_written), 0)::bigint FROM {schema}.pg_stat_statements AS s WHERE s.dbid = $1 AND s.query LIKE $2", connection);
        command.Parameters.AddWithValue(databaseId);
        command.Parameters.AddWithValue(textPattern);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static string LowWorkMemConnectionString(string connectionString) =>
        new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false, Options = "-c work_mem=" + LowWorkMem }.ConnectionString;

    private static async Task<string> ShowWorkMemAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await ScalarAsync<string>(connection, "SHOW work_mem", ct);

    private static async Task<ServerRuntime> RuntimeAsync(NpgsqlConnectionStringBuilder admin, CancellationToken ct)
    {
        var config = new MonitoredServer
        {
            Name = "pm9062-target",
            Host = admin.Host ?? "127.0.0.1",
            Port = admin.Port,
            Database = admin.Database,
            Engine = "postgres",
            Auth = "sql",
            Username = admin.Username,
            EncryptMode = "Optional",
        };
        var monitoring = MonitoredServerConnection.BuildConnectionString(config, admin.Password ?? "unused");
        long major;
        await using (var connection = new NpgsqlConnection(admin.ConnectionString))
        {
            await connection.OpenAsync(ct);
            major = await ScalarAsync<int>(connection, "SELECT pg_catalog.current_setting('server_version_num')::int / 10000", ct);
        }

        return new ServerRuntime
        {
            Config = config,
            ConnectionString = monitoring,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = (int)major,
                PostgresVersionNum = (int)major * 10000,
            },
            StorageName = "pm9062-target",
            ServerId = ServerId,
        };
    }

    private static async Task ExecAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }
}
