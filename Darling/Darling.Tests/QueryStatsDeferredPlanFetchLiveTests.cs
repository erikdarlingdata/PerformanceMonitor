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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5158 end to end: <c>query_stats</c> with the deferred plan fetch, a real SQL Server as the monitored target
/// and a real PostgreSQL as the store (gated on DARLING_TEST_PG and DARLING_TEST_SQL, with the optional
/// DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD).
///
/// <para>Each test seeds its own scratch database with a few stored procedures, executes each so a full plan is
/// cached, and restricts the collector to that database (every other database is excluded), so the plan counts
/// below are exactly the scratch plans. Plans are counted by the run's <c>plans_rendered</c> measurement, the
/// number of plans the host rendered on the target.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class QueryStatsDeferredPlanFetchLiveTests
{
    private const string SkipReason = "Set DARLING_TEST_PG and DARLING_TEST_SQL to run the deferred plan fetch end-to-end tests.";

    /// <summary>Three procedures with a few statements each, so a run has several plan identities.</summary>
    private const string SeedProcedures = @"
CREATE PROCEDURE dbo.pm_a AS BEGIN SELECT COUNT_BIG(*) AS a1 FROM sys.objects; SELECT COUNT_BIG(*) AS a2 FROM sys.columns; END;
GO
CREATE PROCEDURE dbo.pm_b AS BEGIN SELECT COUNT_BIG(*) AS b1 FROM sys.indexes; END;
GO
CREATE PROCEDURE dbo.pm_c AS BEGIN SELECT COUNT_BIG(*) AS c1 FROM sys.types; END;";

    private static (string Pg, string SqlHost, string? SqlUser, string? SqlPassword) Env()
    {
        var pg = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        var sql = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        Assert.SkipWhen(string.IsNullOrEmpty(pg) || string.IsNullOrEmpty(sql), SkipReason);
        return (pg!, sql!, Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER"), Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD"));
    }

    private sealed class Rig : IAsyncDisposable
    {
        public required NpgsqlDataSource Store { get; init; }
        public required ServerRuntime Server { get; init; }
        public required string Database { get; init; }
        public required string PgConnectionString { get; init; }

        public async ValueTask DisposeAsync() => await Store.DisposeAsync();
    }

    private static async Task<Rig> StartAsync(string name, int wideUnions, CancellationToken ct)
    {
        var (pg, sqlHost, user, password) = Env();
        var database = "pm5158_" + Guid.NewGuid().ToString("N")[..10];

        await using (var admin = new SqlConnection(Connect(sqlHost, user, password, "master")))
        {
            await admin.OpenAsync(ct);
            await Exec(admin, "CREATE DATABASE [" + database + "];", ct);
        }

        await using (var seed = new SqlConnection(Connect(sqlHost, user, password, database)))
        {
            await seed.OpenAsync(ct);
            foreach (var batch in SeedProcedures.Split("GO", StringSplitOptions.RemoveEmptyEntries))
            {
                await Exec(seed, batch, ct);
            }

            if (wideUnions > 0)
            {
                var wide = new System.Text.StringBuilder("CREATE PROCEDURE dbo.pm_wide AS SELECT 1 AS x");
                for (var i = 0; i < wideUnions; i++)
                {
                    wide.Append(" UNION ALL SELECT TOP (1) object_id FROM sys.objects WHERE object_id = ").Append(i);
                }

                await Exec(seed, wide.ToString(), ct);
            }

            await ExecuteProceduresAsync(seed, wideUnions > 0, ct);
        }

        var others = new List<string>();
        await using (var list = new SqlConnection(Connect(sqlHost, user, password, "master")))
        {
            await list.OpenAsync(ct);
            using var command = new SqlCommand("SELECT name FROM sys.databases WHERE name <> @d", list);
            command.Parameters.AddWithValue("@d", database);
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                others.Add(reader.GetString(0));
            }
        }

        var config = new MonitoredServer
        {
            Name = name + "-" + Guid.NewGuid().ToString("N")[..6],
            Host = sqlHost,
            Auth = string.IsNullOrEmpty(user) ? "integrated" : "sql",
            Username = user,
            Password = password,
            TrustServerCertificate = true,
            ExcludedDatabases = others,
        };

        var store = NpgsqlDataSource.Create(pg);
        await using (var migrate = await store.OpenConnectionAsync(ct))
        {
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        return new Rig
        {
            Store = store,
            Server = await DarlingServerConnector.ConnectAsync(config, null, ct),
            Database = database,
            PgConnectionString = pg,
        };
    }

    private static string Connect(string host, string? user, string? password, string database)
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = host,
            InitialCatalog = database,
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            ConnectTimeout = 15,
        };
        if (string.IsNullOrEmpty(user))
        {
            builder.IntegratedSecurity = true;
        }
        else
        {
            builder.UserID = user;
            builder.Password = password;
        }

        return builder.ConnectionString;
    }

    private static async Task Exec(SqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new SqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Executes each procedure twice, so each caches a full compiled plan rather than an ad hoc stub.</summary>
    private static async Task ExecuteProceduresAsync(SqlConnection connection, bool includeWide, CancellationToken ct, string? only = null)
    {
        var names = new List<string> { "pm_a", "pm_b", "pm_c" };
        if (includeWide)
        {
            names.Add("pm_wide");
        }

        foreach (var name in names.Where(n => only is null || n == only))
        {
            for (var i = 0; i < 2; i++)
            {
                using var command = new SqlCommand("EXEC dbo." + name, connection) { CommandTimeout = 120 };
                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct)) { }
            }
        }
    }

    private static async Task TearDownAsync(Rig rig, bool bodySucceeded)
    {
        try
        {
            var (_, sqlHost, user, password) = Env();
            await using var admin = new SqlConnection(Connect(sqlHost, user, password, "master"));
            await admin.OpenAsync(CancellationToken.None);
            await Exec(admin, "IF DB_ID(N'" + rig.Database + "') IS NOT NULL BEGIN ALTER DATABASE [" + rig.Database
                + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [" + rig.Database + "]; END;", CancellationToken.None);
        }
        catch when (!bodySucceeded)
        {
        }

        await LiveStoreCleanup.RunAsync(rig.PgConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
        {
            using var command = new NpgsqlCommand("DELETE FROM query_stats WHERE server_id = $1", cleanup);
            command.Parameters.AddWithValue(rig.Server.ServerId);
            await command.ExecuteNonQueryAsync(cleanupCt);
        });
    }

    /// <summary>The run's plans_rendered measurement, or -1 when the run did not defer its fetch (it renders inline).</summary>
    private static long Rendered(CollectorRunResult result, string label = "plans_rendered") =>
        result.Measurements.Any(m => m.Label == label) ? result.Measurements.First(m => m.Label == label).Value : -1;

    private static DarlingCollectorRunner NewRunner(NpgsqlDataSource store, bool deferred) =>
        new(store, new CollectorDeltaCalculator(), null, capturePlans: () => true, queryStatsDeferredPlanFetch: () => deferred);

    /// <summary>The scratch database's stored rows for this run set: digest, reachable plan through v_query_stats, size.</summary>
    private static async Task<List<StoredRow>> StoredAsync(Rig rig, CancellationToken ct)
    {
        await using var connection = await rig.Store.OpenConnectionAsync(ct);
        using var command = new NpgsqlCommand(
            "SELECT host_object_name, query_plan_digest IS NOT NULL, "
            + "(COALESCE(query_plan_xml, '') <> '' OR query_plan_gz IS NOT NULL), query_plan_xml_bytes, collection_time "
            + "FROM v_query_stats WHERE server_id = $1 AND database_name = $2 AND host_object_name LIKE '%.pm\\_%' ORDER BY collection_time, host_object_name", connection);
        command.Parameters.AddWithValue(rig.Server.ServerId);
        command.Parameters.AddWithValue(rig.Database);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<StoredRow>();
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new StoredRow(
                reader.IsDBNull(0) ? null : reader.GetString(0), reader.GetBoolean(1), reader.GetBoolean(2),
                reader.IsDBNull(3) ? null : reader.GetInt64(3), reader.GetDateTime(4)));
        }

        return rows;
    }

    private sealed record StoredRow(string? Object, bool HasDigest, bool PlanResolves, long? Bytes, DateTime CollectedAt);

    [Fact]
    public async Task RunTwo_RendersNothing_AndItsRowsStillResolveTheirPlans()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync("pm5158-two", wideUnions: 0, ct);
        var ok = false;
        try
        {
            var runner = NewRunner(rig.Store, deferred: true);

            var first = await runner.RunAsync(QueryStatsCollector.Instance, rig.Server, ct);
            Assert.True(first.Rows >= 4, "three procedures, four statements, one row each");
            Assert.True(Rendered(first) >= 4, "a cold cache renders every plan");

            var second = await runner.RunAsync(QueryStatsCollector.Instance, rig.Server, ct);
            Assert.True(second.Rows >= 4);

            /* The point of the change: nothing rendered the second time. With the knob off the run renders inline
               and carries no plans_rendered measurement at all. */
            Assert.Equal(0, Rendered(second));
            Assert.Equal(0, Rendered(second, "plans_rendered_bytes"));

            var stored = await StoredAsync(rig, ct);
            Assert.Equal(8, stored.Count); /* the three procedures hold four statements, collected twice */
            Assert.All(stored, row =>
            {
                Assert.True(row.HasDigest, "every row, reused or rendered, carries the plan digest");
                Assert.True(row.PlanResolves, "and resolves its plan through v_query_stats");
                Assert.True(row.Bytes > 0, "reused rows keep the measured plan size");
            });
            ok = true;
        }
        finally
        {
            await TearDownAsync(rig, ok);
        }
    }

    [Fact]
    public async Task WithTheKnobOff_EveryRunRendersInline_AndNothingIsDeferred()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync("pm5158-off", wideUnions: 0, ct);
        var ok = false;
        try
        {
            var runner = NewRunner(rig.Store, deferred: false);
            var first = await runner.RunAsync(QueryStatsCollector.Instance, rig.Server, ct);
            var second = await runner.RunAsync(QueryStatsCollector.Instance, rig.Server, ct);

            Assert.Equal(-1, Rendered(first));
            Assert.Equal(-1, Rendered(second));
            var stored = await StoredAsync(rig, ct);
            Assert.Equal(8, stored.Count); /* the three procedures hold four statements, collected twice */
            Assert.All(stored, row => Assert.True(row.PlanResolves, "inline capture stores every row's plan"));
            ok = true;
        }
        finally
        {
            await TearDownAsync(rig, ok);
        }
    }

    [Fact]
    public async Task SpRecompile_GivesANewIdentity_ThatIsFetched()
    {
        var ct = TestContext.Current.CancellationToken;
        var (_, sqlHost, user, password) = Env();
        await using var rig = await StartAsync("pm5158-recomp", wideUnions: 0, ct);
        var ok = false;
        try
        {
            var runner = NewRunner(rig.Store, deferred: true);
            var first = await runner.RunAsync(QueryStatsCollector.Instance, rig.Server, ct);
            var second = await runner.RunAsync(QueryStatsCollector.Instance, rig.Server, ct);
            Assert.Equal(0, Rendered(second));

            await using (var target = new SqlConnection(Connect(sqlHost, user, password, rig.Database)))
            {
                await target.OpenAsync(ct);
                await Exec(target, "EXEC sys.sp_recompile N'dbo.pm_a';", ct);
                await ExecuteProceduresAsync(target, includeWide: false, ct, only: "pm_a");
            }

            var third = await runner.RunAsync(QueryStatsCollector.Instance, rig.Server, ct);

            /* pm_a has two statements, each a new plan identity; pm_b and pm_c are unchanged and rendered nothing. */
            Assert.Equal(2, Rendered(third));
            Assert.True(first.Rows >= 4);

            var stored = await StoredAsync(rig, ct);
            var latest = stored.Where(r => r.CollectedAt == stored.Max(x => x.CollectedAt)).ToList();
            Assert.All(latest, row => Assert.True(row.HasDigest && row.PlanResolves));
            ok = true;
        }
        finally
        {
            await TearDownAsync(rig, ok);
        }
    }

    [Fact]
    public async Task AHandleThatAgesOutBetweenThePhases_ShipsNoPlanAndNoSize_WithoutFailingTheRun()
    {
        var ct = TestContext.Current.CancellationToken;
        var (_, sqlHost, user, password) = Env();
        await using var rig = await StartAsync("pm5158-aged", wideUnions: 0, ct);
        var ok = false;
        try
        {
            var runner = NewRunner(rig.Store, deferred: true);
            runner.BetweenPlanPhasesForTests = async token =>
            {
                await using var target = new SqlConnection(Connect(sqlHost, user, password, rig.Database));
                await target.OpenAsync(token);
                using var find = new SqlCommand(
                    "SELECT CONVERT(varchar(130), plan_handle, 1) FROM sys.dm_exec_procedure_stats WHERE database_id = DB_ID() AND object_id = OBJECT_ID(N'dbo.pm_b')", target);
                var handle = (string)(await find.ExecuteScalarAsync(token))!;
                await Exec(target, "DBCC FREEPROCCACHE(" + handle + ");", token);
            };

            var result = await runner.RunAsync(QueryStatsCollector.Instance, rig.Server, ct);
            Assert.True(result.Rows >= 4);

            var stored = await StoredAsync(rig, ct);
            var aged = stored.Where(r => r.Object is not null && r.Object.EndsWith("pm_b", StringComparison.Ordinal)).ToList();
            Assert.NotEmpty(aged);

            /* Today's aged-out pairing: no plan and no size. And nothing was cached for it. */
            Assert.All(aged, row =>
            {
                Assert.False(row.HasDigest);
                Assert.False(row.PlanResolves);
                Assert.Null(row.Bytes);
            });
            Assert.All(stored.Except(aged), row => Assert.True(row.HasDigest && row.PlanResolves));
            ok = true;
        }
        finally
        {
            await TearDownAsync(rig, ok);
        }
    }

    [Fact]
    public async Task AnOverCapPlan_KeepsItsBytesWithANullPlan_AndIsNotRenderedAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await StartAsync("pm5158-wide", wideUnions: 400, ct);
        var ok = false;
        try
        {
            var runner = NewRunner(rig.Store, deferred: true);
            var first = await runner.RunAsync(QueryStatsCollector.Instance, rig.Server, ct);
            Assert.True(Rendered(first, "plans_rendered_bytes") > QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes,
                "the first sighting renders the wide plan once");

            var second = await runner.RunAsync(QueryStatsCollector.Instance, rig.Server, ct);
            Assert.Equal(0, Rendered(second));
            Assert.Equal(0, Rendered(second, "plans_rendered_bytes"));

            var stored = await StoredAsync(rig, ct);
            var wide = stored.Where(r => r.Object is not null && r.Object.EndsWith("pm_wide", StringComparison.Ordinal)).ToList();
            Assert.Equal(2, wide.Count);
            Assert.All(wide, row =>
            {
                Assert.False(row.HasDigest, "an over-cap plan stores no plan");
                Assert.True(row.Bytes > QueryPlanXmlCaptureLimits.MaxCapturedPlanXmlBytes, "but keeps the measured size, run 2 included");
            });
            ok = true;
        }
        finally
        {
            await TearDownAsync(rig, ok);
        }
    }
}
