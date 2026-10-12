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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5158 end to end for <c>procedure_stats</c>: the deferred plan fetch and its shadow mode, with a real SQL Server as
/// the monitored target and a real PostgreSQL as the store (gated on DARLING_TEST_PG and DARLING_TEST_SQL, with the
/// optional DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD).
///
/// <para>Each test seeds its own scratch database with a few stored procedures, executes each so a full plan is
/// cached, and restricts the collector to that database, so the plan counts below are exactly the scratch modules.
/// Plans are counted by the run's <c>plans_rendered</c> measurement, which also reaches the run's collection-log
/// note together with the <c>deferred_*</c> counters.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class ProcedureStatsDeferredPlanFetchLiveTests
{
    private const string SkipReason = "Set DARLING_TEST_PG and DARLING_TEST_SQL to run the procedure_stats deferred plan fetch end-to-end tests.";

    /// <summary>Three simple procedures, a data table, and a procedure whose second branch compiles only when first taken.</summary>
    private const string SeedProcedures = @"
CREATE TABLE dbo.pm_data (k int NOT NULL, v int NOT NULL);
INSERT dbo.pm_data (k, v) SELECT TOP (200) 1, ROW_NUMBER() OVER (ORDER BY (SELECT 1)) FROM sys.all_columns;
GO
CREATE PROCEDURE dbo.pm_a AS BEGIN SELECT COUNT_BIG(*) AS a1 FROM sys.objects; SELECT COUNT_BIG(*) AS a2 FROM sys.columns; END;
GO
CREATE PROCEDURE dbo.pm_b AS BEGIN SELECT COUNT_BIG(*) AS b1 FROM sys.indexes; END;
GO
CREATE PROCEDURE dbo.pm_s AS BEGIN SELECT COUNT_BIG(*) AS s1 FROM dbo.pm_data AS x JOIN dbo.pm_data AS y ON y.v = x.v WHERE x.k = 1; END;
GO
CREATE PROCEDURE dbo.pm_d @branch int AS
BEGIN
    IF @branch = 0 SELECT COUNT_BIG(*) AS d1 FROM sys.types;
    ELSE SELECT COUNT_BIG(*) AS d2 FROM sys.schemas;
END;";

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

    /// <summary>
    /// Builds the rig on the scratch database <paramref name="database"/>. The caller picks the name and owns the drop (#5158),
    /// because a throw after the CREATE below never returns a rig, so only the caller's own teardown can find the database.
    /// </summary>
    private static async Task<Rig> StartAsync(string name, string database, CancellationToken ct)
    {
        var (pg, sqlHost, user, password) = Env();

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

            await ExecuteAsync(seed, ct, "pm_a", "pm_b", "pm_s");
            await ExecuteAsync(seed, ct, "pm_d 0");
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
        try
        {
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
        catch
        {
            // No rig reaches the caller, so nothing else would dispose this store and its pooled connection.
            await store.DisposeAsync();
            throw;
        }
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
    private static async Task ExecuteAsync(SqlConnection connection, CancellationToken ct, params string[] calls)
    {
        foreach (var call in calls)
        {
            for (var i = 0; i < 2; i++)
            {
                using var command = new SqlCommand("EXEC dbo." + call, connection) { CommandTimeout = 120 };
                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct)) { }
            }
        }
    }

    /// <summary>
    /// Drops the scratch database on the monitored side, and skips one that was never created (setup can throw before or at
    /// the CREATE). A drop failure surfaces only when the body succeeded.
    /// </summary>
    private static async Task DropScratchDatabaseAsync(string database, bool bodySucceeded)
    {
        try
        {
            var (_, sqlHost, user, password) = Env();
            await using var admin = new SqlConnection(Connect(sqlHost, user, password, "master"));
            await admin.OpenAsync(CancellationToken.None);
            await Exec(admin, "IF DB_ID(N'" + database + "') IS NOT NULL BEGIN ALTER DATABASE [" + database
                + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [" + database + "]; END;", CancellationToken.None);
        }
        catch when (!bodySucceeded)
        {
        }
    }

    private static async Task DeleteStoredRowsAsync(NpgsqlConnection cleanup, int serverId, CancellationToken cleanupCt)
    {
        foreach (var table in new[] { "procedure_stats", "collection_log" })
        {
            using var command = new NpgsqlCommand("DELETE FROM " + table + " WHERE server_id = $1", cleanup);
            command.Parameters.AddWithValue(serverId);
            await command.ExecuteNonQueryAsync(cleanupCt);
        }
    }

    /// <summary>A measurement of the run, or -1 when the run did not emit it.</summary>
    private static long Measured(CollectorRunResult result, string label) =>
        result.Measurements.Any(m => m.Label == label) ? result.Measurements.First(m => m.Label == label).Value : -1;

    private static DarlingCollectorRunner NewRunner(NpgsqlDataSource store, string mode, int planCycleInterval = 1, ILogger? logger = null) =>
        new(store, new CollectorDeltaCalculator(), logger, capturePlans: () => true,
            procedureStatsPlanCycleInterval: () => planCycleInterval, procedureStatsDeferredPlanFetch: () => mode);

    private sealed record StoredRow(string Object, bool HasDigest, bool PlanResolves, long? Bytes);

    /// <summary>The scratch modules' stored rows: digest present, plan reachable (inline or through the dimension), size.</summary>
    private static async Task<List<StoredRow>> StoredAsync(Rig rig, CancellationToken ct)
    {
        await using var connection = await rig.Store.OpenConnectionAsync(ct);
        using var command = new NpgsqlCommand(
            "SELECT p.object_name, p.query_plan_digest IS NOT NULL, "
            + "(COALESCE(p.query_plan_xml, '') <> '' OR d.query_plan_xml IS NOT NULL OR d.query_plan_gz IS NOT NULL), p.query_plan_xml_bytes "
            + "FROM procedure_stats p LEFT JOIN query_plan_dim d ON d.digest = p.query_plan_digest "
            + "WHERE p.server_id = $1 AND p.database_name = $2 AND p.object_name LIKE 'pm\\_%' ORDER BY p.collection_time, p.object_name", connection);
        command.Parameters.AddWithValue(rig.Server.ServerId);
        command.Parameters.AddWithValue(rig.Database);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<StoredRow>();
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new StoredRow(reader.GetString(0), reader.GetBoolean(1), reader.GetBoolean(2), reader.IsDBNull(3) ? null : reader.GetInt64(3)));
        }

        return rows;
    }

    /// <summary>
    /// The note a run carries: the value the worker writes to the collection log's note column (<c>CollectorRunResult.Note</c>
    /// is exactly that column's content), so the counters are readable from the store without host access.
    /// </summary>
    private static string NoteOf(CollectorRunResult result) => result.Note ?? "";

    private static async Task WithRigAsync(string name, Func<Rig, string, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        var (_, sqlHost, user, password) = Env();

        /* #5158: the scratch name exists before anything is created and the try opens before the CREATE, so a throw anywhere
           in setup (the create, the seeding, the store migration, the connector) still reaches this finally and drops the
           database. The drop skips a database that was never created, and the PostgreSQL rows exist only once a rig does. */
        var database = "pm5158p_" + Guid.NewGuid().ToString("N")[..10];
        Rig? rig = null;
        var ok = false;
        try
        {
            rig = await StartAsync(name, database, ct);
            await body(rig, Connect(sqlHost, user, password, database), ct);
            ok = true;
        }
        finally
        {
            // The rig (and its store) is disposed last, as its await using did before; a null rig has nothing to dispose.
            await using (rig)
            {
                await DropScratchDatabaseAsync(database, ok);
                if (rig is not null)
                {
                    await LiveStoreCleanup.RunAsync(rig.PgConnectionString, ok, async (cleanup, cleanupCt) =>
                        await DeleteStoredRowsAsync(cleanup, rig.Server.ServerId, cleanupCt));
                }
            }
        }
    }

    private static async Task ExecOnTargetAsync(string targetConnectionString, CancellationToken ct, string sql, params string[] calls)
    {
        await using var target = new SqlConnection(targetConnectionString);
        await target.OpenAsync(ct);
        if (sql.Length > 0)
        {
            await Exec(target, sql, ct);
        }

        if (calls.Length > 0)
        {
            await ExecuteAsync(target, ct, calls);
        }
    }

    [Fact]
    public Task On_CycleTwoRendersNothing_AndTheRowsCarryTheDigest() => WithRigAsync("pm5158p-two", async (rig, _, ct) =>
    {
        var runner = NewRunner(rig.Store, "on");

        var first = await runner.RunAsync(ProcedureStatsCollector.Instance, rig.Server, ct);
        Assert.True(first.Rows >= 4, "four scratch procedures, one row each");
        Assert.True(Measured(first, "plans_rendered") >= 4, "a cold cache renders every module plan");

        var second = await runner.RunAsync(ProcedureStatsCollector.Instance, rig.Server, ct);
        Assert.True(second.Rows >= 4);
        Assert.Equal(0, Measured(second, "plans_rendered"));
        Assert.Equal(0, Measured(second, "plans_rendered_bytes"));
        Assert.True(Measured(second, "deferred_hit") >= 4);

        var stored = await StoredAsync(rig, ct);
        Assert.True(stored.Count >= 8);
        Assert.All(stored, row =>
        {
            Assert.True(row.HasDigest, "every row, reused or rendered, carries the plan digest");
            Assert.True(row.PlanResolves, "and resolves its plan through the dimension");
            Assert.True(row.Bytes > 0, "reused rows keep the measured plan size");
        });

        var note = NoteOf(second);
        Assert.Contains("plans_rendered=0", note, StringComparison.Ordinal);
        Assert.Contains("deferred_hit=", note, StringComparison.Ordinal);
    });

    [Fact]
    public Task On_TheCadenceGateStillDecidesWhenMissesRender_AndHitsCarryTheirDigestOnEveryCycle() => WithRigAsync("pm5158p-gate", async (rig, _, ct) =>
    {
        var runner = NewRunner(rig.Store, "on", planCycleInterval: 2);
        var runs = new List<CollectorRunResult>();
        for (var i = 0; i < 6; i++)
        {
            runs.Add(await runner.RunAsync(ProcedureStatsCollector.Instance, rig.Server, ct));
        }

        var firstCapture = runs.FindIndex(r => Measured(r, "plans_rendered") > 0);
        Assert.InRange(firstCapture, 0, 1); /* every other cycle may capture */

        /* A gated cycle with a cold cache renders nothing and ships no plan, as the gate always did. */
        for (var i = 0; i < firstCapture; i++)
        {
            Assert.Equal(0, Measured(runs[i], "plans_rendered"));
            Assert.True(Measured(runs[i], "deferred_miss") >= 4);
            Assert.Equal(0, Measured(runs[i], "deferred_hit"));
        }

        /* After the capture every cycle, gated or not, renders nothing and its hits carry the digest. */
        for (var i = firstCapture + 1; i < runs.Count; i++)
        {
            Assert.Equal(0, Measured(runs[i], "plans_rendered"));
            Assert.True(Measured(runs[i], "deferred_hit") >= 4);
        }

        var stored = await StoredAsync(rig, ct);
        Assert.True(stored.Count(r => r.HasDigest) >= 4 * (runs.Count - firstCapture), "hits on gated cycles carry the digest");
        Assert.True(stored.Count(r => !r.HasDigest && !r.PlanResolves) >= 4 * firstCapture, "gated cold cycles ship no plan");
    });

    [Fact]
    public Task On_AStatementCompiledForTheFirstTime_ChangesTheFingerprint_AndIsRenderedAgain() => WithRigAsync("pm5158p-new", async (rig, target, ct) =>
    {
        var runner = NewRunner(rig.Store, "on");
        await runner.RunAsync(ProcedureStatsCollector.Instance, rig.Server, ct);
        var warm = await runner.RunAsync(ProcedureStatsCollector.Instance, rig.Server, ct);
        Assert.Equal(0, Measured(warm, "plans_rendered"));

        /* pm_d's second branch compiles the first time it is taken: same plan handle, same cached_time, one more statement
           in the module's plan. A key without the fingerprint would hit and keep the old plan's digest. */
        await ExecOnTargetAsync(target, ct, "", "pm_d 1");

        var changed = await runner.RunAsync(ProcedureStatsCollector.Instance, rig.Server, ct);
        Assert.Equal(1, Measured(changed, "plans_rendered"));
        Assert.True(Measured(changed, "deferred_miss") >= 1);
    });

    [Fact]
    public Task On_AStatementRecompiledInPlace_ChangesTheFingerprint_AndIsRenderedAgain() => WithRigAsync("pm5158p-recomp", async (rig, target, ct) =>
    {
        var runner = NewRunner(rig.Store, "on");
        await runner.RunAsync(ProcedureStatsCollector.Instance, rig.Server, ct);
        var warm = await runner.RunAsync(ProcedureStatsCollector.Instance, rig.Server, ct);
        Assert.Equal(0, Measured(warm, "plans_rendered"));

        /* New statistics make the next execution recompile pm_s's one statement inside the cached module plan. */
        await ExecOnTargetAsync(
            target, ct,
            "INSERT dbo.pm_data (k, v) SELECT TOP (5000) 2, ROW_NUMBER() OVER (ORDER BY (SELECT 1)) FROM sys.all_columns AS a CROSS JOIN sys.all_columns AS b; UPDATE STATISTICS dbo.pm_data WITH FULLSCAN;",
            "pm_s");

        var changed = await runner.RunAsync(ProcedureStatsCollector.Instance, rig.Server, ct);
        Assert.Equal(1, Measured(changed, "plans_rendered"));
    });

    [Fact]
    public Task Shadow_OnAnUnchangedModule_CountsWouldHits_AndNoFalseHit_AndWritesWhatOffWrites() => WithRigAsync("pm5158p-shadow", async (rig, _, ct) =>
    {
        var log = new CapturingTestLogger();
        var runner = NewRunner(rig.Store, "shadow", logger: log);

        var first = await runner.RunAsync(ProcedureStatsCollector.Instance, rig.Server, ct);
        Assert.Equal(0, Measured(first, "deferred_would_hit"));
        Assert.True(Measured(first, "deferred_miss") >= 4);
        Assert.True(Measured(first, "plans_rendered") >= 4, "shadow renders inline, exactly as off");

        var second = await runner.RunAsync(ProcedureStatsCollector.Instance, rig.Server, ct);
        Assert.True(Measured(second, "deferred_would_hit") >= 4);
        Assert.Equal(0, Measured(second, "deferred_false_hit"));
        Assert.True(Measured(second, "plans_rendered") >= 4, "shadow still renders every plan");

        var stored = await StoredAsync(rig, ct);
        Assert.All(stored, row => Assert.True(row.PlanResolves, "shadow stores every row's plan inline, as off does"));

        var note = NoteOf(second);
        Assert.Contains("deferred_would_hit=", note, StringComparison.Ordinal);
        Assert.Contains("deferred_false_hit=0", note, StringComparison.Ordinal);
        Assert.Contains("plans_rendered=", note, StringComparison.Ordinal);
        Assert.Contains(log.Lines, line => line.Contains("deferred: would_hit=", StringComparison.Ordinal) && line.Contains("false_hit=0", StringComparison.Ordinal));
        Assert.Equal(0, log.CountAtLevel(LogLevel.Warning));
    });

    /// <summary>Every stored column of the scratch modules' rows as JSON, minus the ones that differ by nature between two runs.</summary>
    private static async Task<Dictionary<long, (string Object, string Json)>> StoredColumnsAsync(Rig rig, CancellationToken ct)
    {
        await using var connection = await rig.Store.OpenConnectionAsync(ct);
        using var command = new NpgsqlCommand(
            "SELECT collection_id, object_name, (to_jsonb(p) - 'collection_id' - 'collection_time')::text "
            + "FROM procedure_stats p WHERE p.server_id = $1 AND p.database_name = $2 AND p.object_name LIKE 'pm\\_%'", connection);
        command.Parameters.AddWithValue(rig.Server.ServerId);
        command.Parameters.AddWithValue(rig.Database);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new Dictionary<long, (string, string)>();
        while (await reader.ReadAsync(ct))
        {
            rows[reader.GetInt64(0)] = (reader.GetString(1), reader.GetString(2));
        }

        return rows;
    }

    [Fact]
    public Task Shadow_StoresExactlyTheColumnsOffStores_ForTheSameProcedures() => WithRigAsync("pm5158p-equal", async (rig, _, ct) =>
    {
        /* One cycle in off, then one in shadow, each from a fresh runner (so each has an empty cache and a fresh delta
           baseline), over procedures nothing runs in between. */
        var offRunner = NewRunner(rig.Store, "off");
        await offRunner.RunAsync(ProcedureStatsCollector.Instance, rig.Server, ct);
        var offRows = await StoredColumnsAsync(rig, ct);
        Assert.True(offRows.Count >= 4, "four scratch procedures in the off cycle");

        var shadowRunner = NewRunner(rig.Store, "shadow");
        var shadowRun = await shadowRunner.RunAsync(ProcedureStatsCollector.Instance, rig.Server, ct);
        Assert.True(Measured(shadowRun, "deferred_miss") >= 4, "the shadow cycle ran in shadow");
        var shadowRows = (await StoredColumnsAsync(rig, ct)).Where(r => !offRows.ContainsKey(r.Key)).Select(r => r.Value).ToList();
        Assert.Equal(offRows.Count, shadowRows.Count);

        /* Only collection_id and collection_time differ by nature between two cycles (dropped in the query). Every other
           column, the plan, its digest and its size included, must be equal object by object. */
        var offByObject = offRows.Values.ToDictionary(v => v.Object, v => v.Json, StringComparer.Ordinal);
        foreach (var (name, json) in shadowRows)
        {
            Assert.True(offByObject.TryGetValue(name, out var offJson), "shadow stored a row off did not: " + name);
            Assert.Equal(offJson, json);
        }

        Assert.All(shadowRows, row => Assert.Contains("\"query_plan_xml_bytes\"", row.Json, StringComparison.Ordinal));
    });

    [Fact]
    public Task Shadow_WhenAStaleEntryIsForcedIntoTheCache_CountsAFalseHit_AndWarnsWithTheKeyOnly() => WithRigAsync("pm5158p-false", async (rig, _, ct) =>
    {
        var log = new CapturingTestLogger();
        var runner = NewRunner(rig.Store, "shadow", logger: log);
        await runner.RunAsync(ProcedureStatsCollector.Instance, rig.Server, ct);

        /* Replace every cached digest with one that cannot match the rendered plan, as a fingerprint that missed a change would. */
        var caches = (System.Collections.IDictionary)typeof(DarlingCollectorRunner)
            .GetField("_procedureStatsPlanCaches", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .GetValue(runner)!;
        var cache = (PlanDigestCache<ProcedureStatsPlanKey>)caches[rig.Server.ServerId]!;
        var entries = (Dictionary<ProcedureStatsPlanKey, PlanDigestEntry>)typeof(PlanDigestCache<ProcedureStatsPlanKey>)
            .GetField("_entries", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .GetValue(cache)!;
        var keys = entries.Keys.ToList();
        Assert.True(keys.Count >= 4);
        foreach (var key in keys)
        {
            cache.AddPending(key, "00FF", 1, DateTime.UtcNow, 1);
        }

        cache.ConfirmPending(keys, DateTime.UtcNow);

        var second = await runner.RunAsync(ProcedureStatsCollector.Instance, rig.Server, ct);
        Assert.True(Measured(second, "deferred_false_hit") >= 4);
        Assert.Equal(Measured(second, "deferred_would_hit"), Measured(second, "deferred_false_hit"));

        var note = NoteOf(second);
        Assert.Contains("deferred_false_hit=", note, StringComparison.Ordinal);
        Assert.DoesNotContain("deferred_false_hit=0", note, StringComparison.Ordinal);

        var warnings = log.Lines.Where(l => l.StartsWith("Warning:", StringComparison.Ordinal) && l.Contains("false hit", StringComparison.Ordinal)).ToList();
        Assert.True(warnings.Count >= 4);
        Assert.All(warnings, w =>
        {
            Assert.Contains("plan_handle 0x", w, StringComparison.Ordinal);
            Assert.DoesNotContain("pm_", w, StringComparison.Ordinal);
            Assert.DoesNotContain("<ShowPlanXML", w, StringComparison.Ordinal);
        });

        /* The stale cache changed nothing that was written. */
        Assert.All(await StoredAsync(rig, ct), row => Assert.True(row.PlanResolves));
    });
}
