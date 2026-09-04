/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.Metadata.Ecma335;
using System.Reflection.PortableExecutable;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using Microsoft.Data.SqlClient;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// M2 slice C: the Darling collector runner. The ungated tests pin the target-detection query
/// (verbatim from Lite's ServerManager, so both SKUs classify a server identically). The full
/// SQL Server → runner → Postgres E2E runs only when BOTH DARLING_TEST_PG (Postgres connection
/// string) and DARLING_TEST_SQL (SQL Server host; optional DARLING_TEST_SQL_USER /
/// DARLING_TEST_SQL_PASSWORD for sql auth) are set — it collects real wait_stats through the
/// shared WaitStatsCollector definition twice, proving watermarks, deltas, and binary COPY
/// against live engines.
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so
   cross-test row churn (inserts/purges/deletes) cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class DarlingCollectorRunnerTests
{
    [Fact]
    public void DetectionQuery_MatchesLiteServerManagerProbe()
    {
        Assert.Contains("SERVERPROPERTY('ProductMajorVersion')", DarlingServerConnector.DetectionQueryText, StringComparison.Ordinal);
        Assert.Contains("SERVERPROPERTY('EngineEdition')", DarlingServerConnector.DetectionQueryText, StringComparison.Ordinal);
        Assert.Contains("DB_ID('rdsadmin')", DarlingServerConnector.DetectionQueryText, StringComparison.Ordinal);
        Assert.Contains("HAS_DBACCESS(N'msdb')", DarlingServerConnector.DetectionQueryText, StringComparison.Ordinal);

        // #1535: edition detection must NOT depend on sys.dm_os_sys_info. On Azure SQL DB that DMV
        // requires VIEW DATABASE STATE; a monitoring login without it made the whole probe throw and
        // silently mis-detect Azure as on-prem (EngineEdition left 0). The detection query is now
        // permission-free scalars only - no DMV, and no sqlserver_start_time (its one DMV-bound column,
        // which the service never surfaced).
        Assert.DoesNotContain("dm_os_sys_info", DarlingServerConnector.DetectionQueryText, StringComparison.Ordinal);
        Assert.DoesNotContain("sqlserver_start_time", DarlingServerConnector.DetectionQueryText, StringComparison.Ordinal);
    }

    /// <summary>
    /// #1556 M1: the Darling Azure per-database read resolves its command timeout as
    /// <c>definition.CommandTimeoutSecondsOverride ?? CommandTimeoutSeconds</c> — the resolution the runner's
    /// Azure branch now applies (it previously passed the constant 60s cap, where Lite's twin already honored
    /// the override). index_object_stats declares a 300s per-database budget, so without the override its Azure
    /// per-database read would have timed out at 60s on a large database. This pins the resolution contract and
    /// the values it depends on; the runner's per-database read passes the resolved value verbatim.
    /// </summary>
    [Fact]
    public void PerDatabaseTimeout_HonorsCollectorOverride_FixingTheLatentSixtySecondCap()
    {
        Assert.Equal(60, DarlingCollectorRunner.CommandTimeoutSeconds);

        Assert.Equal(300, IndexObjectStatsCollector.Instance.CommandTimeoutSecondsOverride);
        Assert.Equal(300, IndexObjectStatsCollector.Instance.CommandTimeoutSecondsOverride ?? DarlingCollectorRunner.CommandTimeoutSeconds);

        /* A collector with no override falls back to the 60s default — the same resolution, unchanged. */
        Assert.Null(WaitStatsCollector.Instance.CommandTimeoutSecondsOverride);
        Assert.Equal(60, WaitStatsCollector.Instance.CommandTimeoutSecondsOverride ?? DarlingCollectorRunner.CommandTimeoutSeconds);
    }

    [Fact]
    public async Task EndToEnd_CollectWaitStats_FromLiveSqlServer_IntoLivePostgres()
    {
        var pg = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        var sqlHost = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        Assert.SkipWhen(string.IsNullOrEmpty(pg) || string.IsNullOrEmpty(sqlHost),
            "Set DARLING_TEST_PG and DARLING_TEST_SQL to run the live collection E2E.");

        var ct = TestContext.Current.CancellationToken;
        var sqlUser = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        var config = new MonitoredServer
        {
            Name = "darling-e2e",
            Host = sqlHost!,
            Auth = string.IsNullOrEmpty(sqlUser) ? "integrated" : "sql",
            Username = sqlUser,
            Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD"),
            TrustServerCertificate = true,
        };

        await using var dataSource = NpgsqlDataSource.Create(pg!);
        await using (var migrateConnection = await dataSource.OpenConnectionAsync(ct))
        {
            await PgMigrations.MigrateAsync(migrateConnection, ct);
        }

        var runtime = await DarlingServerConnector.ConnectAsync(config, null, ct);
        Assert.True(runtime.Target.SqlMajorVersion > 0, "probe should detect a real major version");
        Assert.Equal(PerformanceMonitor.Common.ServerIdHelper.GetDeterministicHashCode(config.StorageName), runtime.ServerId);

        var runner = new DarlingCollectorRunner(dataSource, new CollectorDeltaCalculator());

        /* Pre-clean: a prior service smoke against the same store leaves rows for this same
           server_id, and the exact-count assertion below would misread them as COPY errors. */
        await using (var precleanConnection = await dataSource.OpenConnectionAsync(ct))
        {
            using var preclean = new NpgsqlCommand("DELETE FROM wait_stats WHERE server_id = $1", precleanConnection);
            preclean.Parameters.AddWithValue(runtime.ServerId);
            await preclean.ExecuteNonQueryAsync(ct);
        }

        var bodySucceeded = false;
        try
        {
            /* First cycle: baselines — every wait row's deltas are 0 but rows land. */
            var first = await runner.RunAsync(WaitStatsCollector.Instance, runtime, ct);
            Assert.True(first.Rows > 0, "a live server always has wait stats");

            /* Second cycle: real deltas through the shared CollectorDeltaCalculator. */
            var second = await runner.RunAsync(WaitStatsCollector.Instance, runtime, ct);
            Assert.True(second.Rows > 0);

            await using var verifyConnection = await dataSource.OpenConnectionAsync(ct);
            using var count = new NpgsqlCommand("SELECT COUNT(*) FROM wait_stats WHERE server_id = $1", verifyConnection);
            count.Parameters.AddWithValue(runtime.ServerId);
            Assert.Equal((long)(first.Rows + second.Rows), await count.ExecuteScalarAsync(ct));

            /* The watermark helper sees what was just written. */
            var lastCollected = await runner.GetLastCollectedTimeAsync(runtime.ServerId, "wait_stats", "collection_time", ct);
            Assert.NotNull(lastCollected);

            bodySucceeded = true;
        }
        finally
        {
            /* The data source was never the exposed half here: it hands out a FRESH connection per
               call, so this teardown could not inherit a session the body had closed. What it lacked is
               the masking rule, and a token that survives a cancelled run — it used the body's `ct`, which
               on that path is already signalled, so the delete was skipped exactly when it mattered.
               LiveStoreCleanup supplies both, and its explicit SET search_path is a third thing the data
               source's own connections never did (#1902). */
            await LiveStoreCleanup.RunAsync(pg!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanServerRowsAsync(cleanup, "wait_stats", runtime.ServerId, cleanupCt));
        }
    }

    /// <summary>
    /// #1988: this test used to assert on whatever happened to be in the target's plan cache, and the
    /// cache's state decided the verdict twice over. First, under <c>optimize for ad hoc workloads</c> a
    /// query executed ONCE caches a Compiled Plan STUB — <c>dm_exec_query_stats</c> carries the row and
    /// its text renders, but <c>dm_exec_text_query_plan</c> renders NULL — so a box whose recent
    /// user-database activity was all once-executed ad-hoc text produced rows with no capturable plan,
    /// deterministically. Second, the assertions read the inline <c>query_plan_xml</c> column, which
    /// #1767's payload-dimension diversion writes NULL BY DESIGN (content goes to the digest-keyed
    /// <c>query_plan_dim</c>; every product reader coalesces inline with the dim join) — so even a row
    /// with a captured plan counted as "no plan stored". The test now seeds its own scratch database,
    /// runs a marker query TWICE with byte-identical text (the second execution replaces the stub with a
    /// full Compiled Plan), scopes the plan assertions to that marker row, and reads plans the way the
    /// product does. Neither ambient cache state nor sp_configure state can decide either direction.
    /// </summary>
    [Fact]
    public async Task EndToEnd_QueryStats_PlanCaptureFlag_TogglesStoredPlanXml()
    {
        var pg = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        var sqlHost = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        Assert.SkipWhen(string.IsNullOrEmpty(pg) || string.IsNullOrEmpty(sqlHost),
            "Set DARLING_TEST_PG and DARLING_TEST_SQL to run the live plan-capture E2E.");

        var ct = TestContext.Current.CancellationToken;
        var config = MakeLiveConfig("darling-plan-qs-e2e", sqlHost!);

        await using var dataSource = NpgsqlDataSource.Create(pg!);
        await using (var migrateConnection = await dataSource.OpenConnectionAsync(ct))
        {
            await PgMigrations.MigrateAsync(migrateConnection, ct);
        }

        var runtime = await DarlingServerConnector.ConnectAsync(config, null, ct);
        await CleanServerRowsAsync(dataSource, "query_stats", runtime.ServerId, ct);

        var bodySucceeded = false;
        try
        {
            await using (var seed = new SqlConnection(runtime.ConnectionString))
            {
                await seed.OpenAsync(ct);
                await SeedScratchMarkerAsync(seed, ct);
            }

            /* Flag OFF (Lite parity): the seeded marker lands, and NO row carries a reachable plan —
               inline or diverted. The marker makes this a hard assertion rather than the old
               skip-when-idle: the test now guarantees its own collectable activity. */
            var offRunner = new DarlingCollectorRunner(dataSource, new CollectorDeltaCalculator(), null, capturePlans: () => false);
            var off = await offRunner.RunAsync(QueryStatsCollector.Instance, runtime, ct);
            Assert.True(off.Rows > 0, "the seeded marker guarantees at least one collectable row");
            Assert.True(await CountMarkerRowsAsync(dataSource, runtime.ServerId, ct) > 0,
                "the flag-off cycle should collect the seeded marker row");
            Assert.Equal(0L, await CountReachablePlansAsync(dataSource, runtime.ServerId, ct));

            /* Reset so the flag-on cycle re-collects the same plans (query_stats has no watermark,
               but row-hash dedup on the store side would otherwise skip unchanged rows). */
            await CleanServerRowsAsync(dataSource, "query_stats", runtime.ServerId, ct);

            /* Flag ON (Darling): the MARKER row's plan — guaranteed a full Compiled Plan by the
               double execution — is reachable through the dim join and parses. Other rows may
               legitimately carry no plan (ambient stubs); they prove nothing either way. */
            var onRunner = new DarlingCollectorRunner(dataSource, new CollectorDeltaCalculator(), null, capturePlans: () => true);
            var on = await onRunner.RunAsync(QueryStatsCollector.Instance, runtime, ct);
            Assert.True(on.Rows > 0, "flag-on cycle should still land rows");
            Assert.True(await CountMarkerRowsAsync(dataSource, runtime.ServerId, ct) > 0,
                "the flag-on cycle should re-collect the seeded marker row");

            var planXml = await MarkerPlanXmlAsync(dataSource, runtime.ServerId, ct);
            Assert.False(string.IsNullOrEmpty(planXml), "flag-on capture should store the seeded marker's plan");
            Assert.Equal("ShowPlanXML", XDocument.Parse(planXml!).Root!.Name.LocalName);

            bodySucceeded = true;
        }
        finally
        {
            /* SQL-side teardown follows LiveStoreCleanup's rules: a FRESH connection (the body's
               session may be the thing that died) and a token that survives a cancelled run (#1902).
               A drop failure surfaces only when the body succeeded — otherwise the body's own
               exception is the report that matters. A leftover scratch database self-heals anyway:
               the seed phase recreates it. */
            try
            {
                await using var sqlCleanup = new SqlConnection(runtime.ConnectionString);
                await sqlCleanup.OpenAsync(CancellationToken.None);
                using var drop = new SqlCommand(DropScratchDatabaseSql, sqlCleanup) { CommandTimeout = 60 };
                await drop.ExecuteNonQueryAsync(CancellationToken.None);
            }
            catch when (!bodySucceeded)
            {
            }

            /* The data source was never the exposed half here: it hands out a FRESH connection per
               call, so this teardown could not inherit a session the body had closed. What it lacked is
               the masking rule, and a token that survives a cancelled run — it used the body's `ct`, which
               on that path is already signalled, so the delete was skipped exactly when it mattered.
               LiveStoreCleanup supplies both, and its explicit SET search_path is a third thing the data
               source's own connections never did (#1902). */
            await LiveStoreCleanup.RunAsync(pg!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanServerRowsAsync(cleanup, "query_stats", runtime.ServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task EndToEnd_QueryStore_PlanCaptureFlag_CapturesPlanText_WhenQueryStoreEnabled()
    {
        var pg = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        var sqlHost = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        Assert.SkipWhen(string.IsNullOrEmpty(pg) || string.IsNullOrEmpty(sqlHost),
            "Set DARLING_TEST_PG and DARLING_TEST_SQL to run the live plan-capture E2E.");

        var ct = TestContext.Current.CancellationToken;
        var config = MakeLiveConfig("darling-plan-qds-e2e", sqlHost!);

        await using var dataSource = NpgsqlDataSource.Create(pg!);
        await using (var migrateConnection = await dataSource.OpenConnectionAsync(ct))
        {
            await PgMigrations.MigrateAsync(migrateConnection, ct);
        }

        var runtime = await DarlingServerConnector.ConnectAsync(config, null, ct);
        await CleanServerRowsAsync(dataSource, "query_store_stats", runtime.ServerId, ct);

        var bodySucceeded = false;
        try
        {
            /* Flag ON: probe by collecting. Zero rows => no Query Store-enabled database with recent
               activity on the target — skip with reason rather than fail. */
            var onRunner = new DarlingCollectorRunner(dataSource, new CollectorDeltaCalculator(), null, capturePlans: () => true);
            var on = await onRunner.RunAsync(QueryStoreCollector.Instance, runtime, ct);
            Assert.SkipWhen(on.Rows == 0, "No Query Store-enabled database with recent activity on the target; skipping.");

            var planText = await FirstNonEmptyAsync(dataSource, "query_store_stats", "query_plan_text", runtime.ServerId, ct);
            Assert.False(string.IsNullOrEmpty(planText), "flag-on capture should store at least one non-empty Query Store plan");
            Assert.Equal("ShowPlanXML", XDocument.Parse(planText!).Root!.Name.LocalName);

            /* Flag OFF (Lite parity): reset, re-collect the same window, every plan is NULL. */
            await CleanServerRowsAsync(dataSource, "query_store_stats", runtime.ServerId, ct);
            var offRunner = new DarlingCollectorRunner(dataSource, new CollectorDeltaCalculator(), null, capturePlans: () => false);
            var off = await offRunner.RunAsync(QueryStoreCollector.Instance, runtime, ct);
            Assert.True(off.Rows > 0, "the same window should re-collect after the reset");
            Assert.Equal(0L, await CountNonEmptyAsync(dataSource, "query_store_stats", "query_plan_text", runtime.ServerId, ct));

            bodySucceeded = true;
        }
        finally
        {
            /* The data source was never the exposed half here: it hands out a FRESH connection per
               call, so this teardown could not inherit a session the body had closed. What it lacked is
               the masking rule, and a token that survives a cancelled run — it used the body's `ct`, which
               on that path is already signalled, so the delete was skipped exactly when it mattered.
               LiveStoreCleanup supplies both, and its explicit SET search_path is a third thing the data
               source's own connections never did (#1902). */
            await LiveStoreCleanup.RunAsync(pg!, bodySucceeded, async (cleanup, cleanupCt) =>
                await CleanServerRowsAsync(cleanup, "query_store_stats", runtime.ServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task GetLastCollectedInstanceIdAsync_ReturnsMaxInstanceId_ScopedPerServer()
    {
        /* The numeric (bigint) watermark helper behind job_history's instance_id dedup (#1433). Gated on
           Postgres only — it seeds job_history rows directly and reads MAX(instance_id), proving per-server
           scoping and the first-run null, the numeric twin of GetLastCollectedTimeAsync (line 102). */
        var pg = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(pg), "Set DARLING_TEST_PG to run the numeric-watermark read.");

        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = NpgsqlDataSource.Create(pg!);
        await using (var migrateConnection = await dataSource.OpenConnectionAsync(ct))
        {
            await PgMigrations.MigrateAsync(migrateConnection, ct);
        }

        var runner = new DarlingCollectorRunner(dataSource, new CollectorDeltaCalculator());
        const int serverId = -880088;
        const int otherServerId = -880099;

        await CleanServerRowsAsync(dataSource, "job_history", serverId, ct);
        await CleanServerRowsAsync(dataSource, "job_history", otherServerId, ct);
        var bodySucceeded = false;
        try
        {
            /* First run: no rows for the server yet → null (caller collects all history). */
            Assert.Null(await runner.GetLastCollectedInstanceIdAsync(serverId, "job_history", "instance_id", ct));

            /* Two rows for the server + a higher instance_id for a DIFFERENT server, to prove per-server scoping. */
            await InsertJobHistoryInstanceAsync(dataSource, ct, serverId, jobHistoryId: 9_100_001, instanceId: 100);
            await InsertJobHistoryInstanceAsync(dataSource, ct, serverId, jobHistoryId: 9_100_002, instanceId: 250);
            await InsertJobHistoryInstanceAsync(dataSource, ct, otherServerId, jobHistoryId: 9_100_003, instanceId: 9999);

            var max = await runner.GetLastCollectedInstanceIdAsync(serverId, "job_history", "instance_id", ct);
            Assert.Equal(250L, max);

            bodySucceeded = true;
        }
        finally
        {
            /* The data source was never the exposed half here: it hands out a FRESH connection per
               call, so this teardown could not inherit a session the body had closed. What it lacked is
               the masking rule, and a token that survives a cancelled run — it used the body's `ct`, which
               on that path is already signalled, so the delete was skipped exactly when it mattered.
               LiveStoreCleanup supplies both, and its explicit SET search_path is a third thing the data
               source's own connections never did (#1902). */
            await LiveStoreCleanup.RunAsync(pg!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await CleanServerRowsAsync(cleanup, "job_history", serverId, cleanupCt);
                await CleanServerRowsAsync(cleanup, "job_history", otherServerId, cleanupCt);
            });
        }
    }

    private static async Task InsertJobHistoryInstanceAsync(
        NpgsqlDataSource dataSource, CancellationToken ct, int serverId, long jobHistoryId, long instanceId)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        using var command = new NpgsqlCommand(@"
INSERT INTO job_history (job_history_id, collection_time, server_id, server_name, instance_id)
VALUES ($1, $2, $3, $4, $5)", connection);
        command.Parameters.AddWithValue(jobHistoryId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue("NUM-WM-SRV");
        command.Parameters.AddWithValue(instanceId);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// The scratch database the plan-capture E2E creates on the DARLING_TEST_SQL target. query_stats
    /// only collects USER-database activity (its dbid filter excludes the system databases), so the
    /// marker needs a user database to run in — and a fresh test box has none. Requires CREATE DATABASE
    /// on the target, which the gated-live contract already implies: DARLING_TEST_SQL is a disposable
    /// test server, never a production one.
    /// </summary>
    private const string ScratchDatabaseName = "darling_qs_e2e_scratch";

    /// <summary>The token the plan-capture assertions key on, present only in the marker's text.</summary>
    private const string MarkerToken = "darling_qs_e2e_marker";

    /* ~2M-row serial scan: enough total_elapsed_time that ranking into the collector's cheap-columns
       TOP is not decided by a rounding error, bounded enough to stay well under the command timeout. */
    private const string MarkerQueryText =
        "SELECT " + MarkerToken + " = COUNT_BIG(*) FROM (SELECT TOP (2000000) ac.column_id " +
        "FROM sys.all_columns AS ac CROSS JOIN sys.all_columns AS ac2) AS q OPTION(MAXDOP 1);";

    private static readonly string RecreateScratchDatabaseSql = $@"
IF DB_ID(N'{ScratchDatabaseName}') IS NOT NULL
BEGIN
    ALTER DATABASE [{ScratchDatabaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [{ScratchDatabaseName}];
END;
CREATE DATABASE [{ScratchDatabaseName}];";

    private static readonly string DropScratchDatabaseSql = $@"
IF DB_ID(N'{ScratchDatabaseName}') IS NOT NULL
BEGIN
    ALTER DATABASE [{ScratchDatabaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [{ScratchDatabaseName}];
END;";

    /// <summary>
    /// Recreates the scratch database (a crashed prior run may have left one behind) and runs the
    /// marker TWICE with byte-identical text on the same session. Twice is the load-bearing part:
    /// under <c>optimize for ad hoc workloads</c> the first execution caches only a Compiled Plan
    /// Stub, whose plan renders NULL from <c>dm_exec_text_query_plan</c>; the identical second
    /// execution replaces the stub with a full Compiled Plan. One execution is exactly the state
    /// that made this test's verdict depend on the box's sp_configure settings (#1988).
    /// </summary>
    private static async Task SeedScratchMarkerAsync(SqlConnection connection, CancellationToken ct)
    {
        using (var recreate = new SqlCommand(RecreateScratchDatabaseSql, connection) { CommandTimeout = 60 })
        {
            await recreate.ExecuteNonQueryAsync(ct);
        }

        await connection.ChangeDatabaseAsync(ScratchDatabaseName, ct);
        for (var execution = 0; execution < 2; execution++)
        {
            using var marker = new SqlCommand(MarkerQueryText, connection) { CommandTimeout = 60 };
            await marker.ExecuteScalarAsync(ct);
        }
        await connection.ChangeDatabaseAsync("master", ct);
    }

    /// <summary>
    /// Counts collected rows whose text is the seeded marker's. Reads through #1767's payload
    /// diversion: new rows store text in <c>query_text_dim</c> keyed by <c>query_text_digest</c> and
    /// leave the inline column NULL, and every reader coalesces the two — asserting on the inline
    /// column alone is how this test failed deterministically after that change shipped (#1988).
    /// </summary>
    private static async Task<long> CountMarkerRowsAsync(NpgsqlDataSource dataSource, int serverId, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        using var command = new NpgsqlCommand(@"
SELECT COUNT(*)
FROM query_stats AS qs
LEFT JOIN query_text_dim AS td ON td.digest = qs.query_text_digest
WHERE qs.server_id = $1
AND COALESCE(qs.query_text, td.query_text) LIKE '%' || $2 || '%'", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(MarkerToken);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>Counts rows with a plan reachable inline OR through the plan dim — see CountMarkerRowsAsync.</summary>
    private static async Task<long> CountReachablePlansAsync(NpgsqlDataSource dataSource, int serverId, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        /* #2069: plans written since V54 land as gzip bytes in the dim (text NULL), so "reachable"
           means EITHER form — the same text-else-gz rule every product reader applies. */
        using var command = new NpgsqlCommand(@"
SELECT COUNT(*)
FROM query_stats AS qs
LEFT JOIN query_plan_dim AS pd ON pd.digest = qs.query_plan_digest
WHERE qs.server_id = $1
AND ((COALESCE(qs.query_plan_xml, pd.query_plan_xml) IS NOT NULL
      AND COALESCE(qs.query_plan_xml, pd.query_plan_xml) <> '')
  OR pd.query_plan_gz IS NOT NULL)", connection);
        command.Parameters.AddWithValue(serverId);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>The seeded marker row's plan XML, reachable inline or through the plan dim — see CountMarkerRowsAsync.</summary>
    private static async Task<string?> MarkerPlanXmlAsync(NpgsqlDataSource dataSource, int serverId, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        using var command = new NpgsqlCommand(@"
SELECT COALESCE(qs.query_plan_xml, pd.query_plan_xml), pd.query_plan_gz
FROM query_stats AS qs
LEFT JOIN query_text_dim AS td ON td.digest = qs.query_text_digest
LEFT JOIN query_plan_dim AS pd ON pd.digest = qs.query_plan_digest
WHERE qs.server_id = $1
AND COALESCE(qs.query_text, td.query_text) LIKE '%' || $2 || '%'
AND ((COALESCE(qs.query_plan_xml, pd.query_plan_xml) IS NOT NULL
      AND COALESCE(qs.query_plan_xml, pd.query_plan_xml) <> '')
  OR pd.query_plan_gz IS NOT NULL)
LIMIT 1", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(MarkerToken);
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        /* #2069: text-else-gz, the product readers' rule — the real runner now stores gz. */
        return PayloadDimensions.ResolveContent(
            reader.IsDBNull(0) ? null : reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetFieldValue<byte[]>(1));
    }

    private static MonitoredServer MakeLiveConfig(string name, string sqlHost)
    {
        var sqlUser = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        return new MonitoredServer
        {
            Name = name,
            Host = sqlHost,
            Auth = string.IsNullOrEmpty(sqlUser) ? "integrated" : "sql",
            Username = sqlUser,
            Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD"),
            TrustServerCertificate = true,
        };
    }

    /// <summary>
    /// The pre-clean form, which legitimately draws a connection from the data source the test already holds.
    /// TEARDOWN does not use this overload — see the connection overload below (#1902).
    /// </summary>
    private static async Task CleanServerRowsAsync(NpgsqlDataSource dataSource, string table, int serverId, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await CleanServerRowsAsync(connection, table, serverId, ct);
    }

    private static async Task CleanServerRowsAsync(NpgsqlConnection connection, string table, int serverId, CancellationToken ct)
    {
        using var command = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(serverId);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> CountNonEmptyAsync(NpgsqlDataSource dataSource, string table, string column, int serverId, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        using var command = new NpgsqlCommand(
            $"SELECT COUNT(*) FROM {table} WHERE server_id = $1 AND {column} IS NOT NULL AND {column} <> ''", connection);
        command.Parameters.AddWithValue(serverId);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<string?> FirstNonEmptyAsync(NpgsqlDataSource dataSource, string table, string column, int serverId, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        using var command = new NpgsqlCommand(
            $"SELECT {column} FROM {table} WHERE server_id = $1 AND {column} IS NOT NULL AND {column} <> '' LIMIT 1", connection);
        command.Parameters.AddWithValue(serverId);
        return await command.ExecuteScalarAsync(ct) as string;
    }
}

/// <summary>
/// #2795: the SERVER-scoped watermark read must be boundable on the partitioning column, the way #2344
/// already made the per-database one.
///
/// <para><b>Why this is a category pin and not a one-line regression test.</b> #2344 fixed the unbounded
/// <c>MAX</c> over a non-partitioning column — but only in
/// <see cref="DarlingCollectorRunner.GetLastCollectedTimeForDatabaseAsync"/>, and it pinned the POLICY
/// (<c>WatermarkPolicyTests</c>) rather than the policy's APPLICATION. query_store declares both
/// <c>WatermarkColumn</c> and <c>PerDatabaseWatermarkColumn</c>, so the server-scoped sibling kept running
/// unbounded on every cycle and nothing in the suite could see it. Measured on use1: 40.7 s and 50.6 s
/// against Npgsql's 30 s default, cancelled 2,092 times in a day while the bounded sibling was cancelled
/// 17 times.</para>
///
/// <para>So these assert over the method FAMILY by reflection rather than naming one method: a third
/// timestamp-watermark reader added later is covered without anyone remembering to come back here.</para>
/// </summary>
public class ServerWatermarkReadFloorTests
{
    /// <summary>
    /// Every timestamp-watermark reader on the runner accepts a <c>collectedSince</c> bound. Derived from
    /// the type, so it cannot go stale against a new sibling the way #2344's fix did.
    /// </summary>
    [Fact]
    public void EveryTimestampWatermarkReader_AcceptsACollectedSinceBound()
    {
        var readers = typeof(DarlingCollectorRunner)
            .GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
            .Where(m => m.Name.StartsWith("GetLastCollectedTime", StringComparison.Ordinal)
                        && m.Name.EndsWith("Async", StringComparison.Ordinal))
            .ToList();

        Assert.NotEmpty(readers);

        foreach (var reader in readers)
        {
            Assert.True(
                reader.GetParameters().Any(p =>
                    string.Equals(p.Name, "collectedSince", StringComparison.Ordinal)
                    && p.ParameterType == typeof(DateTime?)),
                $"{reader.Name} reads a MAX over a non-partitioning timestamp column; without a "
                + "collection_time bound it scans every chunk in retention on every cycle (#2344/#2795)");
        }
    }

    /// <summary>
    /// The bound lands on <c>collection_time</c> — the partitioning column — because that is the only
    /// predicate here that prunes a chunk. Asserted against the SHIPPED string, not a retyped copy.
    /// </summary>
    [Fact]
    public void TheBoundedServerWatermarkSql_PredicatesOnThePartitioningColumn()
    {
        var bounded = DarlingCollectorRunner.BuildServerWatermarkSql("query_store_stats", "last_execution_time", bounded: true);
        var unbounded = DarlingCollectorRunner.BuildServerWatermarkSql("query_store_stats", "last_execution_time", bounded: false);

        Assert.Contains("collection_time > $2", bounded, StringComparison.Ordinal);
        Assert.DoesNotContain("collection_time", unbounded, StringComparison.Ordinal);

        /* Both forms still answer the same question about the same column. */
        Assert.Contains("MAX(last_execution_time)", bounded, StringComparison.Ordinal);
        Assert.Contains("MAX(last_execution_time)", unbounded, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", bounded, StringComparison.Ordinal);
    }
}

/// <summary>
/// #2797: the server-scoped watermark read must be skipped when — and ONLY when — this cycle will
/// overwrite <c>context.Watermark</c> with a per-database value before any query is built.
///
/// <para><b>Why this class exists in the shape it does.</b> #2797 proposed gating the read on
/// <c>PerDatabaseWatermarkColumn is not null</c>, the signal the per-item paths already key on. That gate
/// would ship a data-correctness bug, and the assertion that catches it is
/// <see cref="TheServerScopedMembersOfTheFamily_KeepTheirWatermark_WhichTheProposedGateWouldHaveDestroyed"/>.
/// Four collectors declare BOTH watermark columns; only query_store overrides
/// <c>BuildEnumerationQuery</c>, so off Azure the other three fall through to the plain server-scoped path
/// and genuinely CONSUME the value. Gating on the column alone nulls their watermark every cycle and makes
/// them re-collect their whole first-run window forever — #2795's defect, reached from the other side.</para>
///
/// <para><b>Every set here is DERIVED from <see cref="CollectorCatalog.All"/>,</b> the way #2796's pins are
/// derived from the runner's method family and for the same reason: a fifth collector that declares both
/// columns and enumerates is covered the day it appears, and one that stops enumerating is un-covered
/// automatically. No test here names a collector to decide what it expects. The counts are floors rather
/// than equalities so the sets can grow on their own but cannot quietly shrink to nothing and pass
/// vacuously — the failure mode a derived pin has instead of a stale one.</para>
/// </summary>
public class ServerWatermarkDispatchGateTests
{
    /* The both-watermark-columns family, and the two dispatch shapes inside it, derived by invoking the
       definitions' OWN RunsPerDatabase / BuildEnumerationQuery. CollectorCatalog.All is typed as the
       non-generic ICollectorSchemaInfo, so the four members this question turns on — they live on
       ICollectorDefinition<TRow> — are reached by reflection. */
    private static readonly CollectorTargetInfo OnPrem = new() { SqlMajorVersion = 16 };
    private static readonly CollectorTargetInfo AzureSqlDb = new() { IsAzureSqlDb = true, SqlMajorVersion = 12 };
    private static readonly CollectorTargetInfo ManagedInstance = new() { IsAzureManagedInstance = true, SqlMajorVersion = 15 };
    private static readonly CollectorTargetInfo AwsRds = new() { IsAwsRds = true, SqlMajorVersion = 16 };

    private static (string Name, CollectorTargetInfo Target)[] AllTargets() =>
    [
        ("on-prem", OnPrem), ("azure-sql-db", AzureSqlDb), ("managed-instance", ManagedInstance), ("aws-rds", AwsRds),
    ];

    private static CollectorContext Probe(CollectorTargetInfo target) => new()
    {
        ServerId = 1,
        ServerName = "alpha",
        CollectionTime = new DateTime(2026, 9, 4, 12, 0, 0, DateTimeKind.Utc),
        Deltas = new CollectorDeltaCalculator(),
        Target = target,
    };

    private static string? Column(ICollectorSchemaInfo definition, string property) =>
        (string?)definition.GetType()
            .GetProperty(property, BindingFlags.Public | BindingFlags.Instance)
            ?.GetValue(definition);

    private static bool RunsPerDatabase(ICollectorSchemaInfo definition, CollectorTargetInfo target) =>
        (bool)definition.GetType()
            .GetMethod("RunsPerDatabase", BindingFlags.Public | BindingFlags.Instance, [typeof(CollectorTargetInfo)])!
            .Invoke(definition, [target])!;

    private static bool Enumerates(ICollectorSchemaInfo definition, CollectorContext context)
    {
        var method = definition.GetType()
            .GetMethod("BuildEnumerationQuery", BindingFlags.Public | BindingFlags.Instance, [typeof(CollectorContext)])!;
        try
        {
            return method.Invoke(definition, [context]) is not null;
        }
        catch (TargetInvocationException)
        {
            /* A definition that throws off its own path (query_store's BuildQuery does) is not enumerating. */
            return false;
        }
    }

    /// <summary>
    /// The SHIPPED predicate, not a retyped copy of it — the discipline #2796 established with
    /// <c>BuildServerWatermarkSql</c>. Reached through the generic definition interface the runner itself
    /// is handed, so the row type comes from the definition rather than being guessed.
    /// </summary>
    private static bool ShippedGate(ICollectorSchemaInfo definition, CollectorTargetInfo target)
    {
        var rowType = definition.GetType().GetInterfaces()
            .First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ICollectorDefinition<>))
            .GetGenericArguments()[0];

        var gate = typeof(DarlingCollectorRunner)
            .GetMethod(nameof(DarlingCollectorRunner.ServerWatermarkIsDiscarded),
                BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)!;

        return (bool)gate.MakeGenericMethod(rowType).Invoke(null, [definition, Probe(target)])!;
    }

    /// <summary>Definitions declaring both watermark columns — the family the whole question is about.</summary>
    private static List<ICollectorSchemaInfo> BothColumnsFamily() =>
        CollectorCatalog.All
            .Where(d => d.TargetEngine == CollectorTargetEngine.SqlServer
                        && Column(d, "WatermarkColumn") is not null
                        && Column(d, "PerDatabaseWatermarkColumn") is not null)
            .ToList();

    [Fact]
    public void TheFamilyAndBothItsDispatchShapes_AreNonEmpty_SoTheAssertionsBelowCannotPassVacuously()
    {
        var family = BothColumnsFamily();
        var enumerating = family.Where(d => Enumerates(d, Probe(OnPrem))).ToList();
        var serverScoped = family.Where(d => !RunsPerDatabase(d, OnPrem) && !Enumerates(d, Probe(OnPrem))).ToList();

        /* Floors, not equalities: the set may grow. Four is what the catalog held when this was written
           (query_store, deadlocks, blocked_process_report, long_query_completions), and the SPLIT — one
           enumerating, three server-scoped — is the fact #2797's proposed gate got wrong. If either shape
           empties, the pins below stop discriminating and this fails with a number instead. */
        Assert.True(family.Count >= 4,
            $"Only {family.Count} definitions declare both watermark columns; expected at least 4. The "
            + "family shrank, so the assertions below are guarding less than they were written to guard.");
        Assert.True(enumerating.Count >= 1,
            $"No member of the both-columns family enumerates off Azure ({family.Count} in the family). "
            + "The gate's enumeration arm is then untested.");
        Assert.True(serverScoped.Count >= 3,
            $"Only {serverScoped.Count} members of the both-columns family take the PLAIN path off Azure; "
            + "expected at least 3. That set IS the disproof of #2797's proposed gate — if it empties, "
            + "nothing here catches the wrong fix any more.");
    }

    [Fact]
    public void AnEnumeratingMemberOfTheFamily_SkipsTheServerScopedRead_BecauseThePerItemLoopOverwritesIt()
    {
        var enumerating = BothColumnsFamily().Where(d => Enumerates(d, Probe(OnPrem))).ToList();
        Assert.NotEmpty(enumerating);

        foreach (var definition in enumerating)
        {
            Assert.True(
                ShippedGate(definition, OnPrem),
                $"{definition.Name} enumerates off Azure, so the enumeration loop assigns context.Watermark "
                + "from the per-database read before any per-item query is built. The server-scoped read's "
                + "answer is therefore read by nothing and must be skipped.");
        }
    }

    [Fact]
    public void TheServerScopedMembersOfTheFamily_KeepTheirWatermark_WhichTheProposedGateWouldHaveDestroyed()
    {
        /* THE assertion this class exists for. #2797's own suggested gate —
           `definition.PerDatabaseWatermarkColumn is not null` — is true for every member of this family,
           so it would skip the read for all of them. These ones CONSUME it: off Azure they take neither
           fan-out path, so nothing overwrites context.Watermark and definition.BuildQuery(context) reads
           the server-scoped value directly. Skipping it there hands them null every cycle, which is
           indistinguishable from a first run, and they re-collect their whole fallback window forever. */
        var family = BothColumnsFamily();
        var serverScoped = family
            .Where(d => !RunsPerDatabase(d, OnPrem) && !Enumerates(d, Probe(OnPrem)))
            .ToList();

        Assert.NotEmpty(serverScoped);

        foreach (var definition in serverScoped)
        {
            Assert.False(
                ShippedGate(definition, OnPrem),
                $"{definition.Name} declares PerDatabaseWatermarkColumn but takes the PLAIN server-scoped "
                + "path off Azure — it does not run per database and does not enumerate — so it CONSUMES "
                + "the server-scoped watermark. Skipping the read for it sets the watermark null on every "
                + "cycle and it re-collects its entire first-run window forever (#2795, from the other "
                + "side). This is exactly what gating on PerDatabaseWatermarkColumn alone would do.");
        }

        /* And the same three on the other non-Azure targets the fleet actually runs, because
           RunsPerDatabase keys on IsAzureSqlDb and nothing else. */
        foreach (var definition in serverScoped)
        {
            Assert.False(ShippedGate(definition, ManagedInstance), $"{definition.Name} on Managed Instance");
            Assert.False(ShippedGate(definition, AwsRds), $"{definition.Name} on AWS RDS");
        }
    }

    [Fact]
    public void EveryMemberOfTheFamily_SkipsOnAzureSqlDb_WhereThePerDatabaseConnectionLoopOverwritesIt()
    {
        var family = BothColumnsFamily();
        Assert.NotEmpty(family);

        foreach (var definition in family)
        {
            /* All four declare RunsPerDatabase => target.IsAzureSqlDb, and that loop assigns
               context.Watermark from GetLastCollectedTimeForDatabaseAsync inside itself. */
            Assert.True(RunsPerDatabase(definition, AzureSqlDb));
            Assert.True(
                ShippedGate(definition, AzureSqlDb),
                $"{definition.Name} runs per database on Azure SQL DB, so the per-database connection loop "
                + "overwrites the server-scoped watermark before the query is built.");
        }
    }

    [Fact]
    public void ACollectorWithNoPerDatabaseWatermark_AlwaysReads_HoweverItDispatches()
    {
        /* The control, and it is load-bearing rather than decorative: the per-item refresh on BOTH fan-out
           paths is wired only when both columns are declared, so a collector that enumerates with no
           PerDatabaseWatermarkColumn keeps the single server-wide value and still consumes it. The gate
           must not key on the dispatch path alone either. */
        var singleWatermark = CollectorCatalog.All
            .Where(d => d.TargetEngine == CollectorTargetEngine.SqlServer
                        && Column(d, "WatermarkColumn") is not null
                        && Column(d, "PerDatabaseWatermarkColumn") is null)
            .ToList();

        Assert.True(singleWatermark.Count >= 3,
            $"Only {singleWatermark.Count} collectors declare a watermark with no per-database column; "
            + "expected at least 3. This control is then no longer discriminating.");

        foreach (var definition in singleWatermark)
        {
            foreach (var (name, target) in AllTargets())
            {
                Assert.False(
                    ShippedGate(definition, target),
                    $"{definition.Name} on {name} declares no PerDatabaseWatermarkColumn, so no path ever "
                    + "overwrites its server-scoped watermark. It must keep reading.");
            }
        }
    }

    [Fact]
    public void EnumerationNullness_IsAFunctionOfTargetAlone_SoTheProbeContextCannotDisagreeWithDispatch()
    {
        /* The soundness condition for the whole design, asserted rather than assumed.
           The gate has to answer "will this cycle enumerate?" BEFORE the cycle's own CollectorContext can
           exist — HasCollectedBefore is computed from the very read being gated, and it, NumericWatermark
           and State are init-only — so it asks the definition's real BuildEnumerationQuery against a probe
           context carrying the target and nothing the read produces.

           Deriving the answer by CALLING BuildEnumerationQuery is what stops a second "enumerates" flag
           drifting away from it. This pin closes the remaining gap: the probe is only equivalent to the
           dispatch call while null-ness depends on Target alone. Vary everything else and require the
           answer not to move. A future enumerator that keys its null-ness off the watermark fails HERE,
           where the reason is written down, instead of silently mis-gating a collector's watermark. */
        var varied = new List<Func<CollectorTargetInfo, CollectorContext>>();
        foreach (var watermark in new DateTime?[] { null, new DateTime(2026, 9, 4, 11, 0, 0, DateTimeKind.Utc) })
        foreach (var numeric in new long?[] { null, 12345L })
        foreach (var collectedBefore in new[] { false, true })
        foreach (var excluded in new[] { Array.Empty<string>(), new[] { "master", "SomeUserDb" } })
        foreach (var state in new IReadOnlyDictionary<string, string>[]
                 {
                     CollectorContext.NoState,
                     new Dictionary<string, string>(StringComparer.Ordinal) { ["qsowm:alpha"] = "2026-09-04T11:00:00Z" },
                 })
        {
            var w = watermark; var n = numeric; var c = collectedBefore; var x = excluded; var s = state;
            varied.Add(target => new CollectorContext
            {
                ServerId = 1,
                ServerName = "alpha",
                CollectionTime = new DateTime(2026, 9, 4, 12, 0, 0, DateTimeKind.Utc),
                Deltas = new CollectorDeltaCalculator(),
                Target = target,
                Watermark = w,
                NumericWatermark = n,
                HasCollectedBefore = c,
                ExcludedDatabases = x,
                State = s,
            });
        }

        var evaluations = 0;

        foreach (var definition in CollectorCatalog.All.Where(d => d.TargetEngine == CollectorTargetEngine.SqlServer))
        {
            foreach (var (name, target) in AllTargets())
            {
                var baseline = Enumerates(definition, varied[0](target));
                foreach (var build in varied)
                {
                    evaluations++;
                    Assert.Equal(baseline, Enumerates(definition, build(target)));
                }
            }
        }

        Assert.True(evaluations > 1000,
            $"Only {evaluations} (definition, target, context) combinations were evaluated — the sweep "
            + "collapsed and this pin is no longer checking the soundness condition it exists for.");
    }

    /// <summary>
    /// The gate has to sit ON the read, not merely exist near it. Read from IL rather than source text for
    /// the reason <c>ServerScopePhaseSplitTests</c> gives: a byte scan of this same assembly has previously
    /// reported a shipped change as absent.
    ///
    /// <para><b>What this does and does not prove.</b> It proves the gate is CALLED in every method body
    /// that performs the server-scoped watermark read, so deleting the gate — or adding a second, ungated
    /// read site — fails here. It cannot prove the branch polarity; that is what the behavioural pins above
    /// are for. The two together are the claim.</para>
    /// </summary>
    [Fact]
    public void EveryServerScopedWatermarkReadSite_AlsoCallsTheDispatchGate()
    {
        const string Read = nameof(DarlingCollectorRunner.GetLastCollectedTimeAsync);
        const string Gate = nameof(DarlingCollectorRunner.ServerWatermarkIsDiscarded);

        var assemblyPath = typeof(DarlingCollectorRunner).Assembly.Location;
        Assert.True(File.Exists(assemblyPath), $"Service assembly not found at '{assemblyPath}'.");

        using var stream = File.OpenRead(assemblyPath);
        using var peReader = new PEReader(stream);
        var metadata = peReader.GetMetadataReader();

        /* A call resolves through a MemberReference or a MethodDefinition depending on where the caller
           sits, and async state machines move the call into a generated MoveNext. Collect both forms. */
        var tokenToName = new Dictionary<int, string>();
        foreach (var handle in metadata.MemberReferences)
        {
            var name = metadata.GetString(metadata.GetMemberReference(handle).Name);
            if (name is Read or Gate)
            {
                tokenToName[MetadataTokens.GetToken(handle)] = name;
            }
        }

        foreach (var handle in metadata.MethodDefinitions)
        {
            var name = metadata.GetString(metadata.GetMethodDefinition(handle).Name);
            if (name is Read or Gate)
            {
                tokenToName[MetadataTokens.GetToken(handle)] = name;
            }
        }

        /* And the third form, which is the one that actually matters here and cost this pin its first
           red: a call to a GENERIC method is emitted against a MethodSpec token, not against the
           MethodDef or MemberRef the other two loops collect. ServerWatermarkIsDiscarded is generic in
           TRow, so without this the scan reported it as never called while it was called on every cycle.
           Resolved back to the underlying method so the name lookup is the same one. */
        var methodSpecCount = metadata.GetTableRowCount(TableIndex.MethodSpec);
        for (var row = 1; row <= methodSpecCount; row++)
        {
            var specHandle = MetadataTokens.MethodSpecificationHandle(row);
            var target = metadata.GetMethodSpecification(specHandle).Method;

            var name = target.Kind switch
            {
                HandleKind.MethodDefinition =>
                    metadata.GetString(metadata.GetMethodDefinition((MethodDefinitionHandle)target).Name),
                HandleKind.MemberReference =>
                    metadata.GetString(metadata.GetMemberReference((MemberReferenceHandle)target).Name),
                _ => null,
            };

            if (name is Read or Gate)
            {
                tokenToName[MetadataTokens.GetToken(specHandle)] = name;
            }
        }

        Assert.True(tokenToName.Count >= 2,
            $"Only {tokenToName.Count} of the two tracked members resolved to a metadata token — the scan "
            + "read nothing useful, so it would pass for reasons unrelated to the gate.");

        var readerBodies = 0;
        var gatedBodies = 0;
        var totalGateCalls = 0;

        foreach (var handle in metadata.MethodDefinitions)
        {
            var method = metadata.GetMethodDefinition(handle);
            if (method.RelativeVirtualAddress == 0)
            {
                continue;
            }

            var il = peReader.GetMethodBody(method.RelativeVirtualAddress).GetILBytes();
            if (il is null)
            {
                continue;
            }

            var callsRead = false;
            var callsGate = false;

            for (var i = 0; i + 4 < il.Length; i++)
            {
                /* call (0x28) and callvirt (0x6F), each followed by a 4-byte metadata token. */
                if (il[i] != 0x28 && il[i] != 0x6F)
                {
                    continue;
                }

                if (tokenToName.TryGetValue(BitConverter.ToInt32(il, i + 1), out var name))
                {
                    if (name == Read)
                    {
                        callsRead = true;
                    }
                    else
                    {
                        callsGate = true;
                        totalGateCalls++;
                    }
                }

                /* DELIBERATELY no `i += 4` here, unlike the older scan in ServerScopePhaseSplitTests.
                   This is an opcode-shaped byte scan, not a real IL decoder, so a byte inside some other
                   instruction's operand can look like a call and then the skip steps over the four bytes
                   AFTER it — which can be a genuine call's own token. Measured on this very assembly: the
                   single call to the generic gate sits at IL offset 743 of <RunAsync>MoveNext, and the
                   skipping form walked straight past it and reported zero call sites. A false NEGATIVE here
                   is the dangerous direction, because it would let an ungated read read as gated once the
                   offsets shifted. Scanning every offset is a superset: it can only over-report, and the
                   chance of four arbitrary operand bytes equalling one of exactly two tracked metadata
                   tokens inside the same body is negligible. */
            }

            if (callsRead)
            {
                readerBodies++;
                if (callsGate)
                {
                    gatedBodies++;
                }
            }
        }

        Assert.True(readerBodies > 0,
            $"No method body in the service assembly calls {Read} — either the read moved or the IL walk "
            + "resolved nothing. Either way this test can say nothing about the gate.");
        Assert.True(totalGateCalls > 0,
            $"{Gate} is never called anywhere in the service assembly. The gate exists but nothing consults "
            + "it, so the server-scoped watermark read is unguarded and #2797 is not fixed.");
        Assert.Equal(readerBodies, gatedBodies);
    }
}
