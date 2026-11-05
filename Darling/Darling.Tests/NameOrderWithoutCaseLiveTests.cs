/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres, created with the C collation a managed store uses. */

/// <summary>
/// #7550: the store reads behind the Configuration grids, Perfmon Counters and PostgreSQL Settings list names
/// without regard to case. The store compares by byte, so every capitalized name came before every lower-case
/// one and the grid opened in a different order than its own header click. Each read seeds names whose byte order
/// and case-blind order differ and asserts the case-blind one.
/// </summary>

public sealed class NameOrderWithoutCaseLiveTests
{
    private const int ServerId = -975501;
    private const string ServerName = "name-order-case-e2e";

    /* Byte order is ADR Cleaner, Ad Hoc Distributed Queries, access check cache. */
    private static readonly string[] SettingNames = ["ADR Cleaner", "access check cache", "Ad Hoc Distributed Queries"];
    private static readonly string[] SettingNamesInOrder = ["access check cache", "Ad Hoc Distributed Queries", "ADR Cleaner"];
    private static readonly string[] DatabaseNames = ["Zeta", "alpha", "Beta"];
    private static readonly string[] DatabaseNamesInOrder = ["alpha", "Beta", "Zeta"];

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ConfigurationReads_OrderNamesWithoutCase_AgainstLivePostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live name-order test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, cCollation: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        var at = DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-5));
        at = new DateTime(at.Ticks - (at.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);

        foreach (var name in SettingNames)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO server_config (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)", CollectionIdGenerator.Next(), at, ServerId, ServerName, name, 1L, 1L, true, false);
        }

        foreach (var name in DatabaseNames)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name, state_desc, compatibility_level)
VALUES ($1,$2,$3,$4,$5,$6,$7)", CollectionIdGenerator.Next(), at, ServerId, ServerName, name, "ONLINE", 160);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state)
VALUES ($1,$2,$3,$4,$5,$6,$7)", CollectionIdGenerator.Next(), at, ServerId, ServerName, name, "READ_WRITE", "READ_WRITE");
        }

        /* Database scoped configuration: the database first, then the setting, each without case. Two databases that
           share a setting name keep the database order, and one database's settings follow the setting order. */
        foreach (var (db, setting) in new[]
                 {
                     ("Zeta", "MAXDOP"), ("alpha", "Legacy_Cardinality_Estimation"), ("alpha", "ACCELERATED_PLAN_FORCING"),
                     ("alpha", "batch_mode_adaptive_joins"), ("Beta", "MAXDOP"),
                 })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO database_scoped_config (config_id, capture_time, server_id, server_name, database_name, configuration_name, value, value_for_secondary)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)", CollectionIdGenerator.Next(), at, ServerId, ServerName, db, setting, "0", "0");
        }

        Assert.Equal(SettingNamesInOrder, (await DarlingCurrentConfigReader.GetLatestServerConfigAsync(postgres, ServerId, ct)).Rows.Select(r => r.ConfigurationName));
        Assert.Equal(DatabaseNamesInOrder, (await DarlingCurrentConfigReader.GetLatestDatabaseConfigAsync(postgres, ServerId, ct)).Rows.Select(r => r.DatabaseName));
        Assert.Equal(DatabaseNamesInOrder, (await DarlingConfigHistoryReader.GetLatestQueryStoreHealthAsync(postgres, ServerId, ct)).Rows.Select(r => r.DatabaseName));

        var scoped = (await DarlingConfigHistoryReader.GetLatestDatabaseScopedConfigAsync(postgres, ServerId, ct)).Rows
            .Select(r => r.DatabaseName + "/" + r.ConfigurationName).ToArray();
        string[] scopedInOrder =
        [
            "alpha/ACCELERATED_PLAN_FORCING", "alpha/batch_mode_adaptive_joins", "alpha/Legacy_Cardinality_Estimation",
            "Beta/MAXDOP", "Zeta/MAXDOP",
        ];
        Assert.Equal(scopedInOrder, scoped);

        /* The Viewer's twin statements. */
        Assert.Equal(SettingNamesInOrder, (await viewer.GetLatestServerConfigAsync(ServerId, ct)).Select(r => r.ConfigurationName));
        Assert.Equal(DatabaseNamesInOrder, (await viewer.GetLatestDatabaseConfigAsync(ServerId, null, ct)).Select(r => r.DatabaseName));
        Assert.Equal(DatabaseNamesInOrder, (await viewer.GetLatestQueryStoreHealthAsync(ServerId, null, ct)).Select(r => r.DatabaseName));
        Assert.Equal(scopedInOrder,
            (await viewer.GetLatestDatabaseScopedConfigAsync(ServerId, null, ct)).Select(r => r.DatabaseName + "/" + r.ConfigurationName));

    }

    /* Byte order is Alpha, Beta, Gamma, delta, epsilon: every capitalized name before every lower-case one. */
    private static readonly string[] MixedCaseNames = ["Gamma", "delta", "Alpha", "epsilon", "Beta"];
    private static readonly string[] MixedCaseNamesInOrder = ["Alpha", "Beta", "delta", "epsilon", "Gamma"];

    /// <summary>
    /// #7689: the Databases picker, the database sizes read, the version store reads and the protection read ended in a
    /// plain <c>ORDER BY database_name</c>, so a server with Alpha, Beta, Gamma, delta and epsilon listed them in byte order.
    /// </summary>
    [Fact]
    public async Task DatabaseNameReads_OrderNamesWithoutCase_AgainstLivePostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live name-order test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, cCollation: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        var at = DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-5));
        at = new DateTime(at.Ticks - (at.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);

        foreach (var name in MixedCaseNames)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name, state_desc, compatibility_level)
VALUES ($1,$2,$3,$4,$5,$6,$7)", CollectionIdGenerator.Next(), at, ServerId, ServerName, name, "ONLINE", 160);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc, file_name, physical_name, total_size_mb)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)", CollectionIdGenerator.Next(), at, ServerId, ServerName, name, 5, 1, "ROWS", name + "_data", "C:\\data\\" + name + ".mdf", 128.00m);
            /* Every size equal, as on a server without accelerated database recovery: the tie break sets the whole order. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO pvs_stats (collection_id, collection_time, server_id, server_name, database_name, is_accelerated_database_recovery_on, persistent_version_store_size_mb, database_data_size_mb)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)", CollectionIdGenerator.Next(), at, ServerId, ServerName, name, false, 0.0, 100.0);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO collect.database_protection (collection_id, collection_time, server_id, server_name, database_name, state_desc, recovery_model_desc)
VALUES ($1,$2,$3,$4,$5,$6,$7)", CollectionIdGenerator.Next(), at, ServerId, ServerName, name, "ONLINE", "FULL");
        }

        /* The picker read, which the Viewer's Excluded Databases picker shares. */
        await using (var picker = postgres.CreateCommand(CollectedDatabases.NamesSql))
        {
            picker.Parameters.AddWithValue(ServerId);
            var names = new System.Collections.Generic.List<string>();
            await using var reader = await picker.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                names.Add(reader.GetString(0));
            }

            Assert.Equal(MixedCaseNamesInOrder, names);
        }

        /* The database sizes read: one file per database, so the database order is the row order. */
        await using (var sizes = postgres.CreateCommand(DarlingObjectStatsReader.DatabaseSizeLatestSql))
        {
            sizes.Parameters.AddWithValue(ServerId);
            sizes.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, at);
            sizes.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text, Value = DBNull.Value });
            var names = new System.Collections.Generic.List<string>();
            await using var reader = await sizes.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                names.Add(reader.GetString(1));
            }

            Assert.Equal(MixedCaseNamesInOrder, names);
        }

        /* The version store reads, both tools. */
        Assert.Equal(MixedCaseNamesInOrder, (await DarlingPvsReader.GetPvsStatsLatestAsync(postgres, ServerId, ct)).Select(r => r.DatabaseName));
        Assert.Equal(MixedCaseNamesInOrder, (await viewer.GetPvsStatsLatestAsync(ServerId, null, ct)).Select(r => r.DatabaseName));

        /* The protection read behind get_database_protection. */
        var protection = await DarlingDatabaseProtectionReader.ReadAsync(
            postgres, ServerId, DateTime.UtcNow.AddHours(-1), DateTime.UtcNow, cancellationToken: ct);
        Assert.Equal(MixedCaseNamesInOrder, protection.Databases.Select(d => d.DatabaseName));
    }

    /// <summary>
    /// #7689: the plan correction enablement reads (Service and Viewer) and the Viewer's object locking database list are
    /// <c>SELECT DISTINCT</c> reads. PostgreSQL refuses <c>ORDER BY lower(x)</c> on a DISTINCT whose select list lacks it, so each
    /// is wrapped; running them is the proof the wrap is valid and the order is case-blind.
    /// </summary>
    [Fact]
    public async Task DistinctDatabaseReads_OrderNamesWithoutCase_AgainstLivePostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live name-order test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, cCollation: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        var at = DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-5));
        at = new DateTime(at.Ticks - (at.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);

        foreach (var name in MixedCaseNames)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO plan_correction (collection_id, collection_time, server_id, server_name, database_name, force_last_good_plan_desired_state, force_last_good_plan_actual_state, recommendation_name, recommendation_state, query_id, query_text)
VALUES ($1,$2,$3,$4,$5,'Enabled','Enabled',$6,'Active',1000,'SELECT 1')", CollectionIdGenerator.Next(), at, ServerId, ServerName, name, "Rec_" + name);

            /* Two lock rows per database: the DISTINCT must still return each name once. */
            foreach (var table in new[] { "t1", "t2" })
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, object_id, schema_name, table_name, index_id, index_name, reserved_mb, row_lock_wait_in_ms, index_lock_promotion_count) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
                    CollectionIdGenerator.Next(), at, ServerId, ServerName, name, 1, "dbo", table, 1, "PK_" + table, 10m, 100L, 0L);
            }
        }

        Assert.Equal(MixedCaseNamesInOrder,
            (await DarlingPlanCorrectionReader.GetLatestAutomaticTuningAsync(postgres, ServerId, cancellationToken: ct)).Select(r => r.DatabaseName));
        Assert.Equal(MixedCaseNamesInOrder,
            (await viewer.GetLatestAutomaticTuningAsync(ServerId, null, ct)).Select(r => r.DatabaseName));
        Assert.Equal(MixedCaseNamesInOrder, await viewer.GetIndexLockingDatabasesAsync(ServerId, null, ct));
    }

    [Fact]
    public async Task PerfmonCounterPicker_ListsNamesWithoutCase_AgainstLivePostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live name-order test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, cCollation: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        var at = DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-5));
        at = new DateTime(at.Ticks - (at.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);

        /* Byte order would put Alpha, Beta, Delta, alpha, beta, gamma. */
        foreach (var counter in new[] { "gamma", "Delta", "beta", "alpha", "Alpha", "Beta" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)", CollectionIdGenerator.Next(), at, ServerId, ServerName, "SQLServer:Workload Group Stats", counter, "default", 1L, 0L, 60, 65792);
        }

        var now = DateTime.UtcNow;
        Assert.Equal(
            ["Alpha", "alpha", "Beta", "beta", "Delta", "gamma"],
            await viewer.GetDistinctPerfmonCountersAsync(ServerId, now.AddHours(-1), now, cancellationToken: ct));
        Assert.Equal(
            ["Alpha", "alpha", "Beta", "beta", "Delta", "gamma"],
            await DarlingTrendReader.GetDistinctPerfmonCountersAsync(postgres, ServerId, now.AddHours(-1), now, ct));
    }

    [Fact]
    public async Task PerfmonCounters_OrderNamesWithoutCase_ThenByInstance_AgainstLivePostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live name-order test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, cCollation: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var at = DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-5));
        at = new DateTime(at.Ticks - (at.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);

        /* "Active requests" is stored internal first: the instance is the second key, so default still leads. */
        foreach (var (counter, instance) in new[]
                 {
                     ("Active parallel threads", "default"), ("Active requests", "internal"), ("Active Temp Tables", "default"),
                     ("Active requests", "default"),
                 })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds, cntr_type)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)", CollectionIdGenerator.Next(), at, ServerId, ServerName, "SQLServer:Workload Group Stats", counter, instance, 1L, 0L, 60, 65792);
        }

        var rows = (await DarlingDataReader.GetLatestPerfmonStatsAsync(postgres, ServerId, ct)).Rows;
        Assert.Equal(
            ["Active parallel threads/default", "Active requests/default", "Active requests/internal", "Active Temp Tables/default"],
            rows.Select(r => r.CounterName + "/" + r.InstanceName));

    }

    [Fact]
    public async Task PostgresSettings_OrderNamesWithoutCase_WithinEachGroup_AgainstLivePostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live name-order test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, cCollation: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var at = DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-5));
        at = new DateTime(at.Ticks - (at.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);

        /* First group: set in a file to a value that differs from the boot value. Second group: the rest, at their defaults. */
        foreach (var (name, source, setting, boot) in new[]
                 {
                     ("TimeZone", "configuration file", "UTC", "GMT"), ("auto_explain.log_format", "configuration file", "json", "text"),
                     ("DateStyle", "default", "ISO, MDY", "ISO, MDY"), ("application_name", "default", "", ""),
                 })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO pg_server_config (collection_id, collection_time, server_id, server_name, name, setting, unit, category, context, vartype,
                                                source, boot_val, reset_val, sourcefile, sourceline, pending_restart, short_desc)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, name, setting, null, "Reporting and Logging / What to Log", "user", "string",
                source, boot, setting, null, 0, false, null);
        }

        var page = await DarlingPgServerConfigReader.GetCurrentConfigPageAsync(postgres, ServerId, 100, includeDefaults: true, ct);
        Assert.Equal(["auto_explain.log_format", "TimeZone", "application_name", "DateStyle"], page.Rows.Select(r => r.Name));

    }

    [Fact]
    public async Task IdentityScannedDatabases_OrderNamesWithoutCase_AgainstLivePostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live name-order test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, cCollation: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var now = DateTime.UtcNow;
        foreach (var database in new[] { "beta", "Alpha", "gamma", "Delta" })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO collect.identity_capacity (collection_id, collection_time, server_id, server_name, database_name) VALUES ($1,$2,$3,$4,$5)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-5)), ServerId, ServerName, database);
        }

        var read = await DarlingIdentityCapacityReader.ReadAsync(postgres, ServerId, DatabaseFilter.All, now.AddHours(-168), now, 30, cancellationToken: ct);
        Assert.Equal(["Alpha", "beta", "Delta", "gamma"], read.Databases.Select(d => d.DatabaseName));
    }

}
