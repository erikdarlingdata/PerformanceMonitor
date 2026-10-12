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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live facts mint their own scratch databases through ScratchPostgres and never touch another
   test's rows, so they are deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The attention table is a PLAIN table on every store (#5640, seat ruling): the 73rd hypertable is a grid decision
/// (#3035, #3044, #3107), and its rows are low volume. Pins the opt-out census (exactly this one table), what the
/// opt-out leaves unchanged (<see cref="TimescaleSupport.HypertableCount"/> stays 72), the retention path that replaces
/// the Timescale policy (the 30-day time-sliced DELETE), the V179 rung's shape on a live store, and the per-database
/// session-name arm the Azure own-session fallback needs.
/// </summary>
public sealed class AttentionPlainTableTests
{
    private const int TestServerId = -717180;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Version == 179);

    [Fact]
    public void TheOptOutCensus_ListsExactlyTheAttentionTable()
    {
        /* Every opt-out is a table that is NOT in the hypertable grid. The list is exact, so the exception cannot spread
           silently: a second entry reddens here, and its author has to argue the grid decision again. */
        var optedOut = CollectorCatalog.All.Where(c => c.StaysPlainTable).Select(c => c.TargetTable).ToArray();
        Assert.Equal(new[] { "attention_events" }, optedOut);

        Assert.DoesNotContain(TimescaleSupport.HypertableTables, t => t.StaysPlainTable);
        Assert.DoesNotContain(TimescaleSupport.HypertableTables, t => t.TargetTable == "attention_events");
        Assert.DoesNotContain("attention_events", TimescaleSupport.CompressionPhaseOrder);
        Assert.Equal(CollectorCatalog.All.Count - optedOut.Length, TimescaleSupport.HypertableTables.Count);

        /* The grid did not move: 71 catalog hypertables plus collection_log. */
        Assert.Equal(71, TimescaleSupport.HypertableTables.Count);
        Assert.Equal(72, TimescaleSupport.HypertableCount);
    }

    [Fact]
    public void TheOptOut_IsDeclaredOnTheAttentionCollectorOnly_AndNamesTheGridDecision()
    {
        Assert.True(AttentionCollector.Instance.StaysPlainTable);
        Assert.True(((ICollectorSchemaInfo)AttentionCollector.Instance).StaysPlainTable);

        var source = RepoFile.ReadRepoFileLf("PerformanceMonitor.Collectors", "AttentionCollector.cs");
        foreach (var phrase in new[] { "#3035", "#3044", "#3107", "about 1,800 events per read", "AttentionPlainTableTests" })
        {
            Assert.Contains(phrase, source, StringComparison.Ordinal);
        }

        var overrides = RepoFile.ReadRepoFileLf("PerformanceMonitor.Collectors", "CollectorDefinitionBase.cs");
        Assert.Contains("public virtual bool StaysPlainTable => false;", overrides, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRetention_IsThirtyDays_AndThePlainTableTakesTheTimeSlicedDelete()
    {
        Assert.Equal(30, CollectorScheduleDefaults.All["attention"].RetentionDays);

        var delete = DarlingRetention.DeleteSqlFor(AttentionCollector.Instance);
        Assert.Contains("attention_events", delete, StringComparison.Ordinal);
        Assert.Contains("collection_time", delete, StringComparison.Ordinal);

        /* The sweep skips drop_chunks for a table that stays plain, so a TimescaleDB store neither calls it nor warns about it. */
        var retention = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingRetention.cs");
        Assert.Contains("if (timescaleAvailable && !definition.StaysPlainTable)", retention, StringComparison.Ordinal);
    }

    [Fact]
    public void TheV179Index_IsServerIdThenCollectionTime_AsTheGeneratorWouldEmitIt()
    {
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_attention_events_time ON collect.attention_events(server_id, collection_time);", Rung.Sql, StringComparison.Ordinal);
        Assert.Equal(
            "CREATE INDEX IF NOT EXISTS idx_attention_events_time ON attention_events(server_id, collection_time);",
            PgSchemaGenerator.CreateIndex(AttentionCollector.Instance));
    }

    [Fact]
    public void TheAzureOwnSessionFallback_ReadsThePerDatabaseAttentionSessionName()
    {
        var runner = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs");
        Assert.Contains(
            "AttentionCollector => AlwaysOnReadSessionName(server, databaseName, AlwaysOnXeSessionKind.Attention),",
            runner, StringComparison.Ordinal);

        /* All three arms sit in the one switch, so a fourth session kind cannot be added to one half only. */
        var start = runner.IndexOf("context.AlwaysOnSessionName = definition switch", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = runner.IndexOf("_ => null,", start, StringComparison.Ordinal);
        var arms = runner[start..end];
        Assert.Contains("DeadlocksCollector =>", arms, StringComparison.Ordinal);
        Assert.Contains("BlockedProcessReportCollector =>", arms, StringComparison.Ordinal);
        Assert.Contains("AttentionCollector =>", arms, StringComparison.Ordinal);
    }

    [Fact]
    public async Task V179_OnALiveStore_IsIdempotent_HasTheDefaults_AndTheMuteRuleColumn()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the V179 rung's live pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            const string ColumnSql =
                "SELECT data_type || ':' || is_nullable || ':' || coalesce(column_default, '') FROM information_schema.columns "
                + "WHERE table_schema = 'config' AND table_name = '{0}' AND column_name = '{1}'";

            /* The alert settings: off, 10 and 60 per minute. */
            Assert.Equal("boolean:NO:false", await ScalarAsync(connection, string.Format(ColumnSql, "config_alert_settings", "client_timeouts_enabled"), ct));
            Assert.Equal("integer:NO:10", await ScalarAsync(connection, string.Format(ColumnSql, "config_alert_settings", "client_timeouts_warning_per_minute"), ct));
            Assert.Equal("integer:NO:60", await ScalarAsync(connection, string.Format(ColumnSql, "config_alert_settings", "client_timeouts_critical_per_minute"), ct));

            /* The mute-rule match: nullable text, no default. */
            Assert.Equal("text:YES:", await ScalarAsync(connection, string.Format(ColumnSql, "config_mute_rules", "client_app_pattern"), ct));


            /* The table: a plain table, indexed (server_id, collection_time). */
            Assert.Equal(1L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_indexes WHERE schemaname = 'collect' AND tablename = 'attention_events' AND indexname = 'idx_attention_events_time' "
                + "AND indexdef LIKE '%(server_id, collection_time)'", ct));

            /* Run the rung's text twice more: nothing moves, nothing throws. */
            await ExecAsync(connection, Rung.Sql, ct);
            await ExecAsync(connection, Rung.Sql, ct);
            Assert.Equal("text:YES:", await ScalarAsync(connection, string.Format(ColumnSql, "config_mute_rules", "client_app_pattern"), ct));
            Assert.Equal("boolean:NO:false", await ScalarAsync(connection, string.Format(ColumnSql, "config_alert_settings", "client_timeouts_enabled"), ct));

            /* An upgrade: the rung's columns and table taken away (a store one rung behind), then the rung's text brings them back. */
            await ExecAsync(connection, "DROP TABLE collect.attention_events", ct);
            await ExecAsync(connection, "ALTER TABLE config.config_mute_rules DROP COLUMN client_app_pattern", ct);
            await ExecAsync(connection,
                "ALTER TABLE config.config_alert_settings DROP COLUMN client_timeouts_enabled, DROP COLUMN client_timeouts_warning_per_minute, DROP COLUMN client_timeouts_critical_per_minute", ct);

            /* The viewer's mute-rule list on the store below V179: the gated statement answers, the current one fails with 42703. */
            await ExecAsync(connection, "SET search_path = collect, config, public", ct);
            await ExecAsync(connection, "INSERT INTO config.config_mute_rules (id, enabled, created_at_utc) VALUES ('old-rule', TRUE, now() AT TIME ZONE 'UTC')", ct);
            await using (var gated = new NpgsqlCommand(PerformanceMonitor.Darling.Viewer.ViewerDataService.MuteRulesSelectBeforeClientAppSql, connection))
            await using (var reader = await gated.ExecuteReaderAsync(ct))
            {
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(13, reader.FieldCount);
                Assert.True(reader.IsDBNull(12));
            }

            var skew = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var current = new NpgsqlCommand(PerformanceMonitor.Darling.Viewer.ViewerDataService.MuteRulesSelectSql, connection);
                await using var rejected = await current.ExecuteReaderAsync(ct);
            });
            Assert.Equal("42703", skew.SqlState);

            await ExecAsync(connection, Rung.Sql, ct);
            Assert.Equal("text:YES:", await ScalarAsync(connection, string.Format(ColumnSql, "config_mute_rules", "client_app_pattern"), ct));
            Assert.Equal("integer:NO:60", await ScalarAsync(connection, string.Format(ColumnSql, "config_alert_settings", "client_timeouts_critical_per_minute"), ct));
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT count(*) FROM pg_indexes WHERE schemaname = 'collect' AND tablename = 'attention_events'", ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task OnATimescaleStore_TheStartupWalkLeavesItPlain_AndTheSweepPurgesItAtThirtyDays()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the attention plain-table live pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescale = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        if (timescale)
        {
            /* No background policy job reshapes the tables under the assertions. */
            await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        }

        var bodySucceeded = false;
        try
        {
            if (timescale)
            {
                /* The start-up walk: conversion and compression over the grid. Both return the grid's count (71), not the catalog's (72). */
                Assert.Equal(TimescaleSupport.HypertableTables.Count, await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct));
                Assert.Equal(TimescaleSupport.HypertableTables.Count, await TimescaleSupport.ApplyCompressionPolicyAsync(connection, null, ct));

                Assert.Equal(0L, await ScalarAsync(connection,
                    "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_schema = 'collect' AND hypertable_name = 'attention_events'", ct));
                Assert.Equal(0L, await ScalarAsync(connection,
                    "SELECT count(*) FROM timescaledb_information.jobs WHERE hypertable_schema = 'collect' AND hypertable_name = 'attention_events'", ct));
                /* The index V179 made is still there (a hypertable conversion would have re-created it per chunk). */
                Assert.Equal(1L, await ScalarAsync(connection,
                    "SELECT count(*) FROM pg_indexes WHERE schemaname = 'collect' AND tablename = 'attention_events' AND indexname = 'idx_attention_events_time'", ct));
            }

            /* Rows: 45 and 31 days old (past the horizon), 29 days, 1 day and now (inside it). */
            await ExecAsync(connection, "DELETE FROM collect.attention_events WHERE server_id = " + TestServerId, ct);
            await ExecAsync(connection,
                "INSERT INTO collect.attention_events (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count) "
                + "SELECT 700000 + d, (now() AT TIME ZONE 'UTC') - make_interval(days => d), " + TestServerId + ", 'plain-table-test', 'attention', 1 "
                + "FROM unnest(ARRAY[45, 31, 29, 1, 0]) AS d", ct);

            var log = new CapturingTestLogger();
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: timescale, log, ct);

            Assert.Equal("0,1,29", await ScalarAsync(connection,
                "SELECT string_agg(d::text, ',' ORDER BY d) FROM (SELECT ((now() AT TIME ZONE 'UTC')::date - collection_time::date) AS d "
                + "FROM collect.attention_events WHERE server_id = " + TestServerId + ") AS x", ct));

            /* No drop_chunks attempt on the plain table, so no "failed" warning for it. */
            Assert.DoesNotContain("attention_events", string.Join("\n", log.Joined.Split('\n').Where(l => l.Contains("Warning", StringComparison.Ordinal))), StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return await command.ExecuteScalarAsync(ct);
    }
}
