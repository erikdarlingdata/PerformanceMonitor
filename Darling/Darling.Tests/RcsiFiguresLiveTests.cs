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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #7448 on a seeded PostgreSQL store: the DB_CONFIG drill-down writes real RCSI figures per database instead of
/// constants, and the shared target gate (<see cref="FactRemediation.CollectRcsiTargets"/>) can now keep a database.
/// Four RCSI-off databases plus tempdb and master: <c>ReadDb</c> (reader-blocked-by-writer waits and deadlocks),
/// <c>WriteDb</c> (writer/writer waits only), <c>QuietDb</c> (no blocking at all) and <c>OutDb</c> (reader waits
/// OUTSIDE the analysis window). Skipped when DARLING_TEST_PG is not set.
/// </summary>
public sealed class RcsiFiguresLiveTests
{
    private const int ServerId = -744_801;
    private const string ServerName = "darling-rcsi-figures";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql, params object?[] values) =>
        DarlingMcpTestData.ExecAsync(connection, ct, sql, values);

    private static async Task SeedConfigAsync(NpgsqlConnection c, CancellationToken ct, DateTime end, string db)
    {
        await ExecAsync(c, ct, @"
INSERT INTO database_config
    (config_id, capture_time, server_id, server_name, database_name,
     state_desc, compatibility_level, collation_name, recovery_model, is_read_only,
     is_auto_close_on, is_auto_shrink_on, is_auto_create_stats_on, is_auto_update_stats_on,
     is_auto_update_stats_async_on, is_read_committed_snapshot_on, snapshot_isolation_state,
     is_parameterization_forced, is_query_store_on, is_encrypted, is_trustworthy_on, is_db_chaining_on,
     is_broker_enabled, is_cdc_enabled, is_mixed_page_allocation_on, log_reuse_wait_desc, page_verify_option,
     target_recovery_time_seconds, delayed_durability, is_accelerated_database_recovery_on,
     is_memory_optimized_enabled, is_optimized_locking_on)
VALUES ($1, $2, $3, $4, $5,
        'ONLINE', 160, 'SQL_Latin1_General_CP1_CI_AS', 'SIMPLE', FALSE,
        FALSE, FALSE, TRUE, TRUE,
        FALSE, FALSE, 'OFF',
        FALSE, TRUE, FALSE, FALSE, FALSE,
        FALSE, FALSE, FALSE, 'NOTHING', 'CHECKSUM',
        60, 'DISABLED', FALSE,
        FALSE, FALSE)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(end.AddHours(-1)), ServerId, ServerName, db);
    }

    /// <summary>One blocked-process report per wait: each gets its own blocked spid, so each is its own wait.</summary>
    private static async Task SeedWaitsAsync(NpgsqlConnection c, CancellationToken ct, DateTime at, string db, string lockMode, int count, int firstSpid)
    {
        for (var i = 0; i < count; i++)
            await ExecAsync(c, ct, @"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid,
     blocking_status, database_name, lock_mode)
VALUES ($1, $2, $3, $4, $2, 5000, 900, $5, 'suspended', $6, $7)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at.AddMinutes(i)), ServerId, ServerName, firstSpid + i, db, lockMode);
    }

    private static Task SeedDeadlockAsync(NpgsqlConnection c, CancellationToken ct, DateTime at, string db) =>
        ExecAsync(c, ct,
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name) VALUES ($1,$2,$3,$4,$2,$5,$6)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, "<deadlock-list><deadlock /></deadlock-list>", db);

    private static async Task<AnalysisFinding> DrillDownAsync(NpgsqlDataSource postgres, DateTime end, CancellationToken ct)
    {
        var context = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = DarlingMcpTestData.Naive(end.AddHours(-4)),
            TimeRangeEnd = DarlingMcpTestData.Naive(end),
            ServerUtcOffset = TimeSpan.Zero,
            CancellationToken = ct,
        };
        var finding = new AnalysisFinding
        {
            ServerId = ServerId, RootFactKey = "DB_CONFIG", StoryPath = "DB_CONFIG", PathKeys = ["DB_CONFIG"], Severity = 0.3,
        };
        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);
        return finding;
    }

    private static JsonElement Row(AnalysisFinding finding, string database)
    {
        var json = JsonSerializer.SerializeToElement(finding.DrillDown!["config_issues"]);
        return json.EnumerateArray().Single(r => r.GetProperty("database").GetString() == database);
    }

    [Fact]
    public async Task RcsiOffDatabases_GetMeasuredFigures_AndOnlyReaderWriterBlockingMakesATarget()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live RCSI figures tests.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            var end = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            foreach (var db in new[] { "ReadDb", "WriteDb", "QuietDb", "OutDb", "SchemaDb", "DeadlockDb", "OneWaitDb", "tempdb", "master" })
                await SeedConfigAsync(connection, ct, end, db);

            /* ReadDb: nine shared-lock waits and three exclusive waits (75 percent readers), and two deadlocks. */
            await SeedWaitsAsync(connection, ct, end.AddHours(-2), "ReadDb", "S", 9, 100);
            await SeedWaitsAsync(connection, ct, end.AddHours(-2).AddMinutes(30), "readdb", "X", 3, 200);
            await SeedDeadlockAsync(connection, ct, end.AddHours(-2), "ReadDb");
            await SeedDeadlockAsync(connection, ct, end.AddHours(-1), "ReadDb");
            /* WriteDb: only writer/writer waits, and a schema lock that RCSI-irrelevance leaves out of the share. */
            await SeedWaitsAsync(connection, ct, end.AddHours(-2), "WriteDb", "X", 10, 300);
            await SeedWaitsAsync(connection, ct, end.AddHours(-2).AddMinutes(30), "WriteDb", "Sch-M", 2, 400);
            /* OutDb: reader waits a day before the window, which the figures must not count. */
            await SeedWaitsAsync(connection, ct, end.AddHours(-30), "OutDb", "S", 4, 500);
            /* SchemaDb: only schema locks, so there are events but no reader or writer wait to take a share of. */
            await SeedWaitsAsync(connection, ct, end.AddHours(-2), "SchemaDb", "Sch-M", 2, 700);
            /* OneWaitDb: a single shared-lock wait, which is too little to judge and must not make a target. */
            await SeedWaitsAsync(connection, ct, end.AddHours(-2), "OneWaitDb", "S", 1, 800);
            /* DeadlockDb: a deadlock and no blocked-process wait at all. */
            await SeedDeadlockAsync(connection, ct, end.AddHours(-2), "DeadlockDb");
            /* tempdb carries reader waits too: it still must not appear. */
            await SeedWaitsAsync(connection, ct, end.AddHours(-2), "tempdb", "S", 3, 600);

            var finding = await DrillDownAsync(postgres, end, ct);

            var names = JsonSerializer.SerializeToElement(finding.DrillDown!["config_issues"]).EnumerateArray()
                .Select(r => r.GetProperty("database").GetString()).Order().ToArray();
            Assert.Equal(["DeadlockDb", "OneWaitDb", "OutDb", "QuietDb", "ReadDb", "SchemaDb", "WriteDb"], names);

            var read = Row(finding, "ReadDb");
            Assert.Equal(12, read.GetProperty("rcsi_blocking_events").GetInt32());
            Assert.Equal(2, read.GetProperty("rcsi_deadlocks").GetInt32());
            Assert.Equal(75, read.GetProperty("rcsi_reader_writer_pct").GetInt32());

            var write = Row(finding, "WriteDb");
            Assert.Equal(12, write.GetProperty("rcsi_blocking_events").GetInt32());
            Assert.Equal(0, write.GetProperty("rcsi_deadlocks").GetInt32());
            Assert.Equal(0, write.GetProperty("rcsi_reader_writer_pct").GetInt32());

            foreach (var quiet in new[] { "QuietDb", "OutDb" })
            {
                var row = Row(finding, quiet);
                Assert.Equal(0, row.GetProperty("rcsi_blocking_events").GetInt32());
                Assert.Equal(0, row.GetProperty("rcsi_deadlocks").GetInt32());
                Assert.Equal(0, row.GetProperty("rcsi_reader_writer_pct").GetInt32());
            }

            /* Events or deadlocks without a single reader or writer wait: the counts stay and the share is unknown (null), not 0. */
            var schema = Row(finding, "SchemaDb");
            Assert.Equal(2, schema.GetProperty("rcsi_blocking_events").GetInt32());
            Assert.Equal(0, schema.GetProperty("rcsi_deadlocks").GetInt32());
            Assert.Equal(JsonValueKind.Null, schema.GetProperty("rcsi_reader_writer_pct").ValueKind);
            var oneWait = Row(finding, "OneWaitDb");
            Assert.Equal(1, oneWait.GetProperty("rcsi_blocking_events").GetInt32());
            Assert.Equal(JsonValueKind.Null, oneWait.GetProperty("rcsi_reader_writer_pct").ValueKind);
            var deadlockOnly = Row(finding, "DeadlockDb");
            Assert.Equal(0, deadlockOnly.GetProperty("rcsi_blocking_events").GetInt32());
            Assert.Equal(1, deadlockOnly.GetProperty("rcsi_deadlocks").GetInt32());
            Assert.Equal(JsonValueKind.Null, deadlockOnly.GetProperty("rcsi_reader_writer_pct").ValueKind);

            /* The consent text built from the drill-down row alone (no carried figures) states no percent for either. */
            foreach (var unknown in new[] { "SchemaDb", "DeadlockDb", "OneWaitDb" })
            {
                var action = new RemediationAction(
                    "RCSI", "set", Array.Empty<ForcePlanTarget>(),
                    new[] { new DbConfigTarget(unknown, DbConfigSetting.ReadCommittedSnapshotOn, "OFF") });
                var disclosure = Assert.IsType<RiskDisclosure>(FactRiskDisclosure.GetForAction(action, finding));
                var text = string.Join(" ", disclosure.RisksOfNotChanging.Select(r => r.Text));
                Assert.DoesNotContain("%", text);
                Assert.DoesNotContain("majority was writer/writer", text);
            }

            var target = Assert.Single(FactRemediation.CollectRcsiTargets(finding));
            Assert.Equal("ReadDb", target.Database);
            Assert.Equal(12, target.Figures.BlockingEvents);
            Assert.Equal(2, target.Figures.Deadlocks);
            Assert.Equal(75, target.Figures.ReaderWriterPct);
            Assert.NotNull(FactRemediation.BuildRcsiAction(finding));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }
}
