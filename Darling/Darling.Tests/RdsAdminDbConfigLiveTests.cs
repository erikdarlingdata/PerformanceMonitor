/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #9587 on a seeded PostgreSQL store: <c>rdsadmin</c> is the platform's own database on Amazon RDS for SQL Server, which
/// the customer cannot change. The <c>DB_CONFIG</c> fact and its drill-down leave it out beside master, msdb, model and
/// tempdb, so an RDS target whose user databases are all fine raises no <c>DB_CONFIG</c> finding and no RCSI remediation
/// names it. The twin of Lite's <c>RdsAdminDbConfigTests</c>. Skipped when DARLING_TEST_PG is not set.
/// </summary>
public sealed class RdsAdminDbConfigLiveTests
{
    private const int ServerId = -958_701;
    private const string ServerName = "darling-rdsadmin-config";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task SeedConfigAsync(NpgsqlConnection c, CancellationToken ct, DateTime end, string db, bool rcsiOn)
    {
        await DarlingMcpTestData.ExecAsync(c, ct, @"
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
        FALSE, $6, 'OFF',
        FALSE, TRUE, FALSE, FALSE, FALSE,
        FALSE, FALSE, FALSE, 'NOTHING', 'CHECKSUM',
        60, 'DISABLED', FALSE,
        FALSE, FALSE)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(end.AddHours(-1)), ServerId, ServerName, db, rcsiOn);
    }

    private static AnalysisContext ContextAt(DateTime end, CancellationToken ct) => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        TimeRangeStart = DarlingMcpTestData.Naive(end.AddHours(-4)),
        TimeRangeEnd = DarlingMcpTestData.Naive(end),
        ServerUtcOffset = TimeSpan.Zero,
        CancellationToken = ct,
    };

    private static async Task<Fact?> DbConfigFactAsync(NpgsqlDataSource postgres, DateTime end, CancellationToken ct) =>
        (await new PgFactCollector(postgres).CollectFactsAsync(ContextAt(end, ct))).SingleOrDefault(f => f.Key == "DB_CONFIG");

    private static async Task<AnalysisFinding> DrillDownAsync(NpgsqlDataSource postgres, DateTime end, CancellationToken ct)
    {
        var finding = new AnalysisFinding
        {
            ServerId = ServerId, RootFactKey = "DB_CONFIG", StoryPath = "DB_CONFIG", PathKeys = ["DB_CONFIG"], Severity = 0.3,
        };
        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], ContextAt(end, ct));
        return finding;
    }

    /// <summary>Seeds one capture per scenario into its own scratch store and hands the body the store and the window end.</summary>
    private static async Task RunAsync(Func<NpgsqlConnection, NpgsqlDataSource, DateTime, CancellationToken, Task> seedAndAssert)
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live rdsadmin DB_CONFIG tests.");

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

            await seedAndAssert(connection, postgres, DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow), ct);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public Task OnlyRdsadminWithRcsiOff_CountsNoRcsiOffDatabase_AndTheFactScoresNothing() =>
        RunAsync(async (c, postgres, end, ct) =>
        {
            await SeedConfigAsync(c, ct, end, "Sales", rcsiOn: true);
            await SeedConfigAsync(c, ct, end, "rdsadmin", rcsiOn: false);

            var fact = Assert.IsType<Fact>(await DbConfigFactAsync(postgres, end, ct));
            Assert.Equal(0, fact.Metadata["rcsi_off_count"]);
            Assert.Equal(1, fact.Metadata["database_count"]);

            new FactScorer().ScoreAll([fact]);
            Assert.Equal(0.0, fact.BaseSeverity);
        });

    [Fact]
    public Task RdsadminAlone_RaisesNoDbConfigFact() =>
        RunAsync(async (c, postgres, end, ct) =>
        {
            await SeedConfigAsync(c, ct, end, "rdsadmin", rcsiOn: false);

            Assert.Null(await DbConfigFactAsync(postgres, end, ct));
        });

    [Fact]
    public Task AUserDatabaseWithRcsiOff_StillCounts_BesideRdsadmin() =>
        RunAsync(async (c, postgres, end, ct) =>
        {
            await SeedConfigAsync(c, ct, end, "Sales", rcsiOn: false);
            await SeedConfigAsync(c, ct, end, "Orders", rcsiOn: true);
            await SeedConfigAsync(c, ct, end, "rdsadmin", rcsiOn: false);

            var fact = Assert.IsType<Fact>(await DbConfigFactAsync(postgres, end, ct));
            Assert.Equal(1, fact.Metadata["rcsi_off_count"]);
            Assert.Equal(2, fact.Metadata["database_count"]);

            new FactScorer().ScoreAll([fact]);
            Assert.True(fact.BaseSeverity > 0, "a user database with RCSI off must still score");
        });

    [Fact]
    public Task TheDrillDown_ListsNoRdsadminRow_AndNoRemediationTargetsIt() =>
        RunAsync(async (c, postgres, end, ct) =>
        {
            await SeedConfigAsync(c, ct, end, "Sales", rcsiOn: false);
            await SeedConfigAsync(c, ct, end, "Orders", rcsiOn: true);
            await SeedConfigAsync(c, ct, end, "rdsadmin", rcsiOn: false);

            var finding = await DrillDownAsync(postgres, end, ct);

            var names = JsonSerializer.SerializeToElement(finding.DrillDown!["config_issues"]).EnumerateArray()
                .Select(r => r.GetProperty("database").GetString()).ToArray();
            Assert.Equal(["Sales"], names);
            Assert.DoesNotContain(FactRemediation.CollectRcsiTargets(finding), t => t.Database == "rdsadmin");
        });

    [Fact]
    public Task OnlyRdsadminWithRcsiOff_GivesNoDrillDownRows() =>
        RunAsync(async (c, postgres, end, ct) =>
        {
            await SeedConfigAsync(c, ct, end, "Sales", rcsiOn: true);
            await SeedConfigAsync(c, ct, end, "rdsadmin", rcsiOn: false);

            var finding = await DrillDownAsync(postgres, end, ct);

            Assert.True(finding.DrillDown is null || !finding.DrillDown.ContainsKey("config_issues"));
        });
}
