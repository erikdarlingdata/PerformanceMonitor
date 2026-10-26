/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6554: a Query Store read on a server that is a readable secondary, with <c>query_store_replica</c> off, answers
/// <c>not_collected</c> and says why. Before, get_forced_plans said <c>empty</c> and get_query_store_top said
/// "Query Store is READ_ONLY", which sent the reader to look for a fault in a secondary's normal state.
/// </summary>
[Collection("live-postgres")]
public sealed class QueryStoreReadableSecondaryNotCollectedLiveTests
{
    private const string ServerName = "darling-qs-secondary";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<T> WithStoreAsync<T>(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task<T>> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store secondary tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        return await body(connection, postgres, ct);
    }

    private static Task SeedRoleAsync(NpgsqlConnection c, CancellationToken ct, string role) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition, ag_replica_role) " +
            "VALUES ($1, $2, $3, $4, 'Enterprise', '16.0.1000.6', 'RTM', 3, $5)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(DateTime.UtcNow), ServerId, ServerName, role);

    private static Task SeedHealthAsync(NpgsqlConnection c, CancellationToken ct, string db, string state, int readonlyReason, DateTime? at = null) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO collect.query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, interval_length_minutes) " +
            "VALUES ($1, $2, $3, $4, $5, $6, 'READ_WRITE', $7, 60)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at ?? DateTime.UtcNow.AddMinutes(-5)), ServerId, ServerName, db, state, readonlyReason);

    private static Task SeedReplicaSettingAsync(NpgsqlConnection c, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO config.config_monitored_servers (server_id, name, host, query_store_replica) VALUES ($1, $2, 'primary-host', 'replica-host')",
            ServerId, ServerName);

    private static string Message(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("message").GetString()!;
    }

    [Fact]
    public async Task SecondaryRole_ForcedPlansAndQueryStoreTop_AnswerNotCollected_NamingTheSecondary()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            await SeedRoleAsync(c, ct, "Secondary");

            var forced = await DarlingMcpForcedPlanTools.GetForcedPlans(postgres, ServerName, 24, null, DatabaseFilter.All, ct);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(forced));
            Assert.Contains("readable secondary", Message(forced));
            Assert.Contains("query_store_replica", Message(forced));

            var top = await DarlingMcpDataTools.GetQueryStoreTop(postgres, ServerName, 24, 20, null, null, null, null, false, null, ct);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(top));
            Assert.DoesNotContain("READ_ONLY and recording", Message(top));
            return 0;
        });
    }

    [Fact]
    public async Task EveryDatabaseCarriesTheSecondaryBit_QueryStoreTopAnswersNotCollected_NotReadOnly()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            /* An RDS read replica reads 'Standalone' in the role; the health rows from before #6252 carry the engine's bit 8. */
            await SeedRoleAsync(c, ct, "Standalone");
            await SeedHealthAsync(c, ct, "Sales", "READ_ONLY", 8);
            await SeedHealthAsync(c, ct, "Hr", "READ_ONLY", 8);

            var top = await DarlingMcpDataTools.GetQueryStoreTop(postgres, ServerName, 24, 20, null, null, null, null, false, null, ct);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(top));
            Assert.Contains("readable secondary", Message(top));
            return 0;
        });
    }

    [Fact]
    public async Task StorageCapReadOnly_IsStillAPrecondition_NotASecondary()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            await SeedRoleAsync(c, ct, "Primary");
            await SeedHealthAsync(c, ct, "Sales", "READ_ONLY", 65536);

            var top = await DarlingMcpDataTools.GetQueryStoreTop(postgres, ServerName, 24, 20, null, null, null, null, false, null, ct);
            Assert.Equal("precondition", DarlingMcpTestData.StatusOf(top));
            return 0;
        });
    }

    [Fact]
    public async Task SecondaryWithAReplicaSetting_DoesNotClaimNotCollected()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            await SeedRoleAsync(c, ct, "Secondary");
            await SeedReplicaSettingAsync(c, ct);

            var forced = await DarlingMcpForcedPlanTools.GetForcedPlans(postgres, ServerName, 24, null, DatabaseFilter.All, ct);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(forced));
            return 0;
        });
    }

    /// <summary>A Secondary role with a health row for a database that is not flagged secondary (a database outside any availability
    /// group, Query Store READ_WRITE): Query Store is still collected there, so no read says it is a readable secondary.</summary>
    [Fact]
    public async Task SecondaryRole_WithAHealthRowForANonSecondaryDatabase_DoesNotClaimNotCollected()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            await SeedRoleAsync(c, ct, "Secondary");
            await SeedHealthAsync(c, ct, "Standalone", "READ_WRITE", 0);

            var forced = await DarlingMcpForcedPlanTools.GetForcedPlans(postgres, ServerName, 24, null, DatabaseFilter.All, ct);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(forced));

            var top = await DarlingMcpDataTools.GetQueryStoreTop(postgres, ServerName, 24, 20, null, null, null, null, false, null, ct);
            Assert.NotEqual("not_collected", DarlingMcpTestData.StatusOf(top));
            return 0;
        });
    }

    /// <summary>The on-prem capture always holds tempdb with Query Store OFF: on a Secondary role that is still a readable secondary, and a READ_WRITE database beside it still vetoes that.</summary>
    [Fact]
    public async Task SecondaryRole_WithTempdbOffInTheCapture_AnswersNotCollected_UnlessAReadWriteDatabaseIsThere()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            var capture = DateTime.UtcNow.AddMinutes(-5);
            await SeedRoleAsync(c, ct, "Secondary");
            await SeedHealthAsync(c, ct, "tempdb", "OFF", 0, capture);

            var top = await DarlingMcpDataTools.GetQueryStoreTop(postgres, ServerName, 24, 20, null, null, null, null, false, null, ct);
            Assert.Equal("not_collected", DarlingMcpTestData.StatusOf(top));

            await SeedHealthAsync(c, ct, "Standalone", "READ_WRITE", 0, capture);
            top = await DarlingMcpDataTools.GetQueryStoreTop(postgres, ServerName, 24, 20, null, null, null, null, false, null, ct);
            Assert.NotEqual("not_collected", DarlingMcpTestData.StatusOf(top));
            return 0;
        });
    }

    [Fact]
    public async Task NoForcedPlanCapture_WithQueryStoreOffEverywhere_AnswersPrecondition()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            await SeedRoleAsync(c, ct, "Primary");
            await SeedHealthAsync(c, ct, "Sales", "OFF", 0);

            var forced = await DarlingMcpForcedPlanTools.GetForcedPlans(postgres, ServerName, 24, null, DatabaseFilter.All, ct);
            Assert.Equal("precondition", DarlingMcpTestData.StatusOf(forced));
            return 0;
        });
    }

    [Fact]
    public async Task PrimaryWithNothingCollected_KeepsItsEmptyAnswer()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            await SeedRoleAsync(c, ct, "Primary");

            var forced = await DarlingMcpForcedPlanTools.GetForcedPlans(postgres, ServerName, 24, null, DatabaseFilter.All, ct);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(forced));
            return 0;
        });
    }

    private static async Task<string> BriefRulesAsync(NpgsqlConnection c, NpgsqlDataSource postgres, CancellationToken ct, string metric, string trigger)
    {
        var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
        var store = new InvestigationRequestStore(postgres, 30, () => now.AddMinutes(-5));
        var opened = await store.OpenOrJoinAsync(
            new InvestigationAlert(1, ServerId, ServerName, "blocking", metric, now.AddMinutes(-5), new[] { ServerId + ":" + metric }, null,
                20, 100, LeaseMinutes: 15, ExpiryMinutes: 240) { TriggerKind = trigger }, ct);
        var sources = await new PerformanceMonitor.Darling.Service.Investigations.PgInvestigationBriefSourceReader(postgres, utcNow: () => now)
            .ReadAsync(opened.RequestId!.Value, ct);
        var brief = InvestigationBriefComposer.Compose(sources!, PerformanceMonitor.Darling.Service.Investigations.DarlingInvestigationBriefTools.Tools, McpDataDetail.Full, now);
        return JsonDocument.Parse(brief).RootElement.GetProperty("read_rules").ToString();
    }

    /// <summary>#6554: a Change Watch request on a readable secondary (query_store_replica off) says in its brief that the Query Store reads
    /// will be empty and why, in the wording the reads use; an alert request on it, and a Change Watch request on a primary, say nothing.</summary>
    [Fact]
    public async Task ChangeWatchBrief_OnASecondary_SaysTheQueryStoreReadsWillBeEmpty_AndWhy()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            await SeedRoleAsync(c, ct, "Secondary");
            var rules = await BriefRulesAsync(c, postgres, ct, InvestigationTriggerKinds.ChangeWatchMetric, InvestigationTriggerKinds.ChangeWatch);
            Assert.Contains("Query Store reads for this server will be empty", rules);
            Assert.Contains("readable secondary", rules);
            Assert.Contains("query_store_replica", rules);
            return 0;
        });
    }

    [Fact]
    public async Task ChangeWatchBrief_OnAPrimary_AndAlertBriefOnASecondary_SayNothingAboutQueryStore()
    {
        await WithStoreAsync(async (c, postgres, ct) =>
        {
            await SeedRoleAsync(c, ct, "Primary");
            Assert.DoesNotContain("Query Store", await BriefRulesAsync(c, postgres, ct, InvestigationTriggerKinds.ChangeWatchMetric, InvestigationTriggerKinds.ChangeWatch));
            return 0;
        });

        await WithStoreAsync(async (c, postgres, ct) =>
        {
            await SeedRoleAsync(c, ct, "Secondary");
            Assert.DoesNotContain("Query Store", await BriefRulesAsync(c, postgres, ct, "CPU", InvestigationTriggerKinds.Alert));
            return 0;
        });
    }
}
