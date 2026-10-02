/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// V151/#4475 live reader pin: two monitored SECONDARIES of one real Availability Group, its primary
/// unmonitored, carrying the SAME <c>group_id</c> and DISJOINT replica-name sets, count as ONE group through
/// the real reads -- the MCP/web AG reader's <c>distinct_ag_count</c> and the Viewer's <see cref="AgTopology"/>
/// count over its own read -- not just through <see cref="AgTopology.CountDistinctGroups"/> called directly.
/// </summary>
public sealed class AgGroupIdLiveReaderTests
{
    private const string ServerNameA = "darling-ag-group-id-a";
    private const string ServerNameB = "darling-ag-group-id-b";
    private const string GroupId = "11111111-2222-3333-4444-555555555555";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task DistinctAgCount_TwoSecondariesSameGroupId_DisjointReplicaSets_IsOneThroughTheRealReads()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live AG group_id reader pin.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            /* If the branch's V151 rung has not landed yet at the time this runs, plant the column ourselves
               so this pin still compiles and runs against a store that predates it -- said explicitly rather
               than silently swallowed. */
            await using (var addColumn = new NpgsqlCommand(
                "ALTER TABLE ag_replica_states ADD COLUMN IF NOT EXISTS group_id text;", connection))
            {
                await addColumn.ExecuteNonQueryAsync(ct);
            }

            var serverIdA = ServerIdHelper.GetDeterministicHashCode(ServerNameA);
            var serverIdB = ServerIdHelper.GetDeterministicHashCode(ServerNameB);
            await DarlingMcpTestData.RegisterServerAsync(connection, serverIdA, ServerNameA, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, serverIdB, ServerNameB, ct);

            var when = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);

            /* Each server's replica-states row reports ONLY ITSELF -- the real shape a monitored SECONDARY
               produces under sys.dm_hadr_availability_replica_states' local-only rule, with the AG's primary
               unmonitored. The replica-name sets are DISJOINT ({AGNODE-A} vs {AGNODE-B}); only the shared
               group_id links them. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, operational_state_desc, connected_state_desc, recovery_health_desc, synchronization_health_desc, availability_mode_desc, failover_mode_desc, endpoint_url, group_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(when), serverIdA, ServerNameA, "AG1", "AGNODE-A", "SECONDARY",
                "ONLINE", "CONNECTED", "ONLINE", "HEALTHY", "SYNCHRONOUS_COMMIT", "AUTOMATIC", "TCP://AGNODE-A:5022", GroupId);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, operational_state_desc, connected_state_desc, recovery_health_desc, synchronization_health_desc, availability_mode_desc, failover_mode_desc, endpoint_url, group_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(when), serverIdB, ServerNameB, "AG1", "AGNODE-B", "SECONDARY",
                "ONLINE", "CONNECTED", "ONLINE", "HEALTHY", "SYNCHRONOUS_COMMIT", "AUTOMATIC", "TCP://AGNODE-B:5022", GroupId);

            /* The MCP/web read (get_ag_health's distinct_ag_count), through DarlingAgReader.GetAgHealthAsync --
               the same code path /api/ag and get_ag_health call, not AgTopology.CountDistinctGroups called
               directly. */
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var mcpResult = await DarlingAgReader.GetAgHealthAsync(postgres, null, DateTime.UtcNow, cancellationToken: ct);
            Assert.Equal(2, mcpResult.AvailabilityGroupCount);
            Assert.Equal(1, mcpResult.DistinctAgCount);

            /* The Viewer's own read and count, through ViewerDataService.GetAvailabilityGroupsAsync and
               AgTopology.Counts over its cards -- the same call path the WPF AG tab uses, not a manual
               reconstruction of its cards. */
            var viewerService = new ViewerDataService(scratch.ConnectionString);
            try
            {
                var cards = await viewerService.GetAvailabilityGroupsAsync(ct);
                var (groups, reportingServers, views) = AgTopology.Counts(cards);

                Assert.Equal(2, views);
                Assert.Equal(2, reportingServers);
                Assert.Equal(1, groups);
            }
            finally
            {
                await viewerService.DisposeAsync();
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }
}
