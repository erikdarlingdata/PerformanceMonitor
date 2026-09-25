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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4226 lane V1b job 3: against a seeded, rollup-usable store, the new shared
/// <see cref="ViewerDataService.FleetCollectionHealthByServerSql"/> (rollup-composed) must produce, per
/// (server, collector), the SAME numbers the OLD reads it replaced produced —
/// <see cref="ViewerDataService.CollectionHealthSql"/> (the Overview card's old per-server raw scan) and
/// <see cref="ViewerDataService.PermissionDeniedCollectorCountSql"/> (the badge's old raw scan) — over a window
/// that straddles a partial leading hour AND holds rows in the still-open current hour, so both the raw head
/// slice and the composed union get exercised, not just the fully-materialized middle.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here goes through
   OpenStoreAsync (CollectionHealthAggregateTests), which reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and then works entirely inside it. It never touches the shared
   database's tables, so it cannot race the live collection, and serializing it would be pure slowdown. Leave
   it out; this comment is here so the next sweep does not "fix" it. */
public class FleetCollectionHealthByServerLiveEqualityTests
{
    private const int ServerCount = 3;
    private const int CollectorsPerServer = 4;

    /// <summary>A registered but DISABLED server (#4226 regression case): <c>is_enabled = FALSE</c> in
    /// <c>config_monitored_servers</c>, same as a server an operator has paused. Its card, badge and
    /// per-server status-bar tab must still read its real 7-day history — only the status bar's
    /// fleet-CUMULATIVE total is entitled to leave it out.</summary>
    private const int DisabledServerId = 4;

    /// <summary>A server with real <c>collection_log</c> history and NO row at all in
    /// <c>config_monitored_servers</c> (#4226 regression case): the bootstrap window before the config store
    /// is seeded (<c>IsConfigSeededAsync</c> false), where the fleet list comes from the OBSERVED registry
    /// instead. Its rows must still come back — a statement scoped to config-registered servers would drop
    /// this one entirely, and did, which is what
    /// <c>ServerSummary_ReadsEnrichedThreadsMemoryBlockingCollectors_AgainstDevPostgres</c> caught.</summary>
    private const int UnregisteredServerId = 5;

    /// <summary>Plants collection_log rows for <see cref="ServerCount"/> servers × <see cref="CollectorsPerServer"/>
    /// collectors, one every 6 minutes from 7 days + 37 minutes ago (a deliberately non-hour-aligned start, so
    /// the 7-day window's head is a PARTIAL hour) through right now (so the window's tail sits in the still-open
    /// CURRENT hour, never materialized). Collector 0 on server 1 is PERMISSIONS for the newest 90 minutes, so
    /// the badge has something to count in both the raw head slice and, once refreshed, the composed read. Plus
    /// <see cref="DisabledServerId"/> (registered, disabled) and <see cref="UnregisteredServerId"/> (no
    /// config_monitored_servers row at all), same cadence, one collector each, both SUCCESS throughout.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using (var servers = new NpgsqlCommand(@"
INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled)
SELECT s, 'v1b-eq-srv-' || s, 'v1b-eq-host-' || s, TRUE
FROM generate_series(1, @serverCount) s", connection))
        {
            servers.Parameters.AddWithValue("serverCount", ServerCount);
            await servers.ExecuteNonQueryAsync(ct);
        }

        await using (var disabled = new NpgsqlCommand(@"
INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled)
VALUES (@disabledServerId, 'v1b-eq-srv-disabled', 'v1b-eq-host-disabled', FALSE)", connection))
        {
            disabled.Parameters.AddWithValue("disabledServerId", DisabledServerId);
            await disabled.ExecuteNonQueryAsync(ct);
        }
        /* UnregisteredServerId gets NO config_monitored_servers row — that absence is the case. */

        await using var plant = new NpgsqlCommand(@"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT 9000000 + row_number() OVER (), s, 'v1b-eq-srv-' || s, 'collector_' || c, t, 7,
       CASE WHEN s = 1 AND c = 0 AND t >= (now() AT TIME ZONE 'UTC') - INTERVAL '90 minutes'
            THEN 'PERMISSIONS' ELSE 'SUCCESS' END,
       3
FROM generate_series(1, @serverCount) s
CROSS JOIN generate_series(0, @collectorsPerServer - 1) c
CROSS JOIN generate_series((now() AT TIME ZONE 'UTC') - INTERVAL '7 days 37 minutes', (now() AT TIME ZONE 'UTC'), INTERVAL '6 minutes') t", connection);
        plant.CommandTimeout = 120;
        plant.Parameters.AddWithValue("serverCount", ServerCount);
        plant.Parameters.AddWithValue("collectorsPerServer", CollectorsPerServer);
        await plant.ExecuteNonQueryAsync(ct);

        await using var plantExtra = new NpgsqlCommand(@"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT 9900000 + row_number() OVER (), s, 'v1b-eq-srv-' || s, 'collector_0', t, 7, 'SUCCESS', 3
FROM (VALUES (@disabledServerId), (@unregisteredServerId)) srv(s)
CROSS JOIN generate_series((now() AT TIME ZONE 'UTC') - INTERVAL '7 days 37 minutes', (now() AT TIME ZONE 'UTC'), INTERVAL '6 minutes') t", connection);
        plantExtra.CommandTimeout = 120;
        plantExtra.Parameters.AddWithValue("disabledServerId", DisabledServerId);
        plantExtra.Parameters.AddWithValue("unregisteredServerId", UnregisteredServerId);
        await plantExtra.ExecuteNonQueryAsync(ct);
    }

    private sealed record Snapshot(
        long TotalRuns, long SuccessCount, long ErrorCount, DateTime? LastSuccessTime, long PermissionDeniedCount,
        DateTime? LastRunTime, long AbandonedCount, long ExtensionMissingCount, DateTime? LastNonSkipTime, DateTime? LastProductiveTime);

    private static Snapshot ReadSnapshot(NpgsqlDataReader reader) => new(
        Convert.ToInt64(reader.GetValue(reader.GetOrdinal("total_runs"))),
        Convert.ToInt64(reader.GetValue(reader.GetOrdinal("success_count"))),
        Convert.ToInt64(reader.GetValue(reader.GetOrdinal("error_count"))),
        reader.IsDBNull(reader.GetOrdinal("last_success_time")) ? null : reader.GetDateTime(reader.GetOrdinal("last_success_time")),
        Convert.ToInt64(reader.GetValue(reader.GetOrdinal("permission_denied_count"))),
        reader.IsDBNull(reader.GetOrdinal("last_run_time")) ? null : reader.GetDateTime(reader.GetOrdinal("last_run_time")),
        Convert.ToInt64(reader.GetValue(reader.GetOrdinal("abandoned_count"))),
        Convert.ToInt64(reader.GetValue(reader.GetOrdinal("extension_missing_count"))),
        reader.IsDBNull(reader.GetOrdinal("last_non_skip_time")) ? null : reader.GetDateTime(reader.GetOrdinal("last_non_skip_time")),
        reader.IsDBNull(reader.GetOrdinal("last_productive_time")) ? null : reader.GetDateTime(reader.GetOrdinal("last_productive_time")));

    /// <summary>The composed read's per-server rows equal the old per-server raw scan's rows, collector by
    /// collector, and the badge count taken from the composed read equals the old badge scan's count —
    /// checked for EVERY seeded server, over a window with both a partial leading hour and current-hour rows.</summary>
    [Fact]
    public async Task ComposedFleetByServer_MatchesOldRawReads_PerServerAndBadge_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await CollectionHealthAggregateTests.OpenStoreAsync(ct);
        Assert.SkipWhen(store is null, "Set DARLING_TEST_PG to a Postgres connection string with TimescaleDB to run the live equality test.");
        var (scratch, connection, jobId) = store!.Value;
        await using var _s = scratch;
        await using var _c = connection;

        await SeedAsync(connection, ct);
        await CollectionHealthAggregateTests.RunPolicyAsync(connection, jobId, ct);

        var windowStart = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-7), DateTimeKind.Unspecified);
        var headEnd = CollectionHealthRollupSupport.CeilingHour(windowStart);
        /* Not aligned to an hour boundary: proves the window's head is genuinely partial. */
        Assert.NotEqual(windowStart, headEnd);

        var dataSourceBuilder = new NpgsqlDataSourceBuilder(scratch.ConnectionString);
        await using var dataSource = dataSourceBuilder.Build();
        Assert.True(await CollectionHealthRollupSupport.RollupUsableAsync(dataSource, headEnd, ct),
            "the rollup must be usable for this to be a real composed-vs-raw comparison, not raw-vs-raw");

        var composedSql = CollectionHealthRollupSupport.ComposeFleetSql(ViewerDataService.FleetCollectionHealthByServerSql);
        var composedByServer = new Dictionary<int, Dictionary<string, Snapshot>>();
        await using (var composed = new NpgsqlCommand(composedSql, connection))
        {
            composed.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });
            composed.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = headEnd });
            await using var reader = await composed.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var serverId = reader.GetInt32(reader.GetOrdinal("server_id"));
                var collector = reader.GetString(reader.GetOrdinal("collector_name"));
                if (!composedByServer.TryGetValue(serverId, out var byCollector))
                {
                    byCollector = new Dictionary<string, Snapshot>();
                    composedByServer[serverId] = byCollector;
                }
                byCollector[collector] = ReadSnapshot(reader);
            }
        }

        /* #4226 regression coverage: DisabledServerId and UnregisteredServerId alongside the enabled
           1..ServerCount cohort — the composed read must produce rows for BOTH, the same as the old raw
           per-server scans this replaced never cared whether config_monitored_servers even had a row for
           the server, let alone whether it was enabled. */
        var serverIdsToVerify = new List<int>();
        for (var s = 1; s <= ServerCount; s++)
        {
            serverIdsToVerify.Add(s);
        }
        serverIdsToVerify.Add(DisabledServerId);
        serverIdsToVerify.Add(UnregisteredServerId);

        foreach (var serverId in serverIdsToVerify)
        {
            Assert.True(composedByServer.TryGetValue(serverId, out var composedCollectors), $"composed read produced no rows for server {serverId}");

            var oldByCollector = new Dictionary<string, Snapshot>();
            await using (var old = new NpgsqlCommand(ViewerDataService.CollectionHealthSql, connection))
            {
                old.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
                old.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });
                await using var reader = await old.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    oldByCollector[reader.GetString(reader.GetOrdinal("collector_name"))] = ReadSnapshot(reader);
                }
            }

            Assert.Equal(oldByCollector.Count, composedCollectors!.Count);
            foreach (var (collector, oldSnapshot) in oldByCollector)
            {
                Assert.True(composedCollectors.TryGetValue(collector, out var newSnapshot), $"server {serverId} collector {collector} missing from composed read");
                Assert.Equal(oldSnapshot, newSnapshot);
            }

            var newBadge = 0;
            foreach (var snapshot in composedCollectors.Values)
            {
                if (snapshot.PermissionDeniedCount > 0)
                {
                    newBadge++;
                }
            }

            int oldBadge;
            await using (var badge = new NpgsqlCommand(ViewerDataService.PermissionDeniedCollectorCountSql, connection))
            {
                badge.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
                badge.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = windowStart });
                var scalar = await badge.ExecuteScalarAsync(ct);
                oldBadge = scalar is null or DBNull ? 0 : Convert.ToInt32(scalar);
            }

            Assert.Equal(oldBadge, newBadge);
        }

        /* Server 1 / collector_0 was seeded PERMISSIONS for the newest 90 minutes — well inside the still-open
           current hour, so this also proves the raw head slice (not just the materialized middle) is exercised. */
        Assert.True(composedByServer[1]["collector_0"].PermissionDeniedCount > 0);
    }
}
