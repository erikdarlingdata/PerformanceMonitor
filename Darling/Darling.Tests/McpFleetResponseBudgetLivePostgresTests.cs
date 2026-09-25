/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text;
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
/// M2b (#4198, #4199), against a real store: the two rulings the shape/unit tests cannot see through —
/// <c>get_collection_log</c>'s fleet form actually merges and ranks rows ACROSS servers rather than one
/// server repeated, and <c>get_fleet_overview</c>'s <c>detail</c>/<c>worst_only</c>/<c>band</c> parameters
/// actually change what SQL runs and what the JSON carries. The byte-budget tests measure real serialized
/// payloads against <see cref="McpResponseBudget.DefaultBytes"/> on seeded fleets, standing in for the
/// production numbers #4198 measured by hand.
/// </summary>
[Collection("live-postgres")]
public sealed class McpFleetResponseBudgetLivePostgresTests
{
    /* NEGATIVE, per this family's convention (see FleetCardPostgresCpuLivePostgresTests): teardown deletes
       from `servers` by id and a real store assigns ids from a sequence, so a positive sentinel is one
       collision away from removing an operator's row. -994_1xxx is the small ranking fixture, -994_2xxx the
       byte-budget fleets — disjoint ranges so a failed run's leftovers never collide with either. */
    private const int ServerAId = -994_1001;
    private const int ServerBId = -994_1002;
    private const int ServerCId = -994_1003;
    private const string ServerAName = "aa-m2b-fleet-a";
    private const string ServerBName = "bb-m2b-fleet-b";
    private const string ServerCName = "cc-m2b-fleet-c";
    private static readonly int[] SmallFleetIds = [ServerAId, ServerBId, ServerCId];

    private const int ByteFleetBase = -994_2000;
    private const int ByteFleetServerCount = 45;
    private static readonly int[] ByteFleetIds =
        Enumerable.Range(1, ByteFleetServerCount).Select(i => ByteFleetBase - i).ToArray();

    private const int LogFleetBase = -994_3000;
    private const int LogFleetServerCount = 5;
    private const int LogFleetRowsPerServer = 15;
    private static readonly int[] LogFleetIds =
        Enumerable.Range(1, LogFleetServerCount).Select(i => LogFleetBase - i).ToArray();

    [Fact]
    public async Task GetCollectionLog_FleetForm_MergesAcrossServers_AndMinDurationMsRanksAcrossServers()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live MCP fleet-response-budget tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSmallFleetRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            await InsertServerAsync(connection, ServerAId, ServerAName, ct);
            await InsertServerAsync(connection, ServerBId, ServerBName, ct);
            await InsertServerAsync(connection, ServerCId, ServerCName, ct);

            /* The slowest run fleet-wide sits on server B, inserted SECOND and NOT the newest by time --
               so a read that ranked within one server, picked the newest row, or lost the merge across
               servers would all fail the assertions below for a different reason each. */
            await InsertCollectionLogAsync(connection, ServerAId, ServerAName, now.AddMinutes(-5), 100, ct);
            await InsertCollectionLogAsync(connection, ServerBId, ServerBName, now.AddMinutes(-4), 9000, ct);
            await InsertCollectionLogAsync(connection, ServerCId, ServerCName, now.AddMinutes(-3), 200, ct);

            var newestFirst = await DarlingMcpDataTools.GetCollectionLog(postgres, server_name: null, hours_back: 24);
            using (var doc = JsonDocument.Parse(newestFirst))
            {
                var root = doc.RootElement;
                Assert.Equal("fleet", root.GetProperty("scope").GetString());
                var names = root.GetProperty("runs").EnumerateArray()
                    .Select(r => r.GetProperty("server_name").GetString()).Distinct().ToList();
                Assert.True(names.Count >= 2, $"Expected rows from >= 2 servers, got: {string.Join(", ", names)}");
            }

            var slowestFirst = await DarlingMcpDataTools.GetCollectionLog(
                postgres, server_name: null, hours_back: 24, min_duration_ms: 0);
            using (var doc = JsonDocument.Parse(slowestFirst))
            {
                var runs = doc.RootElement.GetProperty("runs").EnumerateArray().ToList();
                Assert.True(runs.Count >= 3);
                /* #4199's stated pin: the FIRST row under min_duration_ms is the FLEET's slowest run, on
                   server B, not merely the slowest row on whichever server the read happened to touch
                   first. */
                Assert.Equal(ServerBName, runs[0].GetProperty("server_name").GetString());
                Assert.Equal(9000, runs[0].GetProperty("duration_ms").GetDouble());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSmallFleetRowsAsync);
        }
    }

    [Fact]
    public async Task GetFleetOverview_SummaryHasNoCards_CardsHasTheOldShape_AndTheFiltersNarrowCards()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live MCP fleet-response-budget tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSmallFleetRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            await InsertServerAsync(connection, ServerAId, ServerAName, ct);
            await InsertServerAsync(connection, ServerBId, ServerBName, ct);
            await InsertServerAsync(connection, ServerCId, ServerCName, ct);

            /* B and C collected moments ago (Healthy); A has NEVER collected, which is the one card whose
               band this fixture does not have to construct through CPU/memory/blocking data -- Offline is
               exactly "collection is long-dead / never happened" (ServerHealthBands.FleetHealthBand). That
               gives one deterministic Offline card for the band/worst_only filters to isolate. */
            await InsertCollectionLogAsync(connection, ServerBId, ServerBName, now.AddSeconds(-30), 50, ct);
            await InsertCollectionLogAsync(connection, ServerCId, ServerCName, now.AddSeconds(-30), 50, ct);

            var summaryJson = await DarlingMcpFleetTools.GetFleetOverview(postgres, hours_back: 1, detail: "summary");
            using (var doc = JsonDocument.Parse(summaryJson))
            {
                var root = doc.RootElement;
                Assert.False(root.TryGetProperty("cards", out _), "summary must not carry a cards array.");
                Assert.False(root.GetProperty("cards_included").GetBoolean());
                Assert.Equal(3, root.GetProperty("total_servers").GetInt32());
                Assert.True(root.GetProperty("offline_count").GetInt32() >= 1);
                Assert.True(root.GetProperty("worst_servers").GetArrayLength() >= 1);
            }

            var cardsJson = await DarlingMcpFleetTools.GetFleetOverview(postgres, hours_back: 1, detail: "cards");
            using (var doc = JsonDocument.Parse(cardsJson))
            {
                var root = doc.RootElement;
                Assert.True(root.GetProperty("cards_included").GetBoolean());
                Assert.Equal(3, root.GetProperty("cards").GetArrayLength());
            }

            var bandJson = await DarlingMcpFleetTools.GetFleetOverview(
                postgres, hours_back: 1, detail: "cards", band: "offline");
            using (var doc = JsonDocument.Parse(bandJson))
            {
                var cards = doc.RootElement.GetProperty("cards").EnumerateArray().ToList();
                Assert.True(cards.Count >= 1);
                Assert.All(cards, c => Assert.Equal("Offline", c.GetProperty("band").GetString()));
            }

            var worstOnlyJson = await DarlingMcpFleetTools.GetFleetOverview(
                postgres, hours_back: 1, detail: "cards", worst_only: true);
            using (var summaryDoc = JsonDocument.Parse(summaryJson))
            using (var worstDoc = JsonDocument.Parse(worstOnlyJson))
            {
                var worstIds = summaryDoc.RootElement.GetProperty("worst_servers").EnumerateArray()
                    .Select(w => w.GetProperty("server_id").GetInt32()).ToHashSet();
                var cardIds = worstDoc.RootElement.GetProperty("cards").EnumerateArray()
                    .Select(c => c.GetProperty("server_id").GetInt32()).ToList();
                Assert.NotEmpty(cardIds);
                Assert.All(cardIds, id => Assert.Contains(id, worstIds));
                Assert.True(cardIds.Count < 3, "worst_only must narrow the 3-card fleet, not return it whole.");
            }

            var invalidBand = await DarlingMcpFleetTools.GetFleetOverview(postgres, detail: "cards", band: "not-a-band");
            using (var doc = JsonDocument.Parse(invalidBand))
            {
                Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSmallFleetRowsAsync);
        }
    }

    /// <summary>
    /// #4198's headline number, reproduced on the rig: <c>get_fleet_overview</c>'s default call
    /// (<c>detail="summary"</c>) on a 45-server fleet stays under <see cref="McpResponseBudget.DefaultBytes"/>,
    /// where <c>detail="cards"</c> (the tool's whole shape before #4198) does not have to. Both byte counts are
    /// written to the test output so the PR body can quote the rig's own before/after numbers.
    /// </summary>
    [Fact]
    public async Task GetFleetOverview_DefaultSummaryCall_OnA45ServerFleet_StaysUnderTheResponseBudget()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live MCP fleet-response-budget tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteByteFleetRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            for (var i = 0; i < ByteFleetIds.Length; i++)
            {
                var id = ByteFleetIds[i];
                var name = $"zz-m2b-byte-{i:D3}";
                await InsertServerAsync(connection, id, name, ct);
                await InsertCollectionLogAsync(connection, id, name, now.AddSeconds(-30), 40 + i, ct);
            }

            var summaryJson = await DarlingMcpFleetTools.GetFleetOverview(postgres, hours_back: 1, detail: "summary");
            var cardsJson = await DarlingMcpFleetTools.GetFleetOverview(postgres, hours_back: 1, detail: "cards");
            var summaryBytes = Encoding.UTF8.GetByteCount(summaryJson);
            var cardsBytes = Encoding.UTF8.GetByteCount(cardsJson);

            TestContext.Current.SendDiagnosticMessage(
                $"get_fleet_overview on {ByteFleetIds.Length} servers: summary={summaryBytes:N0} bytes, cards={cardsBytes:N0} bytes, budget={McpResponseBudget.DefaultBytes:N0} bytes.");

            Assert.True(summaryBytes <= McpResponseBudget.DefaultBytes,
                $"detail=summary was {summaryBytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteByteFleetRowsAsync);
        }
    }

    /// <summary>
    /// #4199's default-limit sizing, reproduced on the rig: a fleet-wide <c>get_collection_log</c> call at
    /// DEFAULT arguments (server_name omitted, limit omitted) on a fleet with more rows than the fleet
    /// default stays under <see cref="McpResponseBudget.DefaultBytes"/>. The byte count is written to the
    /// test output for the PR body.
    /// </summary>
    [Fact]
    public async Task GetCollectionLog_FleetFormDefaultCall_StaysUnderTheResponseBudget()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live MCP fleet-response-budget tests.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteLogFleetRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            for (var s = 0; s < LogFleetIds.Length; s++)
            {
                var id = LogFleetIds[s];
                var name = $"zz-m2b-logfleet-{s:D2}";
                await InsertServerAsync(connection, id, name, ct);

                for (var r = 0; r < LogFleetRowsPerServer; r++)
                {
                    /* A moderate error_message on every third row, so the average row approaches a real
                       row's width instead of the optimistic all-columns-null minimum. */
                    var withError = r % 3 == 0;
                    await InsertCollectionLogWithDetailAsync(
                        connection, id, name, now.AddMinutes(-(r + 1)), 100 + r * 7,
                        withError ? "Timeout expired waiting on a monitored-server round trip during a scheduled collection cycle." : null,
                        ct);
                }
            }

            var defaultJson = await DarlingMcpDataTools.GetCollectionLog(postgres, server_name: null, hours_back: 24);
            var defaultBytes = Encoding.UTF8.GetByteCount(defaultJson);

            TestContext.Current.SendDiagnosticMessage(
                $"get_collection_log fleet form, default args, {LogFleetIds.Length}x{LogFleetRowsPerServer} rows seeded: {defaultBytes:N0} bytes, budget={McpResponseBudget.DefaultBytes:N0} bytes, fleet default limit={McpResponseBudget.CollectionLogFleetDefaultLimit}.");

            Assert.True(defaultBytes <= McpResponseBudget.DefaultBytes,
                $"Fleet-form default call was {defaultBytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteLogFleetRowsAsync);
        }
    }

    private static async Task InsertServerAsync(NpgsqlConnection connection, int serverId, string name, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, engine_kind, created_date)
VALUES ($1, $2, $2, TRUE, 2, $3, $4)", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(MonitoredEngineKind.SqlServer);
        /* Registered two days before "now": a server with an EMPTY window (no collection_log row at all)
           bands Offline only when it was registered before the window started (DarlingFleetReader's
           ClassifyWindowedFreshness comment) — a server registered inside the window instead reads
           AwaitingFirstCollection, a different, non-offline state. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-2), DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertCollectionLogAsync(
        NpgsqlConnection connection, int serverId, string name, DateTime collectionTime, double durationMs, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status, duration_ms, rows_collected)
VALUES ($1, $2, $3, 'wait_stats', $4, 'SUCCESS', $5, 10)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(durationMs);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertCollectionLogWithDetailAsync(
        NpgsqlConnection connection, int serverId, string name, DateTime collectionTime, double durationMs,
        string? errorMessage, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, status, duration_ms, sql_duration_ms,
     duckdb_duration_ms, rows_collected, error_message)
VALUES ($1, $2, $3, 'query_store', $4, $6, $5, ($5 * 0.6)::int, ($5 * 0.1)::int, 250, $7)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(durationMs);
        command.Parameters.AddWithValue(errorMessage is null ? "SUCCESS" : "ERROR");
        command.Parameters.AddWithValue(errorMessage ?? (object)DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task DeleteSmallFleetRowsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        DeleteFleetRowsAsync(connection, SmallFleetIds, ct);

    private static Task DeleteByteFleetRowsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        DeleteFleetRowsAsync(connection, ByteFleetIds, ct);

    private static Task DeleteLogFleetRowsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        DeleteFleetRowsAsync(connection, LogFleetIds, ct);

    private static async Task DeleteFleetRowsAsync(NpgsqlConnection connection, int[] ids, CancellationToken ct)
    {
        var idList = string.Join(", ", ids.Select(i => i.ToString(CultureInfo.InvariantCulture)));

        foreach (var table in new[] { "collection_log", "servers" })
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id IN ({idList});", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
