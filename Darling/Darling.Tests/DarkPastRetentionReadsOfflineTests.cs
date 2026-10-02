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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3967: a server whose whole history the collection log's retention has dropped reads Offline on every
/// surface that reads a server's newest collection with no window, not "Awaiting first collection".
///
/// <para><b>The defect.</b> <c>list_servers</c>, the WPF sidebar dot and the WPF Overview card read the newest
/// collection with no window, and <c>collection_log</c> keeps <see cref="DarlingRetentionHorizons.CollectionLogRetentionDays"/>
/// days. A server dark for longer comes back null, and to <see cref="ServerHealthClassifier.ClassifyFreshness(DateTime?, DateTime)"/>
/// a null is never collected. #3935 fixed the same mislabel on the fleet card, whose read looks back 48 hours;
/// this is the rule it used, moved into <see cref="ServerHealthClassifier"/> and applied to the other reads
/// with the retention as what they could see.</para>
///
/// <para><b>What is pinned.</b> The shared rule over every input, its boundary, the horizon, the fleet card
/// delegating to it, each surface's reading (dot, card, <c>list_servers</c>, the two text cells), every
/// registry read carrying the registration, and the loader stamping it.
/// <see cref="DarkPastRetentionReadsOfflineLivePostgresTests"/> holds the seam against a store.</para>
/// </summary>
public sealed class DarkPastRetentionReadsOfflineTests
{
    private static readonly DateTime Now = new(2026, 9, 23, 12, 0, 0, DateTimeKind.Utc);

    private static DateTime Horizon => DarlingRetentionHorizons.CollectionLogHorizon(Now);

    /// <summary>A registration far enough back that retention has had time to take every row.</summary>
    private static DateTime LongAgo => Now.AddDays(-90);

    /// <summary>A registration inside the retained span: any collection it made would still be there.</summary>
    private static DateTime Recently => Now.AddDays(-10);

    /// <summary>
    /// The rule. A newest collection bands on the ladder, whatever the registration says; with none, a
    /// registration before what the read could see is Offline, one at or after it keeps the ladder's
    /// never-collected reading, and no registration keeps it too.
    /// </summary>
    [Fact]
    public void TheSharedRule_DepartsFromTheLadder_OnlyForANullOnAnOlderRegistration()
    {
        foreach (var registeredAt in new DateTime?[] { null, LongAgo, Horizon.AddTicks(-1), Horizon, Recently })
        {
            foreach (var lastCollection in Readings())
            {
                Assert.Equal(
                    ServerHealthClassifier.ClassifyFreshness(lastCollection, Now),
                    ServerHealthClassifier.ClassifyFreshness(lastCollection, registeredAt, Horizon, Now));
            }
        }

        Assert.Equal(ServerFreshness.Offline, ServerHealthClassifier.ClassifyFreshness(null, LongAgo, Horizon, Now));
        Assert.Equal(ServerFreshness.NeverCollected, ServerHealthClassifier.ClassifyFreshness(null, Recently, Horizon, Now));
        Assert.Equal(ServerFreshness.NeverCollected, ServerHealthClassifier.ClassifyFreshness(null, null, Horizon, Now));
    }

    /// <summary>
    /// The boundary is the instant the read could first see. Registered AT it, any collection would be at or
    /// after it and so still visible, which proves an empty read means never collected. One tick earlier, a
    /// collection the read cannot see is possible.
    /// </summary>
    [Fact]
    public void TheBoundary_IsTheFirstInstantTheReadCouldSee()
    {
        Assert.Equal(ServerFreshness.NeverCollected, ServerHealthClassifier.ClassifyFreshness(null, Horizon, Horizon, Now));
        Assert.Equal(ServerFreshness.Offline, ServerHealthClassifier.ClassifyFreshness(null, Horizon.AddTicks(-1), Horizon, Now));
    }

    /// <summary>The horizon an unbounded read of the collection log can see: the log's retention back, as
    /// naive UTC, the store's convention.</summary>
    [Fact]
    public void TheHorizon_IsTheCollectionLogsRetentionBack_AsNaiveUtc()
    {
        Assert.Equal(60, DarlingRetentionHorizons.CollectionLogRetentionDays);
        Assert.Equal(new DateTime(2026, 7, 25, 12, 0, 0), Horizon);
        Assert.Equal(DateTimeKind.Unspecified, Horizon.Kind);
        Assert.Equal(DarlingRetention.CollectionLogRetentionDays, DarlingRetentionHorizons.CollectionLogRetentionDays);
    }

    /// <summary>
    /// The fleet card is the same rule with its own reach: its read looks back 48 hours, so that is what it
    /// hands the shared rule. One rule, two horizons, and the two can only differ where the reads do.
    /// </summary>
    [Fact]
    public void TheFleetCard_AppliesTheSharedRuleToItsWindow()
    {
        var windowStart = DarlingFleetReader.LastCollectionWindowStart(Now);

        foreach (var registeredAt in new DateTime?[] { null, LongAgo, Recently, windowStart.AddTicks(-1), windowStart, Now.AddMinutes(-10) })
        {
            foreach (var lastCollection in Readings().Append(null))
            {
                Assert.Equal(
                    ServerHealthClassifier.ClassifyFreshness(lastCollection, registeredAt, windowStart, Now),
                    DarlingFleetReader.ClassifyWindowedFreshness(lastCollection, registeredAt, Now));
            }
        }
    }

    /// <summary>
    /// <c>list_servers</c>: the status token, through the tool's own renderer. A server whose history aged out
    /// is Offline with no last collection; one registered inside the retention with none is still
    /// AwaitingFirstCollection, which is the published token the fix must not disturb.
    /// </summary>
    [Fact]
    public void ListServers_ReadsAServerWhoseHistoryAgedOut_Offline()
    {
        Assert.Equal("Offline", DarlingMcpDataTools.FreshnessStatus(null, LongAgo, Now));
        Assert.Equal("AwaitingFirstCollection", DarlingMcpDataTools.FreshnessStatus(null, Recently, Now));
        Assert.Equal("AwaitingFirstCollection", DarlingMcpDataTools.FreshnessStatus(null, null, Now));

        var json = DarlingMcpDataTools.RenderServerList(
            new[]
            {
                new DarlingDataReader.ServerListRow(1, "aged-out", null, 16, null, MonitoredEngineKind.SqlServer, null, LongAgo),
                new DarlingDataReader.ServerListRow(2, "just-added", null, 16, null, MonitoredEngineKind.SqlServer, null, Now.AddMinutes(-10)),
                new DarlingDataReader.ServerListRow(3, "collecting", null, 16, Now.AddSeconds(-30), MonitoredEngineKind.SqlServer, null, LongAgo),
            },
            Now,
            DarlingPeerDirectory.Snapshot.Empty);

        var rows = Statuses(json);
        Assert.Equal("Offline", rows["aged-out"].Status);
        Assert.Null(rows["aged-out"].LastCollection);
        Assert.Equal("AwaitingFirstCollection", rows["just-added"].Status);
        Assert.Equal("Online", rows["collecting"].Status);
    }

    /// <summary>
    /// Every registry read behind an unbounded freshness surface carries the registration: both of the
    /// viewer's server reads, because the sidebar uses <c>ManagedServersSql</c> on any seeded store and
    /// <c>ServersSql</c> otherwise (the #3145 lesson), and <c>list_servers</c>'s. The managed read takes it from
    /// the OBSERVED registry, the only side that has one.
    /// </summary>
    [Fact]
    public void EveryUnboundedFreshnessRead_CarriesTheRegistration()
    {
        Assert.Contains("postgres_major_version, created_date FROM servers", ViewerDataService.ServersSql, StringComparison.Ordinal);
        Assert.Contains("s.created_date", ViewerDataService.ManagedServersSql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN servers s ON s.server_id = c.server_id", ViewerDataService.ManagedServersSql, StringComparison.Ordinal);
        Assert.Contains("s.created_date", DarlingDataReader.ServerListSql, StringComparison.Ordinal);
    }

    /// <summary>The sidebar dot, through the row's own <c>ApplyFreshness</c>: Offline for a server whose
    /// history aged out, awaiting for one registered inside the retention, awaiting for one the service has not
    /// connected to (no registration).</summary>
    [Fact]
    public void TheSidebarDot_ReadsAServerWhoseHistoryAgedOut_Offline()
    {
        Assert.Equal(ServerCollectionStatus.Offline, Dot(LongAgo, null));
        Assert.Equal(ServerCollectionStatus.AwaitingFirstCollection, Dot(Recently, null));
        Assert.Equal(ServerCollectionStatus.AwaitingFirstCollection, Dot(null, null));
    }

    /// <summary>
    /// The Overview card, through its own <c>ApplyFreshness</c>: Offline, and a Last collection cell that says
    /// none is retained rather than "Never". A card awaiting its first collection still says "Never".
    /// </summary>
    [Fact]
    public void TheOverviewCard_ReadsAServerWhoseHistoryAgedOut_Offline_AndSaysNoneRetained()
    {
        var agedOut = Card(LongAgo, null);
        Assert.False(agedOut.IsOnline);
        Assert.Equal(ServerCollectionStatus.Offline.Word(), agedOut.StatusDisplay);
        Assert.Equal("None retained", agedOut.LastCollectionDisplay);

        var justAdded = Card(Recently, null);
        Assert.Null(justAdded.IsOnline);
        Assert.True(justAdded.AwaitingFirstCollection);
        Assert.Equal("Never", justAdded.LastCollectionDisplay);
    }

    /// <summary>
    /// Every reading with a newest collection is untouched on every surface: the dot, the card and
    /// <c>list_servers</c> band fresh, stale and offline exactly as the ladder does, whatever the registration.
    /// </summary>
    [Fact]
    public void FreshStaleAndOfflineReadings_AreUnchangedOnEverySurface()
    {
        foreach (var registeredAt in new DateTime?[] { null, LongAgo, Recently })
        {
            foreach (var lastCollection in Readings())
            {
                var expected = ServerCollectionStatusRules.FromFreshness(ServerHealthClassifier.ClassifyFreshness(lastCollection, Now));
                Assert.Equal(expected, Dot(registeredAt, lastCollection));
                Assert.Equal(expected.Word(), Card(registeredAt, lastCollection).StatusDisplay);
                Assert.Equal(expected.McpToken(), DarlingMcpDataTools.FreshnessStatus(lastCollection, registeredAt, Now));
            }
        }
    }

    /// <summary>
    /// The seam the card cannot pin by itself: the Overview loader stamps the registration from the registry
    /// row it already holds, and it does so BEFORE banding. A stamp after <c>ApplyFreshness</c>, or none, compiles
    /// clean and leaves every aged-out card on "Awaiting first collection".
    /// </summary>
    [Fact]
    public void TheOverviewLoader_StampsTheRegistrationBeforeItBands()
    {
        var loader = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml.cs");

        var stamp = loader.IndexOf("summary.RegisteredAt = server.RegisteredAt;", StringComparison.Ordinal);
        var band = loader.IndexOf("summary.ApplyFreshness(nowUtc);", StringComparison.Ordinal);
        Assert.True(stamp >= 0, "The Overview loader no longer stamps the registration onto the card.");
        Assert.True(band > stamp, "The Overview loader bands the card before it has the registration.");
        Assert.Equal(band, loader.LastIndexOf("summary.ApplyFreshness(nowUtc);", StringComparison.Ordinal));
    }

    /// <summary>The Manage Servers "Last Collected" cell says none is retained for a server whose history aged
    /// out, and "Never" only for one that has not collected.</summary>
    [Fact]
    public void TheManageServersCell_SaysNoneRetained_ForAServerWhoseHistoryAgedOut()
    {
        var row = new MonitoredServerRow { ServerId = 1, Name = "sql-01", Host = "sql-01" };

        Assert.Equal("None retained", new ManagedServerListItem(row, false) { HistoryAgedOut = true }.LastCollectedDisplay);
        Assert.Equal("Never", new ManagedServerListItem(row, false).LastCollectedDisplay);
    }

    /* ─────────────────────────── helpers ─────────────────────────── */

    /// <summary>A newest collection on each rung of the ladder.</summary>
    private static IEnumerable<DateTime?> Readings() => new DateTime?[]
    {
        Now.AddSeconds(-30),
        Now - ServerHealthThresholds.StaleThreshold - TimeSpan.FromMinutes(1),
        Now - ServerHealthThresholds.OfflineThreshold - TimeSpan.FromHours(2),
        Now.AddDays(-30),
    };

    private static ServerCollectionStatus Dot(DateTime? registeredAt, DateTime? lastCollection)
    {
        var server = new DarlingServer(1, "sql-01", "sql-01", true, 16, registeredAt: registeredAt);
        server.ApplyFreshness(lastCollection, Now);
        return server.CardStatus;
    }

    private static ServerSummaryItem Card(DateTime? registeredAt, DateTime? lastCollection)
    {
        var card = new ServerSummaryItem { ServerId = 1, DisplayName = "sql-01", LastCollectionTime = lastCollection, RegisteredAt = registeredAt };
        card.ApplyFreshness(Now);
        return card;
    }

    private static Dictionary<string, (string Status, string? LastCollection)> Statuses(string json)
    {
        using var document = JsonDocument.Parse(json);
        var rows = new Dictionary<string, (string, string?)>(StringComparer.Ordinal);
        foreach (var entry in document.RootElement.GetProperty("servers").EnumerateArray())
        {
            rows[entry.GetProperty("server_name").GetString()!] = (
                entry.GetProperty("status").GetString()!,
                entry.GetProperty("last_collection").ValueKind == JsonValueKind.Null ? null : entry.GetProperty("last_collection").GetString());
        }

        return rows;
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG): the seam, against a store. Four registered servers — one whose history retention
/// has dropped (registered 90 days ago, no rows), one registered ten minutes ago with no rows yet, one dark
/// for five days inside the retention, and one collecting — read through the SHIPPED reads: <c>list_servers</c>,
/// both of the viewer's registry reads, the sidebar's freshness read, the Overview card's summary read, and
/// the fleet overview. Every surface must call the first Offline and the second awaiting, and all of them must
/// agree about all four.
/// </summary>
[Collection("live-postgres")]
public sealed class DarkPastRetentionReadsOfflineLivePostgresTests
{
    private const int AgedOut = -967301;
    private const int JustRegistered = -967302;
    private const int DarkInsideRetention = -967303;
    private const int Collecting = -967304;

    private static readonly int[] s_all = { AgedOut, JustRegistered, DarkInsideRetention, Collecting };

    [Fact]
    public async Task EverySurface_ReadsAServerWhoseHistoryAgedOut_Offline_AndAgrees_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #3967 test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSentinelRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var now = Micro(DateTime.UtcNow);
            var registrations = new Dictionary<int, DateTime>
            {
                [AgedOut] = now.AddDays(-90),
                [JustRegistered] = now.AddMinutes(-10),
                [DarkInsideRetention] = now.AddDays(-30),
                [Collecting] = now.AddDays(-9),
            };

            foreach (var (id, registeredAt) in registrations)
            {
                await ExecAsync(connection, ct,
                    "INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, sql_major_version, engine_kind, created_date, modified_date) VALUES ($1, $2, $2, TRUE, 3, 16, 'sqlserver', $3, $3)",
                    id, Name(id), registeredAt);
                await ExecAsync(connection, ct,
                    "INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled, created_at, modified_at) VALUES ($1, $2, $2, TRUE, $3, $3) ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE",
                    id, Name(id), registeredAt);
            }

            await ExecAsync(connection, ct, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING");

            /* The aged-out server's rows are gone, which is the whole case; the other two with history keep it. */
            await InsertCollectionLogAsync(connection, ct, DarkInsideRetention, now.AddDays(-5));
            await InsertCollectionLogAsync(connection, ct, Collecting, now.AddMinutes(-1));

            /* list_servers. */
            var list = Statuses(await DarlingMcpDataTools.ListServers(postgres));
            Assert.Equal("Offline", list[Name(AgedOut)].Status);
            Assert.Null(list[Name(AgedOut)].LastCollection);
            Assert.Equal("AwaitingFirstCollection", list[Name(JustRegistered)].Status);
            Assert.Equal("Offline", list[Name(DarkInsideRetention)].Status);
            Assert.Equal("Online", list[Name(Collecting)].Status);

            /* Both registry reads carry the registration, off the ordinals the shipped readers use. */
            Assert.True(await viewer.IsConfigSeededAsync(ct), "the store reads as unseeded, so the managed read would not be the one exercised");
            foreach (var servers in new[] { await viewer.GetServersAsync(ct), await viewer.GetManagedServersAsync(ct) })
            {
                foreach (var id in s_all)
                {
                    Assert.Equal(registrations[id], servers.Single(s => s.ServerId == id).RegisteredAt);
                }
            }

            /* The sidebar dot, through the freshness read and the row's own ApplyFreshness. */
            var managed = (await viewer.GetManagedServersAsync(ct)).Where(s => s_all.Contains(s.ServerId)).ToDictionary(s => s.ServerId);
            var freshness = await viewer.GetServerFreshnessAsync(ct);
            foreach (var server in managed.Values)
            {
                server.ApplyFreshness(freshness.TryGetValue(server.ServerId, out var last) ? last : null, now);
            }

            Assert.Equal(ServerCollectionStatus.Offline, managed[AgedOut].CardStatus);
            Assert.Equal(ServerCollectionStatus.AwaitingFirstCollection, managed[JustRegistered].CardStatus);
            Assert.Equal(ServerCollectionStatus.Offline, managed[DarkInsideRetention].CardStatus);
            Assert.Equal(ServerCollectionStatus.Online, managed[Collecting].CardStatus);

            /* The Overview card: the summary read, stamped the way the loader stamps it, then banded. */
            var cards = new Dictionary<int, ServerSummaryItem>();
            foreach (var server in managed.Values)
            {
                var card = await viewer.GetServerSummaryAsync(server.ServerId, server.DisplayName, cancellationToken: ct);
                card.RegisteredAt = server.RegisteredAt;
                card.ApplyFreshness(now);
                cards[server.ServerId] = card;
            }

            Assert.Equal(ServerCollectionStatus.Offline.Word(), cards[AgedOut].StatusDisplay);
            Assert.Equal("None retained", cards[AgedOut].LastCollectionDisplay);
            Assert.Equal(ServerCollectionStatus.AwaitingFirstCollection.Word(), cards[JustRegistered].StatusDisplay);
            Assert.Equal("Never", cards[JustRegistered].LastCollectionDisplay);

            /* The fleet card (#3935's rule over its 48 hours) agrees with all of them. */
            var fleet = (await DarlingFleetReader.GetFleetOverviewAsync(postgres, now.AddHours(-1), now, now, cancellationToken: ct))
                .Cards.Where(c => s_all.Contains(c.ServerId)).ToDictionary(c => c.ServerId);

            /* One answer per server, on every surface. */
            foreach (var id in s_all)
            {
                var dot = managed[id].CardStatus;
                Assert.Equal(dot.McpToken(), list[Name(id)].Status);
                Assert.Equal(dot.Word(), cards[id].StatusDisplay);
                Assert.Equal(dot.Word(), fleet[id].Status);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelRowsAsync);
        }
    }

    private static string Name(int id) => "retention3967" + id.ToString(System.Globalization.CultureInfo.InvariantCulture);

    private static Task InsertCollectionLogAsync(NpgsqlConnection connection, CancellationToken ct, int id, DateTime at) =>
        ExecAsync(connection, ct,
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1, $2, $3, 'wait_stats', $4, 'SUCCESS')",
            CollectionIdGenerator.Next(), id, Name(id), at);

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql, params object[] args)
    {
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var arg in args)
        {
            command.Parameters.AddWithValue(arg is DateTime dt ? DateTime.SpecifyKind(dt, DateTimeKind.Unspecified) : arg);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static Dictionary<string, (string Status, string? LastCollection)> Statuses(string json)
    {
        using var document = JsonDocument.Parse(json);
        var rows = new Dictionary<string, (string, string?)>(StringComparer.Ordinal);
        foreach (var entry in document.RootElement.GetProperty("servers").EnumerateArray())
        {
            rows[entry.GetProperty("server_name").GetString()!] = (
                entry.GetProperty("status").GetString()!,
                entry.GetProperty("last_collection").ValueKind == JsonValueKind.Null ? null : entry.GetProperty("last_collection").GetString());
        }

        return rows;
    }

    /// <summary>PostgreSQL <c>timestamp</c> is microsecond-resolution; .NET ticks are 100 ns. Seeded instants
    /// are floored to the microsecond so the values read back compare equal to the ones written.</summary>
    private static DateTime Micro(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % 10)), DateTimeKind.Unspecified);

    private static async Task DeleteSentinelRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = string.Join(", ", s_all.Select(id => id.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        foreach (var table in new[] { "collection_log", "servers", "config.config_monitored_servers" })
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id IN ({ids})", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
