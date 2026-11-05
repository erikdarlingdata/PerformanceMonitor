using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7590: a server whose connect has failed reads Offline at once, with the reason and the error, instead of showing
/// "collection stale" and green tiles for up to 30 minutes. A server with the same age and no failed connect keeps today's rule.
/// Driven through the fleet reader's own card builder and roll-up, and the JSON get_fleet_overview serves from them.
/// </summary>
[Collection("live-postgres")]
public class ServerUnreachableReadsOfflineTests
{
    private static readonly DateTime Now = new(2026, 11, 4, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime FiveMinutesAgo = Now.AddMinutes(-5);
    private static readonly DateTime DownSince = Now.AddMinutes(-4);
    private const string ConnectError = "A connection was successfully established with the server, but then an error occurred during the pre-login handshake.";

    private static FleetServerCardHolder Build(DateTime? downSince) => new(
        DarlingFleetReader.BuildCard(
            new DarlingFleetReader.FleetServerRow(1, "sql-01", "sql-01", 3, MonitoredEngineKind.SqlServer, false, downSince, downSince.HasValue ? ConnectError : null),
            new DarlingFleetReader.CpuRow(20, 5),
            default,
            default,
            default,
            new DarlingFleetReader.ThreadsRow(512, 100, 0, 0),
            default,
            default,
            default,
            FiveMinutesAgo,
            new DarlingFleetReader.CollectorCounts(40, 0, 40, 0),
            null,
            Now,
            TimeSpan.FromHours(1),
            DeadlockRateThresholds.Default,
            Now.AddDays(-10)));

    private readonly record struct FleetServerCardHolder(FleetServerCard Card);

    [Fact]
    public void A_failed_connect_with_a_five_minute_old_collection_reads_Offline_with_the_unreachable_reason()
    {
        var card = Build(DownSince).Card;

        Assert.Equal(FleetHealthBand.Offline, card.Band);
        Assert.Equal("Offline", card.Status);
        Assert.False(card.IsOnline);
        Assert.False(card.CollectionStale);

        var reason = DarlingFleetReader.BuildReason(card);
        Assert.Equal("Server unreachable since 2026-11-04 11:56 UTC: " + ConnectError, reason);
        Assert.DoesNotContain("collection stale", reason, StringComparison.Ordinal);
    }

    [Fact]
    public void The_same_age_with_no_failed_connect_still_reads_stale()
    {
        var card = Build(null).Card;

        Assert.Equal(FleetHealthBand.Warning, card.Band);
        Assert.True(card.IsOnline);
        Assert.True(card.CollectionStale);
        Assert.Contains("collection stale", DarlingFleetReader.BuildReason(card), StringComparison.Ordinal);
        Assert.Null(card.ConnectDownSince);
    }

    /* #1776 own-store: this fact mints its own scratch database through ScratchPostgres. */
    [Fact]
    public void List_servers_reads_a_failed_connect_Offline_with_the_reason_while_a_quiet_server_keeps_the_age_rule()
    {
        var json = DarlingMcpDataTools.RenderServerList(
            new[]
            {
                new DarlingDataReader.ServerListRow(1, "down", null, 16, FiveMinutesAgo, MonitoredEngineKind.SqlServer, null, FiveMinutesAgo.AddDays(-3),
                    ConnectDownSince: DownSince, LastConnectError: ConnectError),
                new DarlingDataReader.ServerListRow(2, "quiet", null, 16, FiveMinutesAgo, MonitoredEngineKind.SqlServer, null, FiveMinutesAgo.AddDays(-3)),
            },
            Now,
            DarlingPeerDirectory.Snapshot.Empty);

        using var document = JsonDocument.Parse(json);
        var rows = document.RootElement.GetProperty("servers").EnumerateArray()
            .ToDictionary(e => e.GetProperty("server_name").GetString()!, e => e, StringComparer.Ordinal);

        Assert.Equal("Offline", rows["down"].GetProperty("status").GetString());
        Assert.Equal("Server unreachable since 2026-11-04 11:56 UTC: " + ConnectError, rows["down"].GetProperty("reason").GetString());
        Assert.Equal(DownSince.ToString("o"), rows["down"].GetProperty("connect_down_since").GetString());
        Assert.Equal(ConnectError, rows["down"].GetProperty("last_connect_error").GetString());

        Assert.Equal("Warning", rows["quiet"].GetProperty("status").GetString());
        Assert.Equal(JsonValueKind.Null, rows["quiet"].GetProperty("reason").ValueKind);
        Assert.Equal(JsonValueKind.Null, rows["quiet"].GetProperty("connect_down_since").ValueKind);
    }

    [Fact]
    public void The_admin_servers_grid_reads_a_failed_connect_Offline_too()
    {
        DarlingAdminServersReader.Row Row(DateTime? downSince) =>
            new(1, "cfg", "host", null, false, "sqlserver", 0, "integrated", true, 0m, new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Unspecified),
                "srv", "srv", 16, null, null, MonitoredEngineKind.SqlServer, FiveMinutesAgo.AddDays(-3), FiveMinutesAgo, null, downSince);

        Assert.Equal("Offline", DarlingAdminServersReader.Build(new[] { Row(DownSince) }, Now).Single().freshness);
        Assert.Equal("Warning", DarlingAdminServersReader.Build(new[] { Row(null) }, Now).Single().freshness);
    }

    [Fact]
    public async Task A_failed_connect_is_stored_read_back_by_the_fleet_read_and_a_good_connect_clears_it()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live connect-state test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        static async Task UpsertAsync(NpgsqlDataSource source, CancellationToken token)
        {
            await using var command = source.CreateCommand(DarlingObservability.UpsertServerSql);
            command.Parameters.AddWithValue(77);
            command.Parameters.AddWithValue("sql-live");
            command.Parameters.AddWithValue("sql-live");
            command.Parameters.AddWithValue(3);
            command.Parameters.AddWithValue(16);
            command.Parameters.AddWithValue("sqlserver");
            command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(0m);
            command.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
            command.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            command.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Boolean });
            command.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Boolean });
            await command.ExecuteNonQueryAsync(token);
        }

        static async Task<(DateTime? Since, string? Error)> FleetReadAsync(NpgsqlDataSource source, CancellationToken token)
        {
            await using var command = source.CreateCommand(DarlingFleetReader.FleetServersSql);
            await using var reader = await command.ExecuteReaderAsync(token);
            Assert.True(await reader.ReadAsync(token));
            return (reader.IsDBNull(6) ? null : reader.GetDateTime(6), reader.IsDBNull(7) ? null : reader.GetString(7));
        }

        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await using var config = new NpgsqlCommand(
                "INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES (77, 'sql-live', 'sql-live', TRUE)", connection);
            await config.ExecuteNonQueryAsync(ct);
        }

        /* A server that has never connected has no registry row: the first failed connect writes one from the configuration (#7590 part F;
           the live test of the real connect path is ServerNeverConnectedTests). Facts only a connect learns stay NULL. */
        var configured = new PerformanceMonitor.Darling.Service.MonitoredServer { Name = "sql-live", Host = "sql-live", StoredServerId = 77 };
        await DarlingObservability.MarkConnectFailedAsync(postgres, configured, "no route", null, ct);
        await using (var row = postgres.CreateCommand("SELECT count(*) FILTER (WHERE sql_major_version IS NULL AND sql_engine_edition IS NULL AND connect_down_since IS NOT NULL AND last_connect_error = 'no route') FROM servers WHERE server_id = 77"))
        {
            Assert.Equal(1L, await row.ExecuteScalarAsync(ct));
        }

        await UpsertAsync(postgres, ct);
        Assert.Equal((null, null), await FleetReadAsync(postgres, ct));

        await DarlingObservability.MarkConnectFailedAsync(postgres, configured, "first error", null, ct);
        var (firstSince, firstError) = await FleetReadAsync(postgres, ct);
        Assert.NotNull(firstSince);
        Assert.Equal("first error", firstError);

        /* The second failure keeps the outage's first time and takes the newest error. */
        await DarlingObservability.MarkConnectFailedAsync(postgres, configured, "second error", null, ct);
        var (secondSince, secondError) = await FleetReadAsync(postgres, ct);
        Assert.Equal(firstSince, secondSince);
        Assert.Equal("second error", secondError);

        /* list_servers and the admin grid read the same two columns and band the row Offline. */
        var listed = Assert.Single(await DarlingDataReader.GetServerListAsync(postgres, ct));
        Assert.Equal(secondSince, listed.ConnectDownSince);
        Assert.Equal("second error", listed.LastConnectError);
        Assert.Equal("Offline", DarlingMcpDataTools.FreshnessStatus(DateTime.UtcNow.AddHours(-1), listed.RegisteredAt, DateTime.UtcNow, listed.ConnectDownSince));
        var admin = Assert.Single(await DarlingAdminServersReader.ReadAsync(postgres, ct));
        Assert.Equal(secondSince, admin.ConnectDownSince);

        var connection2 = await DarlingMcpDataTools.ReadServerConnectionAsync(postgres, 77, DateTime.UtcNow, null, ct);
        Assert.Equal("waiting_on_connection", JsonDocument.Parse(JsonSerializer.Serialize(connection2)).RootElement.GetProperty("status").GetString());

        /* A collection newer than the first failure proves a later connect worked: the block goes, though the flag is still stored. */
        await using (var seed = postgres.CreateCommand(
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1, 77, 'sql-live', 'wait_stats', $2, 'SUCCESS')"))
        {
            seed.Parameters.AddWithValue(CollectionIdGenerator.Next());
            seed.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow.AddSeconds(5), DateTimeKind.Unspecified));
            await seed.ExecuteNonQueryAsync(ct);
        }

        Assert.Null(await DarlingMcpDataTools.ReadServerConnectionAsync(postgres, 77, DateTime.UtcNow, null, ct));
        await using (var clear = postgres.CreateCommand("DELETE FROM collection_log WHERE server_id = 77"))
        {
            await clear.ExecuteNonQueryAsync(ct);
        }

        Assert.NotNull(await DarlingMcpDataTools.ReadServerConnectionAsync(postgres, 77, DateTime.UtcNow, null, ct));

        await UpsertAsync(postgres, ct);
        Assert.Equal((null, null), await FleetReadAsync(postgres, ct));
        Assert.Null(Assert.Single(await DarlingDataReader.GetServerListAsync(postgres, ct)).ConnectDownSince);
        Assert.Null(await DarlingMcpDataTools.ReadServerConnectionAsync(postgres, 77, DateTime.UtcNow, null, ct));
    }

    [Fact]
    public void Get_fleet_overview_json_carries_the_offline_band_the_reason_and_the_error()
    {
        var rollup = DarlingFleetReader.BuildRollup(new[] { Build(DownSince).Card }, Now, Now.AddHours(-1), Now);
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(rollup));
        var worst = doc.RootElement.GetProperty("worst_servers")[0];
        Assert.StartsWith("Server unreachable since 2026-11-04 11:56 UTC", worst.GetProperty("reason").GetString(), StringComparison.Ordinal);

        var card = doc.RootElement.GetProperty("cards")[0];
        Assert.Equal("Offline", card.GetProperty("status").GetString());
        Assert.Equal(ConnectError, card.GetProperty("last_connect_error").GetString());
        Assert.True(card.TryGetProperty("connect_down_since", out var since) && since.ValueKind == JsonValueKind.String);
    }

    [Fact]
    public void The_last_known_label_gives_the_age_of_the_values()
    {
        Assert.Equal("last known, 12 min ago", ServerHealthClassifier.LastKnownLabel(Now.AddMinutes(-12), Now));
        Assert.Equal("last known, 3 h ago", ServerHealthClassifier.LastKnownLabel(Now.AddHours(-3), Now));
        Assert.Equal("no last known value", ServerHealthClassifier.LastKnownLabel(null, Now));
    }

    /* The clearing write rides a swallowed upsert, so a flag that survives it must not hold a collecting server Offline: a collection newer than
       the first failure proves a later connect worked. */
    private static readonly DateTime StaleFlag = FiveMinutesAgo.AddMinutes(-30);

    [Fact]
    public void A_stored_failed_connect_older_than_the_newest_collection_reads_by_age_not_Offline()
    {
        var card = Build(StaleFlag).Card;

        Assert.Equal(FleetHealthBand.Warning, card.Band);
        Assert.True(card.IsOnline);
        Assert.Null(card.ConnectDownSince);
        Assert.Null(card.LastConnectError);
        var reason = DarlingFleetReader.BuildReason(card);
        Assert.DoesNotContain("unreachable", reason, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("collection stale", reason, StringComparison.Ordinal);

        /* The web page reads this card (collector-run.js turns Run all collectors now off on connect_down_since or an Offline band). */
        var rollup = DarlingFleetReader.BuildRollup(new[] { card }, Now, Now.AddHours(-1), Now);
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(rollup));
        var json = doc.RootElement.GetProperty("cards")[0];
        Assert.NotEqual("Offline", json.GetProperty("status").GetString());
        Assert.Equal(JsonValueKind.Null, json.GetProperty("connect_down_since").ValueKind);
    }

    [Fact]
    public void List_servers_and_the_offline_note_ignore_a_failed_connect_older_than_the_newest_collection()
    {
        var json = DarlingMcpDataTools.RenderServerList(
            new[]
            {
                new DarlingDataReader.ServerListRow(1, "recovered", null, 16, FiveMinutesAgo, MonitoredEngineKind.SqlServer, null, FiveMinutesAgo.AddDays(-3),
                    ConnectDownSince: StaleFlag, LastConnectError: ConnectError),
            },
            Now,
            DarlingPeerDirectory.Snapshot.Empty);

        using var document = JsonDocument.Parse(json);
        var row = document.RootElement.GetProperty("servers")[0];
        Assert.Equal("Warning", row.GetProperty("status").GetString());
        Assert.Equal(JsonValueKind.Null, row.GetProperty("reason").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("connect_down_since").ValueKind);
        Assert.Equal(JsonValueKind.Null, row.GetProperty("last_connect_error").ValueKind);

        Assert.Equal("Warning", DarlingAdminServersReader.Build(new[]
        {
            new DarlingAdminServersReader.Row(1, "cfg", "host", null, false, "sqlserver", 0, "integrated", true, 0m, new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Unspecified),
                "srv", "srv", 16, null, null, MonitoredEngineKind.SqlServer, FiveMinutesAgo.AddDays(-3), FiveMinutesAgo, null, StaleFlag),
        }, Now).Single().freshness);

        /* The age here is five minutes, so the note says nothing: it must not claim the server is unreachable. */
        Assert.Null(DarlingServerOfflineNote.LeadFor("recovered", FiveMinutesAgo, FiveMinutesAgo.AddDays(-3), Now, StaleFlag, ConnectError));
        Assert.Contains("cannot connect", DarlingServerOfflineNote.LeadFor("down", FiveMinutesAgo, FiveMinutesAgo.AddDays(-3), Now, DownSince, ConnectError), StringComparison.Ordinal);
    }

    [Fact]
    public void The_viewer_row_ignores_a_failed_connect_older_than_the_newest_collection_so_Run_all_stays_on()
    {
        var server = new DarlingServer(1, "sql-01", "sql-01", true, 16, registeredAt: FiveMinutesAgo.AddDays(-3));
        server.ConnectDownSince = StaleFlag;
        server.LastConnectError = ConnectError;
        server.ApplyFreshness(FiveMinutesAgo, Now);

        /* ViewerServerTab.ShowServerConnectionState and the Run all gate read exactly this field. */
        Assert.Null(server.ConnectDownSince);
        Assert.NotEqual(ServerCollectionStatus.Offline, server.CardStatus);
        Assert.DoesNotContain("unreachable", server.DotTooltip, StringComparison.OrdinalIgnoreCase);

        var stillDown = new DarlingServer(2, "sql-02", "sql-02", true, 16, registeredAt: FiveMinutesAgo.AddDays(-3));
        stillDown.ConnectDownSince = DownSince;
        stillDown.ApplyFreshness(FiveMinutesAgo, Now);
        Assert.Equal(DownSince, stillDown.ConnectDownSince);
        Assert.Equal(ServerCollectionStatus.Offline, stillDown.CardStatus);

        var summary = new ServerSummaryItem { LastCollectionTime = FiveMinutesAgo, RegisteredAt = FiveMinutesAgo.AddDays(-3), ConnectDownSince = StaleFlag };
        summary.ApplyFreshness(Now);
        Assert.Null(summary.ConnectDownSince);
        Assert.NotEqual(ServerCollectionStatus.Offline, summary.CardStatus);

        /* The tab header: a recovered server reads by age, and a SQL Server error's own period is not doubled. */
        Assert.DoesNotContain("unreachable", ViewerServerTab.FreshnessLabel(FiveMinutesAgo, FiveMinutesAgo.AddDays(-3), Now, null, StaleFlag, ConnectError), StringComparison.OrdinalIgnoreCase);
        var down = ViewerServerTab.FreshnessLabel(FiveMinutesAgo, FiveMinutesAgo.AddDays(-3), Now, null, DownSince, "Login failed for user 'x'.");
        Assert.Contains("Login failed for user 'x'. Values shown are", down, StringComparison.Ordinal);
        Assert.DoesNotContain("..", down, StringComparison.Ordinal);
    }

    /* #1776 own-store: this fact mints its own scratch database through ScratchPostgres. */
    [Fact]
    public async Task A_new_outage_replaces_a_flag_older_than_the_newest_collection_and_one_outage_keeps_its_first_time()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live connect-state test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var config = new NpgsqlCommand(
                "INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES (78, 'sql-outage', 'sql-outage', TRUE)", connection);
            await config.ExecuteNonQueryAsync(ct);
        }

        static DateTime Unspecified(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

        /* The failed-connect write at a chosen time, without the clearing write a good connect would have made in between. */
        async Task FailAtAsync(DateTime at, string error)
        {
            await using var command = postgres.CreateCommand(DarlingObservability.MarkConnectFailedSql);
            command.Parameters.AddWithValue(78);
            command.Parameters.AddWithValue(Unspecified(at));
            command.Parameters.AddWithValue(error);
            command.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp });
            command.Parameters.AddWithValue("sql-outage");
            command.Parameters.AddWithValue("sql-outage");
            command.Parameters.AddWithValue("sqlserver");
            command.Parameters.AddWithValue(0m);
            await command.ExecuteNonQueryAsync(ct);
        }

        async Task<DateTime?> StoredSinceAsync()
        {
            await using var command = postgres.CreateCommand("SELECT connect_down_since FROM servers WHERE server_id = 78");
            var value = await command.ExecuteScalarAsync(ct);
            return value as DateTime?;
        }

        /* Whole seconds: the store keeps microseconds, a .NET tick is finer. */
        var t0 = DateTime.UtcNow.AddHours(-3);
        t0 = new DateTime(t0.Ticks - (t0.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);
        var t1 = t0.AddMinutes(10);
        var t2 = t0.AddHours(1);
        var t3 = t0.AddHours(2);

        /* One outage: the second failure keeps the first time. */
        await FailAtAsync(t0, "first outage");
        await FailAtAsync(t1, "first outage, retried");
        Assert.Equal(Unspecified(t0), await StoredSinceAsync());

        /* The reconnect worked and collection resumed at t2, but the clearing write was lost: the flag still reads t0. A cycle row after t0 is
           not a collection and does not count. */
        await using (var seed = postgres.CreateCommand(
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1, 78, 'sql-outage', $3, $2, 'SUCCESS'), ($4, 78, 'sql-outage', 'wait_stats', $5, 'SUCCESS')"))
        {
            seed.Parameters.AddWithValue(CollectionIdGenerator.Next());
            seed.Parameters.AddWithValue(Unspecified(t1.AddMinutes(1)));
            seed.Parameters.AddWithValue(CollectionCycleRow.CollectorName);
            seed.Parameters.AddWithValue(CollectionIdGenerator.Next());
            seed.Parameters.AddWithValue(Unspecified(t2));
            await seed.ExecuteNonQueryAsync(ct);
        }

        /* A new outage starts at t3: its first failure replaces the stale flag, and a second failure keeps t3. */
        await FailAtAsync(t3, "second outage");
        Assert.Equal(Unspecified(t3), await StoredSinceAsync());
        await FailAtAsync(t3.AddMinutes(5), "second outage, retried");
        Assert.Equal(Unspecified(t3), await StoredSinceAsync());

        /* The reads now say why: Offline with the unreachable reason, though the newest collection is over an hour old. */
        using var list = JsonDocument.Parse(await DarlingMcpDataTools.ListServers(postgres, ct));
        var server = list.RootElement.GetProperty("servers")[0];
        Assert.Equal("Offline", server.GetProperty("status").GetString());
        Assert.Contains("Server unreachable since", server.GetProperty("reason").GetString(), StringComparison.Ordinal);
    }
}
