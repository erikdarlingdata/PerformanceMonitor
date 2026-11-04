using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7590: a server whose connect has failed reads Offline at once, with the reason and the error, instead of showing
/// "collection stale" and green tiles for up to 30 minutes. A server with the same age and no failed connect keeps today's rule.
/// Driven through the fleet reader's own card builder and roll-up, and the JSON get_fleet_overview serves from them.
/// </summary>
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

        /* A server that has never connected has no registry row: the failed-connect write matches nothing and does not invent one. */
        await DarlingObservability.MarkConnectFailedAsync(postgres, 77, "no route", null, ct);
        await using (var count = postgres.CreateCommand("SELECT count(*) FROM servers WHERE server_id = 77"))
        {
            Assert.Equal(0L, await count.ExecuteScalarAsync(ct));
        }

        await UpsertAsync(postgres, ct);
        Assert.Equal((null, null), await FleetReadAsync(postgres, ct));

        await DarlingObservability.MarkConnectFailedAsync(postgres, 77, "first error", null, ct);
        var (firstSince, firstError) = await FleetReadAsync(postgres, ct);
        Assert.NotNull(firstSince);
        Assert.Equal("first error", firstError);

        /* The second failure keeps the outage's first time and takes the newest error. */
        await DarlingObservability.MarkConnectFailedAsync(postgres, 77, "second error", null, ct);
        var (secondSince, secondError) = await FleetReadAsync(postgres, ct);
        Assert.Equal(firstSince, secondSince);
        Assert.Equal("second error", secondError);

        var connection2 = await DarlingMcpDataTools.ReadServerConnectionAsync(postgres, 77, DateTime.UtcNow, null, ct);
        Assert.Equal("waiting_on_connection", JsonDocument.Parse(JsonSerializer.Serialize(connection2)).RootElement.GetProperty("status").GetString());

        await UpsertAsync(postgres, ct);
        Assert.Equal((null, null), await FleetReadAsync(postgres, ct));
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
}
