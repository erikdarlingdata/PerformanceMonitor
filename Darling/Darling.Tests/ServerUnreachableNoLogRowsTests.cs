using System;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7590: a registered server whose connect is failing and whose collection log holds no rows in the window (the outage outlasted it, or
/// retention dropped them) still says so. <c>get_collection_health</c> keeps its <c>unavailable</c> word and carries the
/// <c>server_connection</c> block and the waiting sentence, and the offline note an empty read opens with names the unreachable server.
/// </summary>
public class ServerUnreachableNoLogRowsTests
{
    private const string ConnectError = "A network-related or instance-specific error occurred while establishing a connection to SQL Server.";

    private static readonly DateTime Now = new(2026, 11, 4, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void The_offline_note_names_an_unreachable_server_even_with_a_recent_collection()
    {
        var recent = Now.AddMinutes(-5);
        var registered = Now.AddDays(-10);
        var since = Now.AddMinutes(-4);

        var lead = DarlingServerOfflineNote.LeadFor("sql-01", recent, registered, Now, since, ConnectError);
        Assert.NotNull(lead);
        Assert.Contains("sql-01 is offline", lead, StringComparison.Ordinal);
        Assert.Contains("Server unreachable since 2026-11-04 11:56 UTC: " + ConnectError, lead, StringComparison.Ordinal);
        Assert.DoesNotContain("no successful collection", lead, StringComparison.Ordinal);

        /* The same age with no failed connect keeps the age rule: five minutes is not offline. */
        Assert.Null(DarlingServerOfflineNote.LeadFor("sql-01", recent, registered, Now));
    }

    [Fact]
    public async Task A_failed_connect_with_no_collection_log_rows_still_carries_the_connection_block_and_the_offline_lead()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live connect-state test.");
        var ct = TestContext.Current.CancellationToken;
        /* #1776 own-store: this fact mints its own scratch database through ScratchPostgres. */
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var config = new NpgsqlCommand(
                "INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES (78, 'sql-quiet', 'sql-quiet', TRUE)", connection);
            await config.ExecuteNonQueryAsync(ct);
        }

        await using (var command = postgres.CreateCommand(DarlingObservability.UpsertServerSql))
        {
            command.Parameters.AddWithValue(78);
            command.Parameters.AddWithValue("sql-quiet");
            command.Parameters.AddWithValue("sql-quiet");
            command.Parameters.AddWithValue(3);
            command.Parameters.AddWithValue(16);
            command.Parameters.AddWithValue("sqlserver");
            command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(0m);
            command.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlDbType.Integer });
            command.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
            command.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlDbType.Boolean });
            command.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlDbType.Boolean });
            await command.ExecuteNonQueryAsync(ct);
        }

        /* Reachable and no log rows: the old answer, no block. */
        using (var up = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealthUncut(postgres, "sql-quiet", ct)))
        {
            Assert.Equal("unavailable", up.RootElement.GetProperty("status").GetString());
            Assert.False(up.RootElement.TryGetProperty("server_connection", out _));
        }

        await DarlingObservability.MarkConnectFailedAsync(postgres, 78, ConnectError, null, ct, DateTime.UtcNow.AddMinutes(2));

        using var down = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealthUncut(postgres, "sql-quiet", ct));
        var root = down.RootElement;
        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        Assert.Contains("Collection is waiting on the connection", root.GetProperty("message").GetString(), StringComparison.Ordinal);
        var block = root.GetProperty("server_connection");
        Assert.Equal("waiting_on_connection", block.GetProperty("status").GetString());
        Assert.Equal(ConnectError, block.GetProperty("last_connect_error").GetString());
        Assert.Equal(JsonValueKind.String, block.GetProperty("since").ValueKind);
        Assert.Equal(JsonValueKind.String, block.GetProperty("next_connect_attempt").ValueKind);
        Assert.DoesNotContain("collector rows are the last runs", block.GetProperty("message").GetString(), StringComparison.Ordinal);

        /* An empty read for this server opens with the unreachable note, not the age text. */
        var empty = await DarlingServerOfflineNote.EmptyAsync(
            postgres, 78, "sql-quiet", DateTime.UtcNow, McpHelpers.Status("empty", "nothing"), ct);
        using var note = JsonDocument.Parse(empty);
        Assert.Equal("unavailable", note.RootElement.GetProperty("status").GetString());
        Assert.Contains("Server unreachable since", note.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
    }
}
