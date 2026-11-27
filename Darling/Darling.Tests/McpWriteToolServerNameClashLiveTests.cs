/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test seeds its own scratch database, so nothing here shares rows with another test and the class is
   deliberately NOT [Collection("live-postgres")]. */
/// <summary>
/// #9073 (gated, DARLING_TEST_PG): <c>set_quiet_window</c>, <c>write_investigation_note</c> and <c>save_environment_profile</c> given a
/// <c>server_name</c> that is one registration's stored name and a different registration's display name. Each refuses the name on
/// <c>server_name</c> with both registrations listed and writes nothing; a name only one registration answers to writes to that
/// registration.
/// </summary>
public sealed class McpWriteToolServerNameClashLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the server_name clash live tests.";

    /// <summary>The first registration's stored name, and the second registration's display name.</summary>
    private const string Shared = "clash-9073-shared";

    /// <summary>The first registration's display name: only it answers to this.</summary>
    private const string FirstDisplay = "clash-9073-first-display";

    /// <summary>The second registration's stored name: only it answers to this.</summary>
    private const string OtherStored = "clash-9073-other";

    private static readonly int FirstId = ServerIdHelper.GetDeterministicHashCode(Shared);
    private static readonly int OtherId = ServerIdHelper.GetDeterministicHashCode(OtherStored);

    private static async Task WithStoreAsync(Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var storeString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var connection = new NpgsqlConnection(storeString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, FirstId, Shared, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, OtherId, OtherStored, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET display_name = $1 WHERE server_id = $2", FirstDisplay, FirstId);
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET display_name = $1 WHERE server_id = $2", Shared, OtherId);

            /* A profile is saved only for a server that has a definition row. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES ($1, $2, 'host-first')", FirstId, Shared);
            await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES ($1, $2, 'host-other')", OtherId, OtherStored);
        }

        await using var postgres = NpgsqlDataSource.Create(storeString);

        /* The note and the profile are written as a person, which the tools read from the caller's token identity. */
        ReadCaller.SetAmbientMcpToken("named token ops");
        await body(postgres, ct);
    }

    private static async Task<string> ScalarAsync(NpgsqlDataSource postgres, string sql, CancellationToken ct)
    {
        await using var command = postgres.CreateCommand(sql);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty;
    }

    /// <summary>The answer is the <c>invalid</c> envelope on <c>server_name</c>, and its message lists both registrations by stored name.</summary>
    private static void AssertRefusedWithBothListed(string answer)
    {
        var json = JsonNode.Parse(answer)!;
        Assert.Equal("invalid", (string?)json["status"]);
        Assert.Equal("server_name", (string?)json["hints"]?["parameter"]);
        var message = (string?)json["message"] ?? string.Empty;
        Assert.Contains($"{Shared} ({FirstDisplay})", message, StringComparison.Ordinal);
        Assert.Contains($"{OtherStored} ({Shared})", message, StringComparison.Ordinal);
    }

    private static string Status(string answer) => (string?)JsonNode.Parse(answer)!["status"] ?? string.Empty;

    private static string InOneHour() =>
        DateTime.UtcNow.AddHours(1).ToString("yyyy-MM-ddTHH:mm:ssZ", System.Globalization.CultureInfo.InvariantCulture);

    [Fact]
    public Task SetQuietWindow_RefusesANameTwoRegistrationsAnswerTo_WritesNothing_AndAUniqueNameQuietsThatRegistration() =>
        WithStoreAsync(async (postgres, ct) =>
        {
            const string count = "SELECT count(*) FROM config_mute_rules";
            const string servers = "SELECT string_agg(server_id::text, ',' ORDER BY server_id) FROM config_mute_rules";

            AssertRefusedWithBothListed(await DarlingMcpQuietWindowTools.SetQuietWindow(postgres, InOneHour(), Shared, cancellationToken: ct));
            Assert.Equal("0", await ScalarAsync(postgres, count, ct));

            Assert.Equal("created", Status(await DarlingMcpQuietWindowTools.SetQuietWindow(postgres, InOneHour(), OtherStored, cancellationToken: ct)));
            Assert.Equal(OtherId.ToString(System.Globalization.CultureInfo.InvariantCulture), await ScalarAsync(postgres, servers, ct));

            Assert.Equal("created", Status(await DarlingMcpQuietWindowTools.SetQuietWindow(postgres, InOneHour(), FirstDisplay, cancellationToken: ct)));
            Assert.Equal("2", await ScalarAsync(postgres, count, ct));
            Assert.Equal("1", await ScalarAsync(postgres, count + " WHERE server_id = " + FirstId.ToString(System.Globalization.CultureInfo.InvariantCulture), ct));
        });

    [Fact]
    public Task WriteInvestigationNote_RefusesANameTwoRegistrationsAnswerTo_WritesNothing_AndAUniqueNameWritesToThatRegistration() =>
        WithStoreAsync(async (postgres, ct) =>
        {
            const string count = "SELECT count(*) FROM config.investigation_notes";
            const string servers = "SELECT string_agg(server_id::text, ',' ORDER BY note_id) FROM config.investigation_notes";

            AssertRefusedWithBothListed(await DarlingMcpInvestigationAdminTools.WriteInvestigationNote(postgres, "Index rebuilds finish by 03:00.", Shared, cancellationToken: ct));
            Assert.Equal("0", await ScalarAsync(postgres, count, ct));

            Assert.Equal("written", Status(await DarlingMcpInvestigationAdminTools.WriteInvestigationNote(postgres, "Index rebuilds finish by 03:00.", OtherStored, cancellationToken: ct)));
            Assert.Equal("written", Status(await DarlingMcpInvestigationAdminTools.WriteInvestigationNote(postgres, "Nightly load starts at 02:00.", FirstDisplay, cancellationToken: ct)));
            Assert.Equal(
                OtherId.ToString(System.Globalization.CultureInfo.InvariantCulture) + "," + FirstId.ToString(System.Globalization.CultureInfo.InvariantCulture),
                await ScalarAsync(postgres, servers, ct));
        });

    [Fact]
    public Task SaveEnvironmentProfile_RefusesANameTwoRegistrationsAnswerTo_WritesNothing_AndAUniqueNameSavesThatRegistrationsProfile() =>
        WithStoreAsync(async (postgres, ct) =>
        {
            const string count = "SELECT count(*) FROM config.agent_environment_profiles";

            AssertRefusedWithBothListed(await DarlingMcpInvestigationFeedbackTools.SaveEnvironmentProfile(postgres, "Reporting server.", Shared, cancellationToken: ct));
            Assert.Equal("0", await ScalarAsync(postgres, count, ct));

            Assert.Equal("saved", Status(await DarlingMcpInvestigationFeedbackTools.SaveEnvironmentProfile(postgres, "Reporting server.", OtherStored, cancellationToken: ct)));
            Assert.Equal("1", await ScalarAsync(postgres, count, ct));
            Assert.Equal("1", await ScalarAsync(postgres, count + " WHERE scope_server_id = " + OtherId.ToString(System.Globalization.CultureInfo.InvariantCulture), ct));

            Assert.Equal("saved", Status(await DarlingMcpInvestigationFeedbackTools.SaveEnvironmentProfile(postgres, "Order entry server.", FirstDisplay, cancellationToken: ct)));
            Assert.Equal("2", await ScalarAsync(postgres, count, ct));
            Assert.Equal("1", await ScalarAsync(postgres, count + " WHERE scope_server_id = " + FirstId.ToString(System.Globalization.CultureInfo.InvariantCulture), ct));
        });
}
