/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using Edit = PerformanceMonitor.Darling.Service.Mcp.DarlingMcpServerAdminTools;

namespace Darling.Tests;

/* #1776 own-store: each live fact mints its own scratch database through ScratchPostgres and never touches another one. */

/// <summary>
/// The desktop viewer's server edit follows the same password rule as the web and MCP edit (#5240): when a SQL or
/// service-principal server's host or port changes, the stored password is not reused, and the save is refused until a
/// newly entered password comes with it. The rule lives in <see cref="ViewerDataService.UpsertMonitoredServerAsync"/>,
/// so any caller of the upsert gets it, and the dialog asks for the password again before the save and the connection
/// test. The viewer cannot reference the service, so the rule and its sentence are copies; the first facts hold them to
/// the service's. The last facts need a store.
/// </summary>
[Collection("live-postgres")]
public sealed class ServerEditPasswordRuleViewerTests
{
    private const int ServerId = 7501;
    private const string StoredHost = "edit-rule.example.test";
    private const string MovedHost = "edit-rule-moved.example.test";
    private const string StoredBlob = "stored-blob";
    private const string NewBlob = "newly-protected-blob";

    [Fact]
    public void TheViewersSentence_IsTheWebAndMcpEditsSentence()
    {
        Assert.Equal(Edit.EditPasswordNeededText, ViewerDataService.EditPasswordNeededText);
        Assert.Equal(Edit.EditPasswordNeededText, new MonitoredServerPasswordNeededException().Message);
    }

    [Theory]
    [InlineData("edit-rule.example.test", 0, "edit-rule.example.test", 0, false)]
    [InlineData("edit-rule.example.test", 0, "  edit-rule.example.test ", 0, false)]
    [InlineData("edit-rule.example.test", 0, "edit-rule-moved.example.test", 0, true)]
    [InlineData("edit-rule.example.test", 0, "EDIT-RULE.example.test", 0, true)]
    [InlineData("edit-rule.example.test\\inst", 0, "edit-rule.example.test", 0, true)]
    [InlineData("edit-rule.example.test", 0, "edit-rule.example.test", 5433, true)]
    [InlineData("edit-rule.example.test", 5433, "edit-rule.example.test", 5433, false)]
    public void TheViewersMoveRule_IsTheCoresHostAndPortRule(string storedHost, int storedPort, string newHost, int newPort, bool moved)
    {
        Assert.Equal(moved, ViewerDataService.ReachMoved(storedHost, storedPort, newHost, newPort));
    }

    [Fact]
    public void TheDialogDoesNotPrefillTheStoredSecret_AndAsksAgainWhenTheReachMoves()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "AddServerDialog.xaml.cs");
        Assert.DoesNotContain("TryUnprotect(existing.EncryptedPassword)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("PasswordBox.Password = decrypted", source, StringComparison.Ordinal);
        Assert.DoesNotContain("AzureClientSecretBox.Password = decrypted", source, StringComparison.Ordinal);

        /* Both keep-the-stored-blob branches (SQL and service principal) ask first. */
        var guard = "if (ReachMovedFromForm())";
        var first = source.IndexOf(guard, StringComparison.Ordinal);
        Assert.True(first >= 0, "missing: " + guard);
        Assert.True(source.IndexOf(guard, first + 1, StringComparison.Ordinal) > first, "the second keep-blob branch has no guard");
        Assert.Contains("catch (MonitoredServerPasswordNeededException ex)", source, StringComparison.Ordinal);
        Assert.True(source.IndexOf("catch (MonitoredServerPasswordNeededException ex)", StringComparison.Ordinal)
            < source.IndexOf("Error saving server:", StringComparison.Ordinal), "the refusal is caught before the general catch");
    }

    private sealed record Rig(ScratchPostgres Scratch, NpgsqlDataSource Owner) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await Owner.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    private static async Task<Rig> OpenAsync(string auth, string? blob, CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live viewer edit password tests (each mints its own scratch database).");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var connection = new NpgsqlConnection(ownerString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        var owner = NpgsqlDataSource.Create(ownerString);
        await using (var command = owner.CreateCommand(@"INSERT INTO config_monitored_servers
            (server_id, name, host, auth, username, encrypted_password, encrypt_mode, excluded_databases, is_enabled, monthly_cost_usd)
            VALUES ($1, 'edit-rule', $2, $3, 'app-user', $4, 'Mandatory', ARRAY[]::text[], TRUE, 0)"))
        {
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(StoredHost);
            command.Parameters.AddWithValue(auth);
            command.Parameters.AddWithValue(blob is null ? DBNull.Value : blob);
            await command.ExecuteNonQueryAsync(ct);
        }

        return new Rig(scratch, owner);
    }

    private static MonitoredServerRow Row(string auth, string host, string? blob, string encryptMode = "Mandatory", int port = 0) => new()
    {
        ServerId = ServerId,
        Name = "edit-rule",
        Host = host,
        Port = port,
        Auth = auth,
        Username = "app-user",
        EncryptedPassword = blob,
        EncryptMode = encryptMode,
    };

    private static async Task<(string Host, string? Blob, string EncryptMode)> StoredAsync(Rig rig, CancellationToken ct)
    {
        await using var command = rig.Owner.CreateCommand(
            "SELECT host, encrypted_password, encrypt_mode FROM config_monitored_servers WHERE server_id = $1");
        command.Parameters.AddWithValue(ServerId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.GetString(2));
    }

    [Theory]
    [InlineData("sql")]
    [InlineData("serviceprincipal")]
    public async Task AHostChange_ThatKeepsTheStoredBlob_IsRefused_WithTheWebsSentence_AndNothingIsWritten(string auth)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(auth, StoredBlob, ct);
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);

        var refused = await Assert.ThrowsAsync<MonitoredServerPasswordNeededException>(
            () => viewer.UpsertMonitoredServerAsync(Row(auth, MovedHost, StoredBlob), ct));
        Assert.Equal(Edit.EditPasswordNeededText, refused.Message);

        Assert.Equal((StoredHost, StoredBlob, "Mandatory"), await StoredAsync(rig, ct));
    }

    [Fact]
    public async Task AHostChange_WithNoBlobAtAll_AndAPortChange_AreRefusedToo()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync("sql", StoredBlob, ct);
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);

        await Assert.ThrowsAsync<MonitoredServerPasswordNeededException>(
            () => viewer.UpsertMonitoredServerAsync(Row("sql", MovedHost, null), ct));
        await Assert.ThrowsAsync<MonitoredServerPasswordNeededException>(
            () => viewer.UpsertMonitoredServerAsync(Row("sql", StoredHost, StoredBlob, port: 5433), ct));

        Assert.Equal((StoredHost, StoredBlob, "Mandatory"), await StoredAsync(rig, ct));
    }

    [Fact]
    public async Task AHostChange_WithANewlyEnteredPassword_Saves()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync("sql", StoredBlob, ct);
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);

        await viewer.UpsertMonitoredServerAsync(Row("sql", MovedHost, NewBlob), ct);

        Assert.Equal((MovedHost, NewBlob, "Mandatory"), await StoredAsync(rig, ct));
    }

    [Fact]
    public async Task ANonReachChange_KeepsTheStoredPassword()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync("sql", StoredBlob, ct);
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);

        await viewer.UpsertMonitoredServerAsync(Row("sql", StoredHost, StoredBlob, encryptMode: "Strict"), ct);

        Assert.Equal((StoredHost, StoredBlob, "Strict"), await StoredAsync(rig, ct));
    }

    [Fact]
    public async Task AWindowsAuthHostChange_Saves()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync("integrated", null, ct);
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);

        await viewer.UpsertMonitoredServerAsync(Row("integrated", MovedHost, null), ct);

        Assert.Equal((MovedHost, null, "Mandatory"), await StoredAsync(rig, ct));
    }
}
