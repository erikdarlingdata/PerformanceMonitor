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

    /// <summary>The rule the viewer's edit applies (<see cref="ServerConnectionRule.ConnectionSettingsDiffer"/>, with the
    /// form's host trimmed of spaces as the dialog trims it) to a form that changes only the host and port.</summary>
    private static bool ViewerMoves(string storedHost, int storedPort, string newHost, int newPort) =>
        ServerConnectionRule.ConnectionSettingsDiffer(
            ServerConnectionSettings.WithDefaults(newHost.Trim(' '), newPort, null, null, null, null, null, null, null, null),
            ServerConnectionSettings.WithDefaults(storedHost, storedPort, null, null, null, null, null, null, null, null));

    [Theory]
    [InlineData("edit-rule.example.test", 0, "edit-rule.example.test", 0, false)]
    [InlineData("edit-rule.example.test", 0, "  edit-rule.example.test ", 0, false)]
    [InlineData("edit-rule.example.test", 0, "edit-rule-moved.example.test", 0, true)]
    [InlineData("edit-rule.example.test", 0, "EDIT-RULE.example.test", 0, true)]
    [InlineData("edit-rule.example.test\\inst", 0, "edit-rule.example.test", 0, true)]
    [InlineData("edit-rule.example.test", 0, "edit-rule.example.test", 5433, true)]
    [InlineData("edit-rule.example.test", 5433, "edit-rule.example.test", 5433, false)]
    /* Only the incoming host is trimmed; the stored text is compared exactly, as the core's SameAddress does. */
    [InlineData("edit-rule.example.test ", 0, "edit-rule.example.test", 0, true)]
    [InlineData("edit-rule.example.test ", 0, "edit-rule.example.test ", 0, true)]
    public void TheViewersMoveRule_IsTheCoresHostAndPortRule(string storedHost, int storedPort, string newHost, int newPort, bool moved)
    {
        Assert.Equal(moved, ViewerMoves(storedHost, storedPort, newHost, newPort));
    }

    [Theory]
    [InlineData("edit-rule.example.test", 0, "edit-rule.example.test", 0)]
    [InlineData("edit-rule.example.test", 0, "  edit-rule.example.test ", 0)]
    [InlineData("edit-rule.example.test", 0, "EDIT-RULE.example.test", 0)]
    [InlineData("edit-rule.example.test ", 0, "edit-rule.example.test", 0)]
    [InlineData("edit-rule.example.test ", 0, "edit-rule.example.test ", 0)]
    [InlineData("edit-rule.example.test", 0, "edit-rule.example.test", 5433)]
    public void TheViewersMoveRule_AgreesWithTheCoresSameAddress_OnTheTrimmedIncomingHost(string storedHost, int storedPort, string newHost, int newPort)
    {
        /* The core trims a request's host when it reads it, then calls SameAddress with the stored text as it is. */
        Assert.Equal(
            !Edit.SameAddress(newHost.Trim(' '), newPort, storedHost, storedPort),
            ViewerMoves(storedHost, storedPort, newHost, newPort));
    }

    [Theory]
    [InlineData("serviceprincipal", "sql")]
    [InlineData("sql", "serviceprincipal")]
    [InlineData("integrated", "sql")]
    public void TheViewersSwitchSentences_AreTheCoresSentences(string storedAuth, string newAuth)
    {
        var row = new Edit.ServerEditRow(
            ServerId: ServerId, Name: "edit-rule", Host: StoredHost, Port: 0, Database: null, ReadOnlyIntent: false, Engine: "sqlserver",
            Auth: storedAuth, Username: "app-user", EncryptMode: "Mandatory", TrustServerCertificate: false,
            MultiSubnetFailover: false, MonthlyCostUsd: 0m, ModifiedAt: new DateTime(2026, 10, 5, 12, 0, 0, DateTimeKind.Unspecified));
        var (changes, parseError) = Edit.ParseEditChanges("{\"auth\":\"" + (newAuth == "sql" ? "SQL" : "ServicePrincipal") + "\",\"username\":\"app-user\"}");
        Assert.Null(parseError);

        var (plan, error) = Edit.PlanEdit(row, changes!, isWindows: true);

        Assert.Null(plan);
        Assert.Equal(
            newAuth == "sql" ? ViewerDataService.EditSwitchToSqlNeedsPasswordText : ViewerDataService.EditSwitchToServicePrincipalNeedsSecretText,
            error);
    }

    [Fact]
    public void TheDialogDoesNotPrefillTheStoredSecret_AndAsksAgainWhenTheReachMoves()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "AddServerDialog.xaml.cs");
        Assert.DoesNotContain("TryUnprotect(existing.EncryptedPassword)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("PasswordBox.Password = decrypted", source, StringComparison.Ordinal);
        Assert.DoesNotContain("AzureClientSecretBox.Password = decrypted", source, StringComparison.Ordinal);

        /* Both keep-the-stored-blob branches (SQL and service principal) ask first. */
        var guard = "if (ReachMovedFromForm(";
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

    private static async Task<Rig> OpenAsync(string auth, string? blob, CancellationToken ct, string? remediationBlob = null)
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

        if (remediationBlob is not null)
        {
            await using var remediation = owner.CreateCommand(
                "UPDATE config_monitored_servers SET remediation_username = 'fix-login', remediation_encrypted_password = $2 WHERE server_id = $1");
            remediation.Parameters.AddWithValue(ServerId);
            remediation.Parameters.AddWithValue(remediationBlob);
            await remediation.ExecuteNonQueryAsync(ct);
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

        var renamed = Row("sql", StoredHost, StoredBlob);
        renamed.Name = "edit-rule-renamed";
        renamed.MonthlyCostUsd = 5m;
        await viewer.UpsertMonitoredServerAsync(renamed, ct);

        Assert.Equal((StoredHost, StoredBlob, "Mandatory"), await StoredAsync(rig, ct));
        await using var read = rig.Owner.CreateCommand("SELECT name FROM config_monitored_servers WHERE server_id = $1");
        read.Parameters.AddWithValue(ServerId);
        Assert.Equal("edit-rule-renamed", (string?)await read.ExecuteScalarAsync(ct));
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

    [Theory]
    [InlineData("sql", "serviceprincipal", "Switching to ServicePrincipal authentication needs the client secret as password.")]
    [InlineData("serviceprincipal", "sql", "Switching to SQL authentication needs the password.")]
    [InlineData("integrated", "sql", "Switching to SQL authentication needs the password.")]
    public async Task AnAuthSwitch_ThatKeepsTheStoredBlob_OrCarriesNone_IsRefused_WithTheCoresSentence_AndNothingIsWritten(
        string storedAuth, string newAuth, string sentence)
    {
        var ct = TestContext.Current.CancellationToken;
        var storedBlob = storedAuth == "integrated" ? null : StoredBlob;
        await using var rig = await OpenAsync(storedAuth, storedBlob, ct);
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);

        if (storedBlob is not null)
        {
            var kept = await Assert.ThrowsAsync<MonitoredServerPasswordNeededException>(
                () => viewer.UpsertMonitoredServerAsync(Row(newAuth, StoredHost, StoredBlob), ct));
            Assert.Equal(sentence, kept.Message);
        }

        var none = await Assert.ThrowsAsync<MonitoredServerPasswordNeededException>(
            () => viewer.UpsertMonitoredServerAsync(Row(newAuth, StoredHost, null), ct));
        Assert.Equal(sentence, none.Message);

        Assert.Equal((StoredHost, storedBlob, "Mandatory"), await StoredAsync(rig, ct));
    }

    [Theory]
    [InlineData("sql", "serviceprincipal")]
    [InlineData("serviceprincipal", "sql")]
    [InlineData("integrated", "sql")]
    public async Task AnAuthSwitch_WithANewlyEnteredSecret_Saves(string storedAuth, string newAuth)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync(storedAuth, storedAuth == "integrated" ? null : StoredBlob, ct);
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);

        await viewer.UpsertMonitoredServerAsync(Row(newAuth, StoredHost, NewBlob), ct);

        Assert.Equal((StoredHost, NewBlob, "Mandatory"), await StoredAsync(rig, ct));
    }

    [Fact]
    public async Task ASwitchOutOfASecretMode_NeedsNoSecret_AndSaves()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync("sql", StoredBlob, ct);
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);

        await viewer.UpsertMonitoredServerAsync(Row("integrated", StoredHost, null), ct);

        Assert.Equal((StoredHost, null, "Mandatory"), await StoredAsync(rig, ct));
    }

    public static IEnumerable<object[]> ConnectionFields() =>
        new[] { "encrypt mode", "trust certificate", "multi-subnet", "username", "database", "read-only", "engine", "port", "host" }
            .Select(f => new object[] { f });

    private static MonitoredServerRow Changed(string field, string auth, string? blob)
    {
        var row = Row(auth, StoredHost, blob);
        switch (field)
        {
            case "encrypt mode": row.EncryptMode = "Optional"; break;
            case "trust certificate": row.TrustServerCertificate = true; break;
            case "multi-subnet": row.MultiSubnetFailover = true; break;
            case "username": row.Username = "other-user"; break;
            case "database": row.Database = "tempdb"; break;
            case "read-only": row.ReadOnlyIntent = true; break;
            case "engine": row.Engine = "postgres"; break;
            case "port": row.Port = 5433; break;
            case "host": row.Host = MovedHost; break;
            default: throw new ArgumentException(field);
        }

        return row;
    }

    /// <summary>The sentence the web and MCP edit give for a row with a remediation login. Held here as text because the
    /// two apps carry it as separate constants.</summary>
    private const string RemediationSentence =
        "This server has a remediation login stored. Change how it is reached on the service host, in the configuration file or with --add-server.";

    [Fact]
    public void TheViewersRemediationSentence_IsTheSharedSentence()
    {
        Assert.Equal(RemediationSentence, ViewerDataService.RemediationKeptText);
        Assert.Equal("PW003", ViewerDataService.StoreRemediationKeptSqlState);
    }

    [Theory]
    [MemberData(nameof(ConnectionFields))]
    public async Task AnyConnectionChange_ThatKeepsTheStoredBlob_IsRefusedBeforeTheWrite(string field)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync("sql", StoredBlob, ct);
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);

        var refused = await Assert.ThrowsAsync<MonitoredServerPasswordNeededException>(
            () => viewer.UpsertMonitoredServerAsync(Changed(field, "sql", StoredBlob), ct));
        Assert.Equal(Edit.EditPasswordNeededText, refused.Message);
        var blank = await Assert.ThrowsAsync<MonitoredServerPasswordNeededException>(
            () => viewer.UpsertMonitoredServerAsync(Changed(field, "sql", null), ct));
        Assert.Equal(Edit.EditPasswordNeededText, blank.Message);

        Assert.Equal((StoredHost, StoredBlob, "Mandatory"), await StoredAsync(rig, ct));
    }

    [Theory]
    [MemberData(nameof(ConnectionFields))]
    public async Task AnyConnectionChange_WithANewlyEnteredPassword_Saves(string field)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync("sql", StoredBlob, ct);
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);

        await viewer.UpsertMonitoredServerAsync(Changed(field, "sql", NewBlob), ct);

        var stored = await StoredAsync(rig, ct);
        Assert.Equal(NewBlob, stored.Blob);
    }

    [Theory]
    [InlineData("sql", "stored-blob")]
    [InlineData("integrated", null)]
    public async Task AConnectionChange_OnARowWithARemediationSecret_IsRefusedBeforeTheWrite_WithTheSharedSentence(string auth, string? blob)
    {
        var ct = TestContext.Current.CancellationToken;
        foreach (var field in ConnectionFields().Select(f => (string)f[0]))
        {
            await using var rig = await OpenAsync(auth, blob, ct, remediationBlob: "remediation-blob");
            await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);

            /* A password typed with the change does not help: the refusal does not depend on it. */
            var typed = await Assert.ThrowsAsync<MonitoredServerPasswordNeededException>(
                () => viewer.UpsertMonitoredServerAsync(Changed(field, auth, auth == "sql" ? NewBlob : null), ct));
            Assert.Equal(RemediationSentence, typed.Message);

            Assert.Equal((StoredHost, blob, "Mandatory"), await StoredAsync(rig, ct));
        }
    }

    [Fact]
    public async Task ANonConnectionChange_OnARowWithARemediationSecret_Saves()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = await OpenAsync("sql", StoredBlob, ct, remediationBlob: "remediation-blob");
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);

        var renamed = Row("sql", StoredHost, StoredBlob);
        renamed.MonthlyCostUsd = 7m;
        await viewer.UpsertMonitoredServerAsync(renamed, ct);

        Assert.Equal((StoredHost, StoredBlob, "Mandatory"), await StoredAsync(rig, ct));
    }
}
