/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The Lite "Add Multiple Servers" pure mapping + gate helpers (WPF-free statics on
/// <see cref="AddMultipleServersDialog"/>): the row → <see cref="ServerConnection"/> projection that feeds
/// both Test All and Add, the MFA rejection belt, the unique-profile-name suffixer, and the ratified
/// OrdinalIgnoreCase dedupe gate over the shared <see cref="ServerIdHelper"/> storage name.
/// </summary>
public sealed class BulkServerOnboardingTests
{
    private static BulkServerParseLine Line(string server, string? display = null, string? database = null) =>
        new(1, server, server, display, database);

    [Fact]
    public void BuildServerConnection_DisplayName_FallsBackToServerName()
    {
        var (server, error) = AddMultipleServersDialog.BuildServerConnection(
            Line("SQL2022"),
            new BulkSharedSettings { AuthType = AuthenticationTypes.Windows });

        Assert.Null(error);
        Assert.NotNull(server);
        Assert.Equal("SQL2022", server!.DisplayName);

        var (withDisplay, _) = AddMultipleServersDialog.BuildServerConnection(
            Line("SQL2022", "Production"),
            new BulkSharedSettings { AuthType = AuthenticationTypes.Windows });
        Assert.Equal("Production", withDisplay!.DisplayName);
    }

    [Fact]
    public void BuildServerConnection_Database_IsNullWhenBlank_TrimmedWhenSet()
    {
        var (blank, _) = AddMultipleServersDialog.BuildServerConnection(
            Line("SQL2022", null, null),
            new BulkSharedSettings { AuthType = AuthenticationTypes.Windows });
        Assert.Null(blank!.DatabaseName);

        var (set, _) = AddMultipleServersDialog.BuildServerConnection(
            Line("azure.database.windows.net", null, "  AdventureWorks  "),
            new BulkSharedSettings { AuthType = AuthenticationTypes.Windows });
        Assert.Equal("AdventureWorks", set!.DatabaseName);
    }

    [Fact]
    public void BuildServerConnection_WindowsAuth_HasNoProfileAndNoSecret()
    {
        var (server, _) = AddMultipleServersDialog.BuildServerConnection(
            Line("SQL2022"),
            new BulkSharedSettings { AuthType = AuthenticationTypes.Windows });

        Assert.Equal(AuthenticationTypes.Windows, server!.AuthenticationType);
        Assert.Null(server.CredentialProfileId);
        Assert.Null(server.ManagedIdentityClientId);
        Assert.True(server.IsEnabled);
    }

    [Fact]
    public void BuildServerConnection_SqlProfileBacked_CarriesProfileIdAndAuth()
    {
        var (server, _) = AddMultipleServersDialog.BuildServerConnection(
            Line("SQL2022"),
            new BulkSharedSettings { AuthType = AuthenticationTypes.SqlServer, ProfileId = "profile-123" });

        Assert.Equal(AuthenticationTypes.SqlServer, server!.AuthenticationType);
        Assert.Equal("profile-123", server.CredentialProfileId);
        // A profile-backed row keeps no inline Azure identity (M-3 hygiene).
        Assert.Null(server.AzureClientId);
    }

    [Fact]
    public void BuildServerConnection_InlineManagedIdentity_CarriesClientId_NoProfile()
    {
        var (server, _) = AddMultipleServersDialog.BuildServerConnection(
            Line("azure.database.windows.net"),
            new BulkSharedSettings { AuthType = AuthenticationTypes.ManagedIdentity, ManagedIdentityClientId = "mi-abc" });

        Assert.Equal(AuthenticationTypes.ManagedIdentity, server!.AuthenticationType);
        Assert.Equal("mi-abc", server.ManagedIdentityClientId);
        Assert.Null(server.CredentialProfileId);
    }

    [Fact]
    public void BuildServerConnection_EncryptAndTrust_PassThrough()
    {
        var (server, _) = AddMultipleServersDialog.BuildServerConnection(
            Line("SQL2022"),
            new BulkSharedSettings { AuthType = AuthenticationTypes.Windows, EncryptMode = "Strict", TrustServerCertificate = true });

        Assert.Equal("Strict", server!.EncryptMode);
        Assert.True(server.TrustServerCertificate);
    }

    [Theory]
    [InlineData(AuthenticationTypes.EntraMFA, "Entra MFA")]
    [InlineData(AuthenticationTypes.EntraDeviceCode, "Device Code")]
    public void BuildServerConnection_ResolvedInteractiveMode_IsRejected_TheBelt(string authType, string named)
    {
        // No supported bulk path mints either one (neither radio exists; the profile editor creates neither),
        // but the mapping helper still rejects a resolved interactive mode at the single choke point covering
        // Test All + Add. Parameterised because the belt now asks RequiresInteractiveSignIn rather than
        // testing EntraMFA, so both modes must reach the same refusal through the same line.
        var (server, error) = AddMultipleServersDialog.BuildServerConnection(
            Line("azure.database.windows.net"),
            new BulkSharedSettings { AuthType = authType });

        Assert.Null(server);

        // The refusal NAMES the mode, so a row's status says which of the shared settings was the problem.
        Assert.Contains(named, error, System.StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(AuthenticationTypes.Windows)]
    [InlineData(AuthenticationTypes.SqlServer)]
    [InlineData(AuthenticationTypes.ServicePrincipal)]
    [InlineData(AuthenticationTypes.ManagedIdentity)]
    [InlineData(AuthenticationTypes.EntraDefaultCredential)]
    public void BuildServerConnection_NonInteractiveModes_StillPassTheBelt(string authType)
    {
        /* The other half of the pin above, and the reason it is not vacuous: a belt that rejected
           everything would satisfy every assertion there. EntraDefaultCredential is the case that
           discriminates - it is an Azure mode, it is NOT interactive, and bulk add must keep
           accepting it. */
        var (server, error) = AddMultipleServersDialog.BuildServerConnection(
            Line("azure.database.windows.net"),
            new BulkSharedSettings { AuthType = authType });

        Assert.Null(error);
        Assert.NotNull(server);
        Assert.Equal(authType, server!.AuthenticationType);
    }

    [Fact]
    public void MakeUniqueProfileName_SuffixesOnCollision_StartingAtTwo()
    {
        Assert.Equal("SQL — sa", AddMultipleServersDialog.MakeUniqueProfileName("SQL — sa", new List<string>()));

        Assert.Equal("SQL — sa (2)",
            AddMultipleServersDialog.MakeUniqueProfileName("SQL — sa", new[] { "SQL — sa" }));

        // Case-insensitive collision detection; the next free suffix.
        Assert.Equal("SQL — sa (3)",
            AddMultipleServersDialog.MakeUniqueProfileName("SQL — sa", new[] { "sql — sa", "SQL — SA (2)" }));
    }

    [Fact]
    public void DedupeGate_CaseVariant_IsSkipped_EvenThoughStoredHashesDiffer()
    {
        // The raison d'être: hostnames are case-insensitive in reality, so an existing SQL01 must block a
        // bulk 'sql01' even though ServerIdHelper's stored hash differs by case. Pins against a future
        // "simplify back to the raw hash" regression.
        var existing = new[] { new ServerConnection { ServerName = "SQL01" } };
        var storedDiffer =
            RemoteCollectorServiceIdOf("SQL01") != RemoteCollectorServiceIdOf("sql01");
        Assert.True(storedDiffer);

        var seen = AddMultipleServersDialog.SeedGate(existing);
        var candidate = new ServerConnection { ServerName = "sql01" };
        Assert.False(seen.Add(AddMultipleServersDialog.GateKey(candidate)));
    }

    [Fact]
    public void DedupeGate_ReadOnlyIntent_SeedDoesNotBlockReadWrite_ButReadWriteDoes()
    {
        // An existing read-only SQL01:RO does NOT block a bulk read-write SQL01 (bulk rows are read-write)…
        var seenRo = AddMultipleServersDialog.SeedGate(new[] { new ServerConnection { ServerName = "SQL01", ReadOnlyIntent = true } });
        Assert.True(seenRo.Add(AddMultipleServersDialog.GateKey(new ServerConnection { ServerName = "SQL01" })));

        // …but an existing read-write SQL01 DOES.
        var seenRw = AddMultipleServersDialog.SeedGate(new[] { new ServerConnection { ServerName = "SQL01" } });
        Assert.False(seenRw.Add(AddMultipleServersDialog.GateKey(new ServerConnection { ServerName = "SQL01" })));
    }

    [Fact]
    public void DedupeGate_DifferentDatabases_StayDistinct()
    {
        var seen = AddMultipleServersDialog.SeedGate(new[] { new ServerConnection { ServerName = "host", DatabaseName = "db1" } });
        Assert.True(seen.Add(AddMultipleServersDialog.GateKey(new ServerConnection { ServerName = "host", DatabaseName = "db2" })));
    }

    private static int RemoteCollectorServiceIdOf(string serverName) =>
        ServerIdHelper.GetDeterministicHashCode(ServerIdHelper.BuildStorageName(serverName, null, false));
}
