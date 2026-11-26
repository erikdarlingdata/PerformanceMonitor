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
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9730: a server (or credential profile) with no saved password under this Windows logon connects with an empty user and
/// the engine answers <c>Login failed for user ''</c>. Resolving its connection string logs one warning that names the cause
/// and the fix, once per server (or profile) and process, and none when a password is saved or the server uses Windows
/// authentication. Collection <c>app-logger-statics</c>: the assertions drain the process-wide log buffer.
/// </summary>
[Collection("app-logger-statics")]
[InMemoryCredentials]
public sealed class MissingSavedPasswordWarningTests
{
    private const string FirstSentence = "No saved password was found for";

    private sealed class Lookup : IProfileLookup
    {
        private readonly Dictionary<string, CredentialProfile> _byId = new();
        public void Add(CredentialProfile p) => _byId[p.Id] = p;
        public CredentialProfile? GetProfile(string id) => _byId.TryGetValue(id, out var p) ? p : null;
    }

    private static ServerConnection NewServer(string authType) => new()
    {
        DisplayName = "warn-" + Guid.NewGuid().ToString("N")[..8],
        ServerName = "localhost",
        AuthenticationType = authType,
        AzureClientId = "client-id"
    };

    private static List<string> WarningsFor(string name) =>
        AppLogger.DrainBufferedLines().Where(l => l.Contains(FirstSentence, StringComparison.Ordinal)
            && l.Contains($"'{name}'", StringComparison.Ordinal)).ToList();

    [Fact]
    public void SqlServerWithNoSavedPassword_ResolvedTwice_WarnsOnce()
    {
        var server = NewServer(AuthenticationTypes.SqlServer);
        var cs = new CredentialService();
        AppLogger.DrainBufferedLines();

        ServerConnection.ResolveConnectionString(server, cs, null);
        ServerConnection.ResolveConnectionString(server, cs, null);

        Assert.Single(WarningsFor(server.DisplayName));
    }

    [Fact]
    public void ServicePrincipalWithNoSavedSecret_ResolvedTwice_WarnsOnce()
    {
        var server = NewServer(AuthenticationTypes.ServicePrincipal);
        var cs = new CredentialService();
        AppLogger.DrainBufferedLines();

        ServerConnection.ResolveConnectionString(server, cs, null);
        ServerConnection.ResolveConnectionString(server, cs, null);

        Assert.Single(WarningsFor(server.DisplayName));
    }

    [Fact]
    public void SqlServerWithASavedPassword_WritesNoWarning()
    {
        var server = NewServer(AuthenticationTypes.SqlServer);
        var cs = new CredentialService();
        Assert.True(cs.SaveCredential(server.Id, "test-user", "not-a-real-secret"));
        AppLogger.DrainBufferedLines();

        ServerConnection.ResolveConnectionString(server, cs, null);

        Assert.Empty(WarningsFor(server.DisplayName));
    }

    [Fact]
    public void WindowsAuthentication_WritesNoWarning()
    {
        var server = NewServer(AuthenticationTypes.Windows);
        var cs = new CredentialService();
        AppLogger.DrainBufferedLines();

        ServerConnection.ResolveConnectionString(server, cs, null);

        Assert.Empty(WarningsFor(server.DisplayName));
    }

    [Fact]
    public void ProfileWithNoSavedSecret_UsedByTwoServers_WarnsOnceForTheProfile()
    {
        var profile = new CredentialProfile
        {
            Name = "prof-" + Guid.NewGuid().ToString("N")[..8],
            AuthType = AuthenticationTypes.SqlServer
        };
        var lookup = new Lookup();
        lookup.Add(profile);
        var first = NewServer(AuthenticationTypes.SqlServer);
        first.CredentialProfileId = profile.Id;
        var second = NewServer(AuthenticationTypes.SqlServer);
        second.CredentialProfileId = profile.Id;
        var cs = new CredentialService();
        AppLogger.DrainBufferedLines();

        ServerConnection.ResolveConnectionString(first, cs, lookup);
        ServerConnection.ResolveConnectionString(second, cs, lookup);

        Assert.Single(WarningsFor(profile.Name));
    }

    [Fact]
    public void ProfileWithASavedSecret_WritesNoWarning()
    {
        var profile = new CredentialProfile
        {
            Name = "prof-" + Guid.NewGuid().ToString("N")[..8],
            AuthType = AuthenticationTypes.SqlServer
        };
        var lookup = new Lookup();
        lookup.Add(profile);
        var server = NewServer(AuthenticationTypes.SqlServer);
        server.CredentialProfileId = profile.Id;
        var cs = new CredentialService();
        Assert.True(cs.SaveCredential(ProfileManager.ProfileCredentialId(profile.Id), "test-user", "not-a-real-secret"));
        AppLogger.DrainBufferedLines();

        ServerConnection.ResolveConnectionString(server, cs, lookup);

        Assert.Empty(WarningsFor(profile.Name));
    }
}
