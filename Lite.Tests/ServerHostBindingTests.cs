/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A server's saved password, and the user's own sign-in, are used only for the host this user saved or confirmed for that
/// server. servers.json is one shared list; each test gets its own temp folder for it and its own per-user record, and the one
/// test that saves a password does it under a key prefix made of a fresh GUID, never under Lite's own prefix.
/// </summary>
[Collection("credential-manager")]
public sealed class ServerHostBindingTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "LiteHostBinding_" + Guid.NewGuid().ToString("N"));
    private readonly string _sharedDir;
    private readonly string _bindingsPath;

    public ServerHostBindingTests()
    {
        _sharedDir = Path.Combine(_root, "shared");
        Directory.CreateDirectory(_sharedDir);
        _bindingsPath = Path.Combine(_root, "user", ServerHostBindings.FileName);
    }

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }

    private void WriteSharedServers(params ServerConnection[] servers) =>
        File.WriteAllText(Path.Combine(_sharedDir, "servers.json"), JsonSerializer.Serialize(new { Servers = servers }));

    private ServerManager NewManager() => new(_sharedDir, hostBindingsPath: _bindingsPath);

    private static ServerConnection Server(string id, string host, string auth, string? replica = null) => new()
    {
        Id = id,
        ServerName = host,
        DisplayName = "server " + id,
        AuthenticationType = auth,
        QueryStoreReplica = replica
    };

    [Fact]
    public void Upgrade_WithNoRecord_BindsEveryServerToItsCurrentHostOnFirstRun_InTheUserFolderNotTheSharedOne()
    {
        WriteSharedServers(
            Server("a", "sql01", AuthenticationTypes.SqlServer),
            Server("b", "sql02,1433", AuthenticationTypes.Windows));
        Assert.False(File.Exists(_bindingsPath));

        var manager = NewManager();

        Assert.True(File.Exists(_bindingsPath));
        Assert.False(File.Exists(Path.Combine(_sharedDir, ServerHostBindings.FileName)));
        Assert.Null(manager.HostChange(manager.GetServerById("a")!));
        Assert.Null(manager.HostChange(manager.GetServerById("b")!));
        Assert.Contains("sql02", manager.CredentialResolver.GetConnectionString(manager.GetServerById("b")!), StringComparison.Ordinal);

        /* From then on it is enforced: the next run, with the record in place, does not bind a changed host again. */
        WriteSharedServers(
            Server("a", "elsewhere", AuthenticationTypes.SqlServer),
            Server("b", "sql02,1433", AuthenticationTypes.Windows));
        var next = NewManager();

        var change = next.HostChange(next.GetServerById("a")!);
        Assert.NotNull(change);
        Assert.Equal("sql01", change.ConfirmedHost);
        Assert.Null(next.HostChange(next.GetServerById("b")!));
    }

    [Fact]
    public void SameHost_AsWritten_IsConfirmed_TrimmedAndCaseInsensitive()
    {
        WriteSharedServers(Server("a", "SQL01\\Prod,1433", AuthenticationTypes.Windows));
        NewManager();

        WriteSharedServers(Server("a", "  sql01\\PROD,1433 ", AuthenticationTypes.Windows));
        var manager = NewManager();

        Assert.Null(manager.HostChange(manager.GetServerById("a")!));
    }

    [Fact]
    public void ChangedPort_OrInstance_IsAChangedHost()
    {
        WriteSharedServers(Server("a", "sql01,1433", AuthenticationTypes.Windows), Server("b", "sql01\\one", AuthenticationTypes.Windows));
        NewManager();

        WriteSharedServers(Server("a", "sql01,1444", AuthenticationTypes.Windows), Server("b", "sql01\\two", AuthenticationTypes.Windows));
        var manager = NewManager();

        Assert.NotNull(manager.HostChange(manager.GetServerById("a")!));
        Assert.NotNull(manager.HostChange(manager.GetServerById("b")!));
    }

    [Fact]
    public async Task SqlLogin_ChangedHost_IsRefusedAndFlagged_AndAsksForThePasswordAgain()
    {
        WriteSharedServers(Server("a", "sql01", AuthenticationTypes.SqlServer));
        NewManager();

        WriteSharedServers(Server("a", "sql99.example", AuthenticationTypes.SqlServer));
        var manager = NewManager();
        var server = manager.GetServerById("a")!;
        var resolver = manager.CredentialResolver;

        var refused = Assert.Throws<ServerHostNotConfirmedException>(() => resolver.GetConnectionString(server));
        Assert.Equal("The host name for this server changed to sql99.example. Enter the password again to connect to it.", refused.Message);
        Assert.Throws<ServerHostNotConfirmedException>(() => resolver.GetUtilityConnectionString(server));
        Assert.Throws<ServerHostNotConfirmedException>(() => resolver.GetConnectionStringForDatabase(server, "db"));
        Assert.Throws<ServerHostNotConfirmedException>(() => resolver.GetQueryStoreReplicaConnectionString(server));
        Assert.True(ServerHostChange.AsksForSecret(server));

        /* The connection check tries nothing and says why; unknown rather than offline, so no unreachable alert fires. */
        var status = await manager.CheckConnectionAsync("a", allowInteractiveAuth: true);
        Assert.Null(status.IsOnline);
        Assert.Equal(refused.Message, status.ErrorMessage);
        Assert.Equal(refused.Message, manager.GetConnectionStatus("a").ErrorMessage);
    }

    [Fact]
    public void ServicePrincipal_ChangedHost_AsksForTheClientSecretAgain()
    {
        WriteSharedServers(Server("a", "one.database.windows.net", AuthenticationTypes.ServicePrincipal));
        NewManager();
        WriteSharedServers(Server("a", "two.database.windows.net", AuthenticationTypes.ServicePrincipal));
        var manager = NewManager();
        var server = manager.GetServerById("a")!;

        var refused = Assert.Throws<ServerHostNotConfirmedException>(() => manager.CredentialResolver.GetConnectionString(server));
        Assert.Equal("The host name for this server changed to two.database.windows.net. Enter the client secret again to connect to it.", refused.Message);
    }

    [Fact]
    public async Task IntegratedAuth_ChangedHost_IsRefusedUntilConfirmed_ThenConnectsToTheNewHost()
    {
        WriteSharedServers(Server("a", "sql01", AuthenticationTypes.Windows));
        NewManager();

        WriteSharedServers(Server("a", "sql77", AuthenticationTypes.Windows));
        var manager = NewManager();
        var server = manager.GetServerById("a")!;

        var refused = Assert.Throws<ServerHostNotConfirmedException>(() => manager.CredentialResolver.GetConnectionString(server));
        Assert.Equal("The host name for this server changed to sql77. Open Edit Server and save to confirm it before Lite connects.", refused.Message);
        Assert.False(ServerHostChange.AsksForSecret(server));
        Assert.Null((await manager.CheckConnectionAsync("a")).IsOnline);

        manager.ConfirmHost(server);

        var builder = new SqlConnectionStringBuilder(manager.CredentialResolver.GetConnectionString(server));
        Assert.Equal("tcp:sql77", builder.DataSource);
        Assert.True(builder.IntegratedSecurity);

        /* The confirmation is kept for the next run. */
        var next = NewManager();
        Assert.Null(next.HostChange(next.GetServerById("a")!));
    }

    [Fact]
    public void EntraAndProfileServers_ChangedHost_AreRefusedUntilConfirmed()
    {
        WriteSharedServers(
            Server("mfa", "a.database.windows.net", AuthenticationTypes.EntraMFA),
            Server("mi", "b.database.windows.net", AuthenticationTypes.ManagedIdentity),
            Server("def", "c.database.windows.net", AuthenticationTypes.EntraDefaultCredential),
            Server("dc", "d.database.windows.net", AuthenticationTypes.EntraDeviceCode),
            new ServerConnection { Id = "prof", ServerName = "e.database.windows.net", AuthenticationType = AuthenticationTypes.SqlServer, CredentialProfileId = "p1" });
        NewManager();

        WriteSharedServers(
            Server("mfa", "x.example", AuthenticationTypes.EntraMFA),
            Server("mi", "x.example", AuthenticationTypes.ManagedIdentity),
            Server("def", "x.example", AuthenticationTypes.EntraDefaultCredential),
            Server("dc", "x.example", AuthenticationTypes.EntraDeviceCode),
            new ServerConnection { Id = "prof", ServerName = "x.example", AuthenticationType = AuthenticationTypes.SqlServer, CredentialProfileId = "p1" });
        var manager = NewManager();

        foreach (var id in new[] { "mfa", "mi", "def", "dc", "prof" })
        {
            var server = manager.GetServerById(id)!;
            var refused = Assert.Throws<ServerHostNotConfirmedException>(() => manager.CredentialResolver.GetConnectionString(server));
            Assert.EndsWith("Open Edit Server and save to confirm it before Lite connects.", refused.Message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ChangedQueryStoreReplica_IsRefusedUntilConfirmed_ARemovedOneIsNot()
    {
        WriteSharedServers(
            Server("a", "sql01", AuthenticationTypes.Windows, replica: "sql01-r"),
            Server("b", "sql02", AuthenticationTypes.Windows, replica: "sql02-r"));
        NewManager();

        WriteSharedServers(
            Server("a", "sql01", AuthenticationTypes.Windows, replica: "other-r"),
            Server("b", "sql02", AuthenticationTypes.Windows, replica: null));
        var manager = NewManager();

        var change = manager.HostChange(manager.GetServerById("a")!);
        Assert.NotNull(change);
        Assert.True(change.ReplicaChanged);
        Assert.StartsWith("The Query Store replica for this server changed to other-r.", change.Describe(manager.GetServerById("a")!), StringComparison.Ordinal);
        Assert.Throws<ServerHostNotConfirmedException>(() => manager.CredentialResolver.GetQueryStoreReplicaConnectionString(manager.GetServerById("a")!));
        Assert.Null(manager.HostChange(manager.GetServerById("b")!));
    }

    [Fact]
    public void ServerFirstSeenAfterTheRecordExists_IsNotConnectedUntilConfirmed()
    {
        WriteSharedServers(Server("a", "sql01", AuthenticationTypes.Windows));
        NewManager();

        WriteSharedServers(Server("a", "sql01", AuthenticationTypes.Windows), Server("new", "sql05", AuthenticationTypes.Windows));
        var manager = NewManager();

        var change = manager.HostChange(manager.GetServerById("new")!);
        Assert.NotNull(change);
        Assert.Null(change.ConfirmedHost);
        Assert.Throws<ServerHostNotConfirmedException>(() => manager.CredentialResolver.GetConnectionString(manager.GetServerById("new")!));
    }

    [Fact]
    public void ServerAddedByThisUser_IsConfirmedForThem()
    {
        WriteSharedServers();
        var manager = NewManager();

        var added = Server(Guid.NewGuid().ToString(), "sql03", AuthenticationTypes.Windows);
        manager.AddServer(added);

        Assert.Null(manager.HostChange(added));
        Assert.Null(NewManager().HostChange(added));
    }

    [Fact]
    public void UnreadableRecord_LeavesEveryServerUnconfirmed_RatherThanBindingWhatServersJsonSaysNow()
    {
        WriteSharedServers(Server("a", "sql01", AuthenticationTypes.Windows));
        Directory.CreateDirectory(Path.GetDirectoryName(_bindingsPath)!);
        File.WriteAllText(_bindingsPath, "{ not json");

        var manager = NewManager();

        Assert.NotNull(manager.HostChange(manager.GetServerById("a")!));
    }

    [Fact]
    public void SqlLogin_SavedPassword_IsUsedForItsOwnHost_RefusedForAChangedOne_AndUsedAgainOnceReentered()
    {
        /* A store under a fresh GUID prefix, so nothing of the user's Lite credentials is read or touched. */
        var store = new CredentialStore("PerformanceMonitorLiteHostTest" + Guid.NewGuid().ToString("N") + "_");
        var credentials = new CredentialService(store);
        var bindings = new ServerHostBindings(_bindingsPath);
        var server = Server(Guid.NewGuid().ToString(), "sql01", AuthenticationTypes.SqlServer);
        try
        {
            Assert.SkipWhen(!credentials.SaveCredential(server.Id, "monitor", "first-secret"), "This session cannot write to Windows Credential Manager.");
            bindings.BindAllOnFirstRun([server]);
            var resolver = new CredentialResolver(credentials, null, bindings);

            var same = new SqlConnectionStringBuilder(resolver.GetConnectionString(server));
            Assert.Equal("tcp:sql01", same.DataSource);
            Assert.Equal("first-secret", same.Password);

            server.ServerName = "sql99";
            Assert.Throws<ServerHostNotConfirmedException>(() => resolver.GetConnectionString(server));

            /* The Edit dialog's save after the password is entered again: the credential is replaced, the host confirmed. */
            Assert.True(credentials.UpdateCredential(server.Id, "monitor", "second-secret"));
            bindings.Confirm(server);

            var rebound = new SqlConnectionStringBuilder(resolver.GetConnectionString(server));
            Assert.Equal("tcp:sql99", rebound.DataSource);
            Assert.Equal("second-secret", rebound.Password);
        }
        finally
        {
            credentials.DeleteCredential(server.Id);
        }
    }
}
