/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7639: saving the Excluded Databases dialog, in either mode, must not touch Credential Manager. The save used to go through
/// <c>ServerManager.UpdateServer</c> with no username, which deletes an Entra MFA server's stored username (the UPN lives only
/// there). All Credential Manager use is under a prefix of the test's own.
/// </summary>
[Collection("credential-manager")]
public sealed class ExcludedDatabasesDialogSaveTests : IDisposable
{
    private const string Upn = "dba@contoso.example";

    private static readonly string Tag = "Test_" + Guid.NewGuid().ToString("N") + "_";
    private readonly string _prefix = CredentialService.CredentialPrefix + Tag;
    private readonly string _folder = Path.Combine(Path.GetTempPath(), "lite-excl-dialog-" + Guid.NewGuid().ToString("N"));
    private readonly List<string> _ids = new();

    public ExcludedDatabasesDialogSaveTests() => Directory.CreateDirectory(_folder);

    public void Dispose()
    {
        var credentials = Credentials();
        foreach (var id in _ids)
        {
            credentials.DeleteCredential(id);
        }

        try { Directory.Delete(_folder, recursive: true); } catch { /* best effort */ }
    }

    private CredentialService Credentials() => new(new CredentialStore(_prefix, "Performance Monitor Lite test"));

    private (ServerManager Manager, ServerConnection Server) EntraMfaServerWithStoredUpn()
    {
        var manager = new ServerManager(_folder, credentialService: Credentials());
        var server = new ServerConnection
        {
            ServerName = "contoso.database.windows.net",
            DisplayName = "Contoso Azure",
            AuthenticationType = AuthenticationTypes.EntraMFA,
        };
        _ids.Add(server.Id);
        manager.AddServer(server, Upn, string.Empty);
        Assert.Equal(Upn, Credentials().GetCredential(server.Id)?.Username);
        return (manager, server);
    }

    [Fact]
    public void SavingTheXeSessionList_KeepsAnEntraMfaServersStoredUsername_AndSavesTheList()
    {
        var (manager, server) = EntraMfaServerWithStoredUpn();

        ExcludedDatabasesDialog.PersistSelection(manager, server, new List<string> { "Sales", "Orders" }, xeSessionsMode: true);

        Assert.Equal(Upn, Credentials().GetCredential(server.Id)?.Username);
        Assert.Equal(new[] { "Sales", "Orders" }, new ServerManager(_folder, credentialService: Credentials()).GetServerById(server.Id)!.XeSessionDatabases);
    }

    [Fact]
    public void SavingTheExcludedDatabases_KeepsAnEntraMfaServersStoredUsername_AndSavesTheList()
    {
        var (manager, server) = EntraMfaServerWithStoredUpn();

        ExcludedDatabasesDialog.PersistSelection(manager, server, new List<string> { "Scratch" }, xeSessionsMode: false);

        Assert.Equal(Upn, Credentials().GetCredential(server.Id)?.Username);
        var reloaded = new ServerManager(_folder, credentialService: Credentials()).GetServerById(server.Id)!;
        Assert.Equal(new[] { "Scratch" }, reloaded.ExcludedDatabases);
        Assert.Empty(reloaded.XeSessionDatabases);
    }
}
