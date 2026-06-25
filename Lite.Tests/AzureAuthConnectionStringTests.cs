/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Connection-string SHAPE tests for the non-interactive Azure auth modes (#1038):
/// service principal and managed identity. The testable surface is the keywords emitted by
/// ServerConnection.BuildConnectionString / ApplyAuthentication. Real Azure auth (token
/// acquisition against a tenant) is a maintainer E2E step and is NOT exercised here.
/// </summary>
public class AzureAuthConnectionStringTests
{
    private static SqlConnectionStringBuilder Parse(string connStr) => new(connStr);

    // ---- Service Principal ----------------------------------------------------------------

    [Fact]
    public void ServicePrincipal_SetsActiveDirectoryServicePrincipal_WithClientIdAndSecret()
    {
        var server = new ServerConnection
        {
            ServerName = "myazure.database.windows.net",
            DatabaseName = "mydb",
            AuthenticationType = AuthenticationTypes.ServicePrincipal,
            EncryptMode = "Mandatory"
        };

        var conn = Parse(server.BuildConnectionString("client-id-123", "the-secret"));

        Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryServicePrincipal, conn.Authentication);
        Assert.Equal("client-id-123", conn.UserID);
        Assert.Equal("the-secret", conn.Password);
        Assert.False(conn.IntegratedSecurity);
        Assert.Equal(SqlConnectionEncryptOption.Mandatory, conn.Encrypt);
    }

    [Fact]
    public void ServicePrincipal_FallsBackToAzureClientId_WhenUsernameNull()
    {
        var server = new ServerConnection
        {
            ServerName = "myazure.database.windows.net",
            AuthenticationType = AuthenticationTypes.ServicePrincipal,
            AzureClientId = "model-client-id"
        };

        var conn = Parse(server.BuildConnectionString(null, "secret"));

        Assert.Equal("model-client-id", conn.UserID);
        Assert.Equal("secret", conn.Password);
    }

    [Fact]
    public void ServicePrincipal_HonorsStrictEncrypt()
    {
        var server = new ServerConnection
        {
            ServerName = "s",
            AuthenticationType = AuthenticationTypes.ServicePrincipal,
            EncryptMode = "Strict"
        };

        var conn = Parse(server.BuildConnectionString("c", "p"));

        Assert.Equal(SqlConnectionEncryptOption.Strict, conn.Encrypt);
    }

    // ---- Managed Identity -----------------------------------------------------------------

    [Fact]
    public void ManagedIdentity_SystemAssigned_NoUserIdNoPassword()
    {
        var server = new ServerConnection
        {
            ServerName = "myazure.database.windows.net",
            AuthenticationType = AuthenticationTypes.ManagedIdentity
        };

        var conn = Parse(server.BuildConnectionString(null, null));

        Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryManagedIdentity, conn.Authentication);
        Assert.True(string.IsNullOrEmpty(conn.UserID));
        Assert.True(string.IsNullOrEmpty(conn.Password));
        Assert.False(conn.IntegratedSecurity);
    }

    [Fact]
    public void ManagedIdentity_UserAssigned_SetsUserIdToClientId()
    {
        var server = new ServerConnection
        {
            ServerName = "myazure.database.windows.net",
            AuthenticationType = AuthenticationTypes.ManagedIdentity,
            ManagedIdentityClientId = "uami-client-id"
        };

        var conn = Parse(server.BuildConnectionString(null, null));

        Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryManagedIdentity, conn.Authentication);
        Assert.Equal("uami-client-id", conn.UserID);
        Assert.True(string.IsNullOrEmpty(conn.Password));
    }

    // ---- Regression: existing modes unchanged ---------------------------------------------

    [Fact]
    public void Windows_UsesIntegratedSecurity_NoAuthenticationKeyword()
    {
        var server = new ServerConnection { ServerName = "s", AuthenticationType = AuthenticationTypes.Windows };
        var conn = Parse(server.BuildConnectionString(null, null));

        Assert.True(conn.IntegratedSecurity);
        Assert.Equal(SqlAuthenticationMethod.NotSpecified, conn.Authentication);
    }

    [Fact]
    public void SqlServer_SetsUserIdAndPassword_NoAuthenticationKeyword()
    {
        var server = new ServerConnection { ServerName = "s", AuthenticationType = AuthenticationTypes.SqlServer };
        var conn = Parse(server.BuildConnectionString("sa", "pw"));

        Assert.False(conn.IntegratedSecurity);
        Assert.Equal("sa", conn.UserID);
        Assert.Equal("pw", conn.Password);
        Assert.Equal(SqlAuthenticationMethod.NotSpecified, conn.Authentication);
    }

    [Fact]
    public void EntraMfa_SetsActiveDirectoryInteractive()
    {
        var server = new ServerConnection { ServerName = "s", AuthenticationType = AuthenticationTypes.EntraMFA };
        var conn = Parse(server.BuildConnectionString("user@tenant.com", null));

        Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryInteractive, conn.Authentication);
        Assert.Equal("user@tenant.com", conn.UserID);
    }

    // ---- AuthenticationDisplay switch -----------------------------------------------------

    [Theory]
    [InlineData(AuthenticationTypes.ServicePrincipal, "Azure — Service Principal")]
    [InlineData(AuthenticationTypes.ManagedIdentity, "Azure — Managed Identity")]
    [InlineData(AuthenticationTypes.EntraMFA, "Microsoft Entra MFA")]
    [InlineData(AuthenticationTypes.SqlServer, "SQL Server")]
    [InlineData(AuthenticationTypes.Windows, "Windows")]
    public void AuthenticationDisplay_MapsEachMode(string authType, string expected)
    {
        var server = new ServerConnection { AuthenticationType = authType };
        Assert.Equal(expected, server.AuthenticationDisplay);
    }

    // ---- Two-build-site PARITY ------------------------------------------------------------
    // The dialog's Test-Connection builder and ServerConnection's production builder both route
    // their auth keywords through the shared ServerConnection.ApplyAuthentication helper. This
    // proves they emit identical Authentication / UserID / Password / IntegratedSecurity for
    // each auth mode given the same inputs (the dialog's other knobs — ConnectTimeout etc. —
    // intentionally differ and are out of scope for auth parity).

    private static (SqlAuthenticationMethod Auth, string? User, string? Pwd, bool Integrated) AuthShape(
        string authType, string? user, string? pwd, string? azureClientId, string? miClientId)
    {
        var b = new SqlConnectionStringBuilder();
        ServerConnection.ApplyAuthentication(b, authType, user, pwd, azureClientId, miClientId);
        return (b.Authentication, b.UserID, b.Password, b.IntegratedSecurity);
    }

    [Theory]
    [InlineData(AuthenticationTypes.Windows)]
    [InlineData(AuthenticationTypes.SqlServer)]
    [InlineData(AuthenticationTypes.EntraMFA)]
    [InlineData(AuthenticationTypes.ServicePrincipal)]
    [InlineData(AuthenticationTypes.ManagedIdentity)]
    public void BuildSites_ProduceIdenticalAuthShape_PerMode(string authType)
    {
        // Production builder (ServerConnection) call shape: (username, password, AzureClientId, ManagedIdentityClientId).
        // Dialog builder call shape: (UserID-from-control, secret-from-PasswordBox, null azureClientId, miClientId).
        // For SP the dialog passes the client id as the username, so both reduce to the same inputs.
        string? user = authType switch
        {
            AuthenticationTypes.SqlServer => "sa",
            AuthenticationTypes.EntraMFA => "user@tenant.com",
            AuthenticationTypes.ServicePrincipal => "client-id",
            _ => null
        };
        string? pwd = authType is AuthenticationTypes.SqlServer or AuthenticationTypes.ServicePrincipal ? "secret" : null;
        string? mi = authType == AuthenticationTypes.ManagedIdentity ? "uami-id" : null;

        var production = AuthShape(authType, user, pwd, azureClientId: "client-id", miClientId: mi);
        var dialog = AuthShape(authType, user, pwd, azureClientId: null, miClientId: mi);

        Assert.Equal(production, dialog);
    }

    // ---- GetConnectionString / HasStoredCredentials (Credential Manager) ------------------
    // These touch the real Windows Credential Manager (DPAPI, per-user). Each test uses a unique
    // GUID server id and deletes its credential afterward.

    // Profile-less resolver: regression-identical to the removed GetConnectionString(CredentialService)
    // instance overload. A null IProfileLookup means every server resolves to its own inline auth.
    private static CredentialResolver ProfileLessResolver(CredentialService cs) => new(cs, profileLookup: null);

    [Fact]
    public void GetConnectionString_ServicePrincipal_FetchesStoredSecret()
    {
        var cs = new CredentialService();
        var resolver = ProfileLessResolver(cs);
        var server = new ServerConnection
        {
            ServerName = "s",
            AuthenticationType = AuthenticationTypes.ServicePrincipal
        };
        try
        {
            Assert.True(cs.SaveCredential(server.Id, "stored-client-id", "stored-secret"));

            var conn = Parse(resolver.GetConnectionString(server));

            Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryServicePrincipal, conn.Authentication);
            Assert.Equal("stored-client-id", conn.UserID);
            Assert.Equal("stored-secret", conn.Password);
        }
        finally
        {
            cs.DeleteCredential(server.Id);
        }
    }

    [Fact]
    public void GetConnectionString_ManagedIdentity_FetchesNoSecret()
    {
        var cs = new CredentialService();
        var resolver = ProfileLessResolver(cs);
        var server = new ServerConnection
        {
            ServerName = "s",
            AuthenticationType = AuthenticationTypes.ManagedIdentity
        };
        try
        {
            // Even if a stale credential somehow existed, MI must not pull it into the string.
            cs.SaveCredential(server.Id, "leftover", "leftover-secret");

            var conn = Parse(resolver.GetConnectionString(server));

            Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryManagedIdentity, conn.Authentication);
            Assert.True(string.IsNullOrEmpty(conn.Password));
            Assert.NotEqual("leftover", conn.UserID);
        }
        finally
        {
            cs.DeleteCredential(server.Id);
        }
    }

    [Fact]
    public void HasStoredCredentials_ManagedIdentity_IsTrue_WithoutCredential()
    {
        var cs = new CredentialService();
        var resolver = ProfileLessResolver(cs);
        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            AuthenticationType = AuthenticationTypes.ManagedIdentity
        };

        Assert.True(resolver.HasStoredCredentials(server));
    }

    [Fact]
    public void HasStoredCredentials_ServicePrincipal_ReflectsCredentialManager()
    {
        var cs = new CredentialService();
        var resolver = ProfileLessResolver(cs);
        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            AuthenticationType = AuthenticationTypes.ServicePrincipal
        };
        try
        {
            Assert.False(resolver.HasStoredCredentials(server));

            Assert.True(cs.SaveCredential(server.Id, "client", "secret"));
            Assert.True(resolver.HasStoredCredentials(server));
        }
        finally
        {
            cs.DeleteCredential(server.Id);
        }
    }

    // ---- ServerManager.UpdateServer credential lifecycle (Credential Manager) -------------
    // ServerManager owns its own CredentialService (real Windows Credential Manager, DPAPI),
    // same seam the tests above use. These drive UpdateServer against a temp config directory
    // and a unique GUID server id, asserting the orphaned-secret cleanup, then clean up.

    [Fact]
    public void UpdateServer_ServicePrincipalToEntraMfa_BlankUsername_DeletesOrphanedSecret()
    {
        var configDir = Path.Combine(Path.GetTempPath(), "pmlite-test-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(configDir);
        var manager = new ServerManager(configDir);

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = "myazure.database.windows.net",
            AuthenticationType = AuthenticationTypes.ServicePrincipal
        };
        var cs = manager.CredentialService;

        try
        {
            // Add as ServicePrincipal: stores client id + client secret in Credential Manager.
            manager.AddServer(server, "client-id-123", "the-sp-secret");
            Assert.True(cs.CredentialExists(server.Id), "precondition: SP secret should be stored");

            // Switch to EntraMFA with a BLANK MFA username (the legitimate, reachable case).
            // No username means the username-storing EntraMFA arm must NOT fire, and the stale
            // SP secret must be deleted rather than left orphaned (LOW-1).
            server.AuthenticationType = AuthenticationTypes.EntraMFA;
            manager.UpdateServer(server, username: string.Empty, password: null);

            Assert.False(cs.CredentialExists(server.Id),
                "orphaned SP secret must be deleted when switching SP -> EntraMFA with a blank username");
        }
        finally
        {
            cs.DeleteCredential(server.Id);
            try { Directory.Delete(configDir, recursive: true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public void UpdateServer_ServicePrincipalToEntraMfa_NonBlankUsername_StoresUsername()
    {
        // Regression guard for the fix: a NON-blank EntraMFA username must still hit the earlier
        // EntraMFA arm and STORE the username (not get deleted by the broadened cleanup arm).
        var configDir = Path.Combine(Path.GetTempPath(), "pmlite-test-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(configDir);
        var manager = new ServerManager(configDir);

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = "myazure.database.windows.net",
            AuthenticationType = AuthenticationTypes.ServicePrincipal
        };
        var cs = manager.CredentialService;

        try
        {
            manager.AddServer(server, "client-id-123", "the-sp-secret");

            server.AuthenticationType = AuthenticationTypes.EntraMFA;
            manager.UpdateServer(server, username: "user@tenant.com", password: null);

            Assert.True(cs.CredentialExists(server.Id),
                "non-blank EntraMFA username must be stored, not deleted");
            var stored = cs.GetCredential(server.Id);
            Assert.NotNull(stored);
            Assert.Equal("user@tenant.com", stored!.Value.Username);
        }
        finally
        {
            cs.DeleteCredential(server.Id);
            try { Directory.Delete(configDir, recursive: true); } catch { /* best effort */ }
        }
    }
}
