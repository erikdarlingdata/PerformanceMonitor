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
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Tests for shared credential profiles (#1038 Phase B): ProfileManager CRUD/persistence, the
/// fail-closed atomic-tuple resolution (profile creds NOT the server's stale fields), the HARD
/// dangling-profile test, profile-aware HasStoredCredentials, secret keying, referential integrity,
/// switch-cleanup, and import round-trip + secret-absent tolerance.
///
/// ProfileManager/ServerManager tests touch the real Windows Credential Manager (DPAPI, per-user)
/// and a temp config dir; each test cleans up. Pure resolution tests use an in-memory IProfileLookup
/// stub so they don't depend on Credential Manager for the profile non-secret fields.
/// </summary>
public class CredentialProfileTests
{
    private static SqlConnectionStringBuilder Parse(string connStr) => new(connStr);

    /// <summary>In-memory profile lookup for resolution tests (no Credential Manager dependency for fields).</summary>
    private sealed class StubProfileLookup : IProfileLookup
    {
        private readonly Dictionary<string, CredentialProfile> _byId = new();
        public void Add(CredentialProfile p) => _byId[p.Id] = p;
        public CredentialProfile? GetProfile(string id) => _byId.TryGetValue(id, out var p) ? p : null;
    }

    private static string NewTempConfigDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pmlite-proftest-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    // ---- Resolution: profile creds, NOT the server's stale fields (B-3) -----------------------

    [Fact]
    public void Resolution_ProfileServicePrincipal_UsesProfileClientId_NotServerStaleAzureClientId()
    {
        var cs = new CredentialService();
        var lookup = new StubProfileLookup();
        var profileId = Guid.NewGuid().ToString();
        lookup.Add(new CredentialProfile
        {
            Id = profileId,
            Name = "fleet-sp",
            AuthType = AuthenticationTypes.ServicePrincipal,
            AzureClientId = "profile-client-id"
        });

        // The server carries a STALE inline AzureClientId that must NOT leak into the resolved string.
        var server = new ServerConnection
        {
            ServerName = "myazure.database.windows.net",
            AuthenticationType = AuthenticationTypes.SqlServer, // overridden by the profile
            AzureClientId = "STALE-server-client-id",
            CredentialProfileId = profileId
        };

        try
        {
            // Profile secret lives under profile_{id}.
            Assert.True(cs.SaveCredential(ProfileManager.ProfileCredentialId(profileId), "profile-client-id", "profile-secret"));

            var conn = Parse(ServerConnection.ResolveConnectionString(server, cs, lookup));

            Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryServicePrincipal, conn.Authentication);
            Assert.Equal("profile-client-id", conn.UserID);       // from the profile, not the server
            Assert.Equal("profile-secret", conn.Password);
            Assert.NotEqual("STALE-server-client-id", conn.UserID); // server's stale field never used
        }
        finally
        {
            cs.DeleteCredential(ProfileManager.ProfileCredentialId(profileId));
        }
    }

    [Fact]
    public void Resolution_ProfileLessServer_IsRegressionIdentical()
    {
        var cs = new CredentialService();
        var lookup = new StubProfileLookup();
        var server = new ServerConnection
        {
            ServerName = "s",
            AuthenticationType = AuthenticationTypes.SqlServer
            // no CredentialProfileId
        };
        try
        {
            Assert.True(cs.SaveCredential(server.Id, "sa", "pw"));

            var conn = Parse(ServerConnection.ResolveConnectionString(server, cs, lookup));

            Assert.False(conn.IntegratedSecurity);
            Assert.Equal("sa", conn.UserID);
            Assert.Equal("pw", conn.Password);
            Assert.Equal(SqlAuthenticationMethod.NotSpecified, conn.Authentication);
        }
        finally
        {
            cs.DeleteCredential(server.Id);
        }
    }

    // ---- HARD: dangling profile must THROW and never self-fall-back (B-2) ----------------------

    [Fact]
    public void Resolution_DanglingProfile_Throws_AndNeverFallsBackToServerSelfAuth()
    {
        var cs = new CredentialService();
        var lookup = new StubProfileLookup(); // intentionally EMPTY — the referenced profile is missing
        var missingId = Guid.NewGuid().ToString();

        // The server has its OWN inline SQL auth that must NEVER be used as a silent fallback.
        var server = new ServerConnection
        {
            ServerName = "s",
            AuthenticationType = AuthenticationTypes.SqlServer,
            AzureClientId = "server-self-client",
            CredentialProfileId = missingId
        };
        try
        {
            // Even with a per-server secret present, the dangling profile must fail closed.
            cs.SaveCredential(server.Id, "server-self-user", "server-self-pw");

            var ex = Assert.Throws<InvalidOperationException>(
                () => ServerConnection.ResolveConnectionString(server, cs, lookup));

            Assert.Contains("credential profile", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(missingId, ex.Message);

            // No connection string was produced; assert the inline auth was never converted into one.
            // (If a wrong-identity fallback existed it would have built a string with these values.)
            Assert.DoesNotContain("server-self-user", ex.Message);
            Assert.DoesNotContain("server-self-pw", ex.Message);
        }
        finally
        {
            cs.DeleteCredential(server.Id);
        }
    }

    // ---- HasStoredCredentials profile-aware (N-1) ----------------------------------------------

    [Fact]
    public void HasStoredCredentials_ProfileBacked_ReflectsProfileSecret_NotServerKey()
    {
        var cs = new CredentialService();
        var lookup = new StubProfileLookup();
        var profileId = Guid.NewGuid().ToString();
        lookup.Add(new CredentialProfile { Id = profileId, Name = "p", AuthType = AuthenticationTypes.ServicePrincipal });

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            AuthenticationType = AuthenticationTypes.SqlServer,
            CredentialProfileId = profileId
        };
        try
        {
            // A secret under the SERVER key must not satisfy a profile-backed check.
            cs.SaveCredential(server.Id, "server-user", "server-pw");
            Assert.False(ServerConnection.HasStoredCredentials(server, cs, lookup));

            // Storing under the PROFILE key satisfies it.
            cs.SaveCredential(ProfileManager.ProfileCredentialId(profileId), "client", "secret");
            Assert.True(ServerConnection.HasStoredCredentials(server, cs, lookup));
        }
        finally
        {
            cs.DeleteCredential(server.Id);
            cs.DeleteCredential(ProfileManager.ProfileCredentialId(profileId));
        }
    }

    [Fact]
    public void HasStoredCredentials_ManagedIdentityProfile_IsTrue_WithoutSecret()
    {
        var cs = new CredentialService();
        var lookup = new StubProfileLookup();
        var profileId = Guid.NewGuid().ToString();
        lookup.Add(new CredentialProfile { Id = profileId, Name = "mi", AuthType = AuthenticationTypes.ManagedIdentity });

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            AuthenticationType = AuthenticationTypes.Windows,
            CredentialProfileId = profileId
        };

        Assert.True(ServerConnection.HasStoredCredentials(server, cs, lookup));
    }

    [Fact]
    public void HasStoredCredentials_DanglingProfile_IsFalse()
    {
        var cs = new CredentialService();
        var lookup = new StubProfileLookup(); // empty
        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            AuthenticationType = AuthenticationTypes.ManagedIdentity, // would be true if not profile-backed
            CredentialProfileId = Guid.NewGuid().ToString()
        };

        Assert.False(ServerConnection.HasStoredCredentials(server, cs, lookup));
    }

    // ---- ProfileManager CRUD + persistence + secret keying -------------------------------------

    [Fact]
    public void ProfileManager_Add_Get_Persists_AndStoresSecretUnderProfileKey()
    {
        var configDir = NewTempConfigDir();
        var sm = new ServerManager(configDir);
        var pm = new ProfileManager(sm);
        var cs = pm.CredentialService;

        var profile = new CredentialProfile
        {
            Id = Guid.NewGuid().ToString(),
            Name = "sql-fleet",
            AuthType = AuthenticationTypes.SqlServer,
            Username = "sa"
        };
        try
        {
            pm.AddProfile(profile, "sa", "the-password");

            // Secret stored under profile_{id}, NOT a bare server GUID key.
            Assert.True(cs.CredentialExists(ProfileManager.ProfileCredentialId(profile.Id)));
            Assert.False(cs.CredentialExists(profile.Id)); // bare id key must be empty

            // profiles.json on disk, no secret in it.
            var json = File.ReadAllText(Path.Combine(configDir, "profiles.json"));
            Assert.Contains("sql-fleet", json);
            Assert.DoesNotContain("the-password", json);

            // Round-trip: a fresh manager loads it.
            var pm2 = new ProfileManager(new ServerManager(configDir));
            var loaded = pm2.GetProfile(profile.Id);
            Assert.NotNull(loaded);
            Assert.Equal("sql-fleet", loaded!.Name);
            Assert.Equal(AuthenticationTypes.SqlServer, loaded.AuthType);
        }
        finally
        {
            cs.DeleteCredential(ProfileManager.ProfileCredentialId(profile.Id));
            try { Directory.Delete(configDir, recursive: true); } catch { }
        }
    }

    [Fact]
    public void ProfileManager_ManagedIdentity_StoresNoSecret()
    {
        var configDir = NewTempConfigDir();
        var pm = new ProfileManager(new ServerManager(configDir));
        var cs = pm.CredentialService;

        var profile = new CredentialProfile
        {
            Id = Guid.NewGuid().ToString(),
            Name = "mi",
            AuthType = AuthenticationTypes.ManagedIdentity
        };
        try
        {
            pm.AddProfile(profile, username: null, secret: null);
            Assert.False(cs.CredentialExists(ProfileManager.ProfileCredentialId(profile.Id)));
            Assert.True(pm.HasStoredSecret(profile)); // MI needs none
        }
        finally
        {
            try { Directory.Delete(configDir, recursive: true); } catch { }
        }
    }

    [Fact]
    public void ProfileManager_Delete_RemovesProfileAndSecret()
    {
        var configDir = NewTempConfigDir();
        var pm = new ProfileManager(new ServerManager(configDir));
        var cs = pm.CredentialService;

        var profile = new CredentialProfile
        {
            Id = Guid.NewGuid().ToString(),
            Name = "to-delete",
            AuthType = AuthenticationTypes.ServicePrincipal,
            AzureClientId = "cid"
        };
        try
        {
            pm.AddProfile(profile, "cid", "sec");
            Assert.True(cs.CredentialExists(ProfileManager.ProfileCredentialId(profile.Id)));

            pm.DeleteProfile(profile.Id);

            Assert.Null(pm.GetProfile(profile.Id));
            Assert.False(cs.CredentialExists(ProfileManager.ProfileCredentialId(profile.Id)));
        }
        finally
        {
            cs.DeleteCredential(ProfileManager.ProfileCredentialId(profile.Id));
            try { Directory.Delete(configDir, recursive: true); } catch { }
        }
    }

    // ---- Referential integrity: delete blocked while referenced (§6) ---------------------------

    [Fact]
    public void ProfileManager_Delete_BlockedWhileServerReferences_AllowedAfterReassign()
    {
        var configDir = NewTempConfigDir();
        var sm = new ServerManager(configDir);
        var pm = new ProfileManager(sm);
        var cs = pm.CredentialService;

        var profile = new CredentialProfile
        {
            Id = Guid.NewGuid().ToString(),
            Name = "shared",
            AuthType = AuthenticationTypes.SqlServer,
            Username = "sa"
        };
        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = "s",
            CredentialProfileId = profile.Id
        };
        try
        {
            pm.AddProfile(profile, "sa", "pw");
            sm.AddServer(server);

            // Blocked while referenced.
            var ex = Assert.Throws<InvalidOperationException>(() => pm.DeleteProfile(profile.Id));
            Assert.Contains("used by", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.NotNull(pm.GetProfile(profile.Id));

            // Reassign (clear the reference) then delete succeeds.
            server.CredentialProfileId = null;
            sm.UpdateServer(server);
            pm.DeleteProfile(profile.Id);
            Assert.Null(pm.GetProfile(profile.Id));
        }
        finally
        {
            cs.DeleteCredential(ProfileManager.ProfileCredentialId(profile.Id));
            cs.DeleteCredential(server.Id);
            try { Directory.Delete(configDir, recursive: true); } catch { }
        }
    }

    // ---- Switch-cleanup: assigning a profile deletes the orphaned per-server secret (M-3) ------
    // Mirrors the dialog's M-3 scrub: store no per-server secret + explicitly delete the server key.

    [Fact]
    public void SwitchToProfile_DeletesOrphanedPerServerSecret()
    {
        var configDir = NewTempConfigDir();
        var sm = new ServerManager(configDir);
        var cs = sm.CredentialService;

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = "s",
            AuthenticationType = AuthenticationTypes.SqlServer
        };
        try
        {
            // Server starts with an inline SQL secret.
            sm.AddServer(server, "sa", "inline-pw");
            Assert.True(cs.CredentialExists(server.Id));

            // Switch onto a profile: the dialog's M-3 path nulls per-server identity, persists with no
            // secret, and explicitly deletes the per-server key.
            server.CredentialProfileId = Guid.NewGuid().ToString();
            server.AzureClientId = null;
            server.AzureTenantId = null;
            server.ManagedIdentityClientId = null;
            sm.UpdateServer(server, null, null);
            cs.DeleteCredential(server.Id);

            Assert.False(cs.CredentialExists(server.Id), "orphaned per-server secret must be deleted");
        }
        finally
        {
            cs.DeleteCredential(server.Id);
            try { Directory.Delete(configDir, recursive: true); } catch { }
        }
    }

    // ---- Import round-trip + secret-absent tolerance (§7) --------------------------------------

    [Fact]
    public void Import_RoundTrips_AndBuildTolerates_SecretAbsentImportedProfile()
    {
        var srcDir = NewTempConfigDir();
        var dstDir = NewTempConfigDir();
        var cs = new CredentialService();

        // Build a source profiles.json via a source ProfileManager (no secret stored for it; imports
        // never carry secrets).
        var srcPm = new ProfileManager(new ServerManager(srcDir));
        var profile = new CredentialProfile
        {
            Id = Guid.NewGuid().ToString(),
            Name = "imported-sp",
            AuthType = AuthenticationTypes.ServicePrincipal,
            AzureClientId = "imp-client"
        };
        // Add to source WITHOUT a secret to simulate "secret not importable".
        srcPm.AddProfile(profile, username: null, secret: null);
        var srcProfilesPath = Path.Combine(srcDir, "profiles.json");

        var dstSm = new ServerManager(dstDir);
        var dstPm = new ProfileManager(dstSm);
        dstSm.ProfileLookup = dstPm;

        try
        {
            var (imported, _) = dstPm.ImportProfilesFromFile(srcProfilesPath);
            Assert.Equal(1, imported);

            var loaded = dstPm.GetProfile(profile.Id);
            Assert.NotNull(loaded);
            Assert.Equal("imported-sp", loaded!.Name);

            // A server pointed at the imported (secret-absent) profile resolves WITHOUT a wrong identity:
            // the SP build simply has an empty Password (no secret in Credential Manager), never the
            // server's own creds. It does NOT throw (the profile exists; only its secret is missing).
            var server = new ServerConnection
            {
                Id = Guid.NewGuid().ToString(),
                ServerName = "s",
                AuthenticationType = AuthenticationTypes.SqlServer,
                CredentialProfileId = profile.Id
            };
            var conn = Parse(ServerConnection.ResolveConnectionString(server, cs, dstPm));
            Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryServicePrincipal, conn.Authentication);
            Assert.Equal("imp-client", conn.UserID); // profile's non-secret client id
            Assert.True(string.IsNullOrEmpty(conn.Password)); // secret absent → empty, never wrong identity
        }
        finally
        {
            cs.DeleteCredential(ProfileManager.ProfileCredentialId(profile.Id));
            try { Directory.Delete(srcDir, recursive: true); } catch { }
            try { Directory.Delete(dstDir, recursive: true); } catch { }
        }
    }

    [Fact]
    public void ProfileManager_NameUniqueness_EnforcedPerProcess()
    {
        var configDir = NewTempConfigDir();
        var pm = new ProfileManager(new ServerManager(configDir));
        var cs = pm.CredentialService;

        var a = new CredentialProfile { Id = Guid.NewGuid().ToString(), Name = "dup", AuthType = AuthenticationTypes.ManagedIdentity };
        var b = new CredentialProfile { Id = Guid.NewGuid().ToString(), Name = "dup", AuthType = AuthenticationTypes.ManagedIdentity };
        try
        {
            pm.AddProfile(a);
            Assert.Throws<InvalidOperationException>(() => pm.AddProfile(b));
        }
        finally
        {
            try { Directory.Delete(configDir, recursive: true); } catch { }
        }
    }
}
