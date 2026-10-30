/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
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

    /* A credential store under a fresh GUID prefix: the first-run step reads saved credentials, and never reads the user's. */
    private readonly CredentialService _credentials =
        new(new CredentialStore("PerformanceMonitorLiteHostTest" + Guid.NewGuid().ToString("N") + "_"));

    private ServerManager NewManager() => new(_sharedDir, hostBindingsPath: _bindingsPath, credentialService: _credentials);

    private static string StoredName(ServerConnection server) => RemoteCollectorService.GetServerNameForStorage(server);

    /// <summary>The first run for a user who had Lite data and collection rows for every server in the list.</summary>
    private ServerManager FirstRunAsAUserWhoUsedEveryServer()
    {
        var manager = NewManager();
        manager.BindHostsOnFirstRun(hadLiteData: true, manager.GetAllServers().Select(StoredName).ToArray());
        return manager;
    }

    /// <summary>A profile list the test changes as profiles.json would.</summary>
    private sealed class Profiles : IProfileLookup
    {
        public System.Collections.Generic.Dictionary<string, CredentialProfile> ById { get; } = new();
        public CredentialProfile? GetProfile(string id) => ById.TryGetValue(id, out var profile) ? profile : null;
    }

    [Fact]
    public void ProfileServer_ProfileSignInTypeOrClientIdChange_IsRefused_ServersJsonCopyIsNotWhatIsChecked()
    {
        var profiles = new Profiles();
        profiles.ById["p1"] = new CredentialProfile { Id = "p1", Name = "Ops", AuthType = AuthenticationTypes.ManagedIdentity, ManagedIdentityClientId = "mi-1" };
        var bindings = new ServerHostBindings(_bindingsPath, () => profiles);
        var resolver = new CredentialResolver(_credentials, profiles, bindings);
        var server = new ServerConnection { Id = "a", ServerName = "sql01", AuthenticationType = AuthenticationTypes.ManagedIdentity, CredentialProfileId = "p1" };
        bindings.Confirm(server);
        Assert.Null(bindings.Check(server));

        /* The copy in servers.json is not what a profile server connects with, so it is not what is checked. */
        server.AuthenticationType = AuthenticationTypes.SqlServer;
        Assert.Null(bindings.Check(server));

        profiles.ById["p1"].AuthType = AuthenticationTypes.ServicePrincipal;
        var change = bindings.Check(server);
        Assert.NotNull(change);
        Assert.Equal("sign-in method", Assert.Single(change.Settings).Name);
        Assert.Throws<ServerHostNotConfirmedException>(() => resolver.GetConnectionString(server));

        profiles.ById["p1"].AuthType = AuthenticationTypes.ManagedIdentity;
        Assert.Null(bindings.Check(server));
        profiles.ById["p1"].ManagedIdentityClientId = "mi-2";
        Assert.Equal("The managed identity client ID for this server changed from mi-1 to mi-2.", bindings.Check(server)!.DescribeChange());

        /* The next start, with the same profile list, refuses the same change. */
        Assert.NotNull(new ServerHostBindings(_bindingsPath, () => profiles).Check(server));
    }

    [Fact]
    public void ProfileServer_ProfileNotFound_IsRefused()
    {
        var profiles = new Profiles();
        profiles.ById["p1"] = new CredentialProfile { Id = "p1", Name = "Ops", AuthType = AuthenticationTypes.SqlServer };
        var bindings = new ServerHostBindings(_bindingsPath, () => profiles);
        var server = new ServerConnection { Id = "a", ServerName = "sql01", AuthenticationType = AuthenticationTypes.SqlServer, CredentialProfileId = "p1" };
        bindings.Confirm(server);
        Assert.Null(bindings.Check(server));

        profiles.ById.Clear();
        var change = bindings.Check(server);
        Assert.NotNull(change);
        Assert.Equal("The credential profile for this server changed from p1 to a profile Lite cannot find.", change.DescribeChange());
        Assert.Throws<ServerHostNotConfirmedException>(() => new CredentialResolver(_credentials, profiles, bindings).GetConnectionString(server));
    }

    [Fact]
    public void ProfilesJson_ProfileWithASignInTypeAProfileCannotHave_IsNotLoaded()
    {
        WriteSharedServers();
        File.WriteAllText(Path.Combine(_sharedDir, "profiles.json"), JsonSerializer.Serialize(new
        {
            Profiles = new[]
            {
                new CredentialProfile { Id = "sql", Name = "Sql", AuthType = AuthenticationTypes.SqlServer },
                new CredentialProfile { Id = "sp", Name = "Sp", AuthType = AuthenticationTypes.ServicePrincipal },
                new CredentialProfile { Id = "mi", Name = "Mi", AuthType = AuthenticationTypes.ManagedIdentity },
                new CredentialProfile { Id = "win", Name = "Win", AuthType = AuthenticationTypes.Windows },
                new CredentialProfile { Id = "mfa", Name = "Mfa", AuthType = AuthenticationTypes.EntraMFA },
                new CredentialProfile { Id = "other", Name = "Other", AuthType = "Something" }
            }
        }));

        var profiles = new ProfileManager(NewManager());

        Assert.NotNull(profiles.GetProfile("sql"));
        Assert.NotNull(profiles.GetProfile("sp"));
        Assert.NotNull(profiles.GetProfile("mi"));
        Assert.Null(profiles.GetProfile("win"));
        Assert.Null(profiles.GetProfile("mfa"));
        Assert.Null(profiles.GetProfile("other"));
    }

    [Fact]
    public void FirstRun_MatchesStoredServerNames_NotTheirHashIds()
    {
        /* Two host names with the same 32-bit FNV-1a id: rows stored under one say nothing about the other. */
        Assert.Equal(RemoteCollectorService.GetDeterministicHashCode("sql-1022789"), RemoteCollectorService.GetDeterministicHashCode("sql-1239192"));
        WriteSharedServers(Server("a", "sql-1239192", AuthenticationTypes.Windows), Server("b", "sql02", AuthenticationTypes.Windows));
        var manager = NewManager();

        /* The stored name is matched case-insensitively. */
        Assert.Equal(1, manager.BindHostsOnFirstRun(hadLiteData: true, ["sql-1022789", "SQL02"]));

        Assert.Null(manager.HostChange(manager.GetServerById("a")!)!.ConfirmedHost);
        Assert.Null(manager.HostChange(manager.GetServerById("b")!));
    }

    [Fact]
    public void DataFolderThisBuildCreated_IsNotAnUpgradeOnTheNextStart_EvenWhenTheRecordWriteFailed()
    {
        var dataDir = Path.Combine(_root, "data");
        var database = Path.Combine(dataDir, "monitor.duckdb");
        var config = Path.Combine(dataDir, "config");
        var recordPath = Path.Combine(config, ServerHostBindings.FileName);

        /* The first start: nothing there yet. The database is created next, and the record cannot be written. */
        Assert.False(ServerHostBindings.IsFirstStartInUsedFolder(database, config));
        Directory.CreateDirectory(dataDir);
        File.WriteAllText(database, "");
        WriteSharedServers(Server("a", "sql01", AuthenticationTypes.Windows));
        Directory.CreateDirectory(recordPath);
        Assert.Equal(0, NewManagerIn(config).BindHostsOnFirstRun(hadLiteData: false, []));
        Directory.Delete(recordPath);

        /* The next start sees the database and no record, and still binds nothing. */
        var hadLiteData = ServerHostBindings.IsFirstStartInUsedFolder(database, config);
        Assert.False(hadLiteData);
        var next = NewManagerIn(config);
        Assert.Equal(0, next.BindHostsOnFirstRun(hadLiteData, ["sql01"]));
        Assert.NotNull(next.HostChange(next.GetServerById("a")!));
    }

    /// <summary>A manager whose per-user record sits in <paramref name="config"/>, as it does in Lite's own config folder.</summary>
    private ServerManager NewManagerIn(string config) =>
        new(_sharedDir, hostBindingsPath: Path.Combine(config, ServerHostBindings.FileName), credentialService: _credentials);

    [Fact]
    public void FolderAnEarlierLiteUsed_StartThatNeverBound_LeavesNoMarker_AndTheNextStartStillBindsTheServersWithRows()
    {
        var oldDir = Path.Combine(_root, "old");
        var database = Path.Combine(oldDir, "monitor.duckdb");
        var config = Path.Combine(oldDir, "config");
        Directory.CreateDirectory(oldDir);
        File.WriteAllText(database, "");
        WriteSharedServers(Server("a", "sql01", AuthenticationTypes.Windows), Server("b", "sql02", AuthenticationTypes.Windows));

        /* The first start of this build: the folder is an earlier Lite's. The local store then fails to open (or the window is
           closed), so the first-run step never runs. */
        Assert.True(ServerHostBindings.IsFirstStartInUsedFolder(database, config));
        Assert.False(File.Exists(Path.Combine(config, ServerHostBindings.StartedMarkerName)));

        /* The next start still reads the folder as an earlier Lite's and binds the server that has rows. */
        Assert.True(ServerHostBindings.IsFirstStartInUsedFolder(database, config));
        var manager = NewManagerIn(config);
        Assert.Equal(1, manager.BindHostsOnFirstRun(hadLiteData: true, ["sql01"]));
        Assert.Null(manager.HostChange(manager.GetServerById("a")!));
        Assert.NotNull(manager.HostChange(manager.GetServerById("b")!));

        /* Once the record is written the marker is too, and later starts are not upgrades. */
        Assert.True(File.Exists(Path.Combine(config, ServerHostBindings.StartedMarkerName)));
        Assert.False(ServerHostBindings.IsFirstStartInUsedFolder(database, config));
    }

    [Fact]
    public void FolderAnEarlierLiteUsed_FailedRecordWrite_LeavesNoMarker_AndTheNextStartBindsTheServersWithRows()
    {
        var oldDir = Path.Combine(_root, "old");
        var database = Path.Combine(oldDir, "monitor.duckdb");
        var config = Path.Combine(oldDir, "config");
        var recordPath = Path.Combine(config, ServerHostBindings.FileName);
        Directory.CreateDirectory(oldDir);
        File.WriteAllText(database, "");
        WriteSharedServers(Server("a", "sql01", AuthenticationTypes.Windows));

        /* A folder where the record file goes: the write fails the way a locked or read-only file does. */
        Directory.CreateDirectory(recordPath);
        var firstStart = ServerHostBindings.IsFirstStartInUsedFolder(database, config);
        Assert.True(firstStart);
        Assert.Equal(0, NewManagerIn(config).BindHostsOnFirstRun(firstStart, ["sql01"]));
        Assert.False(File.Exists(Path.Combine(config, ServerHostBindings.StartedMarkerName)));
        Directory.Delete(recordPath);

        var hadLiteData = ServerHostBindings.IsFirstStartInUsedFolder(database, config);
        Assert.True(hadLiteData);
        var next = NewManagerIn(config);
        Assert.Equal(1, next.BindHostsOnFirstRun(hadLiteData, ["sql01"]));
        Assert.Null(next.HostChange(next.GetServerById("a")!));
    }

    [Theory]
    [InlineData("{\"Servers\":{}}")]
    [InlineData("this is not json")]
    public void FolderWithARecordAndNoMarker_GetsTheMarkerOnTheFirstRunStep_SoTheNextStartIsNotAnUpgrade(string recordText)
    {
        var oldDir = Path.Combine(_root, "old");
        var database = Path.Combine(oldDir, "monitor.duckdb");
        var config = Path.Combine(oldDir, "config");
        var marker = Path.Combine(config, ServerHostBindings.StartedMarkerName);
        Directory.CreateDirectory(config);
        File.WriteAllText(database, "");
        File.WriteAllText(Path.Combine(config, ServerHostBindings.FileName), recordText);
        WriteSharedServers(Server("a", "sql01", AuthenticationTypes.Windows));

        /* A record an earlier start wrote (its marker write failed), or one that does not parse: with no marker the folder
           reads as an upgrade, and every later start would run the stored-server-name sweep again. */
        Assert.True(ServerHostBindings.IsFirstStartInUsedFolder(database, config));

        Assert.Equal(0, NewManagerIn(config).BindHostsOnFirstRun(hadLiteData: true, ["sql01"]));

        Assert.True(File.Exists(marker));
        Assert.False(ServerHostBindings.IsFirstStartInUsedFolder(database, config));
    }

    [Fact]
    public void TypedPassword_WithWeakerEncryptionOrADatabaseChange_StillAsks()
    {
        var server = Server("a", "sql01", AuthenticationTypes.SqlServer);
        var (bindings, _) = Confirmed(server);
        var login = HostConfirmationGate.CredentialWords(AuthenticationTypes.SqlServer, null);

        server.EncryptMode = "Optional";
        Assert.Equal("Connect to sql01 with these settings?\n\nThe encryption for this server changed from Mandatory to Optional.",
            HostConfirmationGate.Question(bindings.Check(server), isEdit: true, login, secretTypedNow: true));

        server.EncryptMode = "Mandatory";
        server.TrustServerCertificate = true;
        Assert.NotNull(HostConfirmationGate.Question(bindings.Check(server), isEdit: true, login, secretTypedNow: true));

        server.TrustServerCertificate = false;
        server.UtilityDatabase = "Tools";
        Assert.EndsWith("The utility database for this server changed from none to Tools.",
            HostConfirmationGate.Question(bindings.Check(server), isEdit: true, login, secretTypedNow: true), StringComparison.Ordinal);

        /* A host change alone is confirmed by typing the password. */
        server.UtilityDatabase = null;
        server.ServerName = "sql02";
        Assert.Null(HostConfirmationGate.Question(bindings.Check(server), isEdit: true, login, secretTypedNow: true));
    }

    [Fact]
    public void LongHostName_ShowsBothEnds()
    {
        var host = "prod-" + new string('x', 70) + ".corp.example.com";

        var shown = ServerHostChange.Shown(host);

        Assert.Equal(80, shown.Length);
        Assert.Equal(host[..38] + "..." + host[^39..], shown);
        Assert.EndsWith(".corp.example.com", shown, StringComparison.Ordinal);
    }

    /// <summary>A record with the one server confirmed as it is now, and the resolver that enforces it.</summary>
    private (ServerHostBindings Bindings, CredentialResolver Resolver) Confirmed(ServerConnection server)
    {
        var bindings = new ServerHostBindings(_bindingsPath);
        bindings.Confirm(server);
        return (bindings, new CredentialResolver(_credentials, null, bindings));
    }

    [Fact]
    public void AuthenticationTypeOrCredentialProfileChange_IsRefused()
    {
        var server = Server("a", "sql01", AuthenticationTypes.Windows);
        var (bindings, resolver) = Confirmed(server);
        Assert.Null(bindings.Check(server));

        server.AuthenticationType = AuthenticationTypes.EntraDefaultCredential;
        var change = bindings.Check(server);
        Assert.NotNull(change);
        Assert.Equal("sign-in method", Assert.Single(change.Settings).Name);
        Assert.Equal("The sign-in method for this server changed from Windows sign-in to Microsoft Entra default credential.", change.DescribeChange());
        Assert.Throws<ServerHostNotConfirmedException>(() => resolver.GetConnectionString(server));

        server.AuthenticationType = AuthenticationTypes.Windows;
        Assert.Null(bindings.Check(server));

        server.CredentialProfileId = "p2";
        Assert.Equal("credential profile", Assert.Single(bindings.Check(server)!.Settings).Name);
        Assert.Throws<ServerHostNotConfirmedException>(() => resolver.GetConnectionString(server));

        /* The record on disk keeps both: a new instance (the next start) refuses the same change. */
        Assert.NotNull(new ServerHostBindings(_bindingsPath).Check(server));
    }

    [Fact]
    public void UtilityDatabaseOrDatabaseChange_IsRefused()
    {
        var server = Server("a", "sql01", AuthenticationTypes.Windows);
        server.UtilityDatabase = "DBA";
        var (bindings, resolver) = Confirmed(server);
        Assert.Contains("Initial Catalog=DBA", resolver.GetUtilityConnectionString(server), StringComparison.Ordinal);

        server.UtilityDatabase = "OtherDb";
        Assert.Equal("utility database", Assert.Single(bindings.Check(server)!.Settings).Name);
        Assert.Throws<ServerHostNotConfirmedException>(() => resolver.GetUtilityConnectionString(server));

        server.UtilityDatabase = "DBA";
        server.DatabaseName = "OtherDb";
        Assert.Equal("database", Assert.Single(bindings.Check(server)!.Settings).Name);
        Assert.Throws<ServerHostNotConfirmedException>(() => resolver.GetUtilityConnectionString(server));
    }

    [Fact]
    public void WeakerEncryption_IsRefused_StrongerPasses_AndIsRecorded()
    {
        var server = Server("a", "sql01", AuthenticationTypes.Windows);
        server.EncryptMode = "Mandatory";
        server.TrustServerCertificate = false;
        var (bindings, resolver) = Confirmed(server);

        server.EncryptMode = "Optional";
        Assert.Equal("encryption", Assert.Single(bindings.Check(server)!.Settings).Name);
        Assert.Throws<ServerHostNotConfirmedException>(() => resolver.GetConnectionString(server));

        server.EncryptMode = "Mandatory";
        server.TrustServerCertificate = true;
        Assert.Equal("certificate check", Assert.Single(bindings.Check(server)!.Settings).Name);

        /* Stricter passes, and the record takes it: stepping back to the old setting is then a change. */
        server.TrustServerCertificate = false;
        server.EncryptMode = "Strict";
        Assert.Null(bindings.Check(server));
        Assert.Equal("tcp:sql01", new SqlConnectionStringBuilder(resolver.GetConnectionString(server)).DataSource);

        server.EncryptMode = "Mandatory";
        Assert.NotNull(bindings.Check(server));
        Assert.NotNull(new ServerHostBindings(_bindingsPath).Check(server));

        /* A dialog's unsaved stricter setting is not recorded. */
        var other = Server("b", "sql02", AuthenticationTypes.Windows);
        other.TrustServerCertificate = true;
        bindings.Confirm(other);
        other.TrustServerCertificate = false;
        Assert.Null(bindings.Check(other, recordStricter: false));
        other.TrustServerCertificate = true;
        Assert.Null(bindings.Check(other));
    }

    [Fact]
    public void NewUser_WithNoLiteData_BindsNothing_AndEveryServerSaysSo()
    {
        WriteSharedServers(
            Server("a", "sql01", AuthenticationTypes.Windows),
            Server("b", "sql02", AuthenticationTypes.Windows));
        var manager = NewManager();

        Assert.Equal(0, manager.BindHostsOnFirstRun(hadLiteData: false, manager.GetAllServers().Select(StoredName).ToArray()));

        foreach (var id in new[] { "a", "b" })
        {
            var server = manager.GetServerById(id)!;
            var change = manager.HostChange(server);
            Assert.NotNull(change);
            Assert.Null(change.ConfirmedHost);
            Assert.StartsWith($"You have not connected to this server's host, {server.ServerName}, from this Windows account yet.", change.Describe(server), StringComparison.Ordinal);
            Assert.Throws<ServerHostNotConfirmedException>(() => manager.CredentialResolver.GetConnectionString(server));
        }

        /* The record is written, so the next start (now with Lite data) does not bind the list either. */
        Assert.True(File.Exists(_bindingsPath));
        var next = NewManager();
        Assert.Equal(0, next.BindHostsOnFirstRun(hadLiteData: true, next.GetAllServers().Select(StoredName).ToArray()));
        Assert.NotNull(next.HostChange(next.GetServerById("a")!));
    }

    [Fact]
    public void UpgradingUser_BindsOnlyTheServersWithStoredRows_NotOnesWithOnlyASavedSecret()
    {
        var withRows = Server("rows", "sql01", AuthenticationTypes.Windows);
        var withPassword = Server(Guid.NewGuid().ToString(), "sql02", AuthenticationTypes.SqlServer);
        var profileId = Guid.NewGuid().ToString();
        var withProfile = new ServerConnection { Id = "prof", ServerName = "sql03", AuthenticationType = AuthenticationTypes.SqlServer, CredentialProfileId = profileId };
        var unused = Server("unused", "sql04", AuthenticationTypes.Windows);
        var unusedLogin = Server("unused-login", "sql05", AuthenticationTypes.SqlServer);
        WriteSharedServers(withRows, withPassword, withProfile, unused, unusedLogin);
        try
        {
            Assert.SkipWhen(!_credentials.SaveCredential(withPassword.Id, "monitor", "secret"), "This session cannot write to Windows Credential Manager.");
            Assert.True(_credentials.SaveCredential(ProfileManager.ProfileCredentialId(profileId), "monitor", "secret"));

            var manager = NewManager();
            manager.ProfileLookup = new OneProfile(new CredentialProfile { Id = profileId, Name = "Ops", AuthType = AuthenticationTypes.SqlServer });

            /* A saved password or profile secret is kept by id and says nothing about the host: those servers are asked once. */
            Assert.Equal(1, manager.BindHostsOnFirstRun(hadLiteData: true, [StoredName(withRows)]));

            Assert.Null(manager.HostChange(manager.GetServerById("rows")!));
            Assert.Null(manager.HostChange(manager.GetServerById(withPassword.Id)!)!.ConfirmedHost);
            Assert.Null(manager.HostChange(manager.GetServerById("prof")!)!.ConfirmedHost);
            Assert.Null(manager.HostChange(manager.GetServerById("unused")!)!.ConfirmedHost);
            Assert.Null(manager.HostChange(manager.GetServerById("unused-login")!)!.ConfirmedHost);
        }
        finally
        {
            _credentials.DeleteCredential(withPassword.Id);
            _credentials.DeleteCredential(ProfileManager.ProfileCredentialId(profileId));
        }
    }

    [Fact]
    public void FailedSave_IsSurfaced_AndTheFirstRunBindsNothing()
    {
        WriteSharedServers(Server("a", "sql01", AuthenticationTypes.Windows));
        /* A folder where the record file goes: the write fails the way a locked or read-only file does. */
        Directory.CreateDirectory(_bindingsPath);
        var manager = NewManager();
        var shown = new System.Collections.Generic.List<string>();
        manager.HostRecordSaveFailed += shown.Add;

        Assert.Equal(0, manager.BindHostsOnFirstRun(hadLiteData: true, manager.GetAllServers().Select(StoredName).ToArray()));

        var message = Assert.Single(shown);
        Assert.StartsWith("Lite could not save the server settings you confirmed to ", message, StringComparison.Ordinal);
        Assert.Equal(message, manager.HostRecordSaveProblem);
        Assert.NotNull(manager.HostChange(manager.GetServerById("a")!));

        /* A confirmation holds for this run and is reported again; nothing is written that a later start would bind from. */
        manager.ConfirmHost(manager.GetServerById("a")!);
        Assert.Equal(2, shown.Count);
        Assert.Null(manager.HostChange(manager.GetServerById("a")!));
        Assert.False(File.Exists(_bindingsPath));
    }

    [Fact]
    public void ChangeText_SaysFromAndTo_WithHiddenAndLookAlikeCharactersEscaped()
    {
        var server = Server("a", "sql01", AuthenticationTypes.Windows);
        var (bindings, _) = Confirmed(server);

        server.ServerName = "sql" + (char)0x202E + "10lqs" + (char)0x0A + (char)0x0455 + "ql01";
        var text = bindings.Check(server)!.Describe(server);

        Assert.Equal("The host name for this server changed from sql01 to sql?10lqs?\\u0455ql01. Open Edit Server and save to confirm it before Lite connects.", text);
    }

    [Fact]
    public void ConfirmationGate_AsksOnlyForAnEditedServerWithUnconfirmedSettings_AndACredentialNotTypedNow()
    {
        var server = Server("a", "old-host", AuthenticationTypes.Windows, replica: "old-r");
        var (bindings, _) = Confirmed(server);
        var windows = HostConfirmationGate.CredentialWords(AuthenticationTypes.Windows, null);

        /* Unchanged settings, Add Server, or a password typed in the dialog: nothing to ask. */
        Assert.Null(HostConfirmationGate.Question(bindings.Check(server), isEdit: true, windows, secretTypedNow: false));
        server.ServerName = "new-host";
        var change = bindings.Check(server);
        Assert.Null(HostConfirmationGate.Question(change, isEdit: false, windows, secretTypedNow: false));
        Assert.Null(HostConfirmationGate.Question(change, isEdit: true, "the saved SQL Server login", secretTypedNow: true));

        Assert.Equal("Connect to new-host (was old-host) with your Windows sign-in?",
            HostConfirmationGate.Question(change, isEdit: true, windows, secretTypedNow: false));
        Assert.Equal("Connect to new-host (was old-host) with the credential profile 'Ops'?",
            HostConfirmationGate.Question(change, isEdit: true, HostConfirmationGate.CredentialWords(AuthenticationTypes.SqlServer, "Ops"), secretTypedNow: false));

        /* The replica Test asks only when the replica itself changed. */
        Assert.Null(HostConfirmationGate.Question(change, isEdit: true, windows, secretTypedNow: false, replica: true));
        server.ServerName = "old-host";
        server.QueryStoreReplica = "new-r";
        Assert.Equal("Connect to the Query Store replica new-r (was old-r) with your Windows sign-in?",
            HostConfirmationGate.Question(bindings.Check(server), isEdit: true, windows, secretTypedNow: false, replica: true));

        /* A settings change on the same host is asked about too, and says what changed. */
        server.QueryStoreReplica = "old-r";
        server.EncryptMode = "Optional";
        Assert.Equal("Connect to old-host (was old-host) with your Windows sign-in?\n\nThe encryption for this server changed from Mandatory to Optional.",
            HostConfirmationGate.Question(bindings.Check(server), isEdit: true, windows, secretTypedNow: false));

        /* A server this user never confirmed. */
        Assert.Equal("Connect to old-host and the Query Store replica old-r (not confirmed from this Windows account) with your Windows sign-in?",
            HostConfirmationGate.Question(new ServerHostBindings(null).Check(server), isEdit: true, windows, secretTypedNow: false));
    }

    [Fact]
    public void ReplicaTest_ForAServerNeverConfirmed_AsksNothingMoreOnceTheServersOwnQuestionWasAnswered()
    {
        var server = Server("a", "sql01", AuthenticationTypes.Windows, replica: "replica01");
        var windows = HostConfirmationGate.CredentialWords(AuthenticationTypes.Windows, null);
        var never = new ServerHostBindings(null).Check(server);

        /* The server's own question is the one question, and it names the replica the user typed. */
        Assert.Equal("Connect to sql01 and the Query Store replica replica01 (not confirmed from this Windows account) with your Windows sign-in?",
            HostConfirmationGate.Question(never, isEdit: true, windows, secretTypedNow: false));

        /* Answered Yes in this dialog: the replica Test does not ask again. */
        Assert.Null(HostConfirmationGate.Question(never, isEdit: true, windows, secretTypedNow: false, replica: true, confirmedInDialog: true));

        /* Not answered: the replica is still not connected to without a question (the rule that no host is used unconfirmed holds). */
        Assert.Equal("Connect to the Query Store replica replica01 (was none) with your Windows sign-in?",
            HostConfirmationGate.Question(never, isEdit: true, windows, secretTypedNow: false, replica: true));

        /* A server this user confirmed keeps its own confirmation: an unchanged replica is not asked about. */
        var (bindings, _) = Confirmed(server);
        Assert.Null(HostConfirmationGate.Question(bindings.Check(server), isEdit: true, windows, secretTypedNow: false, replica: true));
    }

    [Fact]
    public void ReplicaTest_WithNoReplicaTyped_NeverBuildsAQuestionFromAnEmptyName()
    {
        var server = Server("a", "sql01", AuthenticationTypes.Windows);
        var windows = HostConfirmationGate.CredentialWords(AuthenticationTypes.Windows, null);

        var never = new ServerHostBindings(null).Check(server);
        Assert.Null(HostConfirmationGate.Question(never, isEdit: true, windows, secretTypedNow: false, replica: true));
        Assert.Null(HostConfirmationGate.Question(never, isEdit: true, windows, secretTypedNow: false, replica: true, confirmedInDialog: true));

        /* The server's own question for a server without a replica does not mention one. */
        Assert.Equal("Connect to sql01 (not confirmed from this Windows account) with your Windows sign-in?",
            HostConfirmationGate.Question(never, isEdit: true, windows, secretTypedNow: false));

        /* Nor for a server confirmed earlier whose host changed. */
        var (bindings, _) = Confirmed(server);
        server.ServerName = "sql02";
        Assert.Null(HostConfirmationGate.Question(bindings.Check(server), isEdit: true, windows, secretTypedNow: false, replica: true));
    }

    private sealed class OneProfile(CredentialProfile profile) : IProfileLookup
    {
        public CredentialProfile? GetProfile(string id) => id == profile.Id ? profile : null;
    }

    private static ServerConnection Server(string id, string host, string auth, string? replica = null) => new()
    {
        Id = id,
        ServerName = host,
        DisplayName = "server " + id,
        AuthenticationType = auth,
        QueryStoreReplica = replica
    };

    [Fact]
    public void Upgrade_WithNoRecord_BindsTheServersUsedToTheirCurrentSettingsOnFirstRun_InTheUserFolderNotTheSharedOne()
    {
        WriteSharedServers(
            Server("a", "sql01", AuthenticationTypes.SqlServer),
            Server("b", "sql02,1433", AuthenticationTypes.Windows));
        Assert.False(File.Exists(_bindingsPath));

        /* Building the manager binds nothing; the first-run step does. */
        Assert.NotNull(NewManager().HostChange(Server("a", "sql01", AuthenticationTypes.SqlServer)));
        Assert.False(File.Exists(_bindingsPath));
        var manager = FirstRunAsAUserWhoUsedEveryServer();

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
        FirstRunAsAUserWhoUsedEveryServer();

        WriteSharedServers(Server("a", "  sql01\\PROD,1433 ", AuthenticationTypes.Windows));
        var manager = NewManager();

        Assert.Null(manager.HostChange(manager.GetServerById("a")!));
    }

    [Fact]
    public void ChangedPort_OrInstance_IsAChangedHost()
    {
        WriteSharedServers(Server("a", "sql01,1433", AuthenticationTypes.Windows), Server("b", "sql01\\one", AuthenticationTypes.Windows));
        FirstRunAsAUserWhoUsedEveryServer();

        WriteSharedServers(Server("a", "sql01,1444", AuthenticationTypes.Windows), Server("b", "sql01\\two", AuthenticationTypes.Windows));
        var manager = NewManager();

        Assert.NotNull(manager.HostChange(manager.GetServerById("a")!));
        Assert.NotNull(manager.HostChange(manager.GetServerById("b")!));
    }

    [Fact]
    public async Task SqlLogin_ChangedHost_IsRefusedAndFlagged_AndAsksForThePasswordAgain()
    {
        WriteSharedServers(Server("a", "sql01", AuthenticationTypes.SqlServer));
        FirstRunAsAUserWhoUsedEveryServer();

        WriteSharedServers(Server("a", "sql99.example", AuthenticationTypes.SqlServer));
        var manager = NewManager();
        var server = manager.GetServerById("a")!;
        var resolver = manager.CredentialResolver;

        var refused = Assert.Throws<ServerHostNotConfirmedException>(() => resolver.GetConnectionString(server));
        Assert.Equal("The host name for this server changed from sql01 to sql99.example. Enter the password again to connect to it.", refused.Message);
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
        FirstRunAsAUserWhoUsedEveryServer();
        WriteSharedServers(Server("a", "two.database.windows.net", AuthenticationTypes.ServicePrincipal));
        var manager = NewManager();
        var server = manager.GetServerById("a")!;

        var refused = Assert.Throws<ServerHostNotConfirmedException>(() => manager.CredentialResolver.GetConnectionString(server));
        Assert.Equal("The host name for this server changed from one.database.windows.net to two.database.windows.net. Enter the client secret again to connect to it.", refused.Message);
    }

    [Fact]
    public async Task IntegratedAuth_ChangedHost_IsRefusedUntilConfirmed_ThenConnectsToTheNewHost()
    {
        WriteSharedServers(Server("a", "sql01", AuthenticationTypes.Windows));
        FirstRunAsAUserWhoUsedEveryServer();

        WriteSharedServers(Server("a", "sql77", AuthenticationTypes.Windows));
        var manager = NewManager();
        var server = manager.GetServerById("a")!;

        var refused = Assert.Throws<ServerHostNotConfirmedException>(() => manager.CredentialResolver.GetConnectionString(server));
        Assert.Equal("The host name for this server changed from sql01 to sql77. Open Edit Server and save to confirm it before Lite connects.", refused.Message);
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
        FirstRunAsAUserWhoUsedEveryServer();

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
        FirstRunAsAUserWhoUsedEveryServer();

        WriteSharedServers(
            Server("a", "sql01", AuthenticationTypes.Windows, replica: "other-r"),
            Server("b", "sql02", AuthenticationTypes.Windows, replica: null));
        var manager = NewManager();

        var change = manager.HostChange(manager.GetServerById("a")!);
        Assert.NotNull(change);
        Assert.True(change.ReplicaChanged);
        Assert.StartsWith("The Query Store replica for this server changed from sql01-r to other-r.", change.Describe(manager.GetServerById("a")!), StringComparison.Ordinal);
        Assert.Throws<ServerHostNotConfirmedException>(() => manager.CredentialResolver.GetQueryStoreReplicaConnectionString(manager.GetServerById("a")!));
        Assert.Null(manager.HostChange(manager.GetServerById("b")!));
    }

    [Fact]
    public void ServerFirstSeenAfterTheRecordExists_IsNotConnectedUntilConfirmed()
    {
        WriteSharedServers(Server("a", "sql01", AuthenticationTypes.Windows));
        FirstRunAsAUserWhoUsedEveryServer();

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
            bindings.BindOnFirstRun([server], hadLiteData: true, wasUsed: _ => true);
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
