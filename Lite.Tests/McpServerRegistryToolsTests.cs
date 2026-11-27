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
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9640: <c>add_servers</c> and <c>remove_server</c>, Lite's twins of Darling's, and the <c>mcp_allow_server_admin</c> switch that serves them. The
/// tools run over a real <see cref="ServerManager"/> on a temp folder and an in-memory credential store, and each result is read back through the
/// server list and the credential store the Add Server and Manage Servers windows use.
/// </summary>
[InMemoryCredentials]
public sealed class McpServerRegistryToolsTests : IDisposable
{
    private const string Secret = "not-a-real-secret-value";
    private readonly string _root = Path.Combine(Path.GetTempPath(), "McpServerRegistryToolsTests_" + Guid.NewGuid().ToString("N"));
    private readonly InMemoryCredentialStore _store = new();

    public McpServerRegistryToolsTests() => Directory.CreateDirectory(_root);

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); }
        catch (IOException) { /* a temp folder left behind is harmless */ }
    }

    private string HostsFile => Path.Combine(_root, "user", ServerHostBindings.FileName);

    private ServerManager NewManager() => new(_root, null, HostsFile, new CredentialService(_store));

    private sealed class Hooks
    {
        public int Changed;
        public List<string> Removed = [];
        public McpServerAdminHooks For(ServerManager manager) => new()
        {
            ServersChanged = () => Changed++,
            RemoveServer = server =>
            {
                Removed.Add(server.DisplayName);
                manager.DeleteServer(server.Id);
                return Task.FromResult(true);
            },
        };
    }

    private static Task<string?> Reachable(ServerConnection server, string? user, string? password, System.Threading.CancellationToken ct) => Task.FromResult<string?>(null);

    private static Func<ServerConnection, string?, string?, System.Threading.CancellationToken, Task<string?>> Unreachable(string failure) =>
        (_, _, _, _) => Task.FromResult<string?>(failure);

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement.Clone();

    private const string OneSqlServer =
        "[{\"host\":\"sql01\",\"display_name\":\"Primary\",\"auth\":\"sql\",\"username\":\"monitor\",\"password\":\"" + Secret + "\",\"encrypt_mode\":\"Optional\"}]";

    [Fact]
    public async Task AddServers_SavesTheServerAndItsLogin_WhereTheWindowsReadThem_AndRefreshesTheList()
    {
        var manager = NewManager();
        var hooks = new Hooks();

        var answer = await McpServerRegistryTools.AddServersCore(manager, hooks.For(manager), OneSqlServer, Reachable);

        var root = Parse(answer);
        Assert.Equal("ok", root.GetProperty("status").GetString());
        Assert.Equal(1, root.GetProperty("added").GetInt32());
        Assert.DoesNotContain(Secret, answer, StringComparison.Ordinal);
        Assert.Equal(1, hooks.Changed);

        /* Read back through a second manager over the same folder: what a restart of Lite would load. */
        var reloaded = NewManager();
        var saved = Assert.Single(reloaded.GetAllServers());
        Assert.Equal("sql01", saved.ServerName);
        Assert.Equal("Primary", saved.DisplayName);
        Assert.Equal("Optional", saved.EncryptMode);
        Assert.Equal(AuthenticationTypes.SqlServer, saved.AuthenticationType);
        Assert.Equal(CredentialReadOutcome.Found, reloaded.CredentialService.TryReadCredential(saved.Id, out var credential, out _));
        Assert.Equal(("monitor", Secret), credential!.Value);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("  ")]
    [InlineData("{\"host\":\"sql01\"}")]
    [InlineData("[]")]
    [InlineData("[{\"display_name\":\"no host\"}]")]
    [InlineData("[{\"host\":\"sql01\",\"auth\":\"kerberos\"}]")]
    [InlineData("[{\"host\":\"sql01\",\"auth\":\"sql\",\"username\":\"monitor\"}]")]
    [InlineData("[{\"host\":\"sql01\",\"encrypt_mode\":\"sometimes\"}]")]
    [InlineData("[{\"host\":\"sql01\",\"engine\":\"postgres\"}]")]
    [InlineData("[{\"host\":\"sql01\",\"trust_server_certificate\":\"yes\"}]")]
    [InlineData("not json")]
    public async Task AddServers_Refuses_AndSavesNothing_WhenTheInputBreaksARule(string? serversJson)
    {
        var manager = NewManager();
        var hooks = new Hooks();

        var answer = await McpServerRegistryTools.AddServersCore(manager, hooks.For(manager), serversJson, Reachable);

        var root = Parse(answer);
        Assert.NotEqual("ok", root.GetProperty("status").GetString());
        Assert.Empty(manager.GetAllServers());
        Assert.Equal(0, hooks.Changed);
        Assert.DoesNotContain("stdin", answer, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AddServers_AnUnreachableEntry_RefusesTheWholeCall_AndSavesNone()
    {
        var manager = NewManager();
        var json = "[{\"host\":\"sql01\"},{\"host\":\"sql02\",\"auth\":\"sql\",\"username\":\"m\",\"password\":\"" + Secret + "\"}]";
        var tested = new List<string>();

        var answer = await McpServerRegistryTools.AddServersCore(manager, null, json, (server, _, _, _) =>
        {
            tested.Add(server.ServerName);
            return Task.FromResult<string?>(server.ServerName == "sql02" ? "login failed" : null);
        });

        var root = Parse(answer);
        Assert.Equal("refused", root.GetProperty("status").GetString());
        Assert.Contains("sql02", root.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Contains("login failed", root.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.DoesNotContain(Secret, answer, StringComparison.Ordinal);
        Assert.Equal(["sql01", "sql02"], tested);
        Assert.Empty(NewManager().GetAllServers());
    }

    [Fact]
    public async Task AddServers_AListedServer_IsUpdatedInPlace_AndAFailedTestLeavesTheRunningListUntouched()
    {
        var manager = NewManager();
        await McpServerRegistryTools.AddServersCore(manager, null, OneSqlServer, Reachable);
        var listed = Assert.Single(manager.GetAllServers());
        var id = listed.Id;

        /* The test fails: the instance the running app holds must keep every setting it had, and the list file too. */
        var refused = await McpServerRegistryTools.AddServersCore(manager, null,
            "[{\"host\":\"sql01\",\"display_name\":\"Renamed\",\"auth\":\"windows\",\"encrypt_mode\":\"Strict\"}]", Unreachable("no route to host"));
        Assert.Equal("refused", Parse(refused).GetProperty("status").GetString());
        Assert.Equal("Primary", listed.DisplayName);
        Assert.Equal("Optional", listed.EncryptMode);
        Assert.Equal(AuthenticationTypes.SqlServer, listed.AuthenticationType);
        Assert.Same(listed, Assert.Single(manager.GetAllServers()));

        var updated = await McpServerRegistryTools.AddServersCore(manager, null,
            "[{\"host\":\"sql01\",\"display_name\":\"Renamed\",\"auth\":\"sql\",\"username\":\"monitor\",\"password\":\"another-value\",\"encrypt_mode\":\"Strict\"}]", Reachable);
        var root = Parse(updated);
        Assert.Equal(1, root.GetProperty("updated").GetInt32());
        Assert.Equal(0, root.GetProperty("added").GetInt32());
        var reloaded = Assert.Single(NewManager().GetAllServers());
        Assert.Equal(id, reloaded.Id);
        Assert.Equal("Renamed", reloaded.DisplayName);
        Assert.Equal("Strict", reloaded.EncryptMode);
        Assert.Equal(CredentialReadOutcome.Found, manager.CredentialService.TryReadCredential(id, out var credential, out _));
        Assert.Equal("another-value", credential!.Value.Password);
    }

    /// <summary>A store that refuses the save for one login, and deletes the old entry first: the worst a refused update can do.</summary>
    private sealed class RefusingStore(InMemoryCredentialStore inner, string username) : ICredentialStore
    {
        public bool SaveCredential(string serverId, string user, string password) => user == username ? Refuse(serverId) : inner.SaveCredential(serverId, user, password);
        public CredentialSaveResult TrySaveCredential(string serverId, string user, string password) => inner.TrySaveCredential(serverId, user, password);
        public CredentialSaveResult TryDeleteCredential(string serverId) => inner.TryDeleteCredential(serverId);
        public (string Username, string Password)? GetCredential(string serverId) => inner.GetCredential(serverId);
        public CredentialReadOutcome TryReadCredential(string serverId, out (string Username, string Password)? credential, out string? failure)
            => inner.TryReadCredential(serverId, out credential, out failure);
        public bool DeleteCredential(string serverId) => inner.DeleteCredential(serverId);
        public bool CredentialExists(string serverId) => inner.CredentialExists(serverId);
        public bool UpdateCredential(string serverId, string user, string password)
            => user == username ? Refuse(serverId) : inner.UpdateCredential(serverId, user, password);
        public IReadOnlyList<string> ListKeys(string keyPrefix) => inner.ListKeys(keyPrefix);

        private bool Refuse(string serverId)
        {
            inner.DeleteCredential(serverId);
            return false;
        }
    }

    [Fact]
    public async Task AddServers_AFailedUpdate_ChangesNothing_WhileTheEntryBeforeItStaysSaved_AndTheAnswerNamesIt()
    {
        var manager = new ServerManager(_root, null, HostsFile, new CredentialService(new RefusingStore(_store, "monitor2")));
        await McpServerRegistryTools.AddServersCore(manager, null, OneSqlServer, Reachable);
        var listed = Assert.Single(manager.GetAllServers());
        var id = listed.Id;

        /* The second entry updates the listed server and its login save is refused, after the list has been written with the update. */
        var answer = await McpServerRegistryTools.AddServersCore(manager, null,
            "[{\"host\":\"sql02\",\"display_name\":\"Second\",\"auth\":\"windows\"},"
            + "{\"host\":\"sql01\",\"display_name\":\"Renamed\",\"auth\":\"sql\",\"username\":\"monitor2\",\"password\":\"another-value\",\"encrypt_mode\":\"Strict\"}]",
            Reachable);

        var root = Parse(answer);
        Assert.Equal("refused", root.GetProperty("status").GetString());
        Assert.Equal(1, root.GetProperty("added").GetInt32());
        Assert.Equal(0, root.GetProperty("updated").GetInt32());
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("stay saved", message, StringComparison.Ordinal);
        Assert.Contains("Second", message, StringComparison.Ordinal);

        /* The failed update left the listed server as it was, in memory and in the file, and its login as it was. */
        Assert.Equal(2, manager.GetAllServers().Count);
        var live = manager.GetServerById(id)!;
        Assert.Same(listed, live);
        Assert.Equal("Primary", live.DisplayName);
        Assert.Equal("Optional", live.EncryptMode);
        var reloaded = NewManager().GetServerById(id)!;
        Assert.Equal("Primary", reloaded.DisplayName);
        Assert.Equal("Optional", reloaded.EncryptMode);
        Assert.Equal(("monitor", Secret), _store.GetCredential(id));
        Assert.DoesNotContain(Secret, answer, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AddServers_WhenOnlyTheHostRecordWriteFails_SaysEveryEntryStaysSaved_NotThatOneFailed()
    {
        var manager = NewManager();
        /* A folder where the record file goes: the write fails the way a locked or read-only file does, after every entry has saved. */
        Directory.CreateDirectory(HostsFile);

        var answer = await McpServerRegistryTools.AddServersCore(manager, null,
            "[{\"host\":\"sql01\",\"display_name\":\"First\",\"auth\":\"windows\"},{\"host\":\"sql02\",\"display_name\":\"Second\",\"auth\":\"windows\"}]",
            Reachable);

        var root = Parse(answer);
        Assert.Equal("refused", root.GetProperty("status").GetString());
        Assert.Equal(2, root.GetProperty("added").GetInt32());
        Assert.Equal(2, manager.GetAllServers().Count);
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("Every entry stays saved", message, StringComparison.Ordinal);
        Assert.Contains("First (sql01)", message, StringComparison.Ordinal);
        Assert.Contains("Second (sql02)", message, StringComparison.Ordinal);
        Assert.Contains("host record could not be written", message, StringComparison.Ordinal);
        Assert.DoesNotContain("before this one", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RemoveServer_RunsTheWindowsRemoval_DropsTheLogin_AndAnswersWithAnUndoThatHoldsNoPassword()
    {
        var manager = NewManager();
        await McpServerRegistryTools.AddServersCore(manager, null, OneSqlServer, Reachable);
        var id = Assert.Single(manager.GetAllServers()).Id;
        var hooks = new Hooks();

        var answer = await McpServerRegistryTools.RemoveServerCore(manager, hooks.For(manager), "primary");

        var root = Parse(answer);
        Assert.Equal("removed", root.GetProperty("status").GetString());
        Assert.Equal("exact", root.GetProperty("matched_by").GetString());
        Assert.Equal(["Primary"], hooks.Removed);
        Assert.Empty(NewManager().GetAllServers());
        Assert.NotEqual(CredentialReadOutcome.Found, manager.CredentialService.TryReadCredential(id, out _, out _));
        Assert.DoesNotContain(Secret, answer, StringComparison.Ordinal);
        var previous = root.GetProperty("previous");
        Assert.Equal("sql01", previous.GetProperty("host").GetString());
        Assert.Equal("monitor", previous.GetProperty("username").GetString());
        Assert.Equal("<password>", previous.GetProperty("password").GetString());
        Assert.DoesNotContain(Secret, root.GetProperty("undo").GetString()!, StringComparison.Ordinal);
        Assert.Contains("password", root.GetProperty("undo").GetString()!, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(AuthenticationTypes.EntraMFA, true)]
    [InlineData(AuthenticationTypes.EntraDeviceCode, true)]
    [InlineData(AuthenticationTypes.Windows, false)]
    public async Task RemoveServer_SaysTheUndoCannotBeReplayed_ForAServerThatSignsInWithEntraMfaOrDeviceCode(string auth, bool cannotReplay)
    {
        var manager = NewManager();
        manager.AddServer(new ServerConnection { ServerName = "sql07", DisplayName = "Interactive", AuthenticationType = auth });
        var hooks = new Hooks();

        var answer = await McpServerRegistryTools.RemoveServerCore(manager, hooks.For(manager), "Interactive");

        var note = Parse(answer).GetProperty("undo_note").GetString()!;
        Assert.Equal(cannotReplay, note.Contains("cannot be replayed", StringComparison.Ordinal));
    }

    [Fact]
    public async Task RemoveServer_RemovesNothing_WhenTheNameIsAmbiguousUnknownOrMissing()
    {
        var manager = NewManager();
        await McpServerRegistryTools.AddServersCore(manager, null, "[{\"host\":\"sql01a\"},{\"host\":\"sql01b\"}]", Reachable);
        var hooks = new Hooks();

        var ambiguous = Parse(await McpServerRegistryTools.RemoveServerCore(manager, hooks.For(manager), "sql01"));
        Assert.Equal("ambiguous", ambiguous.GetProperty("status").GetString());
        Assert.Equal(2, ambiguous.GetProperty("candidates").GetArrayLength());

        var unknown = Parse(await McpServerRegistryTools.RemoveServerCore(manager, hooks.For(manager), "nowhere"));
        Assert.Equal("not_found", unknown.GetProperty("status").GetString());

        var blank = Parse(await McpServerRegistryTools.RemoveServerCore(manager, hooks.For(manager), " "));
        Assert.Contains("server_name", blank.GetProperty("message").GetString(), StringComparison.Ordinal);

        var noRemoval = Parse(await McpServerRegistryTools.RemoveServerCore(manager, new McpServerAdminHooks(), "sql01a"));
        Assert.Equal("unavailable", noRemoval.GetProperty("status").GetString());

        Assert.Empty(hooks.Removed);
        Assert.Equal(2, NewManager().GetAllServers().Count);

        /* A full storage name picks one registration even when a part of its name fits both. */
        var exact = Parse(await McpServerRegistryTools.RemoveServerCore(manager, hooks.For(manager), "sql01a"));
        Assert.Equal("removed", exact.GetProperty("status").GetString());
        Assert.Equal("sql01b", Assert.Single(NewManager().GetAllServers()).ServerName);
    }

    /// <summary>A manager over a servers.json written as given, which is how a pair saved before the duplicate check, or a hand-edited Id, reaches Lite.</summary>
    private ServerManager Seeded(params ServerConnection[] servers)
    {
        File.WriteAllText(Path.Combine(_root, "servers.json"), JsonSerializer.Serialize(new { Servers = servers }));
        return NewManager();
    }

    private static ServerConnection Entry(string id, string host, string name) =>
        new() { Id = id, ServerName = host, DisplayName = name, AuthenticationType = AuthenticationTypes.Windows };

    [Fact]
    public async Task RemoveServer_ReadsNoSavedLogin_ForAServerWhoseIdIsNotAGuid()
    {
        const string LegacyUser = "svc_legacy_login";
        var entry = Entry("legacy-id", "sql01", "Legacy");
        entry.AuthenticationType = AuthenticationTypes.SqlServer;
        var manager = Seeded(entry);
        manager.CredentialService.SaveCredential("legacy-id", LegacyUser, Secret);
        var hooks = new Hooks();

        var answer = await McpServerRegistryTools.RemoveServerCore(manager, hooks.For(manager), "Legacy");

        var root = Parse(answer);
        Assert.Equal("removed", root.GetProperty("status").GetString());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("previous").GetProperty("username").ValueKind);
        Assert.DoesNotContain(LegacyUser, answer, StringComparison.Ordinal);
        Assert.DoesNotContain(Secret, answer, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RemoveServer_RemovesNothing_WhenTwoEntriesShareAStorageName()
    {
        var manager = Seeded(Entry(Guid.NewGuid().ToString(), "sql01", "First"), Entry(Guid.NewGuid().ToString(), "sql01", "Second"));
        var hooks = new Hooks();

        var answer = Parse(await McpServerRegistryTools.RemoveServerCore(manager, hooks.For(manager), "sql01"));

        Assert.Equal("invalid", answer.GetProperty("status").GetString());
        Assert.Contains("'sql01'", answer.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Contains("Lite window", answer.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Empty(hooks.Removed);
        Assert.Equal(2, NewManager().GetAllServers().Count);
    }

    [Fact]
    public async Task RemoveServer_RemovesNothing_WhenTwoEntriesShareOneId()
    {
        var id = Guid.NewGuid().ToString();
        var manager = Seeded(Entry(id, "sql01", "First"), Entry(id, "sql02", "Second"));
        var hooks = new Hooks();

        var answer = Parse(await McpServerRegistryTools.RemoveServerCore(manager, hooks.For(manager), "sql01"));

        Assert.Equal("invalid", answer.GetProperty("status").GetString());
        Assert.Contains("shares the id", answer.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Empty(hooks.Removed);
        Assert.Equal(2, NewManager().GetAllServers().Count);
    }

    [Fact]
    public async Task RemoveServer_RemovesNothing_OnALonePartialMatch_AndAsksForTheFullName()
    {
        var manager = NewManager();
        await McpServerRegistryTools.AddServersCore(manager, null, "[{\"host\":\"sql01a\"},{\"host\":\"other02\"}]", Reachable);
        var hooks = new Hooks();

        var answer = Parse(await McpServerRegistryTools.RemoveServerCore(manager, hooks.For(manager), "sql01"));

        Assert.Equal("partial_match", answer.GetProperty("status").GetString());
        Assert.Equal("partial", answer.GetProperty("matched_by").GetString());
        Assert.Contains("full name", answer.GetProperty("message").GetString(), StringComparison.Ordinal);
        var candidate = Assert.Single(answer.GetProperty("candidates").EnumerateArray());
        Assert.Empty(hooks.Removed);
        Assert.Equal(2, NewManager().GetAllServers().Count);

        var again = Parse(await McpServerRegistryTools.RemoveServerCore(manager, hooks.For(manager), candidate.GetProperty("server").GetString()));
        Assert.Equal("removed", again.GetProperty("status").GetString());
        Assert.Equal("other02", Assert.Single(NewManager().GetAllServers()).ServerName);
    }

    [Theory]
    [InlineData(false, false, false, false)]
    [InlineData(false, true, false, false)]
    [InlineData(true, false, true, false)]
    [InlineData(true, true, true, true)]
    public void TheServerAdminTools_AreServedOnlyWithBothSwitches(bool writes, bool admin, bool otherWritesServed, bool adminServed)
    {
        var all = McpWritePolicyTests.RegisteredTools();
        var names = McpWritePolicy.ToolNames(McpWritePolicy.FilterToolCollection(all, writes, admin));

        Assert.Equal(otherWritesServed, names.Contains("mute_analysis_finding"));
        foreach (var tool in McpWritePolicy.ServerAdminTools)
        {
            Assert.Equal(adminServed, names.Contains(tool));
        }

        var applied = McpWritePolicyTests.RegisteredTools();
        McpWritePolicy.Apply(applied, writes, admin);
        Assert.Equal(names.Order(StringComparer.Ordinal), McpWritePolicy.ToolNames(applied).Order(StringComparer.Ordinal));
    }

    [Theory]
    [InlineData(true, true, true)]
    [InlineData(false, true, true)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    public void DecideServing_ServesTheServerAdminTools_OnlyWithWritesAndAUsableToken(bool writes, bool admin, bool usableToken)
    {
        var token = usableToken ? "abcdefghijklmnopqrstuvwxyz0123456789ABCDEF" : "short";
        var serving = McpHostService.DecideServing(token, allowTokenlessLoopback: false, allowWrites: writes, allowInvestigations: false, allowServerAdmin: admin);

        Assert.Equal(writes && admin && usableToken, serving.ServerAdmin);
        Assert.Equal(admin, serving.AskedServerAdmin);

        var lines = McpHostService.StartupLines(serving);
        Assert.Equal(serving.ServerAdmin, lines.Any(l => l.Level == LogLevel.Warning && l.Message.Contains(McpWritePolicy.ServerAdminSetting, StringComparison.Ordinal)));
        Assert.Equal(admin && !serving.ServerAdmin, lines.Any(l => l.Level == LogLevel.Critical && l.Message.Contains(McpWritePolicy.ServerAdminSetting, StringComparison.Ordinal)));
    }

    [Fact]
    public void TheSetting_DefaultsOff_IsReadFromSettingsJson_AndAWrongKindIsAProblem()
    {
        Assert.False(new McpSettings().AllowServerAdmin);
        Assert.Equal("mcp_allow_server_admin", McpWritePolicy.ServerAdminSetting);
        Assert.False(Load(null).AllowServerAdmin);
        Assert.False(Load("{ \"mcp_enabled\": true }").AllowServerAdmin);
        Assert.True(Load("{ \"mcp_enabled\": true, \"mcp_allow_server_admin\": true }").AllowServerAdmin);

        var quoted = Load("{ \"mcp_enabled\": true, \"mcp_allow_server_admin\": \"true\" }");
        Assert.False(quoted.Enabled);
        Assert.False(quoted.AllowServerAdmin);
        Assert.Contains("mcp_allow_server_admin", quoted.Problem!, StringComparison.Ordinal);

        Assert.Contains("mcp_allow_server_admin", SettingsImport.McpGateKeys);
    }

    private McpSettings Load(string? content)
    {
        var dir = Path.Combine(_root, "settings_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        if (content is not null)
        {
            File.WriteAllText(Path.Combine(dir, "settings.json"), content);
        }

        return McpSettings.Load(dir);
    }
}
