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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9640: <c>edit_server</c> and <c>set_server_excluded_databases</c> over a temp folder and an in-memory credential store, with a fake connection
/// test, so no real servers.json, host record, credential or SQL Server is touched. Each write is read back through a fresh
/// <see cref="ServerManager"/> over the same folder, which is what the Manage Servers window reads.
/// </summary>
[InMemoryCredentials]
public sealed class McpServerAdminToolsTests : IDisposable
{
    private const string Secret = "not-a-real-secret-value";
    private const string NewSecret = "another-fake-secret-value";
    private readonly string _root = Path.Combine(Path.GetTempPath(), "McpServerAdminToolsTests_" + Guid.NewGuid().ToString("N"));
    private readonly InMemoryCredentialStore _store = new();

    public McpServerAdminToolsTests() => Directory.CreateDirectory(_root);

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); }
        catch (IOException) { /* a temp folder left behind is harmless */ }
    }

    private string HostsFile => Path.Combine(_root, "user", ServerHostBindings.FileName);

    private ServerManager Manager() => new(_root, null, HostsFile, new CredentialService(_store));

    /// <summary>A manager holding a SQL login server and a Windows server, both confirmed for this user.</summary>
    private ServerManager Seeded()
    {
        var manager = Manager();
        var sql = new ServerConnection { ServerName = "sql01", DisplayName = "Orders", AuthenticationType = AuthenticationTypes.SqlServer, EncryptMode = "Mandatory" };
        manager.AddServer(sql, "collector", Secret);
        manager.ConfirmHost(sql);
        var windows = new ServerConnection { ServerName = "sql02", DisplayName = "Billing", AuthenticationType = AuthenticationTypes.Windows, EncryptMode = "Mandatory" };
        manager.AddServer(windows);
        manager.ConfirmHost(windows);
        return manager;
    }

    private sealed class Probe
    {
        public int Calls;
        public string? Failure;
        public string? LastUser;
        public string? LastPassword;
        public string? LastHost;

        public Task<string?> RunAsync(ServerConnection server, string? user, string? password, CancellationToken token)
        {
            Calls++;
            LastUser = user;
            LastPassword = password;
            LastHost = server.ServerName;
            return Task.FromResult(Failure);
        }
    }

    private static Task<string> Edit(ServerManager manager, string server, string changes, Probe probe) =>
        McpServerAdminTools.EditAsync(manager, server, changes, probe.RunAsync, CancellationToken.None);

    private static JsonObject Parse(string answer) => JsonNode.Parse(answer)!.AsObject();

    private static string Status(string answer) => Parse(answer)["status"]!.GetValue<string>();

    private ServerConnection Reread(string name) => Manager().GetAllServers().Single(s => s.DisplayName == name);

    [Fact]
    public async Task Edit_DisplayNameAndCost_IsSavedThroughTheWindowsStore_WithNoProbe()
    {
        var manager = Seeded();
        var probe = new Probe();

        var answer = await Edit(manager, "Orders", "{\"display_name\":\"Orders DB\",\"monthly_cost_usd\":120.5,\"description\":\"main\"}", probe);

        var result = Parse(answer);
        Assert.Equal("updated", result["status"]!.GetValue<string>());
        Assert.Equal(0, probe.Calls);
        var changed = result["changed"]!.AsArray().Select(n => n!.GetValue<string>()).ToArray();
        Assert.Equal(new[] { "display_name", "description", "monthly_cost_usd" }.OrderBy(x => x), changed.OrderBy(x => x));
        var stored = Reread("Orders DB");
        Assert.Equal(120.5m, stored.MonthlyCostUsd);
        Assert.Equal("main", stored.Description);
        /* The login and its password are untouched. */
        Assert.Equal(("collector", Secret), new CredentialService(_store).GetCredential(stored.Id)!.Value);
        /* The undo is the call that sets the earlier values back. */
        var undo = result["undo"]!["arguments"]!;
        Assert.Equal("sql01", undo["server_name"]!.GetValue<string>());
        var back = Parse(await Edit(manager, undo["server_name"]!.GetValue<string>(), undo["changes_json"]!.GetValue<string>(), probe));
        Assert.Equal("updated", back["status"]!.GetValue<string>());
        Assert.Equal("Orders", Reread("Orders").DisplayName);
    }

    [Fact]
    public async Task Edit_AnAnswerNeverCarriesAPassword()
    {
        var manager = Seeded();
        var probe = new Probe();

        var answer = await Edit(manager, "Orders", $"{{\"password\":\"{NewSecret}\"}}", probe);

        Assert.Equal("updated", Status(answer));
        Assert.DoesNotContain(NewSecret, answer, StringComparison.Ordinal);
        Assert.DoesNotContain(Secret, answer, StringComparison.Ordinal);
        Assert.True(Parse(answer)["password_changed"]!.GetValue<bool>());
        Assert.Equal(("collector", NewSecret), new CredentialService(_store).GetCredential(Reread("Orders").Id)!.Value);
        /* A password the call sent is the one the connection test used. */
        Assert.Equal(1, probe.Calls);
        Assert.Equal(NewSecret, probe.LastPassword);
    }

    [Fact]
    public async Task Edit_AChangedHost_IsTriedFirstWithTheStoredLogin_AndSavedWhenTheServerAnswers()
    {
        var manager = Seeded();
        var probe = new Probe();

        var answer = await Edit(manager, "Orders", "{\"host\":\"sql09\",\"password\":\"" + NewSecret + "\"}", probe);

        Assert.Equal("updated", Status(answer));
        Assert.True(Parse(answer)["tested"]!.GetValue<bool>());
        Assert.Equal("sql09", probe.LastHost);
        Assert.Equal("collector", probe.LastUser);
        Assert.Equal(NewSecret, probe.LastPassword);
        Assert.Equal("sql09", Reread("Orders").ServerName);
    }

    [Fact]
    public async Task Edit_AConnectionThatFails_SavesNothing()
    {
        var manager = Seeded();
        var probe = new Probe { Failure = "Login failed for user 'collector'." };

        var answer = await Edit(manager, "Orders", "{\"read_only_intent\":true}", probe);

        Assert.Equal("connection_failed", Status(answer));
        Assert.Equal(1, probe.Calls);
        /* The stored login is what was tried. */
        Assert.Equal(Secret, probe.LastPassword);
        Assert.False(Reread("Orders").ReadOnlyIntent);
        Assert.Equal(("collector", Secret), new CredentialService(_store).GetCredential(Reread("Orders").Id)!.Value);
    }

    [Fact]
    public async Task Edit_ACollectionOffServer_IsSavedWithoutAProbe()
    {
        var manager = Seeded();
        var probe = new Probe { Failure = "down" };

        var answer = await Edit(manager, "Billing", "{\"description\":\"retired\",\"is_enabled\":false}", probe);

        Assert.Equal("updated", Status(answer));
        Assert.Equal(0, probe.Calls);
        var stored = Reread("Billing");
        Assert.Equal("retired", stored.Description);
        Assert.False(stored.IsEnabled);
    }

    [Fact]
    public async Task Edit_SettingsThisUserHasNotConfirmed_AreRefusedWithTheWindowsQuestion_AndNothingIsSaved()
    {
        var manager = Seeded();
        var probe = new Probe();

        /* Moving the server to another host without sending the password: the stored one must not go to a place nobody confirmed. */
        var answer = await Edit(manager, "Orders", "{\"host\":\"sql09\"}", probe);

        Assert.Equal("refused", Status(answer));
        Assert.Contains("Edit Server window", Parse(answer)["message"]!.GetValue<string>(), StringComparison.Ordinal);
        Assert.Equal(0, probe.Calls);
        Assert.Equal("sql01", Reread("Orders").ServerName);
    }

    [Theory]
    [InlineData("{\"engine\":\"postgresql\"}", "engine")]
    [InlineData("{\"excluded_databases\":[\"a\"]}", "excluded_databases")]
    [InlineData("{\"auth\":\"kerberos\"}", "auth")]
    [InlineData("{\"auth\":\"EntraMFA\"}", "auth")]
    [InlineData("{\"encrypt_mode\":\"Sometimes\"}", "encrypt_mode")]
    [InlineData("{\"host\":\"np:sql01\"}", "host")]
    [InlineData("{\"host\":\"\"}", "host")]
    [InlineData("{\"monthly_cost_usd\":-5}", "monthly_cost_usd")]
    [InlineData("{\"monthly_cost_usd\":\"lots\"}", "monthly_cost_usd")]
    [InlineData("{\"trust_server_certificate\":\"yes\"}", "trust_server_certificate")]
    [InlineData("{\"alert_delivery_mode\":\"daily\"}", "alert_delivery_mode")]
    [InlineData("{\"query_store_replica\":\"np:replica\"}", "query_store_replica")]
    [InlineData("{\"password\":\"x\",\"auth\":\"windows\"}", "password")]
    [InlineData("{}", "changes_json")]
    [InlineData("[1]", "changes_json")]
    [InlineData("not json", "changes_json")]
    public async Task Edit_AnInvalidChange_IsRefusedNamingTheArgument_AndSavesNothing(string changes, string parameter)
    {
        var manager = Seeded();
        var probe = new Probe();

        var answer = await Edit(manager, "Orders", changes, probe);

        var result = Parse(answer);
        Assert.Equal("invalid", result["status"]!.GetValue<string>());
        Assert.Equal(parameter, result["hints"]!["parameter"]!.GetValue<string>());
        Assert.Equal(0, probe.Calls);
        var stored = Reread("Orders");
        Assert.Equal("sql01", stored.ServerName);
        Assert.Equal(0m, stored.MonthlyCostUsd);
    }

    [Fact]
    public async Task Edit_SqlAuthWithNoStoredPassword_NeedsOne()
    {
        var manager = Seeded();

        var answer = await Edit(manager, "Billing", "{\"auth\":\"sql\",\"username\":\"collector\"}", new Probe());

        var result = Parse(answer);
        Assert.Equal("invalid", result["status"]!.GetValue<string>());
        Assert.Equal("password", result["hints"]!["parameter"]!.GetValue<string>());
        Assert.Equal(AuthenticationTypes.Windows, Reread("Billing").AuthenticationType);
    }

    [Fact]
    public async Task Edit_AServerThatWouldTakeAnotherServersId_IsRefusedAsCollides()
    {
        var manager = Seeded();

        var answer = await Edit(manager, "Billing", "{\"host\":\"sql01\",\"is_enabled\":false}", new Probe());

        Assert.Equal("collides", Status(answer));
        Assert.Equal("sql02", Reread("Billing").ServerName);
    }

    [Fact]
    public async Task Edit_TheSameValues_AnswersUnchanged_AndAnUnknownServerIsNotFound()
    {
        var manager = Seeded();

        Assert.Equal("unchanged", Status(await Edit(manager, "Orders", "{\"display_name\":\"Orders\"}", new Probe())));
        Assert.Equal("invalid", Status(await Edit(manager, "nope", "{\"display_name\":\"x\"}", new Probe())));
        Assert.Equal("invalid", Status(await Edit(manager, "", "{\"display_name\":\"x\"}", new Probe())));
    }

    [Fact]
    public void SetExcluded_ReplacesTheList_AndTheWindowsStoreReadsItBack()
    {
        var manager = Seeded();

        var answer = McpServerAdminTools.SetExcluded(manager, "Orders", new[] { "Archive_*", "tempdb_x", "archive_*", " " });

        var result = Parse(answer);
        Assert.Equal("changed", result["status"]!.GetValue<string>());
        Assert.Equal(new[] { "Archive_*", "tempdb_x" }, Reread("Orders").ExcludedDatabases);
        var undo = result["undo"]!["arguments"]!;
        Assert.Empty(undo["databases"]!.AsArray());

        Assert.Equal("unchanged", Status(McpServerAdminTools.SetExcluded(manager, "Orders", new[] { "Archive_*", "tempdb_x" })));
        Assert.Equal("changed", Status(McpServerAdminTools.SetExcluded(manager, "Orders", Array.Empty<string>())));
        Assert.Empty(Reread("Orders").ExcludedDatabases);
    }

    /* #9640: a tool call runs off the UI thread, while the Edit Server and Manage Servers windows read and save the same live
       ServerConnection objects. A host with a window hands the change over to its UI thread; these pin that the field change and its
       save run inside that hook (nothing is written before it, and the saved file has the change when it returns), and that a
       failure in the hook leaves the server as it was. */
    [Fact]
    public async Task Edit_AndSetExcluded_RunTheirChangeAndSaveOnTheHostsUiThread()
    {
        var manager = Seeded();
        var runs = 0;
        string? displayNameBefore = null;
        var hooks = new McpServerAdminHooks
        {
            OnUiThread = work =>
            {
                runs++;
                displayNameBefore = Reread("Orders").DisplayName;
                work();
            },
        };

        var edited = await McpServerAdminTools.EditAsync(manager, "Orders", "{\"description\":\"main\"}", new Probe().RunAsync, CancellationToken.None, hooks);
        Assert.Equal("updated", Status(edited));
        Assert.Equal(1, runs);
        Assert.Equal("main", Reread("Orders").Description);

        var excluded = McpServerAdminTools.SetExcluded(manager, "Orders", new[] { "Archive_*" }, hooks);
        Assert.Equal("changed", Status(excluded));
        Assert.Equal(2, runs);
        Assert.Equal(new[] { "Archive_*" }, Reread("Orders").ExcludedDatabases);
        Assert.Equal("Orders", displayNameBefore);
    }

    [Fact]
    public void ASaveThatThrowsOnTheUiThread_LeavesTheStoreAsItWas()
    {
        var manager = Seeded();
        var hooks = new McpServerAdminHooks { OnUiThread = _ => throw new InvalidOperationException("the window is closing") };

        Assert.Throws<InvalidOperationException>(() => McpServerAdminTools.SetExcluded(manager, "Orders", new[] { "Archive_*" }, hooks));

        Assert.True(Reread("Orders").ExcludedDatabases is null or { Count: 0 });
    }

    [Theory]
    [InlineData("*")]
    [InlineData("***")]
    [InlineData("bad\u0001name")]
    public void SetExcluded_AnEntryTheWindowCouldNotHold_IsRefused_AndTheListIsKept(string entry)
    {
        var manager = Seeded();
        McpServerAdminTools.SetExcluded(manager, "Orders", new[] { "keep_me" });

        var answer = McpServerAdminTools.SetExcluded(manager, "Orders", new[] { entry });

        var result = Parse(answer);
        Assert.Equal("invalid", result["status"]!.GetValue<string>());
        Assert.Equal("databases", result["hints"]!["parameter"]!.GetValue<string>());
        Assert.Equal(new[] { "keep_me" }, Reread("Orders").ExcludedDatabases);
    }

    [Fact]
    public void SetExcluded_TooManyOrTooLongEntries_AreRefused_AndNoListIsAnError()
    {
        var manager = Seeded();

        Assert.Equal("invalid", Status(McpServerAdminTools.SetExcluded(manager, "Orders", Enumerable.Range(0, 257).Select(i => "db" + i).ToArray())));
        Assert.Equal("invalid", Status(McpServerAdminTools.SetExcluded(manager, "Orders", new[] { new string('x', 129) })));
        Assert.Equal("invalid", Status(McpServerAdminTools.SetExcluded(manager, "Orders", null)));
        Assert.Equal("invalid", Status(McpServerAdminTools.SetExcluded(manager, "nope", new[] { "a" })));
    }

    [Fact]
    public void ServerAdminTools_AreServedOnlyWithBothSwitches_AndTheExcludedListWithTheWritesAlone()
    {
        var all = new McpServerPrimitiveCollection<McpServerTool>();
        foreach (var tool in McpToolsListBudgetTests.BuildServedTools().Tools)
        {
            all.Add(tool);
        }

        string[] Served(bool writes, bool admin) =>
            McpWritePolicy.FilterToolCollection(all, writes, admin).Select(t => t.ProtocolTool.Name).ToArray();

        Assert.DoesNotContain("edit_server", Served(false, false));
        Assert.DoesNotContain("edit_server", Served(false, true));
        Assert.DoesNotContain("edit_server", Served(true, false));
        Assert.Contains("edit_server", Served(true, true));
        Assert.DoesNotContain("set_server_excluded_databases", Served(false, true));
        Assert.Contains("set_server_excluded_databases", Served(true, false));
        Assert.Contains("set_server_excluded_databases", Served(true, true));
    }
}
