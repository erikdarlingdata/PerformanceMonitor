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
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9640: <c>add_servers</c> and <c>edit_server</c> refuse Windows authentication over a connection whose server identity is not verified, before any
/// connection test, unless <c>mcp_allow_windows_auth_over_unverified_tls</c> is on. Darling's twin is <c>DarlingUnverifiedWindowsAuthScreen</c>. Each
/// test counts the connection tests the tool ran, because the point of the rule is that Windows credentials never reach the host.
/// </summary>
[InMemoryCredentials]
public sealed class McpUnverifiedWindowsAuthTests : IDisposable
{
    private const string Secret = "not-a-real-secret-value";
    private readonly string _root = Path.Combine(Path.GetTempPath(), "McpUnverifiedWindowsAuthTests_" + Guid.NewGuid().ToString("N"));
    private readonly InMemoryCredentialStore _store = new();

    public McpUnverifiedWindowsAuthTests() => Directory.CreateDirectory(_root);

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); }
        catch (IOException) { /* a temp folder left behind is harmless */ }
    }

    private string HostsFile => Path.Combine(_root, "user", ServerHostBindings.FileName);

    private ServerManager NewManager() => new(_root, null, HostsFile, new CredentialService(_store));

    private sealed class Probe
    {
        public int Calls;

        public Task<string?> RunAsync(ServerConnection server, string? user, string? password, CancellationToken token)
        {
            Calls++;
            return Task.FromResult<string?>(null);
        }
    }

    private static McpServerAdminHooks Hooks(bool allow, Action? changed = null) =>
        new() { AllowWindowsAuthOverUnverifiedTls = allow, ServersChanged = changed };

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement.Clone();

    private static string Status(string answer) => Parse(answer).GetProperty("status").GetString()!;

    /// <summary>A Windows server and a SQL login server, both confirmed for this user and with a verified connection.</summary>
    private ServerManager Seeded()
    {
        var manager = NewManager();
        var sql = new ServerConnection { ServerName = "sql01", DisplayName = "Orders", AuthenticationType = AuthenticationTypes.SqlServer, EncryptMode = "Mandatory" };
        manager.AddServer(sql, "collector", Secret);
        manager.ConfirmHost(sql);
        var windows = new ServerConnection { ServerName = "sql02", DisplayName = "Billing", AuthenticationType = AuthenticationTypes.Windows, EncryptMode = "Mandatory" };
        manager.AddServer(windows);
        manager.ConfirmHost(windows);
        return manager;
    }

    private static string Entry(string host, string auth, string tls) =>
        auth == "sql"
            ? $"[{{\"host\":\"{host}\",\"auth\":\"sql\",\"username\":\"monitor\",\"password\":\"{Secret}\"{tls}}}]"
            : $"[{{\"host\":\"{host}\",\"auth\":\"{auth}\"{tls}}}]";

    [Theory]
    [InlineData(",\"trust_server_certificate\":true")]
    [InlineData(",\"encrypt_mode\":\"Optional\"")]
    [InlineData(",\"trust_server_certificate\":true,\"encrypt_mode\":\"Optional\"")]
    public async Task AddServers_RefusesWindowsAuthOverUnverifiedTls_BeforeAnyConnectionTest(string tls)
    {
        var manager = NewManager();
        var probe = new Probe();
        var changed = 0;

        var answer = await McpServerRegistryTools.AddServersCore(manager, Hooks(false, () => changed++), Entry("sql03", "windows", tls), probe.RunAsync);

        Assert.Equal("invalid", Status(answer));
        Assert.Equal(0, probe.Calls);
        Assert.Equal(0, changed);
        Assert.Empty(manager.GetAllServers());
        Assert.Empty(NewManager().GetAllServers());
    }

    [Fact]
    public async Task AddServers_RefusesAWholeCall_WhenOneEntryIsWindowsAuthOverUnverifiedTls_AndTestsNone()
    {
        var manager = NewManager();
        var probe = new Probe();
        var json = "[{\"host\":\"sql01\",\"auth\":\"sql\",\"username\":\"monitor\",\"password\":\"" + Secret + "\"},{\"host\":\"sql03\",\"trust_server_certificate\":true}]";

        var answer = await McpServerRegistryTools.AddServersCore(manager, Hooks(false), json, probe.RunAsync);

        Assert.Equal("invalid", Status(answer));
        Assert.Equal("servers_json", Parse(answer).GetProperty("hints").GetProperty("parameter").GetString());
        Assert.Contains("sql03", Parse(answer).GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Equal(0, probe.Calls);
        Assert.Empty(NewManager().GetAllServers());
    }

    [Fact]
    public async Task AddServers_RefusesAnUpdate_WhoseListedServerIsUnverified_WhenTheEntryLeavesTheTlsFieldsOut()
    {
        var manager = NewManager();
        var listed = new ServerConnection { ServerName = "sql03", DisplayName = "Old", AuthenticationType = AuthenticationTypes.Windows, EncryptMode = "Optional" };
        manager.AddServer(listed);
        var probe = new Probe();

        var answer = await McpServerRegistryTools.AddServersCore(manager, Hooks(false), "[{\"host\":\"sql03\",\"display_name\":\"Renamed\"}]", probe.RunAsync);

        Assert.Equal("invalid", Status(answer));
        Assert.Equal(0, probe.Calls);
        Assert.Equal("Old", Assert.Single(NewManager().GetAllServers()).DisplayName);
    }

    [Theory]
    [InlineData("windows", "")]
    [InlineData("windows", ",\"encrypt_mode\":\"Mandatory\"")]
    [InlineData("windows", ",\"encrypt_mode\":\"Strict\",\"trust_server_certificate\":false")]
    [InlineData("sql", ",\"trust_server_certificate\":true")]
    [InlineData("sql", ",\"encrypt_mode\":\"Optional\"")]
    public async Task AddServers_StillServes_VerifiedWindowsAuth_AndSqlAuthOverAnyTls(string auth, string tls)
    {
        var manager = NewManager();
        var probe = new Probe();

        var answer = await McpServerRegistryTools.AddServersCore(manager, Hooks(false), Entry("sql03", auth, tls), probe.RunAsync);

        Assert.Equal("ok", Status(answer));
        Assert.Equal(1, probe.Calls);
        Assert.Single(NewManager().GetAllServers());
    }

    [Fact]
    public async Task AddServers_ServesWindowsAuthOverUnverifiedTls_WhenTheSettingIsOn()
    {
        var manager = NewManager();
        var probe = new Probe();

        var answer = await McpServerRegistryTools.AddServersCore(manager, Hooks(true), Entry("sql03", "windows", ",\"trust_server_certificate\":true"), probe.RunAsync);

        Assert.Equal("ok", Status(answer));
        Assert.Equal(1, probe.Calls);
        Assert.True(Assert.Single(NewManager().GetAllServers()).TrustServerCertificate);
    }

    [Theory]
    [InlineData("{\"trust_server_certificate\":true}")]
    [InlineData("{\"encrypt_mode\":\"Optional\"}")]
    public async Task EditServer_RefusesWindowsAuthOverUnverifiedTls_BeforeAnyConnectionTest(string changes)
    {
        var manager = Seeded();
        var probe = new Probe();

        var answer = await McpServerAdminTools.EditAsync(manager, "Billing", changes, probe.RunAsync, CancellationToken.None, Hooks(false));

        Assert.Equal("invalid", Status(answer));
        Assert.Equal(0, probe.Calls);
        var stored = NewManager().GetAllServers().Single(s => s.DisplayName == "Billing");
        Assert.False(stored.TrustServerCertificate);
        Assert.Equal("Mandatory", stored.EncryptMode);
    }

    [Fact]
    public async Task EditServer_RefusesChangingASqlServerToWindowsAuth_OverAnUnverifiedConnection()
    {
        var manager = NewManager();
        var sql = new ServerConnection { ServerName = "sql01", DisplayName = "Orders", AuthenticationType = AuthenticationTypes.SqlServer, EncryptMode = "Optional", TrustServerCertificate = true };
        manager.AddServer(sql, "collector", Secret);
        manager.ConfirmHost(sql);
        var probe = new Probe();

        var answer = await McpServerAdminTools.EditAsync(manager, "Orders", "{\"auth\":\"Windows\"}", probe.RunAsync, CancellationToken.None, Hooks(false));

        Assert.Equal("invalid", Status(answer));
        Assert.Equal(0, probe.Calls);
        Assert.Equal(AuthenticationTypes.SqlServer, NewManager().GetAllServers().Single().AuthenticationType);
    }

    /* The Edit Server window's own host question (a changed TLS setting on a confirmed host) still applies after the screen, and a tool call cannot
       answer it, so these two tests assert only that the screen is not what stops the edit. */
    [Fact]
    public async Task EditServer_IsNotStoppedByTheScreen_WhenTheSettingIsOn()
    {
        var manager = Seeded();
        var probe = new Probe();

        var refused = await McpServerAdminTools.EditAsync(manager, "Billing", "{\"trust_server_certificate\":true}", probe.RunAsync, CancellationToken.None, Hooks(false));
        var allowed = await McpServerAdminTools.EditAsync(manager, "Billing", "{\"trust_server_certificate\":true}", probe.RunAsync, CancellationToken.None, Hooks(true));

        Assert.Equal("invalid", Status(refused));
        Assert.NotEqual("invalid", Status(allowed));
    }

    [Fact]
    public async Task EditServer_IsNotStoppedByTheScreen_ForSqlAuth_AndSavesAnEditOfAnotherFieldOnAWindowsServer()
    {
        var manager = Seeded();
        var probe = new Probe();

        var sqlAnswer = await McpServerAdminTools.EditAsync(manager, "Orders", "{\"trust_server_certificate\":true,\"password\":\"another-fake-secret-value\"}", probe.RunAsync, CancellationToken.None, Hooks(false));
        var nameAnswer = await McpServerAdminTools.EditAsync(manager, "Billing", "{\"description\":\"month end\"}", probe.RunAsync, CancellationToken.None, Hooks(false));

        Assert.NotEqual("invalid", Status(sqlAnswer));
        Assert.Equal("updated", Status(nameAnswer));
    }

    [Fact]
    public void TheSetting_DefaultsOff_IsReadFromSettingsJson_AndAWrongKindIsAProblem()
    {
        Assert.False(new McpSettings().AllowWindowsAuthOverUnverifiedTls);
        Assert.False(new McpServerAdminHooks().AllowWindowsAuthOverUnverifiedTls);
        Assert.False(Load("{ \"mcp_enabled\": true }").AllowWindowsAuthOverUnverifiedTls);
        Assert.True(Load("{ \"mcp_enabled\": true, \"mcp_allow_windows_auth_over_unverified_tls\": true }").AllowWindowsAuthOverUnverifiedTls);

        var quoted = Load("{ \"mcp_enabled\": true, \"mcp_allow_windows_auth_over_unverified_tls\": \"true\" }");
        Assert.False(quoted.Enabled);
        Assert.False(quoted.AllowWindowsAuthOverUnverifiedTls);
        Assert.False(string.IsNullOrEmpty(quoted.Problem));

        /* Import Settings never brings a loosening setting in from a previous install. */
        Assert.Contains(McpUnverifiedWindowsAuthScreen.Setting, SettingsImport.McpGateKeys);
    }

    private McpSettings Load(string content)
    {
        var dir = Path.Combine(_root, "settings_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "settings.json"), content);
        return McpSettings.Load(dir);
    }
}
