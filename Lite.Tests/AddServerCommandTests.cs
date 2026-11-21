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
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9237: <c>--add-server</c> adds servers without the window. Every test runs over a temp folder and an in-memory credential store,
/// with a fake connection test, so no real servers.json, host record, credential or SQL Server is touched. The one test that
/// uses Credential Manager does so under a prefix of its own and deletes what it wrote.
/// </summary>
public sealed class AddServerCommandTests : IDisposable
{
    private const string Secret = "not-a-real-secret-value";
    private readonly string _root = Path.Combine(Path.GetTempPath(), "AddServerCommandTests_" + Guid.NewGuid().ToString("N"));
    private readonly InMemoryCredentialStore _store = new();
    private readonly CapturingLogger _log = new();

    public AddServerCommandTests() => Directory.CreateDirectory(_root);

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); }
        catch (IOException) { /* a temp folder left behind is harmless */ }
    }

    private string ServersFile => Path.Combine(_root, "servers.json");
    private string HostsFile => Path.Combine(_root, "user", ServerHostBindings.FileName);

    private sealed class CapturingLogger : ILogger<ServerManager>
    {
        public List<string> Lines { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Lines.Add(formatter(state, exception));
    }

    private sealed class Run
    {
        public int Exit;
        public string Out = "";
        public string Err = "";
        public List<string> Tested = [];
        public bool ManagerMade;
    }

    private async Task<Run> RunVerbAsync(
        string[] args, string stdin, bool redirected = true, bool liteRunning = false, Func<ServerConnection, string?>? connect = null)
    {
        var run = new Run();
        var machine = new AddServerCommand.Machine(
            () =>
            {
                run.ManagerMade = true;
                return new ServerManager(_root, _log, HostsFile, new CredentialService(_store));
            },
            () => liteRunning,
            (server, user, password) =>
            {
                run.Tested.Add(server.ServerName);
                return Task.FromResult(connect?.Invoke(server));
            },
            "TESTDOMAIN\\collector",
            Path.GetDirectoryName(HostsFile)!);
        using var output = new StringWriter();
        using var error = new StringWriter();
        run.Exit = await AddServerCommand.RunAsync(args, new StringReader(stdin), redirected, output, error, machine);
        run.Out = output.ToString();
        run.Err = error.ToString();
        return run;
    }

    private string[] ServerNames()
    {
        if (!File.Exists(ServersFile))
            return [];
        using var doc = JsonDocument.Parse(File.ReadAllText(ServersFile));
        return doc.RootElement.GetProperty("Servers").EnumerateArray().Select(s => s.GetProperty("ServerName").GetString()!).ToArray();
    }

    /// <summary>Every key in the store. Server ids are GUIDs, so each starts with one hex digit.</summary>
    private string[] StoredKeys() => "0123456789abcdef".SelectMany(c => _store.ListKeys(c.ToString())).ToArray();

    private ServerManager Reload() => new(_root, null, HostsFile, new CredentialService(_store));

    [Theory]
    [InlineData("--add-server")]
    [InlineData("--add-servers")]
    [InlineData("--ADD-SERVER")]
    public void TheVerb_StartsNothing(string arg)
    {
        var plan = LiteStartupPlan.FromArgs([arg, "--host", "sql01"]);

        Assert.True(plan.AddServer);
        Assert.False(plan.StartsServices);
        Assert.False(plan.TakesInstanceLock);
        Assert.False(plan.ShowsMainWindow);
        Assert.False(LiteStartupPlan.FromArgs(["--headless"]).AddServer);
    }

    [Fact]
    public void Grammar_ReadsFlagsInBothForms()
    {
        Assert.True(AddServerCommand.TryParseArgs(
            ["--add-server", "--host", "sql01", "--name=Primary", "--auth", "integrated", "--login", "monitor", "--database=Utility"],
            out var flags, out var error));

        Assert.Null(error);
        Assert.Equal(new AddServerFlags("sql01", "Primary", "integrated", "monitor", "Utility"), flags);
    }

    [Theory]
    [InlineData("--password", "x")]
    [InlineData("--password=x")]
    [InlineData("--PWD", "x")]
    [InlineData("--engine", "postgres")]
    [InlineData("--bogus")]
    [InlineData("stray")]
    public void Grammar_RefusesAPasswordFlagAndAnythingElseUnknown(params string[] extra)
    {
        var ok = AddServerCommand.TryParseArgs(["--add-server", "--host", "sql01", .. extra], out _, out var error);

        Assert.False(ok);
        Assert.NotNull(error);
    }

    [Fact]
    public async Task Grammar_FlagWithNoValueIsRefused()
    {
        var run = await RunVerbAsync(["--add-server", "--host"], "");

        Assert.Equal(1, run.Exit);
        Assert.Empty(run.Tested);
    }

    [Fact]
    public async Task StdinArray_AddsTheServer_WithItsCredentialAndHostConfirmation_AndNamesTheAccountAndFolders()
    {
        var run = await RunVerbAsync(
            ["--add-server"],
            $$"""[{"host":"sql01","display_name":"Primary","auth":"sql","username":"monitor","password":"{{Secret}}","database":"Utility"}]""");

        Assert.Equal(0, run.Exit);
        Assert.Equal(["sql01"], ServerNames());
        var added = Reload().GetAllServers().Single();
        Assert.Equal("Primary", added.DisplayName);
        Assert.Equal("Utility", added.DatabaseName);
        Assert.Equal(AuthenticationTypes.SqlServer, added.AuthenticationType);
        Assert.Equal(("monitor", Secret), _store.GetCredential(added.Id));
        Assert.Contains(added.Id, File.ReadAllText(HostsFile));
        Assert.Null(Reload().HostChange(added));
        Assert.Contains("TESTDOMAIN\\collector", run.Out);
        Assert.Contains(_root, run.Out);
    }

    [Fact]
    public async Task Flags_WithAPipedPassword_AddsOneServer()
    {
        var run = await RunVerbAsync(["--add-server", "--host", "sql01", "--auth", "sql", "--username", "monitor"], Secret + "\r\n");

        Assert.Equal(0, run.Exit);
        var added = Reload().GetAllServers().Single();
        Assert.Equal(("monitor", Secret), _store.GetCredential(added.Id));
    }

    [Fact]
    public async Task Flags_WindowsAuth_NeedsNoPassword_AndStoresNoCredential()
    {
        var run = await RunVerbAsync(["--add-server", "--host", "sql01", "--auth", "integrated"], "", redirected: false);

        Assert.Equal(0, run.Exit);
        var added = Reload().GetAllServers().Single();
        Assert.Equal(AuthenticationTypes.Windows, added.AuthenticationType);
        Assert.Null(_store.GetCredential(added.Id));
    }

    [Fact]
    public async Task Flags_SqlLoginWithStdinAtTheKeyboard_IsRefused_AndChangesNothing()
    {
        var run = await RunVerbAsync(["--add-server", "--host", "sql01", "--auth", "sql", "--username", "monitor"], "", redirected: false);

        Assert.Equal(1, run.Exit);
        Assert.Empty(ServerNames());
        Assert.Empty(run.Tested);
        Assert.False(File.Exists(HostsFile));
    }

    [Theory]
    [InlineData("""[{"host":"sql01","auth":"sql","password":"p"}]""")]
    [InlineData("""[{"host":"np:sql01","auth":"windows"}]""")]
    [InlineData("""[{"host":"sql01","auth":"sql","username":"monitor"}]""")]
    [InlineData("""[{"host":"sql01","auth":"serviceprincipal","username":"client"}]""")]
    [InlineData("""[{"host":"sql01","auth":"entramfa"}]""")]
    [InlineData("""[{"host":"sql01","engine":"postgres"}]""")]
    [InlineData("""[{"auth":"windows"}]""")]
    [InlineData("""{"host":"sql01"}""")]
    [InlineData("not json")]
    [InlineData("[]")]
    public async Task ARefusedEntry_ExitsOne_AndChangesNothing(string stdin)
    {
        var run = await RunVerbAsync(["--add-server"], stdin);

        Assert.Equal(1, run.Exit);
        Assert.Empty(ServerNames());
        Assert.False(File.Exists(HostsFile));
        Assert.Empty(StoredKeys());
        Assert.Empty(run.Tested);
    }

    [Theory]
    [InlineData("""[{"host":"np:sql01","auth":"windows"}]""")]
    [InlineData("""[{"host":"sql01","auth":"sql","password":"p"}]""")]
    [InlineData("""[{"host":"sql01","auth":"entramfa"}]""")]
    [InlineData("""[{"host":"sql01","auth":"windows"},{"host":"sql01","auth":"windows"}]""")]
    public async Task ARefusalThatNeedsNoServerList_NeverOpensOrCreatesServersJson(string stdin)
    {
        var run = await RunVerbAsync(["--add-server"], stdin);

        Assert.Equal(1, run.Exit);
        Assert.False(run.ManagerMade);
        Assert.False(File.Exists(ServersFile));
    }

    [Fact]
    public async Task AFailedConnectionTest_ExitsOne_AndStoresNothing()
    {
        var run = await RunVerbAsync(
            ["--add-server"], $$"""[{"host":"sql01","auth":"sql","username":"monitor","password":"{{Secret}}"}]""",
            connect: _ => "Login failed.");

        Assert.Equal(1, run.Exit);
        Assert.Equal(["sql01"], run.Tested);
        Assert.Empty(ServerNames());
        Assert.Empty(StoredKeys());
        Assert.False(File.Exists(HostsFile));
        Assert.Contains("Login failed.", run.Err);
    }

    [Fact]
    public async Task OneRefusedEntryInABatch_AddsNoneOfIt()
    {
        var run = await RunVerbAsync(
            ["--add-server"], """[{"host":"sql01","auth":"windows"},{"host":"sql02","auth":"windows"}]""",
            connect: server => server.ServerName == "sql02" ? "Login failed." : null);

        Assert.Equal(1, run.Exit);
        Assert.Empty(ServerNames());
        Assert.False(File.Exists(HostsFile));
    }

    [Fact]
    public async Task AddingTheSameServerAgain_ReplacesThePassword_AndAddsNoSecondEntry()
    {
        await RunVerbAsync(["--add-server"], """[{"host":"sql01","auth":"sql","username":"monitor","password":"first-value"}]""");
        var id = Reload().GetAllServers().Single().Id;

        var run = await RunVerbAsync(["--add-server"], """[{"host":"sql01","auth":"sql","username":"monitor2","password":"second-value"}]""");

        Assert.Equal(0, run.Exit);
        var servers = Reload().GetAllServers();
        Assert.Equal(id, Assert.Single(servers).Id);
        Assert.Equal(("monitor2", "second-value"), _store.GetCredential(id));
    }

    [Fact]
    public async Task ARunningLite_RefusesTheVerb_AndChangesNothing()
    {
        var run = await RunVerbAsync(["--add-server"], """[{"host":"sql01","auth":"windows"}]""", liteRunning: true);

        Assert.Equal(1, run.Exit);
        Assert.Empty(ServerNames());
        Assert.False(File.Exists(HostsFile));
        Assert.Empty(run.Tested);
    }

    [Fact]
    public async Task ThePassword_NeverReachesTheOutput_TheErrorOrTheLog()
    {
        var added = await RunVerbAsync(["--add-server"], $$"""[{"host":"sql01","auth":"sql","username":"monitor","password":"{{Secret}}"}]""");
        var failed = await RunVerbAsync(
            ["--add-server"], $$"""[{"host":"sql02","auth":"sql","username":"monitor","password":"{{Secret}}"}]""", connect: _ => "Login failed.");
        var refused = await RunVerbAsync(["--add-server"], $$"""[{"host":"np:sql03","auth":"sql","username":"monitor","password":"{{Secret}}"}]""");

        foreach (var run in new[] { added, failed, refused })
        {
            Assert.DoesNotContain(Secret, run.Out);
            Assert.DoesNotContain(Secret, run.Err);
        }

        Assert.DoesNotContain(_log.Lines, line => line.Contains(Secret, StringComparison.Ordinal));
        Assert.DoesNotContain(Secret, File.ReadAllText(ServersFile));
        Assert.DoesNotContain(Secret, File.ReadAllText(HostsFile));
    }

    [Fact]
    public void TheCredentialRule_IsTheOneTheWindowApplies()
    {
        Assert.NotNull(ServerEntryRules.CredentialRefusal(AuthenticationTypes.SqlServer, "", "p"));
        Assert.Null(ServerEntryRules.CredentialRefusal(AuthenticationTypes.SqlServer, "u", ""));
        Assert.NotNull(ServerEntryRules.CredentialRefusal(AuthenticationTypes.ServicePrincipal, "client", ""));
        Assert.NotNull(ServerEntryRules.CredentialRefusal(AuthenticationTypes.ServicePrincipal, "", "secret"));
        Assert.Null(ServerEntryRules.CredentialRefusal(AuthenticationTypes.Windows, null, null));
    }

    /// <summary>The credential lands in the real Credential Manager under the prefix the store was given, as Lite's own
    /// <c>PerformanceMonitorLite_</c> prefix does for the running user. The prefix here is the test's own, and the entry is deleted.</summary>
    [Collection("credential-manager")]
    public sealed class RealCredentialManager : IDisposable
    {
        private readonly string _root = Path.Combine(Path.GetTempPath(), "AddServerCommandTests_" + Guid.NewGuid().ToString("N"));

        public void Dispose()
        {
            try { Directory.Delete(_root, recursive: true); }
            catch (IOException) { /* harmless */ }
        }

        [Fact]
        public async Task TheCredential_IsInCredentialManager_UnderTheStoresPrefix_AndAReAddReplacesIt()
        {
            Directory.CreateDirectory(_root);
            var prefix = "PerformanceMonitorLiteTest" + Guid.NewGuid().ToString("N") + "_";
            var store = new CredentialStore(prefix, "Performance Monitor Lite test");
            var credentials = new CredentialService(store);
            string? id = null;
            try
            {
                async Task<int> Add(string password)
                {
                    var manager = new ServerManager(_root, null, Path.Combine(_root, "hosts", ServerHostBindings.FileName), credentials);
                    var machine = new AddServerCommand.Machine(
                        () => manager, () => false, (_, _, _) => Task.FromResult<string?>(null), "account", Path.Combine(_root, "hosts"));
                    using var output = new StringWriter();
                    using var error = new StringWriter();
                    var json = $$"""[{"host":"sql01","auth":"sql","username":"monitor","password":"{{password}}"}]""";
                    var code = await AddServerCommand.RunAsync(["--add-server"], new StringReader(json), true, output, error, machine);
                    id ??= manager.GetAllServers().SingleOrDefault()?.Id;
                    return code;
                }

                Assert.SkipWhen(!store.SaveCredential("probe", "u", "p"), "This session cannot write to Windows Credential Manager.");
                store.DeleteCredential("probe");

                Assert.Equal(0, await Add("first-value"));
                Assert.Equal(("monitor", "first-value"), credentials.GetCredential(id!));
                Assert.Equal(0, await Add("second-value"));
                Assert.Equal(("monitor", "second-value"), credentials.GetCredential(id!));
            }
            finally
            {
                if (id is not null)
                    store.DeleteCredential(id);
                store.DeleteCredential("probe");
            }
        }
    }
}
