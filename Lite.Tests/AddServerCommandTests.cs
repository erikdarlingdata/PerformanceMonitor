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
using System.Text.Json.Nodes;
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

    /// <summary>
    /// <paramref name="stdin"/> is what the JSON form reads as text. The flag form reads its password as bytes (#9753): the UTF-8 bytes of
    /// <paramref name="stdin"/>, or <paramref name="stdinBytes"/> when a test sends bytes that text cannot spell, so the same run holds both.
    /// </summary>
    private async Task<Run> RunVerbAsync(
        string[] args, string stdin, bool redirected = true, bool liteRunning = false, Func<ServerConnection, string?>? connect = null,
        Func<bool>? liteRunningCheck = null, ICredentialStore? store = null, byte[]? stdinBytes = null)
    {
        var run = new Run();
        var machine = new AddServerCommand.Machine(
            () =>
            {
                run.ManagerMade = true;
                return new ServerManager(_root, _log, HostsFile, new CredentialService(store ?? _store));
            },
            liteRunningCheck ?? (() => liteRunning),
            (server, user, password) =>
            {
                run.Tested.Add(server.ServerName);
                return Task.FromResult(connect?.Invoke(server));
            },
            "TESTDOMAIN\\collector",
            Path.GetDirectoryName(HostsFile)!);
        using var output = new StringWriter();
        using var error = new StringWriter();
        using var passwordInput = new MemoryStream(stdinBytes ?? new System.Text.UTF8Encoding(false).GetBytes(stdin));
        run.Exit = await AddServerCommand.RunAsync(args, new StringReader(stdin), redirected, output, error, machine, passwordInput);
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

    private static readonly string[] SqlFlags = ["--add-server", "--host", "sql01", "--auth", "sql", "--username", "monitor"];

    /// <summary>#9753: a refused piped password changes nothing: no server, no connection test, no credential, no host record.</summary>
    private void AssertNothingChanged(Run run)
    {
        Assert.Equal(1, run.Exit);
        Assert.Empty(ServerNames());
        Assert.Empty(run.Tested);
        Assert.Empty(StoredKeys());
        Assert.False(File.Exists(HostsFile));
    }

    [Fact]
    public async Task Flags_APipedPasswordWithAByteOrderMark_IsRefused_AndChangesNothing()
    {
        var run = await RunVerbAsync(SqlFlags, "\uFEFF" + Secret + "\r\n", stdinBytes: [0xEF, 0xBB, 0xBF, .. System.Text.Encoding.UTF8.GetBytes(Secret + "\r\n")]);

        AssertNothingChanged(run);
        Assert.Equal(StrictStdinPassword.HasByteOrderMarkText, run.Err.Trim());
    }

    [Fact]
    public async Task Flags_APipedPasswordWithASecondNonEmptyLine_IsRefused_AndChangesNothing()
    {
        var run = await RunVerbAsync(SqlFlags, Secret + "\r\nsecond-line\r\n");

        AssertNothingChanged(run);
        Assert.Equal(StrictStdinPassword.HasManyLinesText, run.Err.Trim());
        Assert.DoesNotContain("second-line", run.Err);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Flags_APipedPasswordInUtf16_IsRefused_AndChangesNothing(bool withByteOrderMark)
    {
        var utf16 = System.Text.Encoding.Unicode.GetBytes(Secret + "\r\n");
        var run = await RunVerbAsync(SqlFlags, Secret, stdinBytes: withByteOrderMark ? [0xFF, 0xFE, .. utf16] : utf16);

        AssertNothingChanged(run);
        Assert.Equal(StrictStdinPassword.HasNulText, run.Err.Trim());
    }

    [Fact]
    public async Task Flags_APipedPasswordThatIsNotUtf8_IsRefused_AndChangesNothing()
    {
        /* A pipe written in a console code page sends the single byte E9 for an e with an acute accent. */
        var run = await RunVerbAsync(SqlFlags, "caf\u00e9", stdinBytes: [0x63, 0x61, 0x66, 0xE9, 0x0D, 0x0A]);

        AssertNothingChanged(run);
        Assert.Equal("The password contains characters that cannot be stored. Nothing was changed.", run.Err.Trim());
    }

    [Fact]
    public async Task Flags_ANonAsciiPipedPassword_IsSavedExactly()
    {
        const string password = "p\u00e4ss w\u00f6rd \u2014 \u65e5\u672c\u8a9e \U0001F511 ";
        var run = await RunVerbAsync(SqlFlags, password + "\r\n");

        Assert.Equal(0, run.Exit);
        Assert.Equal(("monitor", password), _store.GetCredential(Reload().GetAllServers().Single().Id));
    }

    [Theory]
    [InlineData("\n")]
    [InlineData("\r\n")]
    [InlineData("")]
    public async Task Flags_OnePipedTrailingNewline_IsStripped_AndNothingElse(string ending)
    {
        var run = await RunVerbAsync(SqlFlags, Secret + ending);

        Assert.Equal(0, run.Exit);
        Assert.Equal(("monitor", Secret), _store.GetCredential(Reload().GetAllServers().Single().Id));
    }

    [Fact]
    public async Task Flags_APipedPasswordOverTheLimit_IsRefusedUnread_AndChangesNothing()
    {
        var run = await RunVerbAsync(SqlFlags, new string('p', AddServerCommand.MaxPipedPasswordBytes + 3));

        AssertNothingChanged(run);
        Assert.Equal("The password is longer than can be stored. Nothing was changed.", run.Err.Trim());
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

    /// <summary>#9787: the same rule as Darling's writers. Each of the four text fields with a line feed, a line separator or an
    /// invisible tag character is refused by field name, the error lines carry no control character, and nothing changes.</summary>
    [Theory]
    [InlineData("display_name", "\n")]
    [InlineData("host", "\u2028")]
    [InlineData("database", "\u202E")]
    [InlineData("username", "\U000E0041")]
    public async Task AFieldHoldingAControlCharacter_IsRefusedByName_AndChangesNothing(string field, string character)
    {
        var entry = new Dictionary<string, object> { ["host"] = "sql01", ["auth"] = "windows", [field] = "x" + character + "y" };
        var run = await RunVerbAsync(["--add-server"], JsonSerializer.Serialize(new[] { entry }));

        Assert.Equal(1, run.Exit);
        Assert.Contains($"{field} holds a control character", run.Err, StringComparison.Ordinal);
        Assert.All(run.Err.Split('\n'), line => Assert.DoesNotContain(line.TrimEnd('\r'), ServerFieldText.IsControl));
        Assert.Empty(ServerNames());
        Assert.Empty(run.Tested);
        Assert.Empty(StoredKeys());
    }

    /// <summary>#9787: a zero-width non-joiner is part of an ordinary Persian word, so a database holding one is added.</summary>
    [Fact]
    public async Task ADatabaseHoldingAZeroWidthNonJoiner_IsAdded()
    {
        var database = "\u0645\u06CC\u200C\u062E\u0648\u0627\u0647\u0645";
        var entry = new Dictionary<string, object> { ["host"] = "sql01", ["auth"] = "windows", ["database"] = database };
        var run = await RunVerbAsync(["--add-server"], JsonSerializer.Serialize(new[] { entry }));

        Assert.Equal(0, run.Exit);
        Assert.Equal(["sql01"], ServerNames());
        Assert.Equal(database, Reload().GetAllServers().Single().DatabaseName);
    }

    [Fact]
    public void ServerManagerAddServer_RefusesAControlCharacterInTheDisplayName_AndWritesNothing()
    {
        var manager = Reload();

        var refused = Assert.Throws<InvalidOperationException>(
            () => manager.AddServer(new ServerConnection { ServerName = "sql01", DisplayName = "a\u2028b" }));

        Assert.Contains("display_name holds a control character", refused.Message, StringComparison.Ordinal);
        Assert.Empty(manager.GetAllServers());
        Assert.Empty(ServerNames());
    }

    /// <summary>#9787: ServerManager.UpdateServer holds a CHANGED display name, host, database or login to the rule, so every caller
    /// is covered. A field that keeps its saved text is not checked, so a row saved before the rule stays editable on its other fields.</summary>
    [Fact]
    public void UpdateServer_RefusesAChangedDisplayNameHoldingAnOverride_AndAcceptsAnEditToAnotherFieldOfALegacyRow()
    {
        var manager = Reload();
        var server = new ServerConnection { ServerName = "sql01", DisplayName = "Orders", AuthenticationType = AuthenticationTypes.Windows };
        manager.AddServer(server);

        /* A row saved before the rule: the name is written without the rule, as an older build did. */
        server.DisplayName = "Or\u202Eders";
        manager.UpdateServerSettings(server);
        var legacy = Reload();
        var row = legacy.GetAllServers().Single();
        Assert.Equal("Or\u202Eders", row.DisplayName);

        row.Description = "an edit to another field";
        legacy.UpdateServer(row);
        Assert.Equal("an edit to another field", Reload().GetAllServers().Single().Description);
        Assert.Equal("Or\u202Eders", Reload().GetAllServers().Single().DisplayName);

        row.DisplayName = "Pay\u202Eroll";
        var refused = Assert.Throws<InvalidOperationException>(() => legacy.UpdateServer(row));
        Assert.Contains("display_name holds a control character", refused.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("Pay", refused.Message, StringComparison.Ordinal);
        Assert.Equal("Or\u202Eders", Reload().GetAllServers().Single().DisplayName);
    }

    [Theory]
    [InlineData("""[{"host":"np:sql01","auth":"windows"}]""")]
    [InlineData("""[{"host":"sql01","auth":"sql","password":"p"}]""")]
    [InlineData("""[{"host":"sql01","auth":"entramfa"}]""")]
    [InlineData("""[{"host":"np:sql01","auth":"windows"},{"host":"np:sql02","auth":"windows"}]""")]
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
    public async Task EveryEntryOfABatchIsTested_AndEveryFailureIsReportedWithItsNumberAndHost()
    {
        var run = await RunVerbAsync(
            ["--add-server"], """[{"host":"sql01","auth":"windows"},{"host":"sql02","auth":"windows"},{"host":"sql03","auth":"windows"}]""",
            connect: _ => "The certificate chain was issued by an authority that is not trusted.");

        Assert.Equal(1, run.Exit);
        Assert.Equal(["sql01", "sql02", "sql03"], run.Tested);
        Assert.Contains("Entry 1 (sql01)", run.Err);
        Assert.Contains("Entry 2 (sql02)", run.Err);
        Assert.Contains("Entry 3 (sql03)", run.Err);
        Assert.EndsWith(AddServerCommand.RefusedSummary(3, 3), run.Err.TrimEnd());
        Assert.Empty(ServerNames());
        Assert.False(File.Exists(HostsFile));
    }

    [Fact]
    public async Task APassingEntryBesideARefusedOne_IsNotAdded_AndTheRunSaysSo()
    {
        var run = await RunVerbAsync(
            ["--add-server"], """[{"host":"sql01","auth":"windows"},{"host":"sql02","auth":"windows"},{"host":"sql03","auth":"windows"}]""",
            connect: server => server.ServerName == "sql02" ? "Login failed." : null);

        Assert.Equal(1, run.Exit);
        Assert.Equal(["sql01", "sql02", "sql03"], run.Tested);
        Assert.Contains("Entry 2 (sql02)", run.Err);
        Assert.DoesNotContain("Entry 1", run.Err);
        Assert.DoesNotContain("Entry 3", run.Err);
        Assert.EndsWith(AddServerCommand.RefusedSummary(1, 3), run.Err.TrimEnd());
        Assert.Empty(ServerNames());
        Assert.Empty(StoredKeys());
        Assert.False(File.Exists(HostsFile));
    }

    [Fact]
    public async Task ARuleRefusalAndAFailedConnectionTest_AreBothReportedInOneRun()
    {
        var run = await RunVerbAsync(
            ["--add-server"], """[{"host":"np:sql01","auth":"windows"},{"host":"sql02","auth":"windows"},{"host":"sql03","auth":"windows"}]""",
            connect: server => server.ServerName == "sql03" ? "Login failed." : null);

        Assert.Equal(1, run.Exit);
        Assert.Equal(["sql02", "sql03"], run.Tested);
        Assert.Contains("Entry 1 (np:sql01)", run.Err);
        Assert.Contains("Entry 3 (sql03)", run.Err);
        Assert.DoesNotContain("Entry 2", run.Err);
        Assert.EndsWith(AddServerCommand.RefusedSummary(2, 3), run.Err.TrimEnd());
        Assert.Empty(ServerNames());
    }

    [Fact]
    public async Task ADuplicateOfARefusedEntry_IsReportedInTheSameRun()
    {
        var run = await RunVerbAsync(
            ["--add-server"], """[{"host":"sql01","auth":"sql","username":"monitor"},{"host":"sql01","auth":"windows"}]""");

        Assert.Equal(1, run.Exit);
        Assert.Contains("Entry 1 (sql01)", run.Err);
        Assert.Contains("Entry 2 (sql01): the same server is listed twice", run.Err);
        Assert.False(run.ManagerMade);
        Assert.Empty(run.Tested);
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
    public async Task ABadJsonLiteral_ReportsThePosition_NotTheInputAfterIt()
    {
        var run = await RunVerbAsync(["--add-server"], """[{"host":"h","auth":"sql","username":"u","password":nextValue99!x}]""");

        Assert.Equal(1, run.Exit);
        Assert.DoesNotContain("nextValue99", run.Err);
        Assert.DoesNotContain("nextValue99", run.Out);
        Assert.Contains("line 1", run.Err);
        Assert.Empty(run.Tested);
    }

    [Fact]
    public async Task AStrayArgument_IsNamedByItsPosition_NotItsValue()
    {
        var run = await RunVerbAsync(["--add-server", "--username", "monitor", "stray-value-77"], "");

        Assert.Equal(1, run.Exit);
        Assert.DoesNotContain("stray-value-77", run.Err);
        Assert.Contains("argument 3", run.Err);
    }

    [Fact]
    public async Task BareVerbAtAConsole_IsRefused_WithoutReadingTheKeyboard()
    {
        var run = await RunVerbAsync(["--add-server"], """[{"host":"sql01","auth":"windows"}]""", redirected: false);

        Assert.Equal(1, run.Exit);
        Assert.Contains("not redirected", run.Err);
        Assert.Empty(ServerNames());
        Assert.Empty(run.Tested);
        Assert.False(run.ManagerMade);
    }

    [Fact]
    public async Task ALiteStartedDuringTheConnectionTests_RefusesTheWrite_AndChangesNothing()
    {
        var checks = 0;
        var run = await RunVerbAsync(
            ["--add-server"], """[{"host":"sql01","auth":"sql","username":"monitor","password":"first-value"}]""",
            liteRunningCheck: () => ++checks > 1);

        Assert.Equal(1, run.Exit);
        Assert.Equal(2, checks);
        Assert.Equal(["sql01"], run.Tested);
        Assert.Contains("Lite is running", run.Err);
        Assert.Empty(ServerNames());
        Assert.Empty(StoredKeys());
        Assert.False(File.Exists(HostsFile));
    }

    private List<string> UnconfirmedFor(ServerConnection listed)
    {
        var manager = Reload();
        return AddServerCommand.UnconfirmedSettings(listed, manager.HostChangeIfSaved(listed), keepsManagedIdentityClientId: false);
    }

    [Fact]
    public async Task TheJsonFields_SetEncryptionAndCertificateTrust_AndThisAccountConfirmsThem()
    {
        ServerConnection? tested = null;
        var run = await RunVerbAsync(
            ["--add-server"],
            """[{"host":"sql01","encrypt_mode":"Optional","trust_server_certificate":true,"read_only_intent":false,"multi_subnet_failover":true}]""",
            connect: server =>
            {
                tested = server;
                return null;
            });

        Assert.Equal(0, run.Exit);
        Assert.NotNull(tested);
        Assert.Equal("Optional", tested.EncryptMode);
        Assert.True(tested.TrustServerCertificate);
        var listed = Reload().GetAllServers().Single();
        Assert.Equal("Optional", listed.EncryptMode);
        Assert.True(listed.TrustServerCertificate);
        Assert.False(listed.ReadOnlyIntent);
        Assert.True(listed.MultiSubnetFailover);
        Assert.Empty(UnconfirmedFor(listed));
    }

    [Fact]
    public async Task ANewServer_ThatGivesNone_TakesTheDefaults()
    {
        var run = await RunVerbAsync(["--add-server"], """[{"host":"sql01"}]""");

        Assert.Equal(0, run.Exit);
        var listed = Reload().GetAllServers().Single();
        Assert.Equal("Mandatory", listed.EncryptMode);
        Assert.False(listed.TrustServerCertificate);
        Assert.False(listed.ReadOnlyIntent);
        Assert.False(listed.MultiSubnetFailover);
    }

    [Theory]
    [InlineData("""[{"host":"sql01","encrypt_mode":"Sometimes"}]""", "encrypt_mode")]
    [InlineData("""[{"host":"sql01","encrypt_mode":3}]""", "encrypt_mode")]
    [InlineData("""[{"host":"sql01","trust_server_certificate":"true"}]""", "trust_server_certificate")]
    [InlineData("""[{"host":"sql01","read_only_intent":1}]""", "read_only_intent")]
    [InlineData("""[{"host":"sql01","multi_subnet_failover":"yes"}]""", "multi_subnet_failover")]
    [InlineData("""[{"host":"sql02"},{"host":"sql01","encrypt_mode":"Sometimes"}]""", "Entry 2")]
    public async Task ARefusedValue_ExitsWithOne_AndWritesNothing(string json, string named)
    {
        var run = await RunVerbAsync(["--add-server"], json);

        Assert.Equal(1, run.Exit);
        Assert.Contains(named, run.Err);
        Assert.Empty(run.Tested);
        Assert.False(run.ManagerMade);
        Assert.Empty(ServerNames());
        Assert.False(File.Exists(HostsFile));
    }

    [Fact]
    public async Task AnUnknownEncryptionFlag_ExitsWithOne_AndWritesNothing()
    {
        var run = await RunVerbAsync(["--add-server", "--host", "sql01", "--encrypt-mode", "Sometimes"], "");

        Assert.Equal(1, run.Exit);
        Assert.Contains("encrypt_mode", run.Err);
        Assert.Empty(ServerNames());
        Assert.False(File.Exists(HostsFile));
    }

    [Fact]
    public async Task TheFlags_SetTheSameValues_AndThisAccountConfirmsThem()
    {
        var run = await RunVerbAsync(
            ["--add-server", "--host", "sql01", "--encrypt-mode", "optional", "--trust-server-certificate", "--read-only-intent", "false", "--multi-subnet-failover=true"],
            "");

        Assert.Equal(0, run.Exit);
        var listed = Reload().GetAllServers().Single();
        Assert.Equal("Optional", listed.EncryptMode);
        Assert.True(listed.TrustServerCertificate);
        Assert.False(listed.ReadOnlyIntent);
        Assert.True(listed.MultiSubnetFailover);
        Assert.Empty(UnconfirmedFor(listed));
    }

    [Fact]
    public void Grammar_ReadsTheEncryptionSwitches()
    {
        Assert.True(AddServerCommand.TryParseArgs(
            ["--add-server", "--host", "sql01", "--trust-server-certificate", "--read-only-intent=false", "--multi-subnet-failover", "true", "--encrypt-mode", "strict"],
            out var flags, out var error));

        Assert.Null(error);
        Assert.Equal(new AddServerFlags("sql01", EncryptMode: "strict", TrustServerCertificate: true, ReadOnlyIntent: false, MultiSubnetFailover: true), flags);
        Assert.False(AddServerCommand.TryParseArgs(["--add-server", "--host", "sql01", "--trust-server-certificate=maybe"], out _, out var refused));
        Assert.Contains("--trust-server-certificate", refused);
    }

    [Fact]
    public async Task AnUpdate_ThatOmitsTheFields_KeepsTheListedValues()
    {
        await RunVerbAsync(["--add-server"], """[{"host":"sql01","encrypt_mode":"Optional","trust_server_certificate":true,"multi_subnet_failover":true}]""");

        var run = await RunVerbAsync(["--add-server"], """[{"host":"sql01","display_name":"Renamed"}]""");

        Assert.Equal(0, run.Exit);
        var listed = Reload().GetAllServers().Single();
        Assert.Equal("Renamed", listed.DisplayName);
        Assert.Equal("Optional", listed.EncryptMode);
        Assert.True(listed.TrustServerCertificate);
        Assert.True(listed.MultiSubnetFailover);
    }

    [Fact]
    public async Task AnUpdate_ThatGivesTheFields_ReplacesTheListedValues()
    {
        await RunVerbAsync(["--add-server"], """[{"host":"sql01","encrypt_mode":"Optional","trust_server_certificate":true}]""");

        var run = await RunVerbAsync(["--add-server"], """[{"host":"sql01","encrypt_mode":"Strict","trust_server_certificate":false}]""");

        Assert.Equal(0, run.Exit);
        var listed = Reload().GetAllServers().Single();
        Assert.Equal("Strict", listed.EncryptMode);
        Assert.False(listed.TrustServerCertificate);
    }

    [Fact]
    public async Task AnUpdate_ThatGivesAValue_ReplacesAnUnconfirmedListedOne_AndConfirmsIt()
    {
        await AddSql01Async();
        EditListedEntry(entry =>
        {
            entry["TrustServerCertificate"] = true;
            entry["EncryptMode"] = "Optional";
        });

        var run = await RunVerbAsync(
            ["--add-server"], """[{"host":"sql01","auth":"sql","username":"monitor","password":"second-value","encrypt_mode":"Optional","trust_server_certificate":true}]""");

        Assert.Equal(0, run.Exit);
        var listed = Reload().GetAllServers().Single();
        Assert.True(listed.TrustServerCertificate);
        Assert.Empty(UnconfirmedFor(listed));
    }

    [Fact]
    public async Task AnUpdate_ThatOmitsAField_StillRefusesAnUnconfirmedListedValue()
    {
        await AddSql01Async();
        EditListedEntry(entry =>
        {
            entry["TrustServerCertificate"] = true;
            entry["EncryptMode"] = "Optional";
        });

        var run = await RunVerbAsync(
            ["--add-server"], """[{"host":"sql01","auth":"sql","username":"monitor","password":"second-value","encrypt_mode":"Mandatory"}]""");

        Assert.Equal(1, run.Exit);
        Assert.Contains("TrustServerCertificate", run.Err);
        Assert.DoesNotContain("encryption", run.Err);
    }

    [Fact]
    public async Task ReadOnlyIntent_IsPartOfTheServersIdentity_SoItAddsASecondEntry()
    {
        await RunVerbAsync(["--add-server"], """[{"host":"sql01"}]""");

        var run = await RunVerbAsync(["--add-server"], """[{"host":"sql01","read_only_intent":true}]""");

        Assert.Equal(0, run.Exit);
        var listed = Reload().GetAllServers();
        Assert.Equal(2, listed.Count);
        Assert.Single(listed, s => s.ReadOnlyIntent);
    }

    /// <summary>Edits the first listed server in servers.json, as any local account can.</summary>
    private void EditListedEntry(Action<JsonObject> edit)
    {
        var root = JsonNode.Parse(File.ReadAllText(ServersFile))!.AsObject();
        edit(root["Servers"]!.AsArray()[0]!.AsObject());
        File.WriteAllText(ServersFile, root.ToJsonString());
    }

    private async Task<string> AddSql01Async()
    {
        await RunVerbAsync(["--add-server"], """[{"host":"sql01","auth":"sql","username":"monitor","password":"first-value"}]""");
        return Reload().GetAllServers().Single().Id;
    }

    [Theory]
    [InlineData("QueryStoreReplica", "replica01", "Query Store replica")]
    [InlineData("QueryStoreReplica", "np:replica01", "Query Store replica")]
    [InlineData("UtilityDatabase", "utility", "utility database")]
    [InlineData("TrustServerCertificate", true, "TrustServerCertificate")]
    [InlineData("EncryptMode", "Optional", "encryption")]
    public async Task AReAdd_OfAnEntryHoldingAnUnconfirmedSetting_IsRefused_AndChangesNothing(string property, object value, string named)
    {
        var id = await AddSql01Async();
        EditListedEntry(entry => entry[property] = value is bool flag ? JsonValue.Create(flag) : JsonValue.Create((string)value));
        var serversBefore = File.ReadAllBytes(ServersFile);
        var hostsBefore = File.ReadAllBytes(HostsFile);

        var run = await RunVerbAsync(["--add-server"], """[{"host":"sql01","auth":"sql","username":"monitor2","password":"second-value"}]""");

        Assert.Equal(1, run.Exit);
        Assert.Contains(named, run.Err);
        Assert.Contains("Add Server window", run.Err);
        Assert.DoesNotContain("second-value", run.Err);
        Assert.Empty(run.Tested);
        Assert.Equal(serversBefore, File.ReadAllBytes(ServersFile));
        Assert.Equal(hostsBefore, File.ReadAllBytes(HostsFile));
        Assert.Equal(("monitor", "first-value"), _store.GetCredential(id));
    }

    [Fact]
    public async Task AReAdd_OfAnEntryWithSettingsThisUserConfirmed_GoesThrough()
    {
        var id = await AddSql01Async();
        var manager = Reload();
        var listed = manager.GetAllServers().Single();
        listed.QueryStoreReplica = "replica01";
        listed.UtilityDatabase = "utility";
        listed.TrustServerCertificate = true;
        listed.EncryptMode = "Optional";
        manager.UpdateServerSettings(listed);
        manager.ConfirmHost(listed);

        var run = await RunVerbAsync(["--add-server"], """[{"host":"sql01","auth":"sql","username":"monitor2","password":"second-value"}]""");

        Assert.Equal(0, run.Exit);
        Assert.Equal(["sql01"], run.Tested);
        Assert.Equal(("monitor2", "second-value"), _store.GetCredential(id));
    }

    [Fact]
    public async Task AReAdd_OfAnEntryWithBlankEncryption_GoesThrough_BecauseBlankIsMandatory()
    {
        var id = await AddSql01Async();
        EditListedEntry(entry => entry["EncryptMode"] = "");

        var run = await RunVerbAsync(["--add-server"], """[{"host":"sql01","auth":"sql","username":"monitor2","password":"second-value"}]""");

        Assert.Equal(0, run.Exit);
        Assert.Equal(("monitor2", "second-value"), _store.GetCredential(id));
    }

    [Theory]
    [InlineData("Mandatory", "Mandatory")]
    [InlineData("Strict", "Strict")]
    [InlineData("Optional", "Optional")]
    [InlineData("", "Mandatory")]
    [InlineData(null, "Mandatory")]
    [InlineData("SomethingElse", "Mandatory")]
    public void TheTestedEncryption_FailsClosed_ForABlankOrUnknownMode(string? mode, string expected)
    {
        var option = ServerEntryRules.EncryptOptionFor(mode);

        Assert.Equal(Microsoft.Data.SqlClient.SqlConnectionEncryptOption.Parse(expected), option);
    }

    [Fact]
    public async Task AReAdd_OfAnEntryWhoseIdIsNotAGuid_IsRefused_AndChangesNothing()
    {
        await AddSql01Async();
        EditListedEntry(entry => entry["Id"] = "not-a-guid");
        var serversBefore = File.ReadAllBytes(ServersFile);
        var hostsBefore = File.ReadAllBytes(HostsFile);
        var keysBefore = StoredKeys();

        var run = await RunVerbAsync(["--add-server"], """[{"host":"sql01","auth":"sql","username":"monitor2","password":"second-value"}]""");

        Assert.Equal(1, run.Exit);
        Assert.Contains("sql01", run.Err);
        Assert.Contains("not a GUID", run.Err);
        Assert.DoesNotContain("Updated", run.Out);
        Assert.DoesNotContain("Credentials were stored", run.Out);
        Assert.Empty(run.Tested);
        Assert.Equal(serversBefore, File.ReadAllBytes(ServersFile));
        Assert.Equal(hostsBefore, File.ReadAllBytes(HostsFile));
        Assert.Equal(keysBefore, StoredKeys());
    }

    /// <summary>A store that refuses (or throws on) the save for one login. A refused update deletes the old entry first, the worst a store can do.</summary>
    private sealed class RefusingStore(InMemoryCredentialStore inner, string username, bool throwInstead) : ICredentialStore
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
            return throwInstead ? throw new InvalidOperationException("The credential store failed.") : false;
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ACredentialFailureAtTheSecondOfTwoEntries_PutsBackTheListTheHostRecordAndTheCredentials(bool throws)
    {
        var existingId = await AddSql01Async();
        var serversBefore = File.ReadAllBytes(ServersFile);
        var hostsBefore = File.ReadAllBytes(HostsFile);
        var keysBefore = StoredKeys();
        var store = new RefusingStore(_store, "user2", throws);

        var run = await RunVerbAsync(
            ["--add-server"],
            """[{"host":"sql02","auth":"sql","username":"user1","password":"value-one"},{"host":"sql03","auth":"sql","username":"user2","password":"value-two"}]""",
            store: store);

        Assert.Equal(1, run.Exit);
        Assert.Contains("put back", run.Err);
        Assert.DoesNotContain("Added", run.Out);
        Assert.DoesNotContain("value-one", run.Err);
        Assert.DoesNotContain("value-two", run.Err);
        Assert.Equal(serversBefore, File.ReadAllBytes(ServersFile));
        Assert.Equal(hostsBefore, File.ReadAllBytes(HostsFile));
        Assert.Equal(keysBefore, StoredKeys());
        Assert.Equal(("monitor", "first-value"), _store.GetCredential(existingId));
    }

    [Fact]
    public async Task ACredentialFailureOnAReAdd_LeavesTheOldCredentialAsItWas()
    {
        var id = await AddSql01Async();
        var serversBefore = File.ReadAllBytes(ServersFile);
        var hostsBefore = File.ReadAllBytes(HostsFile);
        var store = new RefusingStore(_store, "monitor2", throwInstead: false);

        var run = await RunVerbAsync(
            ["--add-server"], """[{"host":"sql01","auth":"sql","username":"monitor2","password":"second-value"}]""", store: store);

        Assert.Equal(1, run.Exit);
        Assert.Equal(("monitor", "first-value"), _store.GetCredential(id));
        Assert.Equal(serversBefore, File.ReadAllBytes(ServersFile));
        Assert.Equal(hostsBefore, File.ReadAllBytes(HostsFile));
    }

    [Fact]
    public async Task AFirstRunThatFails_LeavesAnEmptyServerList_AndNoHostRecordEntry_AndNoCredential()
    {
        var store = new RefusingStore(_store, "user1", throwInstead: false);

        var run = await RunVerbAsync(
            ["--add-server"], """[{"host":"sql02","auth":"sql","username":"user1","password":"value-one"}]""", store: store);

        Assert.Equal(1, run.Exit);
        /* The manager makes an empty list file when it opens a folder with none. An empty list is harmless, so it is kept. */
        Assert.True(File.Exists(ServersFile));
        using (var doc = JsonDocument.Parse(File.ReadAllText(ServersFile)))
            Assert.Equal(0, doc.RootElement.GetProperty("Servers").GetArrayLength());
        Assert.False(File.Exists(HostsFile));
        Assert.Empty(StoredKeys());
    }

    [Theory]
    [InlineData("""[{"host":"SQL01","auth":"sql","username":"monitor2","password":"second-value"}]""")]
    [InlineData("""[{"host":"sql01","database":"Utility","auth":"sql","username":"monitor2","password":"second-value"}]""")]
    public async Task AReAdd_SpelledInAnotherLetterCase_IsRefusedAndAddsNoSecondEntry(string reAdd)
    {
        var lower = reAdd.Contains("Utility")
            ? """[{"host":"sql01","database":"utility","auth":"sql","username":"monitor","password":"first-value"}]"""
            : """[{"host":"sql01","auth":"sql","username":"monitor","password":"first-value"}]""";
        await RunVerbAsync(["--add-server"], lower);
        var id = Reload().GetAllServers().Single().Id;
        var serversBefore = File.ReadAllBytes(ServersFile);
        var hostsBefore = File.ReadAllBytes(HostsFile);

        var run = await RunVerbAsync(["--add-server"], reAdd);

        Assert.Equal(1, run.Exit);
        Assert.Contains("another letter case", run.Err);
        Assert.Contains("spelled as the entry has them", run.Err);
        Assert.Empty(run.Tested);
        Assert.Equal(serversBefore, File.ReadAllBytes(ServersFile));
        Assert.Equal(hostsBefore, File.ReadAllBytes(HostsFile));
        Assert.Equal(("monitor", "first-value"), _store.GetCredential(id));
        Assert.Single(Reload().GetAllServers());
    }

    private const string ReAddSql01 = """[{"host":"sql01","auth":"sql","username":"monitor2","password":"second-value"}]""";

    [Fact]
    public async Task AReAdd_OfAnEntryWhoseRecordIsForAnotherHost_TreatsEverySettingAsUnconfirmed()
    {
        /* This user confirmed sql01 (with Optional encryption) under one Id. servers.json now names sql02 under that Id. */
        var id = await AddSql01Async();
        var manager = Reload();
        var listed = manager.GetAllServers().Single();
        listed.EncryptMode = "Optional";
        manager.UpdateServerSettings(listed);
        manager.ConfirmHost(listed);
        EditListedEntry(entry => entry["ServerName"] = "sql02");
        var serversBefore = File.ReadAllBytes(ServersFile);
        var hostsBefore = File.ReadAllBytes(HostsFile);

        var run = await RunVerbAsync(["--add-server"], ReAddSql01.Replace("sql01", "sql02"));

        Assert.Equal(1, run.Exit);
        Assert.Contains("encryption", run.Err);
        Assert.Empty(run.Tested);
        Assert.Equal(serversBefore, File.ReadAllBytes(ServersFile));
        Assert.Equal(hostsBefore, File.ReadAllBytes(HostsFile));
        Assert.Equal(("monitor", "first-value"), _store.GetCredential(id));
    }

    [Fact]
    public async Task AHostWithAProtocolPrefix_IsNotTheListedEntryWhoseHostAndDatabaseJoinToTheSameName()
    {
        await RunVerbAsync(["--add-server"], """[{"host":"tcp","database":"sql01","auth":"sql","username":"monitor","password":"first-value"}]""");
        var id = Reload().GetAllServers().Single().Id;
        var serversBefore = File.ReadAllBytes(ServersFile);
        var hostsBefore = File.ReadAllBytes(HostsFile);

        var run = await RunVerbAsync(["--add-server"], """[{"host":"tcp:sql01","auth":"sql","username":"monitor2","password":"second-value"}]""");

        Assert.Equal(1, run.Exit);
        Assert.Contains("different host or database", run.Err);
        Assert.Empty(run.Tested);
        Assert.Equal(serversBefore, File.ReadAllBytes(ServersFile));
        Assert.Equal(hostsBefore, File.ReadAllBytes(HostsFile));
        Assert.Equal(("monitor", "first-value"), _store.GetCredential(id));
    }

    [Fact]
    public async Task AReAdd_WhenAnotherListedEntryHasTheSameId_IsRefused_AndChangesNothing()
    {
        var id = await AddSql01Async();
        var root = JsonNode.Parse(File.ReadAllText(ServersFile))!.AsObject();
        var servers = root["Servers"]!.AsArray();
        var other = JsonNode.Parse(servers[0]!.ToJsonString())!.AsObject();
        other["ServerName"] = "sql02";
        other["DisplayName"] = "second-entry";
        servers.Insert(0, other);
        File.WriteAllText(ServersFile, root.ToJsonString());
        var serversBefore = File.ReadAllBytes(ServersFile);
        var hostsBefore = File.ReadAllBytes(HostsFile);

        var run = await RunVerbAsync(["--add-server"], ReAddSql01);

        Assert.Equal(1, run.Exit);
        Assert.Contains("second-entry", run.Err);
        Assert.Contains("same Id", run.Err);
        Assert.Empty(run.Tested);
        Assert.Equal(serversBefore, File.ReadAllBytes(ServersFile));
        Assert.Equal(hostsBefore, File.ReadAllBytes(HostsFile));
        Assert.Equal(("monitor", "first-value"), _store.GetCredential(id));
    }

    private const string ManagedIdentityClientId = "00000000-0000-0000-0000-000000000001";

    private async Task AddManagedIdentityAsync()
    {
        var added = await RunVerbAsync(["--add-server"], """[{"host":"sql01","auth":"managedidentity"}]""");
        Assert.Equal(0, added.Exit);
    }

    [Fact]
    public async Task AReAdd_OfAManagedIdentityEntryWithAnUnconfirmedClientId_IsRefused_AndChangesNothing()
    {
        await AddManagedIdentityAsync();
        EditListedEntry(entry => entry["ManagedIdentityClientId"] = ManagedIdentityClientId);
        var serversBefore = File.ReadAllBytes(ServersFile);
        var hostsBefore = File.ReadAllBytes(HostsFile);

        var run = await RunVerbAsync(["--add-server"], """[{"host":"sql01","auth":"managedidentity"}]""");

        Assert.Equal(1, run.Exit);
        Assert.Contains("a managed identity client id", run.Err);
        Assert.Empty(run.Tested);
        Assert.Equal(serversBefore, File.ReadAllBytes(ServersFile));
        Assert.Equal(hostsBefore, File.ReadAllBytes(HostsFile));
    }

    [Fact]
    public async Task AReAdd_OfAManagedIdentityEntryWhoseClientIdThisUserConfirmed_GoesThrough()
    {
        await AddManagedIdentityAsync();
        EditListedEntry(entry => entry["ManagedIdentityClientId"] = ManagedIdentityClientId);
        var manager = Reload();
        manager.ConfirmHost(manager.GetAllServers().Single());

        var run = await RunVerbAsync(["--add-server"], """[{"host":"sql01","auth":"managedidentity"}]""");

        Assert.Equal(0, run.Exit);
        Assert.Equal(["sql01"], run.Tested);
    }

    [Fact]
    public async Task AHostRecordThatCannotBeWritten_FailsTheRun_AndPutsEverythingBack()
    {
        /* A folder where the record's temporary file goes: the write fails, and the manager reports that instead of throwing. */
        Directory.CreateDirectory(HostsFile + ".tmp");

        var run = await RunVerbAsync(["--add-server"], ReAddSql01);

        Assert.Equal(1, run.Exit);
        Assert.Contains("could not save", run.Err);
        Assert.Contains("put back", run.Err);
        Assert.DoesNotContain("Added", run.Out);
        Assert.Empty(ServerNames());
        Assert.False(File.Exists(HostsFile));
        Assert.Empty(StoredKeys());
    }

    [Theory]
    [InlineData("-PSecret1")]
    [InlineData("-Password:Secret1")]
    public async Task AnUnknownOptionThatCarriesAPassword_IsNamedByItsPosition_NotItsText(string option)
    {
        var run = await RunVerbAsync(["--add-server", "--username", "sa", option], "");

        Assert.Equal(1, run.Exit);
        Assert.DoesNotContain("Secret1", run.Err);
        Assert.DoesNotContain("Secret1", run.Out);
        Assert.Contains("argument 3", run.Err);
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
