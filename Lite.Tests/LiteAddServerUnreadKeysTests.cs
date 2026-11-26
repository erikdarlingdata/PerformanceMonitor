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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9780: an <c>add_servers</c> / <c>--add-server</c> entry key that Lite's parse does not read is dropped, so <c>name</c> for <c>display_name</c> adds the
/// server under its host and a misspelt <c>trustServerCertifcate</c> adds it with the default. The answer names each such key, through the MCP tool and
/// through the verb. Both run over a temp folder and an in-memory credential store with a fake connection test.
/// </summary>
[InMemoryCredentials]
public sealed class LiteAddServerUnreadKeysTests : IDisposable
{
    private const string Secret = "not-a-real-secret-value";
    private readonly string _root = Path.Combine(Path.GetTempPath(), "LiteAddServerUnreadKeysTests_" + Guid.NewGuid().ToString("N"));
    private readonly InMemoryCredentialStore _store = new();

    public LiteAddServerUnreadKeysTests() => Directory.CreateDirectory(_root);

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); }
        catch (IOException) { /* a temp folder left behind is harmless */ }
    }

    private string HostsFile => Path.Combine(_root, "user", ServerHostBindings.FileName);

    private ServerManager NewManager() => new(_root, null, HostsFile, new CredentialService(_store));

    private static Task<string?> Reachable(ServerConnection server, string? user, string? password, CancellationToken ct) => Task.FromResult<string?>(null);

    private static Task<string?> Unreachable(ServerConnection server, string? user, string? password, CancellationToken ct) =>
        Task.FromResult<string?>("the certificate chain was issued by an authority that is not trusted");

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement.Clone();

    private static string[] IgnoredKeyNames(JsonElement row) =>
        row.TryGetProperty("ignored_keys", out var ignored) ? ignored.EnumerateArray().Select(k => k.GetProperty("key").GetString()!).ToArray() : [];

    private static string? DidYouMean(JsonElement row, string key) =>
        row.GetProperty("ignored_keys").EnumerateArray().Single(k => k.GetProperty("key").GetString() == key).GetProperty("did_you_mean").GetString();

    private async Task<(int Exit, string Out, string Err)> RunVerbAsync(string stdin, Func<ServerConnection, string?>? connect = null)
    {
        var machine = new AddServerCommand.Machine(
            () => NewManager(),
            () => false,
            (server, user, password) => Task.FromResult(connect?.Invoke(server)),
            "TESTDOMAIN\\collector",
            Path.GetDirectoryName(HostsFile)!);
        using var output = new StringWriter();
        using var error = new StringWriter();
        var exit = await AddServerCommand.RunAsync(["--add-server"], new StringReader(stdin), true, output, error, machine);
        return (exit, output.ToString(), error.ToString());
    }

    [Fact]
    public async Task Mcp_NamesAnUnreadKeyBesideItsRow_AndStillAddsTheServers()
    {
        var manager = NewManager();

        var answer = await McpServerRegistryTools.AddServersCore(
            manager, null, "[{\"host\":\"test-01\",\"name\":\"Primary\"},{\"host\":\"test-02\",\"trustServerCertifcate\":true}]", Reachable);

        var root = Parse(answer);
        Assert.Equal("ok", root.GetProperty("status").GetString());
        Assert.Equal(2, root.GetProperty("added").GetInt32());
        var rows = root.GetProperty("results").EnumerateArray().ToArray();
        Assert.Equal(["name"], IgnoredKeyNames(rows[0]));
        Assert.Equal("display_name", DidYouMean(rows[0], "name"));
        Assert.Equal(["trustServerCertifcate"], IgnoredKeyNames(rows[1]));
        Assert.Equal("trust_server_certificate", DidYouMean(rows[1], "trustServerCertifcate"));

        /* The entries were still processed, with the dropped key not applied. */
        var listed = NewManager().GetAllServers().OrderBy(s => s.ServerName, StringComparer.Ordinal).ToArray();
        Assert.Equal(["test-01", "test-02"], listed.Select(s => s.DisplayName).ToArray());
        Assert.False(listed[1].TrustServerCertificate);
    }

    [Fact]
    public async Task Mcp_AKnownKeyInAnotherLetterCase_IsNotReported()
    {
        var answer = await McpServerRegistryTools.AddServersCore(
            NewManager(), null, "[{\"Host\":\"test-01\",\"Display_Name\":\"Primary\",\"TRUST_SERVER_CERTIFICATE\":false,\"// note\":\"kept\"}]", Reachable);

        var root = Parse(answer);
        Assert.Equal("ok", root.GetProperty("status").GetString());
        var row = Assert.Single(root.GetProperty("results").EnumerateArray());
        Assert.False(row.TryGetProperty("ignored_keys", out _));
        Assert.Equal("Primary", row.GetProperty("server").GetString());
    }

    [Fact]
    public async Task Mcp_ADarlingOnlyKey_IsNamedWithNoHint_BecauseLiteDoesNotReadIt()
    {
        var answer = await McpServerRegistryTools.AddServersCore(
            NewManager(), null, "[{\"host\":\"test-01\",\"port\":1433,\"capture_plans\":true,\"engine\":\"sqlserver\"}]", Reachable);

        var row = Assert.Single(Parse(answer).GetProperty("results").EnumerateArray());
        Assert.Equal(["port", "capture_plans"], IgnoredKeyNames(row));
        Assert.Equal(JsonValueKind.Null, row.GetProperty("ignored_keys").EnumerateArray().First().GetProperty("did_you_mean").ValueKind);
    }

    [Fact]
    public async Task Mcp_ARefusedRun_NamesTheKeyThatCouldBeTheReason_AndNeverAValue()
    {
        var manager = NewManager();

        var answer = await McpServerRegistryTools.AddServersCore(
            manager, null, "[{\"host\":\"test-01\",\"auth\":\"sql\",\"username\":\"monitor\",\"password\":\"" + Secret + "\",\"trustServerCertifcate\":true,\"pasword\":\"" + Secret + "\"}]", Unreachable);

        var root = Parse(answer);
        Assert.Equal("refused", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("trustServerCertifcate", message, StringComparison.Ordinal);
        Assert.Contains("pasword", message, StringComparison.Ordinal);
        Assert.DoesNotContain(Secret, answer, StringComparison.Ordinal);
        Assert.Empty(manager.GetAllServers());
    }

    [Fact]
    public async Task Mcp_AnEntryRefusedWhileRead_NamesItsUnreadKeys()
    {
        var manager = NewManager();

        var answer = await McpServerRegistryTools.AddServersCore(manager, null, "[{\"hostname\":\"test-01\",\"displayName\":\"Primary\"}]", Reachable);

        var root = Parse(answer);
        Assert.NotEqual("ok", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("hostname", message, StringComparison.Ordinal);
        Assert.Contains("displayName (did you mean display_name?)", message, StringComparison.Ordinal);
        Assert.Empty(manager.GetAllServers());
    }

    [Fact]
    public async Task Mcp_TheRemoveServerUndoEntry_HasNoUnreadKey()
    {
        var manager = NewManager();
        await McpServerRegistryTools.AddServersCore(
            manager, null,
            "[{\"host\":\"test-01\",\"display_name\":\"Primary\",\"database\":\"Utility\",\"auth\":\"sql\",\"username\":\"monitor\",\"password\":\"" + Secret
            + "\",\"encrypt_mode\":\"Strict\",\"trust_server_certificate\":false,\"read_only_intent\":true,\"multi_subnet_failover\":true}]", Reachable);
        var removed = Parse(await McpServerRegistryTools.RemoveServerCore(manager, new McpServerAdminHooks { RemoveServer = s => { manager.DeleteServer(s.Id); return Task.FromResult(true); } }, "Primary"));
        var previous = removed.GetProperty("previous").GetRawText();

        var (entries, problem) = AddServerCommand.ReadEntries(new AddServerFlags(), new StringReader("[" + previous + "]"), inputRedirected: true);

        Assert.Null(problem);
        Assert.Null(Assert.Single(entries!).UnreadKeys);
    }

    [Fact]
    public async Task Verb_PrintsOneLineUnderEachEntryWithAnUnreadKey_AndExitsZero()
    {
        var run = await RunVerbAsync(
            "[{\"host\":\"test-01\",\"name\":\"Primary\"},{\"host\":\"test-02\",\"trustServerCertifcate\":true,\"owner\":\"ops\"},{\"host\":\"test-03\",\"Display_Name\":\"Third\"}]");

        Assert.Equal(AddServerCommand.ExitAdded, run.Exit);
        var lines = run.Out.Split(Environment.NewLine);
        var first = Array.FindIndex(lines, l => l.StartsWith("Added test-01 ", StringComparison.Ordinal));
        var second = Array.FindIndex(lines, l => l.StartsWith("Added test-02 ", StringComparison.Ordinal));
        var third = Array.FindIndex(lines, l => l.StartsWith("Added Third ", StringComparison.Ordinal));
        Assert.True(first >= 0 && second >= 0 && third >= 0);
        Assert.Contains("name (did you mean display_name?)", lines[first + 1], StringComparison.Ordinal);
        Assert.Contains("trustServerCertifcate (did you mean trust_server_certificate?), owner", lines[second + 1], StringComparison.Ordinal);
        Assert.DoesNotContain("Not read", lines[third + 1], StringComparison.Ordinal);
        Assert.Equal(2, lines.Count(l => l.Contains("Not read", StringComparison.Ordinal)));
    }

    [Fact]
    public async Task Verb_ACleanRun_PrintsNothingExtra()
    {
        var run = await RunVerbAsync("[{\"host\":\"test-01\",\"DISPLAY_NAME\":\"Primary\",\"Auth\":\"windows\"}]");

        Assert.Equal(AddServerCommand.ExitAdded, run.Exit);
        Assert.DoesNotContain("Not read", run.Out, StringComparison.Ordinal);
        Assert.DoesNotContain("Not read", run.Err, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Verb_ARefusedRun_NamesTheUnreadKeyOnStandardError_AndNeverAValue()
    {
        var run = await RunVerbAsync(
            "[{\"host\":\"test-01\",\"auth\":\"sql\",\"username\":\"monitor\",\"password\":\"" + Secret + "\",\"trustServerCertifcate\":true,\"pasword\":\"" + Secret + "\"}]",
            connect: _ => "the certificate chain was issued by an authority that is not trusted");

        Assert.Equal(AddServerCommand.ExitRefused, run.Exit);
        Assert.Contains("trustServerCertifcate (did you mean trust_server_certificate?)", run.Err, StringComparison.Ordinal);
        Assert.DoesNotContain(Secret, run.Out + run.Err, StringComparison.Ordinal);
    }
}
