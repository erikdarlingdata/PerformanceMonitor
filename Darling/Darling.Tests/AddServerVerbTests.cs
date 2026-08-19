/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2256: the <c>--add-server</c> verb, which exists because a headless host had no supported way to register a
/// monitored server at all.
///
/// <para><b>Why it was impossible.</b> <c>darling.json</c> seeds the registry only while it is empty, so file
/// edits after the first start are ignored (#2254); the web surface keeps <c>add_servers</c> off
/// <c>/api/read/*</c> deliberately because it writes; and there was no CLI verb. The field report ran the
/// service on Windows Server 2012, which cannot run the Viewer, so the only remaining routes were a GUI on
/// another machine or standing up an MCP client.</para>
///
/// <para><b>What is pinned here</b> is the part that has no store in it: verb recognition, the stdin contract,
/// and the result formatting plus its exit-code policy. The registration itself is
/// <c>DarlingMcpServerAdminTools.AddServers</c>, already covered by its own tests — the verb deliberately adds
/// no second implementation of validation, dedupe, probing, encryption or identity computation.</para>
/// </summary>
public sealed class AddServerVerbTests
{
    [Theory]
    [InlineData("--add-server")]
    [InlineData("--add-servers")]
    [InlineData("--ADD-SERVER")]
    public void BothSpellingsAreRecognized_CaseInsensitively(string arg)
    {
        Assert.True(DarlingCliCommands.IsAddServerVerb(arg));

        /* And the classifier must dispatch it rather than fall through to starting the host — the #1581
           incident was a verb that reached a real startup and spawned a second instance. */
        Assert.Equal(StartupAction.RunKnownVerb, DarlingCliCommands.ClassifyStartupArgs(new[] { arg }));
    }

    [Fact]
    public void TheVerbIsDiscoverable_FromHelp()
    {
        Assert.Contains("--add-server", DarlingCliCommands.UsageText(), StringComparison.Ordinal);
    }

    /// <summary>
    /// Empty stdin must EXPLAIN itself on stdout and change nothing.
    ///
    /// <para>Stdout rather than stderr is the [#2097] lesson: in the PowerShell ISE, remoting sessions and some
    /// integrated terminals stderr is not surfaced, so a verb that writes only there reads as hung — which is
    /// exactly how the first setup step was reported as broken.</para>
    /// </summary>
    [Fact]
    public async Task EmptyStdin_ExplainsItselfOnStdout_AndChangesNothing()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        /* No config path and no store: it must return before touching either, which is itself the assertion —
           a store connection here would throw rather than return 1. */
        var exit = await DarlingCliCommands.AddServerAsync(
            configPath: null, input: new StringReader(string.Empty), output: output, error: error,
            cancellationToken: CancellationToken.None);

        Assert.Equal(1, exit);
        Assert.Equal(string.Empty, error.ToString());

        var text = output.ToString();
        Assert.Contains("stdin", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("--add-server", text, StringComparison.Ordinal);
        /* The reason the password is not an argument, stated where the operator will look for it. */
        Assert.Contains("process list", text, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task WhitespaceOnlyStdin_IsTreatedAsEmpty()
    {
        var output = new StringWriter();
        var exit = await DarlingCliCommands.AddServerAsync(
            null, new StringReader("   \r\n  "), output, new StringWriter(), CancellationToken.None);

        Assert.Equal(1, exit);
        Assert.Contains("stdin", output.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>A server that landed: the ADDED line, and the sentence that saves the operator a restart they do
    /// not need — the registry write bumps <c>config_version</c>, which the worker polls every sweep.</summary>
    [Fact]
    public void AnAddedServer_ReportsItAndSaysNoRestartIsNeeded()
    {
        var (lines, exit) = DarlingCliCommands.FormatAddServerOutcome(
            """{"added":1,"skipped":0,"failed":0,"results":[{"server":"sql01","status":"added","detail":"SQL major version 16"}]}""");

        Assert.Equal(0, exit);
        Assert.Contains(lines, l => l.Contains("[ADDED] sql01", StringComparison.Ordinal));
        Assert.Contains(lines, l => l.Contains("SQL major version 16", StringComparison.Ordinal));
        Assert.Contains(lines, l => l.Contains("1 added, 0 already registered, 0 failed.", StringComparison.Ordinal));
        Assert.Contains(lines, l => l.Contains("no restart is needed", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>Re-running the same file is idempotent, not a failure — so a batch of pure duplicates exits 0
    /// and does NOT claim a restart is pending, because nothing changed.</summary>
    [Fact]
    public void PureDuplicates_ExitZero_AndPromiseNoReload()
    {
        var (lines, exit) = DarlingCliCommands.FormatAddServerOutcome(
            """{"added":0,"skipped":2,"failed":0,"results":[{"server":"a","status":"duplicate","detail":"already registered"},{"server":"b","status":"duplicate","detail":"already registered"}]}""");

        Assert.Equal(0, exit);
        Assert.Equal(2, lines.Count(l => l.Contains("[SKIP]", StringComparison.Ordinal)));
        Assert.DoesNotContain(lines, l => l.Contains("no restart", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>Anything that failed is a non-zero exit, even alongside a success — the verb is usable as a
    /// deployment gate, and a partial batch must not read as clean.</summary>
    [Fact]
    public void AnyFailure_ExitsNonZero_EvenBesideASuccess()
    {
        var (lines, exit) = DarlingCliCommands.FormatAddServerOutcome(
            """{"added":1,"skipped":0,"failed":1,"results":[{"server":"good","status":"added","detail":"ok"},{"server":"bad","status":"connection_failed","detail":"login failed"}]}""");

        Assert.Equal(1, exit);
        Assert.Contains(lines, l => l.Contains("[FAIL] bad", StringComparison.Ordinal));
        Assert.Contains(lines, l => l.Contains("login failed", StringComparison.Ordinal));
    }

    /// <summary>Nothing landed at all — an empty array, or every entry rejected — must not report success to a
    /// script. A verb that changed nothing and exits 0 is the failure mode this policy exists for.</summary>
    [Theory]
    [InlineData("""{"added":0,"skipped":0,"failed":0,"results":[]}""")]
    [InlineData("""{"added":0,"skipped":0,"failed":1,"results":[{"server":"x","status":"invalid","detail":"unsupported auth"}]}""")]
    public void NothingLanded_ExitsNonZero(string json)
    {
        var (_, exit) = DarlingCliCommands.FormatAddServerOutcome(json);

        Assert.Equal(1, exit);
    }

    /// <summary>The whole-payload rejection shape carries no <c>results</c> array (the tool returns it without
    /// opening the store when the JSON is not an array at all), so the formatter must render its message rather
    /// than throw on the missing property.</summary>
    [Fact]
    public void AWholePayloadRejection_IsRenderedNotThrown()
    {
        var (lines, exit) = DarlingCliCommands.FormatAddServerOutcome(
            """{"status":"invalid","message":"servers_json must be a JSON array"}""");

        Assert.Equal(1, exit);
        Assert.Contains(lines, l => l.Contains("must be a JSON array", StringComparison.Ordinal));
    }

    /// <summary>
    /// A store failure AFTER the request parsed does not arrive as JSON at all: <c>AddServersAsync</c>'s
    /// catch-all returns <c>McpHelpers.FormatError</c>, which is plain text. That text IS the message the
    /// operator needs, so it must be surfaced verbatim rather than buried under a "could not parse" wrapper —
    /// which is what happened before, precisely when the verb is being used as a deployment gate.
    /// </summary>
    [Fact]
    public void APlainTextStoreError_IsSurfacedVerbatim_NotWrapped()
    {
        var (lines, exit) = DarlingCliCommands.FormatAddServerOutcome(
            "Error during add_servers: 57P01: terminating connection due to administrator command");

        Assert.Equal(1, exit);
        Assert.Contains(lines, l => l.Contains("57P01", StringComparison.Ordinal));
        Assert.DoesNotContain(lines, l => l.Contains("Could not parse", StringComparison.Ordinal));
    }

    /// <summary>Something that LOOKED like JSON and was not still says so, and still shows the payload — the
    /// two cases are told apart by shape so neither hides the other.</summary>
    [Fact]
    public void MalformedJson_SaysSo_AndShowsThePayload()
    {
        var (lines, exit) = DarlingCliCommands.FormatAddServerOutcome("{\"added\":1, oops");

        Assert.Equal(1, exit);
        Assert.Contains(lines, l => l.Contains("Could not parse", StringComparison.Ordinal));
        Assert.Contains(lines, l => l.Contains("oops", StringComparison.Ordinal));
    }
}
