// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4605: on the web compose path the client <c>CommandTimeout</c> used to equal the role's server-side
/// <c>statement_timeout</c> (both 60 s), so the two raced. When Npgsql's client timer won, the run threw an
/// <see cref="NpgsqlException"/> wrapping a <see cref="TimeoutException"/> that landed in the generic arm
/// and recorded Error. The client deadline now sits above the server's, and that exception shape is
/// classified as a timeout with the same text 57014 gives, but only when it fired while the statement executed;
/// a pool-wait or connect timeout has the same exception shape and is answered as the server error it is.
/// </summary>
public class ComposeClientTimeoutAttributionTests
{
    [Fact]
    public void WebComposeClientDeadline_IsStrictlyAboveTheServerStatementTimeout()
    {
        const int server = 60;
        var client = DarlingWebEndpoints.ComposeClientDeadlineSeconds(server, DarlingWebEndpoints.ComposeClientDeadlineHeadroomSeconds);

        Assert.True(client > server, $"web compose client deadline {client}s must exceed the server statement_timeout {server}s so 57014 wins the race");
        Assert.True(DarlingWebEndpoints.ComposeClientDeadlineHeadroomSeconds > 0);
    }

    [Fact]
    public void McpComposeCaller_KeepsItsDeadline_NoHeadroom()
    {
        Assert.Equal(60, DarlingWebEndpoints.ComposeClientDeadlineSeconds(60, 0));
    }

    private static NpgsqlException StatementPhaseTimeout() =>
        new("Exception while reading from stream", new TimeoutException("Timeout during reading attempt"));

    [Fact]
    public void StatementPhaseClientTimeout_OnTheWebPath_MapsToTimeoutWithTheActionableText()
    {
        var ex = new DarlingWebEndpoints.ComposeStatementClientTimeoutException(StatementPhaseTimeout());

        var outcome = DarlingWebEndpoints.FromRunException(ex, remapClientTimeout: true);

        Assert.False(outcome.IsServerError);
        Assert.Null(outcome.Fault);
        Assert.Equal("57014", outcome.AuthorSqlState);
        Assert.Equal(DarlingWebEndpoints.StatementTimeoutText, outcome.Error);
        Assert.Equal(
            DarlingWebEndpoints.FromPostgresException(new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014")).Error,
            outcome.Error);
    }

    [Fact]
    public void StatementPhaseClientTimeout_WithoutTheRemapFlag_KeepsTheGenericServerError()
    {
        var ex = new DarlingWebEndpoints.ComposeStatementClientTimeoutException(StatementPhaseTimeout());

        foreach (var outcome in new[] { DarlingWebEndpoints.FromRunException(ex), DarlingWebEndpoints.FromRunException(ex, remapClientTimeout: false) })
        {
            Assert.True(outcome.IsServerError);
            Assert.Null(outcome.AuthorSqlState);
            Assert.StartsWith("Error running query:", outcome.Error, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void PoolExhaustedTimeout_IsAServerErrorNamingTheCause_NotAStatementTimeout()
    {
        var ex = new NpgsqlException(
            "The connection pool has been exhausted, either raise 'Max Pool Size' (currently 24) or 'Timeout' (currently 15) in the connection string.",
            new TimeoutException());

        var outcome = DarlingWebEndpoints.FromRunException(ex, remapClientTimeout: true);

        Assert.True(outcome.IsServerError);
        Assert.NotEqual("57014", outcome.AuthorSqlState);
        Assert.Null(outcome.AuthorSqlState);
        Assert.Contains("had no free connection in time", outcome.Error, StringComparison.Ordinal);
        Assert.Contains("connection pool has been exhausted", outcome.Error, StringComparison.Ordinal);
        Assert.DoesNotContain("statement timeout", outcome.Error, StringComparison.Ordinal);
    }

    [Fact]
    public void ConnectTimeout_IsAServerErrorNamingTheCause_NotAStatementTimeout()
    {
        var ex = new NpgsqlException("Failed to connect to 10.0.0.1:5432", new TimeoutException("Timeout during connection attempt"));

        var outcome = DarlingWebEndpoints.FromRunException(ex, remapClientTimeout: true);

        Assert.True(outcome.IsServerError);
        Assert.Null(outcome.AuthorSqlState);
        Assert.Contains("could not connect to the store in time", outcome.Error, StringComparison.Ordinal);
        Assert.Contains("Failed to connect", outcome.Error, StringComparison.Ordinal);
        Assert.DoesNotContain("statement timeout", outcome.Error, StringComparison.Ordinal);
    }

    [Fact]
    public void OtherExceptions_StillMapToTheGenericServerError()
    {
        var plain = DarlingWebEndpoints.FromRunException(new InvalidOperationException("boom"), remapClientTimeout: true);
        Assert.True(plain.IsServerError);
        Assert.Equal("Error running query: boom", plain.Error);
        Assert.Null(plain.AuthorSqlState);

        var npgsqlOther = DarlingWebEndpoints.FromRunException(new NpgsqlException("connection refused", new System.IO.IOException("x")), remapClientTimeout: true);
        Assert.True(npgsqlOther.IsServerError);
    }

    // ---- wiring pins: the call sites, read from source with comments and strings stripped ----

    private static string Code(params string[] relative)
    {
        var dir = Path.GetDirectoryName(ThisFile())!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        var text = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(Path.Combine(new[] { dir! }.Concat(relative).ToArray())));
        return Regex.Replace(text.ReplaceLineEndings("\n"), @"\s+", " ");
    }

    private static string ThisFile([CallerFilePath] string path = "") => path;

    private static string WebCode() => Code("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");

    private static string CallArguments(string code, string callStart)
    {
        var at = code.IndexOf(callStart, StringComparison.Ordinal);
        Assert.True(at >= 0, $"call '{callStart}' not found");
        var end = code.IndexOf(");", at, StringComparison.Ordinal);
        Assert.True(end > at);
        return code[at..end];
    }

    [Fact]
    public void TheComposeRunRoute_PassesTheHeadroomAndTheRemapFlag()
    {
        var call = CallArguments(WebCode(), "await RunComposedPanelAsync(postgres, body, context.RequestAborted");

        Assert.Contains("ComposeClientDeadlineHeadroomSeconds", call, StringComparison.Ordinal);
        Assert.Contains("remapClientTimeout: true", call, StringComparison.Ordinal);
    }

    [Fact]
    public void TheMcpCaller_PassesNoHeadroomAndNoRemap()
    {
        var call = CallArguments(
            Code("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpCustomViewTools.cs"),
            "DarlingWebEndpoints.RunComposedPanelAsync(");

        Assert.DoesNotContain("Headroom", call, StringComparison.Ordinal);
        Assert.DoesNotContain("remapClientTimeout", call, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRunner_PassesTheClientDeadlineToTheMeasureAndAnnotationQueries()
    {
        var code = WebCode();

        Assert.Contains("var clientSeconds = ComposeClientDeadlineSeconds(", code, StringComparison.Ordinal);
        Assert.Contains("RunComposedQueryAsync(postgres, compiled!, clientSeconds,", code, StringComparison.Ordinal);
        Assert.Contains("RunAnnotationsAsync(postgres, plan!, runContext, clientSeconds,", code, StringComparison.Ordinal);
        Assert.DoesNotContain("composedQuerySeconds, cancellationToken", code.Replace("var composedQuerySeconds = await", string.Empty), StringComparison.Ordinal);
    }

    [Fact]
    public void TheGenericCatch_RemapsThroughTheFlag()
    {
        Assert.Contains("return FromRunException(ex, remapClientTimeout);", WebCode(), StringComparison.Ordinal);
    }
}
