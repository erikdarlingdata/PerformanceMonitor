// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Linq;
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
/// a failure to open the store connection has the same exception shape and is answered as the server error it is.
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

    public static TheoryData<string> OpenFailureShapes() => new()
    {
        { "The connection pool has been exhausted, either raise 'Max Pool Size' (currently 24) or 'Timeout' (currently 15) in the connection string." },
        { "Failed to connect to 10.0.0.1:5432" },
        { "The operation has timed out" },
        { "Exception while reading from stream" },
    };

    [Theory]
    [MemberData(nameof(OpenFailureShapes))]
    public void OpenTimeout_IsAServerErrorNamingTheOpen_NotAStatementTimeout(string innerMessage)
    {
        var inner = new NpgsqlException(innerMessage, new TimeoutException());
        var ex = new DarlingWebEndpoints.ComposeStoreOpenException(inner);

        foreach (var flag in new[] { true, false })
        {
            var outcome = DarlingWebEndpoints.FromRunException(ex, remapClientTimeout: flag);

            Assert.True(outcome.IsServerError);
            Assert.Null(outcome.AuthorSqlState);
            Assert.NotEqual("57014", outcome.AuthorSqlState);
            Assert.StartsWith("Error running query: could not get a store connection in time: ", outcome.Error, StringComparison.Ordinal);
            Assert.Contains(innerMessage, outcome.Error, StringComparison.Ordinal);
            Assert.DoesNotContain(DarlingWebEndpoints.StatementTimeoutText, outcome.Error, StringComparison.Ordinal);
        }

    }

    [Fact]
    public void OpenFailure_WithoutATimeout_AnswersCouldNotOpen()
    {
        var ex = new DarlingWebEndpoints.ComposeStoreOpenException(new NpgsqlException("boom", new System.Net.Sockets.SocketException()));

        foreach (var flag in new[] { true, false })
        {
            var outcome = DarlingWebEndpoints.FromRunException(ex, remapClientTimeout: flag);

            Assert.True(outcome.IsServerError);
            Assert.Null(outcome.AuthorSqlState);
            Assert.Equal("Error running query: could not open a store connection: boom", outcome.Error);
        }
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

    private static string Code(params string[] relative) =>
        Regex.Replace(CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFileLf(relative)), @"\s+", " ");

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

        Assert.Equal("await RunComposedPanelAsync(postgres, body, context.RequestAborted, readLatencyRecorder, ComposeClientDeadlineHeadroomSeconds, remapClientTimeout: true", call);
    }

    [Fact]
    public void TheMcpCaller_PassesNoHeadroom_AndTheRemapFlag()
    {
        var call = CallArguments(
            Code("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpCustomViewTools.cs"),
            "DarlingWebEndpoints.RunComposedPanelAsync(");

        Assert.Equal("DarlingWebEndpoints.RunComposedPanelAsync(postgres, body, CancellationToken.None, readLatency, remapClientTimeout: true", call);
    }

    [Fact]
    public void TheRunner_PassesTheClientDeadlineToTheMeasureAndAnnotationQueries()
    {
        var code = WebCode();

        Assert.Contains("var clientSeconds = ComposeClientDeadlineSeconds(composedQuerySeconds, clientDeadlineHeadroomSeconds);", code, StringComparison.Ordinal);
        Assert.Contains("RunComposedQueryAsync(postgres, compiled!, clientSeconds,", code, StringComparison.Ordinal);
        Assert.Contains("RunAnnotationsAsync(postgres, plan!, runContext, clientSeconds,", code, StringComparison.Ordinal);
        Assert.Equal(2, Regex.Matches(code, Regex.Escape("command.CommandTimeout = clientDeadlineSeconds;")).Count);
        Assert.DoesNotContain("command.CommandTimeout = composedQuerySeconds;", code, StringComparison.Ordinal);
        Assert.DoesNotContain("composedQuerySeconds, cancellationToken", code.Replace("var composedQuerySeconds = await", string.Empty), StringComparison.Ordinal);
    }

    private static string MethodBody(string code, string signature)
    {
        var at = code.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(at >= 0, $"method '{signature}' not found");
        var next = code.IndexOf("private static", at + signature.Length, StringComparison.Ordinal);
        Assert.True(next > at);
        return code[at..next];
    }

    [Theory]
    [InlineData("private static async Task<JsonArray> RunComposedQueryAsync(")]
    [InlineData("> ReadServerClocksAsync(")]
    public void EachRunner_OpensTheConnectionBeforeTheStatementTry(string signature)
    {
        var body = MethodBody(WebCode(), signature);

        var open = body.IndexOf("OpenComposeConnectionAsync(", StringComparison.Ordinal);
        var statementTry = body.IndexOf("try {", StringComparison.Ordinal);
        var execute = body.IndexOf("ExecuteReaderAsync(", StringComparison.Ordinal);

        Assert.True(open >= 0, "the runner must open its connection through OpenComposeConnectionAsync");
        Assert.True(statementTry > open, "the open must come before the statement try");
        Assert.True(execute > statementTry, "ExecuteReaderAsync must sit inside the statement try, after the open");
        Assert.DoesNotContain("CreateCommand(", body, StringComparison.Ordinal);
    }

    [Fact]
    public void TheMarkers_AreThrownFromTheOneOpenHelperAndTheTwoStatementCatches()
    {
        var code = WebCode();

        Assert.Equal(2, Regex.Matches(code, Regex.Escape("throw new ComposeStatementClientTimeoutException(ex);")).Count);
        Assert.Single(Regex.Matches(code, Regex.Escape("throw new ComposeStoreOpenException(ex);")));
        Assert.Equal(2, Regex.Matches(code, Regex.Escape("catch (NpgsqlException ex) when (ex.InnerException is TimeoutException)")).Count);
        Assert.Contains("catch (NpgsqlException ex) when (ex is not PostgresException) { throw new ComposeStoreOpenException(ex); }", code, StringComparison.Ordinal);
        Assert.Equal(2, Regex.Matches(code, Regex.Escape("await OpenComposeConnectionAsync(postgres, cancellationToken);")).Count);
    }

    [Fact]
    public void TheGenericCatch_RemapsThroughTheFlag()
    {
        Assert.Contains("return FromRunException(ex, remapClientTimeout);", WebCode(), StringComparison.Ordinal);
    }
}
