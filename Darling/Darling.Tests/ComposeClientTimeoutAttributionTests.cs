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
using PerformanceMonitor.Darling.Storage;
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
        var client = ComposePanelRunner.ComposeClientDeadlineSeconds(server, DarlingWebEndpoints.ComposeClientDeadlineHeadroomSeconds);

        Assert.True(client > server, $"web compose client deadline {client}s must exceed the server statement_timeout {server}s so 57014 wins the race");
        Assert.True(DarlingWebEndpoints.ComposeClientDeadlineHeadroomSeconds > 0);
    }

    [Fact]
    public void McpComposeCaller_KeepsItsDeadline_NoHeadroom()
    {
        Assert.Equal(60, ComposePanelRunner.ComposeClientDeadlineSeconds(60, 0));
    }

    private static NpgsqlException StatementPhaseTimeout() =>
        new("Exception while reading from stream", new TimeoutException("Timeout during reading attempt"));

    [Fact]
    public void StatementPhaseClientTimeout_OnTheWebPath_MapsToTimeoutWithTheActionableText()
    {
        var ex = new ComposePanelRunner.ComposeStatementClientTimeoutException(StatementPhaseTimeout());

        var outcome = ComposePanelRunner.FromRunException(ex, remapClientTimeout: true);

        Assert.False(outcome.IsServerError);
        Assert.Null(outcome.Fault);
        Assert.Equal("57014", outcome.AuthorSqlState);
        Assert.Equal(ComposePanelRunner.StatementTimeoutText, outcome.Error);
        Assert.Equal(
            ComposePanelRunner.FromPostgresException(new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014")).Error,
            outcome.Error);
    }

    [Fact]
    public void StatementPhaseClientTimeout_WithoutTheRemapFlag_KeepsTheGenericServerError()
    {
        var ex = new ComposePanelRunner.ComposeStatementClientTimeoutException(StatementPhaseTimeout());

        foreach (var outcome in new[] { ComposePanelRunner.FromRunException(ex), ComposePanelRunner.FromRunException(ex, remapClientTimeout: false) })
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
        var ex = new ComposePanelRunner.ComposeStoreOpenException(inner);

        foreach (var flag in new[] { true, false })
        {
            var outcome = ComposePanelRunner.FromRunException(ex, remapClientTimeout: flag);

            Assert.True(outcome.IsServerError);
            Assert.Null(outcome.AuthorSqlState);
            Assert.NotEqual("57014", outcome.AuthorSqlState);
            Assert.StartsWith("Error running query: could not get a store connection in time: ", outcome.Error, StringComparison.Ordinal);
            Assert.Contains(innerMessage, outcome.Error, StringComparison.Ordinal);
            Assert.DoesNotContain(ComposePanelRunner.StatementTimeoutText, outcome.Error, StringComparison.Ordinal);
        }

    }

    [Fact]
    public void OpenFailure_WithoutATimeout_AnswersCouldNotOpen()
    {
        var ex = new ComposePanelRunner.ComposeStoreOpenException(new NpgsqlException("boom", new System.Net.Sockets.SocketException((int)System.Net.Sockets.SocketError.ConnectionRefused)));

        foreach (var flag in new[] { true, false })
        {
            var outcome = ComposePanelRunner.FromRunException(ex, remapClientTimeout: flag);

            Assert.True(outcome.IsServerError);
            Assert.Null(outcome.AuthorSqlState);
            Assert.Equal("Error running query: could not open a store connection: boom", outcome.Error);
        }
    }

    [Fact]
    public void OtherExceptions_StillMapToTheGenericServerError()
    {
        var plain = ComposePanelRunner.FromRunException(new InvalidOperationException("boom"), remapClientTimeout: true);
        Assert.True(plain.IsServerError);
        Assert.Equal("Error running query: boom", plain.Error);
        Assert.Null(plain.AuthorSqlState);

        var npgsqlOther = ComposePanelRunner.FromRunException(new NpgsqlException("connection refused", new System.IO.IOException("x")), remapClientTimeout: true);
        Assert.True(npgsqlOther.IsServerError);
    }

    // ---- wiring pins: the call sites, read from source with comments and strings stripped ----

    private static string Code(params string[] relative) =>
        Regex.Replace(CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFileLf(relative)), @"\s+", " ");

    /* #6390 S2c: the run core moved to Storage; the rules read the endpoints and the runner as one text. */
    private static string WebCode() => Code("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs") + Code("Darling", "PerformanceMonitor.Darling.Storage", "Compose", "ComposePanelRunner.cs");

    [Fact]
    public void TheRunner_PassesTheClientDeadlineToTheMeasureAndAnnotationQueries()
    {
        var code = WebCode();

        Assert.Contains("var clientSeconds = ComposeClientDeadlineSeconds(composedQuerySeconds, clientDeadlineHeadroomSeconds);", code, StringComparison.Ordinal);
        Assert.Contains("RunComposedQueryAsync(postgres, compiled!, clientSeconds,", code, StringComparison.Ordinal);
        Assert.Contains("RunAnnotationsAsync(postgres, plan!, runContext, clientSeconds,", code, StringComparison.Ordinal);
        Assert.Equal(2, Regex.Matches(code, Regex.Escape("command.CommandTimeout = clientDeadlineSeconds;")).Count);
        Assert.DoesNotContain("command.CommandTimeout = composedQuerySeconds;", code, StringComparison.Ordinal);
        Assert.DoesNotContain("RunComposedQueryAsync(postgres, compiled!, composedQuerySeconds", code, StringComparison.Ordinal);
        Assert.DoesNotContain("RunAnnotationsAsync(postgres, plan!, runContext, composedQuerySeconds", code, StringComparison.Ordinal);
    }

    private static string MethodBody(string code, string signature)
    {
        var at = code.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(at >= 0, $"method '{signature}' not found");
        var next = code.IndexOf("private static", at + signature.Length, StringComparison.Ordinal);
        /* #6390 S2c: the runner file ends with this method's neighbours, so the last one has no private member after it. */
        if (next < 0)
        {
            next = code.Length;
        }

        Assert.True(next > at);
        return code[at..next];
    }

    [Theory]
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

    /// <summary>The panel runner opens its store connection before the statement try, so a store that cannot be reached is
    /// answered as an open failure (ComposeStoreOpenException), never as a statement timeout. It runs the real runner against
    /// a port nothing listens on; this replaces a source-text pin on the runner's signature.</summary>
    [Fact]
    public async Task ThePanelRunner_AnUnreachableStore_IsAnOpenFailure_NotAStatementTimeout()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var unreachable = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=darling;Database=none;Timeout=5;Pooling=false");
        var compiled = new ComposeCompiled(
            "SELECT 1 AS n", new System.Collections.Generic.List<NpgsqlParameter>(), new ComposeRoute(ComposeSourceTier.Raw, null));

        await Assert.ThrowsAsync<ComposePanelRunner.ComposeStoreOpenException>(
            () => ComposePanelRunner.RunComposedQueryAsync(unreachable, compiled, 30, ct));
    }

    [Fact]
    public void TheMarkers_AreThrownFromTheOneOpenHelperAndTheTwoStatementCatches()
    {
        var code = WebCode();

        Assert.Equal(2, Regex.Matches(code, Regex.Escape("throw new ComposeStatementClientTimeoutException(ex);")).Count);
        /* #5016: two arms, both inside the one open helper: the driver's NpgsqlException, and the bare TimeoutException it
           throws when a wait for a data source's first-time setup ends. A cancellation is in neither arm: it is the caller's. */
        Assert.Equal(2, Regex.Matches(code, Regex.Escape("throw new ComposeStoreOpenException(ex);")).Count);
        Assert.Equal(2, Regex.Matches(code, Regex.Escape("catch (NpgsqlException ex) when (ex.InnerException is TimeoutException)")).Count);
        Assert.Contains("catch (NpgsqlException ex) when (ex is not PostgresException) { throw new ComposeStoreOpenException(ex); } catch (TimeoutException ex) { throw new ComposeStoreOpenException(ex); }", code, StringComparison.Ordinal);        /* Three open sites: the panel runner, the server-clocks runner and the hourly-edges snapshot. */
        Assert.Equal(3, Regex.Matches(code, Regex.Escape("await OpenComposeConnectionAsync(postgres, cancellationToken);")).Count);
    }

    [Fact]
    public void TheOpenSentence_AsksTheOneHelper_WhetherTheOpenTimedOut()
    {
        var code = WebCode();

        Assert.Contains("PostgresOpenTimeout.IsTimedOutOpen(open.InnerException!)", code, StringComparison.Ordinal);
        Assert.DoesNotContain("InnerException!.InnerException is TimeoutException", code, StringComparison.Ordinal);
    }

    [Fact]
    public void TheGenericCatch_RemapsThroughTheFlag()
    {
        Assert.Contains("return FromRunException(ex, remapClientTimeout);", WebCode(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheOpenHelper_ClocksTheOpen_AndFlagsALateFailure_AndTheSentenceReadsTheFlag()
    {
        var code = WebCode();
        var helper = MethodBody(code, "private static async Task<NpgsqlConnection> OpenComposeConnectionAsync(");

        Assert.Contains("Stopwatch.StartNew()", helper, StringComparison.Ordinal);
        Assert.Contains("PostgresOpenTimeout.IsTimedOutByClock(open.Cause, started.Elapsed, timeout, cancellationToken.IsCancellationRequested)", helper, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(helper, Regex.Escape("{ FailedAfterTimeout = true }")));
        Assert.Contains("|| open.FailedAfterTimeout", code, StringComparison.Ordinal);
    }
}
