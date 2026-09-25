/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4283: three OLDER paths (a tool's own caught exception through <c>ToHttpResult</c>, <c>/api/compose/run</c>'s
/// 500, the triage page's notes) still put <c>ex.Message</c> on the wire, on top of the six write endpoints and
/// two fleet-sweep reads the round-1 #4281 review's grep did not reach. This class is three things: a source
/// census that the web host never builds an HTTP answer from a caught exception's own text (an allow-list of
/// the two sites where the text is either safely contained or genuinely needed), unit tests for the new
/// sentence-based half of <see cref="DarlingWebFailureLog"/> and <see cref="DarlingWebEndpoints.ToHttpResult"/>,
/// and one live test proving a REAL tool-caught statement_timeout answers 503 with no exception text.
/// </summary>
public sealed class WebExceptionTextCensusTests
{
    /* ═══════════════════════════ the census ═══════════════════════════ */

    /// <summary>Every file that answers an HTTP request on the web/triage/fleet-sweep surface — the roster
    /// <c>#4283</c>'s own issue names (<c>DarlingWebEndpoints.cs</c>, the triage page) plus the one more this
    /// lane's own grep found (<c>DarlingFleetSweepEndpoints.cs</c>) and the OIDC exchange the issue asked to
    /// be traced. <c>Mcp/DarlingWebHostService.cs</c> is traced too (#4283) but NOT in the roster: every
    /// <c>ex.Message</c> there is a <c>logger.LogError</c>/<c>LogWarning</c> argument in the host's own
    /// startup/shutdown/OIDC-config lifecycle (service diagnostics), or the sign-in flow's own
    /// <c>DarlingHttpRefusalLog</c> line — never a byte written to an HTTP response; the ONE place it DOES
    /// answer a request, the #4281 backstop, already passes the <see cref="Exception"/> itself through
    /// <see cref="DarlingWebFailureLog.Report(ILogger,string,long,Exception)"/>, never its bare
    /// <c>.Message</c>. A file NOT in this list is not swept by the census — the same tradeoff
    /// <c>AlertReadFailureSurfaceTests</c>' fixed roster makes, over scanning every collector file that
    /// legitimately logs <c>ex.Message</c> with nothing to do with a browser.</summary>
    private static readonly string[] s_roster =
    {
        Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs"),
        Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingTriageEndpoint.cs"),
        Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingFleetSweepEndpoints.cs"),
        Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Hosting", "DarlingWebOidc.cs"),
    };

    /// <summary>Each surviving <c>ex.Message</c> / <c>ex.MessageText</c> CODE occurrence (comments stripped),
    /// matched by a distinguishing snippet, with the reason it is not #4283's target.</summary>
    private static readonly (string File, string Snippet, string Reason)[] s_allowList =
    {
        (
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs"),
            "Query failed: {ex.MessageText}",
            "The 400 arm of /api/compose/run's PostgresException catch — a Custom Views author needs the real " +
            "syntax/statement_timeout error to fix their own panel; #4283's issue names this one to KEEP."
        ),
        (
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs"),
            "Error running query: {ex.Message}",
            "ComposeRunOutcome.ServerError's text, built once and shared by two consumers: the web route " +
            "reclassifies it through DarlingWebFailureLog before it ever reaches a browser (ServerErrorResult), " +
            "and the MCP run_custom_view_panel tool (DarlingMcpCustomViewTools) reads the same field unchanged " +
            "— #4283 does not touch what an MCP client sees."
        ),
        (
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Hosting", "DarlingWebOidc.cs"),
            "return new ExchangeResult(null, ex.Message);",
            "Traced (#4283): ExchangeResult.Error's only reader is DarlingWebHostService's sign-in callback, " +
            "which routes it ONLY to ReportSignInRefusal (a log line) and answers the browser a fixed sentence " +
            "('The identity provider rejected the sign-in exchange...') regardless. Never reaches a browser."
        ),
    };

    /// <summary>Strips <c>/* ... */</c> and <c>// ...</c> so the census reads CODE, not this class's own doc
    /// comments quoting the pattern under discussion (every file above has several, post-#4283).</summary>
    private static string StripComments(string source)
    {
        var noBlock = Regex.Replace(source, @"/\*.*?\*/", "", RegexOptions.Singleline);
        return Regex.Replace(noBlock, @"//[^\n]*", "");
    }

    private static readonly Regex s_exMessagePattern = new(@"\b(ex|exception)\.Message(Text)?\b", RegexOptions.Compiled);

    [Fact]
    public void NoWebEndpoint_BuildsAnAnswerFromExMessage_ExceptTheNamedAllowList()
    {
        var root = RepoFile.Root;
        var unaccounted = new List<string>();

        foreach (var relative in s_roster)
        {
            var path = Path.Combine(root, relative);
            Assert.True(File.Exists(path), $"#4283 census target not found: {path}");

            var code = StripComments(File.ReadAllText(path));
            var matches = s_exMessagePattern.Matches(code);

            foreach (Match match in matches)
            {
                var window = code.Substring(Math.Max(0, match.Index - 60), Math.Min(120, code.Length - Math.Max(0, match.Index - 60)));
                var accounted = false;
                foreach (var (file, snippet, _) in s_allowList)
                {
                    if (string.Equals(file, relative, StringComparison.Ordinal) && window.Contains(snippet, StringComparison.Ordinal))
                    {
                        accounted = true;
                        break;
                    }
                }

                if (!accounted)
                {
                    unaccounted.Add($"{relative}: ...{window}...");
                }
            }
        }

        Assert.True(unaccounted.Count == 0,
            "#4283: an ex.Message/ex.MessageText reached web-surface code outside the named allow-list:\n"
            + string.Join("\n", unaccounted));
    }

    /// <summary>The allow-list's OTHER direction: every entry's snippet must still be found (a snippet that
    /// vanished means the code moved or was fixed and the allow-list enn should shrink, not silently stop
    /// covering nothing).</summary>
    [Fact]
    public void AllowList_EveryEntry_IsStillPresentInItsFile()
    {
        var root = RepoFile.Root;
        foreach (var (file, snippet, reason) in s_allowList)
        {
            var code = StripComments(File.ReadAllText(Path.Combine(root, file)));
            Assert.True(code.Contains(snippet, StringComparison.Ordinal),
                $"#4283 allow-list entry no longer found in {file} (\"{reason}\"): {snippet}");
        }
    }

    /* ═══════════════════════════ DarlingWebFailureLog: the sentence-based twin ═══════════════════════════ */

    [Fact]
    public void IsStatementTimeoutSentence_CarriesTheSqlStateToken_IsTrue()
    {
        var sentence = McpHelpers.ErrorSentence("get_blocking",
            new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014"));

        Assert.True(DarlingWebFailureLog.IsStatementTimeoutSentence(sentence));
    }

    [Fact]
    public void IsStatementTimeoutSentence_OtherPostgresError_IsFalse()
    {
        var sentence = McpHelpers.ErrorSentence("get_blocking",
            new PostgresException("relation \"x\" does not exist", "ERROR", "ERROR", "42P01"));

        Assert.False(DarlingWebFailureLog.IsStatementTimeoutSentence(sentence));
    }

    [Fact]
    public void IsStatementTimeoutSentence_DigitRunThatIsNot57014_IsFalse()
    {
        /* The word-boundary defense (MigrationDataMovingRungCensusPins' s_cancelTrap precedent): a row count
           or identifier that merely CONTAINS 57014 as a substring must not false-positive. */
        Assert.False(DarlingWebFailureLog.IsStatementTimeoutSentence("Error during get_x: 5701400 rows affected"));
        Assert.False(DarlingWebFailureLog.IsStatementTimeoutSentence("Error during get_x: table_157014 missing"));
    }

    [Fact]
    public void StatusCodeAndBody_SentenceOverload_MatchTheExceptionOverload()
    {
        var ex = new PostgresException("cancelled", "ERROR", "ERROR", "57014");
        var sentence = McpHelpers.ErrorSentence("get_x", ex);

        Assert.Equal(DarlingWebFailureLog.StatusCode(ex), DarlingWebFailureLog.StatusCode(sentence));
        Assert.Equal(DarlingWebFailureLog.Body(ex)["error"]!.GetValue<string>(), DarlingWebFailureLog.Body(sentence)["error"]!.GetValue<string>());
    }

    [Fact]
    public void Body_SentenceOverload_NeverCarriesTheExceptionText()
    {
        var sentence = McpHelpers.ErrorSentence("get_object_locking",
            new InvalidOperationException("Host=10.0.0.5;Port=5432 connection refused"));

        var wire = DarlingWebFailureLog.Body(sentence).ToJsonString();
        Assert.DoesNotContain("10.0.0.5", wire, StringComparison.Ordinal);
        Assert.Equal(DarlingWebFailureLog.GenericMessage, DarlingWebFailureLog.Body(sentence)["error"]!.GetValue<string>());
    }

    [Fact]
    public void Report_SentenceOverload_WritesExactlyOneWarningForATimeoutToken()
    {
        var logger = new CapturingTestLogger();
        var sentence = McpHelpers.ErrorSentence("get_blocking",
            new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014"));

        DarlingWebFailureLog.Report(logger, "/api/read/get_blocking", 12, sentence);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Error));
        Assert.Contains("/api/read/get_blocking", logger.Joined, StringComparison.Ordinal);
        Assert.Contains(sentence, logger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public void Report_SentenceOverload_WritesExactlyOneErrorWhenNoTimeoutToken()
    {
        var logger = new CapturingTestLogger();
        var sentence = McpHelpers.ErrorSentence("get_object_locking", new InvalidOperationException("boom"));

        DarlingWebFailureLog.Report(logger, "/api/read/get_object_locking", 3, sentence);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Error));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
    }

    /* ═══════════════════════════ DarlingWebEndpoints.ToHttpResult: the ServerError arm ═══════════════════════════ */

    [Fact]
    public void ToHttpResult_ToolCaughtTimeout_MapsTo503_NoExceptionText_OneWarning()
    {
        var logger = new CapturingTestLogger();
        var ex = new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014");
        var envelope = McpHelpers.FormatError("get_blocking", ex);

        var result = DarlingWebEndpoints.ToHttpResult(envelope, "/api/read/get_blocking", logger, 77);

        var json = ResultBody(result, out var statusCode);
        Assert.Equal(StatusCodes.Status503ServiceUnavailable, statusCode);
        Assert.Equal(DarlingWebFailureLog.TimeoutMessage, json.RootElement.GetProperty("error").GetString());
        Assert.DoesNotContain("57014", json.RootElement.GetProperty("error").GetString(), StringComparison.Ordinal);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
    }

    [Fact]
    public void ToHttpResult_ToolCaughtGenericFailure_MapsTo500_NoExceptionText_OneError()
    {
        var logger = new CapturingTestLogger();
        var ex = new InvalidOperationException("Host=store.internal;Port=5432 role \"app_rw\" failed");
        var envelope = McpHelpers.FormatError("get_object_locking", ex);

        var result = DarlingWebEndpoints.ToHttpResult(envelope, "/api/read/get_object_locking", logger, 5);

        var json = ResultBody(result, out var statusCode);
        Assert.Equal(StatusCodes.Status500InternalServerError, statusCode);
        Assert.Equal(DarlingWebFailureLog.GenericMessage, json.RootElement.GetProperty("error").GetString());
        Assert.DoesNotContain("store.internal", json.RootElement.GetProperty("error").GetString(), StringComparison.Ordinal);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Error));
    }

    [Fact]
    public void ToHttpResult_Refusal_IsUnchanged_StillTheEnvelopeAt400()
    {
        var logger = new CapturingTestLogger();
        var refusal = McpHelpers.Refusal("hours_back", "hours_back must be between 1 and 8760.");

        var result = DarlingWebEndpoints.ToHttpResult(refusal, "/api/read/get_x", logger, 1);

        ResultBody(result, out var statusCode);
        Assert.Equal(StatusCodes.Status400BadRequest, statusCode);
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Error));
    }

    /// <summary>Reads an <see cref="IResult"/> built by <c>Results.Json</c>/<c>Results.Text</c> the same way
    /// the pipeline would, via <see cref="DefaultHttpContext"/>'s response body.</summary>
    private static JsonDocument ResultBody(IResult result, out int statusCode)
    {
        /* Results.Json/Results.Text resolve IOptions<JsonOptions> from RequestServices; AddOptions() alone
           supplies the default-valued IOptions<T> that resolves to (no explicit configuration needed). */
        var services = new ServiceCollection();
        services.AddOptions();
        services.AddLogging();
        var context = new DefaultHttpContext { RequestServices = services.BuildServiceProvider() };
        var stream = new MemoryStream();
        context.Response.Body = stream;

        result.ExecuteAsync(context).GetAwaiter().GetResult();

        statusCode = context.Response.StatusCode;
        stream.Position = 0;
        using var reader = new StreamReader(stream);
        return JsonDocument.Parse(reader.ReadToEnd());
    }

    /* ═══════════════════════════ live: a REAL tool-caught statement_timeout, on the rig ═══════════════════════════ */

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// #4283's centerpiece, end to end against the rig: a route registered AFTER the REAL <c>MapAll</c> (the
    /// same after-ConfigurePipeline registration <see cref="DarlingWebFailureHandlingTests"/>'s own
    /// <c>/api/__test/*</c> routes use) runs <c>SELECT pg_sleep(5)</c> under a connection-string
    /// <c>statement_timeout</c> of 100 ms — deterministic regardless of the test database's own size or
    /// speed, unlike racing a real tool's own (possibly sub-millisecond, on an empty test database) query
    /// against the clock. The server cancels it at SQLSTATE 57014, the route's catch is the SAME shape every
    /// real PostgreSQL tool uses (<c>catch (Exception ex) { return McpHelpers.FormatError(op, ex); }</c>), and
    /// the result is handed to the REAL <see cref="DarlingWebEndpoints.ToHttpResult"/> the <c>/api/read/*</c>
    /// dispatch loop calls — proving THIS lane's new sentence classifier against a REAL exception's REAL
    /// <see cref="PostgresException.Message"/> text, not a hand-built stand-in.
    /// </summary>
    [Fact]
    public async Task RealToolCaughtStatementTimeout_OnTheRig_Returns503_NoExceptionText_OneLogLine()
    {
        var cs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live #4283 timeout test.");

        var ct = TestContext.Current.CancellationToken;
        await using (var setupConnection = new NpgsqlConnection(cs))
        {
            await setupConnection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setupConnection, ct);
        }

        var timeoutCs = new NpgsqlConnectionStringBuilder(cs) { Options = "-c statement_timeout=100" }.ToString();
        await using var postgres = NpgsqlDataSource.Create(timeoutCs);

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(postgres);

        var app = builder.Build();
        var logger = new CapturingTestLogger();

        DarlingWebEndpoints.MapAll(app, postgres, new CollectorRuntimeState(), logger);

        app.MapGet("/api/__test/tool-caught-timeout", async (HttpContext context) =>
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            string result;
            try
            {
                await using var connection = await postgres.OpenConnectionAsync(context.RequestAborted);
                await using var command = connection.CreateCommand();
                command.CommandText = "SELECT pg_sleep(5)";
                await command.ExecuteNonQueryAsync(context.RequestAborted);
                result = "{\"status\":\"empty\"}";
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* The tools' own shape (McpHelpers.FormatError, #3653 Q11) — this route exists ONLY to force a
                   deterministic real 57014 into that shape; it is not itself a tool. */
                result = McpHelpers.FormatError("test_tool", ex);
            }

            return DarlingWebEndpoints.ToHttpResult(result, "/api/__test/tool-caught-timeout", logger, stopwatch.ElapsedMilliseconds);
        });

        await app.StartAsync(ct);
        using var server = app.GetTestServer();

        var httpContext = await server.SendAsync(request =>
        {
            request.Request.Method = "GET";
            request.Request.Path = "/api/__test/tool-caught-timeout";
            request.Request.Headers.Host = "localhost";
        });

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, httpContext.Response.StatusCode);

        using var reader = new StreamReader(httpContext.Response.Body);
        var bodyText = await reader.ReadToEndAsync(ct);
        using var body = JsonDocument.Parse(bodyText);

        Assert.Equal(DarlingWebFailureLog.TimeoutMessage, body.RootElement.GetProperty("error").GetString());
        Assert.DoesNotContain("57014", bodyText, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_sleep", bodyText, StringComparison.Ordinal);
        Assert.DoesNotContain("canceling statement", bodyText, StringComparison.Ordinal);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Error));
        Assert.Contains("57014", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("/api/__test/tool-caught-timeout", logger.Joined, StringComparison.Ordinal);

        await app.StopAsync(ct);
    }
}
