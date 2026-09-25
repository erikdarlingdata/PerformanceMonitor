/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Net;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4276: a web read that timed out came back as an empty HTTP 500 with no trace in the service log, because
/// <c>/api/ag</c> and <c>/api/fleet</c> had no try/catch of their own — their exception reached ASP.NET Core's
/// own error handling, which writes into the providers <c>ConfigurePipeline</c> clears on purpose (see its
/// <c>ClearProviders</c> comment). This class covers the fix in two layers: <see cref="DarlingWebFailureLog"/>
/// itself (the classifier + the one log line + the body, unit-tested directly), and the WIRED pipeline (the
/// #4128 <c>TestServer</c> pattern <see cref="DarlingWebResponseCompressionTests"/> established) so the ordering
/// and end-to-end behavior are proven against the real <c>ConfigurePipeline</c>, not a hand-copied second one.
/// </summary>
public sealed class DarlingWebFailureHandlingTests
{
    /* ═══════════════════════════ DarlingWebFailureLog: the shared classifier ═══════════════════════════ */

    [Fact]
    public void IsStatementTimeout_PostgresException57014_IsTrue()
    {
        var ex = new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014");
        Assert.True(DarlingWebFailureLog.IsStatementTimeout(ex));
    }

    [Fact]
    public void IsStatementTimeout_NpgsqlExceptionWrappingTimeoutException_IsTrue()
    {
        var ex = new NpgsqlException("Exception while reading from stream", new TimeoutException());
        Assert.True(DarlingWebFailureLog.IsStatementTimeout(ex));
    }

    [Fact]
    public void IsStatementTimeout_OtherPostgresException_IsFalse()
    {
        var ex = new PostgresException("relation \"x\" does not exist", "ERROR", "ERROR", "42P01");
        Assert.False(DarlingWebFailureLog.IsStatementTimeout(ex));
    }

    [Fact]
    public void IsStatementTimeout_NpgsqlExceptionWithoutTimeoutInner_IsFalse()
    {
        var ex = new NpgsqlException("Exception while reading from stream", new IOException("connection reset"));
        Assert.False(DarlingWebFailureLog.IsStatementTimeout(ex));
    }

    [Fact]
    public void IsStatementTimeout_PlainException_IsFalse()
    {
        Assert.False(DarlingWebFailureLog.IsStatementTimeout(new InvalidOperationException("boom")));
    }

    [Fact]
    public void StatusCode_Timeout_Is503_GenericFailure_Is500()
    {
        var timeout = new PostgresException("cancelled", "ERROR", "ERROR", "57014");
        var other = new InvalidOperationException("boom");

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, DarlingWebFailureLog.StatusCode(timeout));
        Assert.Equal(StatusCodes.Status500InternalServerError, DarlingWebFailureLog.StatusCode(other));
    }

    /// <summary>Ruled #4276: any other failure returns a generic message, never the exception text — a
    /// stack-shaped string on the wire is an information leak with no reader who benefits from it.</summary>
    [Fact]
    public void Body_GenericFailure_NeverCarriesTheExceptionText()
    {
        var ex = new InvalidOperationException("super secret internal connection string detail");
        var body = DarlingWebFailureLog.Body(ex);
        var wire = body.ToJsonString();

        Assert.DoesNotContain("super secret internal connection string detail", wire, StringComparison.Ordinal);
        Assert.Equal(DarlingWebFailureLog.GenericMessage, body["error"]!.GetValue<string>());
    }

    [Fact]
    public void Body_Timeout_NamesTheRuledMessage()
    {
        var ex = new PostgresException("cancelled", "ERROR", "ERROR", "57014");
        Assert.Equal(DarlingWebFailureLog.TimeoutMessage, DarlingWebFailureLog.Body(ex)["error"]!.GetValue<string>());
    }

    [Fact]
    public void Report_Timeout_WritesExactlyOneWarning_NamingRouteElapsedKindTypeAndSqlState()
    {
        var logger = new CapturingTestLogger();
        var ex = new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014");

        DarlingWebFailureLog.Report(logger, "/api/ag", 15650, ex);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Error));
        Assert.Contains("/api/ag", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("15650", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("timeout", logger.Joined, StringComparison.Ordinal);
        Assert.Contains(nameof(PostgresException), logger.Joined, StringComparison.Ordinal);
        Assert.Contains("57014", logger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public void Report_GenericFailure_WritesExactlyOneError_NamingRouteElapsedKindAndType()
    {
        var logger = new CapturingTestLogger();
        var ex = new InvalidOperationException("boom");

        DarlingWebFailureLog.Report(logger, "/api/fleet", 42, ex);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Error));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Contains("/api/fleet", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("42", logger.Joined, StringComparison.Ordinal);
        Assert.Contains(nameof(InvalidOperationException), logger.Joined, StringComparison.Ordinal);
    }

    /// <summary>#4281: <c>route</c> is request-supplied (the backstop passes <c>context.Request.Path.Value</c>
    /// straight off the wire), and Kestrel decodes a percent-encoded CR/LF in a path into the real characters
    /// — so an unsanitized route could forge a second log line. <see cref="DarlingWebFailureLog.Report"/> must
    /// sanitize it the same way <see cref="DarlingHttpRefusalLog.Sanitize"/> does a Host header: CR/LF become
    /// '.', so the forged text lands inertly inside the one real entry instead of starting a line of its own.</summary>
    [Fact]
    public void Report_RouteCarriesCrLf_SanitizesSoNoForgedLineReachesTheLog()
    {
        var logger = new CapturingTestLogger();
        var ex = new InvalidOperationException("boom");

        DarlingWebFailureLog.Report(logger, "/api/ag\r\nForged: line", 5, ex);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Error));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.DoesNotContain("\r", logger.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("\n", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("/api/ag..Forged: line", logger.Joined, StringComparison.Ordinal);
    }

    /* ═══════════════════════════ the wired pipeline: the top-of-pipeline backstop ═══════════════════════════ */

    /// <summary>Adapts <see cref="CapturingTestLogger"/> (a plain <see cref="ILogger"/>) to the generic
    /// <see cref="ILogger{TCategoryName}"/> <see cref="DarlingWebHostService"/>'s constructor requires.</summary>
    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public readonly CapturingTestLogger Inner = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => Inner.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => Inner.IsEnabled(logLevel);

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Inner.Log(logLevel, eventId, state, exception, formatter);
    }

    /// <summary>
    /// Builds a <see cref="TestServer"/> over the REAL <c>ConfigureResponseCompression</c> + <c>ConfigurePipeline</c>
    /// (the #4128 pattern <see cref="DarlingWebResponseCompressionTests"/> established), plus three test-only
    /// throwing routes registered AFTER <c>ConfigurePipeline</c> returns. Registering them after is legal and
    /// still covered by the #4276 backstop: <c>Map*</c> only adds to the endpoint data source the implicit
    /// <c>UseRouting</c>/<c>UseEndpoints</c> pair reads at request time, and <see cref="DarlingWebResponseCompressionTests"/>
    /// already proves a route registered from INSIDE <c>ConfigurePipeline</c> (via <c>MapAll</c>) is wrapped by
    /// middleware registered textually before it — the same nesting these test-only routes rely on. No live
    /// Postgres needed: every route here throws before ever opening the (unopened) pool.
    /// </summary>
    private static async Task<(TestServer Server, CapturingTestLogger Logger)> BuildServer()
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);

        DarlingWebHostService.ConfigureResponseCompression(builder.Services);

        var app = builder.Build();

        var capturing = new CapturingLogger<DarlingWebHostService>();
        var host = new DarlingWebHostService(
            capturing,
            new WebRuntimeState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache());

        host.ConfigurePipeline(
            app,
            postgres,
            networkMode: false,
            networkListenIp: null,
            allowedCidr: IPNetwork.Parse("127.0.0.1/32"),
            accessToken: "unused-in-loopback-mode",
            oidcClient: null);

        app.MapGet("/api/__test/timeout", (HttpContext _) =>
            throw new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014"));

        app.MapGet("/api/__test/generic", (HttpContext _) =>
            throw new InvalidOperationException("super secret internal connection string detail"));

        app.MapGet("/api/__test/abort", (HttpContext context) =>
        {
            /* The shape a real aborted read throws: OperationCanceledException carrying the SAME token
               RequestAborted resolves to, already cancelled — exactly what the #4276 handler's
               `when (context.RequestAborted.IsCancellationRequested)` guard tests. */
            context.RequestAborted.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        });

        await app.StartAsync();
        return (app.GetTestServer(), capturing.Inner);
    }

    private static Task<HttpContext> Send(TestServer server, string path, CancellationToken? requestAborted = null)
    {
        return server.SendAsync(ctx =>
        {
            ctx.Request.Method = "GET";
            ctx.Request.Path = path;
            ctx.Request.Headers.Host = "localhost";
            ctx.Connection.RemoteIpAddress = IPAddress.Loopback;
            if (requestAborted is { } token)
            {
                ctx.RequestAborted = token;
            }
        });
    }

    private static async Task<JsonDocument> ReadJsonBody(HttpContext ctx)
    {
        using var reader = new StreamReader(ctx.Response.Body);
        return JsonDocument.Parse(await reader.ReadToEndAsync());
    }

    [Fact]
    public async Task StatementTimeout_Returns503_WithTheRuledJsonBody_AndWritesOneWarning()
    {
        var (server, logger) = await BuildServer();
        using var _ = server;

        var ctx = await Send(server, "/api/__test/timeout");

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, ctx.Response.StatusCode);
        Assert.Equal("application/json; charset=utf-8", ctx.Response.ContentType);

        using var json = await ReadJsonBody(ctx);
        Assert.Equal(DarlingWebFailureLog.TimeoutMessage, json.RootElement.GetProperty("error").GetString());

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Error));
        Assert.Contains("/api/__test/timeout", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("57014", logger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GenericException_Returns500_WithNoExceptionText_AndWritesOneError()
    {
        var (server, logger) = await BuildServer();
        using var _ = server;

        var ctx = await Send(server, "/api/__test/generic");

        Assert.Equal(StatusCodes.Status500InternalServerError, ctx.Response.StatusCode);

        using var json = await ReadJsonBody(ctx);
        var message = json.RootElement.GetProperty("error").GetString();
        Assert.Equal(DarlingWebFailureLog.GenericMessage, message);
        Assert.DoesNotContain("super secret internal connection string detail", message, StringComparison.Ordinal);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Error));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        /* The log line is where the real detail belongs — unlike the wire body, checked above. */
        Assert.Contains("InvalidOperationException", logger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BrowserAbort_WritesNoLogLine_AndNoBody()
    {
        var (server, logger) = await BuildServer();
        using var _ = server;

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var ctx = await Send(server, "/api/__test/abort", cts.Token);

        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Error));
        Assert.Equal("(no log lines captured)", logger.Joined);

        using var reader = new StreamReader(ctx.Response.Body);
        Assert.Equal(string.Empty, await reader.ReadToEndAsync());
    }

    /* ═══════════════════════════ source pin: registered ahead of every /api/* route ═══════════════════════════ */

    /// <summary>
    /// <c>ConfigurePipeline</c> maps every <c>/api/*</c> route (the read dispatch, Custom Views, alerts, mute
    /// rules, fleet sweep, triage) through the ONE <c>DarlingWebEndpoints.MapAll</c> call — it has no
    /// <c>Map*</c> of its own. So "the backstop covers every route" reduces to "the backstop is registered
    /// (<c>app.Use</c>) textually ahead of that one call" — middleware registered earlier wraps everything a
    /// later <c>Map*</c> adds, exactly as <see cref="DarlingWebResponseCompressionTests"/> already proves for
    /// the no-store stamp. A backstop registered AFTER would compile and pass every OTHER test in this class
    /// (they all call <c>ConfigurePipeline</c> directly) while leaving production's real routes uncovered.
    /// </summary>
    [Fact]
    public void ExceptionBackstop_IsRegistered_AheadOfMapAll_SoEveryApiRouteIsCovered()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingWebHostService.cs"));

        var handler = code.IndexOf("DarlingWebFailureLog.Report(_logger", StringComparison.Ordinal);
        Assert.True(handler >= 0,
            "ConfigurePipeline no longer calls DarlingWebFailureLog.Report; this pin is reading nothing.");

        var mapAll = code.IndexOf("DarlingWebEndpoints.MapAll(app", StringComparison.Ordinal);
        Assert.True(mapAll >= 0,
            "ConfigurePipeline no longer calls DarlingWebEndpoints.MapAll; this pin is reading nothing.");

        Assert.True(
            handler < mapAll,
            "The #4276 exception backstop must be registered (app.Use) AHEAD of DarlingWebEndpoints.MapAll, so "
          + "it wraps every /api/* route MapAll adds. It is currently registered AFTER, which leaves those "
          + "routes reaching ASP.NET Core's own error handling again — the empty, untraced 500 #4276 reports.");
    }
}
