/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Metrics;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9013: the metrics port's request pipeline, in order: the allowFrom check (403), the bearer token (401, empty body), then the
/// scrape. No store: the cache never refreshes, so a scrape that gets through answers the meta series only. Every collaborator is a
/// per-test instance on a TestServer, so the class touches no process-wide state.
/// </summary>
public sealed class DarlingMetricsHostAuthTests
{
    private const string Token = DarlingMetricsHostTests.GoodToken;
    private static readonly IPAddress Outside = IPAddress.Parse("203.0.113.9");
    private static readonly IPAddress Inside = IPAddress.Parse("10.1.2.3");

    private static MetricsCheck CheckWith(string? allowFrom)
    {
        var check = MetricsConfig.Check(
            new MetricsConfig { Enabled = true, Port = 5155, Listen = "127.0.0.1", Token = Token, AllowFrom = allowFrom },
            webPort: null, mcpPort: null, healthPort: null);
        Assert.Empty(check.Problems);
        return check;
    }

    private static async Task<WebApplication> StartAsync(
        IPAddress? remote, string? allowFrom = null, DarlingMetricsHostTests.CapturingLogger? logger = null, TimeProvider? clock = null)
    {
        var check = CheckWith(allowFrom);
        var ct = TestContext.Current.CancellationToken;
        var cache = new DarlingMetricsSnapshotCache(
            _ => throw new InvalidOperationException("no store in this test"), () => false, TimeProvider.System, NullLogger.Instance, ct);
        var refusals = new DarlingHttpRefusalLog();
        var throttle = new DarlingSignInThrottle();
        var time = clock ?? TimeProvider.System;
        ILogger log = logger ?? (ILogger)NullLogger.Instance;

        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        var app = builder.Build();
        if (remote is not null)
        {
            app.Use((context, next) =>
            {
                context.Connection.RemoteIpAddress = remote;
                return next(context);
            });
        }

        DarlingMetricsHostService.MapRoutes(app, context => DarlingMetricsHostService.Answer(context, check, cache, refusals, throttle, log, time));
        await app.StartAsync(ct);
        return app;
    }

    private static async Task<HttpResponseMessage> ScrapeAsync(WebApplication app, AuthenticationHeaderValue? authorization)
    {
        using var client = app.GetTestClient();
        using var request = new HttpRequestMessage(HttpMethod.Get, "/metrics");
        request.Headers.Authorization = authorization;
        return await client.SendAsync(request, TestContext.Current.CancellationToken);
    }

    private static AuthenticationHeaderValue Bearer(string token) => new("Bearer", token);

    private static async Task AssertUnauthorizedAsync(HttpResponseMessage response)
    {
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.Equal("", await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
        var challenge = Assert.Single(response.Headers.WwwAuthenticate);
        Assert.Equal("Bearer", challenge.ToString());
    }

    [Fact]
    public async Task MissingHeader_Is401_EmptyBody()
    {
        await using var app = await StartAsync(IPAddress.Loopback);
        using var response = await ScrapeAsync(app, null);
        await AssertUnauthorizedAsync(response);
    }

    [Fact]
    public async Task WrongToken_Is401_EmptyBody()
    {
        await using var app = await StartAsync(IPAddress.Loopback);
        using var response = await ScrapeAsync(app, Bearer(Token + "x"));
        await AssertUnauthorizedAsync(response);
    }

    [Fact]
    public async Task WrongScheme_Is401()
    {
        await using var app = await StartAsync(IPAddress.Loopback);
        using var response = await ScrapeAsync(app, new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.ASCII.GetBytes("scraper:" + Token))));
        await AssertUnauthorizedAsync(response);
    }

    [Fact]
    public async Task RightToken_Is200_WithTheContentType()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await StartAsync(IPAddress.Loopback);
        using var response = await ScrapeAsync(app, Bearer(Token));

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(PrometheusText.ContentType, response.Content.Headers.ContentType?.ToString());
        Assert.Contains("darling_metrics_last_refresh_success 0\n", await response.Content.ReadAsStringAsync(ct), StringComparison.Ordinal);
    }

    [Fact]
    public async Task EachRefusal_IsLoggedOnce_WithoutThePresentedValue()
    {
        const string presented = "wrong-9013-value";
        var logger = new DarlingMetricsHostTests.CapturingLogger();
        await using var app = await StartAsync(IPAddress.Loopback, logger: logger);

        using var response = await ScrapeAsync(app, Bearer(presented));

        await AssertUnauthorizedAsync(response);
        var warning = Assert.Single(logger.Lines, l => l.Level == LogLevel.Warning);
        Assert.Contains("Metrics", warning.Text, StringComparison.Ordinal);
        Assert.DoesNotContain(logger.Lines, l => l.Text.Contains(presented, StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Lines, l => l.Text.Contains(Token, StringComparison.Ordinal));
    }

    [Fact]
    public async Task WrongTokens_BelowTheFreeAllowance_StayPlain401s()
    {
        await using var app = await StartAsync(IPAddress.Loopback, clock: new ManualTimeProvider());

        for (var i = 0; i < DarlingSignInThrottle.FreeFailures; i++)
        {
            using var refused = await ScrapeAsync(app, Bearer("wrong-9013-" + i));
            await AssertUnauthorizedAsync(refused);
            Assert.False(refused.Headers.Contains("Retry-After"));
        }

        using var right = await ScrapeAsync(app, Bearer(Token));
        Assert.Equal(HttpStatusCode.OK, right.StatusCode);
    }

    [Fact]
    public async Task RepeatedWrongTokens_FromOneSource_AreHeldWith429AndRetryAfter_ThenTheRightTokenIsServedAfterTheWait()
    {
        var clock = new ManualTimeProvider();
        var logger = new DarlingMetricsHostTests.CapturingLogger();
        await using var app = await StartAsync(Outside, logger: logger, clock: clock);

        /* The free allowance and the miss that starts the wait are each compared and answered 401. */
        for (var i = 0; i <= DarlingSignInThrottle.FreeFailures; i++)
        {
            using var refused = await ScrapeAsync(app, Bearer("wrong-9013-" + i));
            await AssertUnauthorizedAsync(refused);
        }

        /* Inside the wait even the right token is held, never compared. */
        using var held = await ScrapeAsync(app, Bearer(Token));
        Assert.Equal(HttpStatusCode.TooManyRequests, held.StatusCode);
        Assert.Equal(
            ((int)Math.Ceiling(DarlingSignInThrottle.BaseDelay.TotalSeconds)).ToString(System.Globalization.CultureInfo.InvariantCulture),
            Assert.Single(held.Headers.GetValues("Retry-After")));
        Assert.Contains(logger.Lines, l => l.Text.Contains("HTTP 429", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Lines, l => l.Text.Contains(Token, StringComparison.Ordinal));

        clock.Advance(DarlingSignInThrottle.BaseDelay);
        using var after = await ScrapeAsync(app, Bearer(Token));
        Assert.Equal(HttpStatusCode.OK, after.StatusCode);
    }

    [Fact]
    public async Task OutsideAllowFrom_Is403_EvenWithTheRightToken()
    {
        var ct = TestContext.Current.CancellationToken;
        var logger = new DarlingMetricsHostTests.CapturingLogger();
        await using var app = await StartAsync(Outside, allowFrom: "10.0.0.0/8", logger: logger);

        using var response = await ScrapeAsync(app, Bearer(Token));

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.Equal("", await response.Content.ReadAsStringAsync(ct));
        var warning = Assert.Single(logger.Lines, l => l.Level == LogLevel.Warning);
        Assert.Contains("metrics.allowFrom", warning.Text, StringComparison.Ordinal);
        Assert.DoesNotContain(Token, warning.Text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task InsideAllowFrom_WithTheToken_Is200()
    {
        await using var app = await StartAsync(Inside, allowFrom: "10.0.0.0/8");
        using var response = await ScrapeAsync(app, Bearer(Token));
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task EmptyAllowFrom_AdmitsAnyAddressWithTheToken()
    {
        await using var app = await StartAsync(Outside, allowFrom: null);
        using var response = await ScrapeAsync(app, Bearer(Token));
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task LoopbackOutsideAllowFrom_WithTheToken_Is200()
    {
        await using var app = await StartAsync(IPAddress.Loopback, allowFrom: "10.0.0.0/8");
        using var response = await ScrapeAsync(app, Bearer(Token));
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task OutsideAllowFrom_IsRefusedBeforeTheTokenIsChecked()
    {
        var logger = new DarlingMetricsHostTests.CapturingLogger();
        await using var app = await StartAsync(Outside, allowFrom: "10.0.0.0/8", logger: logger);

        using var response = await ScrapeAsync(app, Bearer("wrong-9013-value"));

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
        Assert.False(response.Headers.Contains("WWW-Authenticate"));
        var warning = Assert.Single(logger.Lines, l => l.Level == LogLevel.Warning);
        Assert.Contains("HTTP 403", warning.Text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task UnknownPath_404()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await StartAsync(IPAddress.Loopback);
        using var client = app.GetTestClient();

        foreach (var path in new[] { "/", "/metrics/x", "/livez" })
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, path);
            request.Headers.Authorization = Bearer(Token);
            using var response = await client.SendAsync(request, ct);
            Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
            Assert.Equal("", await response.Content.ReadAsStringAsync(ct));
        }
    }

    [Theory]
    [InlineData("HEAD")]
    [InlineData("POST")]
    [InlineData("PUT")]
    [InlineData("DELETE")]
    [InlineData("PATCH")]
    public async Task OtherMethods_Are405WithAnEmptyBody(string method)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await StartAsync(IPAddress.Loopback);
        using var client = app.GetTestClient();
        using var request = new HttpRequestMessage(new HttpMethod(method), "/metrics");
        request.Headers.Authorization = Bearer(Token);

        using var response = await client.SendAsync(request, ct);

        Assert.Equal(HttpStatusCode.MethodNotAllowed, response.StatusCode);
        Assert.Equal("", await response.Content.ReadAsStringAsync(ct));
    }
}
