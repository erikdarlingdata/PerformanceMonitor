/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6231: a failed webhook post stores "HTTP &lt;status&gt; &lt;reason&gt;" and nothing the endpoint wrote. The alert log's
/// send_error and the channel health text are read over MCP, and a destination can be any address the host can reach, so the
/// endpoint's reply body goes to the service log only. The Test buttons are interactive and store nothing, so they keep showing
/// the body to the operator who clicked.
/// </summary>
public sealed class WebhookErrorTextTests
{
    private const string Body = "internal-admin-console: token=ZZ-9f31 table=users";

    private static (WebhookAlertService Service, LineLogger Log) Build(string genericUrl)
    {
        var config = new DarlingConfig();
        config.Webhooks.GenericUrl = genericUrl;
        var log = new LineLogger();
        return (new WebhookAlertService(new DarlingAlertSettings(config), DarlingAlertDeliverer.Branding, log), log);
    }

    [Fact]
    public async Task AFailedPost_StoresTheStatusOnly_AndTheLogGetsTheBody()
    {
        using var endpoint = new CapturingWebhookEndpoint(statusCode: 500, responseBody: Body);
        var (webhooks, log) = Build(endpoint.Url);

        var result = await webhooks.TrySendWebhookAlertsAsync("High CPU", "SQL01", "97%", "90%");

        Assert.Equal(AlertChannelOutcome.Failed, result.Outcome);
        Assert.Equal("Generic: HTTP 500 Internal Server Error", result.SendError);
        Assert.Equal("HTTP 500 Internal Server Error", webhooks.GetGenericHealth().LastError);
        Assert.DoesNotContain("ZZ-9f31", result.SendError, StringComparison.Ordinal);

        var lines = string.Join("\n", log.Lines);
        Assert.Contains(Body, lines, StringComparison.Ordinal);
        Assert.Contains("HTTP 500 Internal Server Error", lines, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheStoredText_IsTheStandardStatusName_NotTheReasonPhraseTheEndpointSent()
    {
        /* The endpoint's own reason phrase on its status line is "Error"; the stored reason is the standard name of the code. */
        using var endpoint = new CapturingWebhookEndpoint(statusCode: 429, responseBody: Body);
        var (webhooks, _) = Build(endpoint.Url);

        var result = await webhooks.TrySendWebhookAlertsAsync("High CPU", "SQL01", "97%", "90%");

        Assert.Equal("Generic: HTTP 429 Too Many Requests", result.SendError);
    }

    [Fact]
    public async Task TheTestButton_StillShowsTheBodyToTheOperator()
    {
        using var endpoint = new CapturingWebhookEndpoint(statusCode: 400, responseBody: "invalid_payload");

        var error = await WebhookAlertService.SendTestSlackAsync(endpoint.Url, null, DarlingAlertDeliverer.Branding);

        Assert.Equal("HTTP 400 Bad Request: invalid_payload", error);
    }

    private sealed class LineLogger : ILogger<WebhookAlertService>
    {
        private readonly List<string> _lines = new();

        public IReadOnlyList<string> Lines
        {
            get
            {
                lock (_lines)
                {
                    return _lines.ToArray();
                }
            }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (_lines)
            {
                _lines.Add(formatter(state, exception));
            }
        }
    }
}
