/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9636: <c>send_test_notification</c> sends the Viewer's fixed test message from the service, through the delivery code real alerts
/// use, with the settings the service holds. A configured channel gets exactly one post and the answer reports its status, a channel
/// that is not set up sends nothing, and a failed send answers and logs without any part of the destination.
/// </summary>
public sealed class SendTestNotificationTests
{
    private const string SecretPath = "/services/T0AAA-secret-path/B1BBB-secret-hook";
    private const string SecretQuery = "?token=Zq9x-secret-token";

    private static NotificationTestSeat Seat(Action<DarlingConfig> configure)
    {
        var config = new DarlingConfig();
        configure(config);
        var seat = new NotificationTestSeat();
        seat.Publish(new DarlingAlertSettings(config));
        return seat;
    }

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement.Clone();

    private static int ClosedPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    [Theory]
    [InlineData("teams")]
    [InlineData("slack")]
    [InlineData("webhook")]
    public async Task AConfiguredWebhookChannel_GetsExactlyOnePost_AndTheAnswerReportsItsStatus(string channel)
    {
        using var endpoint = new CapturingWebhookEndpoint(statusCode: 200);
        var seat = Seat(c =>
        {
            c.Webhooks.TeamsUrl = endpoint.Url;
            c.Webhooks.SlackUrl = endpoint.Url;
            c.Webhooks.GenericUrl = endpoint.Url;
        });
        var log = new LineLogger();

        var answer = Parse(await DarlingMcpAlertTools.SendTestNotification(channel, seat, log));

        Assert.Equal("delivered", answer.GetProperty("status").GetString());
        Assert.Equal(channel, answer.GetProperty("channel").GetString());
        Assert.True(answer.GetProperty("delivered").GetBoolean());
        Assert.Equal(200, answer.GetProperty("http_status").GetInt32());
        Assert.Single(endpoint.Bodies);
        Assert.DoesNotContain(endpoint.Url, string.Join("\n", log.Lines), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("teams")]
    [InlineData("slack")]
    [InlineData("pagerduty")]
    [InlineData("webhook")]
    [InlineData("email")]
    public async Task AChannelThatIsNotSetUp_AnswersNotConfigured_AndSendsNothing(string channel)
    {
        /* A webhook is configured on every OTHER channel, so a send that wandered onto the wrong one would show as a post. */
        using var endpoint = new CapturingWebhookEndpoint(statusCode: 200);
        var seat = Seat(c =>
        {
            if (channel != "teams") c.Webhooks.TeamsUrl = endpoint.Url;
            if (channel != "slack") c.Webhooks.SlackUrl = endpoint.Url;
            if (channel != "webhook") c.Webhooks.GenericUrl = endpoint.Url;
        });

        var answer = Parse(await DarlingMcpAlertTools.SendTestNotification(channel, seat));

        Assert.Equal("not_configured", answer.GetProperty("status").GetString());
        Assert.False(answer.GetProperty("delivered").GetBoolean());
        Assert.Empty(endpoint.Bodies);
    }

    [Fact]
    public async Task AnEmptySeat_AnswersNotReady_AndAnUnknownChannelIsInvalid()
    {
        Assert.Equal("not_ready", Parse(await DarlingMcpAlertTools.SendTestNotification("teams", new NotificationTestSeat())).GetProperty("status").GetString());
        var seat = Seat(_ => { });
        Assert.Equal("invalid", Parse(await DarlingMcpAlertTools.SendTestNotification("carrier-pigeon", seat)).GetProperty("status").GetString());
    }

    [Fact]
    public async Task AnEmailChannel_SendsOneMessage_AndAnswersWithTheSmtpResult()
    {
        using var smtp = new CapturingSmtpEndpoint();
        var seat = Seat(c =>
        {
            c.Smtp.Host = "127.0.0.1";
            c.Smtp.Port = smtp.Port;
            c.Smtp.UseSsl = false;
            c.Smtp.From = "monitor@example.test";
            c.Smtp.To = "ops@example.test";
        });

        var answer = Parse(await DarlingMcpAlertTools.SendTestNotification("email", seat));

        Assert.Equal("delivered", answer.GetProperty("status").GetString());
        Assert.Equal("Ok", answer.GetProperty("smtp_status").GetString());
        Assert.Single(smtp.Messages);
    }

    [Fact]
    public async Task AnEndpointThatAnswers500_ReportsTheStatus_AndNotWhatTheEndpointWrote()
    {
        using var endpoint = new CapturingWebhookEndpoint(statusCode: 500, responseBody: "internal-console token=ZZ-9f31");
        var seat = Seat(c => c.Webhooks.SlackUrl = endpoint.Url);

        var raw = await DarlingMcpAlertTools.SendTestNotification("slack", seat);
        var answer = Parse(raw);

        Assert.Equal("failed", answer.GetProperty("status").GetString());
        Assert.False(answer.GetProperty("delivered").GetBoolean());
        Assert.Equal(500, answer.GetProperty("http_status").GetInt32());
        Assert.Contains("HTTP 500", answer.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.DoesNotContain("ZZ-9f31", raw, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("teams")]
    [InlineData("slack")]
    [InlineData("webhook")]
    public async Task AnEndpointThatCannotBeReached_AnswersAndLogsWithNoPartOfTheUrl(string channel)
    {
        var port = ClosedPort();
        var url = $"http://127.0.0.1:{port}{SecretPath}{SecretQuery}";
        var seat = Seat(c =>
        {
            c.Webhooks.TeamsUrl = url;
            c.Webhooks.SlackUrl = url;
            c.Webhooks.GenericUrl = url;
            c.Webhooks.GenericHeaders = "{\"Authorization\":\"Bearer hdr-secret-value\"}";
        });
        var log = new LineLogger();

        var raw = await DarlingMcpAlertTools.SendTestNotification(channel, seat, log);
        var answer = Parse(raw);

        Assert.Equal("failed", answer.GetProperty("status").GetString());
        Assert.False(answer.GetProperty("delivered").GetBoolean());
        Assert.Equal(JsonValueKind.Null, answer.GetProperty("http_status").ValueKind);
        Assert.Equal("HttpRequestException", answer.GetProperty("exception_type").GetString());
        var everything = raw + "\n" + string.Join("\n", log.Lines);
        foreach (var part in new[] { "secret-path", "secret-hook", "secret-token", "hdr-secret-value", "127.0.0.1", port.ToString(System.Globalization.CultureInfo.InvariantCulture), "http://" })
        {
            Assert.DoesNotContain(part, everything, StringComparison.Ordinal);
        }

        Assert.Single(log.Lines);
    }

    [Fact]
    public void TheTool_IsAWriteTool_ServedOnlyWhenWritesAreOn()
    {
        Assert.True(DarlingMcpWritePolicy.IsWrite("send_test_notification"));
        Assert.Contains("send_test_notification", DarlingMcpWritePolicy.NeverAgentTools);
        var (tools, _, _) = McpToolsListBudgetTests.BuildServedTools();
        var all = new ModelContextProtocol.Server.McpServerPrimitiveCollection<ModelContextProtocol.Server.McpServerTool>();
        foreach (var tool in tools) all.Add(tool);
        var off = DarlingMcpWritePolicy.FilterToolCollection(all, DarlingMcpWritePolicy.Effective.Off).Select(t => t.ProtocolTool.Name).ToArray();
        var on = DarlingMcpWritePolicy.FilterToolCollection(all, new DarlingMcpWritePolicy.Effective(Writes: true, ServerAdmin: false)).Select(t => t.ProtocolTool.Name).ToArray();

        Assert.DoesNotContain("send_test_notification", off);
        Assert.Contains("send_test_notification", on);
    }

    private sealed class LineLogger : ILogger
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
                _lines.Add(formatter(state, exception) + (exception?.Message ?? ""));
            }
        }
    }
}
