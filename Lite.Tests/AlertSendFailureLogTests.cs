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
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// <see cref="EmailAlertService.TrySendAlertEmailAsync"/> never throws: a failure anywhere in the send is logged at
/// Error and answered with null. The entry it logs carries the exception itself, not only the text its message
/// formats into the line, so Lite's log gets the type, the stack and the inner chain of the throw.
/// </summary>
public sealed class AlertSendFailureLogTests
{
    [Fact]
    public async Task AFailedSend_LogsTheExceptionItself_NotOnlyItsMessage()
    {
        var log = new CapturingLogger();
        var settings = new AlertWebhookCancelTests.GenericWebhookSettings("");
        var webhooks = new WebhookAlertService(settings, EmailAlertService.Branding, new AppLoggerAdapter<WebhookAlertService>());
        var service = new EmailAlertService(settings, new ThrowingHistoryStore(), webhooks, log);

        var delivery = await service.TrySendAlertEmailAsync("High CPU", "SQL01", "97%", "90%", serverId: 1);

        Assert.Null(delivery);
        Assert.Contains(
            log.Entries,
            x => x.Level == LogLevel.Error
                && x.Message == "TrySendAlertEmailAsync outer error: history row write boom"
                && x.Exception is InvalidOperationException { Message: "history row write boom" });
    }

    /// <summary>A history store whose row write fails, the way a locked or closed DuckDB file fails it. The cooldown
    /// reads answer empty so the send reaches the write.</summary>
    private sealed class ThrowingHistoryStore : IAlertHistoryStore
    {
        public Task RecordAlertAsync(AlertHistoryRecord record) =>
            throw new InvalidOperationException("history row write boom");

        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) =>
            Task.FromResult<DateTime?>(null);

        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) =>
            Task.FromResult<DateTime?>(null);
    }

    /// <summary>Records each entry with the exception object the logger was handed, null when a call passed only text.</summary>
    private sealed class CapturingLogger : ILogger<EmailAlertService>
    {
        public List<(LogLevel Level, string Message, Exception? Exception)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Entries.Add((logLevel, formatter(state, exception), exception));
    }
}
