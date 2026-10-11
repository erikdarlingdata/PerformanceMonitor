/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The retry half of #5655: a service version write that failed at the start is tried again on the next sweep and on each one
/// after, until it succeeds, and a write that succeeded is never repeated. No store is needed: the write is a delegate here.
/// </summary>
public sealed class ServiceVersionWriterTests
{
    private static DarlingServiceVersionWriter WriterAnswering(List<bool> retryFlags, params bool[] answers)
    {
        var next = 0;
        return new DarlingServiceVersionWriter((retry, _) =>
        {
            retryFlags.Add(retry);
            return Task.FromResult(answers[next++]);
        });
    }

    [Fact]
    public async Task AFailedFirstWrite_IsTriedAgainOnTheNextSweep_AndNotRepeatedOnceItLands()
    {
        var ct = TestContext.Current.CancellationToken;
        var retryFlags = new List<bool>();
        var writer = WriterAnswering(retryFlags, false, false, true);

        await writer.TryAsync(ct);
        Assert.True(writer.Pending);

        await writer.TryAsync(ct);
        Assert.True(writer.Pending);

        await writer.TryAsync(ct);
        Assert.False(writer.Pending);

        /* Written: further sweeps do nothing (a fourth call would run past the three answers and throw). */
        await writer.TryAsync(ct);
        await writer.TryAsync(ct);

        Assert.Equal(new[] { false, true, true }, retryFlags);
    }

    [Fact]
    public async Task ASuccessfulFirstWrite_IsNotRepeated()
    {
        var ct = TestContext.Current.CancellationToken;
        var retryFlags = new List<bool>();
        var writer = WriterAnswering(retryFlags, true);

        await writer.TryAsync(ct);
        await writer.TryAsync(ct);
        await writer.TryAsync(ct);

        Assert.False(writer.Pending);
        Assert.Equal(new[] { false }, retryFlags);
    }

    [Fact]
    public async Task ACancelledAttempt_EscapesTheRetry()
    {
        var writer = new DarlingServiceVersionWriter((_, ct) =>
        {
            ct.ThrowIfCancellationRequested();
            return Task.FromResult(true);
        });
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => writer.TryAsync(cancelled.Token));
        Assert.True(writer.Pending);
    }

    [Fact]
    public async Task TheFirstFailureWarns_AndEveryRetryAfterItLogsAtDebug()
    {
        var ct = TestContext.Current.CancellationToken;
        /* Nothing listens on port 1, so every attempt fails at the connect without a store. */
        await using var dataSource = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=2;Pooling=false");
        var logger = new CapturingLogger();
        var writer = new DarlingServiceVersionWriter((retry, token) =>
            DarlingServiceVersionRecorder.RecordAsync(dataSource, logger, "HOST-A", "3.11.0", DateTime.UtcNow, token, retry));

        await writer.TryAsync(ct);
        await writer.TryAsync(ct);
        await writer.TryAsync(ct);

        Assert.True(writer.Pending);
        Assert.Equal(3, logger.Levels.Count);
        Assert.Equal(LogLevel.Warning, logger.Levels[0]);
        Assert.Equal(LogLevel.Debug, logger.Levels[1]);
        Assert.Equal(LogLevel.Debug, logger.Levels[2]);
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<LogLevel> Levels { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Levels.Add(logLevel);
    }
}
