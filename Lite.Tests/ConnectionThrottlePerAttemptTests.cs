/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4722: the connection throttle is held for one connect ATTEMPT, never across the backoff between
/// attempts.
///
/// <para><b>Why it matters.</b> The dead-server errors (-2, -1, 2, 53, 10060, 10061) are on the transient
/// list, so a server that went down since its last status check is retried: four opens, each up to the
/// connect timeout, with 1 + 2 + 4 seconds of backoff between them. Holding a slot across that whole loop
/// meant one dead server tied up a slot for the full length of it, for every collector that tried it, and
/// seven of them at once stopped collection for every other server.</para>
///
/// <para><b>How it is pinned without a server.</b> The throttle and the retry loop live in
/// <c>RemoteCollectorService.ExecuteThrottledWithRetryAsync</c>, which takes the semaphore as a parameter.
/// These tests hand it a semaphore of their own (the production one is process-wide and the suite runs
/// classes in parallel, so exact-count assertions against it would be a flake), a logger that reads the
/// semaphore at the moment <c>RetryHelper</c> announces its backoff, and attempts that fail the way a dead
/// server does. A source pin then ties <c>CreateConnectionAsync</c> to the seam and to the ordering the
/// issue asks for: the interactive sign-in lock first, the throttle inside it.</para>
///
/// <para>The same source pin covers what an attempt does with the connection it built when its open fails:
/// it disposes it, for every kind of server, so a run of failed attempts against a server that is down does
/// not leave a run of undisposed connections behind.</para>
/// </summary>
public class ConnectionThrottlePerAttemptTests
{
    /// <summary>The width of <c>RemoteCollectorService.s_connectionThrottle</c>.</summary>
    private const int Width = 7;

    private const string Operation = "Connect to test";

    [Fact]
    public void TheTestException_IsATransientSqlError()
    {
        /* Guards the factory below: if the driver's internals move and the number stops reaching
           SqlException.Errors, every retry test in this class would exercise the wrong branch. */
        foreach (var number in new[] { -2, -1, 2, 53, 10060, 10061 })
        {
            var ex = SqlExceptionFactory.Create(number);
            Assert.Equal(number, ex.Number);
            Assert.True(RetryHelper.IsTransient(ex), $"error {number} must be on the transient list");
        }

        Assert.False(RetryHelper.IsTransient(SqlExceptionFactory.Create(18456)));
    }

    [Fact]
    public async Task TheSlotIsFree_WhileTheRetryBackoffWaits()
    {
        var throttle = new SemaphoreSlim(Width, Width);
        using var cancel = new CancellationTokenSource();
        var log = new BackoffProbeLog(throttle, cancel);
        var attempts = 0;

        Task<int> DeadServer()
        {
            attempts++;
            return Task.FromException<int>(SqlExceptionFactory.Create(-2));
        }

        /* The probe cancels as it reads, so RetryHelper's Task.Delay ends at once and the test does not
           wait out the second of backoff it is inspecting. */
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            RemoteCollectorService.ExecuteThrottledWithRetryAsync<int>(throttle, DeadServer, log, Operation, cancel.Token));

        Assert.Equal(1, attempts);
        var freeAtBackoff = Assert.Single(log.FreeSlotsAtBackoff);
        Assert.True(freeAtBackoff == Width,
            $"the throttle read {freeAtBackoff} of {Width} free while RetryHelper waited out its backoff; " +
            "a slot is held across the delay");
        Assert.Equal(Width, throttle.CurrentCount);
    }

    [Fact]
    public async Task EveryAttempt_RunsHoldingASlot()
    {
        var throttle = new SemaphoreSlim(Width, Width);
        var freeInsideAttempt = new List<int>();

        async Task<string> RefusedOnceThenOpens()
        {
            freeInsideAttempt.Add(throttle.CurrentCount);
            if (freeInsideAttempt.Count == 1)
            {
                throw SqlExceptionFactory.Create(10061);
            }

            await Task.Yield();
            return "opened";
        }

        /* One real backoff second: the retried attempt is the case a fix that took the throttle once and
           then let go of it would get wrong, and RetryHelper has no clock to fake. */
        var result = await RemoteCollectorService.ExecuteThrottledWithRetryAsync<string>(
            throttle, RefusedOnceThenOpens, logger: null, Operation, CancellationToken.None);

        Assert.Equal("opened", result);
        Assert.Equal(new[] { Width - 1, Width - 1 }, freeInsideAttempt);
        Assert.Equal(Width, throttle.CurrentCount);
    }

    [Fact]
    public async Task ASuccessfulAttempt_HoldsASlotWhileItRuns_AndReleasesItOnReturn()
    {
        var throttle = new SemaphoreSlim(Width, Width);
        var freeInsideAttempt = -1;

        var result = await RemoteCollectorService.ExecuteThrottledWithRetryAsync<int>(
            throttle,
            async () =>
            {
                freeInsideAttempt = throttle.CurrentCount;
                await Task.Yield();
                return 42;
            },
            logger: null, Operation, CancellationToken.None);

        Assert.Equal(42, result);
        /* Read inside the attempt: this is what fails a fix that stops taking the throttle at all, which the
           backoff test above cannot see - a semaphore nobody touches also reads 7 of 7. */
        Assert.Equal(Width - 1, freeInsideAttempt);
        Assert.Equal(Width, throttle.CurrentCount);
    }

    [Fact]
    public async Task ACancelledAttempt_ReleasesItsSlot()
    {
        var throttle = new SemaphoreSlim(Width, Width);
        using var cancel = new CancellationTokenSource();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var run = RemoteCollectorService.ExecuteThrottledWithRetryAsync<int>(
            throttle,
            async () =>
            {
                started.SetResult();
                await Task.Delay(Timeout.Infinite, cancel.Token);
                return 0;
            },
            logger: null, Operation, cancel.Token);

        await started.Task.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(Width - 1, throttle.CurrentCount);

        cancel.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);
        Assert.Equal(Width, throttle.CurrentCount);
    }

    [Fact]
    public async Task APermanentFailure_ReleasesItsSlot_AndIsNotRetried()
    {
        var throttle = new SemaphoreSlim(Width, Width);
        var attempts = 0;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            RemoteCollectorService.ExecuteThrottledWithRetryAsync<int>(
                throttle,
                () =>
                {
                    attempts++;
                    return Task.FromException<int>(new InvalidOperationException("bad credentials"));
                },
                logger: null, Operation, CancellationToken.None));

        Assert.Equal(1, attempts);
        Assert.Equal(Width, throttle.CurrentCount);
    }

    [Fact]
    public async Task ACancelledWaitForASlot_ReleasesNothing()
    {
        var throttle = new SemaphoreSlim(Width, Width);
        for (var i = 0; i < Width; i++)
        {
            await throttle.WaitAsync();
        }

        using var cancel = new CancellationTokenSource();
        var attempts = 0;

        var run = RemoteCollectorService.ExecuteThrottledWithRetryAsync<int>(
            throttle,
            () =>
            {
                attempts++;
                return Task.FromResult(1);
            },
            logger: null, Operation, cancel.Token);

        Assert.False(run.IsCompleted, "every slot is taken, so the attempt must queue");
        cancel.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);
        Assert.Equal(0, attempts);
        /* 0, not 1: a slot the attempt never held must not be handed back to the pool. */
        Assert.Equal(0, throttle.CurrentCount);

        throttle.Release(Width);
        Assert.Equal(Width, throttle.CurrentCount);
    }

    [Fact]
    public void CreateConnectionAsync_TakesTheSignInLockFirst_ThenRunsItsAttemptsThroughTheThrottle()
    {
        var source = ReadRepoFile(Path.Combine("Lite", "Services", "RemoteCollectorService.cs"));
        var body = MethodBody(source, "Task<SqlConnection> CreateConnectionAsync(");

        var signInLock = body.IndexOf("s_mfaAuthLock.WaitAsync(", StringComparison.Ordinal);
        var throttled = body.IndexOf("ExecuteThrottledWithRetryAsync(s_connectionThrottle,", StringComparison.Ordinal);
        var deviceCodeWindow = body.IndexOf("EntraDeviceCodeAuth.Begin(", StringComparison.Ordinal);

        Assert.True(signInLock >= 0, "CreateConnectionAsync no longer takes the interactive sign-in lock");
        Assert.True(throttled >= 0,
            "CreateConnectionAsync must run its attempts through ExecuteThrottledWithRetryAsync(s_connectionThrottle, ...)");

        /* The interactive lock is the OUTER one: it is taken first and held across the attempts, so two
           collectors starting together still raise one prompt. */
        Assert.True(signInLock < throttled, "the interactive sign-in lock must be taken before the throttle");

        /* The device-code window is raised by the attempt, so it must sit inside the throttled call: a slot
           is held before the window shows. Narrowing the throttle to OpenAsync would move it out. */
        Assert.True(deviceCodeWindow > throttled,
            "EntraDeviceCodeAuth.Begin must run inside the throttled attempt, not before it");

        /* Neither the throttle nor the retry loop is driven from here any more: both live in the seam, so
           there is one place that decides how long a slot is held. */
        Assert.DoesNotContain("s_connectionThrottle.WaitAsync", body, StringComparison.Ordinal);
        Assert.DoesNotContain("s_connectionThrottle.Release", body, StringComparison.Ordinal);
        Assert.DoesNotContain("RetryHelper.ExecuteWithRetryAsync", body, StringComparison.Ordinal);
    }

    [Fact]
    public void AFailedOpen_DisposesItsConnection_ForEveryKindOfServer()
    {
        /* Each attempt builds a SqlConnection of its own, so a failed open used to leave one undisposed
           per attempt - up to four per collector per cycle against a server that is down.
           OpenAzureDatabaseConnectionAsync already disposes on failure (catch { conn.Dispose(); throw; });
           this pins the same for the retry lambda. Read as code, comments and literals blanked, so the
           braces counted below are the real ones. */
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            ReadRepoFile(Path.Combine("Lite", "Services", "RemoteCollectorService.cs")).Replace("\r\n", "\n", StringComparison.Ordinal));
        var body = MethodBody(code, "Task<SqlConnection> CreateConnectionAsync(");

        var open = body.IndexOf("await connection.OpenAsync(", StringComparison.Ordinal);
        Assert.True(open >= 0, "the retry lambda no longer opens 'connection' with OpenAsync");

        var catchAt = body.IndexOf("catch", open, StringComparison.Ordinal);
        Assert.True(catchAt >= 0, "the retry lambda no longer handles a failed open");
        var openBrace = body.IndexOf('{', catchAt);
        var clause = body[catchAt..openBrace].Trim();

        /* Not filtered. The interactive branch decides whether a decline is recorded; it is not a reason
           to keep the connection of a failed attempt alive, and a 'when (isInteractiveServer)' filter would
           dispose for interactive servers only. */
        Assert.Matches(@"^catch(\s*\(\s*Exception\s+\w+\s*\))?$", clause);

        var block = CSharpSourceWalker.BraceBalanced(body, openBrace);
        var dispose = block.IndexOf("connection.Dispose();", StringComparison.Ordinal);
        var rethrow = block.LastIndexOf("throw;", StringComparison.Ordinal);

        Assert.True(dispose >= 0, "a failed open must dispose the connection its attempt built");
        Assert.True(rethrow > dispose, "the connection must be disposed before the failure is rethrown");

        /* At the catch block's own depth, not inside the interactive branch: depth 1 is the catch's own
           opening brace and nothing nested under it. */
        var before = block[..dispose];
        Assert.Equal(1, before.Count(c => c == '{') - before.Count(c => c == '}'));

        /* Success still hands the open connection to the caller: nothing on the way to the catch disposes it. */
        var attempt = body[open..catchAt];
        Assert.Contains("return connection;", attempt, StringComparison.Ordinal);
        Assert.DoesNotContain("Dispose", attempt, StringComparison.Ordinal);
    }

    private static string MethodBody(string source, string signature)
    {
        var text = source.Replace("\r\n", "\n", StringComparison.Ordinal);
        var start = text.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"'{signature}' was not found");

        /* The method's own closing brace is the first line that is exactly four spaces and a brace. */
        var end = text.IndexOf("\n    }\n", start, StringComparison.Ordinal);
        Assert.True(end > start, $"the end of '{signature}' was not found");
        return text.Substring(start, end - start);
    }

    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile);
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }

    /// <summary>
    /// Records how many slots are free the moment <c>RetryHelper</c> announces a backoff, then cancels so the
    /// test does not sit through the delay it is inspecting.
    /// </summary>
    private sealed class BackoffProbeLog : ILogger
    {
        private readonly SemaphoreSlim _throttle;
        private readonly CancellationTokenSource _cancel;

        public BackoffProbeLog(SemaphoreSlim throttle, CancellationTokenSource cancel)
        {
            _throttle = throttle;
            _cancel = cancel;
        }

        public List<int> FreeSlotsAtBackoff { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (formatter(state, exception).Contains("Retrying in", StringComparison.Ordinal))
            {
                FreeSlotsAtBackoff.Add(_throttle.CurrentCount);
                _cancel.Cancel();
            }
        }
    }

    /// <summary>SqlException has no public constructor; this builds one through the driver's internals.</summary>
    private static class SqlExceptionFactory
    {
        public static SqlException Create(int number, byte errorClass = 20)
        {
            const BindingFlags all = BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static;
            var errorCtor = typeof(SqlError).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)
                .OrderByDescending(c => c.GetParameters().Length).First();
            var ints = new Queue<object>(new object[] { number, 0, 0, 0 });
            var bytes = new Queue<object>(new object[] { (byte)0, errorClass, (byte)0, (byte)0 });
            var strings = new Queue<object>(new object[] { "server", "message", "procedure", "extra", "extra" });
            var args = errorCtor.GetParameters().Select(p =>
                p.ParameterType == typeof(int) ? ints.Dequeue()
                : p.ParameterType == typeof(byte) ? bytes.Dequeue()
                : p.ParameterType == typeof(string) ? strings.Dequeue()
                : p.ParameterType == typeof(uint) ? (object)0u
                : p.ParameterType == typeof(Exception) ? null!
                : p.HasDefaultValue ? p.DefaultValue! : throw new InvalidOperationException($"unmapped SqlError ctor parameter {p.ParameterType}")).ToArray();
            var error = errorCtor.Invoke(args);

            var collection = Activator.CreateInstance(typeof(SqlErrorCollection), true)!;
            typeof(SqlErrorCollection).GetMethod("Add", all, null, new[] { typeof(SqlError) }, null)!.Invoke(collection, new[] { error });

            var create = typeof(SqlException).GetMethods(all)
                .Where(m => m.Name == "CreateException" && m.GetParameters().Length >= 2
                    && m.GetParameters()[0].ParameterType == typeof(SqlErrorCollection)
                    && m.GetParameters()[1].ParameterType == typeof(string))
                .OrderBy(m => m.GetParameters().Length).First();
            var createArgs = create.GetParameters().Select((p, i) =>
                i == 0 ? collection
                : i == 1 ? "11.0"
                : p.ParameterType == typeof(Guid) ? Guid.Empty
                : p.HasDefaultValue ? p.DefaultValue! : null!).ToArray();
            return (SqlException)create.Invoke(null, createArgs)!;
        }
    }
}
