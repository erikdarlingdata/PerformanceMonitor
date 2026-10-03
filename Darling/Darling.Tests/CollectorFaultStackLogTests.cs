/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5003: a failed collector run logged only the exception's message, so "Collection was modified; enumeration
/// operation may not execute" could not say which collection or which line. The first failure of each kind from each
/// collector now carries its full text; the repeats keep the one line.
/// </summary>
public class CollectorFaultStackLogTests
{
    private static Exception Thrown(Exception exception)
    {
        try
        {
            throw exception;
        }
        catch (Exception caught)
        {
            return caught;
        }
    }

    [Fact]
    public void TheFirstFailureOfAKindCarriesTheTypeTheMessageAndTheFrameThatThrew()
    {
        var log = new CollectorFaultStackLog();

        var text = log.TakeFirst("pg_log_events", Thrown(new InvalidOperationException("Collection was modified; enumeration operation may not execute.")));

        Assert.NotNull(text);
        Assert.Contains("System.InvalidOperationException", text, StringComparison.Ordinal);
        Assert.Contains("Collection was modified", text, StringComparison.Ordinal);
        Assert.Contains(nameof(Thrown), text, StringComparison.Ordinal);
    }

    [Fact]
    public void ARepeatOfTheSameKindFromTheSameCollectorGetsNothing()
    {
        var log = new CollectorFaultStackLog();

        Assert.NotNull(log.TakeFirst("pg_log_events", Thrown(new InvalidOperationException("one"))));
        Assert.Null(log.TakeFirst("pg_log_events", Thrown(new InvalidOperationException("two"))));
        Assert.Null(log.TakeFirst("pg_log_events", Thrown(new InvalidOperationException("three"))));
    }

    [Fact]
    public void AnotherCollectorOrAnotherTypeIsAnotherKind()
    {
        var log = new CollectorFaultStackLog();

        Assert.NotNull(log.TakeFirst("pg_log_events", Thrown(new InvalidOperationException("x"))));
        Assert.NotNull(log.TakeFirst("pg_deadlocks", Thrown(new InvalidOperationException("x"))));
        Assert.NotNull(log.TakeFirst("pg_log_events", Thrown(new ArgumentException("x"))));
    }

    /// <summary>
    /// An RDS log read wraps every fault in <see cref="RdsLogUnavailableException"/>. A network fault that came first
    /// must not use up the stack for a bug inside the read that comes later, so the type underneath is part of the kind.
    /// </summary>
    [Fact]
    public void AWrapperOverADifferentCauseIsAnotherKind()
    {
        var log = new CollectorFaultStackLog();

        var network = new RdsLogUnavailableException("unreachable", false, new HttpRequestException("unreachable"));
        var bug = new RdsLogUnavailableException(
            "Collection was modified; enumeration operation may not execute.", false, Thrown(new InvalidOperationException("Collection was modified; enumeration operation may not execute.")));

        Assert.NotNull(log.TakeFirst("pg_log_events", Thrown(network)));
        Assert.NotNull(log.TakeFirst("pg_log_events", Thrown(bug)));
        Assert.Null(log.TakeFirst("pg_log_events", Thrown(bug)));

        var text = new CollectorFaultStackLog().TakeFirst("pg_log_events", Thrown(bug));
        Assert.Contains("System.InvalidOperationException", text, StringComparison.Ordinal);
    }

    /// <summary>The failure arm is where an out-of-memory run lands, and building the text allocates.</summary>
    [Fact]
    public void AnOutOfMemoryFailureGetsNoStackAndUsesUpNothing()
    {
        var log = new CollectorFaultStackLog();

        Assert.Null(log.TakeFirst("wait_stats", new OutOfMemoryException()));
        Assert.Null(log.TakeFirst("wait_stats", new InsufficientMemoryException()));
        Assert.Null(log.TakeFirst("wait_stats", new OutOfMemoryException()));
    }

    /// <summary>
    /// The RDS ingestors wrap every fault that is not a cancellation in <see cref="RdsLogUnavailableException"/>, so an
    /// out-of-memory read reaches the failure arm one level down. It takes the one-line arm like a bare one, wherever it
    /// sits under the wrapper, and a wrapper over any other cause still gets its stack.
    /// </summary>
    [Fact]
    public void AnOutOfMemoryUnderAWrapperGetsNoStackEither()
    {
        var log = new CollectorFaultStackLog();

        var wrapped = new RdsLogUnavailableException("The log read failed.", false, new OutOfMemoryException());
        var deeper = new InvalidOperationException("outer", new RdsLogUnavailableException("The log read failed.", false, new InsufficientMemoryException()));
        var outerOverOther = new OutOfMemoryException("outer", new HttpRequestException("unreachable"));

        Assert.Null(log.TakeFirst("pg_log_events", Thrown(wrapped)));
        Assert.Null(log.TakeFirst("pg_log_events", Thrown(wrapped)));
        Assert.Null(log.TakeFirst("pg_deadlocks", Thrown(deeper)));
        Assert.Null(log.TakeFirst("pg_plan_capture", Thrown(outerOverOther)));

        var network = new RdsLogUnavailableException("unreachable", false, new HttpRequestException("unreachable"));

        Assert.NotNull(log.TakeFirst("pg_log_events", Thrown(network)));
    }

    /// <summary>An exception whose own <see cref="Exception.ToString"/> throws, as a type with a faulty override could.</summary>
    private sealed class UnrenderableException : Exception
    {
        public UnrenderableException() : base("this exception cannot render itself") { }

        public override string ToString() => throw new InvalidOperationException("ToString failed");
    }

    /// <summary>
    /// The kind is claimed after the text is built. When building it throws, the kind still logs once, with a short line
    /// that names the exception and what threw, and the second failure of the kind gets nothing.
    /// </summary>
    [Fact]
    public void AnExceptionWhoseTextCannotBeBuiltStillLogsItsKindOnceWithAShortLine()
    {
        var log = new CollectorFaultStackLog();

        var first = log.TakeFirst("pg_log_events", Thrown(new UnrenderableException()));

        Assert.NotNull(first);
        Assert.Contains(nameof(UnrenderableException), first, StringComparison.Ordinal);
        Assert.Contains("System.InvalidOperationException", first, StringComparison.Ordinal);
        Assert.DoesNotContain(Environment.NewLine, first, StringComparison.Ordinal);

        Assert.Null(log.TakeFirst("pg_log_events", Thrown(new UnrenderableException())));
        Assert.NotNull(log.TakeFirst("pg_deadlocks", Thrown(new UnrenderableException())));
    }

    [Fact]
    public async Task EightRunsFailingTogetherInOneWayYieldExactlyOneStack()
    {
        var log = new CollectorFaultStackLog();
        using var start = new Barrier(8);

        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(i => Task.Factory.StartNew(
            () =>
            {
                start.SignalAndWait();
                return log.TakeFirst("pg_log_events", Thrown(new InvalidOperationException("run " + i)));
            },
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default))).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(1, results.Count(text => text is not null));
    }

    /// <summary>
    /// The wiring, pinned from source because the failure arm needs a store to run. The one line every failure logs is
    /// unchanged (<c>StoreCopyPhaseTests</c> pins its shape), the full text is a second line written only for the first
    /// failure of a kind, and the timeout arm does not take it: its filter admits only a command timeout from the driver,
    /// so a stack there would be driver frames, and the authored sentence already names everything that matters.
    /// </summary>
    [Fact]
    public void OnlyTheGeneralFailureArmWritesTheFullTextAndItIsTheFirstOfAKindOnly()
    {
        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

        Assert.Single(System.Text.RegularExpressions.Regex.Matches(worker, @"_collectorFaultStacks\.TakeFirst\(collectorName, ex\)"));

        var errorLine = worker.IndexOf("=> ERROR: {Message}\",", StringComparison.Ordinal);
        var detail = worker.IndexOf("_collectorFaultStacks.TakeFirst(collectorName, ex)", StringComparison.Ordinal);
        var dropRuntime = worker.IndexOf("ConnectionFaultDisposition.ShouldDropRuntimeAsync(", detail, StringComparison.Ordinal);
        var timeoutArm = worker.IndexOf("=> ERROR (timeout): {Message}", StringComparison.Ordinal);

        Assert.True(timeoutArm > 0 && timeoutArm < errorLine, "The timeout arm comes before the general arm.");
        Assert.True(errorLine < detail && detail < dropRuntime, "The full text is written after the one line and before the connection check.");

        /* The timeout arm and everything up to the general arm's own line: no full text there. */
        Assert.DoesNotContain("TakeFirst", worker[timeoutArm..errorLine], StringComparison.Ordinal);

        /* Same exception object, not a copy, and a LogError so the level is the arm's own. */
        Assert.Contains("_logger.LogError(ex,", worker[detail..dropRuntime], StringComparison.Ordinal);
    }
}
