/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data.Common;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9743: the once-per-server decision behind the blocked process threshold bootstrap that Lite and Darling share. A server that
/// refuses the batch is asked and reported once, a server that answered is not asked again, a timeout or dropped connection is
/// asked again, and a change to the connection settings asks again.
/// </summary>
public sealed class BlockedProcessThresholdBootstrapTests
{
    private const string Settings = "Data Source=one;User ID=monitor";

    /// <summary>A database error: the stand-in for a SqlException, which the hosts classify with their own transient rules.</summary>
    private sealed class ServerError : DbException
    {
        public ServerError(string message, bool transient) : base(message) => Transient = transient;

        public bool Transient { get; }
    }

    private static bool IsTransient(DbException ex) => ex is ServerError { Transient: true };

    private static ServerError Refusal() => new("the login lacks permission", transient: false);

    /// <summary>A stand-in for the batch: counts how often it is sent and answers from the queue of behaviors.</summary>
    private sealed class Batch
    {
        private readonly Func<int, Task<int>> _behavior;

        public Batch(Func<int, Task<int>> behavior) => _behavior = behavior;

        public int Sent { get; private set; }

        public Task<int> SendAsync() => _behavior(++Sent);
    }

    private static Batch Answers(int threshold) => new(_ => Task.FromResult(threshold));

    private static Batch Throws(Exception ex) => new(_ => Task.FromException<int>(ex));

    [Fact]
    public async Task A_refusing_server_is_asked_and_reported_once()
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var batch = Throws(Refusal());

        var first = await bootstrap.RunAsync(1, Settings, batch.SendAsync, IsTransient);
        var refusals = first.Refusal is null ? 0 : 1;
        for (var run = 0; run < 9; run++)
        {
            var later = await bootstrap.RunAsync(1, Settings, batch.SendAsync, IsTransient);
            Assert.Equal(BlockedProcessThresholdOutcome.AlreadyTried, later.Outcome);
            refusals += later.Refusal is null ? 0 : 1;
        }

        Assert.Equal(BlockedProcessThresholdOutcome.Refused, first.Outcome);
        Assert.Equal(1, batch.Sent);
        Assert.Equal(1, refusals);
    }

    [Fact]
    public async Task A_server_that_set_the_threshold_is_not_asked_again()
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var batch = Answers(0);

        var first = await bootstrap.RunAsync(1, Settings, batch.SendAsync, IsTransient);
        var second = await bootstrap.RunAsync(1, Settings, batch.SendAsync, IsTransient);

        Assert.Equal(BlockedProcessThresholdOutcome.Configured, first.Outcome);
        Assert.Equal(BlockedProcessThresholdOutcome.AlreadyTried, second.Outcome);
        Assert.Equal(1, batch.Sent);
    }

    [Fact]
    public async Task A_server_that_already_had_a_threshold_is_not_asked_again()
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var batch = Answers(5);

        var first = await bootstrap.RunAsync(1, Settings, batch.SendAsync, IsTransient);
        var second = await bootstrap.RunAsync(1, Settings, batch.SendAsync, IsTransient);

        Assert.Equal(BlockedProcessThresholdOutcome.AlreadySet, first.Outcome);
        Assert.Equal(BlockedProcessThresholdOutcome.AlreadyTried, second.Outcome);
        Assert.Equal(1, batch.Sent);
    }

    [Fact]
    public async Task Each_server_is_decided_on_its_own()
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var refusing = Throws(Refusal());
        var accepting = Answers(0);

        await bootstrap.RunAsync(1, Settings, refusing.SendAsync, IsTransient);
        var other = await bootstrap.RunAsync(2, Settings, accepting.SendAsync, IsTransient);

        Assert.Equal(BlockedProcessThresholdOutcome.Configured, other.Outcome);
        Assert.Equal(1, accepting.Sent);
    }

    [Fact]
    public async Task A_change_to_the_connection_settings_asks_again()
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var batch = Throws(Refusal());

        await bootstrap.RunAsync(1, Settings, batch.SendAsync, IsTransient);
        var unchanged = await bootstrap.RunAsync(1, Settings, batch.SendAsync, IsTransient);
        var changed = await bootstrap.RunAsync(1, "Data Source=one;User ID=another-login", batch.SendAsync, IsTransient);
        var changedAgain = await bootstrap.RunAsync(1, "Data Source=one;User ID=another-login", batch.SendAsync, IsTransient);

        Assert.Equal(BlockedProcessThresholdOutcome.AlreadyTried, unchanged.Outcome);
        Assert.Equal(BlockedProcessThresholdOutcome.Refused, changed.Outcome);
        Assert.Equal(BlockedProcessThresholdOutcome.AlreadyTried, changedAgain.Outcome);
        Assert.Equal(2, batch.Sent);
    }

    [Fact]
    public async Task A_forgotten_server_is_asked_again()
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var batch = Throws(Refusal());

        await bootstrap.RunAsync(1, Settings, batch.SendAsync, IsTransient);
        bootstrap.Forget(1);
        var again = await bootstrap.RunAsync(1, Settings, batch.SendAsync, IsTransient);

        Assert.Equal(BlockedProcessThresholdOutcome.Refused, again.Outcome);
        Assert.Equal(2, batch.Sent);
    }

    [Fact]
    public async Task A_timeout_says_nothing_about_the_server_and_is_asked_again()
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var batch = new Batch(attempt => attempt == 1
            ? Task.FromException<int>(new ServerError("timeout", transient: true))
            : Task.FromResult(0));

        var timedOut = await bootstrap.RunAsync(1, Settings, batch.SendAsync, IsTransient);
        var retried = await bootstrap.RunAsync(1, Settings, batch.SendAsync, IsTransient);
        var settled = await bootstrap.RunAsync(1, Settings, batch.SendAsync, IsTransient);

        Assert.Equal(BlockedProcessThresholdOutcome.Transient, timedOut.Outcome);
        Assert.Null(timedOut.Refusal);
        Assert.Equal(BlockedProcessThresholdOutcome.Configured, retried.Outcome);
        Assert.Equal(BlockedProcessThresholdOutcome.AlreadyTried, settled.Outcome);
        Assert.Equal(2, batch.Sent);
    }

    [Fact]
    public async Task A_cancelled_attempt_leaves_the_server_untried()
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var cancelled = Throws(new OperationCanceledException());
        var accepting = Answers(0);

        await Assert.ThrowsAsync<OperationCanceledException>(() => bootstrap.RunAsync(1, Settings, cancelled.SendAsync, IsTransient));
        var next = await bootstrap.RunAsync(1, Settings, accepting.SendAsync, IsTransient);

        Assert.Equal(BlockedProcessThresholdOutcome.Configured, next.Outcome);
    }

    [Fact]
    public void The_refusal_message_names_the_server_detail_and_differs_by_platform()
    {
        var rds = BlockedProcessThresholdBootstrap.RefusalMessage(new CollectorTargetInfo { IsAwsRds = true }, "the server said no");
        var other = BlockedProcessThresholdBootstrap.RefusalMessage(new CollectorTargetInfo(), "the server said no");

        Assert.Contains("the server said no", rds);
        Assert.Contains("the server said no", other);
        Assert.NotEqual(rds, other);
    }
}
