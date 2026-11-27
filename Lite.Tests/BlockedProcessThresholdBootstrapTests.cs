/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9743: the refusal memory behind the blocked process threshold bootstrap that Lite and Darling share. A server that refuses the
/// batch (a permission denial, or no sp_configure for it) is asked and reported once, a server that answered is read again on the
/// next run, any other failure (a timeout, a dropped connection, a deadlock victim, a lock timeout) is asked again, a change to the
/// connection settings asks again, and a removed server is asked again. The SqlExceptions are real ones, read by Lite's own
/// <see cref="RemoteCollectorService.ErrorNumbersOf"/>, the reader Lite hands the bootstrap.
/// </summary>
public sealed class BlockedProcessThresholdBootstrapTests
{
    private const string Settings = "Data Source=one;User ID=monitor";

    /// <summary>A database error that carries no SQL Server error number, as a non-SqlClient provider raises.</summary>
    private sealed class NumberlessDbError : DbException
    {
        public NumberlessDbError(string message) : base(message)
        {
        }
    }

    private static IEnumerable<int> Numbers(DbException ex) => RemoteCollectorService.ErrorNumbersOf(ex);

    private static SqlException Error(int number) => SqlExceptionFactory.Create(number, errorClass: 14, message: $"SQL Server error {number}.");

    /// <summary>What sp_configure answers a login that lacks ALTER SETTINGS, and what AWS RDS answers the batch with.</summary>
    private static SqlException Refusal() => Error(15247);

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

        var first = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
        var refusals = first.Refusal is null ? 0 : 1;
        for (var run = 0; run < 9; run++)
        {
            var later = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
            Assert.Equal(BlockedProcessThresholdOutcome.AlreadyRefused, later.Outcome);
            refusals += later.Refusal is null ? 0 : 1;
        }

        Assert.Equal(BlockedProcessThresholdOutcome.Refused, first.Outcome);
        Assert.Equal(1, batch.Sent);
        Assert.Equal(1, refusals);
    }

    [Fact]
    public async Task A_server_that_set_the_threshold_is_read_again_on_the_next_run()
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var batch = new Batch(attempt => Task.FromResult(attempt == 1 ? 0 : 5));

        var first = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
        var second = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);

        Assert.Equal(BlockedProcessThresholdOutcome.Configured, first.Outcome);
        Assert.Equal(BlockedProcessThresholdOutcome.AlreadySet, second.Outcome);
        Assert.Equal(2, batch.Sent);
    }

    [Fact]
    public async Task A_server_that_already_had_a_threshold_is_read_again_on_every_run()
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var batch = Answers(5);

        for (var run = 0; run < 3; run++)
        {
            var result = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
            Assert.Equal(BlockedProcessThresholdOutcome.AlreadySet, result.Outcome);
        }

        Assert.Equal(3, batch.Sent);
    }

    /// <summary>An availability group listener fails over to a replica whose threshold is 0: the connection string is the same, and the next run sets it.</summary>
    [Fact]
    public async Task A_server_that_fails_over_to_a_replica_with_no_threshold_is_configured_on_the_next_run()
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var batch = new Batch(attempt => Task.FromResult(attempt == 1 ? 5 : 0));

        var primary = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
        var replica = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);

        Assert.Equal(BlockedProcessThresholdOutcome.AlreadySet, primary.Outcome);
        Assert.Equal(BlockedProcessThresholdOutcome.Configured, replica.Outcome);
        Assert.Equal(2, batch.Sent);
    }

    [Fact]
    public async Task Each_server_is_decided_on_its_own()
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var refusing = Throws(Refusal());
        var accepting = Answers(0);

        await bootstrap.RunAsync(1, Settings, refusing.SendAsync, Numbers);
        var other = await bootstrap.RunAsync(2, Settings, accepting.SendAsync, Numbers);
        var refusedAgain = await bootstrap.RunAsync(1, Settings, refusing.SendAsync, Numbers);

        Assert.Equal(BlockedProcessThresholdOutcome.Configured, other.Outcome);
        Assert.Equal(1, accepting.Sent);
        Assert.Equal(BlockedProcessThresholdOutcome.AlreadyRefused, refusedAgain.Outcome);
        Assert.Equal(1, refusing.Sent);
    }

    [Fact]
    public async Task A_change_to_the_connection_settings_asks_again()
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var batch = Throws(Refusal());

        await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
        var unchanged = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
        var changed = await bootstrap.RunAsync(1, "Data Source=one;User ID=another-login", batch.SendAsync, Numbers);
        var changedAgain = await bootstrap.RunAsync(1, "Data Source=one;User ID=another-login", batch.SendAsync, Numbers);

        Assert.Equal(BlockedProcessThresholdOutcome.AlreadyRefused, unchanged.Outcome);
        Assert.Equal(BlockedProcessThresholdOutcome.Refused, changed.Outcome);
        Assert.Equal(BlockedProcessThresholdOutcome.AlreadyRefused, changedAgain.Outcome);
        Assert.Equal(2, batch.Sent);
    }

    [Fact]
    public async Task A_forgotten_server_is_asked_again()
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var batch = Throws(Refusal());
        var other = Throws(Refusal());

        await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
        await bootstrap.RunAsync(2, Settings, other.SendAsync, Numbers);
        bootstrap.Forget(1);
        var again = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
        var untouched = await bootstrap.RunAsync(2, Settings, other.SendAsync, Numbers);

        Assert.Equal(BlockedProcessThresholdOutcome.Refused, again.Outcome);
        Assert.Equal(2, batch.Sent);
        Assert.Equal(BlockedProcessThresholdOutcome.AlreadyRefused, untouched.Outcome);
        Assert.Equal(1, other.Sent);
    }

    /// <summary>Removing a server in Lite clears its collector health, and its refusal goes with it, so the same server added again is asked again.</summary>
    [Fact]
    public async Task A_removed_servers_refusal_is_forgotten_in_lite()
    {
        var service = new RemoteCollectorService(duckDb: null!, serverManager: null!, scheduleManager: null!);
        var batch = Throws(Refusal());
        var other = Throws(Refusal());

        await service.BlockedProcessThresholdBootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
        await service.BlockedProcessThresholdBootstrap.RunAsync(2, Settings, other.SendAsync, Numbers);
        service.ClearHealthForServer(1);
        var again = await service.BlockedProcessThresholdBootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
        var untouched = await service.BlockedProcessThresholdBootstrap.RunAsync(2, Settings, other.SendAsync, Numbers);

        Assert.Equal(BlockedProcessThresholdOutcome.Refused, again.Outcome);
        Assert.Equal(2, batch.Sent);
        Assert.Equal(BlockedProcessThresholdOutcome.AlreadyRefused, untouched.Outcome);
        Assert.Equal(1, other.Sent);
    }

    [Fact]
    public async Task A_timeout_says_nothing_about_the_server_and_is_asked_again()
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var batch = new Batch(attempt => attempt == 1
            ? Task.FromException<int>(Error(-2))
            : Task.FromResult(0));

        var timedOut = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
        var retried = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);

        Assert.Equal(BlockedProcessThresholdOutcome.Transient, timedOut.Outcome);
        Assert.Null(timedOut.Refusal);
        Assert.Equal(BlockedProcessThresholdOutcome.Configured, retried.Outcome);
        Assert.Equal(2, batch.Sent);
    }

    /// <summary>
    /// The errors that are the server's own answer to the batch: permission denied from sp_configure (15247), from RECONFIGURE (5812)
    /// and on an object or the server (229, 297, 300), the option missing (15123) and sp_configure missing (2812).
    /// </summary>
    [Theory]
    [InlineData(15247)]
    [InlineData(5812)]
    [InlineData(229)]
    [InlineData(297)]
    [InlineData(300)]
    [InlineData(15123)]
    [InlineData(2812)]
    public async Task The_servers_own_refusal_is_recorded_once_and_the_next_run_does_not_ask(int number)
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var batch = Throws(Error(number));

        var first = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
        var next = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);

        Assert.Equal(BlockedProcessThresholdOutcome.Refused, first.Outcome);
        Assert.NotNull(first.Refusal);
        Assert.Equal(BlockedProcessThresholdOutcome.AlreadyRefused, next.Outcome);
        Assert.Null(next.Refusal);
        Assert.Equal(1, batch.Sent);
    }

    /// <summary>
    /// A fault of the moment says nothing about the server: a deadlock victim (1205), a lock timeout (1222), a command timeout (-2),
    /// the SNI drops (64 and 121), a reset connection (10054) and a login that failed (18456). None is remembered, so the batch is
    /// sent again on the next run and the threshold is set once the server answers.
    /// </summary>
    [Theory]
    [InlineData(1205)]
    [InlineData(1222)]
    [InlineData(-2)]
    [InlineData(64)]
    [InlineData(121)]
    [InlineData(10054)]
    [InlineData(18456)]
    public async Task A_fault_of_the_moment_is_not_remembered_and_the_batch_is_sent_again_on_the_next_run(int number)
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var batch = new Batch(attempt => attempt == 1 ? Task.FromException<int>(Error(number)) : Task.FromResult(0));

        var faulted = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
        var retried = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);

        Assert.Equal(BlockedProcessThresholdOutcome.Transient, faulted.Outcome);
        Assert.Null(faulted.Refusal);
        Assert.Equal(BlockedProcessThresholdOutcome.Configured, retried.Outcome);
        Assert.Equal(2, batch.Sent);
    }

    /// <summary>A database error with no SQL Server number is not the server's refusal either.</summary>
    [Fact]
    public async Task A_database_error_without_a_sql_number_is_not_remembered()
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var batch = new Batch(attempt => attempt == 1 ? Task.FromException<int>(new NumberlessDbError("no number")) : Task.FromResult(5));

        var faulted = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
        var retried = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);

        Assert.Equal(BlockedProcessThresholdOutcome.Transient, faulted.Outcome);
        Assert.Equal(BlockedProcessThresholdOutcome.AlreadySet, retried.Outcome);
        Assert.Equal(2, batch.Sent);
    }

    /// <summary>One deadlock must not switch blocked process reports off: a fault of the moment, then the server's refusal, is remembered only at the refusal.</summary>
    [Fact]
    public async Task A_fault_of_the_moment_before_a_refusal_leaves_the_refusal_to_be_recorded_once()
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var batch = new Batch(attempt => Task.FromException<int>(attempt == 1 ? Error(1205) : Refusal()));

        var deadlocked = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
        var refused = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
        var later = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);

        Assert.Equal(BlockedProcessThresholdOutcome.Transient, deadlocked.Outcome);
        Assert.Equal(BlockedProcessThresholdOutcome.Refused, refused.Outcome);
        Assert.Equal(BlockedProcessThresholdOutcome.AlreadyRefused, later.Outcome);
        Assert.Equal(2, batch.Sent);
    }

    [Fact]
    public async Task A_cancelled_attempt_leaves_the_server_untried()
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var cancelled = Throws(new OperationCanceledException());
        var accepting = Answers(0);

        await Assert.ThrowsAsync<OperationCanceledException>(() => bootstrap.RunAsync(1, Settings, cancelled.SendAsync, Numbers));
        var next = await bootstrap.RunAsync(1, Settings, accepting.SendAsync, Numbers);

        Assert.Equal(BlockedProcessThresholdOutcome.Configured, next.Outcome);
    }

    [Fact]
    public void The_refusal_message_names_the_server_detail_and_differs_by_platform()
    {
        var rds = BlockedProcessThresholdBootstrap.RefusalMessage(new CollectorTargetInfo { IsAwsRds = true }, new[] { "the server said no" });
        var other = BlockedProcessThresholdBootstrap.RefusalMessage(new CollectorTargetInfo(), new[] { "the server said no" });

        Assert.Contains("the server said no", rds);
        Assert.Contains("the server said no", other);
        Assert.NotEqual(rds, other);
    }

    private const string PermissionDenied = "User does not have permission to perform this action.";

    private const string ReconfigureDenied = "You do not have permission to run the RECONFIGURE statement.";

    /// <summary>
    /// #9818: the batch runs sp_configure and RECONFIGURE and AWS RDS refuses each twice, so the SqlException carries 15247, 5812, 15247
    /// and 5812 and its Message joins the same two messages twice with line breaks. The refusal line names each message once, in the
    /// order the server sent them, on one line, for the RDS wording and for the other.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_refusal_that_repeats_its_errors_names_each_message_once_on_one_line(bool awsRds)
    {
        var bootstrap = new BlockedProcessThresholdBootstrap();
        var answer = SqlExceptionFactory.CreateMany(
            14, (15247, PermissionDenied), (5812, ReconfigureDenied), (15247, PermissionDenied), (5812, ReconfigureDenied));
        var batch = Throws(answer);

        var result = await bootstrap.RunAsync(1, Settings, batch.SendAsync, Numbers);
        var line = BlockedProcessThresholdBootstrap.RefusalMessage(new CollectorTargetInfo { IsAwsRds = awsRds }, RemoteCollectorService.ErrorMessagesOf(result.Refusal));

        Assert.Contains('\n', answer.Message);
        Assert.Equal(BlockedProcessThresholdOutcome.Refused, result.Outcome);
        Assert.DoesNotContain('\n', line);
        Assert.DoesNotContain('\r', line);
        Assert.EndsWith($" {PermissionDenied} {ReconfigureDenied}", line, StringComparison.Ordinal);
        Assert.Equal(1, CountOf(line, PermissionDenied));
        Assert.Equal(1, CountOf(line, ReconfigureDenied));
    }

    /// <summary>The errors are already a list at the shared function: repeats, interleaving, line breaks inside a message and blank entries.</summary>
    [Fact]
    public void The_refusal_detail_names_each_distinct_message_once_in_the_order_sent()
    {
        var line = BlockedProcessThresholdBootstrap.RefusalMessage(
            new CollectorTargetInfo(), new[] { "first", "second", "first", "", "  ", "third\r\nwith a break", "second", "third\nwith a break" });

        Assert.EndsWith(" Detail: first second third with a break", line, StringComparison.Ordinal);
        Assert.DoesNotContain('\n', line);
        Assert.DoesNotContain('\r', line);
    }

    /// <summary>A server that answers with one error keeps its message as sent.</summary>
    [Fact]
    public void A_single_message_is_the_detail_as_sent()
    {
        var line = BlockedProcessThresholdBootstrap.RefusalMessage(new CollectorTargetInfo(), new[] { PermissionDenied });

        Assert.EndsWith($" Detail: {PermissionDenied}", line, StringComparison.Ordinal);
    }

    private static int CountOf(string text, string part)
    {
        var count = 0;
        for (var at = text.IndexOf(part, StringComparison.Ordinal); at >= 0; at = text.IndexOf(part, at + part.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}
