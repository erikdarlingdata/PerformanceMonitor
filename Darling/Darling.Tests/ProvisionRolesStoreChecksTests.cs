/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>The pure decisions of the store checks behind <c>--prepare-provision-roles</c> (#8779).</summary>
[Trait("Stage", "Guard")]
public sealed class ProvisionRolesStoreChecksTests
{
    private static readonly string[] Newlines = { "\r\n", "\n" };

    [Theory]
    [InlineData(null, 266, false)]
    [InlineData(265, 266, false)]
    [InlineData(266, 266, true)]
    [InlineData(300, 266, true)]
    public void SchemaReached_Cases(int? stored, int required, bool expected) =>
        Assert.Equal(expected, ProvisionRolesStoreChecks.SchemaReached(stored, required));

    [Fact]
    public void DescribeSchemaTimeout_NamesBothVersions()
    {
        var none = ProvisionRolesStoreChecks.DescribeSchemaTimeout(null, 266, 900);
        Assert.Contains("none", none, StringComparison.Ordinal);
        Assert.Contains("266", none, StringComparison.Ordinal);
        Assert.Contains("900", none, StringComparison.Ordinal);
        Assert.Contains("has not finished migrating the store", none, StringComparison.Ordinal);
        Assert.Contains("250", ProvisionRolesStoreChecks.DescribeSchemaTimeout(250, 266, 900), StringComparison.Ordinal);
    }

    [Fact]
    public void DescribeSchemaTimeout_WithNoVersionEverRead_NamesTheConnectionError()
    {
        var text = ProvisionRolesStoreChecks.DescribeSchemaTimeout(null, 266, 900, "NpgsqlException: Failed to connect to 10.0.0.1:5432");

        Assert.Contains("Failed to connect to 10.0.0.1:5432", text, StringComparison.Ordinal);
        Assert.Contains("NpgsqlException", text, StringComparison.Ordinal);
        Assert.DoesNotContain("has not finished migrating", text, StringComparison.Ordinal);
        Assert.Contains("266", text, StringComparison.Ordinal);
        Assert.Contains("has not finished migrating the store", ProvisionRolesStoreChecks.DescribeSchemaTimeout(250, 266, 900, "NpgsqlException: down"), StringComparison.Ordinal);
    }

    [Fact]
    public void DescribeOwnership_NullWhenNothingIsForeign() =>
        Assert.Null(ProvisionRolesStoreChecks.DescribeOwnership("app", Array.Empty<ProvisionRolesStoreChecks.ForeignOwnedObject>()));

    [Fact]
    public void DescribeOwnership_ListsEveryObjectAndTheFix()
    {
        var kinds = new[] { "table", "view", "materialized view", "sequence", "function", "procedure" };
        var foreign = Enumerable.Range(0, 30).Select(i => kinds[i % kinds.Length] is "function" or "procedure"
            ? new ProvisionRolesStoreChecks.ForeignOwnedObject(kinds[i % kinds.Length], "config", $"obj_{i}(integer, text)", "postgres")
            : new ProvisionRolesStoreChecks.ForeignOwnedObject(kinds[i % kinds.Length], "collect", $"obj_{i}", "postgres")).ToList();

        var text = ProvisionRolesStoreChecks.DescribeOwnership("app", foreign)!;
        var lines = text.Split(Newlines, StringSplitOptions.RemoveEmptyEntries);

        for (var i = 0; i < 30; i++)
        {
            Assert.Single(lines, line => line.Contains($"obj_{i} ", StringComparison.Ordinal) || line.Contains($"obj_{i}(", StringComparison.Ordinal) && line.Contains("(owner postgres)", StringComparison.Ordinal));
        }

        Assert.Equal(30, lines.Count(line => line.StartsWith("ALTER ", StringComparison.Ordinal) && line.EndsWith("OWNER TO \"app\";", StringComparison.Ordinal)));
        Assert.Contains("ALTER FUNCTION \"config\".\"obj_4\"(integer, text) OWNER TO \"app\";", text, StringComparison.Ordinal);
        Assert.Contains("ALTER MATERIALIZED VIEW \"collect\".\"obj_2\" OWNER TO \"app\";", text, StringComparison.Ordinal);
    }

    [Fact]
    public void DescribeOwnership_AlterSchemaNamesOnlyTheSchema()
    {
        var text = ProvisionRolesStoreChecks.DescribeOwnership("app", new[] { new ProvisionRolesStoreChecks.ForeignOwnedObject("schema", "collect", "collect", "postgres") })!;
        Assert.Contains("ALTER SCHEMA \"collect\" OWNER TO \"app\";", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task WaitForSchemaAsync_NullNull266_ReachedAfterThreePolls()
    {
        var reads = Script(null, null, 266);
        var time = new SteppingTimeProvider();
        var (reached, lastSeen, _) = await Wait(reads, time, new StringWriter());

        Assert.True(reached);
        Assert.Equal(266, lastSeen);
        Assert.Equal(3, reads.Calls);
        Assert.Equal(TimeSpan.FromSeconds(20), time.Elapsed);
    }

    [Fact]
    public async Task WaitForSchemaAsync_ThrowThen266_Reached()
    {
        var reads = Script(new NpgsqlException("down"), 266);
        var (reached, lastSeen, _) = await Wait(reads, new SteppingTimeProvider(), new StringWriter());

        Assert.True(reached);
        Assert.Equal(266, lastSeen);
    }

    [Fact]
    public async Task WaitForSchemaAsync_SocketExceptionCountsAsNotYet()
    {
        var reads = Script(new System.Net.Sockets.SocketException(), 266);
        var (reached, _, _) = await Wait(reads, new SteppingTimeProvider(), new StringWriter());

        Assert.True(reached);
    }

    [Fact]
    public async Task WaitForSchemaAsync_OtherExceptionsPropagate() =>
        await Assert.ThrowsAsync<InvalidOperationException>(() => Wait(Script(new InvalidOperationException("bug")), new SteppingTimeProvider(), new StringWriter()));

    [Fact]
    public async Task WaitForSchemaAsync_StuckAt265_NotReachedWhenBudgetIsGone_AndLogsOneLinePerMinute()
    {
        var log = new StringWriter();
        var time = new SteppingTimeProvider();
        var (reached, lastSeen, _) = await ProvisionRolesStoreChecks.WaitForSchemaAsync(
            _ => Task.FromResult<int?>(265), 266, TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(10), log, time, CancellationToken.None);

        Assert.False(reached);
        Assert.Equal(265, lastSeen);
        Assert.Equal(TimeSpan.FromMinutes(5), time.Elapsed);
        var lines = log.ToString().Split(Newlines, StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(5, lines.Length);
        Assert.All(lines, line =>
        {
            Assert.Contains("265", line, StringComparison.Ordinal);
            Assert.Contains("266", line, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task WaitForSchemaAsync_ReportsTheConnectionError_InTheMinuteLineAndTheResult()
    {
        var log = new StringWriter();
        var (reached, lastSeen, lastError) = await ProvisionRolesStoreChecks.WaitForSchemaAsync(
            _ => throw new NpgsqlException("Failed to connect to 10.0.0.1:5432"), 266, TimeSpan.FromMinutes(2), TimeSpan.FromSeconds(10), log, new SteppingTimeProvider(), CancellationToken.None);

        Assert.False(reached);
        Assert.Null(lastSeen);
        Assert.Equal("NpgsqlException: Failed to connect to 10.0.0.1:5432", lastError);
        var lines = log.ToString().Split(Newlines, StringSplitOptions.RemoveEmptyEntries);
        Assert.NotEmpty(lines);
        Assert.All(lines, line => Assert.Contains("NpgsqlException: Failed to connect to 10.0.0.1:5432", line, StringComparison.Ordinal));
    }

    [Fact]
    public async Task WaitForSchemaAsync_ForgetsTheErrorOnceAReadSucceeds()
    {
        var (_, lastSeen, lastError) = await Wait(Script(new NpgsqlException("down"), 265), new SteppingTimeProvider(), new StringWriter());

        Assert.Equal(265, lastSeen);
        Assert.Null(lastError);
    }

    [Fact]
    public async Task WaitForSchemaAsync_KeepsLastSeenAcrossAFailedRead()
    {
        var reads = Script(265, new NpgsqlException("down"));
        var (reached, lastSeen, _) = await ProvisionRolesStoreChecks.WaitForSchemaAsync(
            reads.ReadAsync, 266, TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(10), new StringWriter(), new SteppingTimeProvider(), CancellationToken.None);

        Assert.False(reached);
        Assert.Equal(265, lastSeen);
    }

    private static Task<(bool Reached, int? LastSeen, string? LastError)> Wait(ScriptedReader reads, TimeProvider time, TextWriter log) =>
        ProvisionRolesStoreChecks.WaitForSchemaAsync(reads.ReadAsync, 266, TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(10), log, time, CancellationToken.None);

    private static ScriptedReader Script(params object?[] steps) => new(steps);

    /// <summary>Returns the scripted values in order (an exception step throws); the last step repeats once the script is used up.</summary>
    private sealed class ScriptedReader
    {
        private readonly object?[] _steps;

        public ScriptedReader(object?[] steps) => _steps = steps;

        public int Calls { get; private set; }

        public Task<int?> ReadAsync(CancellationToken ct)
        {
            var step = _steps[Math.Min(Calls, _steps.Length - 1)];
            Calls++;
            return step is Exception ex ? throw ex : Task.FromResult((int?)step);
        }
    }

    /// <summary>A clock that moves only when a delay is asked for: the delay completes at once and the clock steps forward by it.</summary>
    private sealed class SteppingTimeProvider : TimeProvider
    {
        private readonly DateTimeOffset _start = new(2026, 11, 13, 0, 0, 0, TimeSpan.Zero);
        private DateTimeOffset _now = new(2026, 11, 13, 0, 0, 0, TimeSpan.Zero);

        public TimeSpan Elapsed => _now - _start;

        public override DateTimeOffset GetUtcNow() => _now;

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            _now += dueTime;
            ThreadPool.QueueUserWorkItem(_ => callback(state));
            return new NoTimer();
        }

        private sealed class NoTimer : ITimer
        {
            public bool Change(TimeSpan dueTime, TimeSpan period) => true;

            public void Dispose()
            {
            }

            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }
}
