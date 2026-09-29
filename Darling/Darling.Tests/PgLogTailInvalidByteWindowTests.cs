// #4735: a byte that is invalid in the database encoding, anywhere in the 4 MB window, is refused by
// PostgreSQL (22021) on every read of that window, so the retry from a later start (which cures a start inside
// a character) can never cure it. Once a cycle has used every shift and still failed, the next cycles make one
// read each, until a read succeeds.
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

public sealed class PgLogTailInvalidByteWindowTests
{
    private static readonly CollectorRunResult Success = new(0, 0, 0, CollectorContext.NoMeasurements);

    private static PostgresException Refusal(string sqlState = "22021")
        => new("invalid byte sequence for encoding \"UTF8\": 0xe3 0x81", "ERROR", "ERROR", sqlState);

    /// <summary>A runner over a data source nothing opens: the retry loop reaches the target only through the read
    /// the test hands it.</summary>
    private static (DarlingCollectorRunner Runner, NpgsqlDataSource Source) NewRunner()
    {
        var source = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=unused;Database=unused");
        return (new DarlingCollectorRunner(source, new CollectorDeltaCalculator()), source);
    }

    /// <summary>A read that counts its calls, records the shift of each, and answers from a script: an exception to
    /// throw, or null for a successful read.</summary>
    private sealed class ScriptedRead
    {
        private readonly Func<int, Exception?> _answer;

        public ScriptedRead(Func<int, Exception?> answer) => _answer = answer;

        public List<int> Shifts { get; } = [];

        public Task<CollectorRunResult> RunAsync(int shift, CancellationToken cancellationToken)
        {
            Shifts.Add(shift);
            var failure = _answer(shift);
            return failure is null ? Task.FromResult(Success) : Task.FromException<CollectorRunResult>(failure);
        }
    }

    private static async Task<Exception?> CycleAsync(
        DarlingCollectorRunner runner, string collector, ServerRuntime server, ScriptedRead read)
    {
        try
        {
            await runner.RunWithSplitCharacterRetryAsync(collector, server, read.RunAsync, TestContext.Current.CancellationToken);
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    [Fact]
    public async Task AfterACycleThatUsedEveryShift_TheNextCycleMakesExactlyOneRead()
    {
        var (runner, source) = NewRunner();
        await using var _ = source;
        var server = PgReadBinaryFileCapabilityTests.Runtime("invalid-byte-a");

        var first = new ScriptedRead(_ => Refusal());
        var firstFault = await CycleAsync(runner, "pg_log_events", server, first);
        Assert.IsType<PostgresException>(firstFault);
        Assert.Equal([0, 1, 2, 3], first.Shifts);

        /* The byte is still in the window: one read, at shift 0, no retries, and the refusal still reaches the
           handler that records the error row. */
        var second = new ScriptedRead(_ => Refusal());
        var secondFault = await CycleAsync(runner, "pg_log_events", server, second);
        Assert.Equal("22021", Assert.IsType<PostgresException>(secondFault).SqlState);
        Assert.Equal([0], second.Shifts);

        var third = new ScriptedRead(_ => Refusal());
        await CycleAsync(runner, "pg_log_events", server, third);
        Assert.Equal([0], third.Shifts);
    }

    [Fact]
    public async Task AfterASuccessfulRead_TheNext22021IsRetriedAgain()
    {
        var (runner, source) = NewRunner();
        await using var _ = source;
        var server = PgReadBinaryFileCapabilityTests.Runtime("invalid-byte-b");

        await CycleAsync(runner, "pg_log_events", server, new ScriptedRead(_ => Refusal()));

        /* The byte left the window: the one read succeeds and the memory is cleared. */
        var recovered = new ScriptedRead(_ => null);
        Assert.Null(await CycleAsync(runner, "pg_log_events", server, recovered));
        Assert.Equal([0], recovered.Shifts);

        /* A later 22021 is a new event: the full retry ladder again, and only then the memory. */
        var later = new ScriptedRead(_ => Refusal());
        await CycleAsync(runner, "pg_log_events", server, later);
        Assert.Equal([0, 1, 2, 3], later.Shifts);

        var after = new ScriptedRead(_ => Refusal());
        await CycleAsync(runner, "pg_log_events", server, after);
        Assert.Equal([0], after.Shifts);
    }

    [Fact]
    public async Task AReadThatStartsInsideACharacter_IsRetriedEveryTimeItHappensAndLeavesNothingBehind()
    {
        var (runner, source) = NewRunner();
        await using var _ = source;
        var server = PgReadBinaryFileCapabilityTests.Runtime("invalid-byte-c");

        /* Refused at shifts 0 and 1, read at shift 2: a start inside a three-byte character. */
        var split = new ScriptedRead(shift => shift < 2 ? Refusal() : null);
        Assert.Null(await CycleAsync(runner, "pg_log_events", server, split));
        Assert.Equal([0, 1, 2], split.Shifts);

        /* Not every shift was used, so nothing is remembered: the same thing happening again is retried again. */
        var again = new ScriptedRead(shift => shift < 1 ? Refusal() : null);
        Assert.Null(await CycleAsync(runner, "pg_log_events", server, again));
        Assert.Equal([0, 1], again.Shifts);

        var full = new ScriptedRead(_ => Refusal());
        await CycleAsync(runner, "pg_log_events", server, full);
        Assert.Equal([0, 1, 2, 3], full.Shifts);
    }

    [Fact]
    public async Task TheMemoryBelongsToOneServerAndOneCollector()
    {
        var (runner, source) = NewRunner();
        await using var _ = source;
        var serverA = PgReadBinaryFileCapabilityTests.Runtime("invalid-byte-d1");
        var serverB = PgReadBinaryFileCapabilityTests.Runtime("invalid-byte-d2");

        await CycleAsync(runner, "pg_log_events", serverA, new ScriptedRead(_ => Refusal()));

        var otherServer = new ScriptedRead(_ => Refusal());
        await CycleAsync(runner, "pg_log_events", serverB, otherServer);
        Assert.Equal([0, 1, 2, 3], otherServer.Shifts);

        var otherCollector = new ScriptedRead(_ => Refusal());
        await CycleAsync(runner, "pg_deadlocks", serverA, otherCollector);
        Assert.Equal([0, 1, 2, 3], otherCollector.Shifts);

        var sameOne = new ScriptedRead(_ => Refusal());
        await CycleAsync(runner, "pg_log_events", serverA, sameOne);
        Assert.Equal([0], sameOne.Shifts);

        /* A fresh runner (a restart) knows nothing, the safe direction. */
        var (restarted, restartedSource) = NewRunner();
        await using var _2 = restartedSource;
        var afterRestart = new ScriptedRead(_ => Refusal());
        await CycleAsync(restarted, "pg_log_events", serverA, afterRestart);
        Assert.Equal([0, 1, 2, 3], afterRestart.Shifts);
    }

    [Fact]
    public async Task AFailureThatIsNotTheEncodingRefusal_NeitherSetsNorClearsIt()
    {
        var (runner, source) = NewRunner();
        await using var _ = source;
        var server = PgReadBinaryFileCapabilityTests.Runtime("invalid-byte-e");

        /* A connection fault on a cycle that never reached the byte does not use the ladder or set anything. */
        var fault = new ScriptedRead(_ => Refusal("08006"));
        await CycleAsync(runner, "pg_log_events", server, fault);
        Assert.Equal([0], fault.Shifts);

        var ladder = new ScriptedRead(_ => Refusal());
        await CycleAsync(runner, "pg_log_events", server, ladder);
        Assert.Equal([0, 1, 2, 3], ladder.Shifts);

        /* ...and once it is set, a fault that is not the refusal does not clear it: only a read that succeeds does. */
        await CycleAsync(runner, "pg_log_events", server, new ScriptedRead(_ => Refusal("08006")));
        var stillOne = new ScriptedRead(_ => Refusal());
        await CycleAsync(runner, "pg_log_events", server, stillOne);
        Assert.Equal([0], stillOne.Shifts);
    }

    [Fact]
    public async Task ACollectorThatDoesNotReadTheLogTail_IsNeverRetriedAndNeverRemembered()
    {
        var (runner, source) = NewRunner();
        await using var _ = source;
        var server = PgReadBinaryFileCapabilityTests.Runtime("invalid-byte-f");

        var other = new ScriptedRead(_ => Refusal());
        await CycleAsync(runner, "pg_stat_statements", server, other);
        Assert.Equal([0], other.Shifts);
        await CycleAsync(runner, "pg_stat_statements", server, new ScriptedRead(_ => Refusal()));

        /* The log-tail collector on the same server still gets its full ladder. */
        var tail = new ScriptedRead(_ => Refusal());
        await CycleAsync(runner, "pg_log_events", server, tail);
        Assert.Equal([0, 1, 2, 3], tail.Shifts);
    }

    /// <summary>The recorded error says the retries happened and that later cycles read once, so an operator reading a
    /// cycle that made a single read is not told it made four.</summary>
    [Fact]
    public void TheRecordedErrorSaysLaterCyclesReadOncePerCycleWhileTheRefusalLasts()
    {
        var explanation = DarlingWorker.LogTailUndecodableByteExplanation(
            Refusal(), "pg_log_events", PgReadBinaryFileCapabilityTests.Runtime("invalid-byte-g", connectedDatabase: "appdb"));

        Assert.NotNull(explanation);
        Assert.Contains("1, 2 and 3 bytes later", explanation, StringComparison.Ordinal);
        Assert.Contains("once per cycle", explanation, StringComparison.Ordinal);
        Assert.DoesNotContain("in this cycle and every attempt", explanation, StringComparison.Ordinal);
    }
}
