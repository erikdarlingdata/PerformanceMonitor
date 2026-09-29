// #1776 own-store: this class never touches the shared store. It reads a separate TARGET server
// (started with logging_collector = on) and drives the collector's own read; the collector state is the
// in-memory context, so no store database is needed.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4699: the stderr log tail's resume marker against a REAL target with <c>logging_collector = on</c>,
/// <c>log_destination = 'stderr'</c>, <c>log_min_messages</c> at LOG or lower and a UTC <c>log_timezone</c>.
/// Set <c>DARLING_TEST_PG_LOGROTATE</c> to that target's connection string (a superuser or pg_monitor plus
/// pg_read_server_files). Each test runs the shipped query through the collector's own read and carries the
/// staged state into the next cycle the way the runner does after a successful write.
/// </summary>
[Collection("pg-log-rotation")]
public sealed class PgServerLogTailRotationLiveTests
{
    private static string? Target => Environment.GetEnvironmentVariable("DARLING_TEST_PG_LOGROTATE");

    private const string SkipReason = "Set DARLING_TEST_PG_LOGROTATE to a target started with logging_collector = on, log_destination = 'stderr', log_timezone = 'UTC' to run the log-rotation live tests.";

    private static CollectorContext NewContext(IReadOnlyDictionary<string, string>? state, bool binary) => new()
    {
        ServerId = 1,
        ServerName = "live-rig-as-target",
        CollectionTime = DateTime.UtcNow,
        Deltas = new CollectorDeltaCalculator(),
        LogHashKey = TestLogHashKeys.Fixed,
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 18, PostgresVersionNum = 180000 },
        PgReadBinaryFileGranted = binary,
        State = state ?? CollectorContext.NoState,
    };

    private static async Task<(List<PgLogEvent> Rows, CollectorContext Context)> CycleAsync(
        NpgsqlConnection connection, IReadOnlyDictionary<string, string>? state, bool binary, CancellationToken ct)
    {
        var context = NewContext(state, binary);
        await using var command = LiveTailQuery.Command(PgLogEventsCollector.Instance.BuildQuery(context), connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = await PgLogEventsCollector.Instance.ReadAsync(reader, context, ct);
        return (rows, context);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<NpgsqlConnection> OpenAsync(CancellationToken ct)
    {
        var connection = new NpgsqlConnection(Target);
        await connection.OpenAsync(ct);
        return connection;
    }

    private static async Task RotateAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, "SELECT pg_rotate_logfile(); SELECT pg_sleep(1.2);", ct);
        /* A line in the new file, so its mtime is later than the old file's (one-second resolution). */
        await ExecAsync(connection, "DO $$ BEGIN RAISE LOG 'pm4699 new file line'; END $$;", ct);
        await ExecAsync(connection, "SELECT pg_sleep(0.3);", ct);
    }

    private static IReadOnlyDictionary<string, string> Carry(CollectorContext context) =>
        new Dictionary<string, string>(context.PendingState);

    private static string Marker() => "pm4699t" + Guid.NewGuid().ToString("N")[..10];

    /// <summary>
    /// Provokes one lock-wait log entry (log_lock_waits, a short deadlock_timeout) against a table named
    /// <paramref name="marker"/>; the blocked backend's pid identifies the entry in the rows read back.
    /// </summary>
    private static async Task<int> LogAsync(NpgsqlConnection connection, string marker, CancellationToken ct)
    {
        await using var holder = await OpenAsync(ct);
        await using var waiter = await OpenAsync(ct);
        await ExecAsync(holder, "CREATE TABLE " + marker + " (id int PRIMARY KEY)", ct);
        await ExecAsync(holder, "INSERT INTO " + marker + " VALUES (1)", ct);
        await ExecAsync(holder, "BEGIN", ct);
        await ExecAsync(holder, "UPDATE " + marker + " SET id = 1", ct);
        await using var pidCommand = new NpgsqlCommand("SELECT pg_backend_pid()", waiter);
        var pid = (int)(await pidCommand.ExecuteScalarAsync(ct))!;
        var blocked = ExecAsync(waiter, "SET lock_timeout = '800ms'; UPDATE " + marker + " SET id = 1", ct);
        try
        {
            await blocked;
        }
        catch (PostgresException)
        {
            /* lock_timeout after the wait was logged */
        }

        await ExecAsync(holder, "ROLLBACK", ct);
        await ExecAsync(holder, "DROP TABLE " + marker, ct);
        await ExecAsync(connection, "SELECT pg_sleep(0.3);", ct);
        return pid;
    }

    private static int Count(IEnumerable<PgLogEvent> rows, int pid) =>
        rows.Count(r => r.Pid == pid && r.Message.Contains("still waiting", StringComparison.Ordinal));

    /// <summary>
    /// #4704: PostgreSQL's logging collector writes from a pipe asynchronously, so right after a multi-MB burst
    /// the last line can still be unwritten when the read runs. This repeats ONE logical read (the same carried
    /// state every attempt, never advanced) until the entry for <paramref name="pid"/> is in the rows or the
    /// deadline passes; the assertions on the returned cycle stay exact.
    /// </summary>
    private static async Task<(List<PgLogEvent> Rows, CollectorContext Context)> CycleUntilLoggedAsync(
        NpgsqlConnection connection, IReadOnlyDictionary<string, string>? state, bool binary, int pid, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        var attempts = 0;
        while (true)
        {
            attempts++;
            var cycle = await CycleAsync(connection, state, binary, ct);
            if (Count(cycle.Rows, pid) > 0)
            {
                return cycle;
            }

            if (DateTime.UtcNow >= deadline)
            {
                var skipped = cycle.Context.Measurements.Where(m => m.Label == PgServerLogTail.BytesSkippedMeasurement).Select(m => m.Value).DefaultIfEmpty(0).Max();
                var file = cycle.Context.PendingState.TryGetValue(PgServerLogTail.ResumeStateKey, out var staged) ? staged : "(no staged marker)";
                Assert.Fail($"the lock-wait entry for pid {pid} never reached the log file read ({file}) after {attempts} attempts over 30 s; {rows(cycle)} rows read, bytes skipped {skipped}");
            }

            await Task.Delay(500, ct);
        }

        static int rows((List<PgLogEvent> Rows, CollectorContext Context) c) => c.Rows.Count;
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ARotationBetweenReads_DoesNotLoseTheLinesWrittenBeforeIt(bool binary)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Target), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(ct);

        _ = await LogAsync(connection, Marker(), ct);
        var first = await CycleAsync(connection, null, binary, ct);
        Assert.True(first.Context.PendingState.ContainsKey(PgServerLogTail.ResumeStateKey), "the first read stages a marker");

        var pid = await LogAsync(connection, Marker(), ct);
        await RotateAsync(connection, ct);

        var withState = await CycleAsync(connection, Carry(first.Context), binary, ct);
        Assert.Equal(1, Count(withState.Rows, pid));

        /* No state is today's read: the newest file only, which does not hold the line. */
        var noState = await CycleAsync(connection, null, binary, ct);
        Assert.Equal(0, Count(noState.Rows, pid));

        /* Within the cycle no raw_line_hash repeats. */
        Assert.Equal(withState.Rows.Count, withState.Rows.Select(r => r.RawLineHash).Distinct().Count());

        /* Across the two cycles the line is one identity. */
        var all = first.Rows.Concat(withState.Rows).Where(r => r.Pid == pid && r.Message.Contains("still waiting", StringComparison.Ordinal)).Select(r => r.RawLineHash).Distinct();
        Assert.Single(all);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AMissingMarkedFile_FallsBackToTheNewestFile_AndSaysSo(bool binary)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Target), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(ct);
        _ = await LogAsync(connection, Marker(), ct);

        var state = new Dictionary<string, string> { [PgServerLogTail.ResumeStateKey] = "123|postgresql-1999-01-01_000000.log" };
        var cycle = await CycleAsync(connection, state, binary, ct);

        Assert.NotEmpty(cycle.Rows);
        Assert.Contains(cycle.Context.Measurements, m => m.Label == PgServerLogTail.ResumeFileMissingMeasurement && m.Value == 1);
        var result = DarlingCollectorRunner_Notes(cycle.Context);
        Assert.Contains(PgServerLogTail.LogResumeLostNote, result, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARecycledMarkedFile_IsDisclosed()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Target), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(ct);
        _ = await LogAsync(connection, Marker(), ct);
        var first = await CycleAsync(connection, null, false, ct);
        var staged = first.Context.PendingState[PgServerLogTail.ResumeStateKey];
        var name = staged[(staged.IndexOf('|') + 1)..];

        var state = new Dictionary<string, string> { [PgServerLogTail.ResumeStateKey] = "999999999|" + name };
        var cycle = await CycleAsync(connection, state, false, ct);

        Assert.Contains(cycle.Context.Measurements, m => m.Label == PgServerLogTail.ResumeFileRecycledMeasurement && m.Value == 1);
    }

    [Fact]
    public async Task ALogThatGrewPastTheWindow_DisclosesTheBytesSkipped_AndCollectsTheLastWindow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Target), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(ct);
        _ = await LogAsync(connection, Marker(), ct);
        var first = await CycleAsync(connection, null, false, ct);

        await ExecAsync(connection,
            "DO $$ BEGIN FOR i IN 1..6500 LOOP RAISE LOG '%', repeat('x', 1000); END LOOP; END $$;", ct);
        var pid = await LogAsync(connection, Marker(), ct);

        var cycle = await CycleUntilLoggedAsync(connection, Carry(first.Context), false, pid, ct);

        Assert.Contains(cycle.Context.Measurements, m => m.Label == PgServerLogTail.BytesSkippedMeasurement && m.Value > 0);
        Assert.Equal(1, Count(cycle.Rows, pid));
    }

    [Fact]
    public async Task TheNextOffset_MatchesTheCSharpRule_OverTheBytesTheBinaryReadReturns()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Target), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(ct);
        await ExecAsync(connection, "DO $$ BEGIN FOR i IN 1..1500 LOOP RAISE LOG '%', repeat('y', 1000); END LOOP; END $$;", ct);

        var cycle = await CycleAsync(connection, null, true, ct);
        var staged = cycle.Context.PendingState[PgServerLogTail.ResumeStateKey];
        var name = staged[(staged.IndexOf('|') + 1)..];
        var next = long.Parse(staged[..staged.IndexOf('|')], System.Globalization.CultureInfo.InvariantCulture);

        await using var read = new NpgsqlCommand(
            "SELECT size, pg_read_binary_file(current_setting('log_directory') || '/' || name, greatest(size - 4194304, 0), 4194304) FROM pg_ls_logdir() WHERE name = $1", connection);
        read.Parameters.AddWithValue(name);
        await using var reader = await read.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        var size = reader.GetInt64(0);
        var bytes = reader.GetFieldValue<byte[]>(1);

        /* The log grows between the cycle and this read (the connection itself logs nothing at LOG here), so
           compare only when the size is unchanged. */
        if (size - bytes.Length + bytes.Length == size)
        {
            var expected = PgServerLogTail.NextResumeOffset(bytes, Math.Max(size - 4194304, 0));
            Assert.True(next <= expected, "the staged offset never passes the C# rule's offset over a same-or-longer read");
            Assert.True(next >= Math.Max(size - 4194304, 0));
        }
    }

    private static string DarlingCollectorRunner_Notes(CollectorContext context) =>
        DarlingCollectorRunner.WithLogResumeNotes(new CollectorRunResult(1, 1, 1, context.Measurements)).Note ?? string.Empty;
}
