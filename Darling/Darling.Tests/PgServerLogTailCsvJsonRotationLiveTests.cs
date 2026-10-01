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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4699: the csvlog and jsonlog log tails' resume markers against a REAL target with
/// <c>logging_collector = on</c>, a UTC <c>log_timezone</c>, <c>log_lock_waits = on</c> and a short
/// <c>deadlock_timeout</c>. Set <c>DARLING_TEST_PG_LOGROTATE_CSV</c> to a target whose <c>log_destination</c> is
/// <c>csvlog</c>, and <c>DARLING_TEST_PG_LOGROTATE_JSON</c> to one whose is <c>jsonlog</c> (a superuser or
/// pg_monitor plus pg_read_server_files). Each test runs the shipped query through the collector's own read and
/// carries the staged state into the next cycle the way the runner does after a successful write.
/// </summary>
[Collection("pg-log-rotation")]
public sealed class PgServerLogTailCsvJsonRotationLiveTests
{
    private const string SkipReason = "Set DARLING_TEST_PG_LOGROTATE_CSV / DARLING_TEST_PG_LOGROTATE_JSON to a target started with logging_collector = on, log_destination = 'csvlog' / 'jsonlog', log_timezone = 'UTC' to run the csv/json log-rotation live tests.";

    private static string? TargetFor(bool json) =>
        Environment.GetEnvironmentVariable(json ? "DARLING_TEST_PG_LOGROTATE_JSON" : "DARLING_TEST_PG_LOGROTATE_CSV");

    private static CollectorContext NewContext(IReadOnlyDictionary<string, string>? state, bool binary, bool json) => new()
    {
        ServerId = 1,
        ServerName = "live-rig-as-target",
        CollectionTime = DateTime.UtcNow,
        Deltas = new CollectorDeltaCalculator(),
        LogHashKey = TestLogHashKeys.Fixed,
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 18, PostgresVersionNum = 180000 },
        PgReadBinaryFileGranted = binary,
        PgLogUsesCsvlog = !json,
        PgLogUsesJsonlog = json,
        State = state ?? CollectorContext.NoState,
    };

    private static async Task<(List<PgLogEvent> Rows, CollectorContext Context)> CycleAsync(
        NpgsqlConnection connection, IReadOnlyDictionary<string, string>? state, bool binary, bool json, CancellationToken ct)
    {
        var context = NewContext(state, binary, json);
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

    private static async Task<NpgsqlConnection> OpenAsync(bool json, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(TargetFor(json));
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

    /// <summary>
    /// The evidence for a failed count check (<see cref="PgLogRotationEvidence"/>), with the target's log directory read
    /// now. Called only when a check has already failed, so a passing run never pays for it.
    /// </summary>
    private static async Task<string> DescribeAsync(
        NpgsqlConnection connection, string what, IEnumerable<PgLogEvent> rows, LoggedWait wait,
        IReadOnlyDictionary<string, string>? carriedState, IReadOnlyDictionary<string, string>? newState, CancellationToken ct)
    {
        string listing;
        try
        {
            var lines = new List<string>();
            await using var command = new NpgsqlCommand("SELECT name, size, modification FROM pg_ls_logdir() ORDER BY modification, name", connection);
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                lines.Add("  " + reader.GetString(0) + "  " + reader.GetInt64(1) + "  " + reader.GetDateTime(2).ToString("O", System.Globalization.CultureInfo.InvariantCulture));
            }

            listing = string.Join("\n", lines);
        }
        catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException)
        {
            listing = "  (the log directory could not be read: " + ex.Message + ")";
        }

        return PgLogRotationEvidence.Describe(what, rows, wait.Pid, wait.FloorUtc, r => IsTheWait(r, wait), carriedState, newState, listing);
    }

    private static string Marker() => "pm4699t" + Guid.NewGuid().ToString("N")[..10];

    /// <summary>
    /// One lock wait <see cref="LogAsync"/> provoked: the waiter backend's pid, and the target's own clock read just
    /// before the wait began. A row read back is this wait's entry only when it carries the pid and is no earlier than
    /// that clock (<see cref="IsTheWait"/>).
    /// </summary>
    private readonly record struct LoggedWait(int Pid, DateTime FloorUtc);

    /// <summary>
    /// Provokes one lock-wait log entry (log_lock_waits, a short deadlock_timeout) against a table named
    /// <paramref name="marker"/>; the returned wait identifies the entry in the rows read back.
    /// </summary>
    private static async Task<LoggedWait> LogAsync(NpgsqlConnection connection, bool json, string marker, CancellationToken ct, bool pokeTheWaiter = false)
    {
        await using var holder = await OpenAsync(json, ct);
        await using var waiter = await OpenAsync(json, ct);
        await using var poker = pokeTheWaiter ? await OpenAsync(json, ct) : null;
        /* The floor is the target's own clock, read before any of this wait exists. Its entry is logged at least
           deadlock_timeout (100 ms) after that, and every earlier wait had finished before it. */
        await using var clockCommand = new NpgsqlCommand("SELECT clock_timestamp()", connection);
        var floor = (DateTime)(await clockCommand.ExecuteScalarAsync(ct))!;
        await ExecAsync(holder, "CREATE TABLE " + marker + " (id int PRIMARY KEY)", ct);
        await ExecAsync(holder, "INSERT INTO " + marker + " VALUES (1)", ct);
        await ExecAsync(holder, "BEGIN", ct);
        await ExecAsync(holder, "UPDATE " + marker + " SET id = 1", ct);
        await using var pidCommand = new NpgsqlCommand("SELECT pg_backend_pid()", waiter);
        var pid = (int)(await pidCommand.ExecuteScalarAsync(ct))!;
        var blocked = ExecAsync(waiter, "SET lock_timeout = '" + (pokeTheWaiter ? "3000ms" : "800ms") + "'; UPDATE " + marker + " SET id = 1", ct);
        /* PostgreSQL logs "still waiting" again on every latch wakeup after the first deadlock check, so a wake-up
           between deadlock_timeout (100 ms) and lock_timeout (800 ms; 3000 ms when poked, so the poke always lands inside the wait) gives this one wait a second line. */
        var poke = pokeTheWaiter ? PokeAsync(poker!, pid, ct) : Task.CompletedTask;
        try
        {
            await blocked;
        }
        catch (PostgresException)
        {
            /* lock_timeout after the wait was logged */
        }

        await poke;
        await ExecAsync(holder, "ROLLBACK", ct);
        await ExecAsync(holder, "DROP TABLE " + marker, ct);
        await ExecAsync(connection, "SELECT pg_sleep(0.3);", ct);
        return new LoggedWait(pid, floor);
    }

    /// <summary>Wakes the waiter's latch about 350 ms into its wait, from a third connection.</summary>
    private static async Task PokeAsync(NpgsqlConnection third, int waiterPid, CancellationToken ct)
    {
        await Task.Delay(350, ct);
        await using var command = new NpgsqlCommand("SELECT pg_log_backend_memory_contexts(" + waiterPid.ToString(System.Globalization.CultureInfo.InvariantCulture) + ")", third);
        await command.ExecuteScalarAsync(ct);
    }

    /// <summary>
    /// The messages of the wait's "still waiting" entries, from ONE read of each of the route's log files from byte 0,
    /// parsed with the product's own parser. A wait can log more than one such line (each with its own "after X ms"),
    /// so the set, not a count of one, is what the tail must return exactly.
    /// </summary>
    private static async Task<List<string>> ExpectedWaitLinesAsync(NpgsqlConnection connection, bool json, LoggedWait wait, CancellationToken ct)
    {
        var names = new List<string>();
        await using (var list = new NpgsqlCommand("SELECT name FROM pg_ls_logdir() WHERE name LIKE @pattern ORDER BY name", connection))
        {
            list.Parameters.AddWithValue("pattern", json ? "%.json" : "%.csv");
            await using var reader = await list.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                names.Add(reader.GetString(0));
            }
        }

        var prefix = $"process {wait.Pid} still waiting for ";
        var messages = new List<string>();
        foreach (var name in names)
        {
            string body;
            await using (var read = new NpgsqlCommand("SELECT pg_read_file(current_setting('log_directory') || '/' || @name)", connection))
            {
                read.Parameters.AddWithValue("name", name);
                body = (string)(await read.ExecuteScalarAsync(ct))!;
            }

            var entries = json ? PgServerLogJsonParser.Parse(body, out _) : PgServerLogCsvParser.Parse(body, out _);
            messages.AddRange(entries
                .Where(e => e.Pid == wait.Pid && e.OccurredAtUtc >= wait.FloorUtc && e.Message.StartsWith(prefix, StringComparison.Ordinal))
                .Select(e => e.Message));
        }

        return messages;
    }

    /// <summary>
    /// Asserts the rotation outcome for one wait: the resumed read holds exactly the expected "still waiting" messages
    /// (each once, each with its own hash), the read without state holds none, and across both cycles each message is one identity.
    /// </summary>
    private static async Task AssertExactWaitAsync(
        NpgsqlConnection connection, bool json, LoggedWait wait, List<string> expected,
        (List<PgLogEvent> Rows, CollectorContext Context) first, (List<PgLogEvent> Rows, CollectorContext Context) withState,
        IReadOnlyDictionary<string, string> carried, bool binary, CancellationToken ct)
    {
        var expectedText = "expected from the files: [" + string.Join(" | ", expected) + "]\n";
        Assert.True(expected.Count > 0, "the log files hold no entry for the wait\n" + expectedText);
        Assert.True(expected.Distinct().Count() == expected.Count, "the log files repeat a wait message\n" + expectedText);

        var resumed = withState.Rows.Where(r => IsTheWait(r, wait)).ToList();
        Assert.True(
            resumed.Select(r => r.Message).OrderBy(m => m, StringComparer.Ordinal).SequenceEqual(expected.OrderBy(m => m, StringComparer.Ordinal))
                && resumed.Select(r => r.RawLineHash).Distinct().Count() == resumed.Count,
            $"the resumed read should hold exactly the wait's {expected.Count} expected line(s), each once, got {resumed.Count}\n" + expectedText
            + await DescribeAsync(connection, "resumed read after the rotation", withState.Rows, wait, carried, withState.Context.PendingState, ct));

        /* No state is today's read: the newest file only, which does not hold the lines. */
        var noState = await CycleAsync(connection, null, binary, json, ct);
        var noStateCount = Count(noState.Rows, wait);
        Assert.True(
            noStateCount == 0,
            $"the read without state should not hold the wait's entry, got {noStateCount}\n"
            + await DescribeAsync(connection, "read without state", noState.Rows, wait, null, noState.Context.PendingState, ct));

        /* Within the cycle no raw_line_hash repeats. */
        var distinctHashes = withState.Rows.Select(r => r.RawLineHash).Distinct().Count();
        Assert.True(
            withState.Rows.Count == distinctHashes,
            $"a raw_line_hash repeats within the resumed read: {withState.Rows.Count} rows, {distinctHashes} distinct hashes\n"
            + await DescribeAsync(connection, "resumed read after the rotation", withState.Rows, wait, carried, withState.Context.PendingState, ct));

        /* Across the two cycles each expected message is one identity. */
        var both = first.Rows.Concat(withState.Rows).Where(r => IsTheWait(r, wait)).ToList();
        foreach (var message in expected)
        {
            var hashes = both.Where(r => r.Message == message).Select(r => r.RawLineHash).Distinct().Count();
            Assert.True(
                hashes == 1,
                $"the wait's entry \"{message}\" should have one identity across both reads, got {hashes}\n"
                + await DescribeAsync(connection, "both reads together", both, wait, carried, withState.Context.PendingState, ct));
        }
    }

    /* A pid alone does not identify a wait's entry. The holder and waiter connections come from Npgsql's pool, so a
       later wait often runs on an earlier wait's backend (the same pid), and the resumed read re-reads the previous
       read on purpose (a 1 MiB overlap), so the earlier wait's entry is in the same rows. The floor tells them apart. */
    private static bool IsTheWait(PgLogEvent row, LoggedWait wait) =>
        row.Pid == wait.Pid && row.OccurredAtUtc >= wait.FloorUtc && row.Message.Contains("still waiting", StringComparison.Ordinal);

    private static int Count(IEnumerable<PgLogEvent> rows, LoggedWait wait) =>
        rows.Count(r => IsTheWait(r, wait));

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task ARotationBetweenReads_DoesNotLoseTheLinesWrittenBeforeIt(bool json, bool binary)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(TargetFor(json)), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(json, ct);
        var key = json ? PgServerLogTail.ResumeStateKeyJson : PgServerLogTail.ResumeStateKeyCsv;

        _ = await LogAsync(connection, json, Marker(), ct);
        var first = await CycleAsync(connection, null, binary, json, ct);
        Assert.True(first.Context.PendingState.ContainsKey(key), "the first read stages a marker under the format's own key");
        Assert.False(first.Context.PendingState.ContainsKey(PgServerLogTail.ResumeStateKey), "and not under the stderr key");

        var wait = await LogAsync(connection, json, Marker(), ct);
        await RotateAsync(connection, ct);

        var carried = Carry(first.Context);
        var withState = await CycleAsync(connection, carried, binary, json, ct);
        var expected = await ExpectedWaitLinesAsync(connection, json, wait, ct);
        await AssertExactWaitAsync(connection, json, wait, expected, first, withState, carried, binary, ct);
    }

    [Theory]
    [InlineData(false, true)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(true, false)]
    public async Task ALockWaitThatLogsTwice_IsReadAsTwoLinesEachOnce(bool json, bool binary)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(TargetFor(json)), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(json, ct);

        _ = await LogAsync(connection, json, Marker(), ct);
        var first = await CycleAsync(connection, null, binary, json, ct);

        var wait = await LogAsync(connection, json, Marker(), ct, pokeTheWaiter: true);
        await RotateAsync(connection, ct);

        var carried = Carry(first.Context);
        var withState = await CycleAsync(connection, carried, binary, json, ct);
        var expected = await ExpectedWaitLinesAsync(connection, json, wait, ct);
        Assert.True(expected.Count == 2, $"the poke should make one wait log two lines, the files hold {expected.Count}: [{string.Join(" | ", expected)}]");
        await AssertExactWaitAsync(connection, json, wait, expected, first, withState, carried, binary, ct);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AStderrMarker_OnACsvOrJsonRoute_IsNotAMissingFile(bool json)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(TargetFor(json)), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(json, ct);
        _ = await LogAsync(connection, json, Marker(), ct);

        var state = new Dictionary<string, string> { [PgServerLogTail.ResumeStateKey] = "123|postgresql-1999-01-01_000000.log" };
        var cycle = await CycleAsync(connection, state, false, json, ct);

        Assert.NotEmpty(cycle.Rows);
        Assert.DoesNotContain(cycle.Context.Measurements, m => m.Label == PgServerLogTail.ResumeFileMissingMeasurement);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AMissingMarkedFile_OnTheFormatsOwnKey_IsDisclosed(bool json)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(TargetFor(json)), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(json, ct);
        _ = await LogAsync(connection, json, Marker(), ct);

        var key = json ? PgServerLogTail.ResumeStateKeyJson : PgServerLogTail.ResumeStateKeyCsv;
        var state = new Dictionary<string, string> { [key] = "123|postgresql-1999-01-01_000000." + (json ? "json" : "csv") };
        var cycle = await CycleAsync(connection, state, false, json, ct);

        Assert.NotEmpty(cycle.Rows);
        Assert.Contains(cycle.Context.Measurements, m => m.Label == PgServerLogTail.ResumeFileMissingMeasurement && m.Value == 1);
    }
}
