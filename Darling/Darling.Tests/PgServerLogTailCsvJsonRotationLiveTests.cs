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

    private static string Marker() => "pm4699t" + Guid.NewGuid().ToString("N")[..10];

    private static async Task<int> LogAsync(NpgsqlConnection connection, bool json, string marker, CancellationToken ct)
    {
        await using var holder = await OpenAsync(json, ct);
        await using var waiter = await OpenAsync(json, ct);
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

        var pid = await LogAsync(connection, json, Marker(), ct);
        await RotateAsync(connection, ct);

        var withState = await CycleAsync(connection, Carry(first.Context), binary, json, ct);
        Assert.Equal(1, Count(withState.Rows, pid));

        /* No state is today's read: the newest file only, which does not hold the line. */
        var noState = await CycleAsync(connection, null, binary, json, ct);
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
