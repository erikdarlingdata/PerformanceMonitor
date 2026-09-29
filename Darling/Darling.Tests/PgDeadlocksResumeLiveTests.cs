// #1776 own-store: this class never touches the shared store. It reads a separate TARGET server
// (started with logging_collector = on) and drives the collectors' own reads; the collector state is the
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
/// #4699: <c>pg_deadlocks</c> and <c>pg_plan_capture</c> against a REAL target with <c>logging_collector = on</c>,
/// stderr logging, a UTC <c>log_timezone</c>, <c>log_line_prefix = '%m [%p] '</c> and a short <c>deadlock_timeout</c>.
/// Set <c>DARLING_TEST_PG_LOGROTATE</c> to that target's connection string. A log rotation between two reads must
/// not lose a report written before it, and the marker one cycle stages is the state the next cycle starts from.
/// </summary>
[Collection("pg-log-rotation")]
public sealed class PgDeadlocksResumeLiveTests
{
    private static string? Target => Environment.GetEnvironmentVariable("DARLING_TEST_PG_LOGROTATE");

    private const string SkipReason = "Set DARLING_TEST_PG_LOGROTATE to a target started with logging_collector = on, log_destination = 'stderr', log_timezone = 'UTC' to run the deadlock resume live tests.";

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

    private static async Task<(List<PgDeadlocksCollector.Row> Rows, CollectorContext Context)> CycleAsync(
        NpgsqlConnection connection, IReadOnlyDictionary<string, string>? state, bool binary, CancellationToken ct)
    {
        var context = NewContext(state, binary);
        await using var command = LiveTailQuery.Command(PgDeadlocksCollector.Instance.BuildQuery(context), connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = await PgDeadlocksCollector.Instance.ReadAsync(reader, context, ct);
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

    /// <summary>Two sessions lock two tables in opposite order; PostgreSQL cancels one and logs the report.</summary>
    private static async Task MakeDeadlockAsync(NpgsqlConnection admin, CancellationToken ct)
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var a = "pm4699a" + suffix;
        var b = "pm4699b" + suffix;
        await ExecAsync(admin, $"CREATE TABLE {a} (id int PRIMARY KEY); CREATE TABLE {b} (id int PRIMARY KEY); INSERT INTO {a} VALUES (1); INSERT INTO {b} VALUES (1);", ct);
        await using var one = await OpenAsync(ct);
        await using var two = await OpenAsync(ct);
        await ExecAsync(one, $"BEGIN; UPDATE {a} SET id = 1;", ct);
        await ExecAsync(two, $"BEGIN; UPDATE {b} SET id = 1;", ct);
        var first = ExecAsync(one, $"UPDATE {b} SET id = 1", ct);
        await Task.Delay(500, ct);
        var second = ExecAsync(two, $"UPDATE {a} SET id = 1", ct);
        foreach (var task in new[] { first, second })
        {
            try
            {
                await task;
            }
            catch (PostgresException)
            {
                /* one side is the deadlock victim */
            }
        }

        await ExecAsync(one, "ROLLBACK", ct);
        await ExecAsync(two, "ROLLBACK", ct);
        await ExecAsync(admin, $"DROP TABLE {a}; DROP TABLE {b}; SELECT pg_sleep(0.3);", ct);
    }

    private static async Task RotateAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, "SELECT pg_rotate_logfile(); SELECT pg_sleep(1.2);", ct);
        await ExecAsync(connection, "DO $$ BEGIN RAISE WARNING 'pm4699 new file line'; END $$;", ct);
        await ExecAsync(connection, "SELECT pg_sleep(0.3);", ct);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ADeadlockReportWrittenBeforeARotation_IsCollectedWhenTheNextCycleCarriesTheMarker(bool binary)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Target), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(ct);

        var first = await CycleAsync(connection, null, binary, ct);
        Assert.True(first.Context.PendingState.ContainsKey(PgServerLogTail.ResumeStateKey), "the first read stages a marker");

        await MakeDeadlockAsync(connection, ct);
        await RotateAsync(connection, ct);

        var withState = await CycleAsync(connection, new Dictionary<string, string>(first.Context.PendingState), binary, ct);
        Assert.Single(withState.Rows);

        /* No state is the read the collector made before the marker: the newest file only, which does not hold the report. */
        var noState = await CycleAsync(connection, null, binary, ct);
        Assert.Empty(noState.Rows);
    }
}
