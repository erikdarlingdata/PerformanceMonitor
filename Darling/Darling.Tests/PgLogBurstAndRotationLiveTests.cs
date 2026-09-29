// #1776 own-store: this class never touches the shared store. It reads separate TARGET servers
// (started with logging_collector = on) and drives the collectors' own reads; the collector state is the
// in-memory context, so no store database is needed.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4699: the row cap and the rotation edge cases of the log resume marker, against REAL targets.
/// <c>DARLING_TEST_PG_LOGROTATE</c> is a stderr target (<c>log_line_prefix = '%m [%p] %Q '</c>) and
/// <c>DARLING_TEST_PG_LOGROTATE_CSV</c> a csvlog target; both have <c>logging_collector = on</c>, a UTC
/// <c>log_timezone</c>, <c>log_lock_waits = on</c> and <c>deadlock_timeout = 100ms</c>. The deadlock cap
/// (500 matches) is driven with real deadlocks, run in parallel by pairs of sessions; there is no test seam on the cap.
/// </summary>
[Collection("pg-log-rotation")]
public sealed class PgLogBurstAndRotationLiveTests
{
    private static string? StderrTarget => Environment.GetEnvironmentVariable("DARLING_TEST_PG_LOGROTATE");
    private static string? CsvTarget => Environment.GetEnvironmentVariable("DARLING_TEST_PG_LOGROTATE_CSV");

    private const string SkipReason = "Set DARLING_TEST_PG_LOGROTATE / DARLING_TEST_PG_LOGROTATE_CSV to targets started with logging_collector = on (stderr with a %Q prefix, and csvlog) to run the log burst and rotation live tests.";

    private static CollectorContext NewContext(IReadOnlyDictionary<string, string>? state, bool csv) => new()
    {
        ServerId = 1,
        ServerName = "live-rig-as-target",
        CollectionTime = DateTime.UtcNow,
        Deltas = new CollectorDeltaCalculator(),
        LogHashKey = TestLogHashKeys.Fixed,
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 18, PostgresVersionNum = 180000 },
        PgReadBinaryFileGranted = false,
        PgLogUsesCsvlog = csv,
        State = state ?? CollectorContext.NoState,
    };

    private static async Task<(List<T> Rows, CollectorContext Context)> CycleAsync<T>(
        CollectorDefinitionBase<T> definition, NpgsqlConnection connection, IReadOnlyDictionary<string, string>? state, bool csv, CancellationToken ct)
    {
        var context = NewContext(state, csv);
        await using var command = PostgresTargetProvider.Instance.CreateCommand(definition.BuildQuery(context), connection, 60);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = await definition.ReadAsync(reader, context, ct);
        return (rows, context);
    }

    /// <summary>
    /// #4704: the logging collector writes from a pipe asynchronously, so right after a large burst the tail can
    /// still be unwritten when the read runs. Repeats ONE logical read (the same carried state every attempt) until
    /// <paramref name="ready"/> holds or 30 s pass, then returns the last read; the caller's assertions stay exact.
    /// </summary>
    private static async Task<(List<T> Rows, CollectorContext Context)> CycleUntilAsync<T>(
        CollectorDefinitionBase<T> definition, NpgsqlConnection connection, IReadOnlyDictionary<string, string>? state, bool csv,
        Func<List<T>, bool> ready, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (true)
        {
            var cycle = await CycleAsync(definition, connection, state, csv, ct);
            if (ready(cycle.Rows) || DateTime.UtcNow >= deadline)
            {
                return cycle;
            }

            await Task.Delay(500, ct);
        }
    }

    private static Task<(List<PgDeadlocksCollector.Row> Rows, CollectorContext Context)> DeadlockCycleAsync(
        NpgsqlConnection c, IReadOnlyDictionary<string, string>? state, bool csv, CancellationToken ct) =>
        CycleAsync(PgDeadlocksCollector.Instance, c, state, csv, ct);

    private static Task<(List<PgPlanCaptureCollector.Row> Rows, CollectorContext Context)> PlanCycleAsync(
        NpgsqlConnection c, IReadOnlyDictionary<string, string>? state, bool csv, CancellationToken ct) =>
        CycleAsync(PgPlanCaptureCollector.Instance, c, state, csv, ct);

    private static Task<(List<PgLogEvent> Rows, CollectorContext Context)> EventCycleAsync(
        NpgsqlConnection c, IReadOnlyDictionary<string, string>? state, bool csv, CancellationToken ct) =>
        CycleAsync(PgLogEventsCollector.Instance, c, state, csv, ct);

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.CommandTimeout = 120;
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<NpgsqlConnection> OpenAsync(bool csv, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(csv ? CsvTarget : StderrTarget);
        await connection.OpenAsync(ct);
        return connection;
    }

    private static async Task RotateAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, "SELECT pg_rotate_logfile(); SELECT pg_sleep(1.2);", ct);
        await ExecAsync(connection, "DO $$ BEGIN RAISE LOG 'pm4704 new file line'; END $$;", ct);
        await ExecAsync(connection, "SELECT pg_sleep(0.3);", ct);
    }

    private static IReadOnlyDictionary<string, string> Carry(CollectorContext context) =>
        new Dictionary<string, string>(context.PendingState);

    private static string Tag() => "pm4704" + Guid.NewGuid().ToString("N")[..10];

    /// <summary>
    /// Runs <paramref name="count"/> real deadlocks on <paramref name="workers"/> session pairs in parallel. Each
    /// statement carries <paramref name="tag"/> in a comment, so the reports it produces name it. Returns after the
    /// last deadlock was logged.
    /// </summary>
    private static async Task DeadlocksAsync(bool csv, string tag, int count, int workers, CancellationToken ct)
    {
        var next = 0;
        var tasks = Enumerable.Range(0, workers).Select(async w =>
        {
            await using var one = await OpenAsync(csv, ct);
            await using var two = await OpenAsync(csv, ct);
            while (Interlocked.Increment(ref next) <= count)
            {
                var key = Random.Shared.Next(1, int.MaxValue);
                await ExecAsync(one, $"BEGIN; SELECT pg_advisory_xact_lock({key}, 1) AS {tag}", ct);
                await ExecAsync(two, $"BEGIN; SELECT pg_advisory_xact_lock({key}, 2)", ct);
                var first = ExecAsync(one, $"SELECT pg_advisory_xact_lock({key}, 2) AS {tag}", ct);
                await Task.Delay(40, ct);
                var second = ExecAsync(two, $"SELECT pg_advisory_xact_lock({key}, 1) AS {tag}", ct);
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
            }
        }).ToArray();
        await Task.WhenAll(tasks);
        await using var admin = await OpenAsync(csv, ct);
        await ExecAsync(admin, "SELECT pg_sleep(0.5)", ct);
    }

    private static bool Names(PgDeadlocksCollector.Row row, string tag) =>
        (row.GraphText?.Contains(tag, StringComparison.Ordinal) ?? false)
        || (row.VictimStatement?.Contains(tag, StringComparison.Ordinal) ?? false);

    private static bool Limited(CollectorContext context) =>
        context.Measurements.Any(m => m.Label == "log_matches_limited" && m.Value == 1);

    [Fact]
    public async Task ABurstOfDeadlocksAcrossARotation_AdvancesTheMarker_AndCollectsTheNewFilesReportByTheSecondCycle()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(StderrTarget), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(false, ct);

        var baseline = await DeadlockCycleAsync(connection, null, false, ct);
        var burst = Tag();
        var after = Tag();
        await DeadlocksAsync(false, burst, 540, 16, ct);
        await RotateAsync(connection, ct);
        await DeadlocksAsync(false, after, 1, 1, ct);

        var cycle1 = await DeadlockCycleAsync(connection, Carry(baseline.Context), false, ct);
        var cycle2 = await DeadlockCycleAsync(connection, Carry(cycle1.Context), false, ct);

        Assert.True(cycle1.Context.PendingState.ContainsKey(PgServerLogTail.ResumeStateKey), "the capped cycle stages a marker");
        Assert.True(Limited(cycle1.Context), "the capped cycle discloses the cut");
        Assert.Equal(500, cycle1.Rows.Count);
        Assert.True(cycle1.Rows.Concat(cycle2.Rows).Any(r => Names(r, after)),
            "the report written to the new file is collected by the second cycle at the latest");
    }

    [Fact]
    public async Task ABurstOfDeadlocksInOneFile_AdvancesTheMarker_KeepsTheNewest_AndDisclosesTheCut()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(StderrTarget), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(false, ct);

        var baseline = await DeadlockCycleAsync(connection, null, false, ct);
        var burst = Tag();
        var newest = Tag();
        await DeadlocksAsync(false, burst, 540, 16, ct);
        await DeadlocksAsync(false, newest, 1, 1, ct);

        var cycle = await CycleUntilAsync(PgDeadlocksCollector.Instance, connection, Carry(baseline.Context), false, rows => rows.Any(r => Names(r, newest)), ct);

        Assert.True(cycle.Context.PendingState.ContainsKey(PgServerLogTail.ResumeStateKey), "the marker advances on a capped read");
        Assert.True(Limited(cycle.Context));
        Assert.Equal(500, cycle.Rows.Count);
        Assert.Contains(cycle.Rows, r => Names(r, newest));
    }

    [Fact]
    public async Task ATinyReadUnderTheCap_RecordsNoDisclosure()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(StderrTarget), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(false, ct);
        await RotateAsync(connection, ct);
        var baseline = await DeadlockCycleAsync(connection, null, false, ct);
        var tag = Tag();
        await DeadlocksAsync(false, tag, 3, 1, ct);
        var cycle = await DeadlockCycleAsync(connection, Carry(baseline.Context), false, ct);
        Assert.False(Limited(cycle.Context));
        Assert.Contains(cycle.Rows, r => Names(r, tag));
    }

    [Fact]
    public async Task TwoRotationsInOneInterval_FinishTheMarkedFile_ReadTheNewestAndCountTheSkippedOne()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(StderrTarget), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(false, ct);

        await RotateAsync(connection, ct);
        var baseline = await EventCycleAsync(connection, null, false, ct);
        var pidA = await LockWaitAsync(false, Tag(), ct);
        await RotateAsync(connection, ct);
        var pidB = await LockWaitAsync(false, Tag(), ct);
        await RotateAsync(connection, ct);
        var pidC = await LockWaitAsync(false, Tag(), ct);

        var cycle = await EventCycleAsync(connection, Carry(baseline.Context), false, ct);

        Assert.Equal(1, WaitCount(cycle.Rows, pidA));
        Assert.Equal(0, WaitCount(cycle.Rows, pidB));
        Assert.Equal(1, WaitCount(cycle.Rows, pidC));
        Assert.Contains(cycle.Context.Measurements, m => m.Label == PgServerLogTail.FilesSkippedByRotationMeasurement && m.Value == 1);
    }

    private static async Task RaiseErrorAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        try
        {
            await ExecAsync(connection, sql, ct);
        }
        catch (PostgresException)
        {
            /* the error is the log record under test */
        }

        await ExecAsync(connection, "SELECT pg_sleep(0.3)", ct);
    }

    private static int WaitCount(IEnumerable<PgLogEvent> rows, int pid) =>
        rows.Count(r => r.Pid == pid && r.Message.Contains("still waiting", StringComparison.Ordinal));

    private static async Task<int> LockWaitAsync(bool csv, string table, CancellationToken ct)
    {
        /* Idle pooled sessions are reused LIFO, so two calls could report the same backend pid (a low, quickly reused pid on
           a container); drop them so every call's waiter is a new backend and its pid names only its own wait. */
        NpgsqlConnection.ClearAllPools();
        await using var holder = await OpenAsync(csv, ct);
        await using var waiter = await OpenAsync(csv, ct);
        await ExecAsync(holder, "CREATE TABLE " + table + " (id int PRIMARY KEY)", ct);
        await ExecAsync(holder, "INSERT INTO " + table + " VALUES (1)", ct);
        await ExecAsync(holder, "BEGIN", ct);
        await ExecAsync(holder, "UPDATE " + table + " SET id = 1", ct);
        await using var pidCommand = new NpgsqlCommand("SELECT pg_backend_pid()", waiter);
        var pid = (int)(await pidCommand.ExecuteScalarAsync(ct))!;
        try
        {
            await ExecAsync(waiter, "SET lock_timeout = '800ms'; UPDATE " + table + " SET id = 1", ct);
        }
        catch (PostgresException)
        {
            /* lock_timeout after the wait was logged */
        }

        await ExecAsync(holder, "ROLLBACK", ct);
        await ExecAsync(holder, "DROP TABLE " + table, ct);
        await ExecAsync(holder, "SELECT pg_sleep(0.3)", ct);
        return pid;
    }

    /// <summary>One statement over the auto_explain threshold in a fresh session; the plan names <paramref name="table"/>.</summary>
    private static async Task PlanAsync(bool csv, string table, CancellationToken ct)
    {
        await using var session = await OpenAsync(csv, ct);
        await ExecAsync(session,
            "LOAD 'auto_explain'; SET auto_explain.log_min_duration = 0; SET auto_explain.log_format = 'json'; SET compute_query_id = on;"
            + " CREATE TABLE " + table + " (id int); INSERT INTO " + table + " VALUES (1);", ct);
        await ExecAsync(session, "SELECT * FROM " + table + "; SELECT pg_sleep(0.3)", ct);
        await ExecAsync(session, "DROP TABLE " + table, ct);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task APlanWrittenBeforeARotation_IsCollectedWithTheMarker_AndMissedWithout(bool csv)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(csv ? CsvTarget : StderrTarget), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(csv, ct);

        await RotateAsync(connection, ct);
        var baseline = await PlanCycleAsync(connection, null, csv, ct);
        var table = Tag();
        await PlanAsync(csv, table, ct);
        await RotateAsync(connection, ct);

        var withState = await PlanCycleAsync(connection, Carry(baseline.Context), csv, ct);
        Assert.Contains(withState.Rows, r => r.PlanJson.Contains(table, StringComparison.Ordinal));

        var noState = await PlanCycleAsync(connection, null, csv, ct);
        Assert.DoesNotContain(noState.Rows, r => r.PlanJson.Contains(table, StringComparison.Ordinal));
    }

    [Fact]
    public async Task ADeadlockWrittenBeforeARotation_IsCollectedOnTheCsvRoute()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(CsvTarget), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(true, ct);

        await RotateAsync(connection, ct);
        var baseline = await DeadlockCycleAsync(connection, null, true, ct);
        var tag = Tag();
        await DeadlocksAsync(true, tag, 1, 1, ct);
        await RotateAsync(connection, ct);

        var withState = await DeadlockCycleAsync(connection, Carry(baseline.Context), true, ct);
        Assert.Contains(withState.Rows, r => Names(r, tag));
        Assert.True(withState.Context.PendingState.ContainsKey(PgServerLogTail.ResumeStateKeyCsv));

        var noState = await DeadlockCycleAsync(connection, null, true, ct);
        Assert.DoesNotContain(noState.Rows, r => Names(r, tag));
    }

    [Fact]
    public async Task AResumeOffsetInsideAQuotedMultiLineCsvField_CollectsTheRecordExactlyOnce()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(CsvTarget), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenAsync(true, ct);

        var baseline = await EventCycleAsync(connection, null, true, ct);
        var tag = Tag();
        /* One record of about 2 MB with an embedded newline every 100 bytes: the next read starts 1 MB before the
           end of what this read returned, which is in the middle of this record's quoted message. */
        await RaiseErrorAsync(connection,
            "DO $$ BEGIN RAISE EXCEPTION '%', '" + tag + "' || chr(10) || (SELECT string_agg(repeat('m', 98), chr(10)) FROM generate_series(1, 20000)); END $$;", ct);
        await ExecAsync(connection, "SELECT pg_sleep(0.3)", ct);

        var cycle1 = await CycleUntilAsync(PgLogEventsCollector.Instance, connection, Carry(baseline.Context), true, rows => rows.Any(r => r.Message.Contains(tag, StringComparison.Ordinal)), ct);
        var tag2 = Tag();
        await RaiseErrorAsync(connection, "DO $$ BEGIN RAISE EXCEPTION 'after " + tag2 + "'; END $$", ct);
        var cycle2 = await EventCycleAsync(connection, Carry(cycle1.Context), true, ct);

        var hits = cycle1.Rows.Concat(cycle2.Rows).Where(r => r.Message.Contains(tag, StringComparison.Ordinal)).ToList();
        Assert.True(hits.Count == 1, $"the record was collected {hits.Count} times (cycle1 marker {cycle1.Context.PendingState.GetValueOrDefault(PgServerLogTail.ResumeStateKeyCsv)}, cycle2 marker {cycle2.Context.PendingState.GetValueOrDefault(PgServerLogTail.ResumeStateKeyCsv)}, cycle1 rows {cycle1.Rows.Count}, cycle2 rows {cycle2.Rows.Count})");
        Assert.Contains(cycle2.Rows, r => r.Message.Contains(tag2, StringComparison.Ordinal));
    }
}
