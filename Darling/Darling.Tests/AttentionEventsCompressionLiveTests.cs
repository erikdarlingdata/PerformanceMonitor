/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so none shares a table with another class. */

/// <summary>
/// #9216: <c>collect.attention_events</c> has been a hypertable since #8967, but nothing enabled compression on it or gave it a
/// policy, so every daily chunk stayed uncompressed until retention dropped it. The store convergence pass now does both, and
/// these facts build a store with the service's own step list, so they follow whatever steps the service runs.
///
/// <para>The expected segmentby is a literal on purpose: these facts pin the value a store ends up with, so they must not move
/// when the product's constant does.</para>
/// </summary>
public sealed class AttentionEventsCompressionLiveTests
{
    private const string WantedSegmentBy = "server_id, event_kind";

    private const int ServerA = -921601;
    private const int ServerB = -921602;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// A converged store reports attention_events with compression on, the wanted segmentby and a compression policy job at the
    /// product's cadence. Before #9216 the store reported compression off and no job of any kind.
    /// </summary>
    [Fact]
    public async Task AConvergedStore_HasCompressionOn_TheWantedSegmentBy_AndAPolicyJob()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        await using var connection = await OpenTimescaleStoreAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            await ConvergeAsync(connection, ct);

            var state = await ReadHypertableAsync(connection, ct);
            Assert.True(state.IsHypertable);
            Assert.True(state.CompressionEnabled, "attention_events must have compression enabled after the convergence pass");
            Assert.Equal(WantedSegmentBy, state.SegmentBy);

            var job = await ReadPolicyJobAsync(connection, ct);
            Assert.NotNull(job);
            Assert.Equal(3600L, job!.ScheduleSeconds);
            Assert.Equal(86400L, job.CompressAfterSeconds);
            Assert.Contains(TimescaleSupport.AttentionEventsTable, await ConvergedTablesAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The lock check: with the settings converged, another session holds a lock any ALTER would wait behind. The step finishes at
    /// once and reports the table in place. Had it issued the ALTER, it would have waited out its lock timeout and reported the busy
    /// lock instead (a result of 0).
    /// </summary>
    [Fact]
    public async Task AConvergedStore_IssuesNoAlter_SoAHeldLockDoesNotHoldUpAnotherPass()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        await using var connection = await OpenTimescaleStoreAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            await ConvergeAsync(connection, ct);
            Assert.Equal(WantedSegmentBy, (await ReadHypertableAsync(connection, ct)).SegmentBy);

            await using var holder = new NpgsqlConnection(scratch.ConnectionString);
            await holder.OpenAsync(ct);
            await using var hold = await holder.BeginTransactionAsync(ct);
            await ExecAsync(holder, "SELECT count(*) FROM collect.attention_events", ct, hold);

            var logger = new CapturingTestLogger();
            using var bounded = CancellationTokenSource.CreateLinkedTokenSource(ct);
            bounded.CancelAfter(TimeSpan.FromSeconds(60));
            var result = await AttentionStep().EnsureAsync(connection, logger, bounded.Token);

            Assert.True(result == 1, $"a converged pass did not finish cleanly beside a reader's lock: {logger.Joined}");
            Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));

            await hold.RollbackAsync(ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// Old chunks with rows go through the policy job and compress, and the reads of the attention table return the same rows after.
    /// The rows hold both event kinds, two servers, a NULL kind and NULL text columns, across days older than the compress-after.
    /// </summary>
    [Fact]
    public async Task OldChunksWithRows_CompressThroughThePolicyJob_AndTheAttentionReadsReturnTheSameRows()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        await using var connection = await OpenTimescaleStoreAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            await ConvergeAsync(connection, ct);
            var now = DateTime.SpecifyKind(LiveClock.AnchoredEnd(), DateTimeKind.Unspecified);
            await SeedAsync(connection, now, ct);

            var floor = now.AddDays(-30);
            var ceiling = now.AddHours(1);
            var before = await ReadAllAsync(connection, "collect.attention_events", new[] { ServerA, ServerB }, floor, ceiling, ct);
            Assert.NotEmpty(before["rows"]);

            var uncompressedBefore = await ScalarAsync<long>(connection, "SELECT count(*) " + OldUncompressedChunksSql, ct);
            Assert.True(uncompressedBefore > 0, "the seeded rows must sit in chunks older than the compress-after");

            var job = await ReadPolicyJobAsync(connection, ct);
            Assert.NotNull(job);

            /* The job takes a capped number of chunks per run (#9267), so a backlog needs a few runs. */
            for (var run = 0; run < uncompressedBefore && await ScalarAsync<long>(connection, "SELECT count(*) " + OldUncompressedChunksSql, ct) > 0; run++)
            {
                await ExecAsync(connection, $"CALL run_job({job!.JobId})", ct);
            }

            Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) " + OldUncompressedChunksSql, ct));

            /* The table keeps no orderby of its own (the hypertable's setting is empty), so each chunk takes the default when it compresses. */
            Assert.Equal(
                "collection_time DESC",
                await ScalarAsync<string>(connection,
                    "SELECT string_agg(DISTINCT orderby, '; ') FROM timescaledb_information.chunk_compression_settings WHERE hypertable = 'collect.attention_events'::regclass", ct));
            Assert.True(
                await ScalarAsync<long>(connection, "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = 'attention_events' AND is_compressed", ct) > 0);

            var after = await ReadAllAsync(connection, "collect.attention_events", new[] { ServerA, ServerB }, floor, ceiling, ct);
            Assert.Equal(before.Keys, after.Keys);
            foreach (var (read, rows) in before)
            {
                Assert.True(rows == after[read], $"'{read}' returned different rows once the old chunks were compressed");
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// #9267: a store with a backlog of old chunks gets the policy with a first run an hour away (not at once, which on an upgrade
    /// is the restart warm-up), and the job compresses at most the cap per run, so the backlog goes over several runs. The job is run
    /// by hand as the scheduler would run it. Before #9267 the policy started at once, had no cap, and the first run compressed
    /// every old chunk.
    /// </summary>
    [Fact]
    public async Task ABacklogStore_GetsAPolicyThatStartsLater_AndEachRunCompressesAtMostTheCap()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        await using var connection = await OpenTimescaleStoreAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            var now = DateTime.SpecifyKind(LiveClock.AnchoredEnd(), DateTimeKind.Unspecified);
            await SeedAsync(connection, now, ct);
            var floor = now.AddDays(-30);
            var ceiling = now.AddHours(1);
            var before = await ReadAllAsync(connection, "collect.attention_events", new[] { ServerA, ServerB }, floor, ceiling, ct);

            await ConvergeAsync(connection, ct);

            var cap = TimescaleSupport.AttentionEventsCompressionChunkCap;
            var backlog = await ScalarAsync<long>(connection, "SELECT count(*) " + OldUncompressedChunksSql, ct);
            Assert.True(backlog > cap, $"the fact needs more old chunks than one run may take ({backlog} old chunks, cap {cap})");

            var job = await ReadPolicyJobAsync(connection, ct);
            Assert.NotNull(job);
            Assert.Equal(cap, job!.MaxChunksToCompress);
            Assert.True(job.StartsLater, $"the first run must wait past the restart, but it is due at {job.NextStart}");

            var runs = 0;
            var compressed = 0L;
            while (await ScalarAsync<long>(connection, "SELECT count(*) " + OldUncompressedChunksSql, ct) > 0)
            {
                Assert.True(runs < backlog, "the backlog did not drain");
                await ExecAsync(connection, $"CALL run_job({job.JobId})", ct);
                runs++;
                var total = await ScalarAsync<long>(connection, CompressedCountSql, ct);
                Assert.True(total - compressed <= cap, $"run {runs} compressed {total - compressed} chunks, past the cap of {cap}");
                if (runs == 1)
                {
                    Assert.Equal(cap, total);
                }

                compressed = total;
            }

            Assert.Equal((backlog + cap - 1) / cap, runs);

            var after = await ReadAllAsync(connection, "collect.attention_events", new[] { ServerA, ServerB }, floor, ceiling, ct);
            foreach (var (read, rows) in before)
            {
                Assert.True(rows == after[read], $"'{read}' returned different rows once the backlog was compressed");
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// #9267, a store whose policy already exists (added in the #9216 shape: no cap, on its own schedule) and that already
    /// compressed its backlog: the step adds the cap and nothing else. The schedule, <c>next_start</c> and <c>compress_after</c> stay,
    /// the compressed chunks stay the same chunks, and a later job run compresses nothing more. A second pass issues no
    /// <c>alter_job</c> (the job's catalog row is not rewritten).
    /// </summary>
    [Fact]
    public async Task AStoreWhoseOldPolicyAlreadyCompressedItsBacklog_TakesTheCapAndNothingElseChanges()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        await using var connection = await OpenTimescaleStoreAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            var now = DateTime.SpecifyKind(LiveClock.AnchoredEnd(), DateTimeKind.Unspecified);
            await SeedAsync(connection, now, ct);

            /* The policy as #9216 added it: the shared statement, no initial_start, no cap. The ENABLE comes from the step list. */
            await ConvergeAsync(connection, ct);
            await ExecAsync(connection, $"SELECT remove_compression_policy('collect.{TimescaleSupport.AttentionEventsTable}')", ct);
            await ExecAsync(connection, TimescaleSupport.AddCompressionPolicySql(TimescaleSupport.AttentionEventsTable), ct);

            var old = await ReadPolicyJobAsync(connection, ct);
            Assert.NotNull(old);
            Assert.Null(old!.MaxChunksToCompress);
            await ExecAsync(connection, $"CALL run_job({old.JobId})", ct);
            Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) " + OldUncompressedChunksSql, ct));
            var compressedBefore = await ScalarAsync<string>(connection, CompressedNamesSql, ct);

            var result = await AttentionStep().EnsureAsync(connection, NullLogger.Instance, ct);
            Assert.Equal(1, result);

            var job = await ReadPolicyJobAsync(connection, ct);
            Assert.NotNull(job);
            Assert.Equal(TimescaleSupport.AttentionEventsCompressionChunkCap, job!.MaxChunksToCompress);
            Assert.Equal(old.JobId, job.JobId);
            Assert.Equal(old.NextStart, job.NextStart);
            Assert.Equal(old.FixedSchedule, job.FixedSchedule);
            Assert.Equal(old.ScheduleSeconds, job.ScheduleSeconds);
            Assert.Equal(old.CompressAfterSeconds, job.CompressAfterSeconds);

            await ExecAsync(connection, $"CALL run_job({job.JobId})", ct);
            Assert.Equal(compressedBefore, await ScalarAsync<string>(connection, CompressedNamesSql, ct));
            Assert.Equal(0L, await ScalarAsync<long>(connection, "SELECT count(*) " + OldUncompressedChunksSql, ct));

            /* A second pass finds the cap in place and rewrites nothing. */
            var rowVersionSql = $"SELECT xmin::text FROM _timescaledb_catalog.bgw_job WHERE id = {job.JobId}";
            var rowVersion = await ScalarAsync<string>(connection, rowVersionSql, ct);
            Assert.Equal(1, await AttentionStep().EnsureAsync(connection, NullLogger.Instance, ct));
            Assert.Equal(rowVersion, await ScalarAsync<string>(connection, rowVersionSql, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A store whose attention table is still a plain table (its heap was too large to convert) has neither statement to run: the
    /// step leaves it alone, adds no policy and warns nothing, because the conversion step already says why.
    /// </summary>
    [Fact]
    public async Task APlainAttentionTable_IsLeftAlone_WithNoPolicyAndNoWarning()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        await using var connection = await OpenTimescaleStoreAsync(scratch, ct, convert: false);

        var bodySucceeded = false;
        try
        {
            Assert.False((await ReadHypertableAsync(connection, ct)).IsHypertable, "this fact needs a plain attention table");

            var logger = new CapturingTestLogger();
            var result = await AttentionStep().EnsureAsync(connection, logger, ct);

            Assert.Equal(0, result);
            Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
            Assert.Null(await ReadPolicyJobAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /* ---------------- the store ---------------- */

    private static string RequireLivePostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the attention compression tests (each mints its own scratch database).");
        return ConnectionString!;
    }

    private static async Task<NpgsqlConnection> OpenTimescaleStoreAsync(ScratchPostgres scratch, CancellationToken ct, bool convert = true)
    {
        Assert.True(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "TimescaleDB must be available for the attention compression tests");
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        if (convert)
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        }

        return connection;
    }

    /// <summary>The service's store-object steps up to the compression schedule converge: the ones that decide a hypertable's compression.</summary>
    private static async Task ConvergeAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var tally = new DarlingWorker.StoreObjectConvergenceTally();
        foreach (var step in StoreObjectSteps().TakeWhile(s => s.Name != "stale aggregate reshape"))
        {
            await DarlingWorker.RunStoreObjectConvergenceStepAsync(connection, step, tally, NullLogger.Instance, ct);
        }

        Assert.True(tally.Failed.Count == 0, "A store-object step failed: " + string.Join(", ", tally.Failed));
    }

    private static DarlingWorker.StoreObjectConvergenceStep AttentionStep() =>
        StoreObjectSteps().Single(s => s.Name.Contains("attention_events", StringComparison.Ordinal));

    private static DarlingWorker.StoreObjectConvergenceStep[] StoreObjectSteps()
    {
        var field = typeof(DarlingWorker).GetField("s_storeObjectConvergence", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(field);
        return (DarlingWorker.StoreObjectConvergenceStep[])field!.GetValue(null)!;
    }

    /// <summary>Rows across 12 days up to today: both kinds on two servers, a NULL-kind row and rows with NULL text columns.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        const string sql = @"
INSERT INTO collect.attention_events
    (attention_event_id, collection_time, server_id, server_name, event_time, event_kind, attention_count,
     database_name, client_app_name, client_hostname, session_id, request_id, interrupt_duration_us, statement_fingerprint, statement_text)
SELECT
    -921600000 - ((-s.srv - 921600) * 1000000) - (d.day * 1000) - g.n,
    date_trunc('day', $1::timestamp) - make_interval(days => d.day) + make_interval(mins => g.n * 7),
    s.srv,
    'ATTENTION-SRV',
    NULL,
    CASE WHEN g.n % 5 = 0 THEN NULL WHEN g.n % 2 = 0 THEN 'count' ELSE 'sample' END,
    CASE WHEN g.n % 2 = 0 THEN g.n ELSE NULL END,
    CASE WHEN g.n % 3 = 0 THEN NULL ELSE 'db' || (g.n % 3) END,
    CASE WHEN g.n % 4 = 0 THEN NULL ELSE 'app' || (g.n % 4) END,
    'host' || (g.n % 2),
    g.n,
    NULL,
    2000000 + g.n,
    CASE WHEN g.n % 7 = 0 THEN NULL ELSE 'fp' || (g.n % 6) END,
    CASE WHEN g.n % 7 = 0 THEN NULL ELSE 'SELECT ' || g.n END
FROM (VALUES (-921601), (-921602)) AS s(srv)
CROSS JOIN generate_series(0, 11) AS d(day)
CROSS JOIN generate_series(1, 20) AS g(n)";
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, now);
        await command.ExecuteNonQueryAsync(ct);
    }

    private const string CompressedCountSql =
        "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = 'attention_events' AND is_compressed";

    private const string CompressedNamesSql =
        "SELECT string_agg(chunk_name, ',' ORDER BY chunk_name) FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = 'attention_events' AND is_compressed";

    private const string OldUncompressedChunksSql =
        "FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = 'attention_events' "
        + "AND NOT is_compressed AND range_end <= now() - INTERVAL '1 day'";

    /// <summary>Every row, and the shapes the attention reads take (counts, sample groups, top lists), each rendered to one string.</summary>
    internal static async Task<Dictionary<string, string>> ReadAllAsync(
        NpgsqlConnection connection, string events, int[] servers, DateTime floor, DateTime ceiling, CancellationToken ct)
    {
        var reads = new List<(string Name, string Sql, object[] Args)>
        {
            ("rows",
                "SELECT attention_event_id, collection_time, server_id, event_kind, attention_count, database_name, client_app_name, client_hostname, interrupt_duration_us, statement_fingerprint, statement_text FROM " + events + " ORDER BY attention_event_id",
                Array.Empty<object>()),
        };
        foreach (var server in servers)
        {
            var args = new object[] { server, floor, ceiling };
            reads.Add(($"counts {server}", AttentionWindowSql.Counts(events, bounded: true), args));
            reads.Add(($"sample groups {server}", AttentionWindowSql.SampleGroups(events, bounded: true), args));
            reads.Add(($"statements {server}", AttentionWindowSql.TopStatements(events, bounded: true), args));
            reads.Add(($"apps {server}", AttentionWindowSql.TopBy(events, "client_app_name", bounded: true), args));
            reads.Add(($"hosts {server}", AttentionWindowSql.TopBy(events, "client_hostname", bounded: true), args));
        }

        var results = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (name, sql, args) in reads)
        {
            await using var command = new NpgsqlCommand(sql, connection);
            foreach (var arg in args)
            {
                command.Parameters.AddWithValue(arg is DateTime ? NpgsqlDbType.Timestamp : NpgsqlDbType.Integer, arg);
            }

            var text = new StringBuilder();
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    text.Append(reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture)).Append('|');
                }

                text.Append('\n');
            }

            results[name] = text.ToString();
        }

        return results;
    }

    /* ---------------- reads of the store ---------------- */

    private sealed record HypertableState(bool IsHypertable, bool CompressionEnabled, string? SegmentBy);

    private sealed record PolicyJob(
        int JobId, long ScheduleSeconds, long CompressAfterSeconds, bool FixedSchedule, string NextStart, bool StartsLater, int? MaxChunksToCompress);

    private static async Task<HypertableState> ReadHypertableAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            @"SELECT h.compression_enabled,
                     (SELECT string_agg(cs.attname, ', ' ORDER BY cs.segmentby_column_index)
                      FROM timescaledb_information.compression_settings AS cs
                      WHERE cs.hypertable_schema = h.hypertable_schema
                      AND   cs.hypertable_name = h.hypertable_name
                      AND   cs.segmentby_column_index IS NOT NULL)
              FROM timescaledb_information.hypertables AS h
              WHERE h.hypertable_schema = 'collect' AND h.hypertable_name = 'attention_events'", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return new HypertableState(false, false, null);
        }

        return new HypertableState(true, !reader.IsDBNull(0) && reader.GetBoolean(0), reader.IsDBNull(1) ? null : reader.GetString(1));
    }

    private static async Task<PolicyJob?> ReadPolicyJobAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            @"SELECT job_id, EXTRACT(EPOCH FROM schedule_interval)::bigint,
                     EXTRACT(EPOCH FROM (config ->> 'compress_after')::interval)::bigint,
                     fixed_schedule, COALESCE(next_start::text, ''), COALESCE(next_start > now() + INTERVAL '30 minutes', false),
                     (config ->> 'maxchunks_to_compress')::int
              FROM timescaledb_information.jobs
              WHERE proc_name = 'policy_compression' AND hypertable_schema = 'collect' AND hypertable_name = 'attention_events'", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct)
            ? new PolicyJob(
                reader.GetInt32(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetBoolean(3),
                reader.GetString(4), reader.GetBoolean(5), reader.IsDBNull(6) ? null : reader.GetInt32(6))
            : null;
    }

    private static async Task<IReadOnlySet<string>> ConvergedTablesAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await TimescaleSupport.ReadTablesNeedingCompressionEnableAsync(connection, null, ct)
            ?? throw new InvalidOperationException("the convergence read failed");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, NpgsqlTransaction? transaction = null)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }
}
