/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8967: the attention collector's <c>MAX(event_time)</c> watermark comes from the runner's in-memory cache after one
/// store read per server per runner (service start), and the samples it stores are exactly the ones a fresh store read
/// on every run stores. Each run here takes the runner's own steps: resolve the watermark, take the ring-buffer events
/// newer than the cutoff <see cref="AttentionCollector.BuildQuery"/> derives from it, write them through the runner's
/// <c>WriteBatchAsync</c>, then advance the cache only when the write returned, and drop it when the write threw
/// (what <c>RunAsync</c>'s catch does).
///
/// <para><b>#1776 own-store</b>: every test here creates and drops its own scratch database through
/// <see cref="ScratchPostgres"/>, so it is not in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class AttentionWatermarkCacheLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly MethodInfo ResolveMethod = typeof(DarlingCollectorRunner)
        .GetMethod("ResolveServerWatermarkAsync", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance)!;

    private static readonly MethodInfo CommitMethod = typeof(DarlingCollectorRunner)
        .GetMethod("CommitServerWatermark", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance)!;

    private static readonly MethodInfo WriteBatchMethod = typeof(DarlingCollectorRunner)
        .GetMethod("WriteBatchAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;

    private static readonly FieldInfo CacheField = typeof(DarlingCollectorRunner)
        .GetField("_watermarkCache", BindingFlags.NonPublic | BindingFlags.Instance)!;

    private sealed record RingEvent(DateTime Time, int RequestId);

    /// <summary>One run of the schedule: the events the ring buffer received since the last run, and whether the write fails.</summary>
    private sealed record Step(RingEvent[] Dispatched, bool CountRow, bool WriteFails = false, bool RestartBefore = false);

    private static ServerRuntime MakeServer(int serverId, string name) => new()
    {
        Config = new MonitoredServer { Name = name, Host = name },
        ConnectionString = "Server=" + name,
        Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
        StorageName = name,
        ServerId = serverId,
        EngineEdition = 3,
    };

    private static CollectorContext MakeContext(ServerRuntime server, DateTime collectionTime, DateTime? watermark = null) => new()
    {
        /* #9895: the read names this install's own client-timeout session. */
        AlwaysOnSessionName = AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.DarlingProduct, "0a1b2c3d", AlwaysOnXeSessionKind.Attention),
        ServerId = server.ServerId,
        ServerName = server.StorageName,
        CollectionTime = collectionTime,
        Deltas = new CollectorDeltaCalculator(),
        Target = server.Target,
        Watermark = watermark,
    };

    private static async Task<DateTime?> ResolveAsync(DarlingCollectorRunner runner, ServerRuntime server, CancellationToken ct)
    {
        var probe = MakeContext(server, DateTime.UtcNow);
        var task = (Task)ResolveMethod.MakeGenericMethod(typeof(AttentionCollector.Row)).Invoke(
            runner, new object?[] { server, AttentionCollector.Instance, probe, null, ct })!;
        await task;
        var result = ((DateTime?, bool, long?, long, bool, bool))task.GetType().GetProperty("Result")!.GetValue(task)!;
        return result.Item1;
    }

    private static async Task<int> WriteBatchAsync(
        DarlingCollectorRunner runner, NpgsqlConnection connection, List<AttentionCollector.Row> rows, ServerRuntime server,
        CollectorContext context, CancellationToken ct)
    {
        var task = (Task)WriteBatchMethod.MakeGenericMethod(typeof(AttentionCollector.Row)).Invoke(
            runner, new object?[] { connection, AttentionCollector.Instance, rows, server, context.CollectionTime, context, ct })!;
        await task;
        return (int)task.GetType().GetProperty("Result")!.GetValue(task)!;
    }

    private static void Commit(DarlingCollectorRunner runner, ServerRuntime server, List<AttentionCollector.Row> rows, CollectorContext context)
        => CommitMethod.MakeGenericMethod(typeof(AttentionCollector.Row)).Invoke(
            runner, new object?[] { server, AttentionCollector.Instance, rows, false, context.Measurements });

    private static void Invalidate(DarlingCollectorRunner runner, ServerRuntime server)
    {
        var cache = CacheField.GetValue(runner)!;
        cache.GetType().GetMethod("Invalidate")!.Invoke(cache, new object?[] { server.ServerId, AttentionCollector.Instance.Name });
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Runs the schedule for one server. <paramref name="freshRunnerEveryRun"/> builds a new runner (an empty cache, so a
    /// store read) for every run: the behaviour before #8967. Returns each run's watermark and its store statement count.
    /// </summary>
    private static async Task<(List<DateTime?> Watermarks, List<int> Reads)> RunScheduleAsync(
        NpgsqlDataSource countedStore, CommandCountingLoggerFactory counter, NpgsqlConnection writeConnection,
        ServerRuntime server, IReadOnlyList<Step> steps, bool freshRunnerEveryRun, CancellationToken ct)
    {
        var watermarks = new List<DateTime?>();
        var reads = new List<int>();
        var ring = new List<RingEvent>();
        var runner = new DarlingCollectorRunner(countedStore, new CollectorDeltaCalculator());

        foreach (var step in steps)
        {
            if (freshRunnerEveryRun || step.RestartBefore)
            {
                runner = new DarlingCollectorRunner(countedStore, new CollectorDeltaCalculator());
            }

            ring.AddRange(step.Dispatched);

            counter.Provider.Reset();
            var watermark = await ResolveAsync(runner, server, ct);
            reads.Add(counter.Provider.CountContaining("attention_events"));
            watermarks.Add(watermark);

            var context = MakeContext(server, DateTime.UtcNow, watermark);
            var cutoff = (DateTime)AttentionCollector.Instance.BuildQuery(context).Parameters.Single(p => p.Name == "@cutoff_time").Value!;

            /* The batch's own filter, @timestamp > @cutoff_time, over everything the ring buffer still holds. */
            var rows = ring
                .Where(e => e.Time > cutoff)
                .OrderBy(e => e.Time)
                .Select(e => new AttentionCollector.Row
                {
                    EventKind = AttentionCollector.KindSample,
                    EventTime = e.Time,
                    AttentionCount = 0,
                    RequestId = e.RequestId,
                    ClientAppName = "wm-test",
                })
                .ToList();

            if (step.CountRow)
            {
                rows.Add(new AttentionCollector.Row { EventKind = AttentionCollector.KindCount, AttentionCount = 3 });
            }

            await ExecuteAsync(writeConnection, "UPDATE public.wm_fail SET armed = " + (step.WriteFails ? "true" : "false"), ct);
            var failed = false;
            try
            {
                var written = await WriteBatchAsync(runner, writeConnection, rows, server, context, ct);
                if (written > 0)
                {
                    Commit(runner, server, rows, context);
                }
            }
            catch (Exception) when (step.WriteFails)
            {
                failed = true;
                Invalidate(runner, server);
            }

            Assert.Equal(step.WriteFails, failed);
        }

        await ExecuteAsync(writeConnection, "UPDATE public.wm_fail SET armed = false", ct);
        return (watermarks, reads);
    }

    private static async Task<List<string>> StoredSamplesAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        var samples = new List<string>();
        await using var command = new NpgsqlCommand(
            "SELECT to_char(event_time, 'HH24:MI:SS.US') || '#' || request_id FROM collect.attention_events "
            + "WHERE server_id = $1 AND event_kind = 'sample' ORDER BY event_time, request_id", connection);
        command.Parameters.AddWithValue(serverId);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            samples.Add(reader.GetString(0));
        }

        return samples;
    }

    [Fact]
    public async Task TheWatermarkIsReadOncePerRunner_AndTheStoredSamplesMatchAFreshReadEveryRun_AcrossAFailedWriteAndARestart()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to run the attention watermark cache pin.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var counter = new CommandCountingLoggerFactory();
        await using var countedStore = new NpgsqlDataSourceBuilder(scratch.ConnectionString).UseLoggerFactory(counter).Build();

        var bodySucceeded = false;
        try
        {
            /* A planted, switchable insert failure: the whole COPY fails and nothing of the batch commits. */
            await ExecuteAsync(connection, """
                CREATE TABLE public.wm_fail (armed boolean NOT NULL);
                INSERT INTO public.wm_fail VALUES (false);
                CREATE FUNCTION public.wm_fail_insert() RETURNS trigger LANGUAGE plpgsql AS $f$
                BEGIN
                    IF (SELECT armed FROM public.wm_fail) THEN
                        RAISE EXCEPTION 'planted insert failure';
                    END IF;
                    RETURN NEW;
                END
                $f$;
                CREATE TRIGGER wm_fail_insert BEFORE INSERT ON collect.attention_events
                    FOR EACH ROW EXECUTE FUNCTION public.wm_fail_insert();
                """, ct);

            /* Millisecond event times, as the XE ring buffer stamps them, inside the first run's ten-minute lookback. */
            var now = DateTime.UtcNow;
            var t0 = new DateTime(now.Ticks - now.Ticks % TimeSpan.TicksPerMillisecond, DateTimeKind.Unspecified).AddMinutes(-8);
            var steps = new List<Step>
            {
                /* Two events share a timestamp in one dispatch: both are stored, and the later cutoff equals that time. */
                new(new[] { new RingEvent(t0, 1), new RingEvent(t0.AddSeconds(1), 2), new RingEvent(t0.AddSeconds(1), 3) }, CountRow: true),
                new(Array.Empty<RingEvent>(), CountRow: true),
                new(Array.Empty<RingEvent>(), CountRow: false),
                /* The write fails: event 4 is not stored and the watermark must not pass it. */
                new(new[] { new RingEvent(t0.AddSeconds(2), 4) }, CountRow: true, WriteFails: true),
                new(new[] { new RingEvent(t0.AddSeconds(3), 5) }, CountRow: false),
                new(Array.Empty<RingEvent>(), CountRow: true),
                /* A service restart: a new runner, so an empty cache. */
                new(new[] { new RingEvent(t0.AddSeconds(4), 6), new RingEvent(t0.AddSeconds(4), 7) }, CountRow: false, RestartBefore: true),
                new(Array.Empty<RingEvent>(), CountRow: true),
                new(Array.Empty<RingEvent>(), CountRow: false),
                new(new[] { new RingEvent(t0.AddSeconds(5).AddTicks(1230), 8) }, CountRow: false),
                new(Array.Empty<RingEvent>(), CountRow: false),
            };

            var cachedServer = MakeServer(-896701, "wm-attention-cached");
            var freshServer = MakeServer(-896702, "wm-attention-fresh");

            var cached = await RunScheduleAsync(countedStore, counter, connection, cachedServer, steps, freshRunnerEveryRun: false, ct);
            var fresh = await RunScheduleAsync(countedStore, counter, connection, freshServer, steps, freshRunnerEveryRun: true, ct);

            /* Every run's watermark is the one a fresh store read returns. */
            Assert.Equal(fresh.Watermarks, cached.Watermarks);

            /* The store is read when the cache is empty: the first run (an empty store, so the 6-hour probe, the week and the
               unbounded MAX), the run after the failed write (one probe), and the first run after the restart. Never otherwise.
               Each of those reads also asks once whether the swapped-out legacy table exists (#8967), and there is none here. */
            Assert.Equal(new[] { 4, 0, 0, 0, 2, 0, 2, 0, 0, 0, 0 }, cached.Reads);
            Assert.All(fresh.Reads.Skip(1), reads => Assert.True(reads >= 1));

            /* Each event is stored once: the tie rows are kept, the failed write's event comes back on the next run, and
               nothing at the watermark is written twice. Identical to the fresh-read schedule. */
            var cachedSamples = await StoredSamplesAsync(connection, cachedServer.ServerId, ct);
            var freshSamples = await StoredSamplesAsync(connection, freshServer.ServerId, ct);
            Assert.Equal(new[] { 1, 2, 3, 4, 5, 6, 7, 8 }, cachedSamples.Select(s => int.Parse(s[(s.IndexOf('#') + 1)..])).ToArray());
            Assert.Equal(freshSamples, cachedSamples);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
