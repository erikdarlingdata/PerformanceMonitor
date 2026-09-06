/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Targets;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Takes the one out-of-band, server-wide wait sample <see cref="StallProbeArm"/> decided to spend, and
/// stores it (#2880). One connection, one query, one row, and then it is done — there is no retry path in
/// this type and adding one would be the second problem #2880 warns about.
///
/// <para><b>The pool bound, stated.</b> The sequential per-server model means at most one server-scoped read
/// is in flight per monitored server, so at most one arm per server can be armed, and
/// <see cref="StallProbeGate"/> refuses a second concurrent probe against the same server outright. The new
/// bound is therefore <b>+1 concurrent connection per monitored server, for at most
/// <see cref="StallWaitProbePolicy.HardBudget"/></b>, and nothing about it scales with fleet size or with how
/// often a target stalls. For scale: the target connection strings set no <c>Max Pool Size</c>, so SqlClient's
/// default of 100 per distinct string applies, and the tool's steady state is one. This is also not the first
/// thing in the product to step outside the sequential model — <c>QueryStoreBackfill</c> already runs a
/// genuinely concurrent second reader against the same target (measured at ~128 MB in flight), gated the same
/// single-flight way — so the shape is established and this is by far the lighter of the two.</para>
///
/// <para><b>What it costs when it fires every cycle.</b> The honest answer is: one extra connection and two
/// DMV scans per stalled run, and nothing compounds. Because the arm is one-shot per run and the gate is
/// per-server, a target stalling on every cycle of both affected collectors produces two probes per cycle and
/// never three, whatever the target does. The measured population is roughly 90 probes a day across a
/// 43-server fleet; the pathological population — every cycle of both collectors on every server — is bounded
/// at two per server per cycle by construction rather than by a rate limiter that would need its own state.
/// The store side is bounded the same way: one INSERT per probe, and a retention DELETE the same write pays
/// for.</para>
///
/// <para><b>Every failure is an outcome, not an exception.</b> Nothing here can throw into the caller: the arm
/// does not await this task, so an escaping exception would be an unobserved one, and — more to the point — a
/// probe that could not connect is a MEASUREMENT of something #2880 lists as untested, not a fault to
/// swallow. See <see cref="StallWaitProbePolicy.OutcomeConnectFailed"/>.</para>
/// </summary>
internal static class StallWaitProbeRunner
{
    /* One row per probe. The trigger columns come first because they are what the client believed, then the
       probe's own cost, then the sample. 24 columns, all named, positional binding below in the same order. */
    internal const string InsertSql = @"
INSERT INTO collect.collector_stall_probes
(probe_time, server_id, server_name, collector_name, outcome, budget_ms, trigger_elapsed_ms,
 trigger_rows_read, trigger_bytes_read, trigger_last_read_ms, connect_ms, query_ms,
 waiting_task_count, distinct_wait_types, top_wait_type, top_wait_total_ms, top_wait_max_ms, wait_summary,
 scheduler_count, runnable_tasks, work_queue_length, pending_disk_io, max_runnable_tasks, error_message)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24);";

    /// <summary>
    /// How long a probe row is kept. Matched to <see cref="DarlingRetention.CollectionLogRetentionDays"/>'s
    /// effective 60 days deliberately: a probe row's only use is beside the <c>collection_log</c> row of the
    /// run that triggered it, so outliving that row would leave a sample with nothing to explain, and expiring
    /// first would erase the explanation while the symptom was still queryable.
    /// </summary>
    internal const int RetentionDays = 60;

    /* Paid for by the INSERT rather than by the daily purge, the store_metrics / collector_cost shape. The
       arrival rate is ~90 rows a day fleet-wide, so this deletes nothing on almost every call and the cost of
       asking is a bounded index seek. Kept out of DarlingRetention because that sweep enumerates
       CollectorCatalog.All and this table is deliberately not in it. */
    internal const string PurgeSql = @"
DELETE FROM collect.collector_stall_probes WHERE probe_time < $1;";

    /// <summary>
    /// Truncation ceiling for <c>error_message</c>, the same 4000 as
    /// <c>DarlingObservability.LogCollectionAsync</c> uses on <c>collection_log.error_message</c>. A driver
    /// message can carry a whole stack; the column is a diagnostic, not an archive.
    /// </summary>
    internal const int MaxErrorLength = 4000;

    /// <summary>
    /// Runs one probe and stores its result. Never throws.
    /// </summary>
    /// <param name="wallClockBudget">
    /// The stalled collector's own budget, recorded on the row so <c>trigger_elapsed_ms</c> can be read as a
    /// fraction of it years from now without knowing what the constant was at the time.
    /// </param>
    /// <param name="observation">
    /// The client-side state <see cref="StallProbeArm"/> made its decision on — the same value, not a later
    /// re-reading.
    /// </param>
    /// <param name="cancellationToken">
    /// The service's token. The probe's own <see cref="StallWaitProbePolicy.HardBudget"/> is linked onto it,
    /// so a shutdown cancels the probe as well; the STORE write then runs on
    /// <see cref="CancellationToken.None"/> for the reason <c>DarlingCollectorRunner</c>'s store reopen does —
    /// abandoning the write would discard the measurement the probe just paid a target query for.
    /// </param>
    internal static async Task RunAsync(
        NpgsqlDataSource postgres,
        ServerRuntime server,
        string collectorName,
        TimeSpan wallClockBudget,
        StallProbeObservation observation,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(server);

        var probeTime = DateTime.UtcNow;

        /* Single-flight per server. A held gate means a probe is already out against this target, and a
           second one is refused rather than queued: the point of a bound is that it holds when the target is
           at its worst, which is exactly when a queue would fill. Not recorded as a row — nothing was
           measured and nothing was attempted, so there is no outcome to store. */
        using var lease = StallProbeGate.TryAcquire(server.ServerId);
        if (lease is null)
        {
            logger?.LogDebug(
                "Stall probe for {Collector} on '{Server}' skipped: a probe is already in flight against this target (#2880)",
                collectorName, server.Config.DisplayName);
            return;
        }

        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(StallWaitProbePolicy.HardBudget);

        var outcome = StallWaitProbePolicy.OutcomeNoSample;
        string? error = null;
        int? connectMs = null;
        int? queryMs = null;
        StallWaitSample? sample = null;
        var connected = false;

        var clock = Stopwatch.StartNew();

        try
        {
            var provider = TargetProviders.For(server.Target);

            await using var connection = provider.CreateConnection(server.ConnectionString);
            await connection.OpenAsync(budget.Token).ConfigureAwait(false);
            connectMs = (int)Math.Min(int.MaxValue, clock.ElapsedMilliseconds);
            connected = true;

            var queryClock = Stopwatch.StartNew();
            using var command = provider.CreateCommand(
                new CollectorQuery(StallWaitProbePolicy.QueryText),
                connection,
                StallWaitProbePolicy.CommandTimeoutSeconds);

            await using var reader = await command.ExecuteReaderAsync(budget.Token).ConfigureAwait(false);
            sample = await StallWaitProbePolicy.ReadAsync(reader, budget.Token).ConfigureAwait(false);
            queryMs = (int)Math.Min(int.MaxValue, queryClock.ElapsedMilliseconds);

            outcome = sample is null
                ? StallWaitProbePolicy.OutcomeNoSample
                : StallWaitProbePolicy.OutcomeSampled;
        }
        catch (Exception ex)
        {
            /* Classified on the PHASE and on the TOKEN, never on the exception type — the
               ItemBudgetExpired reasoning: cancelling a SqlClient operation does not reliably surface as
               OperationCanceledException, and a service shutdown must not be mislabelled as the probe's own
               deadline. Whether the connection opened is what separates the connect outcomes from the query
               ones, which is the split #2880 asks to be able to see. */
            var timedOut = budget.IsCancellationRequested && !cancellationToken.IsCancellationRequested;

            outcome = (connected, timedOut) switch
            {
                (false, true) => StallWaitProbePolicy.OutcomeConnectTimedOut,
                (false, false) => StallWaitProbePolicy.OutcomeConnectFailed,
                (true, true) => StallWaitProbePolicy.OutcomeQueryTimedOut,
                (true, false) => StallWaitProbePolicy.OutcomeQueryFailed,
            };

            error = Truncate(ex.Message);

            logger?.LogInformation(
                "Stall probe for {Collector} on '{Server}' ended {Outcome} after {Elapsed}ms: {Message} (#2880)",
                collectorName, server.Config.DisplayName, outcome, clock.ElapsedMilliseconds, ex.Message);
        }

        if (outcome == StallWaitProbePolicy.OutcomeSampled && sample is not null)
        {
            logger?.LogWarning(
                "Stall probe for {Collector} on '{Server}': {WaitingTasks} waiting task(s) across {WaitTypes} type(s), "
                + "top {TopWait} ({TopWaitMs}ms), {Runnable} runnable / {PendingIo} pending IO across {Schedulers} scheduler(s) "
                + "— sampled {Elapsed}ms into a {Budget}s budget (#2880)",
                collectorName, server.Config.DisplayName, sample.WaitingTaskCount, sample.DistinctWaitTypes,
                sample.TopWaitType ?? "(nothing waiting)", sample.TopWaitTotalMs, sample.RunnableTasks,
                sample.PendingDiskIo, sample.SchedulerCount, observation.ElapsedMs,
                (int)wallClockBudget.TotalSeconds);
        }

        await StoreAsync(
            postgres, server, collectorName, wallClockBudget, observation, probeTime,
            outcome, connectMs, queryMs, sample, error, logger).ConfigureAwait(false);
    }

    private static async Task StoreAsync(
        NpgsqlDataSource postgres,
        ServerRuntime server,
        string collectorName,
        TimeSpan wallClockBudget,
        StallProbeObservation observation,
        DateTime probeTimeUtc,
        string outcome,
        int? connectMs,
        int? queryMs,
        StallWaitSample? sample,
        string? error,
        ILogger? logger)
    {
        try
        {
            /* CancellationToken.None: the target query is already paid for, so a shutdown arriving between
               the sample and the write would throw away a measurement that cannot be retaken. */
            await using var connection = await postgres.OpenConnectionAsync(CancellationToken.None).ConfigureAwait(false);

            await using (var command = new NpgsqlCommand(InsertSql, connection))
            {
                command.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;

                /* Naive-UTC storage: Npgsql 6+ infers timestamptz from Kind=Utc and silently zone-shifts —
                   see PgCollectorRowWriter. */
                command.Parameters.AddWithValue(DateTime.SpecifyKind(probeTimeUtc, DateTimeKind.Unspecified));
                command.Parameters.AddWithValue(server.ServerId);
                command.Parameters.AddWithValue(server.StorageName);
                command.Parameters.AddWithValue(collectorName);
                command.Parameters.AddWithValue(outcome);
                command.Parameters.AddWithValue((int)wallClockBudget.TotalMilliseconds);
                command.Parameters.AddWithValue((int)Math.Min(int.MaxValue, observation.ElapsedMs));

                /* -1 is the counting reader's "not measured" sentinel and it must not be stored as a
                   measurement of minus one row. NULL is the column's own way of saying not recorded, the
                   V109 write-side rule. */
                AddNullableBigint(command, observation.RowsRead >= 0 ? observation.RowsRead : null);
                AddNullableBigint(command, observation.BytesRead >= 0 ? observation.BytesRead : null);
                AddNullableInt(command, observation.LastReadMs >= 0 ? (int)Math.Min(int.MaxValue, observation.LastReadMs) : null);
                AddNullableInt(command, connectMs);
                AddNullableInt(command, queryMs);

                AddNullableBigint(command, sample?.WaitingTaskCount);
                AddNullableInt(command, sample?.DistinctWaitTypes);
                AddNullableText(command, sample?.TopWaitType);
                AddNullableBigint(command, sample is null ? null : sample.TopWaitTotalMs);
                AddNullableBigint(command, sample is null ? null : sample.TopWaitMaxMs);
                AddNullableText(command, sample?.WaitSummary);
                AddNullableInt(command, sample?.SchedulerCount);
                AddNullableBigint(command, sample is null ? null : sample.RunnableTasks);
                AddNullableBigint(command, sample is null ? null : sample.WorkQueueLength);
                AddNullableBigint(command, sample is null ? null : sample.PendingDiskIo);
                AddNullableInt(command, sample?.MaxRunnableTasks);
                AddNullableText(command, error);

                await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
            }

            await using (var purge = new NpgsqlCommand(PurgeSql, connection))
            {
                purge.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
                purge.Parameters.AddWithValue(
                    DateTime.SpecifyKind(probeTimeUtc.AddDays(-RetentionDays), DateTimeKind.Unspecified));
                await purge.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            /* Failure-isolated exactly like the other observability writes: a diagnostic that cannot be
               persisted must never surface anywhere near the collection loop. The findings are already on
               the log line above, so the measurement is not lost outright. */
            logger?.LogDebug(
                "Stall probe row for {Collector} on '{Server}' was not stored: {Message}",
                collectorName, server.Config.DisplayName, ex.Message);
        }
    }

    private static void AddNullableInt(NpgsqlCommand command, int? value) =>
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Integer,
            Value = value.HasValue ? value.Value : DBNull.Value,
        });

    private static void AddNullableBigint(NpgsqlCommand command, long? value) =>
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Bigint,
            Value = value.HasValue ? value.Value : DBNull.Value,
        });

    private static void AddNullableText(NpgsqlCommand command, string? value) =>
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Text,
            Value = value is null ? DBNull.Value : value,
        });

    private static string Truncate(string value) =>
        value.Length <= MaxErrorLength ? value : value[..MaxErrorLength];
}

/// <summary>
/// The per-server single-flight gate for <see cref="StallWaitProbeRunner"/>, so the pool bound #2880 asks for
/// is enforced by a gate rather than inferred from the sweep's shape.
///
/// <para><b>Keyed by SERVER, not by (server, collector)</b> — deliberately the opposite choice from
/// <see cref="DetachedCollectorGate"/>, whose collectors are independent workloads that merely share a
/// target. What is being bounded here IS concurrent connections to one target, so two different collectors
/// stalling at once must not buy two probes: the second would learn nothing the first did not, against an
/// instance already struggling to produce rows.</para>
///
/// <para>Its own flag rather than a <see cref="DetachedCollectorGate"/> instance, because that type's doc
/// comment is specifically about excluding two runs of the same collector and reusing it under a different
/// key would make that documentation wrong about what it protects. Same never-blocking
/// <c>CompareExchange</c> shape, for the same reason: this gate has no waiters, ever — a refused probe is
/// simply not taken.</para>
///
/// <para>Never pruned, by design and for <see cref="DetachedCollectorGate"/>'s reason: one flag per monitored
/// server lives for the process, and a server that goes away leaves an int behind.</para>
/// </summary>
internal static class StallProbeGate
{
    private static readonly ConcurrentDictionary<int, Slot> Slots = new();

    internal static IDisposable? TryAcquire(int serverId) =>
        Slots.GetOrAdd(serverId, static _ => new Slot()).TryAcquire();

    private sealed class Slot
    {
        private int _taken;

        internal IDisposable? TryAcquire() =>
            Interlocked.CompareExchange(ref _taken, 1, 0) == 0 ? new Lease(this) : null;

        private void Release() => Volatile.Write(ref _taken, 0);

        private sealed class Lease(Slot slot) : IDisposable
        {
            private Slot? _slot = slot;

            /* Idempotent and Interlocked, DetachedCollectorGate.Lease's reasoning: a double release must
               never clear a flag a different probe has since taken. */
            public void Dispose() => Interlocked.Exchange(ref _slot, null)?.Release();
        }
    }
}
