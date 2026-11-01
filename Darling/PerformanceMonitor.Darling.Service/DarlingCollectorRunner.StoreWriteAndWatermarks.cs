/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

public sealed partial class DarlingCollectorRunner
{
    /// <summary>
    /// Writes the per-item app-log lines for probe failures, capped at
    /// <see cref="EnumeratedCollectorDriver.MaxLoggedProbeFailures"/> with the suppressed remainder
    /// reported as a count. The collection_log row already carries the summary note; this is where the
    /// actual per-database error text lands, and it is why that note says "see the app log". Lite's twin
    /// is <c>RemoteCollectorService.LogEnumerationProbeFailures</c> — same shared templates.
    ///
    /// <para>Serves BOTH channels: an enumeration's second result set (#1837) and a payload collector's
    /// trailing one (#1851). Named for the shared template it writes, which reports the failing step as
    /// an enumeration probe — accurate for both, since a payload collector reaches this only by
    /// enumerating and probing databases inside its own server-side cursor.</para>
    /// </summary>
    internal void LogEnumerationProbeFailures<TRow>(
        ICollectorDefinition<TRow> definition,
        ServerRuntime server,
        IReadOnlyList<EnumerationProbeFailure> probeFailures)
    {
        if (probeFailures.Count == 0)
        {
            return;
        }

        /* #6889: a login with no user in a database is a posture that does not change, so those rows get ONE Information
           line per run (the same ten names the SUCCESS-row note uses) and never a Warning. Every other probe failure keeps
           the capped per-item Warning below. */
        var (noAccess, others) = NoDatabaseAccessNote.Partition(probeFailures);
        if (noAccess.Count > 0)
        {
            _logger?.LogInformation(NoDatabaseAccessNote.NoAccessLogTemplate,
                definition.Name, server.Config.DisplayName, noAccess.Count, NoDatabaseAccessNote.NamedList(noAccess));
        }

        var shown = Math.Min(others.Count, EnumeratedCollectorDriver.MaxLoggedProbeFailures);
        for (var i = 0; i < shown; i++)
        {
            _logger?.LogWarning(EnumeratedCollectorDriver.ProbeFailureLogTemplate,
                definition.Name, server.Config.DisplayName, others[i].Item, others[i].Error);
        }

        if (others.Count > shown)
        {
            _logger?.LogWarning(EnumeratedCollectorDriver.ProbeFailureOverflowLogTemplate,
                definition.Name, server.Config.DisplayName, others.Count, others.Count - shown, shown);
        }
    }

    /// <summary>
    /// Writes ONE batch (one enumerated item / one database, or the whole result set for a plain
    /// collector) to Postgres as a single binary COPY on the caller's already-open connection (#1556).
    /// The three collection paths route through here so the storage logic — the prefix columns, the
    /// naive-UTC stamp, the positional payload — lives once. A batch is atomic and independent: on a
    /// mid-run abort the batches already written stay committed (commit-1..N-1). An empty batch opens
    /// no COPY and returns 0 (rows_collected = Σ non-empty batch counts).
    ///
    /// <para>Collectors that divert large text payloads into the hash-keyed dimension tables (#1767 —
    /// query_stats, procedure_stats) wrap the COPY and the dimension upsert in ONE explicit
    /// transaction, so no reader can observe a fact row whose digest has no dimension row. Every
    /// other collector keeps the pre-#1767 path exactly, where completing the importer is itself the
    /// commit.</para>
    /// </summary>
    private async Task<int> WriteBatchAsync<TRow>(
        NpgsqlConnection pgConnection,
        ICollectorDefinition<TRow> definition,
        List<TRow> rows,
        ServerRuntime server,
        DateTime collectionTime,
        CollectorContext context,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return 0;
        }

        /* #4487: job_history's failback lookback can honestly re-read up to FailbackLookbackDays of an
           epoch this server already stored — the numeric watermark alone cannot see that, because it
           re-reads a HIGHER epoch's own rows, not the current low epoch's. Read the batch's own natural
           keys back from the store ONCE, here, before the #3099 retry loop below, and filter `rows` once:
           a start-phase re-attempt below reuses this already-filtered list rather than re-reading, which
           is what keeps this read outside #3099's retry semantics entirely. Every other collector's
           definition does not implement the contract, so this is a null check and nothing else for them. */
        if (definition is INaturalKeyDedupedCollector<TRow> dedupe)
        {
            rows = await DropAlreadyStoredJobHistoryRowsAsync(
                pgConnection, definition, dedupe, rows, server, cancellationToken);

            if (rows.Count == 0)
            {
                return 0;
            }
        }

        /* A deadlock both of an Azure master registration's reads return is stored once: read the batch's
           own identities (time and full graph text) back from the store ONCE, here, before the #3099 retry
           loop below, and keep only the rows it does not already hold. A start-phase re-attempt reuses this
           filtered list. Every other collector's definition skips this null check. */
        if (definition is IStoredIdentityDedupedCollector<TRow> identityDedupe)
        {
            rows = await DropAlreadyStoredIdentityRowsAsync(
                pgConnection, identityDedupe, rows, server, cancellationToken);

            if (rows.Count == 0)
            {
                return 0;
            }
        }

        /* #3099: ONE re-attempt, gated on the COPY's START phase, and lossless because `rows` is still
           the parameter this method was handed. The gate is what makes it lossless rather than merely
           cheap: a start-phase fault sent no row, so a COPY ... FROM STDIN cannot have committed and the
           store is byte-identical, and it ran no WritePayload, so no counter-moved row's delta baseline moved (procedure_stats'
           idle skip advances only UNCHANGED counters at read time, #5449, which is lossless). Both are
           forfeit past that point — see CopyBatchOnceAsync, which stamps the phase and carries the
           argument. StoreWriteReattempt.IsSafeToReattempt requires Start positively, so an unstamped
           fault and a data-phase fault both decline and cost a sample rather than authorising a duplicate
           or a fabricated zero.

           What it recovers: a start-phase transport fault used to cost the whole cycle. Collectors with a
           WatermarkColumn re-read the same range next cycle and lose nothing, but the base default in
           CollectorDefinitionBase is `WatermarkColumn => null` — those are cumulative-counter snapshots
           with no way to ask for that instant again, so the sample is simply gone.

           The re-attempt takes a FRESH connection, and that is the mechanism rather than any wait: the first
           attempt's connector is dead — the same reasoning DarlingManagedPostgres' post-start loop records —
           so a second attempt on the caller's handle would fail on the protocol rather than on the store.
           There is deliberately no delay between them. The sweep permit and the caller's borrowed store
           connection are both held for the duration, so a pause long enough to outlast the store's own
           hourly continuous-aggregate refresh (hundreds of seconds) would hold both for minutes, while any
           pause short enough to be safe is noise against that window: no delay is purchasable here. The
           accepted cost is that a write's worst case is two command deadlines rather than one.

           The FIRST attempt also goes to a fresh connection when the caller's is no longer open, and that
           is not tidiness. The caller's connection is shared across a fan-out's batches (#2819 has the
           Query Store plan and text fetches borrowing the same one), and a transport fault breaks it — so
           every batch after the first faulting one inherits a connection that cannot be used, and what it
           throws then need not carry a phase stamp at all, and an unstamped fault declines. Deciding on the
           connection's own STATE rather than on the shape or stamping of a later exception keeps those
           batches out of that question entirely. plan_correction is the collector that makes it matter: it enumerates on
           every non-Azure target and declares no WatermarkColumn, so it both fans out and cannot re-read a
           sample it lost. The cost is that a faulted cycle's remaining batches hold two store connections
           at a time on that server rather than one, for the rest of the cycle. */
        var outcome = await StoreWriteReattempt.RunAsync(
            write: async token => pgConnection.State == ConnectionState.Open
                ? await CopyBatchOnceAsync(
                    pgConnection, definition, rows, server, collectionTime, context, token)
                : await CopyOnAFreshConnectionAsync(
                    definition, rows, server, collectionTime, context, token),
            rewrite: token => CopyOnAFreshConnectionAsync(
                definition, rows, server, collectionTime, context, token),
            onReattempt: firstAttempt => _logger?.LogWarning(
                firstAttempt,
                StoreWriteReattemptLogTemplate,
                definition.Name, server.Config.DisplayName, rows.Count, firstAttempt.Message),
            cancellationToken);

        if (outcome.Reattempted)
        {
            /* Incremented only once the rows are actually stored: a re-attempt that also fails throws out
               of RunAsync into RunOneAsync's fault arms, which record ERROR and never read this. So the
               count means "stored on the second attempt", never "tried twice". */
            context.StoreWriteReattempts++;
        }

        /* #3392: record the over-cap plans this batch saw. AFTER the rows are stored, deliberately — a write
           that threw never reaches here, so the backlog cannot end up claiming a sighting for a cycle whose
           rows were lost. */
        await RecordOversizedPlanSightingsAsync(definition, rows, server, collectionTime, cancellationToken);

        /* #5985: the query keys and callers this batch saw, for the tag map. Same placement and reason as above. */
        await RecordQueryTagSightingsAsync(definition, rows, server, cancellationToken);

        return outcome.RowsWritten;
    }

    /// <summary>
    /// The keyed pre-insert dedupe (#4487): reads the batch's own natural keys back from
    /// <c>collect.job_history</c> (the only table this runs against — <c>definition.TargetTable</c> is
    /// job_history's, since only <see cref="JobHistoryCollector"/> implements
    /// <see cref="INaturalKeyDedupedCollector{TRow}"/>) and drops any row this server already stored under
    /// either the current epoch or a returning one, before the COPY.
    ///
    /// <para><b>The read.</b> A keyed join against store rung V150's
    /// <c>idx_job_history_server_run (server_id, run_datetime DESC, instance_id DESC)</c> — confirmed with
    /// EXPLAIN in the PR's cost check — bounded on <c>collection_time &gt;= batch's MIN(run_datetime) - 1
    /// day</c> for chunk exclusion. That bound is on <c>collection_time</c> (the hypertable's partitioning
    /// column, UTC) using <c>run_datetime</c> (the TARGET's own local wall clock) as its anchor: the two
    /// clocks are within ±14h of each other (the widest real UTC offset), and a row cannot be collected
    /// before the run it describes started, so one full day of slack on top of that is safe margin in
    /// either direction without needing to reason about the exact offset.</para>
    ///
    /// <para><b>Once per batch, not once per retry.</b> Called from <see cref="WriteBatchAsync{TRow}"/>
    /// BEFORE the #3099 retry loop begins, so a start-phase re-attempt reuses the already-filtered list —
    /// re-reading on retry would be wasted work at best, and at worst could re-admit a row a slower first
    /// attempt had already committed between the two reads.</para>
    /// </summary>
    private async Task<List<TRow>> DropAlreadyStoredJobHistoryRowsAsync<TRow>(
        NpgsqlConnection pgConnection,
        ICollectorDefinition<TRow> definition,
        INaturalKeyDedupedCollector<TRow> dedupe,
        List<TRow> rows,
        ServerRuntime server,
        CancellationToken cancellationToken)
    {
        var runDateTimes = new DateTime[rows.Count];
        var instanceIds = new long[rows.Count];
        var jobIds = new string[rows.Count];
        var stepIds = new int[rows.Count];
        var minRunDateTime = DateTime.MaxValue;

        for (var i = 0; i < rows.Count; i++)
        {
            var key = dedupe.GetNaturalKey(rows[i]);
            instanceIds[i] = key.InstanceId;
            jobIds[i] = key.JobId;
            stepIds[i] = key.StepId;
            runDateTimes[i] = DateTime.SpecifyKind(key.RunDateTime, DateTimeKind.Unspecified);

            if (key.RunDateTime < minRunDateTime)
            {
                minRunDateTime = key.RunDateTime;
            }
        }

        var collectionTimeFloor = DateTime.SpecifyKind(minRunDateTime.AddDays(-1), DateTimeKind.Unspecified);

        var storedKeys = new HashSet<(long InstanceId, string JobId, int StepId, DateTime RunDateTime)>();

        try
        {
            await using var command = new NpgsqlCommand(
                $"SELECT jh.instance_id, jh.job_id, jh.step_id, jh.run_datetime " +
                $"FROM {definition.TargetTable} AS jh " +
                "JOIN unnest($2::timestamp[], $3::bigint[], $4::text[], $5::integer[]) " +
                "AS k(run_datetime, instance_id, job_id, step_id) " +
                "ON jh.run_datetime = k.run_datetime AND jh.instance_id = k.instance_id " +
                "AND jh.job_id = k.job_id AND jh.step_id = k.step_id " +
                "WHERE jh.server_id = $1 AND jh.collection_time >= $6",
                pgConnection);
            command.CommandTimeout = CommandTimeoutSeconds;
            command.Parameters.AddWithValue(server.ServerId);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = runDateTimes });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = instanceIds });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = jobIds });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = stepIds });
            command.Parameters.AddWithValue(collectionTimeFloor);

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                storedKeys.Add((
                    reader.GetInt64(0),
                    reader.GetString(1),
                    reader.GetInt32(2),
                    JobHistoryCollector.ToMicroseconds(reader.GetDateTime(3))));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* A failed dedupe read must never turn into a silent duplicate write: fail the batch rather
               than store rows this server might already hold. The #3099 retry above still applies to
               whatever failure the store write itself produces; this read is not part of that retry. */
            _logger?.LogWarning(
                "job_history pre-insert dedupe read failed for server {ServerId} — failing this batch " +
                "rather than risk storing duplicate rows: {Message}",
                server.ServerId, ex.Message);
            throw;
        }

        return dedupe.DropAlreadyStored(rows, storedKeys);
    }

    /// <summary>
    /// The stored graphs of one server at a set of event times, bounded on <c>collection_time</c> (the
    /// hypertable's partitioning column) for chunk exclusion. A null graph is never an identity.
    /// </summary>
    internal const string StoredDeadlockIdentitySql =
        "SELECT deadlock_time, " + DeadlocksCollector.StoredGraphIdentitySql + " FROM deadlocks " +
        "WHERE server_id = $1 AND deadlock_graph_xml IS NOT NULL AND deadlock_graph_xml <> '' " +
        "AND deadlock_time = ANY($2::timestamp[]) AND collection_time >= $3";

    /// <summary>
    /// The exact-identity pre-insert dedupe: reads the stored graphs of this server at the batch's own
    /// identity times (one query) and drops any row whose microsecond time and full graph text a stored row
    /// already carries, before the COPY. The <c>collection_time</c> floor is the earliest batch event time
    /// minus one day: a deadlock cannot be collected before it happened, and the day absorbs the gap
    /// between the target's clock and the store's. Called from <see cref="WriteBatchAsync{TRow}"/> BEFORE
    /// the #3099 retry loop, so a start-phase re-attempt reuses the filtered list. A failed read fails the
    /// batch rather than risk a duplicate.
    /// </summary>
    private async Task<List<TRow>> DropAlreadyStoredIdentityRowsAsync<TRow>(
        NpgsqlConnection pgConnection,
        IStoredIdentityDedupedCollector<TRow> dedupe,
        List<TRow> rows,
        ServerRuntime server,
        CancellationToken cancellationToken)
    {
        var times = rows.Select(dedupe.GetIdentity)
            .Where(identity => identity is not null)
            .Select(identity => DateTime.SpecifyKind(identity!.Value.Time, DateTimeKind.Unspecified))
            .Distinct()
            .ToArray();

        if (times.Length == 0)
        {
            return rows;
        }

        var collectionTimeFloor = DateTime.SpecifyKind(times.Min().AddDays(-1), DateTimeKind.Unspecified);
        var stored = new HashSet<(DateTime Time, string Graph)>();

        try
        {
            await using var command = new NpgsqlCommand(StoredDeadlockIdentitySql, pgConnection);
            command.CommandTimeout = CommandTimeoutSeconds;
            command.Parameters.AddWithValue(server.ServerId);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = times });
            command.Parameters.AddWithValue(collectionTimeFloor);

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                stored.Add((JobHistoryCollector.ToMicroseconds(reader.GetDateTime(0)), reader.GetString(1)));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogWarning(
                "Deadlock pre-insert dedupe read failed for server {ServerId} — failing this batch " +
                "rather than risk storing a duplicate: {Message}",
                server.ServerId, ex.Message);
            throw;
        }

        return dedupe.DropAlreadyStored(rows, stored);
    }

    /// <summary>
    /// Records this batch's over-cap cached plans in <c>collect.oversized_plan_backlog</c> so the
    /// low-frequency sweep can go back for their content (#3392).
    ///
    /// <para><b>The definition decides, not this method.</b> <c>DescribeOversizedPlan</c> is what knows the
    /// plan fetch's own arguments — statement offsets for <c>query_stats</c>, module-grain literals for
    /// <c>procedure_stats</c> — and the deferred fetch is only the same call if it passes the same ones.
    /// Every other collector returns null here and the loop costs one virtual call per row.</para>
    ///
    /// <para><b>Nothing over the cap means no connection at all.</b> The list is not even allocated until a
    /// row describes an observation, so the overwhelmingly common batch pays a null check. The store write
    /// itself is failure-isolated inside <see cref="OversizedPlanBacklog.RecordSightingsAsync"/>: a backlog
    /// that cannot be written is a plan we will not go back for, never a reason to fail a cycle that stored
    /// every row it read.</para>
    /// </summary>
    private async Task RecordOversizedPlanSightingsAsync<TRow>(
        ICollectorDefinition<TRow> definition,
        List<TRow> rows,
        ServerRuntime server,
        DateTime collectionTime,
        CancellationToken cancellationToken)
    {
        List<OversizedPlanObservation>? observations = null;

        foreach (var row in rows)
        {
            if (definition.DescribeOversizedPlan(row) is { } observation)
            {
                (observations ??= new List<OversizedPlanObservation>()).Add(observation);
            }
        }

        if (observations is null)
        {
            return;
        }

        await OversizedPlanBacklog.RecordSightingsAsync(
            _postgres, server.ServerId, definition.Name, observations, collectionTime, _logger, cancellationToken);
    }

    /// <summary>
    /// Buffers the query keys and callers this batch saw, which the tag map step flushes to
    /// <c>collect.query_tag_sightings</c> and resolves (#5985, part of #5971). The definition decides what a row saw
    /// (<c>DescribeQueryTagSightings</c>); this only gathers them into <see cref="QueryTagSightings"/>, in memory: unlike the
    /// oversized-plan sighting beside it, this runs for nearly every batch, so it makes no store round trip. A batch with no
    /// sightings allocates nothing. The one time the append touches the store is a buffer at its cap flushing, and that flush
    /// never throws, so it cannot fail the cycle.
    /// </summary>
    private async Task RecordQueryTagSightingsAsync<TRow>(
        ICollectorDefinition<TRow> definition,
        List<TRow> rows,
        ServerRuntime server,
        CancellationToken cancellationToken)
    {
        List<QueryTagSighting>? sightings = null;

        foreach (var row in rows)
        {
            if (definition.DescribeQueryTagSightings(row) is { Count: > 0 } described)
            {
                (sightings ??= new List<QueryTagSighting>()).AddRange(described);
            }
        }

        if (sightings is null)
        {
            return;
        }

        await QueryTagSightings.AppendAsync(server.ServerId, sightings, cancellationToken);
    }

    /// <summary>
    /// The template for the re-attempt's log line (#3099). A named constant so a pin can assert the shipped
    /// string, and Warning rather than Debug because a store that drops collector writes is a finding even
    /// when the row is recovered — the level is what keeps it on the default filter, where the log-based
    /// measurement that found this reads.
    /// </summary>
    internal const string StoreWriteReattemptLogTemplate =
        "{Collector} on '{Server}': the store write of {Rows} row(s) failed on a transport fault " +
        "({Message}) — re-attempting once on a fresh store connection (#3099). The batch is still in " +
        "memory and nothing was committed, so a successful re-attempt costs no sample.";

    /// <summary>
    /// The collection_log note for a cycle that stored its rows only after a re-attempt (#3099). <c>{0}</c>
    /// = how many of the cycle's writes needed one.
    ///
    /// <para><b>The status stays SUCCESS, and the note is what makes the re-attempt visible.</b> A new
    /// status value would be read as a failure by every consumer of
    /// <c>status IN ('SUCCESS', 'SKIPPED')</c> — seven query clauses across five files: Lite's
    /// collection-health read, the self-alert evaluator's <c>last_success</c> and <c>recent_success</c>, both
    /// MCP readers, and the viewer's two — and would suppress <c>last_success</c> for a cycle that stored every row and
    /// advanced its watermark. That is #2673's defect with the sign flipped, and the same reasoning that
    /// put the whole-cycle-budget message in this channel rather than inventing a sixth status.</para>
    ///
    /// <para>Recording it at all is not decoration. Before the re-attempt a transport fault wrote a
    /// collection_log ERROR row, and those rows are the measurement behind #3099's own in-window versus
    /// out-window error-rate ratio. A silent retry would remove the sample loss AND the only instrument
    /// that can check whether the association it was diagnosed from is real.</para>
    /// </summary>
    internal const string StoreWriteReattemptNoteFormat =
        "{0} store write(s) this cycle stored their rows only on a re-attempt after a transport fault " +
        "(#3099); no rows were lost";

    /// <summary><see cref="StoreWriteReattemptNoteFormat"/> parsed once (CA1863).</summary>
    private static readonly CompositeFormat s_storeWriteReattemptNote =
        CompositeFormat.Parse(StoreWriteReattemptNoteFormat);

    /// <summary>
    /// The note for this cycle's store-write re-attempts, or null when none of them needed one — the shape
    /// every other note producer here takes, so <see cref="EnumeratedCollectorDriver.MergeNotes"/> composes
    /// it with the probe-failure and partial-failure channels without a special case.
    /// </summary>
    internal static string? StoreWriteReattemptNote(int reattempts) =>
        reattempts <= 0
            ? null
            : string.Format(CultureInfo.InvariantCulture, s_storeWriteReattemptNote, reattempts);

    /// <summary>
    /// The collection_log note for a cycle whose Query Store batches stored their raw rows but missed the interval
    /// table (#3953), in <see cref="StoreWriteReattemptNoteFormat"/>'s shape. Raw was not lost; the batches are queued
    /// for replay, and the note is what keeps a failing apply visible on a cycle that otherwise reads as a success.
    /// </summary>
    internal const string QueryStoreIntervalMissNoteFormat =
        "{0} Query Store batch(es) this cycle stored their raw rows but missed the interval table (#3953); they are " +
        "queued for replay, and PLAN_REGRESSION reads raw for this server until they apply";

    /// <summary><see cref="QueryStoreIntervalMissNoteFormat"/> parsed once (CA1863).</summary>
    private static readonly CompositeFormat s_queryStoreIntervalMissNote =
        CompositeFormat.Parse(QueryStoreIntervalMissNoteFormat);

    /// <summary>The note for this cycle's interval-table misses, or null when there were none.</summary>
    internal static string? QueryStoreIntervalMissNote(int misses) =>
        misses <= 0
            ? null
            : string.Format(CultureInfo.InvariantCulture, s_queryStoreIntervalMissNote, misses);

    /// <summary>
    /// One attempt at the batch on a store connection of its own, borrowed for the attempt and returned
    /// when it ends (#3099). Both the re-attempt and a first attempt whose shared connection is already
    /// broken route through here, so "a fresh connection" is one named method with one body rather than two
    /// copies that could drift.
    /// </summary>
    private async Task<int> CopyOnAFreshConnectionAsync<TRow>(
        ICollectorDefinition<TRow> definition,
        List<TRow> rows,
        ServerRuntime server,
        DateTime collectionTime,
        CollectorContext context,
        CancellationToken cancellationToken)
    {
        await using var freshConnection = await _postgres.OpenConnectionAsync(cancellationToken);
        return await CopyBatchOnceAsync(
            freshConnection, definition, rows, server, collectionTime, context, cancellationToken);
    }

    /// <summary>
    /// ONE attempt at the batch: the binary COPY, and for the diverting collectors the transaction that
    /// wraps it and its dimension flush (for query_stats, also the hour ledger's count, #4605, which commits
    /// with the rows or not at all). Separate from <see cref="WriteBatchAsync"/> so the importer and
    /// the transaction are both disposed before the re-attempt runs — a retry that fired while the failed
    /// attempt's transaction was still in scope would re-enter with an aborted transaction on the
    /// connection and fail on 25P02 rather than on anything to do with the store.
    ///
    /// <para><b>The phase this method stamps is what makes a re-attempt sound, and only the start phase
    /// is.</b> #3095's <c>StoreCopyPhase</c> transition sits inside the COPY block below, so
    /// <c>Start</c> means strictly "the importer never came back": no row started, so a
    /// <c>COPY ... FROM STDIN</c> cannot have committed, and <c>WritePayload</c> never ran, so no
    /// <c>CollectorDeltaCalculator</c> baseline moved for a row whose counters moved (procedure_stats advances an
    /// UNCHANGED row's baseline at read time to drop it as idle, and a row kept with its deltas computed carries them;
    /// neither can be re-derived wrongly, #5449). <see cref="StoreWriteReattempt.IsSafeToReattempt"/>
    /// requires that value positively.</para>
    ///
    /// <para><b>Both properties are why the row loop below must never be re-run.</b> Past the transition a
    /// fault may have sent rows and may have lost a commit acknowledgment in flight, and every baseline
    /// the loop reached has advanced — so a second pass would re-derive each delta against its own new
    /// baseline and write a zero. A zero delta reads as a genuinely idle interval, carries no error and
    /// never self-corrects, which is strictly worse than the lost sample the re-attempt exists to prevent.
    /// <c>WritePayload</c> is not a pure function of its row, and that is the reason.</para>
    /// </summary>
    private async Task<int> CopyBatchOnceAsync<TRow>(
        NpgsqlConnection pgConnection,
        ICollectorDefinition<TRow> definition,
        List<TRow> rows,
        ServerRuntime server,
        DateTime collectionTime,
        CollectorContext context,
        CancellationToken cancellationToken)
    {
        var rowsWritten = 0;
        var writer = new PgCollectorRowWriter();

        /* #1767: which payload columns (if any) store a content digest and send their text to a
           dimension table instead of inline onto every row. Derived from the same schema
           CopyCommandFor derives its column list from, so the two cannot disagree. */
        var diversionPlan = PayloadDimensions.DiversionPlanFor(definition);
        var dimensions = new PayloadDimensionBatch();
        if (diversionPlan.Count > 0)
        {
            writer.UseDimensions(diversionPlan, dimensions);
        }

        /* #4605: a query_stats batch also adds its non-restart row count (rows written with a sample_interval_seconds
           other than 0, a NULL included) to the hourly ledger, in the COPY's own transaction below. The count is the
           writer's tally of the value it sends into that column, taken at the exact write and never re-derived. The
           writer is built per attempt, so the tally starts from zero on every attempt and a failed attempt's rows
           cannot be counted twice by the re-attempt. All rows of the batch carry the one storedCollectionTime below,
           so a batch is one hour and one upsert.

           The batch is a ledger batch by the table this COPY writes, not by the definition's CLR type: a definition
           aimed at query_stats that is not the catalog's QueryStatsCollector (a one-off import or backfill) is counted
           too, or fails loudly. IntervalPayloadIndex throws for a definition without the column, and the tally check
           before the COPY completes catches any other overload. */
        var ledgerBatch = string.Equals(definition.TargetTable, QueryStatsCollector.Instance.TargetTable, StringComparison.OrdinalIgnoreCase);
        if (ledgerBatch)
        {
            writer.CountNonZeroAt(QueryStatsHourLedgerWriter.IntervalPayloadIndex(definition));
        }

        /* #5684: a query_store_stats batch also writes its per-database watermark rows (QueryStoreWatermarks, V178), in the COPY's
           own transaction below behind its own savepoint. Chosen by the table this COPY writes, the way ledgerBatch is, so a definition
           aimed at query_store_stats that is not the catalog's QueryStoreCollector (an import or the backfill) opens the transaction too.
           Only QueryStoreCollector.Row rows are summarised (OfType below), so a definition that writes another row type to
           query_store_stats writes no side row, and the reader then reads raw (safe).
           Unlike the ledger, a fault never fails the batch: a missing side row only LOWERS what a reader sees. */
        var watermarkBatch = string.Equals(definition.TargetTable, QueryStoreCollector.Instance.TargetTable, StringComparison.OrdinalIgnoreCase);

        /* #5638: a procedure_stats batch also brings the per-collection totals up to date (ProcedureStatsTotalsWriter), in
           the COPY's own transaction below. Like the ledger, it is chosen by the table this COPY writes, so a definition aimed
           at procedure_stats that is not the catalog's ProcedureStatsCollector (an import or backfill) writes its totals too. */
        var totalsBatch = string.Equals(definition.TargetTable, ProcedureStatsCollector.Instance.TargetTable, StringComparison.OrdinalIgnoreCase);

        /* #3953: a Query Store batch also maintains the latest-snapshot-per-interval table, in the COPY's own
           transaction behind a savepoint (QueryStoreIntervalLatest). Its two wider reads, the coverage row's
           creation and the hourly gap check, run HERE, before the transaction, so nothing inside it reads raw
           except by this batch's collection_time. Neither can block the COPY: a fault just records the batch
           for replay. */
        var queryStoreDatabases = definition is QueryStoreCollector
            ? DistinctQueryStoreDatabases(rows)
            : null;
        var queryStorePrepared = queryStoreDatabases is not null
            && await _queryStoreIntervalLatest.PrepareServerAsync(
                pgConnection, server.ServerId, ServiceCommandDeadlines.CollectionSweepSeconds, cancellationToken);

        /* #3953 (V145): the wide interval table's own prepare, independent of V143's above — its own coverage row
           and its own hourly gap check, on its own claim. A fault here only skips THIS table's apply. */
        var queryStoreWidePrepared = queryStoreDatabases is not null
            && await _queryStoreIntervalWide.PrepareServerAsync(
                pgConnection, server.ServerId, ServiceCommandDeadlines.CollectionSweepSeconds, cancellationToken);

        /* #4659: a Query Store batch tells the store-write fence it is about to change query_store_stats BEFORE
           its transaction opens, and that it is done in a finally AFTER the commit (or after a rollback, a throw,
           or a cancel). This is the one place every Query Store row is written, so the live fan-out (one
           transaction per database under one collection_time), overlapped runs, the re-attempt on a fresh
           connection and the backfill are all covered here and nowhere else. */
        var fencedServerId = queryStoreDatabases is not null ? server.ServerId : (int?)null;
        if (fencedServerId is { } beginServerId)
        {
            _queryStoreWriteFence?.BeginWrite(beginServerId, queryStoreDatabases);
        }

        var fenceSucceeded = false;
        try
        {
            /* Only the diverting collectors, Query Store (#3953) and the ledger's query_stats batches (#4605) need a
               transaction; everything else keeps the pre-#1767 single-COPY commit and pays nothing. query_stats diverts
               its text and plans today, so the ledger term does not add a transaction; it is named so that the ledger
               upsert can never be left without the transaction its atomicity with the COPY depends on. */
            await using var transaction = diversionPlan.Count > 0 || queryStoreDatabases is not null || ledgerBatch || totalsBatch || watermarkBatch
                ? await pgConnection.BeginTransactionAsync(cancellationToken)
                : null;

            /* Naive-UTC storage — see PgCollectorRowWriter. */
            var storedCollectionTime = DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified);

            /* The start phase's deadline. It is separate from the importer's because it has to be: the
               importer does not exist until Begin returns, and the await that returns it runs under the
               connection's CommandTimeout, which Npgsql exposes read-only. StoreCopyStartDeadline carries the
               value, the stop-versus-deadline discrimination and the fault shape; the two lines it costs here
               are the token below and the arm that translates a breach. */
            using var startDeadline = StoreCopyStartDeadline.Start(cancellationToken);

            /* #3095: which COPY phase a fault came out of, stamped onto the exception by the arm below so a
               handler upstream can tell a start-phase failure from a data-phase one. Both render as
               "Exception while reading from stream", so nothing about the exception itself carries this.

               Start until Begin returns, and the transition sits INSIDE the block for that reason: Start has to
               mean strictly "the importer never came back", because that is the state whose two properties a
               consumer relies on — no row started, so a COPY ... FROM STDIN cannot have committed, and no
               delta baseline moved for a row whose counters moved, since CollectorDeltaCalculator advances inside
               WritePayload below (procedure_stats' idle skip advances unchanged counters at read time: lossless, #5449). A
               fault anywhere past this line forfeits both, so it must read as Data even where it happens to
               have sent nothing. The unsafe mislabel is the one that would report Start for a fault that had
               already sent rows; this ordering makes that unreachable rather than unlikely.

               The dimension flush and commit after the block are deliberately left unstamped: they are not the
               COPY, and Unknown is the honest answer for them. */
            var copyPhase = StoreCopyPhase.Start;

            try
            {
                using (var importer = await pgConnection.BeginBinaryImportAsync(
                    PgCollectorRowWriter.CopyCommandFor(definition), startDeadline.Token))
                {
                    copyPhase = StoreCopyPhase.Data;

                    /* #2874: the COPY's own deadline — and a DIFFERENT property from every other site in this
                       regime. NpgsqlBinaryImporter.Timeout is a TimeSpan on the importer, not the int seconds of
                       NpgsqlCommand.CommandTimeout, and there is neither an NpgsqlCommand nor a CreateCommand here,
                       so no regex written for either command shape can see this site. Left unset it is initialised
                       from the connection's CommandTimeout, so this write — PgCollectorRowWriter.CopyCommandFor over
                       every collector, every server, every cycle — inherited the same undocumented 30 s default as
                       the rest of the regime while being invisible to every pin that closed them.

                       It takes the SAME constant because it is the same regime: no enclosing budget, one sweep permit
                       and one borrowed store connection held for the duration, retried on the collector's own cadence.

                       It reaches ONE of the COPY's two phases, and that is a property of the API rather than a
                       choice. Measured against Npgsql 10.0.3: this bounds StartRowAsync / Write / CompleteAsync,
                       and nothing above them — the importer does not exist until Begin has returned. The await
                       that returns it is bounded by startDeadline above, which is why the two lines are separate
                       and why both take the same constant. */
                    importer.Timeout = TimeSpan.FromSeconds(ServiceCommandDeadlines.CollectionSweepSeconds);

                    writer.Importer = importer;

                    foreach (var row in rows)
                    {
                        await importer.StartRowAsync(cancellationToken);

                        if (definition.IncludesCollectionId)
                        {
                            writer.Value(CollectionIdGenerator.Next());
                        }

                        writer.Value(storedCollectionTime)
                              .Value(server.ServerId)
                              .Value(server.StorageName);

                        writer.BeginPayload();
                        definition.WritePayload(row, writer, context);
                        writer.EndPayload(definition.PayloadColumns.Count);
                        rowsWritten++;
                    }

                    /* #4605: every row must have written the interval as an integer for the tally to be the batch's
                       count. A definition that moved the column to another overload would leave rows untallied, and an
                       undercounted ledger is silent: fail the batch here instead, before the COPY completes. */
                    if (ledgerBatch)
                    {
                        QueryStatsHourLedgerWriter.EnsureEveryRowTallied(definition.Name, rowsWritten, writer.CountedWrites);
                    }

                    await importer.CompleteAsync(cancellationToken);
                }
            }
            /* The start phase's deadline, re-raised as the shape a client-side deadline has here. Npgsql
               cancels Begin with an OperationCanceledException, and on this path that word is reserved for the
               service stopping — the arm below excludes it from the phase stamp, and StoreWriteReattempt
               refuses to re-attempt through one. Left in that shape a breach would be bounded and invisible.

               The phase term in the filter is what makes this arm unable to lie, and it is not redundant with
               Breached(). A throw from a catch arm leaves the whole try, so this fault is stamped HERE rather
               than by the arm below — and Start is what the re-attempt gate acts on. Requiring copyPhase to
               still hold its initial value makes the arm unreachable once the row loop has begun, whatever
               Npgsql chooses to throw from inside it, so a data-phase fault cannot be relabelled as the
               recoverable one. The stamp still reads the variable rather than naming a phase. */
            catch (OperationCanceledException cancellation)
                when (copyPhase == StoreCopyPhase.Start && startDeadline.Breached())
            {
                var breach = StoreCopyStartDeadline.Breach(cancellation);
                CollectorFaultCopyPhase.Stamp(breach, copyPhase);
                throw breach;
            }
            /* Stamped, then rethrown bare: the fault keeps its own type, message and stack, so every
               classification arm upstream — PostgresTargetProvider.Classify, the reconnect decision in
               DarlingWorker's general handler, and any predicate walking the inner chain for a transport
               fault — sees exactly what it sees today. The phase rides alongside as an independent axis.

               OperationCanceledException is excluded because a stopping token is not a COPY phase: it says the
               service is shutting down, not which protocol exchange was in flight, and a consumer must not be
               able to read a shutdown as a recoverable start-phase stall. */
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                CollectorFaultCopyPhase.Stamp(ex, copyPhase);
                throw;
            }

            if (transaction is not null)
            {
                if (diversionPlan.Count > 0)
                {
                    var absentDigests = await PayloadDimensionWriter.FlushAsync(
                        pgConnection, transaction, dimensions, storedCollectionTime, cancellationToken,
                        compressPlanContent: _compressPlanContent());

                    /* #5158: digests this batch referenced without content that the store holds no plan for. The host
                       forgets them so the next sighting renders the plan again. FlushAsync reports upper-case hex and
                       Evict compares ignoring case. */
                    if (absentDigests.Count > 0
                        && definition is QueryStatsCollector
                        && _queryStatsPlanCaches.TryGetValue(server.ServerId, out var absentCache))
                    {
                        absentCache.Evict(absentDigests);
                    }

                    if (absentDigests.Count > 0
                        && definition is ProcedureStatsCollector
                        && _procedureStatsPlanCaches.TryGetValue(server.ServerId, out var absentProcedureCache))
                    {
                        absentProcedureCache.Evict(absentDigests);
                    }
                }

                /* #4605: the batch's count joins the hourly ledger on THIS transaction, after the COPY and the dimension
                   flush and before the commit, so raw and the ledger commit together or not at all. No savepoint and no
                   catch, deliberately: a fault here throws out of the batch, the transaction rolls back and the batch's
                   rows are not stored, because rows the ledger never counted would make it smaller than the rollup. It
                   sits in the unstamped region (#3095), so it is never re-attempted; the collector's next cycle is the
                   retry. A count of 0 (a batch of restart rows only) writes nothing: the rollup has no row for that hour. */
                if (ledgerBatch)
                {
                    await QueryStatsHourLedgerWriter.AddBatchAsync(
                        pgConnection, transaction, server.ServerId, server.StorageName, storedCollectionTime,
                        writer.NonZeroCounted, ServiceCommandDeadlines.CollectionSweepSeconds, cancellationToken);
                }

                /* #6009: a watched module's modify_date, onto the rows just copied, on this transaction behind its own savepoint.
                   Nothing runs unless a row carries a date, which is a batch of a server with a watched module. */
                if (totalsBatch)
                {
                    await ProcedureStatsModuleDates.StampAsync(
                        pgConnection, transaction, server.ServerId, storedCollectionTime,
                        rows.OfType<ProcedureStatsCollector.Row>(), ServiceCommandDeadlines.CollectionSweepSeconds, _logger, cancellationToken);
                }

                /* #5638: the collection's per-database totals, read back from the rows the store now holds for this
                   collection_time and upserted, on THIS transaction after the COPY and before the commit, so rows and totals
                   commit together or not at all. No savepoint and no catch HERE, like the ledger above: a fault in the upsert
                   throws out of the batch, the transaction rolls back and the rows are not stored, because rows the totals never
                   saw would make a read of the totals disagree with the rows. The one exception is inside the writer: the gap
                   fill of collections an older binary left behind runs behind its own savepoint and statement_timeout, and its
                   failure is logged and skipped, so it can never stop a server's collection. Recomputed, never added, so a
                   second batch at this collection_time or a re-attempt is exact. A batch of 0 rows writes nothing: the read
                   keeps its collection_log axis for an idle run. */
                if (totalsBatch)
                {
                    await ProcedureStatsTotalsWriter.UpsertBatchAsync(
                        pgConnection, transaction, server.ServerId, storedCollectionTime,
                        rowsWritten, ServiceCommandDeadlines.CollectionSweepSeconds,
                        /* The databases THIS batch holds (one per batch on Azure SQL Database): the read-back is limited to them. Null
                           (rows of another type, an import) reads the whole collection. DatabaseNamesOf matches any
                           IEnumerable<ProcedureStatsCollector.Row>. */
                        ProcedureStatsTotalsWriter.DatabaseNamesOf(rows),
                        cancellationToken,
                        _logger, server.StorageName);
                }

                /* #3953: after the COPY and the dimension flush, in the unstamped region (#3095): a fault here is not
                   the COPY's and is never re-attempted. The apply cannot fail the batch: it rolls back to its own
                   savepoint and records the batch for replay, so raw commits either way. */
                if (queryStoreDatabases is not null)
                {
                    var applied = await _queryStoreIntervalLatest.ApplyBatchAsync(
                        pgConnection, transaction, server.ServerId, storedCollectionTime, queryStoreDatabases,
                        skipApply: !queryStorePrepared, ServiceCommandDeadlines.CollectionSweepSeconds, cancellationToken);
                    if (applied == QueryStoreIntervalLatest.ApplyResult.RecordedAsPending)
                    {
                        context.QueryStoreIntervalMisses++;
                    }

                    /* #3953 (V145): the wide table's apply, under its OWN savepoint (QueryStoreIntervalWide.SavepointName),
                       beside V143's above. A fault here rolls back only to that savepoint: V143's apply just above,
                       already released from its own savepoint, is untouched, and raw still commits either way. */
                    await _queryStoreIntervalWide.ApplyBatchAsync(
                        pgConnection, transaction, server.ServerId, storedCollectionTime, queryStoreDatabases,
                        skipApply: !queryStoreWidePrepared, ServiceCommandDeadlines.CollectionSweepSeconds, cancellationToken);
                }

                /* #5684: the batch's per-database watermark rows, after the COPY and the interval applies, on THIS transaction behind
                   QueryStoreWatermarks.SavepointName. A fault rolls back to the savepoint and is logged; raw commits either way, and
                   the missing side row can only lower a reader's answer (a pair with no side row above the floor reads raw). The rows
                   are summarised in memory per database: the COPY above is a straight binary import, so every row of the batch is in
                   raw once this transaction commits, and no read-back of the shared collection_time is needed (#5707). A batch of 0
                   rows writes nothing. Only QueryStoreCollector.Row rows are summarised; any other row type writes no side row (safe, as above). */
                if (watermarkBatch && rowsWritten > 0)
                {
                    await QueryStoreWatermarks.TryUpsertBatchAsync(
                        pgConnection, transaction, server.ServerId, storedCollectionTime,
                        rows.OfType<QueryStoreCollector.Row>().Select(static r => new QueryStoreWatermarks.BatchRow(r.DatabaseName, r.QueryId, r.PlanId, r.LastExecutionTime)),
                        ServiceCommandDeadlines.CollectionSweepSeconds, _logger, cancellationToken);
                }

                await transaction.CommitAsync(cancellationToken);
            }

            /* Last statement: reaching it means the commit was acknowledged. Any exit before it leaves the write's
               outcome unknown, and the fence poisons the server. */
            fenceSucceeded = true;
        }
        finally
        {
            if (fencedServerId is { } endServerId)
            {
                _queryStoreWriteFence?.EndWrite(endServerId, fenceSucceeded);
            }
        }

        return rowsWritten;
    }

    /// <summary>
    /// The distinct databases of a Query Store batch (#3953): normally exactly one, because the enumerated and Azure
    /// paths write one batch per database per cycle. Null for any other collector's rows.
    /// </summary>
    private static List<string>? DistinctQueryStoreDatabases<TRow>(List<TRow> rows)
    {
        var names = new List<string>();
        foreach (var row in rows)
        {
            if (row is QueryStoreCollector.Row queryStoreRow && !names.Contains(queryStoreRow.DatabaseName))
            {
                names.Add(queryStoreRow.DatabaseName);
            }
        }

        return names;
    }

    /// <summary>
    /// Runs a collector definition against one monitored server's LIVE connection and RETURNS the shredded
    /// rows WITHOUT writing them to the store — the read-only "fetch phase only" twin of <see cref="RunAsync"/>,
    /// for an on-demand read (the live Current Active Queries snapshot the <c>fetch_active_queries</c> command
    /// serves). It builds the SAME <see cref="CollectorContext"/> the scheduled sweep builds (the shared delta
    /// calculator, the live capture-plan / schema-change providers, the server's excluded databases, the
    /// ignored-wait defaults), so the live query is byte-identical to the collector's, then opens ONE SqlClient
    /// connection and runs the definition's single query and shredder. It deliberately supports ONLY the
    /// single-statement path (no per-database enumeration, no per-item enumeration, no supplemental query): it
    /// exists for <see cref="QuerySnapshotsCollector"/>, whose Azure variant already reads what it needs from one
    /// connection. A collector that does not apply to the target yields an empty list (mirrors
    /// <see cref="RunAsync"/>). Cancellation is honored; a <c>SqlException</c> propagates to the caller, which
    /// maps it to a legible command outcome (timeout / permission / error) exactly as the actual-plan handler does.
    /// </summary>
    public async Task<List<TRow>> FetchRowsAsync<TRow>(
        ICollectorDefinition<TRow> definition,
        ServerRuntime server,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        if (!CollectorCatalog.AppliesTo(definition, server.Target))
        {
            return new List<TRow>();
        }

        var context = new CollectorContext
        {
            ServerId = server.ServerId,
            ServerName = server.StorageName,
            CollectionTime = DateTime.UtcNow,
            Deltas = _deltas,
            LogHashKey = _logHashKey,
            Target = server.Target,
            Watermark = null,
            NumericWatermark = null,
            HasCollectedBefore = false,
            IgnoredWaitTypes = IgnoredWaitDefaults.All,
            ExcludedDatabases = server.Config.ExcludedDatabases?.ToArray() ?? Array.Empty<string>(),
            PerfmonCounterOverride = null,
            CapturePlanXml = CapturePlansFor(server.CapturePlans),
            /* #2150: stays FALSE here, and NOT because the readers are behind — this is FetchRowsAsync, the
               on-demand live fetch. It returns the rows straight to the caller and writes nothing: no store
               insert, no text fetch, no watermark. Turning it on would null query_sql_text in rows that have
               no side table to be resolved from, so text would simply be gone. Today's only caller is the
               active-queries snapshot, which has no Query Store text at all, so this is a guard for the next
               caller rather than a live behaviour. */
            FetchQueryTextSeparately = false,
            /* #2164: 0 from the default provider means "no override" — the collector keeps its own
               constant. Converted MB -> bytes here so the store knob stays operator-friendly. */
            TextByteBudgetOverride = _textBudgetMb() > 0 ? _textBudgetMb() * 1024 * 1024 : null,
            CollectSchemaChangeEvents = _collectSchemaChanges(),
        };

        /* Engine-neutral: a Postgres target gets an NpgsqlConnection here and the definition's
           ReadAsync never knows the difference — it reads a DbDataReader either way. */
        var provider = TargetProviders.For(server.Target);
        using var connection = provider.CreateConnection(server.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        /* #6143: built after the open, so the query names each extension by the schema read on this connection. */
        await ResolvePgExtensionSchemasAsync(context, definition, connection, _logger, cancellationToken);
        var plan = definition.BuildQuery(context);
        using var command = CreateCollectorCommand(provider, plan, connection, commandTimeoutSeconds);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await definition.ReadAsync(reader, context, cancellationToken);
    }

    /// <summary>
    /// The server-scoped watermark SQL, exposed so a pin can assert the SHIPPED string rather than a
    /// retyped copy of it. <paramref name="bounded"/> adds the <c>collection_time</c> predicate — the
    /// partitioning column, and the only thing here that prunes a chunk.
    /// </summary>
    internal static string BuildServerWatermarkSql(string tableName, string columnName, bool bounded) =>
        bounded
            ? $"SELECT MAX({columnName}) FROM {tableName} WHERE server_id = $1 AND collection_time > $2"
            : $"SELECT MAX({columnName}) FROM {tableName} WHERE server_id = $1";

    /// <summary>
    /// The server-scoped watermark SQL for a definition that declares a UTC twin beside its watermark column
    /// (#3778): BOTH maxima in one statement, the twin first, so the caller can prefer the twin and fall to
    /// the declared column without a second round trip — and so that the two values come from ONE scan of
    /// the same rows, which is what makes "the twin is NULL, so use the local stamp" a statement about the
    /// store rather than about a race between two reads. Same <c>server_id</c> predicate and the same
    /// optional <c>collection_time</c> bound as <see cref="BuildServerWatermarkSql"/>; exposed for the same
    /// reason, so a pin can assert the SHIPPED string.
    /// </summary>
    internal static string BuildServerWatermarkPairSql(string tableName, string columnName, string utcColumnName, bool bounded) =>
        bounded
            ? $"SELECT MAX({utcColumnName}), MAX({columnName}) FROM {tableName} WHERE server_id = $1 AND collection_time > $2"
            : $"SELECT MAX({utcColumnName}), MAX({columnName}) FROM {tableName} WHERE server_id = $1";

    /// <summary>
    /// The per-database watermark SQL (#1535/#2344), exposed for the same reason as its server-scoped
    /// siblings: a pin asserts the SHIPPED string. <paramref name="bounded"/> adds the same
    /// <c>collection_time</c> partitioning-column predicate, parameterized last ($3) after server and
    /// database.
    /// </summary>
    internal static string BuildServerWatermarkForDatabaseSql(string tableName, string columnName, string databaseColumnName, bool bounded) =>
        bounded
            ? $"SELECT MAX({columnName}) FROM {tableName} WHERE server_id = $1 AND {databaseColumnName} = $2 AND collection_time > $3"
            : $"SELECT MAX({columnName}) FROM {tableName} WHERE server_id = $1 AND {databaseColumnName} = $2";

    /// <summary>
    /// The per-database watermark SQL for the Query Store cache's seed read (#4749): the bounded value, plus its
    /// witness, which is the newest <c>collection_time</c> among the rows AT that value inside the bound. Same
    /// bounded predicates as <see cref="BuildServerWatermarkForDatabaseSql"/> (<c>collection_time</c> last as $3).
    ///
    /// <para>#5684: ONE pass, not the inner-<c>MAX</c> two-scan shape it had. The newest row by
    /// (<c>last_execution_time</c> DESC, <c>collection_time</c> DESC) IS the answer: its first column is the
    /// maximum, its second the newest stamp among the rows at that maximum. <c>IS NOT NULL</c> stands in for
    /// <c>MAX</c> ignoring NULLs (PostgreSQL sorts NULLs FIRST in a descending sort, so without it a NULL row would
    /// win). An empty window and an all-NULL window return NO row where the old text returned one row of NULLs;
    /// <see cref="ReadLastCollectedTimeAndWitnessForDatabaseAsync"/> reads both as (null, null, succeeded). The
    /// leading <see cref="QueryStoreWatermarkReader.SeedMarker"/> (the one copy of the marker, #5707) is what the read-counting tests match. Exposed for the
    /// same reason as its siblings, so a pin asserts the SHIPPED string.</para>
    /// </summary>
    internal static string BuildServerWatermarkWithWitnessForDatabaseSql(string tableName, string columnName, string databaseColumnName) =>
        $"{QueryStoreWatermarkReader.SeedMarker} SELECT {columnName}, collection_time FROM {tableName} "
        + $"WHERE server_id = $1 AND {databaseColumnName} = $2 AND collection_time > $3 "
        + $"AND {columnName} IS NOT NULL "
        + $"ORDER BY {columnName} DESC, collection_time DESC LIMIT 1";

    /// <summary>
    /// The numeric (bigint identity) watermark SQL behind <see cref="GetLastCollectedInstanceIdAsync"/>
    /// (job_history's <c>instance_id</c>), exposed for the same reason as the timestamp builders above. Bounds
    /// on <c>collection_time</c> — job_history's partitioning column — never on <paramref name="columnName"/>
    /// itself, which does not partition the table and so cannot prune a chunk (#4197).
    /// </summary>
    internal static string BuildServerWatermarkInstanceIdSql(string tableName, string columnName, bool bounded) =>
        bounded
            ? $"SELECT MAX({columnName}) FROM {tableName} WHERE server_id = $1 AND collection_time > $2 "
              + $"AND collection_time = (SELECT MAX(collection_time) FROM {tableName} WHERE server_id = $1 AND collection_time > $2)"
            : $"SELECT MAX({columnName}) FROM {tableName} WHERE server_id = $1 "
              + $"AND collection_time = (SELECT MAX(collection_time) FROM {tableName} WHERE server_id = $1)";

    /// <summary>
    /// True when this cycle will OVERWRITE the server-scoped watermark before any query is built, so
    /// <see cref="GetLastCollectedTimeAsync"/>'s answer would be read by nothing and the round trip can be
    /// skipped (#2797).
    ///
    /// <para><b>This is a DISPATCH-PATH question, not a watermark-column one, and that is the whole issue.</b>
    /// #2797 proposed gating on <c>PerDatabaseWatermarkColumn is not null</c> alone — the signal the per-item
    /// paths already key on. That reads plausibly and would ship a data-correctness bug. Four collectors
    /// declare BOTH watermark columns (query_store, deadlocks, blocked_process_report,
    /// long_query_completions) and all four declare <c>RunsPerDatabase =&gt; target.IsAzureSqlDb</c>, but only
    /// query_store overrides <see cref="ICollectorDefinition{TRow}.BuildEnumerationQuery"/>; the other three
    /// inherit <c>CollectorDefinitionBase</c>'s <c>=&gt; null</c>. So OFF Azure — which is the entire on-prem
    /// and RDS fleet — three of the four fall through to the plain server-scoped path and genuinely CONSUME
    /// this value. Gating on the column alone would hand them a null watermark on every cycle and make them
    /// re-collect their whole first-run window forever: #2795's defect, arrived at from the other side.</para>
    ///
    /// <para><b>The three paths.</b> <see cref="RunAsync"/> dispatches down exactly one of them.
    /// <see cref="ICollectorDefinition{TRow}.RunsPerDatabase"/> opens a connection per database and assigns
    /// <c>context.Watermark</c> from <see cref="GetLastCollectedTimeForDatabaseAsync"/> inside the loop; a
    /// non-null <see cref="ICollectorDefinition{TRow}.BuildEnumerationQuery"/> drives the enumerated loop,
    /// which assigns <c>context.Watermark</c> inside its <c>perItemWatermark</c> delegate; anything else
    /// reads the server-scoped value this method guards. Both fan-out paths wire their per-item refresh only
    /// when BOTH watermark columns are declared, which is why that conjunct belongs here too: a collector
    /// that enumerates with no <c>PerDatabaseWatermarkColumn</c> keeps the server-wide value and still
    /// consumes it.</para>
    ///
    /// <para><b>Derived, not declared, so it cannot drift.</b> The enumeration half asks the definition's own
    /// <c>BuildEnumerationQuery</c> rather than a second flag mirroring it — a hand-maintained "enumerates"
    /// signal would just relocate the bug the moment the two disagreed. The price is that the question is
    /// asked against a probe context rather than the cycle's own (which cannot exist yet — see the call
    /// site), and that is sound only while null-ness is a function of
    /// <see cref="CollectorContext.Target"/> alone. That is not assumed: <c>ServerWatermarkDispatchGateTests</c>
    /// varies Watermark, NumericWatermark, HasCollectedBefore, State and ExcludedDatabases across every
    /// definition in <see cref="CollectorCatalog.All"/> and fails if any of them moves the answer.</para>
    /// </summary>
    internal static bool ServerWatermarkIsDiscarded<TRow>(
        ICollectorDefinition<TRow> definition, CollectorContext dispatchProbe) =>
        definition.WatermarkColumn is not null
        && definition.PerDatabaseWatermarkColumn is not null
        && (definition.RunsPerDatabase(dispatchProbe.Target)
            || definition.BuildEnumerationQuery(dispatchProbe) is not null);

    /// <summary>
    /// Gets the most recent value of a timestamp column from Postgres for incremental collection.
    /// Returns null on first run or if the query fails (caller uses a fallback window) — the
    /// Postgres twin of Lite's GetLastCollectedTimeAsync.
    ///
    /// <para><paramref name="collectedSince"/> bounds the read on <c>collection_time</c> — the
    /// PARTITIONING column, so the bound actually prunes chunks. This is #2344's bound applied to the
    /// SERVER-scoped read; #2344 fixed only the per-database sibling
    /// (<see cref="GetLastCollectedTimeForDatabaseAsync"/>), and query_store declares BOTH watermark
    /// columns, so it kept paying the unbounded cost here on every cycle. Pass
    /// <see cref="WatermarkPolicy.ReadFloor"/> only from a caller whose value is clamped, and read that
    /// method's remarks for why the bound provably changes no answer. Null keeps the unbounded
    /// behaviour, which stays correct for any reader whose watermark is NOT clamped.</para>
    ///
    /// <para>Measured on use1 before the bound (query_store_stats, 62.5 GB, 19 chunks): 40.7 s and
    /// 50.6 s cold, 9.3 s warm, against Npgsql's 30 s default CommandTimeout — so the read was being
    /// cancelled mid-flight, which the store's own log recorded 2,092 times in one day while the
    /// per-database bounded sibling was cancelled 17 times. Every one of those cancellations returned
    /// null here, and null is indistinguishable from a first run: the caller fell back to
    /// query_store's 60-minute window instead of the ~5-minute incremental one, re-collected what it
    /// already had, and grew the table that made the next read slower.</para>
    /// </summary>
    public async Task<DateTime?> GetLastCollectedTimeAsync(
        int serverId, string tableName, string columnName, CancellationToken cancellationToken,
        DateTime? collectedSince = null)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

            /* #4197: an explicit collectedSince (query_store's clamped floor, per WatermarkPolicy.ReadFloor)
               stays exactly what #2344/#2795 shipped — one bounded read, and a null result falls through to
               the caller's own clamp rather than a second round trip. Every OTHER caller passes null, which
               used to mean "read every chunk in retention, every cycle" — the ring-buffer cost #4197
               measures. It now means WatermarkPolicy.RecentWatermarkWindow's probe-then-confirm: bound on
               the SAME partitioning column first (chunk exclusion prunes everything older), then, on a miss,
               the week before it (#5515), and only on a second miss fall back to the true unbounded MAX below. A
               hit is never NEWER than the unbounded answer, and equals it whenever the column rises with
               collection_time (instance_id, event_time); see WatermarkPolicy.RecentWatermarkWindow's remarks. For
               job_history's run_datetime, which does not, a hit can be OLDER than the unbounded MAX, which only
               widens the caller's re-read; its natural-key dedupe drops the repeats. */
            var autoWindow = collectedSince is null;
            var bound = collectedSince ?? DateTime.UtcNow - WatermarkPolicy.RecentWatermarkWindow;

            var sql = BuildServerWatermarkSql(tableName, columnName, bounded: true);
            using var command = new NpgsqlCommand(sql, connection);
            /* Explicit rather than Npgsql's 30 s default: that default was never a decision anyone made
               here, and it silently governed a read whose measured cost exceeded it. */
            command.CommandTimeout = CommandTimeoutSeconds;
            command.Parameters.AddWithValue(serverId);
            /* Naive like every other timestamp bound in this store (#1969): a Utc Kind infers
               timestamptz and Postgres would convert it into the session zone on the way in. */
            command.Parameters.AddWithValue(DateTime.SpecifyKind(bound, DateTimeKind.Unspecified));

            var result = await command.ExecuteScalarAsync(cancellationToken);
            if (result is DateTime dt)
            {
                return dt;
            }

            if (autoWindow)
            {
                /* #5515: the probe found no row in RecentWatermarkWindow, which is every cycle's answer for a
                   quiet server (no job run or trace event for hours). Before the whole slice, try the week:
                   the SAME bounded statement with an older bound, so the chunks of seven days answer a server
                   whose newest row is that recent, where the unbounded MAX opened every chunk in retention,
                   compressed ones included (5,426 blocks a call on job_history). */
                using var widened = new NpgsqlCommand(sql, connection);
                widened.CommandTimeout = CommandTimeoutSeconds;
                widened.Parameters.AddWithValue(serverId);
                widened.Parameters.AddWithValue(DateTime.SpecifyKind(
                    DateTime.UtcNow - WatermarkPolicy.WidenedWatermarkWindow, DateTimeKind.Unspecified));
                var widenedResult = await widened.ExecuteScalarAsync(cancellationToken);
                if (widenedResult is DateTime widenedDt)
                {
                    return widenedDt;
                }

                /* Still no row: a gap wider than the week, or a genuinely empty table. Either way only the true
                   unbounded MAX answers correctly; a null here would read as first-run, which is not scoped to
                   the window the way it is for query_store's clamped callers. */
                using var fallback = new NpgsqlCommand(BuildServerWatermarkSql(tableName, columnName, bounded: false), connection);
                fallback.CommandTimeout = CommandTimeoutSeconds;
                fallback.Parameters.AddWithValue(serverId);
                var fallbackResult = await fallback.ExecuteScalarAsync(cancellationToken);
                if (fallbackResult is DateTime fallbackDt)
                {
                    return fallbackDt;
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Caller still uses its fallback window — but SAY SO. Swallowing this silently is what let
               a permanently-failing watermark read look identical to a healthy first run for an unknown
               length of time: nothing reached collection_log, nothing reached this log, and the only
               trace was a cancellation line in the store's own Postgres log.

               The OperationCanceledException guard is the shape this file already uses at its
               collector-run catches: a cancelled read is a service shutdown, not a watermark failure,
               and logging it as one would put a misleading warning per in-flight collector on every
               normal stop. */
            _logger?.LogWarning(
                "Watermark read failed for server {ServerId} on {Table}.{Column} — falling back to the "
                + "collector's default window, which re-collects data already stored: {Message}",
                serverId, tableName, columnName, ex.Message);
        }
        return null;
    }

    /// <summary>
    /// The watermark read for a definition that declares a UTC twin beside its watermark column (#3778):
    /// <c>MAX(utcColumnName)</c> and <c>MAX(columnName)</c> in one round trip, returning the twin's value with
    /// <c>FromUtcColumn = true</c> when the store holds at least one row carrying it, else the declared column's
    /// value with <c>false</c>, else <c>(null, false)</c> — the first-run fallback, exactly as
    /// <see cref="GetLastCollectedTimeAsync"/> signals it. The caller puts the pair on
    /// <see cref="CollectorContext.Watermark"/> and <see cref="CollectorContext.WatermarkFromUtcColumn"/>, and
    /// the definition's dedup compares each row IN THAT FRAME. Only <c>cpu_utilization</c> declares a twin
    /// today; every other definition still goes through <see cref="GetLastCollectedTimeAsync"/> and its
    /// unchanged SQL.
    ///
    /// <para><b>Why the twin wins whenever it exists, without comparing the two maxima.</b> The twin is NULL on
    /// every row written before Darling V134 and non-null on every row the collector has written since, so
    /// "the newest row with a twin" and "the newest row" are the same row once any post-rung row exists —
    /// collection is append-only in time. Comparing <c>MAX(sample_time_utc)</c> against <c>MAX(sample_time)</c>
    /// to decide would itself be the cross-frame comparison this read exists to avoid.</para>
    ///
    /// <para>Same bound, same deadline, same failure contract as the sibling: <paramref name="collectedSince"/>
    /// predicates on the partitioning column (null for the CPU collector, whose watermark is never clamped;
    /// <c>ServerWatermarkReadFloorTests</c> requires every reader in this family to accept it), the command
    /// deadline is explicit rather than Npgsql's default, and a failed read logs and returns the first-run
    /// pair rather than throwing — saying so, because a silent null here is a duplicate hour per server per
    /// cycle for as long as it lasts, not merely a re-collected window.</para>
    /// </summary>
    public async Task<(DateTime? Value, bool FromUtcColumn)> GetLastCollectedTimeWithFrameAsync(
        int serverId, string tableName, string columnName, string utcColumnName, CancellationToken cancellationToken,
        DateTime? collectedSince = null)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

            /* #4197: same probe-then-confirm as the sibling above — see its remarks. collectedSince
               non-null (query_store's clamped floor) is unchanged; null now means "bound to
               RecentWatermarkWindow, then WidenedWatermarkWindow (#5515), fall back to the true unbounded pair
               only on a second miss" instead of "always unbounded". */
            var autoWindow = collectedSince is null;
            var bound = collectedSince ?? DateTime.UtcNow - WatermarkPolicy.RecentWatermarkWindow;

            var sql = BuildServerWatermarkPairSql(tableName, columnName, utcColumnName, bounded: true);
            using (var command = new NpgsqlCommand(sql, connection))
            {
                command.CommandTimeout = CommandTimeoutSeconds;
                command.Parameters.AddWithValue(serverId);
                /* Naive like every other timestamp bound in this store (#1969): a Utc Kind infers
                   timestamptz and Postgres would convert it into the session zone on the way in. */
                command.Parameters.AddWithValue(DateTime.SpecifyKind(bound, DateTimeKind.Unspecified));

                using var reader = await command.ExecuteReaderAsync(cancellationToken);
                if (await reader.ReadAsync(cancellationToken))
                {
                    if (!reader.IsDBNull(0))
                    {
                        return (reader.GetDateTime(0), true);
                    }

                    if (!reader.IsDBNull(1))
                    {
                        return (reader.GetDateTime(1), false);
                    }
                }
            }

            if (autoWindow)
            {
                /* #5515: neither column had a row in RecentWatermarkWindow — try the week first, the sibling's
                   second rung, so a quiet server's pair read is answered by seven days of chunks instead of
                   every chunk in retention. The same bounded statement with an older bound. */
                using (var widened = new NpgsqlCommand(sql, connection))
                {
                    widened.CommandTimeout = CommandTimeoutSeconds;
                    widened.Parameters.AddWithValue(serverId);
                    widened.Parameters.AddWithValue(DateTime.SpecifyKind(
                        DateTime.UtcNow - WatermarkPolicy.WidenedWatermarkWindow, DateTimeKind.Unspecified));
                    using var widenedReader = await widened.ExecuteReaderAsync(cancellationToken);
                    if (await widenedReader.ReadAsync(cancellationToken))
                    {
                        if (!widenedReader.IsDBNull(0))
                        {
                            return (widenedReader.GetDateTime(0), true);
                        }

                        if (!widenedReader.IsDBNull(1))
                        {
                            return (widenedReader.GetDateTime(1), false);
                        }
                    }
                }

                /* Neither column had a row in the week — fall back to the true unbounded pair, the
                   sibling's exact reasoning: this is not query_store's clamped path, so a miss here is not
                   necessarily a first run. */
                using var fallback = new NpgsqlCommand(
                    BuildServerWatermarkPairSql(tableName, columnName, utcColumnName, bounded: false), connection);
                fallback.CommandTimeout = CommandTimeoutSeconds;
                fallback.Parameters.AddWithValue(serverId);
                using var fallbackReader = await fallback.ExecuteReaderAsync(cancellationToken);
                if (await fallbackReader.ReadAsync(cancellationToken))
                {
                    if (!fallbackReader.IsDBNull(0))
                    {
                        return (fallbackReader.GetDateTime(0), true);
                    }

                    if (!fallbackReader.IsDBNull(1))
                    {
                        return (fallbackReader.GetDateTime(1), false);
                    }
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* The sibling's contract, and the sibling's reason for logging rather than swallowing: a null here
               reads as a first run, and for this collector a first run re-collects the ring buffer's whole
               retained history under stamps the store already holds. */
            _logger?.LogWarning(
                "Watermark read failed for server {ServerId} on {Table}.{Column}/{UtcColumn} — falling back to the "
                + "collector's default window, which re-collects data already stored: {Message}",
                serverId, tableName, columnName, utcColumnName, ex.Message);
        }

        return (null, false);
    }

    /// <summary>
    /// The stored per-server state for one collector's declared keys (#1962) — the sibling of
    /// <see cref="GetLastCollectedTimeAsync"/> for state no MAX() over the collected rows can produce.
    /// Read only for the collectors that declare keys, so it costs the rest nothing. An empty result on
    /// failure is the SAFE direction: every definition treats absent state as its conservative path
    /// (default_trace_events re-reads the whole rollover set), so a broken read costs time, never events.
    /// Lite's twin is <c>RemoteCollectorService.GetCollectorStateAsync</c> — same table, same columns.
    /// </summary>
    public async Task<Dictionary<string, string>> GetCollectorStateAsync(
        int serverId, string collectorName, CancellationToken cancellationToken)
    {
        var state = new Dictionary<string, string>(StringComparer.Ordinal);
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(
                "SELECT state_key, state_value FROM collector_state WHERE server_id = $1 AND collector_name = $2", connection);
            command.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(collectorName);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (!reader.IsDBNull(1))
                {
                    state[reader.GetString(0)] = reader.GetString(1);
                }
            }
        }
        catch (Exception ex)
        {
            /* Fail toward "no state" — the definition's conservative path, never a wrong-but-plausible one. */
            _logger?.LogDebug(ex, "Reading collector state for {Collector} failed; using the no-state path", collectorName);
        }
        return state;
    }
}
