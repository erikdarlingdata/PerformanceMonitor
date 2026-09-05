/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// Stores deadlock reports fetched from the RDS log API into <c>collect.pg_deadlocks</c> — the
/// managed-PostgreSQL half of deadlock capture, and <see cref="RdsPlanIngestor"/>'s sibling.
///
/// <para><b>Its own <see cref="RdsLogSource"/>, not a shared one.</b> <see cref="RdsLogSource"/> keeps an
/// in-memory resume marker per (instance, file), consumed on every read. Sharing one instance with plan
/// capture would mean whichever of the two ingestors runs second in a cycle sees only the portion the first
/// one already consumed — starved, not merely redundant. <see cref="PgDeadlocksCollector"/>'s own SQL route
/// already reads "the same bounded tail of the same file as plan capture" independently at the database
/// level; this mirrors that at the RDS-API level rather than inventing a shared-cursor scheme neither route
/// uses.</para>
///
/// <para><b>What it deliberately does NOT duplicate.</b> Parsing and hashing come from
/// <c>PgDeadlockLogParser</c>, shared with the <c>pg_read_file</c> route via its <c>Extract</c> entry point
/// — the same reason <c>PgPlanLogParser</c> is shared by <see cref="RdsPlanIngestor"/>. The WRITE goes
/// through <c>PgCollectorRowWriter</c> and <c>PgDeadlocksCollector</c>'s own definition, so the column order
/// and the COPY command are the collector's rather than a second opinion about them.</para>
/// </summary>
public sealed class RdsDeadlockIngestor
{
    private readonly NpgsqlDataSource _postgres;
    private readonly RdsLogSource _logs;
    private readonly ILogger? _logger;

    public RdsDeadlockIngestor(NpgsqlDataSource postgres, RdsLogSource? logs = null, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logs = logs ?? new RdsLogSource();
        _logger = logger;
    }

    /// <param name="host">The target's connection host. A non-RDS host is skipped silently — that target
    /// uses the <c>pg_read_file</c> route instead, and there is nothing to report.</param>
    /// <returns>Rows stored, or zero when this target is not RDS or had nothing new.</returns>
    /// <exception cref="PgLogTimezoneUnsupportedException">The log is stamped in a non-UTC zone, so its
    /// timestamps are local and nothing in it can be stored (#2993). Propagated for the same reason
    /// <see cref="RdsLogUnavailableException"/> is: the runner classifies it and names the setting, where
    /// swallowing it would return zero rows and be recorded as a log that was read and held no
    /// deadlocks.</exception>
    public async Task<int> IngestAsync(
        int serverId,
        string storageName,
        string host,
        CancellationToken cancellationToken = default)
    {
        RdsLogSource.LogChunk? chunk;

        try
        {
            chunk = await _logs.ReadNewestAsync(host, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* #2633's fix, shared: rethrown so the runner degrades an authorization refusal to PERMISSIONS,
               naming which kind of nothing was found, instead of a SUCCESS row claiming the log was opened
               and empty. */
            _logger?.LogWarning(
                "RDS deadlock log unavailable for {Server}: {Message} — deadlock capture is skipped for "
                + "this target this cycle; every other collector is unaffected.",
                storageName, ex.Message);

            throw new RdsLogUnavailableException(
                ex.Message, RdsLogUnavailableException.IsAuthorizationRefusal(ex), ex);
        }

        if (chunk is null)
        {
            return 0;
        }

        var written = await StoreAsync(serverId, storageName, chunk.Value.Text, cancellationToken);

        /* THE MARKER MOVES HERE AND NOWHERE ELSE. Reaching this line means everything the chunk held is
           either in the store or was nothing to store; anything else threw out of StoreAsync above and
           left the marker where it was, so the next cycle asks RDS for the same window again.

           The order is the fix (#3008). While the marker advanced inside ReadNewestAsync, a parse fault, a
           COPY that tripped its deadline, a dropped store connection or a cancelled cycle each consumed a
           window nobody stored — and DownloadDBLogFilePortion does not hand the same bytes out twice, so
           every deadlock in it was gone with no error naming the loss. */
        _logs.CommitResume(chunk.Value.Resume);

        return written;
    }

    /// <summary>
    /// Parse a chunk and store what it held, or throw. Split out so the resume marker has exactly one
    /// commit point above it: every way this can decline to store rows — empty text, a slab with no
    /// deadlocks in it — is a legitimate zero that loses nothing, and every way it can FAIL leaves via an
    /// exception rather than a zero the caller would have to tell apart from those.
    /// </summary>
    private async Task<int> StoreAsync(
        int serverId,
        string storageName,
        string text,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(text))
        {
            return 0;
        }

        /* Not inside IngestAsync's tolerant catch, which covers the AWS FETCH. A parse refusal is a
           statement about the target's configuration rather than about reaching it, and it has to reach the
           runner to be classified.

           It also has to leave WITHOUT the marker being committed, which is why this whole method sits
           ahead of the commit rather than around it: a refused zone that consumed the window would discard
           every report in it, and the setting that caused the refusal is fixable, so those reports are
           worth still being there afterwards (#3008). */
        var deadlocks = PgDeadlockLogParser.Extract(text);

        if (deadlocks.Count == 0)
        {
            /* A log slab with no deadlocks in it is the ordinary case. Not worth a log line every cycle. */
            return 0;
        }

        var rows = new List<PgDeadlocksCollector.Row>(deadlocks.Count);

        foreach (var deadlock in deadlocks)
        {
            rows.Add(new PgDeadlocksCollector.Row(
                OccurredAtUtc: deadlock.OccurredAtUtc,
                VictimPid: deadlock.VictimPid,
                ParticipantCount: deadlock.ParticipantCount,
                DeadlockHash: deadlock.DeadlockHash,
                LockModes: deadlock.LockModes,
                Resources: deadlock.Resources,
                VictimStatement: deadlock.VictimStatement,
                GraphText: deadlock.GraphText));
        }

        return await WriteAsync(serverId, storageName, rows, cancellationToken);
    }

    /// <summary>
    /// The same binary COPY the collector runner uses, driven by the collector's own definition — so the
    /// column order and COPY command come from one place and cannot drift from the table.
    /// </summary>
    private async Task<int> WriteAsync(
        int serverId,
        string storageName,
        IReadOnlyList<PgDeadlocksCollector.Row> rows,
        CancellationToken cancellationToken)
    {
        var definition = PgDeadlocksCollector.Instance;

        /* Naive UTC, the store's convention for every collector timestamp: the columns are `timestamp`
           without a zone, and letting Kind=Utc through makes Npgsql infer timestamptz and shift the value
           by the store session's offset. */
        var collectionTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

        var writer = new PgCollectorRowWriter();
        var written = 0;

        using (var importer = await connection.BeginBinaryImportAsync(
            PgCollectorRowWriter.CopyCommandFor(definition), cancellationToken))
        {
            /* #2874: the COPY's own deadline, on NpgsqlBinaryImporter.Timeout — a TimeSpan on a different type
               from the rest of the regime, invisible to a command-shaped regex, and inherited from the
               connection's CommandTimeout (30 s) when left unset. Same constant, same regime, and the same
               narrows-not-closes caveat about the Begin phase as DarlingCollectorRunner.WriteBatchAsync. */
            importer.Timeout = TimeSpan.FromSeconds(ServiceCommandDeadlines.CollectionSweepSeconds);

            writer.Importer = importer;

            foreach (var row in rows)
            {
                await importer.StartRowAsync(cancellationToken);

                if (definition.IncludesCollectionId)
                {
                    writer.Value(CollectionIdGenerator.Next());
                }

                writer.Value(collectionTime)
                      .Value(serverId)
                      .Value(storageName);

                writer.BeginPayload();
                definition.WritePayload(row, writer, NullContext(serverId, storageName, collectionTime));
                writer.EndPayload(definition.PayloadColumns.Count);
                written++;
            }

            await importer.CompleteAsync(cancellationToken);
        }

        _logger?.LogInformation(
            "Stored {Count} deadlock report(s) for {Server} from the RDS log API.", written, storageName);

        return written;
    }

    /* WritePayload takes a context for the collectors that consult deltas or watermarks. This one reads
       none of it - the rows are already fully formed by the parser - so the context exists to satisfy the
       signature rather than to carry anything. */
    private static CollectorContext NullContext(int serverId, string storageName, DateTime collectionTime)
        => new()
        {
            ServerId = serverId,
            ServerName = storageName,
            CollectionTime = collectionTime,
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
        };
}
