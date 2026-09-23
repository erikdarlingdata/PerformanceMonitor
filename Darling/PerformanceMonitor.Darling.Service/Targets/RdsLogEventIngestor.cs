/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
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
/// Stores classified log events fetched from the RDS log API into <c>collect.pg_log_events</c> (#3601) —
/// the managed-PostgreSQL half of the log-event pipeline, and <see cref="RdsDeadlockIngestor"/>'s sibling
/// in every structural respect.
///
/// <para><b>Its own <see cref="RdsLogSource"/>, not a shared one</b>, for the deadlock ingestor's reason:
/// the source keeps an in-memory resume marker per (instance, file), consumed on every read, so sharing
/// one with the plan or deadlock ingestor would starve whichever ran second. Three ingestors, three
/// markers, one API. Unifying them onto one read of the chunk and one classifier pass — which is what the
/// pipeline makes possible for the first time — is the follow-up the classifier's header names; tonight
/// the two existing ingestors are untouched and this one arrives beside them.</para>
///
/// <para><b>What it deliberately does NOT duplicate.</b> Assembly, classification, redaction and hashing
/// are <see cref="PgLogEventClassifier"/>'s, the same parsers the <c>pg_read_file</c> route's
/// <see cref="PgLogEventsCollector"/> runs, keyed with the same store key (#4004); this type hands it the chunk's
/// text and gets rows. The WRITE
/// goes through <c>PgCollectorRowWriter</c> and the collector's own definition, so column order and the
/// COPY command are the collector's. The marker moves after the write and nowhere else (#3008).</para>
/// </summary>
public sealed class RdsLogEventIngestor
{
    private readonly NpgsqlDataSource _postgres;
    private readonly PgLogEventClassifier _classifier;
    private readonly RdsLogSource _logs;
    private readonly ILogger? _logger;

    /// <param name="logHashKey">The store's log-hash key (#4004), the same instance the <c>pg_read_file</c> route's
    /// runs carry, so the two transports store identical identities for identical text.</param>
    public RdsLogEventIngestor(NpgsqlDataSource postgres, PgLogHashKey logHashKey, RdsLogSource? logs = null, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _classifier = new PgLogEventClassifier(logHashKey ?? throw new ArgumentNullException(nameof(logHashKey)));
        _logs = logs ?? new RdsLogSource();
        _logger = logger;
    }

    /// <param name="host">The target's connection host. A non-RDS host means this transport does not apply
    /// — it uses the <c>pg_read_file</c> route — and the outcome says so rather than reporting an empty log
    /// (#3017).</param>
    /// <exception cref="PgLogTimezoneUnsupportedException">The log is stamped in a non-UTC zone (#2993).
    /// Propagated so the runner names the setting; the marker is not committed, so the window comes back
    /// once the setting is fixed.</exception>
    public async Task<RdsIngestOutcome> IngestAsync(
        int serverId,
        string storageName,
        string host,
        bool logTimezoneIsUtc = false,
        CancellationToken cancellationToken = default)
    {
        RdsLogSource.LogChunk? chunk;

        try
        {
            chunk = await _logs.ReadNewestAsync(host, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* #2633's fix, shared: rethrown so the runner degrades an authorization refusal to PERMISSIONS
               rather than recording a SUCCESS row that claims the log was opened and held nothing. */
            _logger?.LogWarning(
                "RDS log unavailable for {Server}: {Message} — log-event capture is skipped for this target "
                + "this cycle; every other collector is unaffected.",
                storageName, ex.Message);

            throw new RdsLogUnavailableException(
                ex.Message, RdsLogUnavailableException.IsAuthorizationRefusal(ex), ex);
        }

        if (chunk is null)
        {
            /* #3017: NOT_REACHED, not zero rows — the host is not an RDS or Aurora endpoint, no AWS call was
               made, and nothing is known about the log. */
            return RdsIngestOutcome.NotReached;
        }

        var (written, foreignZoneLines) = await StoreAsync(serverId, storageName, chunk.Value.Text, logTimezoneIsUtc, cancellationToken);

        /* THE MARKER MOVES HERE AND NOWHERE ELSE (#3008). Everything the chunk held is in the store or was
           nothing to store; anything else threw out of StoreAsync and left the marker where it was. */
        _logs.CommitResume(chunk.Value.Resume);

        return RdsIngestOutcome.Read(written, foreignZoneLines);
    }

    private async Task<(int Written, int ForeignZoneLines)> StoreAsync(
        int serverId,
        string storageName,
        string text,
        bool logTimezoneIsUtc,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(text))
        {
            return (0, 0);
        }

        /* Outside IngestAsync's tolerant catch, which covers the AWS FETCH: a zone refusal is a statement
           about the target's configuration and has to reach the runner uncommitted (#3008). #4046 part 1b:
           logTimezoneIsUtc skips and counts a foreign-zone line instead of throwing, the same trade the
           self-hosted route already makes. */
        var events = _classifier.Classify(text, logTimezoneIsUtc, out var foreignZoneLines);

        if (events.Count == 0)
        {
            return (0, foreignZoneLines);
        }

        return (await WriteAsync(serverId, storageName, events, cancellationToken), foreignZoneLines);
    }

    /// <summary>
    /// The same binary COPY the collector runner uses, driven by the collector's own definition — verbatim
    /// the deadlock ingestor's write, over a different definition. A shared write helper for the three
    /// ingestors is the natural follow-up; it was not pulled out tonight because the deadline-and-phase
    /// discipline in it is the part every reviewer of #2874 and #3008 has read in place, and moving it
    /// under a new table is not the night to re-read it.
    /// </summary>
    private async Task<int> WriteAsync(
        int serverId,
        string storageName,
        IReadOnlyList<PgLogEvent> rows,
        CancellationToken cancellationToken)
    {
        var definition = PgLogEventsCollector.Instance;

        var collectionTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

        var writer = new PgCollectorRowWriter();
        var written = 0;

        using var startDeadline = StoreCopyStartDeadline.Start(cancellationToken);

        var copyPhase = StoreCopyPhase.Start;

        try
        {
            using (var importer = await connection.BeginBinaryImportAsync(
                PgCollectorRowWriter.CopyCommandFor(definition), startDeadline.Token))
            {
                copyPhase = StoreCopyPhase.Data;

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
        }
        catch (OperationCanceledException cancellation)
            when (copyPhase == StoreCopyPhase.Start && startDeadline.Breached())
        {
            var breach = StoreCopyStartDeadline.Breach(cancellation);
            CollectorFaultCopyPhase.Stamp(breach, copyPhase);
            throw breach;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            CollectorFaultCopyPhase.Stamp(ex, copyPhase);
            throw;
        }

        _logger?.LogInformation(
            "Stored {Count} log event(s) for {Server} from the RDS log API.", written, storageName);

        return written;
    }

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
