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

    /// <summary>
    /// The csvlog partial-record carry (#4053 part c1), extracted (#4053 part c2) into
    /// <see cref="RdsCsvlogCarryBook"/> at its original behaviour — this ingestor's own book, never shared
    /// with another, for the reason <see cref="RdsLogSource"/>'s own marker remark gives.
    /// </summary>
    private readonly RdsCsvlogCarryBook _csvCarry = new();

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
    /// <param name="pgLogUsesCsvlog">#4053 part c1: whether the target's <c>log_destination</c> includes
    /// <c>csvlog</c>, read by the caller through <see cref="PgLogFormatCapability"/> on its own connection —
    /// this ingestor reaches the log through the AWS API, not SQL, so it has no connection of its own to probe
    /// with. True reads the newest <c>.csv</c> file through <see cref="PgServerLogCsvParser"/> instead of the
    /// stderr file; false is today's stderr route, unchanged.</param>
    public async Task<RdsIngestOutcome> IngestAsync(
        int serverId,
        string storageName,
        string host,
        bool logTimezoneIsUtc = false,
        bool pgLogUsesCsvlog = false,
        CancellationToken cancellationToken = default)
    {
        RdsLogSource.LogChunk? chunk;

        var kind = pgLogUsesCsvlog ? RdsLogSource.LogFileKind.Csv : RdsLogSource.LogFileKind.Stderr;

        try
        {
            chunk = await _logs.ReadNewestAsync(host, kind, cancellationToken);
        }
        catch (PgNoCsvlogFileException)
        {
            /* #4053 review round 1 (item 4): the stale-csv check inside NewestLogFileAsync throws this
               directly — propagated UNWRAPPED, not folded into RdsLogUnavailableException below, so it
               reaches DarlingWorker's own PgNoCsvlogFileException arm, the same arm the self-hosted
               pg_read_file route's "no .csv file yet" fault reaches. That arm invalidates
               PgLogFormatCapability's cached verdict for this server, which is exactly the fix a stale
               "csvlog is on" cache needs: csvlog was turned off, the cache hasn't caught up, and the next
               probe re-reads log_destination instead of this route reading a dead .csv listing forever. */
            throw;
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

        var (carry, carryKey, droppedByRotation, currentFileName) = pgLogUsesCsvlog
            ? _csvCarry.CarryFor(chunk.Value.Resume.Key, chunk.Value.StartsAtFileStart)
            : (RdsCsvlogCarry.CsvCarry.Empty, null, 0, null);

        var (written, foreignZoneLines, csvRecordsDiscarded, nextCarry) = await StoreAsync(
            serverId, storageName, chunk.Value.Text, logTimezoneIsUtc, pgLogUsesCsvlog,
            carry, chunk.Value.MoreAvailable, cancellationToken);

        /* THE MARKER MOVES HERE AND NOWHERE ELSE (#3008), and the csvlog carry moves alongside it for the
           same reason: DownloadDBLogFilePortion is consume-once, so a store failure must leave both the
           marker AND the partial record exactly where they were, not just the marker. A process restart
           between here and the next read loses at most the one record this carry holds. */
        _logs.CommitResume(chunk.Value.Resume);

        /* #4053 review round 1 (item 3): the carry advances ONLY when the marker actually did —
           CommitResume is a no-op on an empty/null marker (a replay), and gluing this portion's carry onto
           itself in that case would apply the same bytes' tail twice. */
        var resumeAdvanced = !string.IsNullOrEmpty(chunk.Value.Resume.Marker);

        if (carryKey is not null && resumeAdvanced)
        {
            csvRecordsDiscarded += _csvCarry.Commit(carryKey, currentFileName, nextCarry, droppedByRotation);
        }

        return RdsIngestOutcome.Read(written, foreignZoneLines, csvRecordsDiscarded);
    }

    private async Task<(int Written, int ForeignZoneLines, int CsvRecordsDiscarded, RdsCsvlogCarry.CsvCarry NextCarry)> StoreAsync(
        int serverId,
        string storageName,
        string text,
        bool logTimezoneIsUtc,
        bool pgLogUsesCsvlog,
        RdsCsvlogCarry.CsvCarry carry,
        bool additionalDataPending,
        CancellationToken cancellationToken)
    {
        List<PgLogEvent> events;
        int foreignZoneLines;
        var csvRecordsDiscarded = 0;
        var nextCarry = RdsCsvlogCarry.CsvCarry.Empty;

        if (pgLogUsesCsvlog)
        {
            if (string.IsNullOrEmpty(text) && string.IsNullOrEmpty(carry.Partial))
            {
                return (0, 0, 0, carry);
            }

            var portion = RdsCsvlogCarry.ParseCsvPortion(carry, text, additionalDataPending);
            var entries = portion.Entries;
            csvRecordsDiscarded = portion.RecordsDiscarded;
            nextCarry = portion.Next;

            /* The SAME foreign-zone rule the stderr path applies below, via PgLogEventClassifier's own
               assembler-backed overload — restated here because the csv parser accepts every zone and leaves
               the decision to this filter (its own #4053 fix), the same trade PgLogEventsCollector.ReadAsync's
               csvlog arm makes on the self-hosted route. */
            var kept = PgLogEventsCollector.FilterForeignZoneEntries(entries, logTimezoneIsUtc, out foreignZoneLines);
            events = _classifier.Classify(kept);
        }
        else
        {
            if (string.IsNullOrEmpty(text))
            {
                return (0, 0, 0, nextCarry);
            }

            /* Outside IngestAsync's tolerant catch, which covers the AWS FETCH: a zone refusal is a statement
               about the target's configuration and has to reach the runner uncommitted (#3008). #4046 part 1b:
               logTimezoneIsUtc skips and counts a foreign-zone line instead of throwing, the same trade the
               self-hosted route already makes. */
            events = _classifier.Classify(text, logTimezoneIsUtc, out foreignZoneLines);
        }

        if (events.Count == 0)
        {
            return (0, foreignZoneLines, csvRecordsDiscarded, nextCarry);
        }

        return (await WriteAsync(serverId, storageName, events, cancellationToken), foreignZoneLines, csvRecordsDiscarded, nextCarry);
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
