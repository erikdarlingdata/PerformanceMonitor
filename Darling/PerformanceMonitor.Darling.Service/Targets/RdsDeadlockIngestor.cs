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

    /// <summary>#4708: where this ingestor's log positions survive a restart, or null for a source that keeps
    /// them in memory only (a test that does not need a store).</summary>
    private readonly RdsResumeStore? _resume;

    /// <summary>
    /// The csvlog partial-record carry (#4053 part c2), the same <see cref="RdsCsvlogCarryBook"/>
    /// <see cref="RdsLogEventIngestor"/> keeps — this ingestor's own book, never shared with another, for
    /// the reason <see cref="RdsLogSource"/>'s own marker remark gives.
    /// </summary>
    private readonly RdsCsvlogCarryBook _csvCarry = new();

    /// <summary>
    /// The deadlock report a chunk ended inside, held for the next chunk (#4735 item 4). One book per ingestor, like
    /// the csvlog one, for the same reason.
    /// </summary>
    private readonly RdsDeadlockCarryBook _reportCarry = new();

    /// <summary>
    /// Where the parsed rows go: null is the real COPY into the store (<see cref="WriteAsync"/>). A test that has no
    /// store sets it, so a report that is stored can be counted, and the marker save that follows it observed, without
    /// one (#4735). Nothing in the service sets it.
    /// </summary>
    internal Func<IReadOnlyList<PgDeadlocksCollector.Row>, CancellationToken, Task<int>>? RowWriter { get; init; }

    public RdsDeadlockIngestor(NpgsqlDataSource postgres, RdsLogSource? logs = null, ILogger? logger = null, RdsResumeStore? resume = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logs = logs ?? new RdsLogSource(logger: logger);
        _logger = logger;
        _resume = resume;
    }

    /// <param name="host">The target's connection host. A non-RDS host means this transport does not apply
    /// to that target — it uses the <c>pg_read_file</c> route instead — and the outcome says so rather than
    /// reporting an empty log (#3017).</param>
    /// <returns>Rows stored and whether the log was reached at all. Reaching it and finding nothing is a
    /// real statement about the log; not reaching it is not, and
    /// <see cref="RdsIngestOutcome.SourceReached"/> is what keeps the runner from making one.</returns>
    /// <exception cref="PgLogTimezoneUnsupportedException">The log is stamped in a non-UTC zone, so its
    /// timestamps are local and nothing in it can be stored (#2993). Propagated for the same reason
    /// <see cref="RdsLogUnavailableException"/> is: the runner classifies it and names the setting, where
    /// swallowing it would return zero rows and be recorded as a log that was read and held no
    /// deadlocks.</exception>
    /// <param name="pgLogUsesCsvlog">#4053 part c2: whether the target's <c>log_destination</c> includes
    /// <c>csvlog</c>, read by the caller the same way <see cref="RdsLogEventIngestor"/>'s own
    /// <c>pgLogUsesCsvlog</c> parameter is (#4053 part c1) — this ingestor reaches the log through the AWS
    /// API, so it has no connection of its own to probe with. True reads the newest <c>.csv</c> file through
    /// the shared <see cref="RdsCsvlogCarry"/> carry instead of the stderr file; false is today's stderr
    /// route, unchanged.</param>
    public async Task<RdsIngestOutcome> IngestAsync(
        int serverId,
        string storageName,
        string host,
        bool logTimezoneIsUtc = false,
        bool pgLogUsesCsvlog = false,
        CancellationToken cancellationToken = default)
    {
        /* #4708: what the last process saved for this server is loaded once, before its first read, so a
           restart resumes from the saved file and marker instead of the newest file's last lines. */
        if (_resume is not null)
        {
            await _resume.RestoreAsync(_logs, serverId, cancellationToken);
        }

        /* #4708: a rotated file is finished and the newest file opened in the SAME cycle (bounded), rather than
           the old file on one cycle and the new one on the next. */
        return await RdsLogSource.RunPassesAsync(
            () => IngestPassAsync(serverId, storageName, host, logTimezoneIsUtc, pgLogUsesCsvlog, cancellationToken));
    }

    /// <summary>
    /// One read of the log: fetch a chunk, store its rows, then commit the position and save it. The
    /// second value is <see cref="RdsLogSource.LogChunk.ReadAgain"/>: the chunk came from a file that is no
    /// longer the newest, so the caller reads again.
    /// </summary>
    private async Task<(RdsIngestOutcome Outcome, bool ReadAgain)> IngestPassAsync(
        int serverId,
        string storageName,
        string host,
        bool logTimezoneIsUtc,
        bool pgLogUsesCsvlog,
        CancellationToken cancellationToken)
    {
        RdsLogSource.LogChunk? chunk;

        var kind = pgLogUsesCsvlog ? RdsLogSource.LogFileKind.Csv : RdsLogSource.LogFileKind.Stderr;

        try
        {
            chunk = await _logs.ReadNewestAsync(host, kind, cancellationToken);
        }
        catch (PgNoCsvlogFileException)
        {
            /* #4053 part c1's own arm on the log-events ingestor, mirrored here: propagated UNWRAPPED so
               DarlingWorker's PgNoCsvlogFileException arm invalidates PgLogFormatCapability's cached verdict
               for this server, the same fix a stale "csvlog is on" cache needs on this route too. */
            throw;
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
            /* #3017: NOT_REACHED, not zero rows. ReadNewestAsync answers null for exactly one reason — the
               host is not an RDS or Aurora endpoint, so RdsEndpoint.TryParse declined and no AWS call was
               made. Every other outcome either throws or hands back a chunk. Returning a bare 0 here made
               this indistinguishable from a log that was opened and held nothing, and the runner stamped the
               cycle with a sentence claiming the second. */
            return (RdsIngestOutcome.NotReached, false);
        }

        var (carry, carryKey, droppedByRotation, currentFileName) = pgLogUsesCsvlog
            ? _csvCarry.CarryFor(chunk.Value.Resume.Key, chunk.Value.StartsAtFileStart)
            : (RdsCsvlogCarry.CsvCarry.Empty, null, 0, null);

        var (heldReport, reportKey, reportFileName) = pgLogUsesCsvlog
            ? (string.Empty, null, null)
            : _reportCarry.CarryFor(chunk.Value.Resume.Key);

        /* A chunk of a file that is no longer the newest and has nothing more pending is that file's last: a report
           it ends inside can never be finished. */
        var moreCanArrive = !chunk.Value.ReadAgain || chunk.Value.MoreAvailable;

        var (written, foreignZoneLines, csvRecordsDiscarded, raiseShapedSkipped, nextCarry, nextReport) = await StoreAsync(
            serverId, storageName, chunk.Value.Text, logTimezoneIsUtc, pgLogUsesCsvlog,
            carry, chunk.Value.MoreAvailable, heldReport, moreCanArrive, cancellationToken);

        /* THE MARKER MOVES HERE AND NOWHERE ELSE. Reaching this line means everything the chunk held is
           either in the store or was nothing to store; anything else threw out of StoreAsync above and
           left the marker where it was, so the next cycle asks RDS for the same window again.

           The order is the fix (#3008). While the marker advanced inside ReadNewestAsync, a parse fault, a
           COPY that tripped its deadline, a dropped store connection or a cancelled cycle each consumed a
           window nobody stored — and DownloadDBLogFilePortion does not hand the same bytes out twice, so
           every deadlock in it was gone with no error naming the loss. The csvlog carry moves alongside it
           for the same reason (#4053 part c2, mirroring RdsLogEventIngestor's own commit). */
        _logs.CommitResume(chunk.Value.Resume);

        var resumeAdvanced = !string.IsNullOrEmpty(chunk.Value.Resume.Marker);

        if (carryKey is not null && resumeAdvanced)
        {
            csvRecordsDiscarded += _csvCarry.Commit(carryKey, currentFileName, nextCarry, droppedByRotation);
        }

        /* The held report moves with the marker for the same reason, and only when it advanced: on a replay the same
           chunk comes back, and holding its tail twice would glue it onto itself. */
        if (reportKey is not null && resumeAdvanced)
        {
            _reportCarry.Commit(reportKey, reportFileName, nextReport);
        }

        /* #4708: the position is saved AFTER the chunk's rows are stored and the in-process position has moved, never
           before, so a crash between the two re-reads a window (rows dedupe on their identity hash) rather than
           resuming past one.

           NOT while a report is held (#4735). The held head lives in memory only, so a position saved past this chunk
           would start a restarted process in the middle of the report: no header left for Extract to find, and the
           deadlock never stored. The saved position stays where the last finished chunk left it, and a restart reads
           this chunk again; the rows before the report dedupe on their identity hash, as for any replay. The csvlog
           carry's held record is left as it was (RdsLogEventIngestor): its holds happen on most chunks, a held report
           is rare. */
        if (_resume is not null && nextReport.Length == 0)
        {
            await _resume.SaveAsync(serverId, kind, chunk.Value.Resume, cancellationToken);
        }

        return (RdsIngestOutcome.Read(written, foreignZoneLines, csvRecordsDiscarded, raiseShapedSkipped: raiseShapedSkipped,
            filesSkipped: chunk.Value.FilesSkipped, resumeFileMissing: chunk.Value.ResumeFileMissing), chunk.Value.ReadAgain);
    }

    /// <summary>
    /// Parse a chunk and store what it held, or throw. Split out so the resume marker has exactly one
    /// commit point above it: every way this can decline to store rows — empty text, a slab with no
    /// deadlocks in it — is a legitimate zero that loses nothing, and every way it can FAIL leaves via an
    /// exception rather than a zero the caller would have to tell apart from those.
    /// </summary>
    private async Task<(int Written, int ForeignZoneLines, int CsvRecordsDiscarded, int RaiseShapedSkipped, RdsCsvlogCarry.CsvCarry NextCarry, string NextReport)> StoreAsync(
        int serverId,
        string storageName,
        string text,
        bool logTimezoneIsUtc,
        bool pgLogUsesCsvlog,
        RdsCsvlogCarry.CsvCarry carry,
        bool additionalDataPending,
        string heldReport,
        bool moreCanArrive,
        CancellationToken cancellationToken)
    {
        List<PgDeadlockLogParser.ParsedDeadlock> deadlocks;
        int foreignZoneLines;
        var csvRecordsDiscarded = 0;
        var raiseShapedSkipped = 0;
        var nextCarry = RdsCsvlogCarry.CsvCarry.Empty;
        var nextReport = string.Empty;

        if (pgLogUsesCsvlog)
        {
            if (string.IsNullOrEmpty(text) && string.IsNullOrEmpty(carry.Partial))
            {
                return (0, 0, 0, 0, carry, nextReport);
            }

            var portion = RdsCsvlogCarry.ParseCsvPortion(carry, text, additionalDataPending);
            var entries = portion.Entries;
            csvRecordsDiscarded = portion.RecordsDiscarded;
            nextCarry = portion.Next;

            /* The SAME foreign-zone rule the stderr path applies below — PgLogEventsCollector's own,
               shared here rather than the private copy PgDeadlocksCollector's SQL route used to keep
               (#4053 part c2 dedupe). */
            var kept = PgLogEventsCollector.FilterForeignZoneEntries(entries, logTimezoneIsUtc, out foreignZoneLines);

            deadlocks = new List<PgDeadlockLogParser.ParsedDeadlock>();
            foreach (var entry in kept)
            {
                /* #4058 item 1: the same restricted RAISE-shaped check PgDeadlocksCollector's own csvlog
                   branch applies — checked only for an entry that already matches the deadlock candidate
                   shape FromEntry itself tests, and counted rather than falling through FromEntry's silent
                   null for "not a deadlock at all". */
                if (entry.Severity == "ERROR"
                    && entry.Message.TrimEnd() == "deadlock detected"
                    && !string.IsNullOrWhiteSpace(entry.Detail)
                    && PgDeadlockLogParser.IsRaiseShaped(entry))
                {
                    raiseShapedSkipped++;
                    continue;
                }

                var parsed = PgDeadlockLogParser.FromEntry(entry);

                if (parsed is not null)
                {
                    deadlocks.Add(parsed.Value);
                }
            }
        }
        else
        {
            if (string.IsNullOrEmpty(text))
            {
                /* Nothing new: nothing to store, and a held report has not seen a chunk. */
                return (0, 0, 0, 0, nextCarry, heldReport);
            }

            /* Not inside IngestAsync's tolerant catch, which covers the AWS FETCH. A parse refusal is a
               statement about the target's configuration rather than about reaching it, and it has to reach the
               runner to be classified.

               It also has to leave WITHOUT the marker being committed, which is why this whole method sits
               ahead of the commit rather than around it: a refused zone that consumed the window would discard
               every report in it, and the setting that caused the refusal is fixable, so those reports are
               worth still being there afterwards (#3008). */
            var portion = RdsDeadlockCarry.Step(heldReport, text, moreCanArrive);
            nextReport = portion.Next;

            if (portion.StoredUnfinished)
            {
                _logger?.LogDebug(
                    "RDS deadlock log for {Server}: a report is still unfinished after one more chunk, or nothing more can "
                    + "arrive for it; storing it as it is (#4735)",
                    storageName);
            }

            deadlocks = PgDeadlockLogParser.Extract(portion.Text, logTimezoneIsUtc, out foreignZoneLines);
        }

        if (deadlocks.Count == 0)
        {
            /* A log slab with no deadlocks in it is the ordinary case. Not worth a log line every cycle. */
            return (0, foreignZoneLines, csvRecordsDiscarded, raiseShapedSkipped, nextCarry, nextReport);
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

        return (
            RowWriter is { } writeRows
                ? await writeRows(rows, cancellationToken)
                : await WriteAsync(serverId, storageName, rows, cancellationToken),
            foreignZoneLines, csvRecordsDiscarded, raiseShapedSkipped, nextCarry, nextReport);
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

        /* The start phase's deadline. It is separate from the importer's because the importer does not
           exist until Begin returns, and the await that returns it runs under the connection's
           CommandTimeout, which Npgsql exposes read-only. StoreCopyStartDeadline carries the value, the
           stop-versus-deadline discrimination and the fault shape — see it for why a breach is re-raised
           as a TimeoutException rather than left as the cancellation Npgsql threw. */
        using var startDeadline = StoreCopyStartDeadline.Start(cancellationToken);

        /* Which COPY phase a fault came out of, on the same terms as
           DarlingCollectorRunner.CopyBatchOnceAsync — see CollectorFaultCopyPhase, which is the authority
           for what each value means and what it authorises.

           Start until Begin returns, and the transition sits INSIDE the block for that reason: Start has to
           mean strictly "the importer never came back". A fault anywhere past that line may have sent rows,
           so it must read as Data even where it happens to have sent none. */
        var copyPhase = StoreCopyPhase.Start;

        try
        {
            using (var importer = await connection.BeginBinaryImportAsync(
                PgCollectorRowWriter.CopyCommandFor(definition), startDeadline.Token))
            {
                copyPhase = StoreCopyPhase.Data;

                /* #2874: the COPY's own deadline, on NpgsqlBinaryImporter.Timeout — a TimeSpan on a different type
                   from the rest of the regime, invisible to a command-shaped regex, and inherited from the
                   connection's CommandTimeout (30 s) when left unset. Same constant, same regime, and it reaches
                   the row loop only — startDeadline above bounds the Begin that returns the importer, on the same
                   terms as DarlingCollectorRunner.CopyBatchOnceAsync. */
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
        /* The start phase's deadline, re-raised as the shape a client-side deadline has here, on the same
           terms as DarlingCollectorRunner.CopyBatchOnceAsync. A throw from a catch arm leaves the whole
           try, so this fault is stamped HERE rather than by the arm below; the phase term in the filter is
           what keeps the arm unreachable once the row loop has begun, whatever Npgsql throws from inside
           it, so a data-phase fault cannot be relabelled as the one a re-attempt trusts. */
        catch (OperationCanceledException cancellation)
            when (copyPhase == StoreCopyPhase.Start && startDeadline.Breached())
        {
            var breach = StoreCopyStartDeadline.Breach(cancellation);
            CollectorFaultCopyPhase.Stamp(breach, copyPhase);
            throw breach;
        }
        /* Stamped, then rethrown bare, for CopyBatchOnceAsync's reason: the fault keeps its own type,
           message and inner chain, so every classification arm upstream sees exactly what it sees without
           this. OperationCanceledException is excluded because a stopping token says the service is shutting
           down, not which protocol exchange was in flight. */
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            CollectorFaultCopyPhase.Stamp(ex, copyPhase);
            throw;
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
