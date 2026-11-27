/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The store reading its OWN server log (#3021) — the second self-monitoring source beside
/// <see cref="StoreSelfMetrics"/>, and the one that covers the store's runtime COMPLAINTS rather than its
/// size and its jobs.
///
/// <para><b>What it is for.</b> During the continuous-aggregate refresh convoy the store's log held the
/// other half of every client-side symptom: each <c>Exception while reading from stream</c> in the service
/// log has a paired <c>ERROR:  canceling statement due to user request</c> in the store's, and the
/// <c>FATAL:  connection to client lost</c> entries clustered inside the refresh windows rather than across
/// them. The product already documents that pairing in prose — see
/// <see cref="StoreSelfMetrics.SweepTimeoutSeconds"/> — and then had no surface that could show it. Reading
/// the file was an operator's <c>Select-String</c> over SSM, which works and is not the product.</para>
///
/// <para><b>Why a collector rather than a read on demand.</b> The log is a weekday ring that TRUNCATES
/// (<c>log_truncate_on_rotation</c>), so by the time a question is asked the file that would answer it may
/// have been overwritten by the same weekday coming round. An on-demand read can only ever see what has not
/// rotated; a capture that already happened is durable. The surface (<c>get_store_log</c>) is then a read
/// over what was captured, not a second transport — which also means it cannot race this sweep's marker.</para>
///
/// <para><b>Why it stores a CENSUS.</b> <see cref="StoreLogClassifier"/> carries that argument: a day holds
/// ~1,100 expected cancels, and a surface that lists them is one nobody reads twice.</para>
///
/// <para><b>Not a collector in the catalog sense.</b> Like <c>collect.store_metrics</c> (#2068) and
/// <c>collect.collector_cost</c> (#2674) this is INTERNAL self-telemetry: plain tables, deliberately absent
/// from <c>CollectorCatalog.All</c>, so the catalog-driven hypertable conversion and the catalog retention
/// purge can never recurse onto the tables that observe them, and no <c>collection_log</c> row is written.
/// That last point is why <c>collect.store_log_captures</c> exists at all: every other sampled read borrows
/// its denominator from <c>collection_log</c> (the <c>get_pg_blocking</c> convention), and this source has no
/// row there to borrow from.</para>
///
/// <para><b>What it never does.</b> It parses no timestamp and no <c>log_line_prefix</c>. See
/// <see cref="StoreLogClassifier"/> for why that is a decision rather than an omission — the short version is
/// that <c>%m</c> renders in <c>log_timezone</c>, which <c>DarlingManagedPostgres</c> deliberately leaves to
/// the host, so the store's own log stamps are host-local and a census binned on the sweep's UTC capture
/// instant needs none of them.</para>
/// </summary>
public static class StoreLogSweep
{
    /// <summary>
    /// Per-statement command timeout. Small on purpose and much smaller than
    /// <see cref="StoreSelfMetrics.SweepTimeoutSeconds"/>: every statement here is either a catalog listing
    /// or a bounded file read, so a slow one means the store is in trouble — and this sweep runs on the same
    /// awaited hourly tick, where patience costs per-server dispatch.
    /// </summary>
    public const int SweepTimeoutSeconds = 60;

    /// <summary>Ceiling on files examined in one capture. The weekday ring is seven files and normally one or
    /// two hold unread bytes; the bound exists so an operator who repointed <c>log_directory</c> at something
    /// large cannot turn this into the incident.</summary>
    public const int MaxFilesPerCapture = 8;

    /// <summary>
    /// How many slabs one capture reads across all files (#8904): the first slab of each file, then further slabs of a
    /// file that still has bytes pending. A capture therefore reads at most
    /// <c>MaxSlabsPerCapture x</c> <see cref="StoreLogSlab.MaxBytesPerRead"/> (32 MiB), and a backlog larger than
    /// that takes the least number of captures <see cref="CapturesNeeded"/> says. One slab is held at a time.
    /// </summary>
    public const int MaxSlabsPerCapture = 8;

    /// <summary>The most log one capture reads (#8904).</summary>
    public const long MaxBytesPerCapture = (long)MaxSlabsPerCapture * StoreLogSlab.MaxBytesPerRead;

    /// <summary>How long a capture keeps reading on (#8904), a tenth of the tick's shared
    /// <see cref="StoreSelfMetrics.SweepTimeoutSeconds"/> budget, so it stops by itself well before that budget cancels
    /// the pass. Slabs already read are kept either way.</summary>
    public static readonly TimeSpan ReadOnBudget = TimeSpan.FromSeconds(StoreSelfMetrics.SweepTimeoutSeconds / 10);

    /// <summary>The least number of captures a backlog needs if nothing more is written (#8904).</summary>
    public static long CapturesNeeded(long bytesPending) =>
        bytesPending <= 0 ? 0 : (bytesPending + MaxBytesPerCapture - 1) / MaxBytesPerCapture;

    /// <summary>How long the census is kept — the same 400 days as the store's own growth series
    /// (<see cref="StoreSelfMetrics.RetentionDays"/>), so the two self-telemetry surfaces answer over one
    /// window and a rate can be compared against the same period last year. Enforced by this sweep's own
    /// bounded DELETEs, not a retention policy.</summary>
    public const int RetentionDays = 400;

    /// <summary>
    /// Every file in the server's log directory with its marker beside it, oldest first.
    ///
    /// <para><c>pg_ls_logdir()</c> rather than a configured name, for
    /// <see cref="StoreLogSweep"/>'s own reason as much as the log routes': <c>log_filename</c> is a
    /// strftime pattern, so the real names are only knowable by asking. A LEFT JOIN and no WHERE clause: the
    /// resume decision is <see cref="StoreLogSlab.ResolveResume"/>'s, which is pure and pinned, and pushing
    /// it into SQL as a size comparison is exactly the shape that misses the truncated-to-the-same-size
    /// corner. Seven rows, so filtering server-side buys nothing.</para>
    /// <para>Ordered oldest-modified first so a capture spanning a rotation reads the outgoing file before
    /// the incoming one.</para>
    /// </summary>
    public const string LogDirectoryListSql = @"
SELECT
    l.name           AS log_file,
    l.size           AS size_bytes,
    m.byte_offset    AS stored_offset,
    m.last_size      AS stored_last_size
FROM pg_catalog.pg_ls_logdir() AS l
LEFT JOIN config.store_log_read_marker AS m
  ON m.log_file = l.name
ORDER BY l.modification, l.name";

    /// <summary>
    /// One bounded read of one log file. $1 file name, $2 offset, $3 length.
    ///
    /// <para><c>pg_read_binary_file</c> rather than <c>pg_read_file</c>, which is what the two log
    /// collectors use: a byte offset can land between the bytes of one character, and the text form would
    /// either raise an encoding error or hand back a mangled first character. Bytes let
    /// <see cref="StoreLogSlab.TrimToLastNewline"/> cut at a newline — the one byte that cannot be part of a
    /// multi-byte character — before anything decodes.</para>
    ///
    /// <para>The path comes from <c>current_setting('log_directory')</c>, which is how the two log
    /// collectors build theirs too. Darling's own v6 block sets it to <c>'log'</c>, so the two
    /// agree on a managed store; asking is what keeps this correct on a store whose owner moved it, and the
    /// absolute-path case is readable because the managed store's <c>darling</c> role is the cluster's
    /// bootstrap superuser.</para>
    /// </summary>
    public const string ReadFileSql = @"
SELECT pg_catalog.pg_read_binary_file(
           pg_catalog.current_setting('log_directory') || '/' || $1,
           $2,
           $3)";

    /// <summary>The census rows for one capture. $1 capture_time, then five parallel arrays.</summary>
    public const string EventInsertSql = @"
INSERT INTO collect.store_log_events
    (capture_time, event_class, severity, occurrences, message_text, sample_line)
SELECT
    $1,
    c.event_class,
    c.severity,
    c.occurrences,
    c.message_text,
    c.sample_line
FROM unnest($2::text[], $3::text[], $4::integer[], $5::text[], $6::text[])
     AS c(event_class, severity, occurrences, message_text, sample_line)";

    /// <summary>
    /// The capture row — the DENOMINATOR, written on every capture including one that classified nothing,
    /// because an absent capture and a capture that found nothing are otherwise the same absence of rows.
    /// $1 capture_time, $2 log_file, $3 bytes_read, $4 bytes_pending, $5 lines_read, $6 entries_read,
    /// $7 offset_reset, $8 groups_dropped.
    /// </summary>
    public const string CaptureInsertSql = @"
INSERT INTO collect.store_log_captures
    (capture_time, log_file, bytes_read, bytes_pending, lines_read, entries_read, offset_reset, groups_dropped)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";

    /// <summary>A further slab of a file already captured this tick (#8904): adds to that capture row instead of
    /// writing a second one, so a capture stays one row per file. $1 capture_time, $2 log_file, $3 bytes_read,
    /// $4 bytes_pending (the level after this slab), $5 lines_read, $6 entries_read, $7 groups_dropped.</summary>
    public const string CaptureAddSql = @"
UPDATE collect.store_log_captures
SET bytes_read     = bytes_read + $3,
    bytes_pending  = $4,
    lines_read     = lines_read + $5,
    entries_read   = entries_read + $6,
    groups_dropped = groups_dropped + $7
WHERE capture_time = $1
AND   log_file     = $2";

    /// <summary>The resume marker, per FILE because rotation is by weekday name. $1 log_file, $2 byte_offset,
    /// $3 last_size, $4 updated_at.</summary>
    public const string MarkerUpsertSql = @"
INSERT INTO config.store_log_read_marker (log_file, byte_offset, last_size, updated_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (log_file) DO UPDATE
SET byte_offset = excluded.byte_offset,
    last_size   = excluded.last_size,
    updated_at  = excluded.updated_at";

    /// <summary>The census' own retention. $1 cutoff (naive UTC).</summary>
    public const string EventRetentionDeleteSql = @"
DELETE FROM collect.store_log_events
WHERE capture_time < $1";

    /// <summary>The denominator's own retention, on the same cutoff so a window never holds events without
    /// the captures that qualify them. $1 cutoff (naive UTC).</summary>
    public const string CaptureRetentionDeleteSql = @"
DELETE FROM collect.store_log_captures
WHERE capture_time < $1";

    /// <summary>What one file's read decided, before it is written. Exposed so the sweep's arithmetic is
    /// assertable without a store.</summary>
    /// <param name="LogFile">The file name as <c>pg_ls_logdir()</c> gave it.</param>
    /// <param name="Offset">Where the read started.</param>
    /// <param name="BytesRead">Bytes accounted for — complete lines only.</param>
    /// <param name="BytesPending">Bytes left unread in the file after this capture.</param>
    /// <param name="OffsetReset">The ring came round under the marker.</param>
    /// <param name="Census">The classification.</param>
    public readonly record struct FileCapture(
        string LogFile,
        long Offset,
        int BytesRead,
        long BytesPending,
        bool OffsetReset,
        StoreLogClassifier.Census Census);

    /// <summary>How many stored rows one re-mask pass examines (#3915, #3944): a bounded slice per hourly tick, so
    /// a store with a long retained history finishes over a few ticks instead of overrunning one tick's
    /// budget.</summary>
    public const int MaxRemaskRowsPerPass = 5000;

    /// <summary>How long one re-mask slice may run (#3920's review). It shares the hourly self-metrics budget,
    /// and a slice that ran that budget out would take the collector-cost flush after it down too; a slice that
    /// runs out of its own time gives up alone and resumes from its cursor next hour.</summary>
    public static readonly TimeSpan RemaskSliceBudget = TimeSpan.FromSeconds(60);

    /// <summary>One page of rows to re-mask, in physical order after <c>$1</c> (null for the start).</summary>
    public const string RemaskPageSql = @"
SELECT
    e.ctid::text,
    e.event_class,
    e.message_text,
    e.sample_line
FROM collect.store_log_events AS e
WHERE ($1::tid IS NULL OR e.ctid > $1::tid)
AND   (e.message_text IS NOT NULL OR e.sample_line IS NOT NULL)
ORDER BY e.ctid
LIMIT $2";

    /// <summary>Writes one row's re-masked key and sample back, by the physical row it was read from.</summary>
    public const string RemaskUpdateSql = @"
UPDATE collect.store_log_events
SET message_text = $1,
    sample_line = $2
WHERE ctid = $3::tid";

    /// <summary>
    /// Brings rows stored before this build to its rules (#3915, #3944), one bounded slice per call. The
    /// releases that captured the store's log (3.7.0 through 3.8.0) kept each entry whole, so an ERROR's
    /// STATEMENT line with a password literal, and the first #3899 build's raw slow statements, sit in
    /// <c>collect.store_log_events</c> for the sweep's 400-day retention, readable by the viewer and mcp roles;
    /// and their message is the text rather than its grouping key. Each row goes through
    /// <see cref="StoreLogClassifier.MaskStoredEvent"/>, which normalizes its SQL and re-keys its message the way
    /// a new capture does, and is rewritten only if that changed it; the function is idempotent, so rows this
    /// build wrote come back unchanged (bar a sample the cap cut inside a token, rewritten once) and a pass that is
    /// cut short loses nothing. Returns the cursor to resume from, null once the table's end is reached, with how
    /// many rows were examined and rewritten.
    /// </summary>
    public static async Task<(string? NextCursor, int Examined, int Rewritten)> RemaskStoredEventsAsync(
        NpgsqlConnection connection, string? afterCursor, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var page = new List<(string Ctid, string EventClass, string? Message, string? Sample)>();
        await using (var select = new NpgsqlCommand(RemaskPageSql, connection) { CommandTimeout = SweepTimeoutSeconds })
        {
            select.Parameters.Add(new NpgsqlParameter { Value = (object?)afterCursor ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            select.Parameters.AddWithValue(MaxRemaskRowsPerPass);
            await using var reader = await select.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                page.Add((
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetString(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3)));
            }
        }

        var rewritten = 0;
        foreach (var row in page)
        {
            var (message, sample) = StoreLogClassifier.MaskStoredEvent(row.EventClass, row.Message, row.Sample);
            if (string.Equals(message, row.Message, StringComparison.Ordinal)
                && string.Equals(sample, row.Sample, StringComparison.Ordinal))
            {
                continue;
            }

            await using var update = new NpgsqlCommand(RemaskUpdateSql, connection) { CommandTimeout = SweepTimeoutSeconds };
            update.Parameters.Add(new NpgsqlParameter { Value = (object?)message ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            update.Parameters.Add(new NpgsqlParameter { Value = (object?)sample ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            update.Parameters.AddWithValue(row.Ctid);
            rewritten += await update.ExecuteNonQueryAsync(cancellationToken);
        }

        var next = page.Count < MaxRemaskRowsPerPass ? null : page[^1].Ctid;
        return (next, page.Count, rewritten);
    }

    /// <summary>
    /// One capture: list the log directory, read what is unread in each file, classify it, and write the
    /// census, the capture row and the advanced marker.
    ///
    /// <para><b>The marker moves inside the SAME transaction as the rows it accounts for.</b> #3008 had to
    /// solve this by ordering — commit the RDS marker strictly after the write, because the marker lives in
    /// the ingestor's memory and the rows live in the store, and nothing can make the two atomic. Here both
    /// live in the store we own, so the ordering problem does not arise: either a file's rows and its new
    /// offset both land, or neither does and the next capture reads the same bytes again. One transaction
    /// per FILE rather than per capture, so a fault reading the second file does not discard the first
    /// file's progress.</para>
    ///
    /// <para>Returns what was captured, per file, so the caller can log it and the tests can assert it.</para>
    /// </summary>
    public static Task<List<FileCapture>> SweepAsync(
        NpgsqlConnection connection,
        DateTime utcNow,
        ILogger? logger,
        CancellationToken cancellationToken = default) =>
        SweepAsync(connection, utcNow, logger, files: null, cancellationToken);

    /// <summary>The log files a capture reads, as a seam (#8904): the store's own <c>pg_ls_logdir()</c> and
    /// <c>pg_read_binary_file</c> in the service, and a fixed set of bytes in a test, because a scratch store has no
    /// logging collector and so no log file to read.</summary>
    internal interface IStoreLogFiles
    {
        /// <summary>Every log file with its size, oldest first.</summary>
        Task<IReadOnlyList<(string Name, long Size)>> ListAsync(CancellationToken cancellationToken);

        /// <summary>Up to <paramref name="length"/> bytes of a file from <paramref name="offset"/>.</summary>
        Task<byte[]> ReadAsync(string name, long offset, int length, CancellationToken cancellationToken);
    }

    /// <summary>The marker of every file, for a source that lists no markers itself.</summary>
    private const string MarkerListSql = @"
SELECT m.log_file, m.byte_offset, m.last_size
FROM config.store_log_read_marker AS m";

    /// <summary><see cref="SweepAsync(NpgsqlConnection, DateTime, ILogger?, CancellationToken)"/> over a given set of log
    /// files; null reads the store's own log directory.</summary>
    internal static Task<List<FileCapture>> SweepAsync(
        NpgsqlConnection connection,
        DateTime utcNow,
        ILogger? logger,
        IStoreLogFiles? files,
        CancellationToken cancellationToken = default) =>
        SweepAsync(connection, utcNow, logger, files, StoreLockWaits.Shared, null, cancellationToken);

    /// <summary>The sweep over a given set of log files with <paramref name="readOnBudget"/> in place of
    /// <see cref="ReadOnBudget"/>, for a test whose point is the byte and slab bounds, so a slow machine cannot end its
    /// capture early.</summary>
    internal static Task<List<FileCapture>> SweepAsync(
        NpgsqlConnection connection,
        DateTime utcNow,
        ILogger? logger,
        IStoreLogFiles? files,
        TimeSpan readOnBudget,
        CancellationToken cancellationToken) =>
        SweepAsync(connection, utcNow, logger, files, StoreLockWaits.Shared, readOnBudget, cancellationToken);

    /// <summary>The sweep over a given set of log files and a given map of sessions (#9439): the service's own map in
    /// production, a test's in a test.</summary>
    internal static Task<List<FileCapture>> SweepAsync(
        NpgsqlConnection connection,
        DateTime utcNow,
        ILogger? logger,
        IStoreLogFiles? files,
        StoreLockWaits.PidMap pids,
        CancellationToken cancellationToken) =>
        SweepAsync(connection, utcNow, logger, files, pids, null, cancellationToken);

    private static async Task<List<FileCapture>> SweepAsync(
        NpgsqlConnection connection,
        DateTime utcNow,
        ILogger? logger,
        IStoreLogFiles? files,
        StoreLockWaits.PidMap pids,
        TimeSpan? readOnBudget,
        CancellationToken cancellationToken)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        /* Naive UTC by the product-wide cross-store contract — the same shape every collector stamps, and
           one value for the whole capture so a capture's rows join. */
        var captureTime = DateTime.SpecifyKind(utcNow, DateTimeKind.Unspecified);
        var captured = new List<FileCapture>();

        var candidates = await ListAsync(connection, files, cancellationToken);

        /* Read once per sweep, with the store login: the wording of a row-carrying DETAIL follows it, so an entry kept
           under a catalogue other than English keeps no DETAIL (StoreLogClassifier.WithheldDetail). A read that
           fails counts as not English. */
        var detailInEnglish = candidates.Count > 0 && await ReadDetailInEnglishAsync(connection, cancellationToken);

        /* #8904: the first slab of every file is read as it always was. A file that still has bytes pending after that
           is read on, oldest file first, one slab at a time, until the capture has read MaxSlabsPerCapture slabs in
           all, the file ends, or the read-on budget runs out. Each slab is its own transaction (its census rows, the
           capture row's totals and the marker move together), so a budget that cancels mid-way keeps every slab it
           finished, and each slab's text is classified and released before the next is read. */
        var clock = Stopwatch.StartNew();
        var timeBudget = readOnBudget ?? ReadOnBudget;
        var byteBudget = new ByteBudget(MaxBytesPerCapture, clock, timeBudget);
        var slabsRead = 0;
        /* #9439: ends of lock waits whose opening the store did not hold when their slab was written. The wait may open in a slab read later in
           this same capture (an older file's later slab), so they are retried once the capture has read on. */
        var unmatchedCloses = new List<StoreLockWaits.UnmatchedClose>();
        var open = new List<(int Index, Candidate Candidate, long NextOffset, List<StoreLogClassifier.Group> Groups, StoreLogClassifier.CaptureBudget Budget)>();

        foreach (var candidate in candidates)
        {
            if (byteBudget.Remaining <= 0)
            {
                /* The capture's bytes are spent, on earlier files' slabs or on the scan past a long line: this file waits for the next capture. */
                break;
            }

            var resume = StoreLogSlab.ResolveResume(candidate.StoredOffset, candidate.StoredLastSize, candidate.SizeBytes);
            if (!resume.HasWork)
            {
                continue;
            }

            var budget = new StoreLogClassifier.CaptureBudget();
            var step = await ReadSlabAsync(connection, files, candidate, resume.Offset, detailInEnglish, budget, byteBudget, cancellationToken);
            if (step is null)
            {
                /* Nothing complete yet - one partial line at the end of a file being written. The marker
                   stays where it is, so the line is read whole next time rather than half now. */
                continue;
            }

            var newOffset = resume.Offset + step.Value.Slab.BytesConsumed;
            var census = step.Value.Census;
            var capture = new FileCapture(
                LogFile: candidate.LogFile,
                Offset: resume.Offset,
                BytesRead: step.Value.Slab.BytesConsumed,
                BytesPending: Math.Max(0, candidate.SizeBytes - newOffset),
                OffsetReset: resume.OffsetReset,
                Census: census);

            await WriteAsync(connection, captureTime, capture, newOffset, candidate.SizeBytes, firstSlab: true, step.Value.Waits, pids, unmatchedCloses, StoreCheckpoints.Parse(step.Value.Slab.Text), logger, cancellationToken);
            captured.Add(capture);
            slabsRead++;

            if (capture.BytesPending > 0)
            {
                open.Add((captured.Count - 1, candidate, newOffset, new List<StoreLogClassifier.Group>(census.Groups), budget));
            }

            if (captured.Count >= MaxFilesPerCapture)
            {
                break;
            }
        }

        foreach (var file in open)
        {
            var nextOffset = file.NextOffset;
            while (slabsRead < MaxSlabsPerCapture
                   && clock.Elapsed < timeBudget
                   && byteBudget.Remaining > 0
                   && file.Candidate.SizeBytes > nextOffset)
            {
                var step = await ReadSlabAsync(connection, files, file.Candidate, nextOffset, detailInEnglish, file.Budget, byteBudget, cancellationToken);
                if (step is null)
                {
                    break;
                }

                var slabOffset = nextOffset;
                nextOffset += step.Value.Slab.BytesConsumed;
                var census = step.Value.Census;
                var before = captured[file.Index];

                /* The slab's own counts go to the store, so a slab's figures are added to the capture row and
                   never counted twice. */
                var slabCapture = new FileCapture(
                    LogFile: before.LogFile,
                    Offset: slabOffset,
                    BytesRead: step.Value.Slab.BytesConsumed,
                    BytesPending: Math.Max(0, file.Candidate.SizeBytes - nextOffset),
                    OffsetReset: false,
                    Census: census);
                await WriteAsync(connection, captureTime, slabCapture, nextOffset, file.Candidate.SizeBytes, firstSlab: false, step.Value.Waits, pids, unmatchedCloses, StoreCheckpoints.Parse(step.Value.Slab.Text), logger, cancellationToken);

                file.Groups.AddRange(census.Groups);
                captured[file.Index] = new FileCapture(
                    LogFile: before.LogFile,
                    Offset: before.Offset,
                    BytesRead: before.BytesRead + slabCapture.BytesRead,
                    BytesPending: slabCapture.BytesPending,
                    OffsetReset: before.OffsetReset,
                    Census: new StoreLogClassifier.Census(
                        Groups: file.Groups,
                        LinesRead: before.Census.LinesRead + census.LinesRead,
                        EntriesRead: before.Census.EntriesRead + census.EntriesRead,
                        ContinuationLines: before.Census.ContinuationLines + census.ContinuationLines,
                        GroupsDropped: before.Census.GroupsDropped + census.GroupsDropped));
                slabsRead++;
            }
        }

        await StoreLockWaits.RetryClosesAsync(connection, unmatchedCloses, cancellationToken);

        if (ShouldWriteHeartbeat(captured.Count, candidates.Count))
        {
            /* THE HEARTBEAT, and it is the whole reason collect.store_log_captures is a table.
               An hour in which no file in the ring had a complete new line - a quiet self-hosted store with
               log_checkpoints off and no warnings - would otherwise write NO row at all, and the read would
               then see fewer captures than the cadence produces and say the sweep skipped a tick. That is a
               false outage on a healthy store, which is the cry-wolf failure this whole shape is arranged
               against. So a tick that found nothing still records that it RAN and found nothing.

               Deliberately NOT written when the directory listing itself came back empty: that is not a
               quiet hour, it is a server with no logging collector writing files for pg_ls_logdir() to
               list, and get_store_log's not_collected arm names exactly that. Recording a heartbeat there
               would turn a real permanent gap into a series of successful empty reads. */
            var newest = candidates[^1];
            long pending = 0;
            foreach (var candidate in candidates)
            {
                var resume = StoreLogSlab.ResolveResume(candidate.StoredOffset, candidate.StoredLastSize, candidate.SizeBytes);
                pending += Math.Max(0, candidate.SizeBytes - resume.Offset);
            }

            var heartbeat = new FileCapture(
                LogFile: newest.LogFile,
                Offset: 0,
                BytesRead: 0,
                BytesPending: pending,
                OffsetReset: false,
                Census: StoreLogClassifier.Classify(null));

            await WriteHeartbeatAsync(connection, captureTime, heartbeat, cancellationToken);
            captured.Add(heartbeat);
        }

        await PurgeAsync(connection, captureTime.AddDays(-RetentionDays), cancellationToken);

        if (captured.Count > 0)
        {
            var entries = 0;
            foreach (var capture in captured)
            {
                entries += capture.Census.EntriesRead;
            }

            logger?.LogDebug(
                "Store log capture read {Files} file(s), {Entries} log entries at {CaptureTime}",
                captured.Count, entries, captureTime);
        }

        return captured;
    }

    /// <summary>
    /// Whether a tick that consumed nothing still records that it RAN — pure, so the decision is assertable
    /// without a store.
    ///
    /// <para>TRUE when the log directory held files but none of them had a complete new line: a quiet
    /// self-hosted store with <c>log_checkpoints</c> off and no warnings that hour. Without the row, the
    /// read sees fewer captures than the cadence produces and reports a skipped tick — a false outage on a
    /// healthy store, and the cry-wolf failure this whole shape is arranged against.</para>
    ///
    /// <para>FALSE when the directory listing itself came back empty, and that asymmetry is the point: that
    /// is not a quiet hour, it is a server with no logging collector writing files for
    /// <c>pg_ls_logdir()</c> to list, which <c>get_store_log</c> reports as <c>not_collected</c> and names.
    /// A heartbeat there would turn a real permanent gap into a series of successful empty reads.</para>
    /// </summary>
    public static bool ShouldWriteHeartbeat(int consumedFiles, int candidateFiles) =>
        consumedFiles == 0 && candidateFiles > 0;

    /// <summary>
    /// Whether a file in the log directory is the stderr-format log <see cref="StoreLogClassifier"/> reads (#3944's
    /// review). A store that also logs to csvlog or jsonlog writes <c>.csv</c> / <c>.json</c> files beside it (the
    /// name with its <c>.log</c> suffix swapped, or the suffix appended). Their records have no tab continuation
    /// and no <c>SEVERITY:  </c> field, so a record whose text happened to hold one would open an entry that took
    /// in the records after it, their statement columns included, and a retained entry keeps its prose as written.
    /// </summary>
    public static bool IsStderrLogFile(string logFile) =>
        !logFile.EndsWith(".csv", StringComparison.OrdinalIgnoreCase)
        && !logFile.EndsWith(".json", StringComparison.OrdinalIgnoreCase);

    /// <summary>One row of <see cref="LogDirectoryListSql"/>.</summary>
    private readonly record struct Candidate(string LogFile, long SizeBytes, long? StoredOffset, long? StoredLastSize);

    private const string LcMessagesSql = "SHOW lc_messages";

    /// <summary>Whether the store's <c>lc_messages</c> writes English (<see cref="StoreLogClassifier.IsEnglishLcMessages"/>);
    /// false when it cannot be read.</summary>
    private static async Task<bool> ReadDetailInEnglishAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        try
        {
            await using var command = new NpgsqlCommand(LcMessagesSql, connection) { CommandTimeout = SweepTimeoutSeconds };
            var value = await command.ExecuteScalarAsync(cancellationToken);
            return StoreLogClassifier.IsEnglishLcMessages(value as string);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }
    }

    private static async Task<List<Candidate>> ListAsync(NpgsqlConnection connection, IStoreLogFiles? files, CancellationToken cancellationToken)
    {
        var rows = new List<Candidate>();

        if (files is not null)
        {
            var markers = new Dictionary<string, (long Offset, long LastSize)>(StringComparer.Ordinal);
            await using (var markerCommand = new NpgsqlCommand(MarkerListSql, connection) { CommandTimeout = SweepTimeoutSeconds })
            await using (var markerReader = await markerCommand.ExecuteReaderAsync(cancellationToken))
            {
                while (await markerReader.ReadAsync(cancellationToken))
                {
                    markers[markerReader.GetString(0)] = (markerReader.GetInt64(1), markerReader.GetInt64(2));
                }
            }

            foreach (var (name, size) in await files.ListAsync(cancellationToken))
            {
                if (!IsStderrLogFile(name))
                {
                    continue;
                }

                var found = markers.TryGetValue(name, out var marker);
                rows.Add(new Candidate(name, size, found ? marker.Offset : null, found ? marker.LastSize : null));
            }

            return rows;
        }

        await using var command = new NpgsqlCommand(LogDirectoryListSql, connection) { CommandTimeout = SweepTimeoutSeconds };
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            if (!IsStderrLogFile(reader.GetString(0)))
            {
                continue;
            }

            rows.Add(new Candidate(
                LogFile: reader.GetString(0),
                SizeBytes: reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                StoredOffset: reader.IsDBNull(2) ? null : reader.GetInt64(2),
                StoredLastSize: reader.IsDBNull(3) ? null : reader.GetInt64(3)));
        }

        return rows;
    }

    /// <summary>The bytes one capture may still read (#9314): every slab, the first of a file included, and every scan
    /// chunk past a line longer than one slab draw on it, and a slab is read no longer than what is left, so the capture's
    /// total stays at <see cref="MaxBytesPerCapture"/>. A scan also stops once the capture's time budget
    /// (<see cref="ReadOnBudget"/>) is spent, as it does when the bytes are.</summary>
    private sealed class ByteBudget(long remaining, Stopwatch clock, TimeSpan timeBudget)
    {
        public long Remaining { get; set; } = remaining;

        public bool ScanMayContinue => Remaining > 0 && clock.Elapsed < timeBudget;
    }

    /// <summary>How much of a long line one scan read holds (#9314).</summary>
    internal const int LongLineScanChunkBytes = 1024 * 1024;

    /// <summary>
    /// One slab read and classified: the trimmed text, and what it classified to. Null when the read held no complete
    /// line and the marker should stay where it is.
    ///
    /// <para><b>A line longer than one slab (#9314).</b> A full slab with no newline in it is either a line the server is
    /// still writing, which waits as it always did (no newline before the end of the file), or a finished line. A
    /// finished line is handed on by its first bytes and the marker moves past its newline, found by scanning on in
    /// chunks. The scan draws on <paramref name="byteBudget"/>: if the capture's bytes or time run out first, the marker
    /// moves to where the scan stopped, so it moves forward on every capture whatever the line's length.</para>
    ///
    /// <para><b>A marker inside a line.</b> That stop leaves the marker mid-line, and a marker whose preceding byte is not
    /// a newline is mid-line (a marker otherwise only ever lands after a newline, or at the start of a file). The read
    /// starts one byte early to see that byte, and the rest of the line is dropped up to its newline before anything is
    /// handed on, so a tail is never classified as a line.</para>
    /// </summary>
    private static async Task<(StoreLogSlab.Slab Slab, StoreLogClassifier.Census Census, StoreLockWaits.ParseResult Waits)?> ReadSlabAsync(
        NpgsqlConnection connection,
        IStoreLogFiles? files,
        Candidate candidate,
        long offset,
        bool detailInEnglish,
        StoreLogClassifier.CaptureBudget budget,
        ByteBudget byteBudget,
        CancellationToken cancellationToken)
    {
        var probe = offset > 0 ? 1 : 0;
        var length = (int)Math.Min(Math.Min(StoreLogSlab.MaxBytesPerRead, candidate.SizeBytes - offset), byteBudget.Remaining);
        var raw = await ReadRangeAsync(connection, files, candidate.LogFile, offset - probe, length + probe, cancellationToken);
        byteBudget.Remaining -= Math.Max(0, raw.Length - probe);
        if (raw.Length <= probe)
        {
            return null;
        }

        var body = raw.AsMemory(probe);
        if (probe == 1 && raw[0] != 0x0A)
        {
            var tailEnd = body.Span.IndexOf((byte)0x0A);
            if (tailEnd >= 0)
            {
                return (new StoreLogSlab.Slab(string.Empty, tailEnd + 1), StoreLogClassifier.Classify(null), StoreLockWaits.ParseResult.Empty);
            }

            if (body.Length < length || offset + body.Length >= candidate.SizeBytes)
            {
                /* The line has not ended by the end of the file: it is still being written, so wait. */
                return null;
            }

            return (new StoreLogSlab.Slab(string.Empty, body.Length), StoreLogClassifier.Classify(null), StoreLockWaits.ParseResult.Empty);
        }

        var slab = StoreLogSlab.TrimToLastNewline(body.Span);
        if (slab.BytesConsumed == 0)
        {
            if (body.Length < StoreLogSlab.MaxBytesPerRead)
            {
                return null;
            }

            var (lineEnd, found) = await ScanForNewlineAsync(connection, files, candidate, offset + body.Length, byteBudget, cancellationToken);
            if (!found && lineEnd >= candidate.SizeBytes)
            {
                return null;
            }

            slab = StoreLogSlab.TrimLongLine(body.Span, (int)(lineEnd - offset));
        }

        return (slab, StoreLogClassifier.Classify(slab.Text, detailInEnglish, budget, lastLineCut: slab.Cut), StoreLockWaits.Parse(slab.Text, detailInEnglish));
    }

    /// <summary>Scans on from <paramref name="from"/> for the first newline, <see cref="LongLineScanChunkBytes"/> at a
    /// time and keeping none of it, until the file ends or <paramref name="byteBudget"/> is spent, in bytes or in time (#9314). Returns the
    /// offset just past the newline with Found true, or the offset the scan stopped at with Found false.</summary>
    private static async Task<(long Offset, bool Found)> ScanForNewlineAsync(
        NpgsqlConnection connection,
        IStoreLogFiles? files,
        Candidate candidate,
        long from,
        ByteBudget byteBudget,
        CancellationToken cancellationToken)
    {
        var at = from;
        while (at < candidate.SizeBytes && byteBudget.ScanMayContinue)
        {
            var length = (int)Math.Min(LongLineScanChunkBytes, Math.Min(candidate.SizeBytes - at, byteBudget.Remaining));
            var chunk = await ReadRangeAsync(connection, files, candidate.LogFile, at, length, cancellationToken);
            if (chunk.Length == 0)
            {
                return (candidate.SizeBytes, false);
            }

            byteBudget.Remaining -= chunk.Length;
            var newline = chunk.AsSpan().IndexOf((byte)0x0A);
            if (newline >= 0)
            {
                return (at + newline + 1, true);
            }

            at += chunk.Length;
        }

        return (at, false);
    }

    private static Task<byte[]> ReadRangeAsync(
        NpgsqlConnection connection,
        IStoreLogFiles? files,
        string logFile,
        long offset,
        int length,
        CancellationToken cancellationToken) =>
        files is null
            ? ReadAsync(connection, logFile, offset, length, cancellationToken)
            : files.ReadAsync(logFile, offset, length, cancellationToken);

    private static async Task<byte[]> ReadAsync(
        NpgsqlConnection connection,
        string logFile,
        long offset,
        int length,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(ReadFileSql, connection) { CommandTimeout = SweepTimeoutSeconds };
        command.Parameters.AddWithValue(logFile);
        command.Parameters.AddWithValue(offset);
        command.Parameters.AddWithValue((long)length);

        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value as byte[] ?? [];
    }

    private static async Task WriteAsync(
        NpgsqlConnection connection,
        DateTime captureTime,
        FileCapture capture,
        long newOffset,
        long sizeBytes,
        bool firstSlab,
        StoreLockWaits.ParseResult waits,
        StoreLockWaits.PidMap pids,
        List<StoreLockWaits.UnmatchedClose> unmatchedCloses,
        StoreCheckpoints.Parsed checkpoints,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        /* #9439: the lock waits of this slab, in the slab's own transaction, so the rows and the marker that
           accounts for them land together or not at all. */
        await StoreLockWaits.WriteAsync(connection, transaction, waits, pids, captureTime, unmatchedCloses, cancellationToken);

        var groups = capture.Census.Groups;
        if (groups.Count > 0)
        {
            var classes = new string[groups.Count];
            var severities = new string[groups.Count];
            var occurrences = new int[groups.Count];
            var messages = new string?[groups.Count];
            var samples = new string?[groups.Count];

            for (var i = 0; i < groups.Count; i++)
            {
                classes[i] = groups[i].EventClass;
                severities[i] = groups[i].Severity;
                occurrences[i] = groups[i].Occurrences;
                messages[i] = groups[i].MessageText;
                samples[i] = groups[i].SampleLine;
            }

            await using var events = new NpgsqlCommand(EventInsertSql, connection, transaction) { CommandTimeout = SweepTimeoutSeconds };
            events.Parameters.AddWithValue(captureTime);
            events.Parameters.AddWithValue(classes);
            events.Parameters.AddWithValue(severities);
            events.Parameters.AddWithValue(occurrences);
            events.Parameters.AddWithValue(messages);
            events.Parameters.AddWithValue(samples);
            await events.ExecuteNonQueryAsync(cancellationToken);
        }

        /* #9414: the slab's checkpoint lines, in the same transaction as its census and its marker, behind a savepoint of their own. */
        await StoreCheckpoints.WriteAsync(connection, transaction, checkpoints, captureTime, logger, SweepTimeoutSeconds, cancellationToken);

        if (firstSlab)
        {
            await using var row = new NpgsqlCommand(CaptureInsertSql, connection, transaction) { CommandTimeout = SweepTimeoutSeconds };
            row.Parameters.AddWithValue(captureTime);
            row.Parameters.AddWithValue(capture.LogFile);
            row.Parameters.AddWithValue((long)capture.BytesRead);
            row.Parameters.AddWithValue(capture.BytesPending);
            row.Parameters.AddWithValue(capture.Census.LinesRead);
            row.Parameters.AddWithValue(capture.Census.EntriesRead);
            row.Parameters.AddWithValue(capture.OffsetReset);
            row.Parameters.AddWithValue(capture.Census.GroupsDropped);
            await row.ExecuteNonQueryAsync(cancellationToken);
        }
        else
        {
            await using var row = new NpgsqlCommand(CaptureAddSql, connection, transaction) { CommandTimeout = SweepTimeoutSeconds };
            row.Parameters.AddWithValue(captureTime);
            row.Parameters.AddWithValue(capture.LogFile);
            row.Parameters.AddWithValue((long)capture.BytesRead);
            row.Parameters.AddWithValue(capture.BytesPending);
            row.Parameters.AddWithValue(capture.Census.LinesRead);
            row.Parameters.AddWithValue(capture.Census.EntriesRead);
            row.Parameters.AddWithValue(capture.Census.GroupsDropped);
            await row.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var marker = new NpgsqlCommand(MarkerUpsertSql, connection, transaction) { CommandTimeout = SweepTimeoutSeconds })
        {
            marker.Parameters.AddWithValue(capture.LogFile);
            marker.Parameters.AddWithValue(newOffset);
            marker.Parameters.AddWithValue(sizeBytes);
            marker.Parameters.AddWithValue(captureTime);
            await marker.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    /// <summary>
    /// The capture row alone, with no census rows and no marker move — a tick that ran and found nothing.
    /// Separate from <see cref="WriteAsync"/> precisely because it must NOT advance a marker: there is
    /// nothing consumed to account for, and a marker moved on an empty read is a marker that skipped bytes.
    /// </summary>
    private static async Task WriteHeartbeatAsync(
        NpgsqlConnection connection,
        DateTime captureTime,
        FileCapture capture,
        CancellationToken cancellationToken)
    {
        await using var row = new NpgsqlCommand(CaptureInsertSql, connection) { CommandTimeout = SweepTimeoutSeconds };
        row.Parameters.AddWithValue(captureTime);
        row.Parameters.AddWithValue(capture.LogFile);
        row.Parameters.AddWithValue((long)capture.BytesRead);
        row.Parameters.AddWithValue(capture.BytesPending);
        row.Parameters.AddWithValue(capture.Census.LinesRead);
        row.Parameters.AddWithValue(capture.Census.EntriesRead);
        row.Parameters.AddWithValue(capture.OffsetReset);
        row.Parameters.AddWithValue(capture.Census.GroupsDropped);
        await row.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task PurgeAsync(NpgsqlConnection connection, DateTime cutoff, CancellationToken cancellationToken)
    {
        await using (var events = new NpgsqlCommand(EventRetentionDeleteSql, connection) { CommandTimeout = SweepTimeoutSeconds })
        {
            events.Parameters.AddWithValue(cutoff);
            await events.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var captures = new NpgsqlCommand(CaptureRetentionDeleteSql, connection) { CommandTimeout = SweepTimeoutSeconds })
        {
            captures.Parameters.AddWithValue(cutoff);
            await captures.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var lockWaits = new NpgsqlCommand(StoreLockWaits.RetentionDeleteSql, connection) { CommandTimeout = SweepTimeoutSeconds };
        lockWaits.Parameters.AddWithValue(cutoff);
        await lockWaits.ExecuteNonQueryAsync(cancellationToken);
        /* #9414: the checkpoint rows go on the same cutoff. */
        await using var checkpoints = new NpgsqlCommand(StoreCheckpoints.RetentionDeleteSql, connection) { CommandTimeout = SweepTimeoutSeconds };
        checkpoints.Parameters.AddWithValue(cutoff);
        await checkpoints.ExecuteNonQueryAsync(cancellationToken);
    }
}
