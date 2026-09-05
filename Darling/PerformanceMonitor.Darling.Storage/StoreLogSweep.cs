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
    /// <para>The path comes from <c>current_setting('log_directory')</c> rather than the literal
    /// <c>'log/'</c> the collectors hardcode. Darling's own v6 block sets it to <c>'log'</c>, so the two
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
    public static async Task<List<FileCapture>> SweepAsync(
        NpgsqlConnection connection,
        DateTime utcNow,
        ILogger? logger,
        CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        /* Naive UTC by the product-wide cross-store contract — the same shape every collector stamps, and
           one value for the whole capture so a capture's rows join. */
        var captureTime = DateTime.SpecifyKind(utcNow, DateTimeKind.Unspecified);
        var captured = new List<FileCapture>();

        var candidates = await ListAsync(connection, cancellationToken);

        foreach (var candidate in candidates)
        {
            var resume = StoreLogSlab.ResolveResume(candidate.StoredOffset, candidate.StoredLastSize, candidate.SizeBytes);
            if (!resume.HasWork)
            {
                continue;
            }

            var length = (int)Math.Min(StoreLogSlab.MaxBytesPerRead, candidate.SizeBytes - resume.Offset);
            var raw = await ReadAsync(connection, candidate.LogFile, resume.Offset, length, cancellationToken);
            var slab = StoreLogSlab.TrimToLastNewline(raw);

            if (slab.BytesConsumed == 0)
            {
                /* Nothing complete yet — one partial line at the end of a file being written. The marker
                   stays where it is, so the line is read whole next time rather than half now. */
                continue;
            }

            var census = StoreLogClassifier.Classify(slab.Text);
            var newOffset = resume.Offset + slab.BytesConsumed;

            var capture = new FileCapture(
                LogFile: candidate.LogFile,
                Offset: resume.Offset,
                BytesRead: slab.BytesConsumed,
                BytesPending: Math.Max(0, candidate.SizeBytes - newOffset),
                OffsetReset: resume.OffsetReset,
                Census: census);

            await WriteAsync(connection, captureTime, capture, newOffset, candidate.SizeBytes, cancellationToken);
            captured.Add(capture);

            if (captured.Count >= MaxFilesPerCapture)
            {
                break;
            }
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

    /// <summary>One row of <see cref="LogDirectoryListSql"/>.</summary>
    private readonly record struct Candidate(string LogFile, long SizeBytes, long? StoredOffset, long? StoredLastSize);

    private static async Task<List<Candidate>> ListAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var rows = new List<Candidate>();

        await using var command = new NpgsqlCommand(LogDirectoryListSql, connection) { CommandTimeout = SweepTimeoutSeconds };
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Candidate(
                LogFile: reader.GetString(0),
                SizeBytes: reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                StoredOffset: reader.IsDBNull(2) ? null : reader.GetInt64(2),
                StoredLastSize: reader.IsDBNull(3) ? null : reader.GetInt64(3)));
        }

        return rows;
    }

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
        CancellationToken cancellationToken)
    {
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

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

        await using (var row = new NpgsqlCommand(CaptureInsertSql, connection, transaction) { CommandTimeout = SweepTimeoutSeconds })
        {
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

    private static async Task PurgeAsync(NpgsqlConnection connection, DateTime cutoff, CancellationToken cancellationToken)
    {
        await using (var events = new NpgsqlCommand(EventRetentionDeleteSql, connection) { CommandTimeout = SweepTimeoutSeconds })
        {
            events.Parameters.AddWithValue(cutoff);
            await events.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var captures = new NpgsqlCommand(CaptureRetentionDeleteSql, connection) { CommandTimeout = SweepTimeoutSeconds };
        captures.Parameters.AddWithValue(cutoff);
        await captures.ExecuteNonQueryAsync(cancellationToken);
    }
}
