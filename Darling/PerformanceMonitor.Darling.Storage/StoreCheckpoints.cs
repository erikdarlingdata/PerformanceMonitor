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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #9414: one row per checkpoint and restartpoint of the monitoring store's own PostgreSQL server, read from the
/// <c>checkpoint starting</c> and <c>checkpoint complete</c> lines the store log sweep (<see cref="StoreLogSweep"/>) already
/// reads. The hourly checkpointer totals say how many checkpoints an hour ran. They cannot say which ones synced many more
/// files than the rest, or whether time or WAL started them, and a checkpoint settings trial needs those figures for every
/// checkpoint in its window.
///
/// <para><b>Shape (V309).</b> <c>collect.store_checkpoints</c>: one row per checkpoint. The start line gives the reason and
/// the flags and opens the row, the complete line closes it and gives the figures. A complete line whose start line was
/// never seen (logging was off, or the start line was in a file the ring has since overwritten) still gets a row, with no
/// reason and a start time worked out from its end time and total time. A start line with no complete line stays
/// open, <c>ended_at</c> NULL: the checkpoint was still running when the log was last read, or, when a later checkpoint of the same kind
/// has started, it died with a crash or restart.</para>
///
/// <para><b>The log's clock.</b> A log line's stamp is <c>%m</c>, rendered in the server's <c>log_timezone</c>, which the
/// service does not set (see <see cref="StoreLogClassifier"/>). The stamp is therefore handed to the store as a naive
/// timestamp and converted there with the server's own <c>log_timezone</c>, so no zone is guessed here. A line with no
/// stamp at all (a <c>log_line_prefix</c> that carries no time) cannot be placed in time and is counted, not stored.</para>
///
/// <para><b>Formats</b>, from <c>LogCheckpointEnd</c> in <c>xlog.c</c> of each stable branch. PostgreSQL 14 and 15 write
/// <c>checkpoint complete: wrote N buffers (P%); A WAL file(s) added, R removed, C recycled; write=..., sync=..., total=...;
/// sync files=N, longest=..., average=...; distance=N kB, estimate=N kB</c>. PostgreSQL 16 and 17 add
/// <c>; lsn=X/X, redo lsn=X/X</c>. PostgreSQL 18 adds <c>, wrote N SLRU buffers</c> after the buffer count. A restartpoint is the
/// same line with that word. The start line is <c>checkpoint starting:</c> followed by space-separated flag words, the same on
/// every major. A line this parser does not recognise as a complete line is left alone. A line it recognises
/// as one but cannot read the figures of is stored with no figures and <c>unparsed</c> set, never guessed.</para>
///
/// <para><b>Retention.</b> The store log's: <see cref="StoreLogSweep.RetentionDays"/>.</para>
/// </summary>
public static class StoreCheckpoints
{
    /// <summary>The store schema version that adds the table. One constant, so a renumber is one edit.</summary>
    public const int Version = 309;

    /// <summary>The rung (V309): one empty plain table and its indexes, no data moves. An older build never reads it.</summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.store_checkpoints
(
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind text NOT NULL,
    started_at timestamp NOT NULL,
    ended_at timestamp,
    reason text,
    flags text,
    buffers_written integer,
    buffers_pct real,
    slru_buffers_written integer,
    wal_files_added integer,
    wal_files_removed integer,
    wal_files_recycled integer,
    write_ms bigint,
    sync_ms bigint,
    total_ms bigint,
    files_synced integer,
    longest_sync_ms bigint,
    average_sync_ms bigint,
    distance_kb bigint,
    estimate_kb bigint,
    lsn text,
    redo_lsn text,
    unparsed boolean NOT NULL DEFAULT false,
    capture_time timestamp NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_store_checkpoints_started
    ON collect.store_checkpoints(started_at);

CREATE INDEX IF NOT EXISTS idx_store_checkpoints_open
    ON collect.store_checkpoints(kind, started_at)
    WHERE ended_at IS NULL;";

    /// <summary>The retention delete. $1 cutoff (naive UTC).</summary>
    public const string RetentionDeleteSql = @"
DELETE FROM collect.store_checkpoints
WHERE started_at < $1";

    /// <summary>A log stamp (naive, in the server's <c>log_timezone</c>) as a naive UTC timestamp. The one place the zone is resolved,
    /// and it is the server's own.</summary>
    private static string StampToUtc(string parameter) =>
        "(((" + parameter + "::timestamp) AT TIME ZONE pg_catalog.current_setting('log_timezone')) AT TIME ZONE 'UTC')";

    /// <summary>A start line opens a row. $1 kind, $2 log stamp, $3 reason, $4 flags, $5 capture_time.</summary>
    public static readonly string StartInsertSql = @"
INSERT INTO collect.store_checkpoints (kind, started_at, reason, flags, capture_time)
VALUES ($1, " + StampToUtc("$2") + ", $3, $4, $5)";

    /// <summary>
    /// The open row a complete line closes: the newest of that kind, still open, that started within a few seconds of the
    /// complete line's end time less its total time (the two clocks are the same checkpoint's). With no total time ($3 NULL) the
    /// newest open one that started within a day before the end. $1 kind, $2 log stamp of the end, $3 total_ms or NULL.
    /// </summary>
    public static readonly string OpenRowSql = @"
WITH e AS (SELECT " + StampToUtc("$2") + @" AS ended)
SELECT c.id
FROM collect.store_checkpoints AS c
CROSS JOIN e
WHERE c.kind = $1
AND   c.ended_at IS NULL
AND   c.started_at <= e.ended
AND   CASE
          WHEN $3::bigint IS NULL THEN c.started_at > e.ended - interval '1 day'
          ELSE abs(extract(epoch FROM (e.ended - c.started_at)) - $3::bigint / 1000.0) <= 5
      END
ORDER BY c.started_at DESC
LIMIT 1";

    private const string FigureColumns = @"
    buffers_written = $3, buffers_pct = $4, slru_buffers_written = $5, wal_files_added = $6, wal_files_removed = $7,
    wal_files_recycled = $8, write_ms = $9, sync_ms = $10, total_ms = $11, files_synced = $12, longest_sync_ms = $13,
    average_sync_ms = $14, distance_kb = $15, estimate_kb = $16, lsn = $17, redo_lsn = $18, unparsed = $19, capture_time = $20";

    /// <summary>Closes an open row. $1 id, $2 log stamp of the end, then the figures as <see cref="Figures"/> lists them ($3 to $19), then $20 capture_time.</summary>
    public static readonly string CloseSql = @"
UPDATE collect.store_checkpoints
SET ended_at = " + StampToUtc("$2") + @",
" + FigureColumns + @"
WHERE id = $1";

    /// <summary>A complete line with no open row of its own: a row with no reason, started total_ms before its end. $1 kind, $2 log stamp of the end,
    /// the figures as in <see cref="CloseSql"/>.</summary>
    public static readonly string CompleteOnlyInsertSql = @"
INSERT INTO collect.store_checkpoints
    (kind, started_at, ended_at, buffers_written, buffers_pct, slru_buffers_written, wal_files_added, wal_files_removed,
     wal_files_recycled, write_ms, sync_ms, total_ms, files_synced, longest_sync_ms, average_sync_ms, distance_kb, estimate_kb,
     lsn, redo_lsn, unparsed, capture_time)
SELECT $1,
       e.ended - COALESCE($11::bigint, 0) * interval '1 millisecond',
       e.ended, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
FROM (SELECT " + StampToUtc("$2") + @" AS ended) AS e";

    /// <summary>What a complete line says, as the store holds it. Every field is null on a line that could not be read.</summary>
    public sealed record Figures(
        int? BuffersWritten,
        float? BuffersPct,
        int? SlruBuffersWritten,
        int? WalFilesAdded,
        int? WalFilesRemoved,
        int? WalFilesRecycled,
        long? WriteMs,
        long? SyncMs,
        long? TotalMs,
        int? FilesSynced,
        long? LongestSyncMs,
        long? AverageSyncMs,
        long? DistanceKb,
        long? EstimateKb,
        string? Lsn,
        string? RedoLsn);

    /// <summary>One checkpoint line. <see cref="Stamp"/> is the log's own stamp, naive, in the server's log_timezone.
    /// <see cref="Reason"/> and <see cref="Flags"/> are set on a start line. <see cref="Figures"/> is set on a complete line,
    /// and its fields are all null when the line could not be read (<see cref="Unparsed"/>).</summary>
    public sealed record Line(
        string Kind,
        bool IsStart,
        DateTime Stamp,
        string? Reason,
        string? Flags,
        Figures? Figures,
        bool Unparsed);

    /// <summary>The most checkpoint lines one slab (4 MiB of log) writes: the newest ones. A slab holds a few hundred in a heavy hour, so this
    /// only bounds the time a log full of checkpoint lines (a test log, a store that checkpoints every second) holds the sweep's transaction.</summary>
    public const int MaxLinesPerSlab = 2000;

    /// <summary>What a slab held: the lines in order, how many checkpoint lines had no stamp to place them by, and how many the cap left out.</summary>
    public sealed record Parsed(IReadOnlyList<Line> Lines, int NoStamp, int Dropped = 0);

    private static readonly Regex s_head = new(
        @"^(?<stamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)\b.*?\bLOG:  (?<kind>checkpoint|restartpoint) (?<what>starting|complete):\s*(?<rest>.*?)\s*$",
        RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));

    private static readonly Regex s_headNoStamp = new(
        @"\bLOG:  (?:checkpoint|restartpoint) (?:starting|complete): ",
        RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));

    private static readonly Regex s_start = new(
        @"^(?<flags>(?:[a-z][a-z-]*)(?: [a-z][a-z-]*)*)?$",
        RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));

    private static string Secs(string name) => "(?<" + name + @">\d+\.\d{3}) s";

    private static readonly Regex s_complete = new(
        @"^wrote (?<buf>\d+) buffers \((?<pct>\d+(?:\.\d+)?)%\)(?:, wrote (?<slru>\d+) SLRU buffers)?; "
        + @"(?<added>\d+) WAL file\(s\) added, (?<removed>\d+) removed, (?<recycled>\d+) recycled; "
        + @"write=" + Secs("write") + @", sync=" + Secs("sync")
        + @", total=" + Secs("total") + @"; "
        + @"sync files=(?<files>\d+), longest=" + Secs("longest")
        + @", average=" + Secs("average") + @"; "
        + @"distance=(?<dist>-?\d+) kB, estimate=(?<est>-?\d+) kB"
        + @"(?:; lsn=(?<lsn>[0-9A-Fa-f]+/[0-9A-Fa-f]+), redo lsn=(?<redo>[0-9A-Fa-f]+/[0-9A-Fa-f]+))?$",
        RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));

    /// <summary>The reason a start line's flag words name. PostgreSQL writes one word per flag, and a checkpoint with no cause word
    /// is one somebody asked for (<c>CHECKPOINT</c>, a base backup, a database create), so it is <c>immediate</c> when it says so and
    /// <c>requested</c> otherwise. Precedence follows how much each word says about why the checkpoint ran.</summary>
    public static string ReasonOf(string? flags)
    {
        var words = (flags ?? string.Empty).Split(' ', StringSplitOptions.RemoveEmptyEntries);
        foreach (var reason in new[] { "end-of-recovery", "shutdown", "wal", "time", "immediate" })
        {
            if (Array.IndexOf(words, reason) >= 0)
            {
                return reason;
            }
        }

        return "requested";
    }

    /// <summary>Seconds with three decimals, as PostgreSQL writes them, as whole milliseconds. Null when the text is not that shape or overflows.</summary>
    internal static long? MillisecondsOf(string seconds)
    {
        var dot = seconds.IndexOf('.');
        if (dot <= 0 || seconds.Length - dot - 1 != 3
            || !long.TryParse(seconds.AsSpan(0, dot), NumberStyles.None, CultureInfo.InvariantCulture, out var whole)
            || !int.TryParse(seconds.AsSpan(dot + 1), NumberStyles.None, CultureInfo.InvariantCulture, out var fraction)
            || whole > long.MaxValue / 1000 - 1)
        {
            return null;
        }

        return whole * 1000 + fraction;
    }

    /// <summary>Parses the checkpoint and restartpoint lines of a slab, in order. A line that is not one is ignored.</summary>
    public static Parsed Parse(string? slab)
    {
        var lines = new List<Line>();
        var noStamp = 0;
        if (string.IsNullOrEmpty(slab) || !slab.Contains("point ", StringComparison.Ordinal))
        {
            return new Parsed(lines, 0);
        }

        foreach (var raw in slab.Split('\n'))
        {
            if (raw.Length == 0 || raw[0] == '\t' || !raw.Contains("point ", StringComparison.Ordinal))
            {
                continue;
            }

            try
            {
                var line = raw.TrimEnd('\r');
                var head = s_head.Match(line);
                if (!head.Success)
                {
                    if (s_headNoStamp.IsMatch(line))
                    {
                        noStamp++;
                    }

                    continue;
                }

                if (!DateTime.TryParse(head.Groups["stamp"].Value, CultureInfo.InvariantCulture, DateTimeStyles.None, out var stamp))
                {
                    noStamp++;
                    continue;
                }

                stamp = DateTime.SpecifyKind(stamp, DateTimeKind.Unspecified);
                var kind = head.Groups["kind"].Value;
                var rest = head.Groups["rest"].Value;

                if (head.Groups["what"].Value == "starting")
                {
                    var flags = s_start.Match(rest);
                    if (!flags.Success)
                    {
                        continue;
                    }

                    var words = flags.Groups["flags"].Value;
                    lines.Add(new Line(kind, true, stamp, ReasonOf(words), words.Length == 0 ? null : words, null, false));
                    continue;
                }

                var figures = ReadFigures(rest);
                lines.Add(new Line(kind, false, stamp, null, null, figures, figures is null));
            }
            catch (RegexMatchTimeoutException)
            {
                /* A line the pattern cannot finish in a second is not a checkpoint line; dropped like any other unread line. */
            }
        }

        var dropped = Math.Max(0, lines.Count - MaxLinesPerSlab);
        if (dropped > 0)
        {
            lines.RemoveRange(0, dropped);
        }

        return new Parsed(lines, noStamp, dropped);
    }

    private static Figures? ReadFigures(string rest)
    {
        var m = s_complete.Match(rest);
        if (!m.Success)
        {
            return null;
        }

        var write = MillisecondsOf(m.Groups["write"].Value);
        var sync = MillisecondsOf(m.Groups["sync"].Value);
        var total = MillisecondsOf(m.Groups["total"].Value);
        var longest = MillisecondsOf(m.Groups["longest"].Value);
        var average = MillisecondsOf(m.Groups["average"].Value);
        if (!int.TryParse(m.Groups["buf"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var buffers)
            || !float.TryParse(m.Groups["pct"].Value, NumberStyles.Float, CultureInfo.InvariantCulture, out var pct)
            || !int.TryParse(m.Groups["added"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var added)
            || !int.TryParse(m.Groups["removed"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var removed)
            || !int.TryParse(m.Groups["recycled"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var recycled)
            || !int.TryParse(m.Groups["files"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var files)
            || !long.TryParse(m.Groups["dist"].Value, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var distance)
            || !long.TryParse(m.Groups["est"].Value, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var estimate)
            || write is null || sync is null || total is null || longest is null || average is null)
        {
            return null;
        }

        int? slru = m.Groups["slru"].Success && int.TryParse(m.Groups["slru"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var s) ? s : null;
        return new Figures(
            buffers, pct, slru, added, removed, recycled, write, sync, total, files, longest, average, distance, estimate,
            m.Groups["lsn"].Success ? m.Groups["lsn"].Value : null,
            m.Groups["redo"].Success ? m.Groups["redo"].Value : null);
    }

    /// <summary>
    /// Writes a slab's checkpoint lines inside the sweep's transaction, behind a savepoint of their own: a fault here (a column
    /// out of range on a line no build has seen) rolls back to the savepoint and is logged, so the census rows and the marker the
    /// same transaction carries still land, and the next capture is not stuck on the same bytes. Returns how many rows it wrote or
    /// closed, or -1 when the savepoint was rolled back.
    /// </summary>
    internal static async Task<int> WriteAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        Parsed parsed,
        DateTime captureTime,
        ILogger? logger,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        if (parsed.NoStamp > 0)
        {
            logger?.LogWarning(
                "{Count} checkpoint line(s) in the store log carry no timestamp, so they are not kept. Put %m or %t first in log_line_prefix.",
                parsed.NoStamp);
        }

        if (parsed.Dropped > 0)
        {
            logger?.LogWarning("{Count} older checkpoint line(s) in one slab of the store log were not kept: a slab keeps its newest {Max}.", parsed.Dropped, MaxLinesPerSlab);
        }

        if (parsed.Lines.Count == 0)
        {
            return 0;
        }

        await transaction.SaveAsync("store_checkpoints", cancellationToken);
        try
        {
            var written = 0;
            foreach (var line in parsed.Lines)
            {
                written += await WriteLineAsync(connection, transaction, line, captureTime, commandTimeoutSeconds, cancellationToken);
            }

            await transaction.ReleaseAsync("store_checkpoints", cancellationToken);
            return written;
        }
        catch (Exception ex) when (ex is NpgsqlException or InvalidOperationException)
        {
            await transaction.RollbackAsync("store_checkpoints", cancellationToken);
            logger?.LogWarning("Store checkpoint rows were not kept for this slab of the log, so its checkpoints are missing: {Message}", ex.Message);
            return -1;
        }
    }

    private static async Task<int> WriteLineAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, Line line, DateTime captureTime, int timeout, CancellationToken cancellationToken)
    {
        if (line.IsStart)
        {
            await using var start = new NpgsqlCommand(StartInsertSql, connection, transaction) { CommandTimeout = timeout };
            start.Parameters.AddWithValue(NpgsqlDbType.Text, line.Kind);
            start.Parameters.AddWithValue(NpgsqlDbType.Timestamp, line.Stamp);
            start.Parameters.AddWithValue(NpgsqlDbType.Text, line.Reason ?? (object)DBNull.Value);
            start.Parameters.AddWithValue(NpgsqlDbType.Text, line.Flags ?? (object)DBNull.Value);
            start.Parameters.AddWithValue(NpgsqlDbType.Timestamp, captureTime);
            return await start.ExecuteNonQueryAsync(cancellationToken);
        }

        var figures = line.Figures;
        long? id = null;
        await using (var find = new NpgsqlCommand(OpenRowSql, connection, transaction) { CommandTimeout = timeout })
        {
            find.Parameters.AddWithValue(NpgsqlDbType.Text, line.Kind);
            find.Parameters.AddWithValue(NpgsqlDbType.Timestamp, line.Stamp);
            find.Parameters.AddWithValue(NpgsqlDbType.Bigint, figures?.TotalMs ?? (object)DBNull.Value);
            id = await find.ExecuteScalarAsync(cancellationToken) as long?;
        }

        await using var write = new NpgsqlCommand(id is null ? CompleteOnlyInsertSql : CloseSql, connection, transaction) { CommandTimeout = timeout };
        if (id is long open)
        {
            write.Parameters.AddWithValue(NpgsqlDbType.Bigint, open);
        }
        else
        {
            write.Parameters.AddWithValue(NpgsqlDbType.Text, line.Kind);
        }

        write.Parameters.AddWithValue(NpgsqlDbType.Timestamp, line.Stamp);
        write.Parameters.AddWithValue(NpgsqlDbType.Integer, figures?.BuffersWritten ?? (object)DBNull.Value);
        write.Parameters.AddWithValue(NpgsqlDbType.Real, figures?.BuffersPct ?? (object)DBNull.Value);
        write.Parameters.AddWithValue(NpgsqlDbType.Integer, figures?.SlruBuffersWritten ?? (object)DBNull.Value);
        write.Parameters.AddWithValue(NpgsqlDbType.Integer, figures?.WalFilesAdded ?? (object)DBNull.Value);
        write.Parameters.AddWithValue(NpgsqlDbType.Integer, figures?.WalFilesRemoved ?? (object)DBNull.Value);
        write.Parameters.AddWithValue(NpgsqlDbType.Integer, figures?.WalFilesRecycled ?? (object)DBNull.Value);
        write.Parameters.AddWithValue(NpgsqlDbType.Bigint, figures?.WriteMs ?? (object)DBNull.Value);
        write.Parameters.AddWithValue(NpgsqlDbType.Bigint, figures?.SyncMs ?? (object)DBNull.Value);
        write.Parameters.AddWithValue(NpgsqlDbType.Bigint, figures?.TotalMs ?? (object)DBNull.Value);
        write.Parameters.AddWithValue(NpgsqlDbType.Integer, figures?.FilesSynced ?? (object)DBNull.Value);
        write.Parameters.AddWithValue(NpgsqlDbType.Bigint, figures?.LongestSyncMs ?? (object)DBNull.Value);
        write.Parameters.AddWithValue(NpgsqlDbType.Bigint, figures?.AverageSyncMs ?? (object)DBNull.Value);
        write.Parameters.AddWithValue(NpgsqlDbType.Bigint, figures?.DistanceKb ?? (object)DBNull.Value);
        write.Parameters.AddWithValue(NpgsqlDbType.Bigint, figures?.EstimateKb ?? (object)DBNull.Value);
        write.Parameters.AddWithValue(NpgsqlDbType.Text, figures?.Lsn ?? (object)DBNull.Value);
        write.Parameters.AddWithValue(NpgsqlDbType.Text, figures?.RedoLsn ?? (object)DBNull.Value);
        write.Parameters.AddWithValue(NpgsqlDbType.Boolean, line.Unparsed);
        write.Parameters.AddWithValue(NpgsqlDbType.Timestamp, captureTime);
        return await write.ExecuteNonQueryAsync(cancellationToken);
    }
}
