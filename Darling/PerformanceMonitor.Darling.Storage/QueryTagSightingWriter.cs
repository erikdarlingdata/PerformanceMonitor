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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common.QueryTags;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Writes query-tag sightings to <c>collect.query_tag_sightings</c> (#5985, part of #5971): the record of every query
/// key and caller a collector saw, which the map builder (<see cref="QueryTagMapBuilder"/>) resolves. The collectors do
/// not call this per batch: they append to a <see cref="QueryTagSightingBuffer"/> in memory, and the map step flushes it
/// here once a minute, so the hot per-batch write path makes no store round trip.
///
/// <para><b>One statement per flush, and no connection for an empty one.</b> The same identity sighted twice is
/// collapsed first (a single <c>INSERT ... ON CONFLICT DO UPDATE</c> cannot touch one row twice), with the occurrences
/// added to <c>seen_count</c>. The conflict target is the table's own unique index (V194), so a key seen under the same
/// caller in the same database again moves <c>last_seen</c> at most once every <see cref="ReSightingRefreshWidth"/> (#8716, widened
/// from an hour in #9456), to a stamp behind the map's extend window (<see cref="ReSightingBackdate"/>), and keeps its first
/// text sample. A re-sighting that fills nothing and is not due for that move writes no row at all (#8979), so <c>seen_count</c> is a lower bound (see
/// <see cref="UpsertSql"/>).</para>
///
/// <para><b>The database is part of the identity (V194).</b> One hash seen in two databases is two rows, so the map
/// builder, which resolves a key against every sighting row it has, tests a database rule against each of them. An
/// empty caller field, an empty database and a missing one are the same identity, as the unique index reads them.</para>
///
/// <para><b>Never throws.</b> A sighting that cannot be written is a key the next pass tags a little later, never a
/// reason to fail a collection cycle that stored every row it read (the shape of
/// <see cref="OversizedPlanBacklog.RecordSightingsAsync"/>).</para>
/// </summary>
public static class QueryTagSightingWriter
{
    public const int CommandTimeoutSeconds = 30;

    /// <summary>
    /// The longest text sample kept, in characters. A sample is the first text a key was sighted with and the upsert keeps
    /// only that one, so every later sighting's text is thrown away. Text rules and <c>/* tag:NAME */</c> comments read this
    /// sample, so they see the first 4,000 characters of the first-sighted text, the same text they saw before sightings were
    /// buffered and the same cut Lite keeps. This is the table's CHECK length itself (#3684): a separate literal that fell behind a lowered CHECK would fail every flush holding a long sample. The buffer is bounded by entry count instead, see
    /// <see cref="QueryTagSightingBuffer.SightingBufferCap"/>.
    /// </summary>
    public const int TextSampleChars = QueryTagSchema.TextSampleMaxLength;

    /// <summary>
    /// The most rows one statement carries. A flush larger than this is several statements, so a buffer at its cap is
    /// not one multi-megabyte parameter. The value is threaded into the loop below, never written in the SQL (#3684).
    /// </summary>
    public const int RowsPerStatement = 10_000;

    /// <summary>
    /// How far behind its statement's <c>now()</c> a plain re-sighting stamps <c>last_seen</c>. A plain re-sighting
    /// changes nothing the map resolver reads, so the map's extend never needs to read it, and the extend reads every
    /// sighting row of each key it finds (<see cref="QueryTagMapStore.SightingsOfKeysSql"/>). The extend finds keys from
    /// the previous pass's clock less <see cref="QueryTagMapBuilder.ExtendOverlap"/>, and a flush runs at a pass's start or
    /// between passes a minute apart, so a stamp this far back is below the cursor of every extend that can see it while
    /// passes start within three minutes of each other. When they do not, the extend reads the key again and the map write
    /// skips a row that already says the same. Inserts and fills keep <c>now()</c>, so the extend still reads them. The
    /// value is threaded into the SQL as a parameter (#3684), never written there.
    /// </summary>
    public static readonly TimeSpan ReSightingBackdate = QueryTagMapBuilder.ExtendOverlap + TimeSpan.FromMinutes(3);

    /// <summary>
    /// The least time between two moves of a re-sighted row's <c>last_seen</c> (#9456). Every move is a full new heap version that
    /// can never be HOT, because <c>last_seen</c> has its own index, so it writes all four indexes of the table and copies a text
    /// sample of up to <see cref="TextSampleChars"/> characters. At an hour, every key the collectors keep seeing was rewritten
    /// once an hour only to move a stamp that nothing reads that finely: the map's changed-keys read needs the stamp to move on an
    /// insert or a fill, which still take <c>now()</c>, and retention purges on a cutoff at least a day back. Six hours is the
    /// width the plan and text dimension tables already use for the same reason (#4503), and it leaves a live key's stamp at most
    /// this width plus <see cref="ReSightingBackdate"/> behind. The one reader that sees the difference is the rule test read, whose
    /// window never goes under <see cref="MinTestWindowHours"/>. The value is threaded into the SQL as a parameter (#3684).
    /// </summary>
    public static readonly TimeSpan ReSightingRefreshWidth = TimeSpan.FromHours(6);

    /// <summary>
    /// The shortest window, in whole hours, the rule test read honours: the refresh width plus the backdate, rounded up. A key still
    /// being seen can have a stamp that old, so a shorter window would drop it while an older one still counts it.
    /// </summary>
    public static readonly int MinTestWindowHours = (int)Math.Ceiling((ReSightingRefreshWidth + ReSightingBackdate).TotalHours);

    /// <summary>
    /// Statement parameters: $1..$10, the ten columns of the flush as arrays (server id first, the count last), $11
    /// <see cref="ReSightingBackdate"/> and $12 <see cref="ReSightingRefreshWidth"/>.
    /// <para><b>A re-sighting moves <c>last_seen</c> at most once every six hours (#8716, #9456).</b> <c>last_seen</c> is indexed, so bumping it on
    /// every conflict made every update non-HOT: each one wrote all four indexes, and a busy store's table and its
    /// <c>last_seen</c> index grew by hundreds of thousands of pages a day with autovacuum removing nothing. <c>last_seen</c> moves to <c>now()</c> when the conflict fills an object
    /// name or text sample the row lacked, because those are what the map resolver reads and the map's changed-keys read
    /// (<see cref="QueryTagMapStore.ChangedKeysSql"/>) finds a key by this column. Otherwise it moves when the stored value
    /// is over six hours older than the backdated stamp, to <c>now()</c> less <see cref="ReSightingBackdate"/>, so an active
    /// row moves once every six hours and never inside the extend window. The move was hourly until #9456: each one is a non-HOT
    /// rewrite of the whole row, and nothing reads the stamp to a finer width than six hours (see <see cref="ReSightingRefreshWidth"/>).
    /// <para><b>A conflict writes only when one of those happens (#8979).</b> The count was the one thing every conflict
    /// changed, and a changed count alone made PostgreSQL write a new heap version of every re-sighted row (HOT, but still a
    /// full row copy, tens of millions a day on a busy store). A row is now written only when it fills an object name or text
    /// sample or its six-hourly <c>last_seen</c> move is due. The same test sits twice: as a <c>NOT EXISTS</c> filter in the select
    /// list, so an unchanged row never reaches the insert and takes no row lock, and in the conflict arm's <c>WHERE</c>, which
    /// decides when a concurrent writer got there first. <c>ORDER BY</c> the ordinal keeps the rows in the caller's identity order
    /// (<see cref="SortByIdentity"/>), which the filter's join could otherwise change.</para>
    /// <para><b>The text samples stay out of every work area (#9038).</b> A flush can carry thousands of samples of up to
    /// <see cref="TextSampleChars"/> characters, and a multi-argument <c>unnest</c> in <c>FROM</c> is always held in a tuple store that
    /// spills to temporary files past <c>work_mem</c>, as would a sort of the filtered rows with their text. The scan and
    /// the sort here carry only the narrow columns, a <c>has_text</c> flag (built by a streaming <c>unnest</c> in the select
    /// list, which holds nothing) and the ordinal. The filter probes the unique index once per flushed row, and a row that
    /// survives it takes its sample from the parameter array by ordinal.</para>
    /// <para><b><c>seen_count</c> is a lower bound.</b> It adds the flush's buffered count only on a write that happens
    /// anyway. A flush whose re-sighting writes nothing drops its count. Nothing reads the column.</para>
    /// The time source is the statement's own <c>now()</c>, reached through the column default and read as <c>EXCLUDED.last_seen</c> (the insert list leaves <c>last_seen</c> out). That is the same clock as the stored value, and it keeps the comparison <c>timestamptz</c> against <c>timestamptz</c>.</para>
    /// </summary>
    internal const string UpsertSql = """
INSERT INTO collect.query_tag_sightings AS t
    (server_id, key_kind, key, database_name, object_name, application_name, login_name, host_name, text_sample, seen_count)
SELECT k.server_id, k.key_kind, k.key, k.database_name, k.object_name, k.application_name, k.login_name, k.host_name, ($9::text[])[k.ord], k.seen_count
FROM (
    SELECT u.server_id, u.key_kind, u.key, u.database_name, u.object_name, u.application_name, u.login_name, u.host_name, u.seen_count, u.ord
    FROM unnest($1::integer[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[],
                (SELECT array_agg(x.t IS NOT NULL) FROM (SELECT unnest($9::text[]) AS t) AS x), $10::bigint[])
        WITH ORDINALITY AS u(server_id, key_kind, key, database_name, object_name, application_name, login_name, host_name, has_text, seen_count, ord)
    WHERE NOT EXISTS (
        SELECT 1
        FROM collect.query_tag_sightings AS s
        WHERE s.server_id = u.server_id AND s.key_kind = u.key_kind AND s.key = u.key
          AND COALESCE(s.application_name, '') = COALESCE(u.application_name, '')
          AND COALESCE(s.login_name, '') = COALESCE(u.login_name, '')
          AND COALESCE(s.host_name, '') = COALESCE(u.host_name, '')
          AND COALESCE(s.database_name, '') = COALESCE(u.database_name, '')
          AND NOT ((s.object_name IS NULL AND u.object_name IS NOT NULL)
                OR (s.text_sample IS NULL AND u.has_text)
                OR s.last_seen < now() - $11::interval - $12::interval))
    ORDER BY u.ord
) AS k
ORDER BY k.ord
ON CONFLICT (server_id, key_kind, key, COALESCE(application_name, ''), COALESCE(login_name, ''), COALESCE(host_name, ''), COALESCE(database_name, ''))
DO UPDATE SET
    last_seen = CASE
        WHEN (t.object_name IS NULL AND EXCLUDED.object_name IS NOT NULL)
          OR (t.text_sample IS NULL AND EXCLUDED.text_sample IS NOT NULL)
        THEN EXCLUDED.last_seen
        WHEN t.last_seen < EXCLUDED.last_seen - $11::interval - $12::interval
        THEN EXCLUDED.last_seen - $11::interval
        ELSE t.last_seen END,
    seen_count = t.seen_count + EXCLUDED.seen_count,
    object_name = COALESCE(t.object_name, EXCLUDED.object_name),
    text_sample = COALESCE(t.text_sample, EXCLUDED.text_sample)
WHERE (t.object_name IS NULL AND EXCLUDED.object_name IS NOT NULL)
   OR (t.text_sample IS NULL AND EXCLUDED.text_sample IS NOT NULL)
   OR t.last_seen < EXCLUDED.last_seen - $11::interval - $12::interval
""";

    /// <summary>The store's name for a key kind: the same text the table's CHECK allows.</summary>
    public static string KindText(QueryTagKeyKind kind) => kind switch
    {
        QueryTagKeyKind.Hash => "hash",
        QueryTagKeyKind.Object => "object",
        _ => "caller",
    };

    /// <summary>
    /// Collapses sightings to one entry per stored identity (kind, key, application, login, host, database; an empty
    /// field and a missing one are the same, as the unique index reads them), counting occurrences and keeping the first
    /// non-empty object and text sample.
    /// </summary>
    internal static List<CollapsedSighting> Collapse(int serverId, IReadOnlyList<QueryTagSighting> sightings)
    {
        var set = new SightingSet(serverId);
        foreach (var s in sightings)
        {
            set.Add(s);
        }

        return set.Items;
    }

    /// <summary>
    /// Records <paramref name="sightings"/> for one server straight to the store. Opens no connection for an empty list.
    /// The collectors do not use this per batch (they buffer, <see cref="QueryTagSightingBuffer"/>); it is the write the
    /// buffer's flush makes, for one server.
    /// </summary>
    /// <returns>How many distinct sightings were written, or 0 when none were attempted or the write failed.</returns>
    public static async Task<int> RecordAsync(
        NpgsqlDataSource postgres,
        int serverId,
        IReadOnlyList<QueryTagSighting> sightings,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(sightings);

        if (sightings.Count == 0)
        {
            return 0;
        }

        var rows = Collapse(serverId, sightings);
        SortByIdentity(rows);
        return await WriteAsync(postgres, rows, logger, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// How many rows of a <see cref="WriteAsync"/> are in the store, kept current as each statement lands, so a caller whose
    /// write was cancelled knows which rows to keep (#5985).
    /// </summary>
    internal sealed class WriteProgress
    {
        public int Written { get; set; }
    }

    /// <summary>
    /// Puts rows in the seven-part identity order (server, kind, key, application, login, host, database; an empty field and
    /// a missing one are the same, as the unique index reads them). Every writer upserts in this one order, so two flushes
    /// that overlap on a key take its row lock in the same order and cannot wait on each other (#5985).
    /// </summary>
    internal static void SortByIdentity(List<CollapsedSighting> rows)
    {
        rows.Sort(static (a, b) =>
        {
            int c = a.ServerId.CompareTo(b.ServerId);
            c = c != 0 ? c : string.CompareOrdinal(a.Kind, b.Kind);
            c = c != 0 ? c : string.CompareOrdinal(a.Key, b.Key);
            c = c != 0 ? c : string.CompareOrdinal(a.Application ?? string.Empty, b.Application ?? string.Empty);
            c = c != 0 ? c : string.CompareOrdinal(a.Login ?? string.Empty, b.Login ?? string.Empty);
            c = c != 0 ? c : string.CompareOrdinal(a.Host ?? string.Empty, b.Host ?? string.Empty);
            return c != 0 ? c : string.CompareOrdinal(a.Database ?? string.Empty, b.Database ?? string.Empty);
        });
    }

    /// <summary>
    /// Upserts already-collapsed rows, <see cref="RowsPerStatement"/> to a statement, in the order given (callers pass
    /// <see cref="SortByIdentity"/> order). The first statement to fail for a reason other than cancellation is logged and ends
    /// the write: the rest are dropped with it (a sighting lost is a key tagged a minute later, and a store that is down is not
    /// retried row group by row group). A cancellation is not a failure of the rows: it escapes, and
    /// <paramref name="progress"/> says how many rows had landed, so the caller can keep the rest.
    /// <para>The shutdown flush passes <paramref name="stopNotice"/> (#9794): a statement that fails because the store is going
    /// down (<see cref="StoreStopNotice.IsStoreUnavailable"/>) then adds its unwritten rows to the notice, which the worker
    /// reports as one Information line for the whole stop, instead of logging the Warning here. Any other failure still logs it.</para>
    /// </summary>
    /// <returns>How many rows were written.</returns>
    internal static async Task<int> WriteAsync(
        NpgsqlDataSource postgres,
        IReadOnlyList<CollapsedSighting> rows,
        ILogger? logger,
        CancellationToken cancellationToken,
        WriteProgress? progress = null,
        StoreStopNotice? stopNotice = null)
    {
        int written = 0;
        for (int start = 0; start < rows.Count; start += RowsPerStatement)
        {
            int length = Math.Min(RowsPerStatement, rows.Count - start);
            try
            {
                await using var connection = await postgres.OpenConnectionAsync(StoreSessionNames.QueryTags, cancellationToken).ConfigureAwait(false);
                await using var command = new NpgsqlCommand(UpsertSql, connection) { CommandTimeout = CommandTimeoutSeconds };
                var serverIds = new int[length];
                var counts = new long[length];
                for (int i = 0; i < length; i++)
                {
                    serverIds[i] = rows[start + i].ServerId;
                    counts[i] = rows[start + i].Count;
                }

                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = serverIds });
                AddText(command, rows, start, length, c => c.Kind);
                AddText(command, rows, start, length, c => c.Key);
                AddText(command, rows, start, length, c => c.Database);
                AddText(command, rows, start, length, c => c.ObjectName);
                AddText(command, rows, start, length, c => c.Application);
                AddText(command, rows, start, length, c => c.Login);
                AddText(command, rows, start, length, c => c.Host);
                AddText(command, rows, start, length, c => c.TextSample);
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = counts });
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = ReSightingBackdate });
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = ReSightingRefreshWidth });
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                written += length;
                if (progress is not null)
                {
                    progress.Written = written;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                if (stopNotice is not null && StoreStopNotice.IsStoreUnavailable(ex))
                {
                    stopNotice.NotWritten(string.Create(CultureInfo.InvariantCulture, $"{rows.Count - start} query-tag sighting(s)"));
                    return written;
                }

                logger?.LogWarning(
                    "Query-tag sightings: writing {Count} sighting(s) failed; they and the {Rest} after them are dropped and tagged when next sighted: {Message}",
                    length, rows.Count - start - length, ex.Message);
                return written;
            }
        }

        return written;
    }

    private static void AddText(NpgsqlCommand command, IReadOnlyList<CollapsedSighting> rows, int start, int length, Func<CollapsedSighting, string?> select)
    {
        var values = new string?[length];
        for (int i = 0; i < length; i++)
        {
            values[i] = select(rows[start + i]);
        }

        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = values });
    }

    /* A NUL character is not valid text in PostgreSQL and would fail the whole statement, losing the batch's sightings. */
    private static string? Clean(string? text) =>
        text is null || text.IndexOf('\0', StringComparison.Ordinal) < 0 ? text : text.Replace("\0", string.Empty, StringComparison.Ordinal);

    private static string? NullIfEmpty(string? text) => string.IsNullOrEmpty(text) ? null : text;

    /* A cut that would split a surrogate pair stops one character earlier, so the sample is always valid text for the store. */
    private static string? CutSample(string? text)
    {
        if (text is null || text.Length <= TextSampleChars)
        {
            return text;
        }

        int length = TextSampleChars;
        if (char.IsHighSurrogate(text[length - 1]))
        {
            length--;
        }

        return text.Substring(0, length);
    }

    /// <summary>
    /// The sightings of one server collapsed to one entry per stored identity, in first-seen order. The one place the
    /// identity is defined, so the direct write and the buffer agree with the unique index.
    /// </summary>
    internal sealed class SightingSet
    {
        private readonly int _serverId;
        private readonly Dictionary<(string, string, string, string, string, string), int> _byIdentity = new();

        public SightingSet(int serverId)
        {
            _serverId = serverId;
        }

        public List<CollapsedSighting> Items { get; } = new();

        public int Count => Items.Count;

        /// <summary>
        /// Folds an already-collapsed row back in (a flush that did not write it, #5985): counts are summed, and the row's
        /// object and text sample win over a newer entry's, since the row is the older sighting and the first sample is kept.
        /// </summary>
        public void Merge(CollapsedSighting row)
        {
            var identity = (row.Kind, row.Key, row.Application ?? string.Empty, row.Login ?? string.Empty, row.Host ?? string.Empty, row.Database ?? string.Empty);
            if (_byIdentity.TryGetValue(identity, out int at))
            {
                var held = Items[at];
                held.Count += row.Count;
                held.ObjectName = row.ObjectName ?? held.ObjectName;
                held.TextSample = row.TextSample ?? held.TextSample;
                return;
            }

            _byIdentity[identity] = Items.Count;
            Items.Add(row);
        }

        /// <summary>Adds one sighting; a key already held is counted, and keeps its first object and text sample.</summary>
        public void Add(QueryTagSighting s)
        {
            if (string.IsNullOrEmpty(s.Key))
            {
                return;
            }

            string key = Clean(s.Key)!;
            string kind = KindText(s.KeyKind);
            string? app = NullIfEmpty(Clean(s.Application));
            string? login = NullIfEmpty(Clean(s.Login));
            string? host = NullIfEmpty(Clean(s.Host));
            string? database = NullIfEmpty(Clean(s.Database));
            var identity = (kind, key, app ?? string.Empty, login ?? string.Empty, host ?? string.Empty, database ?? string.Empty);
            if (_byIdentity.TryGetValue(identity, out int at))
            {
                var seen = Items[at];
                seen.Count++;
                seen.ObjectName ??= NullIfEmpty(Clean(s.ObjectRef?.Qualified));
                seen.TextSample ??= NullIfEmpty(Clean(CutSample(s.TextSample)));
                return;
            }

            _byIdentity[identity] = Items.Count;
            Items.Add(new CollapsedSighting
            {
                ServerId = _serverId,
                Kind = kind,
                Key = key,
                Database = database,
                ObjectName = NullIfEmpty(Clean(s.ObjectRef?.Qualified)),
                Application = app,
                Login = login,
                Host = host,
                TextSample = NullIfEmpty(Clean(CutSample(s.TextSample))),
                Count = 1,
            });
        }
    }

    internal sealed class CollapsedSighting
    {
        public int ServerId { get; init; }
        public string Kind { get; init; } = string.Empty;
        public string Key { get; init; } = string.Empty;
        public string? Database { get; init; }
        public string? ObjectName { get; set; }
        public string? Application { get; init; }
        public string? Login { get; init; }
        public string? Host { get; init; }
        public string? TextSample { get; set; }
        public long Count { get; set; }
    }
}
