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
/// caller in the same database again bumps <c>last_seen</c> and <c>seen_count</c> and keeps its first text sample.</para>
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
    /// only that one, so every later sighting's text is thrown away; cutting it as it is buffered keeps the buffer small.
    /// Text rules and <c>#tag</c> comments read this sample, so they see the first 500 characters of the first-sighted text.
    /// </summary>
    public const int TextSampleChars = 500;

    /// <summary>
    /// The most rows one statement carries. A flush larger than this is several statements, so a buffer at its cap is
    /// not one multi-megabyte parameter. The value is threaded into the loop below, never written in the SQL (#3684).
    /// </summary>
    public const int RowsPerStatement = 10_000;

    /// <summary>Statement parameters: $1..$10, the ten columns of the flush as arrays (server id first, the count last).</summary>
    internal const string UpsertSql = """
INSERT INTO collect.query_tag_sightings AS t
    (server_id, key_kind, key, database_name, object_name, application_name, login_name, host_name, text_sample, seen_count)
SELECT u.server_id, u.key_kind, u.key, u.database_name, u.object_name, u.application_name, u.login_name, u.host_name, u.text_sample, u.seen_count
FROM unnest($1::integer[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[], $9::text[], $10::bigint[])
    AS u(server_id, key_kind, key, database_name, object_name, application_name, login_name, host_name, text_sample, seen_count)
ON CONFLICT (server_id, key_kind, key, COALESCE(application_name, ''), COALESCE(login_name, ''), COALESCE(host_name, ''), COALESCE(database_name, ''))
DO UPDATE SET
    last_seen = now(),
    seen_count = t.seen_count + EXCLUDED.seen_count,
    object_name = COALESCE(t.object_name, EXCLUDED.object_name),
    text_sample = COALESCE(t.text_sample, EXCLUDED.text_sample)
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

        return await WriteAsync(postgres, Collapse(serverId, sightings), logger, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Upserts already-collapsed rows, <see cref="RowsPerStatement"/> to a statement. The first failing statement is logged
    /// and ends the write: the rest are dropped with it (a sighting lost is a key tagged a minute later, and a store that
    /// is down is not retried row group by row group).
    /// </summary>
    /// <returns>How many rows were written.</returns>
    internal static async Task<int> WriteAsync(
        NpgsqlDataSource postgres,
        IReadOnlyList<CollapsedSighting> rows,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        int written = 0;
        for (int start = 0; start < rows.Count; start += RowsPerStatement)
        {
            int length = Math.Min(RowsPerStatement, rows.Count - start);
            try
            {
                await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
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
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                written += length;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
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
