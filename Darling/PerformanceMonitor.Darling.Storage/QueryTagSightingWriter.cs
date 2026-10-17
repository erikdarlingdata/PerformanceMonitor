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
/// Writes one batch's query-tag sightings to <c>collect.query_tag_sightings</c> (#5985, part of #5971): the record
/// of every query key and caller a collector saw, which the map builder (<see cref="QueryTagMapBuilder"/>) resolves.
///
/// <para><b>One statement per batch, and no connection for an empty batch.</b> The same key sighted twice in a
/// batch is collapsed here first (a single <c>INSERT ... ON CONFLICT DO UPDATE</c> cannot touch one row twice), with
/// the occurrences added to <c>seen_count</c>. The conflict target is the table's own unique index, so a key seen
/// under the same caller again bumps <c>last_seen</c> and <c>seen_count</c> and keeps its first text sample.</para>
///
/// <para><b>Never throws.</b> A sighting that cannot be written is a key the next pass tags a little later, never a
/// reason to fail a collection cycle that stored every row it read (the shape of
/// <see cref="OversizedPlanBacklog.RecordSightingsAsync"/>).</para>
/// </summary>
public static class QueryTagSightingWriter
{
    public const int CommandTimeoutSeconds = 30;

    /// <summary>Statement parameters: $1 server_id, $2..$10 the nine columns of the batch as arrays.</summary>
    internal const string UpsertSql = """
INSERT INTO collect.query_tag_sightings AS t
    (server_id, key_kind, key, database_name, object_name, application_name, login_name, host_name, text_sample, seen_count)
SELECT $1, u.key_kind, u.key, u.database_name, u.object_name, u.application_name, u.login_name, u.host_name, u.text_sample, u.seen_count
FROM unnest($2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[], $9::text[], $10::bigint[])
    AS u(key_kind, key, database_name, object_name, application_name, login_name, host_name, text_sample, seen_count)
ON CONFLICT (server_id, key_kind, key, COALESCE(application_name, ''), COALESCE(login_name, ''), COALESCE(host_name, ''))
DO UPDATE SET
    last_seen = now(),
    seen_count = t.seen_count + EXCLUDED.seen_count,
    database_name = COALESCE(t.database_name, EXCLUDED.database_name),
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
    /// Collapses a batch to one entry per stored identity (kind, key, application, login, host; an empty caller field
    /// and a missing one are the same, as the unique index reads them), counting occurrences and keeping the first
    /// non-empty database, object and text sample.
    /// </summary>
    internal static List<CollapsedSighting> Collapse(IReadOnlyList<QueryTagSighting> sightings)
    {
        var byIdentity = new Dictionary<(string, string, string, string, string), int>();
        var result = new List<CollapsedSighting>();
        foreach (var s in sightings)
        {
            if (string.IsNullOrEmpty(s.Key))
            {
                continue;
            }

            string key = Clean(s.Key)!;
            string kind = KindText(s.KeyKind);
            string? app = NullIfEmpty(Clean(s.Application));
            string? login = NullIfEmpty(Clean(s.Login));
            string? host = NullIfEmpty(Clean(s.Host));
            var identity = (kind, key, app ?? string.Empty, login ?? string.Empty, host ?? string.Empty);
            if (byIdentity.TryGetValue(identity, out int at))
            {
                var seen = result[at];
                seen.Count++;
                seen.Database ??= NullIfEmpty(Clean(s.Database));
                seen.ObjectName ??= NullIfEmpty(Clean(s.ObjectRef?.Qualified));
                seen.TextSample ??= NullIfEmpty(Clean(QueryTagLimits.CutText(s.TextSample)));
                continue;
            }

            byIdentity[identity] = result.Count;
            result.Add(new CollapsedSighting
            {
                Kind = kind,
                Key = key,
                Database = NullIfEmpty(Clean(s.Database)),
                ObjectName = NullIfEmpty(Clean(s.ObjectRef?.Qualified)),
                Application = app,
                Login = login,
                Host = host,
                TextSample = NullIfEmpty(Clean(QueryTagLimits.CutText(s.TextSample))),
                Count = 1,
            });
        }

        return result;
    }

    /// <summary>
    /// Records <paramref name="sightings"/> for one server. Opens no connection for an empty list.
    /// </summary>
    /// <returns>How many distinct sightings the statement wrote, or 0 when none were attempted or the write failed.</returns>
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

        var collapsed = Collapse(sightings);
        if (collapsed.Count == 0)
        {
            return 0;
        }

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = new NpgsqlCommand(UpsertSql, connection) { CommandTimeout = CommandTimeoutSeconds };
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            AddText(command, collapsed, c => c.Kind);
            AddText(command, collapsed, c => c.Key);
            AddText(command, collapsed, c => c.Database);
            AddText(command, collapsed, c => c.ObjectName);
            AddText(command, collapsed, c => c.Application);
            AddText(command, collapsed, c => c.Login);
            AddText(command, collapsed, c => c.Host);
            AddText(command, collapsed, c => c.TextSample);
            var counts = new long[collapsed.Count];
            for (int i = 0; i < counts.Length; i++)
            {
                counts[i] = collapsed[i].Count;
            }

            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = counts });
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            return collapsed.Count;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(
                "Query-tag sightings: recording {Count} sighting(s) for server {ServerId} failed and was skipped: {Message}",
                collapsed.Count, serverId, ex.Message);
            return 0;
        }
    }

    private static void AddText(NpgsqlCommand command, List<CollapsedSighting> rows, Func<CollapsedSighting, string?> select)
    {
        var values = new string?[rows.Count];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = select(rows[i]);
        }

        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = values });
    }

    /* A NUL character is not valid text in PostgreSQL and would fail the whole statement, losing the batch's sightings. */
    private static string? Clean(string? text) =>
        text is null || text.IndexOf('\0', StringComparison.Ordinal) < 0 ? text : text.Replace("\0", string.Empty, StringComparison.Ordinal);

    private static string? NullIfEmpty(string? text) => string.IsNullOrEmpty(text) ? null : text;

    internal sealed class CollapsedSighting
    {
        public string Kind { get; init; } = string.Empty;
        public string Key { get; init; } = string.Empty;
        public string? Database { get; set; }
        public string? ObjectName { get; set; }
        public string? Application { get; init; }
        public string? Login { get; init; }
        public string? Host { get; init; }
        public string? TextSample { get; set; }
        public long Count { get; set; }
    }
}
