/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One row of <c>config.query_tag_map</c>: the one tag a key resolved to, and why.</summary>
public sealed record QueryTagMapRow(
    string KeyKind,
    string Key,
    int TagId,
    string Source,
    int? RuleId,
    string? MatchedField,
    string? MatchedValue);

/// <summary>A key whose sightings changed, with the newest <c>last_seen</c> among its sighting rows.</summary>
public sealed record QueryTagChangedKey(string KeyKind, string Key, DateTime LastSeen);

/// <summary>
/// #9308: one server's extend cursor as stored, with the <c>rules_version</c> the map was built at. <see cref="Since"/> is
/// a UTC time that came from the store's own clock or a sighting's <c>last_seen</c>, so it round-trips exactly.
/// <see cref="RebuiltAt"/> is the store clock at this server's last rebuild (the weekly staggered rebuild reads it). A cursor
/// stored before that field existed has none and decodes as <see cref="DateTime.MinValue"/>, "rebuilt long ago", so it is due.
/// </summary>
public sealed record QueryTagMapStoredCursor(long RulesVersion, DateTime Since, string AfterKind, string AfterKey, DateTime RebuiltAt = default)
{
    private sealed record Wire(long V, long T, string K, string Key, long? R = null);

    public string Encode() =>
        JsonSerializer.Serialize(new Wire(
            RulesVersion, DateTime.SpecifyKind(Since, DateTimeKind.Utc).Ticks, AfterKind, AfterKey,
            DateTime.SpecifyKind(RebuiltAt, DateTimeKind.Utc).Ticks));

    public static QueryTagMapStoredCursor? TryDecode(string text)
    {
        try
        {
            var wire = JsonSerializer.Deserialize<Wire>(text);
            return wire is { K: not null, Key: not null }
                ? new QueryTagMapStoredCursor(
                    wire.V, new DateTime(wire.T, DateTimeKind.Utc), wire.K, wire.Key,
                    new DateTime(wire.R ?? 0, DateTimeKind.Utc))
                : null;
        }
        catch (Exception ex) when (ex is JsonException or ArgumentOutOfRangeException)
        {
            return null;
        }
    }
}

/// <summary>Everything the resolver needs, read in one repeatable-read snapshot so the version matches the rules.</summary>
public sealed record QueryTagMapSnapshot(
    long RulesVersion,
    long BuiltVersion,
    IReadOnlyList<QueryTagRule> Rules,
    IReadOnlyList<QueryTagHandAssignment> Assignments,
    IReadOnlyDictionary<string, long> TagIdsByName);

/// <summary>
/// The SQL behind <see cref="QueryTagMapBuilder"/> (#5985, part of #5971): reading the rules, the sightings and the
/// version state, and writing <c>config.query_tag_map</c>. It evaluates no rule; the resolver does.
/// </summary>
public static class QueryTagMapStore
{
    public const int CommandTimeoutSeconds = 60;

    /// <summary>The version and the tag names, read together with the rules and assignments in one snapshot.</summary>
    internal const string ReadStateSql = "SELECT rules_version, built_version FROM config.query_tag_state WHERE id = 1";

    internal const string ReadTagsSql = "SELECT id, name FROM config.query_tags";

    internal const string ReadRulesSql =
        "SELECT id, match_field, pattern, priority, enabled, tag_id, server_id FROM config.query_tag_rules WHERE enabled";

    internal const string ReadAssignmentsSql =
        "SELECT tag_id, kind, server_id, query_hash, database_name, schema_name, object_name FROM config.query_tag_assignments";

    /// <summary>
    /// The keys of one server whose sightings changed after the cursor, oldest first, at most $5. $1 server_id,
    /// $2 the cursor's time, $3 and $4 the cursor's kind and key (the tie-break among keys with the same time: one
    /// batch stamps every key it touches with one <c>now()</c>, so a time alone could not step past a full page).
    /// <para>A re-sighting of a stored row moves <c>last_seen</c> at most once every six hours (#8716, #9456), to a stamp behind the extend
    /// window (<see cref="QueryTagSightingWriter.ReSightingBackdate"/>), while an insert always takes <c>now()</c>. That is
    /// enough: a key's resolution reads only the key, the caller fields, the database and the first text sample, and the
    /// only way those change between rule versions is a NEW sighting row (a new database, application, login or host),
    /// which is an insert, or a conflict that fills an object name or sample the row lacked, which the writer stamps with
    /// <c>now()</c> too. A plain re-sighting changes nothing the resolver reads, so this read leaves it out.</para>
    /// </summary>
    internal const string ChangedKeysSql = """
SELECT key_kind, key, last_seen
FROM (
    SELECT key_kind, key, max(last_seen) AS last_seen
    FROM collect.query_tag_sightings
    WHERE server_id = $1 AND last_seen >= $2
    GROUP BY key_kind, key
) AS s
WHERE (last_seen, key_kind, key) > ($2, $3, $4)
ORDER BY last_seen, key_kind, key
LIMIT $5
""";

    /// <summary>
    /// The sighting rows of the listed keys. $1 server_id, $2 kinds, $3 keys (parallel arrays), in the order the unique index
    /// reads them. The resolver takes the first matching sighting, and which of a key's sightings is first is decided in
    /// code, not here (#9705): <see cref="ReadSightingsOfKeysAsync"/> sorts each key's rows by
    /// <see cref="QueryTagRebuildMerge.SlimRowOrder"/>, so a key gets one tag whatever collation the store sorts text by
    /// (#5985), and this ORDER BY has to agree with nothing. On a store whose text sorts by byte it is already that order.
    /// </summary>
    internal const string SightingsOfKeysSql = """
SELECT s.key_kind, s.key, s.database_name, s.object_name, s.application_name, s.login_name, s.host_name, s.text_sample
FROM collect.query_tag_sightings AS s
JOIN unnest($2::text[], $3::text[]) AS k(key_kind, key) ON k.key_kind = s.key_kind AND k.key = s.key
WHERE s.server_id = $1
ORDER BY s.key_kind, s.key, COALESCE(s.database_name, ''), COALESCE(s.application_name, ''), COALESCE(s.login_name, ''), COALESCE(s.host_name, '')
""";

    /// <summary>
    /// The heap blocks one page of a rebuild's read covers (#9705): 65536 blocks of 8 kB is 512 MB of one table. A rebuild reads
    /// each sightings table in block-range pages, front to back with no index, so no statement of it grows with the server. A
    /// sighting row carries its text sample, so no index can cover it, and a read through an index costs one random heap read
    /// per row (about 80 minutes for a server of 16 million rows on a lab store). A server with a large share of the rows has a
    /// row on nearly every heap block, so the sequential read is the cheap one. The value is threaded into the read as a
    /// parameter (#3684).
    /// </summary>
    public const int RebuildPageBlocks = 65536;

    /// <summary>
    /// The settings of a rebuild's read transaction (#9705): read only, and no index, bitmap or parallel plan for the block-range
    /// pages. A page must stay a Tid Range Scan: with a low row estimate for the server the planner would otherwise pick an
    /// index scan filtered on ctid, which fetches every row of the server once per page, or a parallel sequential scan of the
    /// whole table per page.
    /// </summary>
    internal const string RebuildReadSettingsSql =
        "SET TRANSACTION READ ONLY; SET LOCAL enable_indexscan = off; SET LOCAL enable_bitmapscan = off; SET LOCAL enable_indexonlyscan = off; SET LOCAL max_parallel_workers_per_gather = 0";

    /// <summary>
    /// Puts the plan settings of <see cref="RebuildReadSettingsSql"/> back for the rest of the read transaction (#9705): the
    /// ordered read of the keys that need order (<see cref="SightingsOfKeysSql"/>) finds each key through the unique index, as
    /// the extend's does, and with index scans off it would scan the sightings table.
    /// </summary>
    internal const string RebuildOrderedReadSettingsSql =
        "SET LOCAL enable_indexscan TO DEFAULT; SET LOCAL enable_bitmapscan TO DEFAULT; SET LOCAL enable_indexonlyscan TO DEFAULT; SET LOCAL max_parallel_workers_per_gather TO DEFAULT";

    /// <summary>
    /// One of the two tables a rebuild reads (#9705), with its three statements. <see cref="BlocksSql"/> is the table's heap
    /// block count. <see cref="PageSql"/> is one block-range page: the rows of server $1 whose ctid is at or after $2 and before
    /// $3, both the first row slot of a block, so a page is whole blocks and every block is in exactly one page.
    /// <see cref="LastPageSql"/> has no upper bound, so a block added after the count was read is still read. Both tables are
    /// plain tables (never a hypertable), so a ctid names one row of the one heap. The columns come back in the order of
    /// <see cref="SightingsOfKeysSql"/>, a slim row with no text sample.
    /// </summary>
    internal sealed record RebuildTable(string BlocksSql, string PageSql, string LastPageSql);

    internal static readonly RebuildTable RebuildSightings = RebuildTableOf(
        "collect.query_tag_sightings",
        "key_kind, key, database_name, object_name, application_name, login_name, host_name, text_sample");

    internal static readonly RebuildTable RebuildSlimSightings = RebuildTableOf(
        "collect.query_tag_sightings_slim",
        "key_kind, key, database_name, object_name, application_name, login_name, host_name, NULL::text");

    private static RebuildTable RebuildTableOf(string table, string columns)
    {
        string page = $"SELECT {columns} FROM {table} WHERE server_id = $1 AND ctid >= $2";
        return new RebuildTable(
            $"SELECT pg_relation_size('{table}'::regclass) / current_setting('block_size')::bigint",
            page + " AND ctid < $3",
            page);
    }

    internal const string DeleteServerMapSql = "DELETE FROM config.query_tag_map WHERE server_id = $1";

    /// <summary>
    /// The most map rows one statement of a rebuild's write carries (#9705). A rebuild writes a server's whole map in one
    /// transaction, as batches of this many rows, so no statement and no parameter array grows with the server. Measured
    /// cold on a lab store with a 4 GB memory limit, writing a map of 8.36 million rows, one run a size: the inserts take
    /// 160 s at 1,000 rows a statement and 153 to 154 s at 10,000, 50,000, 200,000 and 1,000,000, and write 3,790 MiB of
    /// WAL at every size (475 bytes a row, the table and its three indexes), so neither gains past 10,000. What grows with
    /// the batch is memory and the time of one statement. The store's backend holds 5 MiB at 1,000 rows, 10 MiB at 10,000,
    /// 35 MiB at 50,000, 127 MiB at 200,000 and 581 MiB at 1,000,000, the slowest statement takes 0.3 s at 10,000 and 19 s
    /// at 1,000,000 against a command timeout of 60 s, and the writing process peaks 380 MB lower at 10,000 than at 50,000.
    /// All 8.36 million rows in one statement take 167 s, 2.8 times that timeout, with 3.07 GiB in the backend, and sent
    /// inside a rebuild, after its read, that statement ended the backend at the store's memory limit. The value is threaded
    /// into the write as a parameter (#3684).
    /// </summary>
    public const int MapWriteBatchRows = 10000;

    /// <summary>Inserts the resolved rows. $1 server_id, $2..$8 the seven columns as parallel arrays.</summary>
    internal const string InsertMapSql = """
INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source, rule_id, matched_field, matched_value)
SELECT $1, u.key_kind, u.key, u.tag_id, u.source, u.rule_id, u.matched_field, u.matched_value
FROM unnest($2::text[], $3::text[], $4::int[], $5::text[], $6::int[], $7::text[], $8::text[])
    AS u(key_kind, key, tag_id, source, rule_id, matched_field, matched_value)
""";

    /// <summary>
    /// The extend path's write: inserts new rows and updates a changed one, and skips a row that already says the
    /// same (so a key re-sighted every minute does not rewrite its map row). Never deletes.
    /// </summary>
    internal const string UpsertMapSql = InsertMapSql + """

ON CONFLICT (server_id, key_kind, key) DO UPDATE SET
    tag_id = EXCLUDED.tag_id,
    source = EXCLUDED.source,
    rule_id = EXCLUDED.rule_id,
    matched_field = EXCLUDED.matched_field,
    matched_value = EXCLUDED.matched_value,
    built_at = now()
WHERE (config.query_tag_map.tag_id, config.query_tag_map.source, config.query_tag_map.rule_id,
       config.query_tag_map.matched_field, config.query_tag_map.matched_value)
    IS DISTINCT FROM (EXCLUDED.tag_id, EXCLUDED.source, EXCLUDED.rule_id, EXCLUDED.matched_field, EXCLUDED.matched_value)
""";

    /// <summary>Stamps the version a rebuild read at its start. Never moves it backwards. $1 version.</summary>
    internal const string StampBuiltVersionSql =
        "UPDATE config.query_tag_state SET built_version = $1, updated_at = now() WHERE id = 1 AND built_version < $1";

    /// <summary>
    /// #9308: where a server's extend cursor lives across a service start, in <c>collect.collector_state</c> (no migration: the
    /// table is the store's small per-server state). One row per server, <c>(server_id, 'query_tag_map', 'cursor')</c>, whose value
    /// is the JSON of <see cref="QueryTagMapStoredCursor"/>.
    /// </summary>
    public const string CursorStateOwner = "query_tag_map";

    public const string CursorStateKey = "cursor";

    /// <summary>Writes one server's cursor. $1 server_id, $2 the cursor's text. The cursor commits in the transaction of the map write it describes.</summary>
    internal const string SaveCursorSql = """
INSERT INTO collect.collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1, 'query_tag_map', 'cursor', $2, now() AT TIME ZONE 'UTC')
ON CONFLICT (server_id, collector_name, state_key) DO UPDATE SET
    state_value = EXCLUDED.state_value,
    updated_at = EXCLUDED.updated_at
""";

    /// <summary>Reads one server's stored cursor. $1 server_id.</summary>
    internal const string ReadCursorSql =
        "SELECT state_value FROM collect.collector_state WHERE server_id = $1 AND collector_name = 'query_tag_map' AND state_key = 'cursor'";

    /// <summary>Drops the stored cursor of every server not in the list. $1 the listed server ids.</summary>
    internal const string DropCursorsOutsideSql =
        "DELETE FROM collect.collector_state WHERE collector_name = 'query_tag_map' AND state_key = 'cursor' AND server_id <> ALL($1::int[])";

    /// <summary>The database's own clock, so a cursor and a sighting's <c>last_seen</c> are on one clock.</summary>
    public static async Task<DateTime> ClockAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT clock_timestamp()", connection) { CommandTimeout = CommandTimeoutSeconds };
        return (DateTime)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
    }

    public static async Task<QueryTagMapSnapshot> ReadSnapshotAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken).ConfigureAwait(false);

        long rulesVersion = 1;
        long builtVersion = 0;
        await using (var command = new NpgsqlCommand(ReadStateSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                rulesVersion = reader.GetInt64(0);
                builtVersion = reader.GetInt64(1);
            }
        }

        /* Two tags that fold to one name are ambiguous to a tag comment, which then matches neither: leave the name out. */
        var byFolded = new Dictionary<string, (long Id, int Count)>(StringComparer.OrdinalIgnoreCase);
        await using (var command = new NpgsqlCommand(ReadTagsSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                string name = reader.GetString(1).Trim();
                byFolded[name] = byFolded.TryGetValue(name, out var seen) ? (seen.Id, seen.Count + 1) : (reader.GetInt32(0), 1);
            }
        }

        var tagIdsByName = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        foreach (var (name, (id, count)) in byFolded)
        {
            if (count == 1)
            {
                tagIdsByName[name] = id;
            }
        }

        var rules = new List<QueryTagRule>();
        await using (var command = new NpgsqlCommand(ReadRulesSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (TryParseField(reader.GetString(1), out var field))
                {
                    rules.Add(new QueryTagRule(
                        reader.GetInt32(0),
                        field,
                        reader.GetString(2),
                        reader.GetInt32(3),
                        reader.GetBoolean(4),
                        reader.IsDBNull(5) ? null : reader.GetInt32(5),
                        reader.IsDBNull(6) ? null : reader.GetInt32(6)));
                }
            }
        }

        var assignments = new List<QueryTagHandAssignment>();
        await using (var command = new NpgsqlCommand(ReadAssignmentsSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                int tagId = reader.GetInt32(0);
                string kind = reader.GetString(1);
                int? serverId = reader.IsDBNull(2) ? null : reader.GetInt32(2);
                if (kind == "hash")
                {
                    string raw = reader.GetString(3);
                    assignments.Add(new QueryTagHandAssignment(tagId, QueryTagKeyKind.Hash, QueryHashIdentity.CanonicalKey(raw) ?? raw, null, serverId));
                }
                else if (kind == "object")
                {
                    var objectRef = new QueryTagObject(reader.GetString(4), reader.GetString(5), reader.GetString(6));
                    assignments.Add(new QueryTagHandAssignment(tagId, QueryTagKeyKind.Object, null, objectRef, serverId));
                }
            }
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new QueryTagMapSnapshot(rulesVersion, builtVersion, rules, assignments, tagIdsByName);
    }

    public static async Task<List<QueryTagChangedKey>> ReadChangedKeysAsync(
        NpgsqlConnection connection,
        int serverId,
        DateTime sinceUtc,
        string afterKind,
        string afterKey,
        int limit,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(ChangedKeysSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.TimestampTz, Value = DateTime.SpecifyKind(sinceUtc, DateTimeKind.Utc) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = afterKind });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = afterKey });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = limit });

        var keys = new List<QueryTagChangedKey>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            keys.Add(new QueryTagChangedKey(reader.GetString(0), reader.GetString(1), reader.GetFieldValue<DateTime>(2)));
        }

        return keys;
    }

    /// <summary>The sightings of the listed keys, grouped by (kind, key).</summary>
    public static async Task<Dictionary<(string Kind, string Key), List<QueryTagSighting>>> ReadSightingsOfKeysAsync(
        NpgsqlConnection connection,
        int serverId,
        IReadOnlyList<QueryTagChangedKey> keys,
        CancellationToken cancellationToken)
    {
        var grouped = new Dictionary<(string, string), List<QueryTagSighting>>();
        if (keys.Count == 0)
        {
            return grouped;
        }

        var kinds = new string[keys.Count];
        var names = new string[keys.Count];
        for (int i = 0; i < keys.Count; i++)
        {
            kinds[i] = keys[i].KeyKind;
            names[i] = keys[i].Key;
        }

        await using var command = new NpgsqlCommand(SightingsOfKeysSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = kinds });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = names });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var sighting = ReadSighting(reader);
            var id = (reader.GetString(0), reader.GetString(1));
            if (!grouped.TryGetValue(id, out var list))
            {
                grouped[id] = list = new List<QueryTagSighting>();
            }

            list.Add(sighting);
        }

        /* The first sighting in order decides a key's tag, and that order is the code's, not the statement's (#9705). The
           sort is stable, so rows the store already returned in this order stay as they came. */
        foreach (var id in grouped.Keys.ToList())
        {
            if (grouped[id].Count > 1)
            {
                grouped[id] = grouped[id].OrderBy(sighting => sighting, QueryTagRebuildMerge.SlimRowComparer).ToList();
            }
        }

        return grouped;
    }

    /// <summary>
    /// Opens the one transaction a rebuild reads in (#9705): REPEATABLE READ, so every page and the ordered re-read see one
    /// snapshot, as one statement would, and a purge that moves a sighting to the slim table between two pages cannot hide the
    /// row from both. The caller commits it when the read is done, before the map write.
    /// </summary>
    internal static async Task<NpgsqlTransaction> BeginRebuildReadAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken).ConfigureAwait(false);
        try
        {
            await using var command = new NpgsqlCommand(RebuildReadSettingsSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await transaction.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return transaction;
    }

    /// <summary>Inside a rebuild's read transaction: lets the ordered read of a few keys use the indexes again (<see cref="RebuildOrderedReadSettingsSql"/>).</summary>
    internal static async Task UseOrderedReadPlansAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(RebuildOrderedReadSettingsSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Streams every row of one server from one of the rebuild's tables (#9705), each row once, in heap order, in pages of
    /// <paramref name="pageBlocks"/> heap blocks, so each statement is bounded by the page and not by the server, and no list of
    /// the server's rows is held. The block count is read first, inside the caller's snapshot when it has one
    /// (<see cref="BeginRebuildReadAsync"/>). The rows come in no key order: <see cref="QueryTagRebuildMerge"/> resolves them.
    /// </summary>
    internal static async IAsyncEnumerable<QueryTagSighting> ReadRebuildRowsAsync(
        NpgsqlConnection connection,
        int serverId,
        RebuildTable table,
        int pageBlocks,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        long blocks;
        await using (var count = new NpgsqlCommand(table.BlocksSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        {
            blocks = Convert.ToInt64(await count.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), System.Globalization.CultureInfo.InvariantCulture);
        }

        long step = Math.Max(1, pageBlocks);
        long from = 0;
        while (true)
        {
            long to = from + step;
            bool last = to >= blocks;
            await using var command = new NpgsqlCommand(last ? table.LastPageSql : table.PageSql, connection) { CommandTimeout = CommandTimeoutSeconds };
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Tid, Value = new NpgsqlTid((uint)from, 0) });
            if (!last)
            {
                command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Tid, Value = new NpgsqlTid((uint)to, 0) });
            }

            await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    yield return ReadSighting(reader);
                }
            }

            if (last)
            {
                yield break;
            }

            from = to;
        }
    }

    /// <summary>The map row of a key that resolved to a tag.</summary>
    internal static QueryTagMapRow MapRow(QueryTagKeyKind kind, string key, QueryTagResolution resolution) =>
        new(
            QueryTagSightingWriter.KindText(kind),
            key,
            checked((int)resolution.TagId!.Value),
            SourceText(resolution.Source),
            resolution.RuleId is { } ruleId ? checked((int)ruleId) : null,
            resolution.MatchedField is { } field ? FieldText(field) : null,
            resolution.MatchedValue);

    /// <summary>
    /// Replaces one server's map rows with <paramref name="rows"/> in one transaction: a reader sees the old map or
    /// the new one, and a failure part way, in any batch, rolls back to the old one. The rows are taken from
    /// <paramref name="rows"/> as they are written and inserted <paramref name="batchRows"/> to a statement (#9705), so the
    /// write holds one batch and a caller that makes its rows on demand never holds the server's map. A
    /// <paramref name="cursor"/> is stored in the same transaction (#9308), so a stored cursor is never ahead of the map it
    /// describes.
    /// </summary>
    public static async Task ReplaceMapAsync(
        NpgsqlConnection connection,
        int serverId,
        IEnumerable<QueryTagMapRow> rows,
        CancellationToken cancellationToken,
        QueryTagMapStoredCursor? cursor = null,
        int batchRows = MapWriteBatchRows)
    {
        ArgumentNullException.ThrowIfNull(rows);
        int batchSize = Math.Max(1, batchRows);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await using (var delete = new NpgsqlCommand(DeleteServerMapSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            await delete.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var batch = new List<QueryTagMapRow>();
        foreach (var row in rows)
        {
            batch.Add(row);
            if (batch.Count == batchSize)
            {
                await InsertBatchAsync().ConfigureAwait(false);
            }
        }

        if (batch.Count > 0)
        {
            await InsertBatchAsync().ConfigureAwait(false);
        }

        async Task InsertBatchAsync()
        {
            await using var insert = MapCommand(InsertMapSql, connection, transaction, serverId, batch);
            await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            batch.Clear();
        }

        if (cursor is not null)
        {
            await SaveCursorAsync(connection, transaction, serverId, cursor, cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The extend path's write: adds and changes rows, never deletes one. A <paramref name="cursor"/> is stored in the same
    /// transaction as the rows (#9308), and with no rows it is stored alone.
    /// </summary>
    public static async Task UpsertMapAsync(
        NpgsqlConnection connection,
        int serverId,
        IReadOnlyList<QueryTagMapRow> rows,
        CancellationToken cancellationToken,
        QueryTagMapStoredCursor? cursor = null)
    {
        if (cursor is null)
        {
            if (rows.Count > 0)
            {
                await using var command = MapCommand(UpsertMapSql, connection, null, serverId, rows);
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            return;
        }

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        if (rows.Count > 0)
        {
            await using var command = MapCommand(UpsertMapSql, connection, transaction, serverId, rows);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await SaveCursorAsync(connection, transaction, serverId, cursor, cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task SaveCursorAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, QueryTagMapStoredCursor cursor, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(SaveCursorSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = cursor.Encode() });
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The cursor stored for one server, or null when there is none or its text does not read back.</summary>
    public static async Task<QueryTagMapStoredCursor?> ReadCursorAsync(NpgsqlConnection connection, int serverId, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(ReadCursorSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is string text ? QueryTagMapStoredCursor.TryDecode(text) : null;
    }

    /// <summary>Drops the stored cursor of every server that is not in <paramref name="serverIds"/>.</summary>
    public static async Task DropCursorsOutsideAsync(
        NpgsqlConnection connection, IReadOnlyCollection<int> serverIds, CancellationToken cancellationToken, NpgsqlTransaction? transaction = null)
    {
        await using var command = new NpgsqlCommand(DropCursorsOutsideSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = serverIds.ToArray() });
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public static async Task StampBuiltVersionAsync(NpgsqlConnection connection, long version, CancellationToken cancellationToken, NpgsqlTransaction? transaction = null)
    {
        await using var command = new NpgsqlCommand(StampBuiltVersionSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = version });
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The store's text for a resolver source, matching the map's CHECK.</summary>
    public static string SourceText(QueryTagSource source) => source switch
    {
        QueryTagSource.HandHash => "hand_hash",
        QueryTagSource.HandObject => "hand_object",
        _ => "rule",
    };

    /// <summary>The store's text for a rule field, matching the rules table's CHECK.</summary>
    public static string FieldText(QueryTagRuleField field) => field switch
    {
        QueryTagRuleField.Application => "application",
        QueryTagRuleField.Login => "login",
        QueryTagRuleField.Host => "host",
        QueryTagRuleField.Database => "database",
        QueryTagRuleField.Object => "object",
        QueryTagRuleField.Text => "text",
        _ => "text_tag",
    };

    internal static bool TryParseField(string text, out QueryTagRuleField field)
    {
        switch (text)
        {
            case "application": field = QueryTagRuleField.Application; return true;
            case "login": field = QueryTagRuleField.Login; return true;
            case "host": field = QueryTagRuleField.Host; return true;
            case "database": field = QueryTagRuleField.Database; return true;
            case "object": field = QueryTagRuleField.Object; return true;
            case "text": field = QueryTagRuleField.Text; return true;
            case "text_tag": field = QueryTagRuleField.TextTag; return true;
            default: field = default; return false;
        }
    }

    private static NpgsqlCommand MapCommand(
        string sql, NpgsqlConnection connection, NpgsqlTransaction? transaction, int serverId, IReadOnlyList<QueryTagMapRow> rows)
    {
        var kinds = new string[rows.Count];
        var keys = new string[rows.Count];
        var tagIds = new int[rows.Count];
        var sources = new string[rows.Count];
        var ruleIds = new int?[rows.Count];
        var fields = new string?[rows.Count];
        var values = new string?[rows.Count];
        for (int i = 0; i < rows.Count; i++)
        {
            kinds[i] = rows[i].KeyKind;
            keys[i] = rows[i].Key;
            tagIds[i] = rows[i].TagId;
            sources[i] = rows[i].Source;
            ruleIds[i] = rows[i].RuleId;
            fields[i] = rows[i].MatchedField;
            values[i] = rows[i].MatchedValue;
        }

        var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = kinds });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = keys });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = tagIds });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = sources });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = ruleIds });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = fields });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = values });
        return command;
    }

    /// <summary>The key kind of a stored <c>key_kind</c> text, the inverse of <see cref="QueryTagSightingWriter.KindText"/>.</summary>
    internal static QueryTagKeyKind KindOf(string kind) => kind switch
    {
        "hash" => QueryTagKeyKind.Hash,
        "object" => QueryTagKeyKind.Object,
        _ => QueryTagKeyKind.Caller,
    };

    private static QueryTagSighting ReadSighting(NpgsqlDataReader reader)
    {
        string kind = reader.GetString(0);
        string key = reader.GetString(1);
        string? database = reader.IsDBNull(2) ? null : reader.GetString(2);
        string? objectName = reader.IsDBNull(3) ? null : reader.GetString(3);
        string? application = reader.IsDBNull(4) ? null : reader.GetString(4);
        string? login = reader.IsDBNull(5) ? null : reader.GetString(5);
        string? host = reader.IsDBNull(6) ? null : reader.GetString(6);
        string? sample = reader.IsDBNull(7) ? null : reader.GetString(7);
        var keyKind = KindOf(kind);

        /* An object key's own text is the qualified name, so it carries the object even if the column was empty. */
        var objectRef = ParseQualified(objectName ?? (keyKind == QueryTagKeyKind.Object ? key : null), database);
        return new QueryTagSighting(keyKind, key, database, objectRef, application, login, host, sample);
    }

    /// <summary>
    /// Reads a <c>database.schema.name</c> text (<see cref="QueryTagObject.Qualified"/>, the only form the sightings
    /// table keeps) back into its parts. The database is the text before the first dot, or the sighting's own database
    /// when the text starts with it (a database name may carry dots); a schema is the text up to the next dot. A name
    /// with dots in its schema is read at the first dot, which is the only ambiguity the one column leaves.
    /// </summary>
    internal static QueryTagObject? ParseQualified(string? qualified, string? database)
    {
        if (string.IsNullOrEmpty(qualified))
        {
            return null;
        }

        string rest;
        string? db;
        if (!string.IsNullOrEmpty(database) && qualified.StartsWith(database + ".", StringComparison.Ordinal))
        {
            db = database;
            rest = qualified[(database.Length + 1)..];
        }
        else
        {
            int first = qualified.IndexOf('.', StringComparison.Ordinal);
            if (first < 0)
            {
                return new QueryTagObject(null, null, qualified);
            }

            db = first == 0 ? null : qualified[..first];
            rest = qualified[(first + 1)..];
        }

        int second = rest.IndexOf('.', StringComparison.Ordinal);
        return second < 0
            ? new QueryTagObject(db, null, rest)
            : new QueryTagObject(db, second == 0 ? null : rest[..second], rest[(second + 1)..]);
    }
}
