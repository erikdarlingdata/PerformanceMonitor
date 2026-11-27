/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>What one map pass did: whether it rebuilt, how many keys it resolved and how many of them got a tag.</summary>
public sealed record QueryTagMapPass(bool Rebuilt, int KeysResolved, int KeysTagged);

/// <summary>
/// One server's extend cursor, held in memory by the caller (#9308: a copy is stored in <c>collector_state</c> with the rules version it was built at). Keys are read strictly after
/// (<see cref="Since"/>, <see cref="AfterKind"/>, <see cref="AfterKey"/>) in (last_seen, key_kind, key) order. After a pass that
/// finished under the cap it is the pass clock minus <see cref="Overlap"/> with empty kind and key; after a pass that hit the
/// cap it is the last key read, so the next pass continues after it. <see cref="RebuiltAt"/> is the clock at this server's last
/// rebuild (an extend carries it forward); a cursor stored before the field existed has none and reads as
/// <see cref="DateTime.MinValue"/>, "rebuilt long ago", so it is due for the weekly staggered rebuild.
/// </summary>
public sealed record QueryTagMapCursor(DateTime Since, string AfterKind, string AfterKey, DateTime RebuiltAt = default)
{
    /// <summary>#9308: how old a server's last rebuild may get before the weekly staggered rebuild takes it.</summary>
    public static readonly TimeSpan MaxRebuildAge = TimeSpan.FromDays(7);

    /// <summary>#9308: how long a server whose age-picked rebuild failed is left out of the age pick.</summary>
    public static readonly TimeSpan AgeRebuildBackoff = TimeSpan.FromHours(1);

    /// <summary>#9308: the time after the collector service starts in which the staggered rebuild never runs.</summary>
    public static readonly TimeSpan RebuildStartupGrace = TimeSpan.FromMinutes(20);

    /// <summary>How far behind the pass clock a finished pass sets the cursor, so a sighting stamped just before the read is seen.</summary>
    public static readonly TimeSpan Overlap = TimeSpan.FromMinutes(2);

    /// <summary>A server with no cursor (a fresh process) starts from the epoch.</summary>
    public static QueryTagMapCursor Epoch { get; } = new(DateTime.UnixEpoch, string.Empty, string.Empty);

    /// <summary>#9308: the oldest stored cursor a fresh process extends from. An older one rebuilds.</summary>
    public static readonly TimeSpan MaxStoredAge = TimeSpan.FromHours(24);

    private sealed record Wire(long V, long T, string K, string Key, long? R = null);

    /// <summary>#9308: the stored text of this cursor, with the <c>rules_version</c> the map was built at.</summary>
    public string Encode(long rulesVersion) =>
        System.Text.Json.JsonSerializer.Serialize(new Wire(
            rulesVersion, DateTime.SpecifyKind(Since, DateTimeKind.Utc).Ticks, AfterKind, AfterKey,
            DateTime.SpecifyKind(RebuiltAt, DateTimeKind.Utc).Ticks));

    /// <summary>The cursor and rules version in stored text, or null when it does not read back.</summary>
    public static (long RulesVersion, QueryTagMapCursor Cursor)? TryDecode(string text)
    {
        try
        {
            var wire = System.Text.Json.JsonSerializer.Deserialize<Wire>(text);
            return wire is { K: not null, Key: not null }
                ? (wire.V, new QueryTagMapCursor(new DateTime(wire.T, DateTimeKind.Utc), wire.K, wire.Key, new DateTime(wire.R ?? 0, DateTimeKind.Utc)))
                : null;
        }
        catch (Exception ex) when (ex is System.Text.Json.JsonException or ArgumentOutOfRangeException)
        {
            return null;
        }
    }
}

public partial class LocalDataService
{
    /* Test seam: runs between the pass's read (under the read lock) and its write (under the write lock). */
    internal Action? AfterQueryTagMapReadForTests { get; set; }

    /// <summary>
    /// #5990: the tag map pass. Reads the tags, hand assignments, rules and sightings and gives every sighted key its one
    /// tag with the shared <see cref="QueryTagResolver"/> (Lite has no matching of its own). When <c>rules_version</c> is
    /// ahead of <c>built_version</c> the pass REBUILDS: it replaces every map row in one transaction and sets
    /// <c>built_version</c> to the version it built from, so a rule written meanwhile makes the next pass rebuild again.
    /// Otherwise it EXTENDS: it resolves only the keys whose sighting was last seen at or after
    /// <paramref name="cursors"/> (per server; a missing server starts at the epoch) and upserts or removes their map rows.
    /// Each server resolves at most <paramref name="keyCap"/> keys (default <see cref="QueryTagLimits.MaxKeysPerServerPerPass"/>),
    /// oldest first, and its cursor moves on past the last key read. The method updates <paramref name="cursors"/> once the
    /// pass has committed.
    /// #9308: a pass that extends also rebuilds ONE server when its last rebuild (the cursor's <c>RebuiltAt</c>) is
    /// <see cref="QueryTagMapCursor.MaxRebuildAge"/> old, the oldest due server, so a key whose sightings retention deleted
    /// leaves the map. Lite rebuilds one server in the same transaction as the extends, so it takes the one-a-pass shape
    /// Darling does. <paramref name="serviceAge"/> is how long the collector service has run; the staggered rebuild waits for
    /// <see cref="QueryTagMapCursor.RebuildStartupGrace"/>, and a null age never staggers.
    /// A failed age rebuild must not stop every server's extend. Lite writes the rebuild and the extends in ONE transaction, so
    /// the failure rolls back the whole pass. When the pass had an age-picked server and <paramref name="ageBackoffUntil"/> is
    /// given, the server is recorded there for <see cref="QueryTagMapCursor.AgeRebuildBackoff"/> (it is left out of the age pick
    /// until then) and the pass runs once more with no age rebuild, so every server still extends. A rebuild that commits clears
    /// its server's entry. A second failure propagates to the caller, as before.
    /// A key that resolves to no tag has no map row (it reads as untagged). The write takes effect in this pass, never
    /// inside the rule or assignment write.
    /// </summary>
    public async Task<QueryTagMapPass> BuildQueryTagMapAsync(
        IDictionary<int, QueryTagMapCursor>? cursors = null, int keyCap = QueryTagLimits.MaxKeysPerServerPerPass, TimeSpan? serviceAge = null,
        IDictionary<int, DateTime>? ageBackoffUntil = null)
    {
        var pick = new AgePick();
        try
        {
            return await BuildQueryTagMapOnceAsync(cursors, keyCap, serviceAge, ageBackoffUntil, pick);
        }
        catch (Exception ex) when (ex is not OperationCanceledException && ageBackoffUntil is not null && pick.Server is { } failed)
        {
            ageBackoffUntil[failed] = DateTime.UtcNow + QueryTagMapCursor.AgeRebuildBackoff;
            return await BuildQueryTagMapOnceAsync(cursors, keyCap, null, ageBackoffUntil, new AgePick());
        }
    }

    /// <summary>The server the age rule picked in one attempt, for the caller that backs it off when the attempt fails.</summary>
    private sealed class AgePick
    {
        public int? Server { get; set; }
    }

    private async Task<QueryTagMapPass> BuildQueryTagMapOnceAsync(
        IDictionary<int, QueryTagMapCursor>? cursors, int keyCap, TimeSpan? serviceAge, IDictionary<int, DateTime>? ageBackoffUntil, AgePick pick)
    {
        /* The clock and the rules version are captured BEFORE the read. The read and the resolve run under the READ lock, so
           the UI's reads are not stalled by a rebuild; only the write takes the write lock. A sightings flush or a rule write
           that lands between the read and the write cannot leave the map stale past the next pass: the cursor is the captured
           clock minus the overlap, so the next extend pass reads what the flush added, and built_version is stamped with the
           CAPTURED rules_version, so a rule written meanwhile (a higher rules_version) makes the next pass rebuild. */
        var clock = DateTime.UtcNow;

        long rulesVersion = 1;
        long builtVersion = 0;
        HashSet<long> tagIds;
        List<(int ServerId, QueryTagKeyKind Kind, string Key, QueryTagResolution Result)> resolved;
        List<(int ServerId, string Kind, string Key)> removed;
        HashSet<int> rebuiltServers;
        Dictionary<int, QueryTagMapCursor> nextCursors;
        int keysSeen;
        bool rebuild;
        using (var readConnection = await OpenConnectionAsync())
        {
            using (var state = readConnection.CreateCommand())
            {
                state.CommandText = "SELECT rules_version, built_version FROM query_tag_state WHERE id = 1";
                using var reader = await state.ExecuteReaderAsync();
                if (await reader.ReadAsync())
                {
                    rulesVersion = reader.GetInt64(0);
                    builtVersion = reader.GetInt64(1);
                }
            }

            rebuild = rulesVersion > builtVersion;

            /* #9308: a server with no in-memory cursor (a fresh process) extends from the cursor the last process stored when
               the map is current, instead of rebuilding. Nothing is stored for a caller that passes no cursors. */
            var stored = cursors is not null && !rebuild && builtVersion == rulesVersion
                ? await ReadStoredQueryTagMapCursorsAsync(readConnection, rulesVersion, clock)
                : new Dictionary<int, QueryTagMapCursor>();

            var tags = await ReadQueryTagsAsync(readConnection);
            tagIds = tags.Select(t => (long)t.Id).ToHashSet();
            var names = new TagNameList(tags.Select(t => new KeyValuePair<string, long>(t.Name, t.Id)));
            var assignments = (await GetQueryTagAssignmentsAsync()).Select(ToHandAssignment).ToList();
            var rules = (await GetQueryTagRulesAsync()).Select(ToRule).ToList();
            rebuiltServers = new HashSet<int>();
            List<(int ServerId, QueryTagKeyKind Kind, string Key, QueryTagSighting Sighting)> sightings;
            (sightings, nextCursors) = await ReadQuerySightingsAsync(readConnection, cursors, stored, keyCap, rebuild, clock, rebuiltServers, serviceAge, ageBackoffUntil, pick);

            resolved = new List<(int ServerId, QueryTagKeyKind Kind, string Key, QueryTagResolution Result)>();
            removed = new List<(int ServerId, string Kind, string Key)>();
            var byKey = sightings.GroupBy(x => (x.ServerId, x.Kind, x.Key)).ToList();
            keysSeen = byKey.Count;
            foreach (var group in byKey)
            {
                var (serverId, kind, key) = group.Key;
                var result = QueryTagResolver.Resolve(serverId, group.Select(g => g.Sighting).ToList(), assignments, rules, names);
                if (result.HasTag && tagIds.Contains(result.TagId!.Value))
                {
                    resolved.Add((serverId, kind, key, result));
                }
                else
                {
                    removed.Add((serverId, KeyKindText(kind), key));
                }
            }
        }

        AfterQueryTagMapReadForTests?.Invoke();

        using var connection = await OpenWriteConnectionAsync();
        using (var begin = connection.CreateCommand())
        {
            begin.CommandText = "BEGIN TRANSACTION";
            await begin.ExecuteNonQueryAsync();
        }

        try
        {
            if (rebuild)
            {
                using var clear = connection.CreateCommand();
                clear.CommandText = "DELETE FROM query_tag_map";
                await clear.ExecuteNonQueryAsync();
            }
            else
            {
                foreach (var serverId in rebuiltServers)
                {
                    using var clearServer = connection.CreateCommand();
                    clearServer.CommandText = "DELETE FROM query_tag_map WHERE server_id = $1";
                    clearServer.Parameters.Add(new DuckDBParameter { Value = serverId });
                    await clearServer.ExecuteNonQueryAsync();
                }

                await DeleteMapRowsAsync(connection, removed);
            }

            await UpsertMapRowsAsync(connection, resolved);

            /* #9308: the cursors commit in the transaction of the map write they describe, so a stored cursor is never ahead of the
               map. A rebuild-all drops every stored cursor first, which also drops the ones of servers with no sightings left. */
            if (cursors is not null)
            {
                await StoreQueryTagMapCursorsAsync(connection, nextCursors, rulesVersion, rebuild);
            }

            if (rebuild)
            {
                using var built = connection.CreateCommand();
                built.CommandText = @"
INSERT INTO query_tag_state (id, rules_version, built_version, updated_at)
VALUES (1, $1, $1, $2)
ON CONFLICT (id) DO UPDATE SET built_version = excluded.built_version";
                built.Parameters.Add(new DuckDBParameter { Value = rulesVersion });
                built.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
                await built.ExecuteNonQueryAsync();
            }

            using var commit = connection.CreateCommand();
            commit.CommandText = "COMMIT";
            await commit.ExecuteNonQueryAsync();
        }
        catch
        {
            try
            {
                using var rollback = connection.CreateCommand();
                rollback.CommandText = "ROLLBACK";
                await rollback.ExecuteNonQueryAsync();
            }
            catch (Exception)
            {
                /* the original failure is the one to report */
            }

            throw;
        }

        if (cursors is not null)
        {
            foreach (var (serverId, cursor) in nextCursors)
            {
                cursors[serverId] = cursor;
            }
        }

        if (ageBackoffUntil is not null)
        {
            foreach (var serverId in rebuiltServers)
            {
                ageBackoffUntil.Remove(serverId);
            }
        }

        return new QueryTagMapPass(rebuild, keysSeen, resolved.Count);
    }

    /// <summary>
    /// Reads the sightings of the keys a pass resolves, one server at a time. A rebuild reads every key. An extend reads up to
    /// <paramref name="cap"/> keys per server, oldest first by (last_seen, key_kind, key), strictly after the server's cursor, and
    /// returns the cursor for that server's next pass: the last key read when the page was full, otherwise the pass clock
    /// minus <see cref="QueryTagMapCursor.Overlap"/>.
    /// </summary>
    private static async Task<(List<(int ServerId, QueryTagKeyKind Kind, string Key, QueryTagSighting Sighting)> Rows, Dictionary<int, QueryTagMapCursor> Next)>
        ReadQuerySightingsAsync(
            LockedConnection connection, IDictionary<int, QueryTagMapCursor>? cursors, Dictionary<int, QueryTagMapCursor> stored,
            int cap, bool rebuild, DateTime clock, HashSet<int> rebuiltServers, TimeSpan? serviceAge,
            IDictionary<int, DateTime>? ageBackoffUntil, AgePick pick)
    {
        var serverIds = new List<int>();
        using (var servers = connection.CreateCommand())
        {
            servers.CommandText = "SELECT DISTINCT server_id FROM query_tag_sightings ORDER BY server_id";
            using var serverReader = await servers.ExecuteReaderAsync();
            while (await serverReader.ReadAsync())
            {
                serverIds.Add(serverReader.GetInt32(0));
            }
        }

        /* #9308: the one server the weekly staggered rebuild takes this pass: the oldest rebuilt_at among those at least
           MaxRebuildAge old. A server with no cursor rebuilds anyway and does not use the slot. */
        int? staggered = null;
        if (!rebuild && cursors is not null && serviceAge is { } age && age >= QueryTagMapCursor.RebuildStartupGrace)
        {
            var oldest = DateTime.MaxValue;
            foreach (var candidate in serverIds)
            {
                if (ageBackoffUntil is not null && ageBackoffUntil.TryGetValue(candidate, out var until) && until > clock)
                {
                    continue;
                }

                if ((cursors.TryGetValue(candidate, out var held) || stored.TryGetValue(candidate, out held))
                    && clock - held.RebuiltAt >= QueryTagMapCursor.MaxRebuildAge && held.RebuiltAt < oldest)
                {
                    staggered = candidate;
                    oldest = held.RebuiltAt;
                }
            }

            pick.Server = staggered;
        }

        var rows = new List<(int, QueryTagKeyKind, string, QueryTagSighting)>();
        var next = new Dictionary<int, QueryTagMapCursor>();
        foreach (var serverId in serverIds)
        {
            /* A server with no cursor (a fresh process, a newly listed server) gets ONE full rebuild, not an epoch drain
               paged by the cap; its cursor is then the pass clock minus the overlap. */
            var cursor = QueryTagMapCursor.Epoch;
            var serverRebuild = rebuild || cursors is null || !(cursors.TryGetValue(serverId, out cursor!) || stored.TryGetValue(serverId, out cursor!));
            var rebuiltAt = serverRebuild ? default(DateTime) : cursor.RebuiltAt;
            if (serverRebuild || serverId == staggered)
            {
                serverRebuild = true;
                rebuiltServers.Add(serverId);
                cursor = QueryTagMapCursor.Epoch;
                rebuiltAt = clock;
            }

            var serverCap = serverRebuild ? int.MaxValue : cap;
            using var command = connection.CreateCommand();
            command.CommandText = @"
SELECT s.key_kind, s.key, s.database_name, s.object_name, s.application_name, s.login_name, s.host_name, s.text_sample, c.ls
FROM query_tag_sightings s
JOIN (
    SELECT key_kind, key, ls FROM (
        SELECT key_kind, key, MAX(last_seen) AS ls FROM query_tag_sightings WHERE server_id = $1 GROUP BY key_kind, key)
    WHERE (ls, key_kind, key) > ($2, $3, $4)
    ORDER BY ls, key_kind, key
    LIMIT $5) c ON s.key_kind = c.key_kind AND s.key = c.key
WHERE s.server_id = $1
ORDER BY c.ls, c.key_kind, c.key, s.database_name, s.application_name, s.login_name, s.host_name, s.object_name, s.text_sample";
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = cursor.Since });
            command.Parameters.Add(new DuckDBParameter { Value = cursor.AfterKind });
            command.Parameters.Add(new DuckDBParameter { Value = cursor.AfterKey });
            command.Parameters.Add(new DuckDBParameter { Value = serverCap });

            var keys = 0;
            string? lastKind = null;
            string? lastKey = null;
            var lastSeen = DateTime.MinValue;
            using (var reader = await command.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    string? Text(int i) => reader.IsDBNull(i) || reader.GetString(i).Length == 0 ? null : reader.GetString(i);
                    var kindText = reader.GetString(0);
                    var kind = kindText switch
                    {
                        "hash" => QueryTagKeyKind.Hash,
                        "object" => QueryTagKeyKind.Object,
                        _ => QueryTagKeyKind.Caller,
                    };
                    var key = reader.GetString(1);
                    if (kindText != lastKind || key != lastKey)
                    {
                        keys++;
                        lastKind = kindText;
                        lastKey = key;
                        lastSeen = reader.GetDateTime(8);
                    }

                    rows.Add((serverId, kind, key, new QueryTagSighting(
                        kind, key, Text(2), ParseObject(Text(3)), Text(4), Text(5), Text(6), Text(7))));
                }
            }

            next[serverId] = !serverRebuild && keys >= cap
                ? new QueryTagMapCursor(lastSeen, lastKind!, lastKey!, rebuiltAt)
                : new QueryTagMapCursor(clock - QueryTagMapCursor.Overlap, string.Empty, string.Empty, rebuiltAt);
        }

        return (rows, next);
    }

    /// <summary>
    /// #9308: the stored cursors a fresh process may extend from. A row counts when it was built at the current
    /// <paramref name="rulesVersion"/> and is under <see cref="QueryTagMapCursor.MaxStoredAge"/> old.
    /// </summary>
    private static async Task<Dictionary<int, QueryTagMapCursor>> ReadStoredQueryTagMapCursorsAsync(LockedConnection connection, long rulesVersion, DateTime clock)
    {
        var usable = new Dictionary<int, QueryTagMapCursor>();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT server_id, state_value FROM collector_state WHERE collector_name = 'query_tag_map' AND state_key = 'cursor'";
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (QueryTagMapCursor.TryDecode(reader.GetString(1)) is { } decoded
                && decoded.RulesVersion == rulesVersion
                && clock - decoded.Cursor.Since < QueryTagMapCursor.MaxStoredAge)
            {
                usable[reader.GetInt32(0)] = decoded.Cursor;
            }
        }

        return usable;
    }

    private static async Task StoreQueryTagMapCursorsAsync(LockedConnection connection, Dictionary<int, QueryTagMapCursor> next, long rulesVersion, bool rebuild)
    {
        if (rebuild)
        {
            using var clear = connection.CreateCommand();
            clear.CommandText = "DELETE FROM collector_state WHERE collector_name = 'query_tag_map' AND state_key = 'cursor'";
            await clear.ExecuteNonQueryAsync();
        }

        foreach (var (serverId, cursor) in next)
        {
            using var save = connection.CreateCommand();
            save.CommandText = @"
INSERT INTO collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1, 'query_tag_map', 'cursor', $2, $3)
ON CONFLICT (server_id, collector_name, state_key) DO UPDATE SET state_value = excluded.state_value, updated_at = excluded.updated_at";
            save.Parameters.Add(new DuckDBParameter { Value = serverId });
            save.Parameters.Add(new DuckDBParameter { Value = cursor.Encode(rulesVersion) });
            save.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
            await save.ExecuteNonQueryAsync();
        }
    }

    /// <summary>Splits <c>database.schema.name</c> (<see cref="QueryTagObject.Qualified"/>) back into an object. The name is
    /// everything after the second dot, so a dot inside a name survives; a dot inside a database or schema name does not.</summary>
    private static QueryTagObject? ParseObject(string? qualified)
    {
        if (string.IsNullOrEmpty(qualified))
        {
            return null;
        }

        var first = qualified.IndexOf('.', StringComparison.Ordinal);
        var second = first < 0 ? -1 : qualified.IndexOf('.', first + 1);
        if (second < 0)
        {
            return new QueryTagObject(null, null, qualified);
        }

        static string? Part(string s) => s.Length == 0 ? null : s;
        return new QueryTagObject(Part(qualified[..first]), Part(qualified[(first + 1)..second]), Part(qualified[(second + 1)..]));
    }

    private static QueryTagHandAssignment ToHandAssignment(QueryTagAssignmentRow a) =>
        a.Kind == "hash"
            ? new QueryTagHandAssignment(a.TagId, QueryTagKeyKind.Hash, QueryHashIdentity.CanonicalKey(a.QueryHash) ?? a.QueryHash, null, a.ServerId)
            : new QueryTagHandAssignment(a.TagId, QueryTagKeyKind.Object, null,
                new QueryTagObject(a.DatabaseName, a.SchemaName, a.ObjectName), a.ServerId);

    private static QueryTagRule ToRule(QueryTagRuleRow r) =>
        new(r.Id, r.MatchField switch
        {
            "application" => QueryTagRuleField.Application,
            "login" => QueryTagRuleField.Login,
            "host" => QueryTagRuleField.Host,
            "database" => QueryTagRuleField.Database,
            "object" => QueryTagRuleField.Object,
            "text" => QueryTagRuleField.Text,
            _ => QueryTagRuleField.TextTag,
        }, r.Pattern, r.Priority, r.Enabled, r.TagId, r.ServerId);

    private static string KeyKindText(QueryTagKeyKind kind) => kind switch
    {
        QueryTagKeyKind.Hash => "hash",
        QueryTagKeyKind.Object => "object",
        _ => "caller",
    };

    private static async Task DeleteMapRowsAsync(LockedConnection connection, List<(int ServerId, string Kind, string Key)> keys)
    {
        if (keys.Count == 0)
        {
            return;
        }

        using var command = connection.CreateCommand();
        command.CommandText = @"
DELETE FROM query_tag_map
WHERE (server_id, key_kind, key) IN (SELECT UNNEST($1), UNNEST($2), UNNEST($3))";
        command.Parameters.Add(new DuckDBParameter { Value = keys.Select(k => k.ServerId).ToList() });
        command.Parameters.Add(new DuckDBParameter { Value = keys.Select(k => k.Kind).ToList() });
        command.Parameters.Add(new DuckDBParameter { Value = keys.Select(k => k.Key).ToList() });
        await command.ExecuteNonQueryAsync();
    }

    private static async Task UpsertMapRowsAsync(
        LockedConnection connection, List<(int ServerId, QueryTagKeyKind Kind, string Key, QueryTagResolution Result)> rows)
    {
        if (rows.Count == 0)
        {
            return;
        }

        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO query_tag_map (server_id, key_kind, key, tag_id, source, rule_id, matched_field, matched_value, built_at)
SELECT s, k, c_key, t, src, NULLIF(r, 0), NULLIF(f, ''), NULLIF(v, ''), $9
FROM (SELECT UNNEST($1) AS s, UNNEST($2) AS k, UNNEST($3) AS c_key, UNNEST($4) AS t, UNNEST($5) AS src,
             UNNEST($6) AS r, UNNEST($7) AS f, UNNEST($8) AS v)
ON CONFLICT (server_id, key_kind, key) DO UPDATE SET
    tag_id = excluded.tag_id, source = excluded.source, rule_id = excluded.rule_id,
    matched_field = excluded.matched_field, matched_value = excluded.matched_value, built_at = excluded.built_at";
        command.Parameters.Add(new DuckDBParameter { Value = rows.Select(r => r.ServerId).ToList() });
        command.Parameters.Add(new DuckDBParameter { Value = rows.Select(r => KeyKindText(r.Kind)).ToList() });
        command.Parameters.Add(new DuckDBParameter { Value = rows.Select(r => r.Key).ToList() });
        command.Parameters.Add(new DuckDBParameter { Value = rows.Select(r => (int)r.Result.TagId!.Value).ToList() });
        command.Parameters.Add(new DuckDBParameter
        {
            Value = rows.Select(r => r.Result.Source switch
            {
                QueryTagSource.HandHash => "hand_hash",
                QueryTagSource.HandObject => "hand_object",
                _ => "rule",
            }).ToList(),
        });
        command.Parameters.Add(new DuckDBParameter { Value = rows.Select(r => (int)(r.Result.RuleId ?? 0)).ToList() });
        command.Parameters.Add(new DuckDBParameter { Value = rows.Select(r => r.Result.MatchedField is { } f ? QueryTagFieldText(f) : string.Empty).ToList() });
        command.Parameters.Add(new DuckDBParameter { Value = rows.Select(r => r.Result.MatchedValue ?? string.Empty).ToList() });
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>The tag names as a list of pairs, so two tags with the same name under different parents both reach the
    /// resolver, which then treats the name as ambiguous. A <see cref="Dictionary{TKey,TValue}"/> would drop one.</summary>
    private sealed class TagNameList(IEnumerable<KeyValuePair<string, long>> pairs) : IReadOnlyDictionary<string, long>
    {
        private readonly List<KeyValuePair<string, long>> _pairs = pairs.ToList();

        public long this[string key] => _pairs.First(p => string.Equals(p.Key, key, StringComparison.OrdinalIgnoreCase)).Value;
        public IEnumerable<string> Keys => _pairs.Select(p => p.Key);
        public IEnumerable<long> Values => _pairs.Select(p => p.Value);
        public int Count => _pairs.Count;
        public bool ContainsKey(string key) => _pairs.Any(p => string.Equals(p.Key, key, StringComparison.OrdinalIgnoreCase));

        public bool TryGetValue(string key, out long value)
        {
            foreach (var p in _pairs)
            {
                if (string.Equals(p.Key, key, StringComparison.OrdinalIgnoreCase))
                {
                    value = p.Value;
                    return true;
                }
            }

            value = 0;
            return false;
        }

        public IEnumerator<KeyValuePair<string, long>> GetEnumerator() => _pairs.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
