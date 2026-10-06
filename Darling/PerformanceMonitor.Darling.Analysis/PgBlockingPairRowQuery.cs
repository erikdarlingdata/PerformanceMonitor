/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// Shared pieces of the blocked-process-report pair-row query used to reconstruct blocking chains —
/// Lite's <c>BlockingPairRowQuery</c> ported onto Npgsql, byte-identical SQL fragments and reader
/// mapping. In Lite three consumers share it (drill-down collector, BLOCKING_CHAIN fact collector,
/// viewer fetch); in Darling the fact collector is the first, and the future drill-down/viewer
/// slices reuse it so the apex and the column order can't drift here either.
///
/// <para><see cref="SpidFilter"/> is the behavioral fix carried over: Lite maps a missing blocker to
/// spid 0 (a phantom root); without filtering it out, every consumer would invent a SPID-0 apex.</para>
///
/// <para><see cref="LeadingColumns"/> is the single source of truth for ordinals 0-8 so the queries
/// can't drift and silently feed <see cref="Read"/> mismapped columns. Only the two SQL-text
/// expressions (ordinals 9-10) stay per-site — the fact collector selects the full text.</para>
///
/// <para>/* PG port: Lite's command factory is <c>Func&lt;DuckDBCommand&gt;</c> and its parameters are
/// <c>DuckDBParameter</c>; here they are the Npgsql equivalents, DateTimes bound naive-UTC
/// Kind-Unspecified (the PgCollectorRowWriter discipline). The SQL text is unchanged. */</para>
/// </summary>
internal static class PgBlockingPairRowQuery
{
    /// <summary>Ordinals 0-8 of the pair-row SELECT. Each query appends its own SQL-text expressions (9-10).</summary>
    public const string LeadingColumns = @"event_time,
    database_name,
    blocked_spid,
    blocked_last_tran_started,
    blocking_spid,
    blocking_last_tran_started,
    wait_time_ms,
    lock_mode,
    blocking_status";

    /// <summary>Ordinals 11-16: session identity for each side. Appended after the per-site SQL-text
    /// expressions so all consumers share one column order for <see cref="Read"/>. The store
    /// denormalizes both sides (Lite's shape), so the apex's identity is available too.</summary>
    public const string IdentityColumns = @"blocked_login_name,
    blocked_host_name,
    blocked_client_app,
    blocking_login_name,
    blocking_host_name,
    blocking_client_app";

    /// <summary>Ordinals 18-20: spid:ecid + monitor_loop, the reconstruction's session identity. Every
    /// pair-row site appends this after contentious_object (ordinal 17) so the shared <see cref="Read"/> sees
    /// one column order across all call sites.</summary>
    public const string TrailingIdentityColumns = @"blocked_ecid,
    blocking_ecid,
    monitor_loop";

    /// <summary>Append to the WHERE clause of every pair-row query (covers NULL and the 0 sentinel).</summary>
    public const string SpidFilter = @"AND blocking_spid IS NOT NULL
AND blocking_spid <> 0";

    /// <summary>The DMV-snapshot fallback fetch (see <see cref="AppendDmvSnapshotRowsAsync"/>) —
    /// Lite's query text verbatim, exposed const for the ungated dialect pins.</summary>
    public const string DmvSnapshotSql = $@"
SELECT
    {LeadingColumns},
    blocked_sql_text, blocking_sql_text,
    {IdentityColumns},
    contentious_object,
    {TrailingIdentityColumns}{DmvSnapshotTail}";

    /// <summary>The same fetch with an empty string where each statement text is (#5361): the drill-down reads the
    /// pair-rows without the text, reconstructs, and then reads the whole text of only the levels it shows
    /// (<see cref="DmvChainLevelTextSql"/>). Every other column and the column order are <see cref="DmvSnapshotSql"/>'s,
    /// so <see cref="Read"/> maps it unchanged.</summary>
    public const string DmvSnapshotTextFreeSql = $@"
SELECT
    {LeadingColumns},
    ''::text AS blocked_sql_text, ''::text AS blocking_sql_text,
    {IdentityColumns},
    contentious_object,
    {TrailingIdentityColumns}{DmvSnapshotTail}";

    private const string DmvSnapshotTail = $@"
FROM v_dmv_blocking_snapshots
WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3
{SpidFilter}
ORDER BY event_time DESC
LIMIT 5000";

    /* #5361: the whole statement text of the pair-rows a chain's levels were built from, by event key. $1 is the
       server and $2 to $6 are the keys' event times and spid:ecid pairs as five parallel arrays; the blocked-process
       report read also takes the collection_time floor the pair-row read used, as $7, so it prunes the same chunks.
       A missing spid or ecid reads as 0 on both sides, exactly as Read maps it (blocking_spid is never NULL: the
       pair-row read filters it). Rows of one key are the same event; when more than one is stored, the one with the
       longest wait is read, which is the row the reconstruction keeps for an edge. */
    private const string ChainLevelTextHead = @"
SELECT DISTINCT ON (v.event_time, COALESCE(v.blocked_spid, 0), COALESCE(v.blocked_ecid, 0), v.blocking_spid, COALESCE(v.blocking_ecid, 0))
    v.event_time,
    COALESCE(v.blocked_spid, 0) AS blocked_spid,
    COALESCE(v.blocked_ecid, 0) AS blocked_ecid,
    v.blocking_spid,
    COALESCE(v.blocking_ecid, 0) AS blocking_ecid,
    v.blocked_sql_text,
    v.blocking_sql_text
FROM ";

    private const string ChainLevelTextJoin = @" AS v
JOIN unnest($2::timestamp[], $3::int[], $4::int[], $5::int[], $6::int[])
    AS k(event_time, blocked_spid, blocked_ecid, blocking_spid, blocking_ecid)
  ON  v.event_time = k.event_time
  AND v.blocking_spid = k.blocking_spid
  AND COALESCE(v.blocked_spid, 0) = k.blocked_spid
  AND COALESCE(v.blocked_ecid, 0) = k.blocked_ecid
  AND COALESCE(v.blocking_ecid, 0) = k.blocking_ecid
WHERE v.server_id = $1";

    private const string ChainLevelTextTail = @"
ORDER BY v.event_time, COALESCE(v.blocked_spid, 0), COALESCE(v.blocked_ecid, 0), v.blocking_spid, COALESCE(v.blocking_ecid, 0),
    v.wait_time_ms DESC NULLS LAST";

    /// <summary>The whole statement text of the blocked-process-report pair-rows behind a chain's levels (#5361).</summary>
    public const string BprChainLevelTextSql = $@"{ChainLevelTextHead}v_blocked_process_reports{ChainLevelTextJoin}
AND   v.collection_time >= $7{ChainLevelTextTail}";

    /// <summary>The whole statement text of the DMV-snapshot pair-rows behind a chain's levels (#5361).</summary>
    public const string DmvChainLevelTextSql = $@"{ChainLevelTextHead}v_dmv_blocking_snapshots{ChainLevelTextJoin}{ChainLevelTextTail}";

    public static BlockingPairRow Read(DbDataReader reader) => new()
    {
        EventTime = reader.IsDBNull(0) ? default : reader.GetDateTime(0),
        DatabaseName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
        BlockedSpid = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
        BlockedTranStarted = reader.IsDBNull(3) ? null : reader.GetDateTime(3),
        BlockingSpid = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
        BlockingTranStarted = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
        WaitTimeMs = reader.IsDBNull(6) ? 0L : ToInt64(reader.GetValue(6)),
        LockMode = reader.IsDBNull(7) ? string.Empty : reader.GetString(7),
        BlockingStatus = reader.IsDBNull(8) ? string.Empty : reader.GetString(8),
        BlockedSqlText = reader.IsDBNull(9) ? string.Empty : reader.GetString(9),
        BlockingSqlText = reader.IsDBNull(10) ? string.Empty : reader.GetString(10),
        BlockedLoginName = reader.IsDBNull(11) ? string.Empty : reader.GetString(11),
        BlockedHostName = reader.IsDBNull(12) ? string.Empty : reader.GetString(12),
        BlockedClientApp = reader.IsDBNull(13) ? string.Empty : reader.GetString(13),
        BlockingLoginName = reader.IsDBNull(14) ? string.Empty : reader.GetString(14),
        BlockingHostName = reader.IsDBNull(15) ? string.Empty : reader.GetString(15),
        BlockingClientApp = reader.IsDBNull(16) ? string.Empty : reader.GetString(16),
        // Ordinal 17: the contended object (every pair-row site appends contentious_object after IdentityColumns).
        ContentiousObject = reader.IsDBNull(17) ? string.Empty : reader.GetString(17),
        // 18-20: spid:ecid + monitor_loop (every site appends TrailingIdentityColumns after contentious_object).
        BlockedEcid = reader.IsDBNull(18) ? 0 : Convert.ToInt32(reader.GetValue(18)),
        BlockingEcid = reader.IsDBNull(19) ? 0 : Convert.ToInt32(reader.GetValue(19)),
        MonitorLoop = reader.IsDBNull(20) ? (int?)null : Convert.ToInt32(reader.GetValue(20))
    };

    /// <summary>
    /// Long conversion for aggregate reads — the single home for the idiom every Darling numeric
    /// reader uses (PgFactCollector delegates here), mirroring Lite's
    /// <c>BlockingPairRowQuery.ToInt64</c>. /* PG port: Lite's BigInteger branch is dropped —
    /// DuckDB boxes wide aggregates as System.Numerics.BigInteger (not IConvertible); Npgsql
    /// never does. Postgres returns SUM(bigint)/AVG-style aggregates as numeric, which arrives
    /// as decimal — Convert.ToInt64 handles that (and every integral type) directly. */
    /// </summary>
    public static long ToInt64(object value) => Convert.ToInt64(value);

    /// <summary>
    /// Fetches DMV-snapshot pair-rows (the always-on blocking fallback) for the window and merges them into
    /// <paramref name="rows"/> (BPR-preferred — see <see cref="BlockingPairRowMerge"/>). Uses the same column
    /// fragments as the blocked-process-report queries, against v_dmv_blocking_snapshots, so <see cref="Read"/>
    /// maps it unchanged. Takes a command factory so the caller's connection runs it — the Lite shape, kept so
    /// the future drill-down/viewer slices call it identically.
    ///
    /// <para>#2443: the token is required, not defaulted. Three callers share this fetch and two of
    /// them are on the analysis pass; a default would have let either keep passing nothing while the
    /// signature claimed the read was abandonable. The viewer's call is the one that legitimately has
    /// no pass to abandon, and it says so at its own call site rather than here.</para>
    ///
    /// <para><b>#2874: <paramref name="createCommand"/> MUST return a command that already carries a
    /// <c>CommandTimeout</c>.</b> This method sets only <c>CommandText</c>, so whatever the factory
    /// hands back is the command that runs - and all three callers originally passed a bare
    /// <c>connection.CreateCommand</c> method group, which inherits Npgsql's undocumented 30 s
    /// default. The deadline is deliberately NOT set here and NOT taken as a parameter: the three
    /// callers sit in three different budget regimes (60 s under the analysis pass, 30 s under the
    /// drill-down, the viewer's interactive read deadline), so there is no one value to apply, and a
    /// "floor if unset" cannot work because Npgsql's default IS 30 and the drill-down's deliberate
    /// choice is also 30 - indistinguishable from here. Keeping it at the call sites also keeps the
    /// deadline where the assembly's own pin can see it: the <c>createCommand()</c> invocation below
    /// matches neither census regex, so a deadline set here would be the one line in this pass that
    /// nothing guards.</para>
    /// </summary>
    internal static async Task AppendDmvSnapshotRowsAsync(
        Func<NpgsqlCommand> createCommand, List<BlockingPairRow> rows, int serverId, DateTime start, DateTime end,
        CancellationToken cancellationToken, bool includeText = true)
    {
        var dmv = new List<BlockingPairRow>();
        using (var cmd = createCommand())
        {
            /* #5361: includeText false is the drill-down's text-free read (empty text, fetched afterwards for the levels it
               shows); the fact collector and the viewer keep the whole text. */
            cmd.CommandText = includeText ? DmvSnapshotSql : DmvSnapshotTextFreeSql;
            cmd.Parameters.AddWithValue(serverId);
            cmd.Parameters.AddWithValue(DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
            cmd.Parameters.AddWithValue(DateTime.SpecifyKind(end, DateTimeKind.Unspecified));

            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                dmv.Add(Read(reader));
        }

        BlockingPairRowMerge.MergeInto(rows, dmv);
    }

    /// <summary>
    /// Reads the whole statement text of the pair-rows behind the chain levels a drill-down shows (#5361), by event key,
    /// from one source: <paramref name="sql"/> is <see cref="BprChainLevelTextSql"/> (pass the pair-row read's
    /// <paramref name="collectedFrom"/> floor) or <see cref="DmvChainLevelTextSql"/> (pass null). Returns each key's
    /// blocked and blocking text, NULL read as empty text as <see cref="Read"/> does; a key whose row is gone is absent.
    /// One round trip however many levels print: the keys travel as five parallel arrays.
    /// </summary>
    internal static async Task<Dictionary<PairRowKey, (string BlockedSql, string BlockingSql)>> ReadChainLevelTextAsync(
        NpgsqlConnection connection, string sql, int serverId, IReadOnlyCollection<PairRowKey> keys,
        DateTime? collectedFrom, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var text = new Dictionary<PairRowKey, (string BlockedSql, string BlockingSql)>();
        if (keys.Count == 0)
            return text;

        using var cmd = new NpgsqlCommand(sql, connection) { CommandTimeout = commandTimeoutSeconds };
        cmd.Parameters.AddWithValue(serverId);
        cmd.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp,
            Value = keys.Select(k => DateTime.SpecifyKind(k.EventTime, DateTimeKind.Unspecified)).ToArray()
        });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = keys.Select(k => k.BlockedSpid).ToArray() });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = keys.Select(k => k.BlockedEcid).ToArray() });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = keys.Select(k => k.BlockingSpid).ToArray() });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = keys.Select(k => k.BlockingEcid).ToArray() });
        if (collectedFrom is { } floor)
            cmd.Parameters.AddWithValue(DateTime.SpecifyKind(floor, DateTimeKind.Unspecified));

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var key = new PairRowKey(
                reader.GetDateTime(0), Convert.ToInt32(reader.GetValue(1)), Convert.ToInt32(reader.GetValue(2)),
                Convert.ToInt32(reader.GetValue(3)), Convert.ToInt32(reader.GetValue(4)));
            text[key] = (reader.IsDBNull(5) ? string.Empty : reader.GetString(5), reader.IsDBNull(6) ? string.Empty : reader.GetString(6));
        }

        return text;
    }
}
