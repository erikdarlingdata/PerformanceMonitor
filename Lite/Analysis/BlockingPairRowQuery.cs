using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// Shared pieces of the blocked-process-report pair-row query used to reconstruct blocking chains, so
/// Lite's three consumers — the drill-down collector, the BLOCKING_CHAIN fact collector, and the viewer's
/// data-service fetch — agree on the apex AND on the column order the shared <see cref="Read"/> depends on.
///
/// <para><see cref="SpidFilter"/> is the behavioral fix: Lite maps a missing blocker to spid 0 (a phantom
/// root); without filtering it out, the fact/drill-down/viewer would each invent a SPID-0 apex. This brings
/// Lite in line with Dashboard's long-standing <c>blocking_spid IS NOT NULL</c>.</para>
///
/// <para><see cref="LeadingColumns"/> is the single source of truth for ordinals 0-8 so the three queries
/// can't drift and silently feed <see cref="Read"/> mismapped columns. Only the two SQL-text expressions
/// (ordinals 9-10) stay per-site — drill-down truncates with <c>LEFT(...,500)</c>; the fact collector and
/// the viewer fetch select the full text.</para>
/// </summary>
internal static class BlockingPairRowQuery
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
    /// expressions so all three queries share one column order for <see cref="Read"/>. Lite denormalizes
    /// both sides, so unlike Dashboard the apex's identity is available too.</summary>
    public const string IdentityColumns = @"blocked_login_name,
    blocked_host_name,
    blocked_client_app,
    blocking_login_name,
    blocking_host_name,
    blocking_client_app";

    /// <summary>Ordinals 18-20: spid:ecid + monitor_loop, the reconstruction's session identity. Every
    /// pair-row site appends this after contentious_object (ordinal 17) so the shared <see cref="Read"/> sees
    /// one column order across all three call sites.</summary>
    public const string TrailingIdentityColumns = @"blocked_ecid,
    blocking_ecid,
    monitor_loop";

    /// <summary>Append to the WHERE clause of every pair-row query (covers NULL and the 0 sentinel).</summary>
    public const string SpidFilter = @"AND blocking_spid IS NOT NULL
AND blocking_spid <> 0";

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
    /// BigInteger-tolerant long conversion — DuckDB can hand wide / aggregate values back boxed as a
    /// <see cref="BigInteger"/>, which is not <see cref="IConvertible"/>, so plain Convert.ToInt64 throws.
    /// The single home for the idiom every Lite numeric reader uses (DuckDbFactCollector delegates here).
    /// </summary>
    public static long ToInt64(object value)
    {
        if (value is BigInteger bi)
            return (long)bi;
        return Convert.ToInt64(value);
    }

    /// <summary>
    /// Fetches DMV-snapshot pair-rows (the always-on blocking fallback) for the window and merges them into
    /// <paramref name="rows"/> (BPR-preferred — see <see cref="BlockingPairRowMerge"/>). Uses the same column
    /// fragments as the blocked-process-report queries, against v_dmv_blocking_snapshots, so <see cref="Read"/>
    /// maps it unchanged. Takes a command factory so the caller's connection (a LockedConnection in the viewer,
    /// a raw DuckDBConnection in the collectors) runs it on the read lock it already holds.
    ///
    /// <para>#2443: the token is required, not defaulted. Three callers share this fetch and two of
    /// them are on the analysis pass; a default would have let either keep passing nothing while the
    /// signature claimed the read was abandonable. The viewer's call is the one that legitimately has
    /// no pass to abandon, and it says so at its own call site rather than here.</para>
    /// </summary>
    internal static async Task AppendDmvSnapshotRowsAsync(
        Func<DuckDBCommand> createCommand, List<BlockingPairRow> rows, int serverId, DateTime start, DateTime end,
        CancellationToken cancellationToken, bool includeText = true)
    {
        var dmv = new List<BlockingPairRow>();
        using (var cmd = createCommand())
        {
            /* #5361: includeText false is the drill-down's text-free read (empty text, fetched afterwards for the
               levels it shows); the fact collector and the viewer keep the whole text. */
            var textColumns = includeText
                ? "blocked_sql_text, blocking_sql_text"
                : "''::VARCHAR AS blocked_sql_text, ''::VARCHAR AS blocking_sql_text";
            cmd.CommandText = $@"
SELECT
    {LeadingColumns},
    {textColumns},
    {IdentityColumns},
    contentious_object,
    {TrailingIdentityColumns}
FROM v_dmv_blocking_snapshots
WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3
{SpidFilter}
ORDER BY event_time DESC
LIMIT 5000";
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = start });
            cmd.Parameters.Add(new DuckDBParameter { Value = end });

            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                dmv.Add(Read(reader));
        }

        BlockingPairRowMerge.MergeInto(rows, dmv);
    }

    /// <summary>The most levels one text read asks for: each key is five parameters, and a long OR/IN list is slow to plan.</summary>
    private const int ChainLevelTextBatch = 200;

    /// <summary>
    /// Reads the whole statement text of the pair-rows behind the chain levels a drill-down shows (#5361), by event key,
    /// from one source: the blocked process reports (<paramref name="dmvSource"/> false, through
    /// <see cref="StoredEventCopies"/> like every read of that table) or the DMV snapshots. Returns each key's blocked and
    /// blocking text, NULL read as empty text as <see cref="Read"/> does; a key whose row is gone is absent. A missing
    /// spid or ecid reads as 0 on both sides, exactly as <see cref="Read"/> maps it. When more than one row of a key is
    /// stored, the one with the longest wait is read, which is the row the reconstruction keeps for an edge.
    /// </summary>
    internal static async Task<Dictionary<PairRowKey, (string BlockedSql, string BlockingSql)>> ReadChainLevelTextAsync(
        Func<DuckDBCommand> createCommand, bool dmvSource, int serverId, IReadOnlyCollection<PairRowKey> keys,
        CancellationToken cancellationToken)
    {
        var text = new Dictionary<PairRowKey, (string BlockedSql, string BlockingSql)>();
        var list = keys.ToList();
        for (var offset = 0; offset < list.Count; offset += ChainLevelTextBatch)
        {
            var batch = list.GetRange(offset, Math.Min(ChainLevelTextBatch, list.Count - offset));
            using var cmd = createCommand();
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = batch.Min(k => k.EventTime) });
            cmd.Parameters.Add(new DuckDBParameter { Value = batch.Max(k => k.EventTime) });
            var tuples = new List<string>(batch.Count);
            var next = 4;
            foreach (var key in batch)
            {
                tuples.Add($"(${next}::TIMESTAMP, ${next + 1}::INTEGER, ${next + 2}::INTEGER, ${next + 3}::INTEGER, ${next + 4}::INTEGER)");
                next += 5;
                cmd.Parameters.Add(new DuckDBParameter { Value = key.EventTime });
                cmd.Parameters.Add(new DuckDBParameter { Value = key.BlockedSpid });
                cmd.Parameters.Add(new DuckDBParameter { Value = key.BlockedEcid });
                cmd.Parameters.Add(new DuckDBParameter { Value = key.BlockingSpid });
                cmd.Parameters.Add(new DuckDBParameter { Value = key.BlockingEcid });
            }

            var where = "server_id = $1 AND event_time >= $2 AND event_time <= $3 AND "
                + "(event_time, COALESCE(blocked_spid, 0), COALESCE(blocked_ecid, 0), blocking_spid, COALESCE(blocking_ecid, 0)) IN ("
                + string.Join(", ", tuples) + ")";
            var source = dmvSource
                ? $"v_dmv_blocking_snapshots WHERE {where}"
                : $"{StoredEventCopies.BlockedProcessReports(where)} AS ev";
            cmd.CommandText = $@"
SELECT
    event_time,
    COALESCE(blocked_spid, 0),
    COALESCE(blocked_ecid, 0),
    blocking_spid,
    COALESCE(blocking_ecid, 0),
    blocked_sql_text,
    blocking_sql_text
FROM {source}
ORDER BY wait_time_ms DESC NULLS LAST";

            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var key = new PairRowKey(
                    reader.GetDateTime(0), Convert.ToInt32(reader.GetValue(1)), Convert.ToInt32(reader.GetValue(2)),
                    Convert.ToInt32(reader.GetValue(3)), Convert.ToInt32(reader.GetValue(4)));
                /* Longest wait first, so TryAdd keeps that row. */
                text.TryAdd(key, (reader.IsDBNull(5) ? string.Empty : reader.GetString(5),
                    reader.IsDBNull(6) ? string.Empty : reader.GetString(6)));
            }
        }

        return text;
    }
}
