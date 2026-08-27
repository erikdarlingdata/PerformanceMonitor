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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
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
    {TrailingIdentityColumns}
FROM v_dmv_blocking_snapshots
WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3
{SpidFilter}
ORDER BY event_time DESC
LIMIT 5000";

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
    /// </summary>
    internal static async Task AppendDmvSnapshotRowsAsync(
        Func<NpgsqlCommand> createCommand, List<BlockingPairRow> rows, int serverId, DateTime start, DateTime end,
        CancellationToken cancellationToken)
    {
        var dmv = new List<BlockingPairRow>();
        using (var cmd = createCommand())
        {
            cmd.CommandText = DmvSnapshotSql;
            cmd.Parameters.AddWithValue(serverId);
            cmd.Parameters.AddWithValue(DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
            cmd.Parameters.AddWithValue(DateTime.SpecifyKind(end, DateTimeKind.Unspecified));

            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                dmv.Add(Read(reader));
        }

        BlockingPairRowMerge.MergeInto(rows, dmv);
    }
}
