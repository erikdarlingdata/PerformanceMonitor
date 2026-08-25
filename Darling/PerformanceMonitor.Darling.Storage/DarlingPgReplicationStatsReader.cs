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
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Reads the stored replication connections (<c>pg_replication_stats</c>, #2544) — the LATEST sample per
/// standby, plus the WORST that standby reached across the window.
///
/// <para><b>Latest and worst together, because either alone misleads.</b> The latest row says whether the
/// standby is behind right now; the window maximum says whether it has been. A replica that drifts 400 MB
/// behind every afternoon and catches up by evening reads as perfectly healthy in any single sample, and it
/// is the one most likely to be useless at the moment somebody needs to fail over to it.</para>
///
/// <para><b>Ranked by the worst REPLAY distance, not by the latest.</b> Replay is the end of the chain: a
/// standby that has received and flushed everything but applied none of it still cannot serve a read and
/// still cannot be promoted without waiting.</para>
///
/// <para><b>Byte distance leads, time lag follows.</b> Measured against a stalled standby, the time lag
/// reported 2.8 seconds for a 33.7 MB backlog — it times the round-trip of the most recently replayed
/// record rather than sizing the backlog, so it is kept for context and never used for ranking.</para>
///
/// <para>Shared by the WPF tab and the MCP surface so there is one copy of this SQL, per #2530.</para>
/// </summary>
public static class DarlingPgReplicationStatsReader
{
    /// <param name="ReplayBytesBehind">Distance at the most recent sample.</param>
    /// <param name="WorstReplayBytesBehind">The furthest behind this standby got in the window — the column
    /// that catches a replica which recovers before anybody looks.</param>
    /// <param name="Samples">How many samples this standby appeared in. A standby present in far fewer
    /// samples than the collector took has been DISCONNECTING, which every other column would show as
    /// healthy.</param>
    public sealed record PgReplicationStatRow(
        string? ApplicationName,
        string? ClientAddr,
        string? State,
        string? SyncState,
        long? SentBytesBehind,
        long? ReplayBytesBehind,
        long? WorstReplayBytesBehind,
        double? ReplayLagMs,
        double? WorstReplayLagMs,
        long Samples,
        long TotalSamples,
        DateTime? BackendStart,
        DateTime LastSeen);

    /* DISTINCT ON gives the newest row per standby; the window aggregates come from a separate grouped scan
       joined back to it, because DISTINCT ON cannot also aggregate over the rows it discards.

       The standby identity is (application_name, client_addr) rather than application_name alone: two
       replicas commonly report the same default application_name ('walreceiver' is what an unconfigured one
       sends - observed), and collapsing them would average two servers into one row. */
    public const string PgReplicationStatsSql = """
        WITH bounded AS (
            SELECT *
            FROM pg_replication_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
        ),
        totals AS (
            SELECT count(DISTINCT collection_time) AS total FROM bounded
        ),
        worst AS (
            SELECT application_name, client_addr,
                   max(replay_bytes_behind)          AS worst_replay_bytes,
                   max(replay_lag_ms)                AS worst_replay_lag_ms,
                   count(DISTINCT collection_time)   AS samples,
                   max(collection_time)              AS last_seen
            FROM bounded
            GROUP BY application_name, client_addr
        ),
        latest AS (
            SELECT DISTINCT ON (application_name, client_addr)
                   application_name, client_addr, state, sync_state,
                   sent_bytes_behind, replay_bytes_behind, replay_lag_ms, backend_start
            FROM bounded
            ORDER BY application_name, client_addr, collection_time DESC
        )
        SELECT
            l.application_name, l.client_addr, l.state, l.sync_state,
            l.sent_bytes_behind, l.replay_bytes_behind,
            w.worst_replay_bytes, l.replay_lag_ms, w.worst_replay_lag_ms,
            w.samples, t.total, l.backend_start, w.last_seen
        FROM latest AS l
        JOIN worst AS w
          ON  w.application_name IS NOT DISTINCT FROM l.application_name
          AND w.client_addr      IS NOT DISTINCT FROM l.client_addr
        CROSS JOIN totals AS t
        /* Worst first, and by BYTES - the time lag understates a stall badly enough that ranking on it would
           put the wrong standby at the top. IS NOT DISTINCT FROM above, not =, because client_addr is NULL
           for a standby on a Unix socket and an equality join would silently drop it. */
        ORDER BY w.worst_replay_bytes DESC NULLS LAST, l.application_name
        LIMIT $4
        """;

    public static async Task<List<PgReplicationStatRow>> GetPgReplicationStatsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgReplicationStatRow>();
        await using var command = postgres.CreateCommand(PgReplicationStatsSql);
        command.Parameters.AddWithValue(serverId);
        /* SpecifyKind(Unspecified) at the BIND, same convention as every other PostgreSQL read here: Npgsql
           does not reject Kind=Utc, it infers timestamptz, and PostgreSQL then resolves the comparison
           against these NAIVE timestamp columns at the store session's TimeZone — so east of UTC the window
           slides off the data and the read returns nothing at all. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgReplicationStatRow(
                ApplicationName: reader.IsDBNull(0) ? null : reader.GetString(0),
                ClientAddr: reader.IsDBNull(1) ? null : reader.GetString(1),
                State: reader.IsDBNull(2) ? null : reader.GetString(2),
                SyncState: reader.IsDBNull(3) ? null : reader.GetString(3),
                SentBytesBehind: reader.IsDBNull(4) ? null : reader.GetInt64(4),
                ReplayBytesBehind: reader.IsDBNull(5) ? null : reader.GetInt64(5),
                WorstReplayBytesBehind: reader.IsDBNull(6) ? null : reader.GetInt64(6),
                ReplayLagMs: reader.IsDBNull(7) ? null : reader.GetDouble(7),
                WorstReplayLagMs: reader.IsDBNull(8) ? null : reader.GetDouble(8),
                Samples: reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                TotalSamples: reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                BackendStart: reader.IsDBNull(11) ? null : DateTime.SpecifyKind(reader.GetDateTime(11), DateTimeKind.Utc),
                LastSeen: reader.IsDBNull(12)
                    ? default
                    : DateTime.SpecifyKind(reader.GetDateTime(12), DateTimeKind.Utc)));
        }

        return rows;
    }
}
