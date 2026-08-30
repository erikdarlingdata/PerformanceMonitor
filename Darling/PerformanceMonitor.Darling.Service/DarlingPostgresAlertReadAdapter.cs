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
using PerformanceMonitor.Alerting;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Darling's <see cref="IPostgresAlertReadAdapter"/> — the three Tier 0 predictors read out of the store.
/// <para>Every query takes the LATEST reading per subject rather than an aggregate, because all three are
/// levels rather than rates: the question is "where does this stand now", not "how much accumulated".</para>
/// </summary>
public sealed class DarlingPostgresAlertReadAdapter : IPostgresAlertReadAdapter
{
    private readonly NpgsqlDataSource _postgres;

    /// <summary>
    /// How far back a reading may be and still count as current. Two hours covers the slowest of the three
    /// cadences (wraparound at 5 minutes) with room for a missed sweep, while still refusing to alert off a
    /// stale row after collection has stopped — an alert fired from yesterday's number is worse than none.
    /// </summary>
    internal static readonly TimeSpan Freshness = TimeSpan.FromHours(2);

    public DarlingPostgresAlertReadAdapter(NpgsqlDataSource postgres) => _postgres = postgres;

    /// <summary>
    /// Latest row per database, carrying the server's own <c>autovacuum_freeze_max_age</c> so the evaluator
    /// can scale its thresholds to this cluster's configuration instead of to a constant.
    /// <para>The two ages are kept separate rather than pre-maxed: which counter is worse decides which
    /// remedy the alert names, and MultiXact exhaustion is a different (and less familiar) problem than XID
    /// exhaustion.</para>
    /// </summary>
    internal const string WraparoundSql = """
        SELECT DISTINCT ON (database_name)
            database_name,
            frozen_xid_age,
            min_multixid_age,
            autovacuum_freeze_max_age,
            autovacuum_multixact_freeze_max_age,
            MAX(frozen_xid_age) OVER (PARTITION BY database_name) AS window_peak_xid_age,
            MAX(min_multixid_age) OVER (PARTITION BY database_name) AS window_peak_multixact_age
        FROM pg_wraparound_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        ORDER BY database_name, collection_time DESC
        """;

    /// <summary>
    /// The current winning holder, plus how persistently THAT HOLDER has won across the window.
    /// <para>Persistence is computed here rather than left to the evaluator because it needs the whole
    /// window, and one pass over the store is cheaper than shipping every row up to be counted. The
    /// observation total counts DISTINCT collection times, not rows: several sources are recorded per
    /// collection, so counting rows would inflate the denominator and make every holder look transient.</para>
    /// <para><b>Held is counted per (source, holder), not per source.</b> Counting by source alone answered a
    /// different question than the alert asks: sixty different sessions each winning once rendered as "pid X
    /// held the horizon 60/60 observations", which is the exact shape of a chronic holder and the opposite of
    /// the truth — sixty short transactions are normal, one that will not end is the incident. The alert names
    /// a specific pid or slot, so persistence has to be that thing's persistence.</para>
    /// <para>The denominator counts collections that recorded ANY holder. The collector emits no rows when the
    /// horizon is unheld, so counting only holder-bearing collections is what makes the ratio mean "of the
    /// times something held it, how often was it this one" — which is the question. Note this became reachable
    /// only once the collector stopped attributing its own backend: while Darling's own snapshot was always a
    /// session holder, every collection had a holder and the distinction was invisible.</para>
    /// </summary>
    internal const string XminSql = """
        WITH latest AS (
            SELECT source, holder, xmin_age, detail
            FROM pg_xmin_horizon
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   is_winner
            ORDER BY collection_time DESC, xmin_age DESC
            LIMIT 1
        ),
        window_stats AS (
            SELECT
                COUNT(DISTINCT collection_time) AS observations_total,
                COUNT(DISTINCT collection_time) FILTER (
                    WHERE is_winner
                    AND   source = (SELECT source FROM latest)
                    AND   holder IS NOT DISTINCT FROM (SELECT holder FROM latest)
                ) AS observations_held
            FROM pg_xmin_horizon
            WHERE server_id = $1
            AND   collection_time >= $2
        )
        SELECT
            l.source,
            l.holder,
            l.xmin_age,
            w.observations_held,
            w.observations_total,
            l.detail
        FROM latest AS l
        CROSS JOIN window_stats AS w
        """;

    /// <summary>
    /// Latest state per slot plus its earliest retained figure in the window, so the evaluator can see
    /// whether the pile is still growing — the difference between a consumer that is behind and a volume
    /// filling in front of you.
    /// </summary>
    internal const string SlotSql = """
        WITH latest AS (
            SELECT DISTINCT ON (slot_name)
                slot_name, wal_status, is_active, retained_wal_bytes, inactive_since
            FROM collect.pg_replication_slot_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            ORDER BY slot_name, collection_time DESC
        ),
        earliest AS (
            SELECT DISTINCT ON (slot_name)
                slot_name, retained_wal_bytes AS first_retained
            FROM collect.pg_replication_slot_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            ORDER BY slot_name, collection_time ASC
        )
        SELECT
            l.slot_name,
            l.wal_status,
            l.is_active,
            l.retained_wal_bytes,
            GREATEST(l.retained_wal_bytes - e.first_retained, 0) AS growth_bytes,
            l.inactive_since
        FROM latest AS l
        JOIN earliest AS e ON e.slot_name = l.slot_name
        """;

    public async Task<List<PostgresWraparoundAlertInfo>> GetWraparoundRiskAsync(
        int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<PostgresWraparoundAlertInfo>();
        await using var command = _postgres.CreateCommand(WraparoundSql);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(NaiveUtcNow() - Freshness);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PostgresWraparoundAlertInfo(
                reader.IsDBNull(0) ? "(unknown)" : reader.GetString(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                /* ordinal 4: autovacuum_multixact_freeze_max_age. The collector has stored it since V63 and
                   this adapter simply never selected it, so the evaluator graded MultiXact age against the
                   XID setting — half the size by default, hence warnings at 2.2x premature. 0 reads as
                   "cannot judge that counter", which is the correct fail-quiet. */
                reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                /* ordinals 5/6: the window's peak for each counter (#2689), read over this SAME freshness
                   window rather than a separate one, so "has it come down" and "is this reading current"
                   answer consistently off one query. 0 reads as "no window data" -> FreezingIsKeepingUp
                   false, the conservative default. */
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt64(6)));
        }

        return rows;
    }

    public async Task<PostgresXminHorizonAlertInfo?> GetXminHorizonAsync(
        int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = _postgres.CreateCommand(XminSql);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(NaiveUtcNow() - Freshness);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new PostgresXminHorizonAlertInfo(
            reader.IsDBNull(0) ? "(unknown)" : reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
            reader.IsDBNull(3) ? 0 : (int)reader.GetInt64(3),
            reader.IsDBNull(4) ? 0 : (int)reader.GetInt64(4),
            reader.IsDBNull(5) ? null : reader.GetString(5));
    }

    public async Task<List<PostgresSlotAlertInfo>> GetReplicationSlotRiskAsync(
        int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<PostgresSlotAlertInfo>();
        await using var command = _postgres.CreateCommand(SlotSql);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(NaiveUtcNow() - Freshness);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PostgresSlotAlertInfo(
                reader.IsDBNull(0) ? "(unknown)" : reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                !reader.IsDBNull(2) && reader.GetBoolean(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                reader.IsDBNull(5) ? null : reader.GetDateTime(5)));
        }

        return rows;
    }

    /// <summary>Naive-UTC now, Kind-Unspecified - the product's PG timestamp discipline (the same
    /// helper DarlingAlertReadAdapter carries). Kind=Utc binds infer timestamptz and shift the freshness
    /// window by the store session's zone offset: east of UTC the three Tier 0 alerts silently never
    /// fire. The doc blocks on the window SQL always said naive UTC; now the binds do too.</summary>
    private static DateTime NaiveUtcNow() =>
        DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
}
