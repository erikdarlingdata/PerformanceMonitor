/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// The PostgreSQL target's clock for the hour-of-week bucket key (#3691, closing #3749's own out-of-lane note).
///
/// <para><b>The gap.</b> #3749 (Q6) re-keyed every hour-of-week bucket to the target's LOCAL clock, resolved from the
/// newest <c>server_properties</c> row — a table only the SQL Server collectors write. A PostgreSQL target has no such
/// row, so the base read answered (NULL, NULL), <see cref="PerformanceMonitor.Analysis.Baselines.BaselineLocalClock.Resolve"/>
/// fell to UTC keying, and every PostgreSQL baseline stayed UTC hour-of-week while the SQL Server side went local: a
/// "Tue 14:00" bucket on a New York PostgreSQL target was 09:00 on the server in winter and 10:00 in summer — the same
/// DST smear the ruling names, on the other engine.</para>
///
/// <para><b>The clock a PostgreSQL target DOES publish.</b> <c>pg_server_config</c> is the hourly snapshot of the
/// target's own <c>pg_settings</c>, and <c>TimeZone</c> is one of its rows: the IANA name the server keeps its
/// clock in (<c>UTC</c>, <c>America/New_York</c>, …), with <c>source</c> saying where it came from. That name is
/// exactly what the resolver prefers — a zone id knows WHEN the offset changes, which is the whole of the DST fix — and
/// <c>TimeZoneInfo.FindSystemTimeZoneById</c> resolves IANA names natively on Linux/macOS and through ICU on Windows
/// (#3749 measured both, and the fixed-offset fallback with its one-per-zone note is what an unresolvable name gets).
/// So the override hands the resolver <c>(null, setting)</c> and inherits everything after: the step-function
/// <c>$4..$6</c> bind, the cached <c>LocalClockWindow</c> the lookup keys through, and the census that forbids a
/// bare <c>collection_time</c> key. <c>UTC</c> resolves to offset 0 — byte-for-byte the pre-#3691 keying — so a
/// target that keeps UTC changes nothing, which is most of the measured population.</para>
///
/// <para><b>Which row: the latest snapshot at or before the window end, with session-scoped sources excluded.</b>
/// This is <c>PgTargetFactCollector.PgTargetConfigSnapshotSql</c>'s rule, copied rather than re-derived. The anchor is
/// <c>MAX(collection_time) &lt;= $2</c> rather than the newest row the server has, because an analysis window can be
/// historical (<c>compare_analysis</c>, an anchored <c>analyze_server</c>) and the clock that applied THEN is the one
/// its buckets should key on; for the ordinary trailing window the two are the same snapshot. The three-value
/// <c>source</c> exclusion (<c>DarlingPgServerConfigReader.SessionScopedSources</c>) matters MORE here than for the
/// config facts: <c>TimeZone</c> is a setting a client can and routinely does set for its own session (libpq's
/// <c>PGTZ</c>, a driver's connection-string option), and a snapshot whose <c>TimeZone</c> row is session-scoped
/// describes the collector's connection, not the server. Such a snapshot yields no row here and the buckets key on
/// UTC for that compute — honest, not wrong: the server's clock is unknown from that snapshot, and an older
/// snapshot's value is not taken in its place because the rule is "the snapshot that applied", not "any snapshot
/// that had an answer". No snapshot at all (a target whose config collector is off, or a store older than V102) is the
/// same (NULL, NULL) the base read gives a row-less SQL Server target, and the same UTC keying.</para>
///
/// <para><b>What this does not do.</b> It does not stamp the resolved zone on the anomaly facts: the detector's
/// <c>baseline_hour</c>/<c>baseline_dow</c> metadata is the bucket's key and <c>BaselineContextFormatter</c> prints it
/// as the target-local hour with no edit, exactly as #3749 left the SQL Server side; the resolved
/// <c>LocalClockWindow</c> lives in the provider's cache and no <c>BaselineBucket</c> carries a zone, so there is
/// nothing for a detector to read without a shared-file change this lane does not make. It does not consult
/// <c>server_properties</c> at all for a PostgreSQL target — that table describes SQL Server hosts.</para>
/// </summary>
public sealed partial class PgTargetBaselineProvider
{
    /// <summary>
    /// The target's <c>TimeZone</c> setting from the latest <c>pg_server_config</c> snapshot at or before <c>$2</c>,
    /// session-scoped sources excluded — the config facts' anchor and exclusion, spelled inline so the parse-analysis
    /// pin over the shipped reads can check it as SQL. <c>$1</c> server_id, <c>$2</c> window end (naive UTC), <c>$3</c>
    /// the lower bound <see cref="PgTargetFactCollector.ConfigSnapshotLowerBounds"/> hands the config facts' reads
    /// (#3928): the day first, every retained snapshot only when that found nothing, so the snapshot that applied is
    /// the same one it always was and only the planning shrinks. One row at most (a snapshot holds one
    /// <c>TimeZone</c> row; the <c>LIMIT</c> says so to the planner), through <c>idx_pg_server_config_time</c> — a
    /// one-row indexed read beside a 30-day aggregate scan, on the compute's own connection and inside its one
    /// classified catch, exactly where the base puts its clock read.
    /// </summary>
    internal const string PgTargetClockSql = @"
SELECT c.setting
FROM pg_server_config AS c
WHERE c.server_id = $1
AND   c.name = 'TimeZone'
AND   c.collection_time >= $3
AND   c.collection_time = (
          SELECT MAX(collection_time)
          FROM pg_server_config
          WHERE server_id = $1
          AND   collection_time >= $3
          AND   collection_time <= $2)
AND   coalesce(c.source, '') NOT IN ('client', 'session', 'override')
/* V138 (#3691): the SERVER's TimeZone, never a per-database or per-role override of it. pg_server_config
   now also holds pg_db_role_setting's rows, and TimeZone is squarely overridable there
   (ALTER DATABASE ... SET TimeZone is a common per-tenant habit) - so without this predicate the LIMIT 1
   could return one database's zone and re-clock EVERY local-time bucket for the whole server, which is the
   worst failure this read can have: every daily/hourly bucket key silently shifts and nothing reports an
   error. The inner MAX(collection_time) is per SERVER, not per name, so it needs no predicate; only the
   outer row select does. */
AND   c.database_name IS NULL
AND   c.role_name IS NULL
AND   c.setting IS NOT NULL
LIMIT 1";

    /// <summary>
    /// The clock seam (<see cref="PgBaselineProvider.ReadServerClockAsync"/>) for a PostgreSQL target: the
    /// <c>TimeZone</c> name as the zone id, no fixed offset (the resolver derives the offsets from the zone, DST and
    /// all), and (null, null) — UTC keying — when the snapshot that applied carries no server-scoped <c>TimeZone</c>.
    /// </summary>
    protected override async Task<(int? UtcOffsetMinutes, string? TimeZoneId)> ReadServerClockAsync(
        NpgsqlConnection connection, int serverId, DateTime windowEndUtc, CancellationToken cancellationToken)
    {
        /* #3928: the day first, and every retained snapshot only when that found nothing. */
        foreach (var lowerBound in PgTargetFactCollector.ConfigSnapshotLowerBounds(windowEndUtc))
        {
            using var cmd = new NpgsqlCommand(PgTargetClockSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(serverId);
            cmd.Parameters.AddWithValue(windowEndUtc);
            cmd.Parameters.AddWithValue(lowerBound);
            if (await cmd.ExecuteScalarAsync(cancellationToken) is string setting)
            {
                return string.IsNullOrWhiteSpace(setting) ? (null, null) : (null, setting.Trim());
            }
        }

        return (null, null);
    }
}
