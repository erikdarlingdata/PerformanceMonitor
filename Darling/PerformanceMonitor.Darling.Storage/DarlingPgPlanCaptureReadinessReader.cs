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
/// Reads the stored plan-capture readiness facets (<c>pg_plan_capture_readiness</c>, #2564) — the LATEST
/// state of each facet, not the history.
///
/// <para><b>Latest per facet, deliberately.</b> Every facet is a parameter-group setting that on Aurora/RDS
/// needs a reboot to change, so the window holds the same rows repeated hourly. A raw read would return
/// the same answer dozens of times and say nothing more than one row would. What the reader wants to know is
/// "can this server capture plans right now, and if not, what do I do" — a current state, and the history
/// exists so somebody can see WHEN it changed, which is a different question and a different read.</para>
///
/// <para><b>Ordered by facet, not by satisfaction.</b> The facets have a causal order —
/// <c>library_loaded</c> gates <c>capture_threshold</c>, and <c>extension_available</c> explains whether
/// either is even possible — so sorting the unsatisfied ones to the top would break the sequence a reader
/// has to follow. <c>extension_available</c> first, then <c>library_loaded</c>, then
/// <c>capture_threshold</c>, then <c>plan_text_setting</c>, then <c>plan_attribution</c>: could it, does
/// it, is it capturing, in what form, and can what it captures be joined to the statement it came from.</para>
///
/// <para>Shared by the WPF tab and the MCP surface so there is one copy of this SQL, per #2530.</para>
/// </summary>
public static class DarlingPgPlanCaptureReadinessReader
{
    /// <param name="Facet">Which fact this row carries.</param>
    /// <param name="IsSatisfied">Whether that fact is in the state plan capture needs.</param>
    /// <param name="Observed">What the monitored server actually answered, verbatim.</param>
    /// <param name="Detail">The consequence and remedy, as the collector framed it against what it saw.</param>
    /// <param name="CaptureTime">When this state was last observed.</param>
    public sealed record PgPlanCaptureReadinessRow(
        string Facet,
        bool IsSatisfied,
        string? Observed,
        string? Detail,
        DateTime CaptureTime);

    /* DISTINCT ON (facet) ordered by collection_time DESC gives the newest row per facet in one pass — the
       standard PostgreSQL idiom, and cheaper than a correlated MAX per facet on a hypertable.

       The outer ORDER BY re-sorts into CAUSAL order, which the inner one cannot do: DISTINCT ON requires its
       ORDER BY to lead with the distinct key, so picking the newest and presenting in reading order are two
       different sorts and need the subquery.

       The facet order is spelled as a CASE rather than left to alphabetical, which would give
       capture_threshold, extension_available, library_loaded, plan_attribution, plan_text_setting - close to
       the reverse of the order somebody has to act in. */
    public const string PgPlanCaptureReadinessSql = """
        SELECT facet, is_satisfied, observed, detail, collection_time
        FROM (
            SELECT DISTINCT ON (facet)
                   facet, is_satisfied, observed, detail, collection_time
            FROM pg_plan_capture_readiness
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY facet, collection_time DESC
        ) AS latest
        ORDER BY CASE facet
                     WHEN 'extension_available' THEN 1
                     WHEN 'library_loaded'      THEN 2
                     WHEN 'capture_threshold'   THEN 3
                     WHEN 'plan_text_setting'   THEN 4
                     WHEN 'plan_attribution'    THEN 5
                     ELSE 6
                 END,
                 facet
        LIMIT $4
        """;

    public static async Task<List<PgPlanCaptureReadinessRow>> GetPgPlanCaptureReadinessAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgPlanCaptureReadinessRow>();
        await using var command = postgres.CreateCommand(PgPlanCaptureReadinessSql);
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
            rows.Add(new PgPlanCaptureReadinessRow(
                Facet: reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                /* An unreadable is_satisfied reads as NOT satisfied. Claiming readiness we cannot prove is
                   the failure direction that matters for a facet whose entire job is to be trusted. */
                IsSatisfied: !reader.IsDBNull(1) && reader.GetBoolean(1),
                Observed: reader.IsDBNull(2) ? null : reader.GetString(2),
                Detail: reader.IsDBNull(3) ? null : reader.GetString(3),
                CaptureTime: reader.IsDBNull(4)
                    ? default
                    : DateTime.SpecifyKind(reader.GetDateTime(4), DateTimeKind.Utc)));
        }

        return rows;
    }
}
