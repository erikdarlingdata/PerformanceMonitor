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
/// Reads the stored extension capability axis (<c>pg_extension_availability</c>, #2545) — the LATEST state
/// of each extension, not the history.
///
/// <para><b>Latest per extension, deliberately.</b> Installing or upgrading an extension is a rare and
/// deliberate act, so the window holds the same answer repeated daily. What a reader wants is "what can this
/// server do right now, and what is one command away"; the history exists so somebody can answer "when did
/// this appear", which is a different question and a different read.</para>
///
/// <para><b>Monitoring-relevant extensions sort FIRST, then by state.</b> A server offers dozens of
/// extensions and we have advice about eight of them. Sorting alphabetically would bury
/// <c>pg_stat_statements</c> under <c>amcheck</c> and <c>autoinc</c>, and the actionable row — an extension
/// this product can use that is <c>available</c> but not installed — is the whole reason the read exists,
/// so it sorts to the top of the relevant group.</para>
///
/// <para>Shared by the WPF tab and the MCP surface so there is one copy of this SQL, per #2530.</para>
/// </summary>
public static class DarlingPgExtensionAvailabilityReader
{
    /// <param name="State"><c>installed</c>, <c>outdated</c>, <c>available</c>, or <c>absent</c>.</param>
    /// <param name="InstalledVersion">The version created in the CONNECTED DATABASE — not the cluster.
    /// Null does not mean "nowhere on this cluster", only "not in the database we are connected to".</param>
    /// <param name="IsMonitoringRelevant">Whether this product can actually use it, as opposed to it merely
    /// being present on the server.</param>
    public sealed record PgExtensionRow(
        string? DatabaseName,
        string ExtensionName,
        string State,
        string? InstalledVersion,
        string? DefaultVersion,
        bool IsMonitoringRelevant,
        string? Comment,
        DateTime CaptureTime);

    /* DISTINCT ON (extension_name) ordered by collection_time DESC gives the newest row per extension in one
       pass - the standard PostgreSQL idiom, and cheaper than a correlated MAX per name on a hypertable.

       The outer ORDER BY re-sorts for reading, which the inner one cannot do: DISTINCT ON requires its
       database_name leads the distinct key (#2599). installed/outdated are read from pg_extension, which
       describes the CONNECTED database only, so one row per extension per SERVER would collapse databases
       that genuinely disagree and let collection_time pick the winner.

       ORDER BY to lead with the distinct key, so picking the newest and presenting in useful order are two
       different sorts and need the subquery. Same shape as DarlingPgPlanCaptureReadinessReader.

       State order is spelled as a CASE rather than left to alphabetical, which would give
       absent, available, installed, outdated - putting the thing you cannot fix above the thing you can. */
    public const string PgExtensionAvailabilitySql = """
        SELECT database_name, extension_name, state, installed_version, default_version,
               is_monitoring_relevant, comment, collection_time
        FROM (
            SELECT DISTINCT ON (database_name, extension_name)
                   database_name, extension_name, state, installed_version, default_version,
                   is_monitoring_relevant, comment, collection_time
            FROM pg_extension_availability
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY database_name, extension_name, collection_time DESC
        ) AS latest
        ORDER BY is_monitoring_relevant DESC,
                 CASE state
                     WHEN 'available' THEN 1
                     WHEN 'outdated'  THEN 2
                     WHEN 'installed' THEN 3
                     WHEN 'absent'    THEN 4
                     ELSE 5
                 END,
                 database_name, extension_name
        LIMIT $4
        """;

    public static async Task<List<PgExtensionRow>> GetPgExtensionAvailabilityAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgExtensionRow>();
        await using var command = postgres.CreateCommand(PgExtensionAvailabilitySql);
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
            rows.Add(new PgExtensionRow(
                DatabaseName: reader.IsDBNull(0) ? null : reader.GetString(0),
                ExtensionName: reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                State: reader.IsDBNull(2) ? "absent" : reader.GetString(2),
                InstalledVersion: reader.IsDBNull(3) ? null : reader.GetString(3),
                DefaultVersion: reader.IsDBNull(4) ? null : reader.GetString(4),
                IsMonitoringRelevant: !reader.IsDBNull(5) && reader.GetBoolean(5),
                Comment: reader.IsDBNull(6) ? null : reader.GetString(6),
                CaptureTime: reader.IsDBNull(7)
                    ? default
                    : DateTime.SpecifyKind(reader.GetDateTime(7), DateTimeKind.Utc)));
        }

        return rows;
    }
}
