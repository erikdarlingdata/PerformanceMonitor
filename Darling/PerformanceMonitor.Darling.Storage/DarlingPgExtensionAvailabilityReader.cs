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
/// <para><b>Rows are the PRODUCT of two populations, so a host outgrows any row cap (#3425).</b> One row per
/// (database, extension) means a ten-database host needs 1,020 rows against a cap of 1,000 - measured on
/// five servers of one store needing 1,020 to 1,632 and getting 1,000. And the truncation is not a random
/// slice: the state ordering below puts non-relevant <c>installed</c> rows behind every non-relevant
/// <c>available</c> one, and <c>plpgsql</c> is the only non-relevant installed extension on the fleet -
/// measured as one row per database at positions 506-510 of a complete 510-row five-database response. So on
/// a ten-database host <c>plpgsql</c>, which is installed in every database of every one of them, has no row
/// at all and an install census comes back looking complete. <see cref="InstallCensusSql"/> and the
/// <c>database_name</c> filter are the two answers to that, and they answer different halves of it.</para>
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
       absent, available, installed, outdated - putting the thing you cannot fix above the thing you can.

       THE CONSEQUENCE OF THAT ORDER UNDER A CAP, which is #3425: `available` ranks first because it is the
       actionable state, so every non-relevant available row on every database sorts ahead of the
       non-relevant INSTALLED ones. Measured on a five-database Aurora 17 target, that is 93 available rows
       per database against exactly one installed (plpgsql), which lands plpgsql at rows 506-510 of 510. The
       ordering is right for reading and it is the reason a truncated response drops precisely the rows an
       install census needs, so this read owes the reader InstallCensusSql beside the rows rather than a
       differently-wrong sort. */
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
            /* THE DATABASE FILTER, and it is INSIDE the DISTINCT ON scope on purpose (#3425). Rows here are
               the product of databases and extension names, and one database's slice is ~102 rows - two
               orders of magnitude under the cap - so narrowing the population is what makes a host
               completable rather than pageable. Applied before the distinct rather than after it because
               the whole point is not to materialise the other databases' rows at all.

               NULL means every database, which is what every existing caller passes. */
            AND   ($5::text IS NULL OR database_name = $5::text)
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

    /// <summary>
    /// One row of the limit-independent install census: an extension name, and how many of this server's
    /// databases have it CREATED versus how many reported on it at all (#3425).
    /// </summary>
    /// <param name="DatabasesTotal">How many databases the collector reported on in this window — the
    /// denominator, repeated on every row because a count of installations without one is the figure this
    /// census exists to stop being guessed at.</param>
    /// <param name="ExtensionNameCount">How many distinct extension names those databases reported. Also the
    /// per-database row count when collection is uniform, which is what makes
    /// <c>DatabasesTotal * ExtensionNameCount</c> the row population a caller's limit is being measured
    /// against. 102 on the measured fleet.</param>
    /// <param name="RowsAvailable">The (database, extension) pairs actually stored, counted rather than
    /// multiplied out. Equal to the product when every database reported every extension, and LOWER when one
    /// database's collection was partial — so a caller comparing it against the product can see that without
    /// this read having to assume uniformity.</param>
    /// <param name="ExtensionName">Null on the single row a server with no stored rows produces: the scalars
    /// are carried by a one-row relation the per-extension groups hang off, so the denominators survive an
    /// empty census.</param>
    /// <param name="DatabasesReporting">How many databases have a row for THIS extension. Below
    /// <paramref name="DatabasesTotal"/> only when collection was uneven, and reported rather than assumed
    /// away.</param>
    /// <param name="DatabasesInstalled">Databases where the extension is created at the default version.</param>
    /// <param name="DatabasesOutdated">Databases where it is created at an OLDER version than the server
    /// offers. Counted apart from <paramref name="DatabasesInstalled"/> and never folded into it: both mean
    /// created, and only one of them means up to date.</param>
    public readonly record struct PgExtensionInstallCensusRow(
        long DatabasesTotal,
        long ExtensionNameCount,
        long RowsAvailable,
        string? ExtensionName,
        long DatabasesReporting,
        long DatabasesInstalled,
        long DatabasesOutdated);

    /* THE INSTALL CENSUS, over the POPULATION and unaffected by any row limit (#3425) — the #3278 pattern
       applied to a read whose truncation drops the rows a census needs. An aggregated row cannot be
       truncated away while its siblings survive, which is exactly what happens to plpgsql in the row list.

       ONE ROW PER EXTENSION, not per (database, extension), so the payload is bounded by how many extension
       names the PostgreSQL build offers rather than by the tenant count — 102 on the measured fleet, and a
       figure that does not grow as databases are added. That is the property that makes this fit inside the
       cap on any host, and it is why it is safe to return uncapped: the row list's problem is a PRODUCT, and
       this is one of its factors.

       ONLY EXTENSIONS SOMEBODY CREATED, via the HAVING. An install census is about what is installed; the
       available-but-not-installed rows are the actionable ones and they already sort FIRST in the row list,
       so they need no aggregate to be seen. Restricting here keeps the block at the handful of rows an
       operator reads (two on the measured target) instead of 102 mostly-zero ones.

       DISTINCT ON is what makes it a population rather than a history: this collector writes every
       extension of every database on every daily cycle, so a plain aggregate over a seven-day window would
       report seven times the installation count. Same tie-break as the row read — newest row per
       (database, extension) — so the census and the rows cannot disagree about an extension's state.

       THE SCALARS HANG OFF A ONE-ROW RELATION via LEFT JOIN ... ON true, the idiom
       DarlingPgIndexBloatReader.CoverageEvidenceSql uses: it is what lets one statement carry both a scalar
       denominator and a variable-length breakdown, and it is what keeps the denominators on a server whose
       census is empty, where an inner join would drop precisely the row that says so.

       count(DISTINCT ...) over `latest` rather than over the raw table: the raw table repeats every pair
       once per cycle, and while a DISTINCT count would survive that, reading both figures off the same
       reduced relation is what stops the two describing different populations.

       Every output column is aliased with AS — an unaliased count(*) comes back named `count`, and there are
       four of them here. $1 server_id, $2 window start, $3 window end. */
    public const string InstallCensusSql = """
        WITH latest AS (
            SELECT DISTINCT ON (database_name, extension_name)
                   database_name, extension_name, state
            FROM pg_extension_availability
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY database_name, extension_name, collection_time DESC
        ),
        totals AS (
            SELECT count(DISTINCT database_name)::bigint  AS databases_total,
                   count(DISTINCT extension_name)::bigint AS extension_name_count,
                   count(*)::bigint                       AS rows_available
            FROM latest
        ),
        created AS (
            SELECT latest.extension_name                                            AS extension_name,
                   count(*)::bigint                                                 AS databases_reporting,
                   count(*) FILTER (WHERE latest.state = 'installed')::bigint        AS databases_installed,
                   count(*) FILTER (WHERE latest.state = 'outdated')::bigint         AS databases_outdated
            FROM latest
            GROUP BY latest.extension_name
            HAVING count(*) FILTER (WHERE latest.state IN ('installed', 'outdated')) > 0
        )
        SELECT totals.databases_total,
               totals.extension_name_count,
               totals.rows_available,
               created.extension_name,
               created.databases_reporting,
               created.databases_installed,
               created.databases_outdated
        FROM totals
        LEFT JOIN created ON true
        ORDER BY created.databases_installed DESC NULLS LAST,
                 created.databases_outdated DESC NULLS LAST,
                 created.extension_name
        """;

    /// <summary>
    /// The limit-independent install census for one server: the (database, extension) population and, for
    /// every extension somebody created anywhere on it, how many databases have it (#3425).
    ///
    /// <para><b>Takes both window ends BECAUSE it uses both</b> — the caller's own window, unlike
    /// <c>DarlingPgIndexBloatReader</c>'s coverage census, which anchors a fixed lookback. That difference is
    /// deliberate: extension state is a config fact a reader asks about over the same span they asked the
    /// rows over, and an accepted-but-ignored parameter is worse than an absent one.</para>
    ///
    /// <para>Raises rather than swallowing. This census is not an explanation bolted onto a result the caller
    /// already has — it is the only complete answer in the response, so a caller that got an exception knows
    /// it has nothing, where a caller handed an empty census would read zero installations.</para>
    /// </summary>
    public static async Task<List<PgExtensionInstallCensusRow>> GetInstallCensusAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgExtensionInstallCensusRow>();
        await using var command = postgres.CreateCommand(InstallCensusSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* SpecifyKind(Unspecified) at the bind, for the reason the row read documents: Kind=Utc infers
           timestamptz and the comparison against these naive columns then resolves at the store session's
           TimeZone, which east of UTC slides the window off the data — and a census that silently returned
           nothing would report zero installations on a server that has them. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PgExtensionInstallCensusRow(
                DatabasesTotal: reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
                ExtensionNameCount: reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                RowsAvailable: reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                /* NULL on the scalars-only row a server with no stored rows produces. Kept rather than
                   skipped: that row is what carries the denominators, and dropping it would turn "we
                   looked and there is nothing installed" into the same empty list as "we did not look". */
                ExtensionName: reader.IsDBNull(3) ? null : reader.GetString(3),
                DatabasesReporting: reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                DatabasesInstalled: reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                DatabasesOutdated: reader.IsDBNull(6) ? 0 : reader.GetInt64(6)));
        }

        return rows;
    }

    /// <param name="databaseName">One database, or null for every database on the server (#3425). The row
    /// population is databases x extension names, so a host's whole product outgrows any row cap while ONE
    /// database's slice — 102 rows on the measured fleet — sits two orders of magnitude under it. This is
    /// what makes a sixteen-database host sixteen completable calls instead of one uncompletable one.</param>
    public static async Task<List<PgExtensionRow>> GetPgExtensionAvailabilityAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        string? databaseName = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgExtensionRow>();
        await using var command = postgres.CreateCommand(PgExtensionAvailabilitySql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* SpecifyKind(Unspecified) at the BIND, same convention as every other PostgreSQL read here: Npgsql
           does not reject Kind=Utc, it infers timestamptz, and PostgreSQL then resolves the comparison
           against these NAIVE timestamp columns at the store session's TimeZone — so east of UTC the window
           slides off the data and the read returns nothing at all. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);
        /* $5, appended rather than inserted — renumbering four working parameter indexes inside a string is
           a silent re-aim the compiler cannot see.

           DBNull for the every-database case, and BLANK counts as absent: the web surface hands over a
           query-string value, so an empty `database_name=` would otherwise select the databases named
           with the empty string - none of them - and return an empty list that reads as a server with no
           extensions. Trimmed for the same reason. Same shape as DarlingPgDeadlockReader's optional
           hash, whose `$n::text IS NULL` form this SQL copies because that one is live-exercised. */
        command.Parameters.AddWithValue(
            string.IsNullOrWhiteSpace(databaseName) ? (object)DBNull.Value : databaseName.Trim());

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
