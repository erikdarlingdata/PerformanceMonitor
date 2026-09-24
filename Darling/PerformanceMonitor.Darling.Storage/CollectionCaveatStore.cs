/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The store side of #3691 part a1: <c>collect.analysis_collection_caveats</c> (V141), one row per
/// (server, family) currently failing to read — the <see cref="OversizedPlanBacklog"/> shape applied to the
/// analysis pass's collection caveats.
///
/// <para><b>Why a table beside the in-memory ledger, not instead of it.</b>
/// <c>PerformanceMonitor.Analysis.CollectionCaveatLedger.Shared</c> already remembers the last several passes
/// per server, in-process, and is shared with Lite by design — this class does not touch it and gives it no
/// store dependency. What the ledger cannot do is survive outside the process that ran the pass: a scheduled
/// sweep builds a fresh analysis service per pass, so the ledger a Viewer process would read (a different
/// process entirely) is always empty. This table is the one thing both can reach: the ledger for
/// "how has this looked over the last N passes", this table for "what does the store say is wrong right
/// now" — the surface a separate process can query without threading a dependency back into the pass.</para>
///
/// <para><b>Current, not historical.</b> <see cref="ApplyPassAsync"/> upserts a row for every family the pass
/// could not read this cycle, and DELETES the row for every family it read successfully. A family is either
/// currently missing or it is not; there is no half-open state worth a status column, and the reader never
/// has to filter "closed" rows out of a growing table.</para>
///
/// <para><b>Best-effort, like the ledger's own writer path.</b> A store failure here must not fail or block
/// the analysis pass — the whole reason #2826 records failures at all is that the pass survives them, and a
/// bookkeeping write about a failure must not become a bigger one. Every public method logs once at Warning
/// and swallows.</para>
/// </summary>
public static class CollectionCaveatStore
{
    /// <summary>
    /// The command deadline for this table's own statements — its own constant rather than an inherited
    /// Npgsql default, matching <see cref="OversizedPlanBacklog.CommandTimeoutSeconds"/>'s reasoning. Every
    /// statement here touches at most (servers × families) rows, a population measured at a few hundred at
    /// most, so 30 s is orders of magnitude above the work.
    /// </summary>
    public const int CommandTimeoutSeconds = 30;

    /// <summary>The table, schema-qualified for the same reason <see cref="OversizedPlanBacklog.TableName"/> is.</summary>
    public const string TableName = "collect.analysis_collection_caveats";

    /// <summary>
    /// One unread family this pass, and the reason it could not be read — the payload spelling from
    /// <c>CollectionFailure.Label</c> (<c>timeout</c>, <c>cancelled</c>, <c>missing_schema</c>, <c>error</c>).
    /// </summary>
    public readonly record struct UnreadFamily(string Family, string Reason);

    /// <summary>
    /// Upserts every family this pass could not read (<paramref name="unread"/>) — keeping
    /// <c>first_seen_utc</c> across re-sightings and refreshing <c>last_seen_utc</c> and <c>reason</c> — and
    /// deletes every OTHER row this server already had, because everything not named in
    /// <paramref name="unread"/> is a family this pass read successfully (or a family that stopped
    /// existing), and a successful read is positive evidence the caveat no longer applies.
    ///
    /// <para><b>Why "delete everything else" rather than an explicit read-families list.</b> Neither
    /// collector names its SUCCESSFUL families anywhere — <see cref="AnalysisContext.CollectionFailures"/>
    /// names only the failures, and <c>CollectionFamilyCount</c> is a count, not a roster — so there is no
    /// list of read family names to hand in. The set this table must hold is exactly "every family this
    /// server is CURRENTLY failing to read", which the unread list already states in full: replacing the
    /// server's whole row set with it is equivalent to upserting the unread ones and deleting the read ones,
    /// without needing their names.</para>
    ///
    /// <para>Opens no connection when <paramref name="unread"/> is empty AND the server has no existing rows
    /// to clear — the common steady-state case (a clean pass with nothing ever recorded) pays nothing. A
    /// clean pass on a server with standing rows still opens a connection, to clear them.</para>
    ///
    /// <para>Never throws. Logged once at Warning and swallowed: a store write that failed loses this pass's
    /// update to the table, not the pass itself, and the next pass tries again.</para>
    /// </summary>
    public static async Task ApplyPassAsync(
        NpgsqlDataSource postgres,
        int serverId,
        IReadOnlyCollection<UnreadFamily> unread,
        DateTime nowUtc,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        unread ??= Array.Empty<UnreadFamily>();

        /* Naive-UTC storage: Npgsql 6+ infers timestamptz from Kind=Utc and silently zone-shifts — the
           PgCollectorRowWriter discipline, restated in OversizedPlanBacklog.RecordSightingsAsync. */
        var now = DateTime.SpecifyKind(nowUtc, DateTimeKind.Unspecified);

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

            if (unread.Count > 0)
            {
                await using var upsert = new NpgsqlCommand(UpsertUnreadFamilySql, connection)
                {
                    CommandTimeout = CommandTimeoutSeconds,
                };
                upsert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
                var family = upsert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });
                var reason = upsert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text });
                upsert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = now });

                foreach (var entry in unread)
                {
                    family.Value = entry.Family;
                    reason.Value = entry.Reason;
                    await upsert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }

            await using var clear = new NpgsqlCommand(DeleteReadFamiliesSql(unread.Count), connection)
            {
                CommandTimeout = CommandTimeoutSeconds,
            };
            clear.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            foreach (var entry in unread)
            {
                clear.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = entry.Family });
            }

            await clear.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(
                "[CollectionCaveatStore] Failed to persist collection caveats for server {ServerId}: {Message}",
                serverId, ex.Message);
        }
    }

    /// <summary>
    /// Deletes rows whose <c>last_seen_utc</c> is older than <paramref name="olderThanUtc"/> — a server that
    /// stopped being analysed (removed, or its passes have stopped landing) and so will never send another
    /// read-or-not-read verdict to retire its own rows. Called on the same cadence as
    /// <see cref="OversizedPlanBacklog"/>'s sweep-driven cleanup, not on retention's catalog-driven purge:
    /// this table is absent from <see cref="CollectorCatalog"/> for the same reason that table is.
    /// </summary>
    /// <returns>How many rows were removed, or 0 on a failed prune (logged, never thrown).</returns>
    public static async Task<int> PruneAsync(
        NpgsqlDataSource postgres,
        DateTime olderThanUtc,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        var cutoff = DateTime.SpecifyKind(olderThanUtc, DateTimeKind.Unspecified);

        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = new NpgsqlCommand(PruneStaleSql, connection)
            {
                CommandTimeout = CommandTimeoutSeconds,
            };
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = cutoff });
            return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning("[CollectionCaveatStore] Failed to prune stale collection caveats: {Message}", ex.Message);
            return 0;
        }
    }

    /// <summary>
    /// One family's upsert: <c>first_seen_utc</c> is kept across a re-sighting ($1,$2,$4,$4) so a chronic
    /// caveat's age is not reset by every pass that re-observes it; <c>last_seen_utc</c> and <c>reason</c>
    /// always take the newest values.
    /// </summary>
    public const string UpsertUnreadFamilySql = @"
INSERT INTO collect.analysis_collection_caveats AS c
(server_id, family, reason, first_seen_utc, last_seen_utc)
VALUES ($1, $2, $3, $4, $4)
ON CONFLICT ON CONSTRAINT pk_analysis_collection_caveats DO UPDATE SET
    reason = EXCLUDED.reason,
    last_seen_utc = EXCLUDED.last_seen_utc;";

    /// <summary>
    /// Every row for the server NOT in the still-unread set — the families the pass just read successfully,
    /// gone rather than stamped closed. <paramref name="unreadCount"/> is the number of <c>$2..</c>
    /// parameters the caller bound, so an empty unread set clears every row the server has (a fully clean
    /// pass) and a non-empty one clears everything except those.
    /// </summary>
    public static string DeleteReadFamiliesSql(int unreadCount)
    {
        if (unreadCount == 0)
        {
            return @"
DELETE FROM collect.analysis_collection_caveats
WHERE server_id = $1;";
        }

        var placeholders = string.Join(", ", Enumerable.Range(2, unreadCount).Select(i => "$" + i.ToString(CultureInfo.InvariantCulture)));
        return @"
DELETE FROM collect.analysis_collection_caveats
WHERE server_id = $1
AND   family NOT IN (" + placeholders + @");";
    }

    /// <summary>Every row whose last sighting predates the cutoff — a server no longer being analysed.</summary>
    public const string PruneStaleSql = @"
DELETE FROM collect.analysis_collection_caveats
WHERE last_seen_utc < $1;";

    /// <summary>How long a row survives past its last sighting before <see cref="PruneAsync"/> reaps it — a
    /// server that stopped being analysed, matching the brief's 7-day figure rather than
    /// <see cref="OversizedPlanBacklog"/>'s longer retention: this table's rows are cheap to regenerate (the
    /// very next successful pass), so there is no reason to hold a stale caveat past the point where it is
    /// almost certainly just a decommissioned or long-paused server.</summary>
    public const int PruneAfterDays = 7;
}
