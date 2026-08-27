/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP surface for per-index usage, paired with the <c>pg_index_usage_stats</c> collector (#2541).
/// <para>The tool's job is the half the collector cannot do: turning "nothing scanned this" into advice that
/// will not break a schema. Every remedy it emits is gated on the catalog facts stored beside the counters,
/// and the default answer is "investigate", not "drop".</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgIndexUsageTools
{
    /// <summary>
    /// Below this many samples the read has not watched the index long enough to say anything about
    /// disuse. Two is the floor rather than a comfortable number: it is the minimum at which
    /// <c>scans_in_window</c> is a difference rather than a single reading, and the tool says how many
    /// samples it actually had so a caller can judge whether two was enough for their question.
    /// </summary>
    internal const int MinimumSamplesForADisuseClaim = 2;

    /// <summary>
    /// Why an index that nothing scanned may still not be droppable — and this is the whole reason the tool
    /// exists rather than the collector being read directly.
    ///
    /// <para><b>The blockers are checked before the finding, not appended after it.</b> The single most
    /// common zero-scan index on any schema is a unique index backing a PRIMARY KEY or UNIQUE constraint:
    /// enforcing uniqueness is not a scan of the kind <c>idx_scan</c> counts, so a constraint index can
    /// enforce correctness on every INSERT for years and still report zero. Advice derived from the counter
    /// alone would tell somebody to drop their primary key.</para>
    /// </summary>
    internal static string DroppabilityFinding(DarlingPgIndexUsageReader.PgIndexUsageRow r, long scansInWindow, int sampleCount)
    {
        /* Invalid first: it is the one state where "unused" and "safe to remove" genuinely coincide, and it
           is a finding regardless of the scan counters or how long we have been watching. */
        if (!r.IsValid)
        {
            return "INVALID index - this is what a failed CREATE INDEX CONCURRENTLY leaves behind. The "
                 + "planner will never use it, so it reports zero scans forever, while INSERTs and UPDATEs "
                 + "still maintain it and every VACUUM still cleans it. This is the one case where the scan "
                 + "count needs no corroboration: it is dead weight by construction. Rebuild it with "
                 + "REINDEX INDEX CONCURRENTLY, or drop it - but confirm first that no application is "
                 + "waiting on the index it was meant to become.";
        }

        if (r.IsPrimaryKey)
        {
            return "Backs the PRIMARY KEY. Zero scans does NOT mean unused: enforcing the key on every "
                 + "INSERT and UPDATE is not a scan and is not counted here. This cannot be dropped without "
                 + "dropping the constraint.";
        }

        if (r.SupportsConstraint || r.IsUnique)
        {
            return "Backs a UNIQUE or EXCLUSION constraint. Zero scans does NOT mean unused - a uniqueness "
                 + "check is not counted as a scan - and dropping the index drops the constraint with it, "
                 + "which changes what data the table will accept. Treat the scan count as irrelevant here "
                 + "unless you have already decided the constraint itself is not wanted.";
        }

        if (r.IsReplicaIdentity)
        {
            return "This index is the table's REPLICA IDENTITY. Dropping it breaks logical replication of "
                 + "UPDATE and DELETE for this table - downstream subscribers would start erroring or "
                 + "silently missing rows. Not droppable while logical replication is in use.";
        }

        if (sampleCount < MinimumSamplesForADisuseClaim)
        {
            return "Not enough history to say anything about disuse: this read has "
                 + sampleCount.ToString(CultureInfo.InvariantCulture) + " sample(s) of this index, and a "
                 + "windowed scan count is a DIFFERENCE that needs at least two. PostgreSQL records no "
                 + "index creation time anywhere, so how long we have been watching is the only evidence "
                 + "available that an index is old enough to judge.";
        }

        if (scansInWindow > 0)
        {
            return "In use - scanned during this window. Nothing to do.";
        }

        /* Zero scans in the window, no structural blocker, enough history to say so. Even here the remedy
           is qualified, because two things the data cannot see remain. */
        var qualifier = r.IsPartial
            ? " This is a PARTIAL index, which reads as unused in two very different situations: nothing "
            + "needs it, or the planner cannot MATCH its predicate to how the queries are now written. "
            + "Those want opposite fixes, and only the index definition and the query text together can "
            + "tell them apart - check the definition below before deciding."
            : r.IsExpression
                ? " This is an EXPRESSION index, which the planner only uses when a query repeats the "
                + "expression exactly as indexed. Zero scans may mean the expression drifted rather than "
                + "that the index is unwanted - compare the definition below against the queries."
                : string.Empty;

        return "No scans recorded in this window and no constraint, key or replica-identity role that would "
             + "stop it being dropped." + qualifier
             + " Two things this data still cannot see: a query that runs less often than the window is "
             + "long - a monthly or quarterly report will look identical to a dead index over 90 days - and "
             + "a rare query whose plan is only acceptable because this index exists. Widen hours_back to "
             + "the longest interval any scheduled job runs on before acting, and prefer making the index "
             + "invisible to the planner first where you can, so the change is reversible without a rebuild.";
    }

    /// <summary>
    /// What the index costs, in the units the cost is actually paid in. Size is the headline because
    /// storage, write amplification and vacuum cleanup all scale with it — but <c>blocks_hit</c> is what
    /// makes the cost visible on an index that is never read: those accesses are the write path maintaining
    /// it, counted by the server rather than inferred from the size.
    /// </summary>
    internal static string CostFinding(long indexBytes, long tableBytes, long blocksHit, long blocksRead)
    {
        if (indexBytes < 0)
        {
            return "Index size was not measured for this sample.";
        }

        var share = tableBytes > 0
            ? $" That is {Math.Round((double)indexBytes / tableBytes * 100, 1).ToString(CultureInfo.InvariantCulture)}% of its table's heap."
            : string.Empty;

        var maintenance = blocksHit + blocksRead > 0
            ? $" The server recorded {(blocksHit + blocksRead).ToString(CultureInfo.InvariantCulture)} block "
            + "accesses against it. On an index with no scans those are the WRITE path maintaining it and "
            + "VACUUM cleaning it, which is the cost being paid stated in the server's own units."
            : string.Empty;

        return $"{DescribeBytes(indexBytes)}." + share + maintenance
             + " On PostgreSQL an unused index costs more than the space: it is maintained by every write "
             + "AND cleaned by every VACUUM, and index cleanup is a large share of vacuum work - so a dead "
             + "index on a hot table slows the maintenance that keeps the table healthy.";
    }

    /// <summary>
    /// The row's severity band, computed HERE rather than in the browser — the web renderer never
    /// re-derives a band, so a grid can only colour a row if the read hands it one. This is what gives the
    /// web dashboard the cue the WPF grid gets from its row-style triggers.
    ///
    /// <para>The ranking mirrors the desktop's exactly: INVALID is <c>Critical</c> (the planner will never
    /// use it while writes still maintain it — the one state where "unused" and "safe to remove" coincide),
    /// an unscanned index with no structural blocker and enough history is <c>Warning</c> (a candidate,
    /// never a conclusion), and everything else is <c>Healthy</c>. An index we have not watched long enough
    /// is Healthy rather than Warning: too-early-to-say must not look like a finding.</para>
    /// </summary>
    internal static string IndexSeverity(DarlingPgIndexUsageReader.PgIndexUsageRow r)
    {
        if (!r.IsValid)
        {
            return "Critical";
        }

        var blocked = r.IsPrimaryKey || r.SupportsConstraint || r.IsUnique || r.IsReplicaIdentity;
        return !blocked && r.ScansInWindow == 0 && r.SampleCount >= MinimumSamplesForADisuseClaim
            ? "Warning"
            : "Healthy";
    }

    /// <summary>
    /// Bytes rendered for PROSE only. The payload carries the raw <c>_bytes</c> plus a numeric <c>_mb</c>,
    /// per the house pattern (<c>get_pg_autovacuum_health</c>'s <c>total_bytes</c>/<c>total_gb</c>); this
    /// exists so a finding sentence can say "6.6 MB" rather than a nine-digit number nobody reads.
    /// </summary>
    internal static string DescribeBytes(long bytes)
    {
        if (bytes < 0)
        {
            return "an unmeasured size";
        }

        if (bytes >= 1024L * 1024 * 1024)
        {
            return Math.Round(bytes / 1024.0 / 1024 / 1024, 2).ToString("0.##", CultureInfo.InvariantCulture) + " GB";
        }

        if (bytes >= 1024L * 1024)
        {
            return Math.Round(bytes / 1024.0 / 1024, 1).ToString("0.#", CultureInfo.InvariantCulture) + " MB";
        }

        return Math.Round(bytes / 1024.0, 0).ToString("0", CultureInfo.InvariantCulture) + " KB";
    }

    [McpServerTool(Name = "get_pg_index_usage")]
    [Description(
        "PostgreSQL per-index usage: how often each index was scanned, what it costs, and - the part a raw "
        + "pg_stat_user_indexes query cannot give you - whether it is actually safe to drop. Reports BOTH the "
        + "server's lifetime scan count since its statistics were last reset AND the scans observed across "
        + "the stored window, which is the more useful figure: an index with millions of lifetime scans and "
        + "none in ninety days is dead weight today, and only collected history can show that. Every index "
        + "carries the catalog facts that decide droppability - primary key, unique or exclusion constraint, "
        + "replica identity, partial, expression, invalid - and the tool refuses to recommend dropping "
        + "anything those facts do not support, including an index it has not watched for at least two "
        + "samples. Ranked by the bytes of an index nothing scanned, largest first, with invalid indexes on "
        + "top. Collected hourly per database on writers only - a replica reports its own scan counts, not "
        + "the writer's, so calling an index unused from a replica would be confidently wrong.")]
    public static async Task<string> GetPgIndexUsage(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 168 (seven days). Widen this to the longest interval any scheduled job runs on before calling an index unused.")] int hours_back = 168,
        [Description("Maximum indexes to return, biggest unscanned first. Default 25.")] int limit = 25,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var end = windowEnd;
            var start = end.AddHours(-hours_back);
            var rows = await DarlingPgIndexUsageReader.GetPgIndexUsageAsync(
                postgres, resolved.ServerId, start, end, limit);

            if (rows.Count == 0)
            {
                return await EmptyAsync(postgres, resolved.ServerId, resolved.ServerName, hours_back, start, end);
            }

            var unscanned = rows.Where(r => r.ScansInWindow == 0 && r.IsValid && !r.IsPrimaryKey
                                            && !r.SupportsConstraint && !r.IsUnique && !r.IsReplicaIdentity
                                            && r.SampleCount >= MinimumSamplesForADisuseClaim).ToList();
            var reclaimable = unscanned.Sum(r => Math.Max(r.IndexBytes, 0));
            var invalidCount = rows.Count(r => !r.IsValid);
            var resetSeen = rows.Any(r => r.StatsWereResetInWindow);

            var indexes = rows.Select(r => new
            {
                database = r.DatabaseName,
                schema = r.SchemaName,
                table = r.TableName,
                index = r.IndexName,
                /* Server-computed, because the browser never re-derives a band. Drives the web grid's cell
                   colour and matches what the WPF row-style triggers paint. */
                severity = IndexSeverity(r),
                index_bytes = r.IndexBytes,
                index_mb = r.IndexBytes >= 0 ? Math.Round(r.IndexBytes / 1024.0 / 1024, 2) : (double?)null,
                table_bytes = r.TableBytes,
                /* Both scan figures travel, always. The lifetime count is what anyone querying the server
                   directly would see, so omitting it would make this read look like it disagreed with
                   psql; the windowed one is the answer to the question actually being asked. */
                total_scans_since_stats_reset = r.TotalScans,
                scans_in_window = r.ScansInWindow,
                tuples_read = r.TuplesRead,
                tuples_fetched = r.TuplesFetched,
                blocks_read = r.BlocksRead,
                blocks_hit = r.BlocksHit,
                /* NULL on PostgreSQL 15 and below, where the server does not record it at all, and on 16+
                   for an index never scanned since the reset. The note says which rather than letting a
                   null be read as either. */
                last_scan = r.LastScan?.ToString("o"),
                last_scan_note = r.LastScan is null
                    ? "No last-scan timestamp. On PostgreSQL 16+ this means the index has not been scanned "
                    + "since the statistics were last reset; on 15 and below the server does not record "
                    + "this at all, so its absence says nothing."
                    : null,
                /* The denominator for the lifetime counter. NULL means never reset, which is the ordinary
                   state and means the counters run back to the start of the statistics system - not that
                   the value is unknown. */
                stats_reset = r.StatsReset?.ToString("o"),
                stats_reset_note = r.StatsReset is null
                    ? "This database's statistics have never been reset, so the lifetime scan count covers "
                    + "the whole life of the statistics system."
                    : "Lifetime scan counts cover only the period since this reset.",
                stats_were_reset_in_window = r.StatsWereResetInWindow,
                is_unique = r.IsUnique,
                is_primary_key = r.IsPrimaryKey,
                is_valid = r.IsValid,
                is_ready = r.IsReady,
                is_replica_identity = r.IsReplicaIdentity,
                is_partial = r.IsPartial,
                is_expression = r.IsExpression,
                supports_constraint = r.SupportsConstraint,
                index_method = r.IndexMethod,
                column_count = r.ColumnCount,
                /* The definition travels because it is what turns a name into a decision - a human can see
                   a partial index's predicate, or an expression index's expression, without opening psql
                   against production. */
                index_definition = r.IndexDefinition,
                droppability_finding = DroppabilityFinding(r, r.ScansInWindow, r.SampleCount),
                cost_finding = CostFinding(r.IndexBytes, r.TableBytes, r.BlocksHit, r.BlocksRead),
                sample_count = r.SampleCount,
                first_seen_at = r.FirstSeenAt.ToString("o"),
                measured_at = r.MeasuredAt.ToString("o"),
            })
            .ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                status = "index_usage",
                index_count = indexes.Count,
                /* Deliberately NOT called "droppable". These are the indexes with no scans in the window
                   and no structural blocker - which is a shortlist to investigate, not a work queue. The
                   field name has to survive being read by something that will act on it. */
                unscanned_without_a_structural_blocker = unscanned.Count,
                bytes_held_by_those_indexes = reclaimable,
                mb_held_by_those_indexes = Math.Round(reclaimable / 1024.0 / 1024, 2),
                invalid_index_count = invalidCount,
                statistics_were_reset_in_window = resetSeen,
                /* Same discipline as get_pg_database_stats: every total above covers the rows the LIMIT let
                   through, and the caller has to be able to tell when the cut bit. */
                limit_reached = indexes.Count >= limit,
                note = "An index with no scans is a CANDIDATE, never a conclusion. Three things this data "
                     + "cannot see, in the order they bite: an index backing a constraint enforces it "
                     + "without ever registering a scan; a query that runs less often than "
                     + $"{hours_back} hour(s) looks identical to a dead index; and a rare query may only be "
                     + "acceptable because this index exists. Counts are cumulative since each database's "
                     + "statistics were last reset, which is reported per index."
                     + (resetSeen
                         ? " At least one index's database had its statistics RESET inside this window, so "
                         + "its windowed scan count is clamped at zero rather than negative and is a lower "
                         + "bound."
                         : string.Empty)
                     + (invalidCount > 0
                         ? $" {invalidCount} INVALID index(es) are present and sorted to the top - those are "
                         + "failed CREATE INDEX CONCURRENTLY leftovers, maintained by writes and used by "
                         + "nobody."
                         : string.Empty)
                     + (indexes.Count >= limit
                         ? $" The row limit of {limit} was REACHED, so the totals cover only the indexes "
                         + "returned. Raise limit for the full picture."
                         : string.Empty),
                indexes,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_pg_index_usage", ex);
        }
    }

    /// <summary>
    /// Which KIND of nothing an empty result is. The engine question is asked FIRST (#2532): "no unused
    /// indexes" is a statement about a PostgreSQL instance, and said about a SQL Server target it is not a
    /// weak answer but a false one — <c>get_index_usage</c> is the tool there.
    /// <para>The denominator is the DATA, on the same relation the read walks, because
    /// <c>pg_index_usage_stats</c> is a PERIODIC surface: a row exists for every index every cycle, so any
    /// stored sample proves somebody looked. One snapshot is called out separately from none, because a
    /// windowed scan count is a difference and needs two.</para>
    /// </summary>
    private static async Task<string> EmptyAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, int hoursBack, DateTime start, DateTime end)
    {
        var gated = await DarlingEngineCapability.NotCollectedStatusAsync(
            postgres, serverId, serverName, "pg_index_usage_stats");
        if (gated != null)
        {
            return gated;
        }

        var probe = await DarlingPgIndexUsageReader.ProbePgIndexUsageAsync(postgres, serverId, start, end);

        var hints = new
        {
            server = serverName,
            hours_back = hoursBack,
            rows_in_window = probe.RowsInWindow,
            snapshots_in_window = probe.SnapshotsInWindow,
            ever_collected = probe.RowsEver > 0,
        };

        if (probe.SnapshotsInWindow >= MinimumSamplesForADisuseClaim)
        {
            return McpHelpers.Status(
                "empty",
                $"No indexes were recorded for {serverName} in the last {hoursBack} hour(s) even though "
                + "collection ran - at least two snapshots exist. Every index on every collected database is "
                + "under the collector's 64 KB size floor, which is the state of a schema whose indexes are "
                + "all trivially small. That is a genuine all-clear rather than missing data.",
                hints);
        }

        if (probe.SnapshotsInWindow == 1)
        {
            return McpHelpers.Status(
                "unavailable",
                $"Only ONE index snapshot exists for {serverName} in the last {hoursBack} hour(s), so this "
                + "is NOT a report about index usage. A windowed scan count is a DIFFERENCE and needs two "
                + "snapshots before it produces anything. This collector runs DAILY, so on a newly added "
                + "server it clears itself within a day or two; widen hours_back in the meantime.",
                hints);
        }

        return McpHelpers.Status(
            "unavailable",
            probe.RowsEver > 0
                ? $"No index snapshots were collected for {serverName} in the last {hoursBack} hour(s), so "
                + "the window says nothing either way. Collection HAS run for this server outside the "
                + "window - this collector runs DAILY, so a window shorter than about 48 hours can "
                + "legitimately contain fewer than two samples. Widen hours_back, or use "
                + "get_collection_health to find where it stopped."
                : $"No index snapshots have EVER been collected for {serverName}, so there is nothing to "
                + "read. Check that collection is running and enabled for this server - and note this "
                + "collector is gated OFF on read replicas, because a replica reports its own scan counts "
                + "rather than the writer's, which would make every index on it look unused.",
            hints);
    }
}
