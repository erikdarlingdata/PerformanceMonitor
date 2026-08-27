/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
/// The MCP surface for the per-database <c>pg_stat_database</c> counters, paired with the
/// <c>pg_database_stats</c> collector (#2539). Four questions, one read: temp-file spills, buffer-cache hit
/// ratio, deadlocks, and the commit/rollback split.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgDatabaseTools
{
    /// <summary>The name PostgreSQL's own NULL-<c>datname</c> row means: activity against SHARED relations
    /// (<c>pg_database</c>, <c>pg_authid</c> and the rest of the cluster-wide catalog), which belongs to no
    /// single database. Labelled rather than filtered, because its block counters are real and dropping it
    /// would overstate every other database's hit ratio.</summary>
    internal const string SharedRelationsLabel = "(shared relations)";

    /// <summary>
    /// What a temp-file figure means and what to do about it — the reason this tool exists.
    /// <para>The split that matters is FILE COUNT against BYTES, because they point at different fixes: many
    /// small files is a <c>work_mem</c> that is slightly too low for a plan that runs constantly, while a
    /// handful of enormous ones is usually a plan or an index problem that no amount of <c>work_mem</c>
    /// makes acceptable. Reporting only "it spilled" would leave a reader choosing between those at
    /// random.</para>
    /// </summary>
    internal static string SpillFinding(long tempFiles, long tempBytes)
    {
        if (tempFiles <= 0)
        {
            return "No temp files written in this window - every sort, hash and materialization fit inside "
                 + "work_mem. This is the all-clear for the spill question.";
        }

        var averageBytes = tempBytes / tempFiles;
        var scale = averageBytes >= 256L * 1024 * 1024
            ? "A handful of very large spill files. work_mem is unlikely to be the whole answer at this "
            + "size: something is sorting or hashing far more data than it should, so look for a missing "
            + "index, a bad row estimate, or a plan that materializes an intermediate it does not need."
            : averageBytes >= 8L * 1024 * 1024
                ? "Mid-sized spill files. This is the shape a work_mem increase usually does fix - but raise "
                + "it per session or per role rather than globally, because work_mem is charged PER SORT OR "
                + "HASH NODE per query, and a global bump multiplies across every concurrent connection."
                : "Many small spill files. work_mem is a little under what the plan needs, so a modest "
                + "increase often removes them entirely - and small files are also the cheapest to leave "
                + "alone if the queries are meeting their deadline.";

        return "This database wrote temp files, which means work_mem could not hold a sort, hash or "
             + "materialization and PostgreSQL pushed it to disk. That is the single most common reason a "
             + "query is slow for a reason invisible in its plan shape. " + scale
             + " pg_stat_database is database-scoped and cannot name the QUERY: on an Amazon Aurora target, "
             + "get_pg_top_queries carries per-statement temp_blks_written (in 8 kB blocks) and is where the "
             + "attribution comes from; on stock PostgreSQL this counter is the only temp-file evidence "
             + "available at all.";
    }

    /// <summary>
    /// The cache hit ratio, with the caveat that makes it honest. The conventional target is above 99%, and
    /// the number is genuinely useful — but <c>blks_read</c> is "not found in shared_buffers", not "read
    /// from disk": the OS page cache, and on Aurora the storage layer's own cache, sit underneath. So a low
    /// ratio bounds the problem rather than measuring it, and saying otherwise would send someone buying
    /// memory for latency that is not there.
    /// </summary>
    internal static string CacheHitFinding(double? hitPct)
    {
        if (hitPct is not { } pct)
        {
            return "No block accesses in this window, so there is no hit ratio to report - not a ratio of "
                 + "zero.";
        }

        var verdict = pct >= 99
            ? "At or above the conventional 99% target: shared_buffers is absorbing effectively all of this "
            + "database's block accesses."
            : pct >= 95
                ? "Below the conventional 99% target but not alarming on its own. Worth pairing with "
                + "get_pg_io_stats, which splits the misses by CONTEXT - a ratio dragged down by bulkread "
                + "(sequential scans deliberately using a small ring buffer) will not improve with more "
                + "memory, and one dragged down by the normal context might."
                : "Well below the conventional 99% target. Before adding memory, check get_pg_io_stats for "
                + "which context the misses are in and get_pg_autovacuum_health for table bloat, because "
                + "scanning bloated heaps produces exactly this signature and more shared_buffers only "
                + "caches the bloat.";

        return $"{pct.ToString("0.##", CultureInfo.InvariantCulture)}% of block accesses were served from "
             + "shared_buffers. " + verdict
             + " Read this as an upper bound on the problem rather than a disk-I/O measurement: blks_read "
             + "means 'not in shared_buffers', and the OS page cache - or, on Aurora, the storage layer's "
             + "own cache - may still have served it without touching a disk.";
    }

    /// <summary>
    /// The commit/rollback split. The RATIO is the finding rather than the rollback count: a busy database
    /// legitimately rolls back more transactions than a quiet one, so a raw count says nothing without its
    /// denominator.
    /// </summary>
    internal static string RollbackFinding(long commits, long rollbacks)
    {
        var total = commits + rollbacks;
        if (total <= 0)
        {
            return "No transactions completed in this window.";
        }

        var pct = (double)rollbacks / total * 100;

        var verdict = pct >= 10
            ? "A rollback storm. At this share something is failing systematically rather than "
            + "occasionally - a deploy that broke a constraint, a deadlock victim loop, a client timing out "
            + "mid-transaction and abandoning it."
            : pct >= 2
                ? "Higher than a healthy application usually runs. Worth finding out whether it is one code "
                + "path or a general rise."
                : "A normal share for a healthy application.";

        return $"{pct.ToString("0.##", CultureInfo.InvariantCulture)}% of completed transactions rolled "
             + $"back ({rollbacks:N0} of {total:N0}). " + verdict
             + " PostgreSQL counts an ERROR-aborted transaction as a rollback, so this includes constraint "
             + "violations and statement timeouts, not only explicit ROLLBACK statements.";
    }

    [McpServerTool(Name = "get_pg_database_stats"), Description("Gets the per-database PostgreSQL counters from pg_stat_database, differenced across the requested window - four separate questions one cheap cluster-wide view answers. (1) TEMP FILE SPILLS: temp_files / temp_bytes are work that did not fit in work_mem and went to disk, which is the most common reason a PostgreSQL query is slow for a reason its plan shape does not show; on stock PostgreSQL this is the only temp-file evidence available anywhere, and on Aurora get_pg_top_queries carries the per-statement attribution. (2) CACHE HIT RATIO: blks_hit versus blks_read, conventionally targeted above 99% - reported as an upper bound rather than a disk measurement, because a miss here may still be served by the OS page cache or by Aurora's storage layer. (3) DEADLOCKS: a server-recorded count, so a zero really is an all-clear for the window rather than 'none was sampled'. (4) COMMIT vs ROLLBACK: the ratio a rollback storm shows up in. Every figure is a windowed difference clamped per interval, and a statistics RESET is reported explicitly (stats_reset_count, counter_rewind_count) rather than being allowed to surface as a negative rate or a spike. Per-database rows, plus PostgreSQL's own shared-relations row. Works on every PostgreSQL major and on a standby, where sorts spill exactly the way they do on a writer.")]
    public static async Task<string> GetPgDatabaseStats(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum databases to return, most temp bytes first. Default 20.")] int limit = 20,
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
            var rows = await DarlingPgDatabaseReader.GetPgDatabaseStatsAsync(
                postgres, resolved.ServerId, start, end, limit);

            if (rows.Count == 0)
            {
                return await EmptyAsync(postgres, resolved.ServerId, resolved.ServerName, hours_back, start, end);
            }

            var totalTempFiles = rows.Sum(r => r.TempFiles);
            var totalTempBytes = rows.Sum(r => r.TempBytes);
            var totalDeadlocks = rows.Sum(r => r.Deadlocks);
            var totalHits = rows.Sum(r => r.BlksHit);
            var totalReads = rows.Sum(r => r.BlksRead);
            var totalAccesses = totalHits + totalReads;
            var resetsSeen = rows.Sum(r => r.StatsResetCount) + rows.Sum(r => r.CounterRewindCount);

            var databases = rows.Select(r =>
            {
                var accesses = r.BlksHit + r.BlksRead;
                double? hitPct = accesses > 0 ? Math.Round((double)r.BlksHit / accesses * 100, 2) : null;
                var transactions = r.XactCommit + r.XactRollback;

                return new
                {
                    /* PostgreSQL's NULL name is a real value, not missing data, so it is LABELLED rather
                       than passed through as null - a null here reads as "the read could not tell", which
                       is the one thing it does not mean. */
                    database = r.DatabaseName ?? SharedRelationsLabel,
                    is_shared_relations = r.DatabaseName is null,
                    temp_files = r.TempFiles,
                    temp_bytes = r.TempBytes,
                    avg_temp_file_bytes = r.TempFiles > 0 ? r.TempBytes / r.TempFiles : (long?)null,
                    spill_finding = SpillFinding(r.TempFiles, r.TempBytes),
                    blks_hit = r.BlksHit,
                    blks_read = r.BlksRead,
                    cache_hit_pct = hitPct,
                    cache_finding = CacheHitFinding(hitPct),
                    deadlocks = r.Deadlocks,
                    xact_commit = r.XactCommit,
                    xact_rollback = r.XactRollback,
                    transactions,
                    rollback_pct = transactions > 0
                        ? Math.Round((double)r.XactRollback / transactions * 100, 2)
                        : (double?)null,
                    rollback_finding = RollbackFinding(r.XactCommit, r.XactRollback),
                    /* The reset evidence, per database, because stats_reset is per database. Both counts
                       travel even when zero: their absence from the payload would be indistinguishable from
                       a reader that never looked. */
                    stats_reset = r.StatsReset,
                    stats_reset_count = r.StatsResetCount,
                    counter_rewind_count = r.CounterRewindCount,
                    counters_were_reset = r.StatsResetCount > 0 || r.CounterRewindCount > 0,
                    reset_note = r.StatsResetCount > 0 || r.CounterRewindCount > 0
                        ? "This database's statistics were RESET during the window (pg_stat_reset, or a "
                        + "crash restart discarding them). The interval spanning the reset contributes zero "
                        + "rather than a negative figure, so every total above is a LOWER BOUND on the real "
                        + "activity - work done between the reset and the next collection is not counted."
                        : null,
                    sample_count = r.SampleCount,
                    first_sample_at = r.FirstSampleAt?.ToString("o"),
                    last_sample_at = r.LastSampleAt?.ToString("o"),
                };
            })
            .ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                status = "database_activity",
                database_count = databases.Count,
                total_temp_files = totalTempFiles,
                total_temp_bytes = totalTempBytes,
                total_deadlocks = totalDeadlocks,
                /* OF_RETURNED, not cluster_. Every total here is summed over the rows the read's LIMIT let
                   through, so on a cluster with more active databases than `limit` this is a ratio over the
                   top-N and NOT the instance's. The name says which, because a `cluster_` prefix would be a
                   number that silently changes when a caller raises the limit - a field claiming more
                   comprehensiveness than it structurally has. Computing the true instance ratio would cost a
                   second unfiltered aggregate on every call for a figure the per-database rows already
                   support, so the honest name is the fix rather than the extra query. */
                cache_hit_pct_of_returned = totalAccesses > 0
                    ? Math.Round((double)totalHits / totalAccesses * 100, 2)
                    : (double?)null,
                /* And the discriminator that makes the caveat actionable rather than fine print: when the
                   limit bit, the caller knows the totals are a top-N and can raise it. */
                limit_reached = databases.Count >= limit,
                /* Named at the top so a reader sees it before drawing a conclusion from any total below. */
                statistics_were_reset_in_window = resetsSeen > 0,
                top_spiller = totalTempBytes > 0 ? databases[0].database : null,
                note = (resetsSeen > 0
                    ? "All figures are windowed differences, clamped per interval so a statistics reset "
                    + "cannot produce a negative rate or a spike. At least one database's statistics WERE "
                    + "reset in this window - see stats_reset_count / counter_rewind_count per database - "
                    + "so its totals are lower bounds rather than exact counts."
                    : "All figures are windowed differences, clamped per interval so a statistics reset "
                    + "cannot produce a negative rate or a spike. No reset was detected in this window, so "
                    + "the totals are complete for the samples collected.")
                    + (databases.Count >= limit
                        ? $" The row limit of {limit} was REACHED, so every total above covers only the "
                        + "databases returned - more may have had activity. Raise limit for the full picture."
                        : string.Empty),
                databases,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_pg_database_stats", ex);
        }
    }

    /// <summary>
    /// Which KIND of nothing an empty result is. The engine question is asked first (#2532) — "this database
    /// was quiet" is a statement about a PostgreSQL instance, and said about a SQL Server target it is not a
    /// weak answer but a false one.
    ///
    /// <para>The denominator is the DATA, on the same relation the read walks: <c>pg_stat_database</c> is a
    /// periodic surface, so any stored sample proves somebody looked. Three misses, not two, because a
    /// cumulative counter needs a SECOND sample before it can be differenced at all — reporting a
    /// single-sample window as a quiet one would be a confident wrong answer for exactly as long as it takes
    /// the next cycle to land.</para>
    /// </summary>
    private static async Task<string> EmptyAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, int hoursBack, DateTime start, DateTime end)
    {
        var gated = await DarlingEngineCapability.NotCollectedStatusAsync(
            postgres, serverId, serverName, "pg_database_stats");
        if (gated != null)
        {
            return gated;
        }

        var (samplesInWindow, everCollected) = await DarlingPgDatabaseReader.GetCoverageAsync(
            postgres, serverId, start, end);

        var hints = new
        {
            server = serverName,
            hours_back = hoursBack,
            samples_in_window = samplesInWindow >= 2 ? "2+" : samplesInWindow.ToString(CultureInfo.InvariantCulture),
            ever_collected = everCollected,
        };

        if (samplesInWindow >= 2)
        {
            return McpHelpers.Status(
                "empty",
                $"No database recorded transactions, block accesses, temp files or deadlocks for {serverName} "
                + $"in the last {hoursBack} hour(s), and no statistics reset either. Collection DID run over "
                + "this window - at least two snapshots exist to difference - so this is a genuine all-clear "
                + "rather than missing data.",
                hints);
        }

        if (samplesInWindow == 1)
        {
            return McpHelpers.Status(
                "unavailable",
                $"Only ONE pg_stat_database snapshot exists for {serverName} in the last {hoursBack} hour(s), "
                + "so this is NOT a report of a quiet server. These are cumulative counters and a windowed "
                + "difference needs two snapshots before it produces anything at all. On a newly added "
                + "server this clears itself on the next collection cycle; otherwise widen hours_back.",
                hints);
        }

        return McpHelpers.Status(
            "unavailable",
            everCollected
                ? $"No pg_stat_database snapshots were collected for {serverName} in the last {hoursBack} "
                + "hour(s), so this is NOT an all-clear - the window says nothing either way. Collection HAS "
                + "run for this server outside the window, so this is a gap rather than a dead collector: "
                + "widen hours_back, or use get_collection_health to find where it stopped."
                : $"No pg_stat_database snapshots have EVER been collected for {serverName}, so there is "
                + "nothing to read and this is NOT a report of a quiet server. Check that collection is "
                + "running for this server and that it is enabled.",
            hints);
    }
}
