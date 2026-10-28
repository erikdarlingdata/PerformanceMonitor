/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #6829: the cause sentence of get_query_store_regressions' no-baseline answers when retention is what removed the baseline.
/// The answer's two stock causes (the server's whole history sits inside the window, or it has nothing older than the baseline
/// lookback) are both false for a server whose collector ran before the baseline window began and whose Query Store data starts after it: on a store that keeps 4 days of raw <c>query_store_stats</c> a 168 hour read compares a baseline of days 14 to 7
/// back, which the store dropped. The collection log says whether the collector ran then; the floor is where the read's data starts
/// (the same instant the answer's <c>effective_start</c> reports). The floor is the oldest surviving row, which equals the store's
/// retention only on unbroken collection, so the sentences report where the data starts and never a retention figure. Both checks
/// allow <see cref="DurationTrendRouting.TruncationSlack"/> (90 minutes): the floor may sit that much before the baseline window's end
/// and still count as after it, and a collector run that much after the baseline window's start still counts as before it.
/// </summary>
internal static class RetentionCause
{
    /// <summary>The server-wide answer. Null (keep the stock causes) unless the collector ran before the baseline window began and
    /// the data starts at or after the baseline window's end (less the slack), so the whole baseline window is older than what is kept.</summary>
    internal static async Task<string?> ForServerAsync(
        NpgsqlDataSource postgres, int serverId, DateTime baselineStart, DateTime baselineEnd, DateTime? floor, bool fromTables,
        CancellationToken cancellationToken)
    {
        if (floor is not { } kept || floor.Value + DurationTrendRouting.TruncationSlack < baselineEnd)
        {
            return null;
        }

        if (!await CollectedBeforeAsync(postgres, serverId, baselineStart, cancellationToken))
        {
            return null;
        }

        /* A smaller hours_back moves the baseline window's END later (it ends where the recent window starts), so the end can pass the
           data's start. The baseline is then the partial span from there, never the whole 7 days, which the kept data cannot hold. */
        return $"The baseline window ({Date(baselineStart)} to {Date(baselineEnd)}) is older than this server's {DataKind(fromTables)}, which starts {Date(kept)}, "
            + $"so there is no baseline to compare against. An hours_back shorter than the span from {Date(kept)} to the window's end moves the baseline's end past that date, and the read then compares against the partial baseline from {Date(kept)} to that end.";
    }

    /// <summary>#6877: the answer for a window that holds nothing on either side because it ENDS before the kept data starts, which an
    /// <c>as_of</c> replay can ask for. Null (keep the first-interval and never-collected answers) unless the data starts at or after
    /// the window's end (less the slack) and the server was being collected before the window began, so what the window would have
    /// compared was stored and later aged out. The wording is <see cref="ForServerAsync"/>'s: where the kept data starts, never a
    /// retention figure. The caller carries the same window-floor hints.</summary>
    internal static async Task<(string Message, DateTime Floor)?> ForWindowAsync(
        NpgsqlDataSource postgres, int serverId, DateTime windowStart, DateTime windowEnd, DateTime? floor, bool fromTables,
        CancellationToken cancellationToken)
    {
        /* The window-floor probe answers null for a window that ends before the data starts (the start would be at or after the
           window's end), which is exactly this case, so the server's oldest row is read here. */
        if (floor is null)
        {
            await using var oldest = postgres.CreateCommand("SELECT min(collection_time) FROM collect.query_store_stats WHERE server_id = $1");
            oldest.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(oldest, serverId);
            floor = await oldest.ExecuteScalarAsync(cancellationToken) is DateTime first ? first : null;
        }

        if (floor is not { } kept || kept + DurationTrendRouting.TruncationSlack < windowEnd)
        {
            return null;
        }

        if (!await CollectedBeforeAsync(postgres, serverId, windowStart, cancellationToken))
        {
            return null;
        }

        return ($"This window ({Date(windowStart)} to {Date(windowEnd)}) is older than this server's {DataKind(fromTables)}, which starts {Date(kept)}, "
            + "so there is nothing to compare. The data from this window was stored and has since aged out.", kept);
    }

    /// <summary>The answer for a database filter, reached only when the server has a baseline. The kept data starts inside the baseline
    /// window, so a database's older baseline may have been dropped, and the sentence says it may be that rather than that it is. Null
    /// unless the collector ran before the baseline window began and the data starts after its start.</summary>
    internal static async Task<string?> ForDatabaseAsync(
        NpgsqlDataSource postgres, int serverId, DateTime baselineStart, DateTime baselineEnd, DateTime? floor, bool fromTables,
        bool singleDatabase, CancellationToken cancellationToken)
    {
        if (!RawWindowFloor.IsTruncated(floor, baselineStart) || floor is not { } kept)
        {
            return null;
        }

        if (!await CollectedBeforeAsync(postgres, serverId, baselineStart, cancellationToken))
        {
            return null;
        }

        return $"This server's {DataKind(fromTables)} starts {Date(kept)} and the baseline window ({Date(baselineStart)} to {Date(baselineEnd)}) reaches back past that, "
            + $"so either {(singleDatabase ? "that database's" : "those databases'")} older baseline was dropped by retention or {(singleDatabase ? "it has" : "they have")} none older than what is kept.";
    }

    /// <summary>What the floor is the start of: "raw Query Store data" on the raw path, "Query Store history" where the per-interval tables served.</summary>
    private static string DataKind(bool fromTables) => fromTables ? "Query Store history" : "raw Query Store data";

    private static string Date(DateTime value) => value.ToString("MMM d", CultureInfo.InvariantCulture);

    /// <summary>True when the collection log holds a successful query_store run that collected rows at or before the baseline window's
    /// start plus the shared slack: Query Store was being collected then, so data from the baseline window existed and retention is
    /// what removed it. Registration is not enough, because Query Store can be turned on weeks after a server is registered. The log
    /// keeps 60 days (<see cref="DarlingRetentionHorizons.CollectionLogRetentionDays"/>), so it reaches back past any baseline window
    /// a read without as_of asks for.</summary>
    private static async Task<bool> CollectedBeforeAsync(NpgsqlDataSource postgres, int serverId, DateTime baselineStart, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(CollectedBeforeSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddText(command, "query_store");
        var cutoff = DateTime.SpecifyKind(baselineStart.Add(DurationTrendRouting.TruncationSlack), DateTimeKind.Unspecified);
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = cutoff });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DarlingRetentionHorizons.CollectionLogHorizon(DateTime.UtcNow) });
        return await command.ExecuteScalarAsync(cancellationToken) is true;
    }

    /// <summary>$1 server_id, $2 collector_name, $3 the latest run time that counts (naive UTC), $4 the collection log's horizon (naive UTC).
    /// #6877: a window older than the log's horizon (60 days) cannot be answered by the log, whose rows aged out, so the server's oldest
    /// stored evidence stands in: the day it was added to monitoring (servers.created_date). That is weaker than a productive run, and
    /// is used only where the log cannot say, so a window the log still covers is never decided by it.</summary>
    internal const string CollectedBeforeSql = """
        SELECT EXISTS
        (
            SELECT 1
            FROM v_collection_log
            WHERE server_id = $1
            AND   collector_name = $2
            AND   status = 'SUCCESS'
            AND   rows_collected > 0
            AND   collection_time <= $3
        )
        OR
        (
            $3 < $4
            AND EXISTS
            (
                SELECT 1
                FROM servers
                WHERE server_id = $1
                AND   created_date <= $3
            )
        )
        """;
}
