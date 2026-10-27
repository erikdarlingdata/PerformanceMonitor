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
/// (the same instant the answer's <c>effective_start</c> reports).
/// </summary>
internal static class RetentionCause
{
    /// <summary>The server-wide answer. Null (keep the stock causes) unless the collector ran before the baseline window began and
    /// the data starts at or after the baseline window's end, so the whole baseline window is older than what is kept.</summary>
    internal static async Task<string?> ForServerAsync(
        NpgsqlDataSource postgres, int serverId, DateTime baselineStart, DateTime baselineEnd, DateTime windowEnd, DateTime? floor, bool fromTables,
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

        return $"The baseline window ({Date(baselineStart)} to {Date(baselineEnd)}) is older than the {DaysKept(kept, windowEnd, fromTables)} this store keeps, "
            + "so there is no baseline to compare against. A smaller hours_back puts the baseline window inside the data that is kept.";
    }

    /// <summary>The answer for a database filter, reached only when the server has a baseline. The kept data starts inside the baseline
    /// window, so a database's older baseline may have been dropped, and the sentence says it may be that rather than that it is. Null
    /// unless the collector ran before the baseline window began and the data starts after its start.</summary>
    internal static async Task<string?> ForDatabaseAsync(
        NpgsqlDataSource postgres, int serverId, DateTime baselineStart, DateTime baselineEnd, DateTime windowEnd, DateTime? floor, bool fromTables,
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

        return $"This store keeps {DaysKept(kept, windowEnd, fromTables)} and the baseline window ({Date(baselineStart)} to {Date(baselineEnd)}) reaches back past that, "
            + $"so either {(singleDatabase ? "that database's" : "those databases'")} older baseline was dropped by retention or {(singleDatabase ? "it has" : "they have")} none older than what is kept.";
    }

    /// <summary>"4 days of raw Query Store data", from the kept data's start to the window's end (at least one day).</summary>
    private static string DaysKept(DateTime floor, DateTime windowEnd, bool fromTables)
    {
        var days = Math.Max(1, (int)Math.Round((windowEnd - floor).TotalDays, MidpointRounding.AwayFromZero));
        return $"{days} {(days == 1 ? "day" : "days")} of {(fromTables ? "Query Store history" : "raw Query Store data")}";
    }

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
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(baselineStart.Add(DurationTrendRouting.TruncationSlack), DateTimeKind.Unspecified) });
        return await command.ExecuteScalarAsync(cancellationToken) is true;
    }

    /// <summary>$1 server_id, $2 collector_name, $3 the latest run time that counts (naive UTC).</summary>
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
        """;
}