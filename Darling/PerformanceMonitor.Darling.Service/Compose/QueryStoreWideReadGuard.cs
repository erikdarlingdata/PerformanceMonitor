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

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #5582: refuses, before it runs, a composed Query Store panel that would read more of
/// <c>collect.query_store_interval_wide</c> than the compose statement timeout allows. A one-day read across 43 servers
/// read 8,457,483 rows (9.2 GB) in 48 to 55 s cold against a 60 s timeout, so a fleet-wide panel over a longer window (or a
/// RankedTimeSeries panel that read the rows twice) ran to the timeout and failed after a minute of wasted work.
/// A refusal in milliseconds, with a message that says what to change, is the better answer.
///
/// <para>The estimate is a per-server daily row count from the #5094 daily summary's built table
/// (<c>collect.query_store_top_daily_built.source_rows</c>, a few hundred rows in all): for each server in scope its most
/// recent built day's rows, times the length of the range the read will count, in days. A server with no built day adds
/// nothing: with no data the guard never refuses (it fails open), and so does any fault in the lookup.</para>
///
/// <para>The counted range is an INPUT of <see cref="EstimateAsync"/> and <see cref="Refusal"/>, not the window: a later
/// change that leaves only the newest hours on the wide table passes the hours the wide table will still be read for.</para>
/// </summary>
internal static class QueryStoreWideReadGuard
{
    /// <summary>
    /// The servers in scope ($1 the scoped names, or NULL for the fleet) that have a built day, each with its most recent
    /// built day's <c>source_rows</c>. Returns the sum and the number of servers that contributed. No <c>is_enabled</c> predicate: the
    /// compiled wide read (ComposeCompiler.BuildFactRelation) joins <c>collect.servers</c> on <c>server_id</c> with none, so it reads a
    /// disabled server's retained rows too, and the estimate counts the servers the read takes.
    /// </summary>
    internal const string PerServerRowsSql = @"
SELECT COALESCE(SUM(b.source_rows), 0)::bigint, COUNT(*)::integer
FROM collect.servers AS s
CROSS JOIN LATERAL
(
    SELECT d.source_rows
    FROM collect.query_store_top_daily_built AS d
    WHERE d.server_id = s.server_id
    ORDER BY d.day DESC
    LIMIT 1
) AS b
WHERE ($1::text[] IS NULL OR s.server_name = ANY($1));";

    /// <summary>The rows one day of the servers in scope holds on the wide table, and how many servers that sums.</summary>
    internal readonly record struct Estimate(long DailyRows, int Servers)
    {
        /// <summary>The wide-table rows a read over the counted range is expected to take.</summary>
        public long RowsOver(DateTime countedStart, DateTime countedEnd) => RowsOver(countedStart, countedEnd, null);

        /// <summary>
        /// The wide-table rows a read over the counted range is expected to take, in equivalent rows (#5582). The hours in
        /// <c>[countedStart, stampThrough)</c> are served from the compose stamp's rollup and count at
        /// <see cref="ComposeLimits.StampRowWeight"/> per wide row; the rest, the tail the stamp has not reached, counts at 1.0.
        /// With no <paramref name="stampThrough"/> every hour counts at 1.0, which is the figure before the stamp existed.
        /// </summary>
        public long RowsOver(DateTime countedStart, DateTime countedEnd, DateTime? stampThrough)
        {
            var days = Math.Max(0d, (countedEnd - countedStart).TotalDays);
            if (stampThrough is not { } through)
            {
                return (long)Math.Ceiling(DailyRows * days);
            }

            var stampedDays = Math.Clamp((through - countedStart).TotalDays, 0d, days);
            var weighted = (days - stampedDays) + (stampedDays * ComposeLimits.StampRowWeight);
            return (long)Math.Ceiling(DailyRows * weighted);
        }
    }

    /// <summary>
    /// Reads the estimate for the servers in <paramref name="serverScope"/> (null or empty: the fleet). Returns null on any fault
    /// other than cancellation, so a lookup that cannot answer never refuses a panel.
    /// </summary>
    internal static async Task<Estimate?> EstimateAsync(
        NpgsqlDataSource postgres, IReadOnlyList<string>? serverScope, ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            await using var command = new NpgsqlCommand(PerServerRowsSql, connection) { CommandTimeout = McpCommandDeadlines.ReadSeconds };
            command.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
                Value = serverScope is { Count: > 0 } ? (object)serverScope.ToArray() : DBNull.Value,
            });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return null;
            }

            return new Estimate(reader.GetInt64(0), reader.GetInt32(1));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogDebug(ex, "The #5582 Query Store wide-read estimate could not be read; the panel runs without it.");
            return null;
        }
    }

    /// <summary>
    /// The refusal for a read of <paramref name="countedStart"/>..<paramref name="countedEnd"/> on the wide table, or null when the
    /// estimate is under <paramref name="limit"/> (or there is no estimate). In the style of the <see cref="ComposeLimits.MaxBuckets"/>
    /// refusal: it says how big the read is and what to change. With <paramref name="stampThrough"/> the figure is weighted
    /// (<see cref="Estimate.RowsOver(DateTime, DateTime, DateTime?)"/>), so it is not a row count and the message does not call it one:
    /// it says the read is too big for the time limit, for these servers and this window, and what to change.
    /// </summary>
    internal static string? Refusal(
        Estimate? estimate, DateTime countedStart, DateTime countedEnd, long limit = ComposeLimits.MaxQueryStoreWideRows,
        DateTime? stampThrough = null)
    {
        if (estimate is not { } found || found.Servers == 0)
        {
            return null;
        }

        var rows = found.RowsOver(countedStart, countedEnd, stampThrough);
        if (rows <= limit)
        {
            return null;
        }

        if (stampThrough is not null)
        {
            return $"This panel reads too much Query Store history to finish inside the time limit ({Servers(found.Servers)}, {Window(countedEnd - countedStart)}). "
                + "Choose fewer servers or a shorter window.";
        }

        var million = (rows / 1_000_000d).ToString("0.#", CultureInfo.InvariantCulture);
        var limitMillion = (limit / 1_000_000d).ToString("0.#", CultureInfo.InvariantCulture);
        return $"This panel needs about {million} million Query Store rows ({Servers(found.Servers)}, {Window(countedEnd - countedStart)}), "
            + $"over the limit of {limitMillion} million. Choose fewer servers or a shorter window.";
    }

    /// <summary>The guard end to end: estimate, then refuse. Null means the panel runs.</summary>
    internal static async Task<string?> CheckAsync(
        NpgsqlDataSource postgres, IReadOnlyList<string>? serverScope, DateTime countedStart, DateTime countedEnd,
        ILogger? logger, CancellationToken cancellationToken, long limit = ComposeLimits.MaxQueryStoreWideRows,
        DateTime? stampThrough = null) =>
        Refusal(await EstimateAsync(postgres, serverScope, logger, cancellationToken), countedStart, countedEnd, limit, stampThrough);

    /// <summary>
    /// The limit for a panel (#5582): <see cref="ComposeLimits.MaxQueryStoreWideRows"/> is the rows ONE scan of the wide table can read
    /// inside the statement timeout, so a RankedTimeSeries panel that still scans the fact rows twice (a <c>query_hash</c> group, a
    /// series past <see cref="ComposeLimits.MaxSingleScanBuckets"/> buckets, or a bucket x member product past
    /// <see cref="ComposeLimits.MaxSingleScanBaseRows"/>) gets <see cref="ComposeLimits.MaxQueryStoreWideRowsTwoScans"/>, which is
    /// measured on the second scan being warm, not half of the one-scan figure.
    /// </summary>
    internal static long LimitFor(bool scansFactRowsTwice) =>
        scansFactRowsTwice ? ComposeLimits.MaxQueryStoreWideRowsTwoScans : ComposeLimits.MaxQueryStoreWideRows;

    private static string Servers(int count) => count == 1 ? "1 server" : $"{count.ToString(CultureInfo.InvariantCulture)} servers";

    private static string Window(TimeSpan span)
    {
        var hours = (long)Math.Round(span.TotalHours);
        if (hours >= 24 && hours % 24 == 0)
        {
            var days = hours / 24;
            return days == 1 ? "1 day" : $"{days.ToString(CultureInfo.InvariantCulture)} days";
        }

        return hours == 1 ? "1 hour" : $"{hours.ToString(CultureInfo.InvariantCulture)} hours";
    }
}
