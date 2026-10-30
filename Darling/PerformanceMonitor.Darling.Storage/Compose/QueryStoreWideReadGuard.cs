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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #5582: refuses, before it runs, a composed Query Store panel that would read more of
/// <c>collect.query_store_interval_wide</c> than the compose statement timeout allows. A one-day read across 43 servers
/// read 8,457,483 rows (9.2 GB) in 48 to 55 s cold against a 60 s timeout, so a fleet-wide panel over a longer window (or a
/// RankedTimeSeries panel that read the rows twice) ran to the timeout and failed after a minute of wasted work.
/// A refusal in milliseconds, with a message that says what to change, is the better answer.
///
/// <para>The estimate is a per-server daily row count from the #5094 daily summary's built table
/// (<c>collect.query_store_top_daily_built.source_rows</c>, a few hundred rows in all): for each server in scope its most
/// recent built day's rows, times the length of the range the read will count, in days. A server with no built day is counted from the
/// collection log instead (#6115, see <see cref="PerServerRowsSql"/>); one with neither adds nothing: with no data the guard never refuses
/// (it fails open), and so does any fault in the lookup.</para>
///
/// <para>The counted range is an INPUT of <see cref="EstimateAsync"/> and <see cref="Refusal"/>, not the window: a later
/// change that leaves only the newest hours on the wide table passes the hours the wide table will still be read for.</para>
///
/// <para>#5811 adds the raw route's twin: a <c>query_stats</c> or <c>procedure_stats</c> panel read raw for its whole window is estimated
/// from the collection log (<see cref="RawCollectorRowsSql"/>) as a <see cref="RawEstimate"/> of the rows the window itself holds, and
/// refused by <see cref="RawRefusal"/> over the per-collector limit.</para>
/// </summary>
internal static class QueryStoreWideReadGuard
{
    /// <summary>
    /// The servers in scope ($1 the scoped names, or NULL for the fleet), each with one day's rows on the wide table. A server with a built
    /// day contributes its most recent built day's <c>source_rows</c>. Returns the sum and the number of servers that contributed. No
    /// <c>is_enabled</c> predicate: the compiled wide read (ComposeCompiler.BuildFactRelation) joins <c>collect.servers</c> on <c>server_id</c> with none, so it
    /// reads a disabled server's retained rows too, and the estimate counts the servers the read takes.
    ///
    /// <para>#6115: a server with NO built day (the copies enrolled most recently: the daily builder plans only days after a server's
    /// coverage <c>filled_since</c>, so it has built nothing yet) used to add 0, so the estimate at 210 servers was about the estimate at 126
    /// (85 of the 211 servers on the load-test store had no built day) and the guard let a panel through that then timed out at 60 s. It is
    /// now counted from the collection log, the way <see cref="RawCollectorRowsSql"/> counts the raw half: the rows the Query Store collector
    /// (<c>query_store</c>) stored over the day that ends at the server's newest log row. <c>rows_collected</c> counts stored snapshots, so
    /// it over-counts the interval table a little and leans toward refusing. A server with neither a built day nor a log row adds 0, and
    /// a fault in the lookup fails open (<see cref="EstimateAsync"/> returns null). $2 are the servers a split panel reads raw (NULL for none).</para>
    ///
    /// <para>The log's rows are counted only INSIDE the panel's window, $3 to $4 (the wide counted range): the rows the server collected there,
    /// spread over the window's days so <see cref="Estimate.RowsOver(DateTime, DateTime)"/> gives back exactly those rows. A server that stopped
    /// collecting before the window (a disabled one, whose built rows the daily builder's GC deletes, or one whose built days expired) adds 0, as
    /// it did before #6115; a newly enrolled one counts what it really collected. With no window ($3 and $4 NULL) the figure is the day that ends
    /// at the server's newest log row.</para>
    /// </summary>
    internal const string PerServerRowsSql = @"
SELECT COALESCE(SUM(t.day_rows), 0)::bigint, COUNT(*)::integer
FROM
(
    SELECT b.source_rows AS day_rows
    FROM collect.servers AS s
    CROSS JOIN LATERAL
    (
        SELECT d.source_rows
        FROM collect.query_store_top_daily_built AS d
        WHERE d.server_id = s.server_id
        ORDER BY d.day DESC
        LIMIT 1
    ) AS b
    WHERE ($1::text[] IS NULL OR s.server_name = ANY($1))
      AND ($2::integer[] IS NULL OR s.server_id <> ALL($2))
    UNION ALL
    SELECT l.day_rows
    FROM collect.servers AS s
    CROSS JOIN LATERAL
    (
        SELECT n.collection_time AS anchor
        FROM collect.collection_log AS n
        WHERE n.server_id = s.server_id
          AND n.collector_name = '" + QueryStoreRawCollector + @"'
        ORDER BY n.collection_time DESC
        LIMIT 1
    ) AS a
    CROSS JOIN LATERAL
    (
        SELECT SUM(x.rows_collected)::bigint AS window_rows
        FROM collect.collection_log AS x
        WHERE x.server_id = s.server_id
          AND x.collector_name = '" + QueryStoreRawCollector + @"'
          AND x.collection_time > COALESCE($3::timestamp, a.anchor - interval '1 day')
          AND x.collection_time <= COALESCE($4::timestamp, a.anchor)
    ) AS w
    CROSS JOIN LATERAL
    (
        SELECT CEIL(w.window_rows / (EXTRACT(EPOCH FROM
                   COALESCE($4::timestamp, a.anchor) - COALESCE($3::timestamp, a.anchor - interval '1 day')) / 86400.0))::bigint AS day_rows
    ) AS l
    WHERE NOT EXISTS (SELECT 1 FROM collect.query_store_top_daily_built AS nb WHERE nb.server_id = s.server_id)
      AND w.window_rows IS NOT NULL
      AND COALESCE($4::timestamp, a.anchor) > COALESCE($3::timestamp, a.anchor - interval '1 day')
      AND ($1::text[] IS NULL OR s.server_name = ANY($1))
      AND ($2::integer[] IS NULL OR s.server_id <> ALL($2))
) AS t;";

    /// <summary>The rows one day of the servers in scope holds on the wide table, and how many servers that sums. <c>RawRows</c> and
    /// <c>RawServers</c> (#6115) are the raw half of a split panel: the rows it reads and how many servers that is.</summary>
    internal readonly record struct Estimate(long DailyRows, int Servers, long RawRows = 0, int RawServers = 0)
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
                return (long)Math.Ceiling(DailyRows * days) + RawRows;
            }

            var stampedDays = Math.Clamp((through - countedStart).TotalDays, 0d, days);
            var weighted = (days - stampedDays) + (stampedDays * ComposeLimits.StampRowWeight);
            return (long)Math.Ceiling(DailyRows * weighted) + RawRows;
        }
    }

    /// <summary>
    /// Reads the estimate for the servers in <paramref name="serverScope"/> (null or empty: the fleet), leaving out
    /// <paramref name="excludedServerIds"/> (#6115: the servers a split panel reads raw, whose rows the wide read never takes).
    /// Returns null on any fault other than cancellation, so a lookup that cannot answer never refuses a panel.
    /// </summary>
    internal static async Task<Estimate?> EstimateAsync(
        NpgsqlDataSource postgres, IReadOnlyList<string>? serverScope, ILogger? logger,
        int commandTimeoutSeconds, CancellationToken cancellationToken, IReadOnlyList<int>? excludedServerIds = null, DateTime? windowStart = null, DateTime? windowEnd = null)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            return await ReadEstimateAsync(connection, PerServerRowsSql, serverScope, excludedServerIds, windowStart, windowEnd, commandTimeoutSeconds, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogDebug(ex, "The #5582 Query Store wide-read estimate could not be read; the panel runs without it.");
            return null;
        }
    }

    /// <summary>
    /// #5811: the raw-route twin of <see cref="PerServerRowsSql"/>. The raw collector tables have no daily summary, so each server's daily
    /// rows come from the collection log: the rows its collector ($2) stored over the day that ends at its newest log row for that
    /// collector (its anchor), which is the rows per collection times the collections in a day. A server counts only for the part of the
    /// counted range ($3 to $4) its collection covers, so a server that stopped collecting adds no rows the read will never touch:
    /// <code>rows = daily_rows * days(LEAST($4, anchor) - GREATEST($3, now - RawRetentionInterval))</code>, and a server whose
    /// covered part is empty (its anchor is at or before the range's start, or older than raw retention) adds nothing and is not
    /// counted. Raw rows older than <see cref="TimescaleSupport.RawRetentionInterval"/> are gone even while the collection log
    /// (<c>CollectionHealthRetentionInterval</c>) still holds their log rows. Read through
    /// <c>idx_collection_log_watermark (server_id, collector_name, collection_time DESC)</c>: one short range per server. A server with no
    /// log row for the collector adds nothing, so the guard fails open exactly as the Query Store estimate does. No <c>is_enabled</c>
    /// predicate, for the reason <see cref="PerServerRowsSql"/> gives: the raw read takes a disabled server's retained rows too.
    /// Returns the rows over the range, the daily rows of the servers that cover any of it, and how many servers those are.
    /// </summary>
    internal const string RawCollectorRowsSql = @"
SELECT COALESCE(CEIL(SUM(b.daily_rows * c.covered_days)), 0)::bigint,
       COALESCE(SUM(b.daily_rows), 0)::bigint,
       COUNT(*)::integer
FROM collect.servers AS s
CROSS JOIN LATERAL
(
    SELECT n.collection_time AS anchor
    FROM collect.collection_log AS n
    WHERE n.server_id = s.server_id
      AND n.collector_name = $2
    ORDER BY n.collection_time DESC
    LIMIT 1
) AS a
CROSS JOIN LATERAL
(
    SELECT SUM(l.rows_collected)::bigint AS daily_rows
    FROM collect.collection_log AS l
    WHERE l.server_id = s.server_id
      AND l.collector_name = $2
      AND l.collection_time > a.anchor - interval '1 day'
) AS b
CROSS JOIN LATERAL
(
    SELECT EXTRACT(EPOCH FROM
               LEAST($4::timestamp, a.anchor)
               - GREATEST($3::timestamp, (now() AT TIME ZONE 'UTC') - interval '" + TimescaleSupport.RawRetentionInterval + @"')
           ) / 86400.0 AS covered_days
) AS c
WHERE b.daily_rows IS NOT NULL
  AND c.covered_days > 0
  AND ($1::text[] IS NULL OR s.server_name = ANY($1))
  AND ($5::integer[] IS NULL OR s.server_id = ANY($5));";

    /// <summary>
    /// The raw-route estimate (#5811): <paramref name="Rows"/> the counted range holds for the servers in scope, each counted only for
    /// the part of the range its collection covers (<see cref="RawCollectorRowsSql"/>); <paramref name="DailyRows"/> a day of those
    /// servers at their newest rate; <paramref name="Servers"/> how many cover any of the range.
    /// </summary>
    internal readonly record struct RawEstimate(long Rows, long DailyRows, int Servers);

    /// <summary>The savepoint the raw estimate takes when it reads inside the hourly-edges snapshot's transaction (#5811).</summary>
    private const string RawEstimateSavepointSql = "SAVEPOINT compose_raw_read_guard";

    /// <summary>Undoes <see cref="RawEstimateSavepointSql"/> after a fault, so the panel statement still runs in that transaction.</summary>
    private const string RawEstimateRollbackSql = "ROLLBACK TO SAVEPOINT compose_raw_read_guard";

    /// <summary>
    /// The collector whose rows a panel reads raw for its whole window (#5811), or null when the guard does not apply: the
    /// <c>query_stats</c> and <c>procedure_stats</c> raw tables only, and only on the raw route. A CAGG route reads the rollup and the
    /// hourly-raw-edges route reads raw only at its two edges. The collector name is the table name.
    /// </summary>
    internal static string? RawCollectorFor(string sourceTable, ComposeRoute route) =>
        route.Tier == ComposeSourceTier.Raw && sourceTable is "query_stats" or "procedure_stats" ? sourceTable : null;

    /// <summary>The raw-route limit for a collector (#5811): <see cref="ComposeLimits.MaxRawQueryStatsRows"/> or
    /// <see cref="ComposeLimits.MaxRawProcedureStatsRows"/>.</summary>
    internal static long RawLimitFor(string collector) =>
        collector == "procedure_stats" ? ComposeLimits.MaxRawProcedureStatsRows : ComposeLimits.MaxRawQueryStatsRows;

    /// <summary>
    /// Reads the raw-route estimate (#5811) for <paramref name="collector"/> and the servers in <paramref name="serverScope"/>. With
    /// <paramref name="snapshotConnection"/> (the hourly-edges snapshot's connection, inside its open transaction) the read runs there
    /// under a savepoint, so the run never asks the pool for a second connection and a fault leaves the transaction usable; otherwise it
    /// opens its own. Null on any fault other than cancellation: a lookup that cannot answer never refuses a panel.
    /// </summary>
    internal static async Task<RawEstimate?> EstimateRawAsync(
        NpgsqlDataSource postgres, NpgsqlConnection? snapshotConnection, IReadOnlyList<string>? serverScope, string collector,
        DateTime countedStart, DateTime countedEnd, ILogger? logger, int commandTimeoutSeconds, CancellationToken cancellationToken,
        IReadOnlyList<int>? onlyServerIds = null)
    {
        /* The savepoint is inside the try: a snapshot connection the store dropped since the snapshot's last statement (a restart, a
           failover, a pooler reset, idle_in_transaction_session_timeout) fails here, and that is a lookup that cannot answer, not a
           failed panel. */
        try
        {
            if (snapshotConnection is not null)
            {
                await ExecuteAsync(snapshotConnection, RawEstimateSavepointSql, commandTimeoutSeconds, cancellationToken);
                return await ReadRawEstimateAsync(snapshotConnection, serverScope, collector, countedStart, countedEnd, onlyServerIds, commandTimeoutSeconds, cancellationToken);
            }

            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            return await ReadRawEstimateAsync(connection, serverScope, collector, countedStart, countedEnd, onlyServerIds, commandTimeoutSeconds, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            if (snapshotConnection is not null)
            {
                /* A rollback that fails too (the connection is gone, or the savepoint never ran) must not replace the fault: the guard
                   still gives no verdict, and the panel statement meets the broken connection itself, where the run reports it. */
                try
                {
                    await ExecuteAsync(snapshotConnection, RawEstimateRollbackSql, commandTimeoutSeconds, cancellationToken);
                }
                catch (Exception rollbackEx) when (rollbackEx is not OperationCanceledException)
                {
                    logger?.LogDebug(rollbackEx, "The #5811 raw-route estimate's savepoint could not be rolled back.");
                }
            }

            logger?.LogDebug(ex, "The #5811 raw-route read estimate could not be read; the panel runs without it.");
            return null;
        }
    }

    /// <summary>
    /// The raw-route refusal (#5811) for a read of <paramref name="countedStart"/>..<paramref name="countedEnd"/>, or null when the
    /// estimate is under <paramref name="limit"/> (or there is none). It states the estimate, the limit and the longest window that fits
    /// for these servers, so a refusal reported from the field can be checked against the collection log.
    /// </summary>
    internal static string? RawRefusal(RawEstimate? estimate, string collector, DateTime countedStart, DateTime countedEnd, long limit)
    {
        if (estimate is not { } found || found.Servers == 0)
        {
            return null;
        }

        var rows = found.Rows;
        if (rows <= limit)
        {
            return null;
        }

        var noun = collector == "procedure_stats" ? "procedure statistics" : "query statistics";
        var fitHours = found.DailyRows > 0 ? (long)Math.Floor(limit * 24d / found.DailyRows) : 0;
        var shorter = fitHours >= 1
            ? $"Choose fewer servers or a window of {Window(TimeSpan.FromHours(fitHours))} or less."
            : "Choose fewer servers.";
        return $"This panel needs about {Million(rows)} million {noun} rows ({Servers(found.Servers)}, {Window(countedEnd - countedStart)}), "
            + $"over the limit of {Million(limit)} million. {shorter}";
    }

    /// <summary>The raw-route guard end to end (#5811): estimate, then refuse. Null means the panel runs.</summary>
    internal static async Task<string?> CheckRawAsync(
        NpgsqlDataSource postgres, NpgsqlConnection? snapshotConnection, IReadOnlyList<string>? serverScope, string collector,
        DateTime countedStart, DateTime countedEnd, ILogger? logger,
        int commandTimeoutSeconds, CancellationToken cancellationToken) =>
        RawRefusal(
            await EstimateRawAsync(postgres, snapshotConnection, serverScope, collector, countedStart, countedEnd, logger, commandTimeoutSeconds, cancellationToken),
            collector, countedStart, countedEnd, RawLimitFor(collector));

    /// <summary>The Query Store estimate's reader: $1 the scoped names (or NULL).</summary>
    private static async Task<Estimate?> ReadEstimateAsync(
        NpgsqlConnection connection, string sql, IReadOnlyList<string>? serverScope, IReadOnlyList<int>? excludedServerIds,
        DateTime? windowStart, DateTime? windowEnd, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add(ScopeParameter(serverScope));
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer,
            Value = excludedServerIds is { Count: > 0 } ? (object)excludedServerIds.ToArray() : DBNull.Value,
        });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = windowStart is { } ws ? DateTime.SpecifyKind(ws, DateTimeKind.Unspecified) : DBNull.Value,
        });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = windowEnd is { } we ? DateTime.SpecifyKind(we, DateTimeKind.Unspecified) : DBNull.Value,
        });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new Estimate(reader.GetInt64(0), reader.GetInt32(1));
    }

    /// <summary>The raw estimate's reader (#5811): $1 the scoped names (or NULL), $2 the collector, $3 and $4 the counted range.</summary>
    private static async Task<RawEstimate?> ReadRawEstimateAsync(
        NpgsqlConnection connection, IReadOnlyList<string>? serverScope, string collector, DateTime countedStart, DateTime countedEnd,
        IReadOnlyList<int>? onlyServerIds, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(RawCollectorRowsSql, connection) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add(ScopeParameter(serverScope));
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = collector });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(countedStart, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(countedEnd, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer,
            Value = onlyServerIds is { Count: > 0 } ? (object)onlyServerIds.ToArray() : DBNull.Value,
        });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new RawEstimate(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt32(2));
    }

    private static NpgsqlParameter ScopeParameter(IReadOnlyList<string>? serverScope) => new()
    {
        NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
        Value = serverScope is { Count: > 0 } ? (object)serverScope.ToArray() : DBNull.Value,
    };

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = commandTimeoutSeconds };
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string Million(long rows) => (rows / 1_000_000d).ToString("0.#", CultureInfo.InvariantCulture);

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
        /* #6115: a split panel whose wide half counts no server still has a raw half to refuse. */
        if (estimate is not { } found || (found.Servers == 0 && found.RawServers == 0))
        {
            return null;
        }

        var servers = found.Servers + found.RawServers;
        var rows = found.RowsOver(countedStart, countedEnd, stampThrough);
        if (rows <= limit)
        {
            return null;
        }

        if (stampThrough is not null)
        {
            return $"This panel reads too much Query Store history to finish inside the time limit ({Servers(servers)}, {Window(countedEnd - countedStart)}). "
                + "Choose fewer servers or a shorter window.";
        }

        var million = (rows / 1_000_000d).ToString("0.#", CultureInfo.InvariantCulture);
        var limitMillion = (limit / 1_000_000d).ToString("0.#", CultureInfo.InvariantCulture);
        return $"This panel needs about {million} million Query Store rows ({Servers(servers)}, {Window(countedEnd - countedStart)}), "
            + $"over the limit of {limitMillion} million. Choose fewer servers or a shorter window.";
    }

    /// <summary>The guard end to end: estimate, then refuse. Null means the panel runs.</summary>
    internal static async Task<string?> CheckAsync(
        NpgsqlDataSource postgres, IReadOnlyList<string>? serverScope, DateTime countedStart, DateTime countedEnd,
        ILogger? logger, int commandTimeoutSeconds, CancellationToken cancellationToken, long limit = ComposeLimits.MaxQueryStoreWideRows,
        DateTime? stampThrough = null, IReadOnlyList<int>? excludedServerIds = null, DateTime? rawCountedStart = null) =>
        Refusal(await EstimateSplitAsync(postgres, serverScope, countedEnd, logger, excludedServerIds, rawCountedStart ?? countedStart, commandTimeoutSeconds, cancellationToken, countedStart), countedStart, countedEnd, limit, stampThrough);

    /// <summary>The collector the Query Store raw half of a split panel is counted under in the collection log (#6115): the Query Store
    /// collector's own name, whose <c>rows_collected</c> is what it stored into <c>collect.query_store_stats</c>.</summary>
    internal const string QueryStoreRawCollector = "query_store";

    /// <summary>
    /// #6115: <see cref="EstimateAsync"/> for a split panel. The wide estimate leaves out <paramref name="rawServerIds"/> and the raw half's rows
    /// for those servers over <paramref name="rawCountedStart"/>..<paramref name="countedEnd"/> ride in <see cref="Estimate.RawRows"/>. No raw
    /// servers is the plain wide estimate. A raw count that cannot be read adds 0 (fail open).
    /// </summary>
    internal static async Task<Estimate?> EstimateSplitAsync(
        NpgsqlDataSource postgres, IReadOnlyList<string>? serverScope, DateTime countedEnd, ILogger? logger,
        IReadOnlyList<int>? rawServerIds, DateTime rawCountedStart, int commandTimeoutSeconds, CancellationToken cancellationToken,
        DateTime? wideCountedStart = null)
    {
        var wide = await EstimateAsync(postgres, serverScope, logger, commandTimeoutSeconds, cancellationToken, rawServerIds, wideCountedStart, wideCountedStart is null ? null : countedEnd);
        if (wide is not { } found || rawServerIds is not { Count: > 0 })
        {
            return wide;
        }

        var raw = await EstimateRawAsync(postgres, null, serverScope, QueryStoreRawCollector, rawCountedStart, countedEnd, logger, commandTimeoutSeconds, cancellationToken, rawServerIds);
        return raw is { } rawFound ? found with { RawRows = rawFound.Rows, RawServers = rawFound.Servers } : found;
    }

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
