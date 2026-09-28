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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetFactCollector
{
    /// <summary>
    /// #4677: the last <see cref="EvictionFinding.WindowHours"/> hourly windows ending at the analysis window's end,
    /// as the <c>pg_statement_stats</c> collection runs recorded them. Row <c>h</c> is the window
    /// <c>($2 - (h+1) hours, $2 - h hours]</c>; a window with no run carrying <c>statements_dealloc=</c> is absent or
    /// has <c>known = false</c>. <c>evicted</c> is any run with a count above zero; <c>epoch_changed</c> is any run
    /// carrying a statements-epoch change of one or more. The labels come from <see cref="ServerEpoch"/> by
    /// concatenation. <c>$1</c> server_id, <c>$2</c> window end (naive UTC).
    /// </summary>
    public const string StatementsEvictionSql =
        "SELECT floor(extract(epoch FROM ($2::timestamp - collection_time)) / 3600)::int AS h,\n"
        + "       coalesce(bool_or(error_message LIKE '%" + ServerEpoch.StatementsDeallocMeasurement + "=%'), false) AS known,\n"
        + "       coalesce(bool_or(coalesce(substring(error_message FROM '(?:^|[ ;])" + ServerEpoch.StatementsDeallocMeasurement + "=([0-9]+)')::bigint, 0) > 0), false) AS evicted,\n"
        + "       coalesce(bool_or(coalesce(substring(error_message FROM '(?:^|[ ;])" + ServerEpoch.StatementsChangesMeasurement + "=([0-9]+)')::bigint, 0) > 0), false) AS epoch_changed\n"
        + "FROM collection_log\n"
        + "WHERE server_id = $1\n"
        + "AND   collector_name = 'pg_statement_stats'\n"
        + "AND   collection_time >  $2::timestamp - interval '6 hours'\n"
        + "AND   collection_time <= $2::timestamp\n"
        + "GROUP BY 1";

    /// <summary>
    /// #4677: <c>pg_stat_statements.max</c> out of the newest <c>pg_server_config</c> snapshot at or before the window's
    /// end, server-wide row only (the per-database and per-role overrides repeat a setting's name under another scope).
    /// Bounded like the other reads of the table (#3928): the snapshot's <c>MAX(collection_time)</c> and the row scan both
    /// take the lower bound <see cref="ConfigSnapshotLowerBounds"/> hands it. <c>$1</c> server_id, <c>$2</c> window end
    /// (naive UTC), <c>$3</c> lower bound.
    /// </summary>
    public const string StatementsMaxEntriesSql = @"
SELECT c.setting
FROM pg_server_config AS c
WHERE c.server_id = $1
AND   c.collection_time >= $3
AND   c.collection_time = (
          SELECT MAX(collection_time)
          FROM pg_server_config
          WHERE server_id = $1
          AND   collection_time >= $3
          AND   collection_time <= $2)
AND   c.name = 'pg_stat_statements.max'
AND   c.database_name IS NULL
AND   c.role_name IS NULL";

    /// <summary>
    /// #4677: <c>CONFIG_PG_STAT_STATEMENTS_EVICTION</c>. Reads the six hourly windows, hands them to
    /// <see cref="EvictionFinding.Decide"/> and, when it fires, emits one fact whose value is the evicting-hour
    /// count and whose <c>max_entries</c> metadata is the recorded <c>pg_stat_statements.max</c>. A window with no
    /// measure is unknown and never counts as zero.
    /// </summary>
    private async Task ReadStatementsEvictionFactAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            var hours = new EvictionFinding.Hour[EvictionFinding.WindowHours];
            using (var cmd = new NpgsqlCommand(StatementsEvictionSql, connection) { CommandTimeout = FactCommandTimeoutSeconds })
            {
                cmd.Parameters.AddWithValue(context.ServerId);
                cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
                {
                    var h = reader.GetInt32(0);
                    if (h < 0 || h >= hours.Length) continue;
                    hours[h] = new EvictionFinding.Hour(reader.GetBoolean(1), reader.GetBoolean(2), reader.GetBoolean(3));
                }
            }

            var (fire, evicting) = EvictionFinding.Decide(hours);
            if (!fire) return;

            var fact = new Fact
            {
                Source = PgTargetSources.ConfigSource,
                Key = PgTargetFactKeys.ConfigStatStatementsEviction,
                Value = evicting,
                ServerId = context.ServerId,
            };

            /* The day first, every retained snapshot only when the day found no row: the same loop the other reads use. */
            foreach (var lowerBound in ConfigSnapshotLowerBounds(context.TimeRangeEnd))
            {
                string? setting;
                using (var cmd = new NpgsqlCommand(StatementsMaxEntriesSql, connection) { CommandTimeout = FactCommandTimeoutSeconds })
                {
                    cmd.Parameters.AddWithValue(context.ServerId);
                    cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
                    cmd.Parameters.AddWithValue(lowerBound);
                    setting = await cmd.ExecuteScalarAsync(context.CancellationToken) as string;
                }

                if (setting is null) continue;
                if (long.TryParse(setting, NumberStyles.Integer, CultureInfo.InvariantCulture, out var max))
                    fact.Metadata[EvictionFinding.MaxEntriesKey] = max;
                break;
            }

            facts.Add(fact);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            ReportCollectionFailure(ex, context);
        }
    }
}
