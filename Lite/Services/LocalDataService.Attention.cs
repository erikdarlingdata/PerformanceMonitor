/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// The Client Timeouts alert's window read (#5640), the DuckDB twin of <c>DarlingAlertReadAdapter.GetAttentionWindowAsync</c>:
    /// the same five statements from the one shared <see cref="AttentionWindowSql"/> text over <c>v_collection_log</c> and
    /// <c>v_attention_events</c> (the archive views, so a floor that reaches into parquet still reads). $1 server_id, $2 the
    /// naive-UTC floor, bound as a full-precision <see cref="DateTime"/> like the Runnable Queue read. Dumb by contract: no
    /// span, threshold or rule is applied here.
    /// </summary>
    public async Task<AttentionWindow> GetAttentionWindowAsync(int serverId, int lookbackMinutes)
    {
        using var connection = await OpenConnectionAsync();
        var floor = DateTime.UtcNow.AddMinutes(-lookbackMinutes);

        var runs = await QueryAttentionAsync(connection, AttentionWindowSql.Runs("v_collection_log"), serverId, floor,
            r => r.GetDateTime(0));
        var counts = await QueryAttentionAsync(connection, AttentionWindowSql.Counts("v_attention_events"), serverId, floor,
            r => new AttentionCountRow(r.GetDateTime(0), r.GetInt64(1)));
        var groups = await QueryAttentionAsync(connection, AttentionWindowSql.SampleGroups("v_attention_events"), serverId, floor,
            r => new AttentionSampleGroup(
                r.GetDateTime(0),
                r.IsDBNull(1) ? null : r.GetString(1),
                r.IsDBNull(2) ? null : r.GetString(2),
                r.GetInt64(3)));
        /* Round 2 of #5714 (L6): the card's top lists cover the samples inside the covered span, not the whole lookback (the span is
           a few minutes; the lookback an hour). A span that kept no samples falls back to the lookback and says so. */
        var (topFloor, topsFromLookback) = AttentionEvaluator.TopListFloor(runs, groups, floor);
        /* Final round of #5714 (L2): the lists are bounded like the count is, > floor AND <= newest + RowSkewSeconds (the MCP read's cut). */
        var topCeiling = AttentionEvaluator.TopListCeiling(runs);
        var statements = await QueryAttentionAsync(connection, AttentionWindowSql.TopStatements("v_attention_events", bounded: true), serverId, topFloor, topCeiling,
            r => new AttentionTopItem(r.GetString(0), r.GetInt64(1), r.IsDBNull(2) ? null : r.GetString(2)));
        var databases = await QueryAttentionAsync(connection, AttentionWindowSql.TopBy("v_attention_events", "database_name", bounded: true), serverId, topFloor, topCeiling,
            r => new AttentionTopItem(r.GetString(0), r.GetInt64(1)));
        var applications = await QueryAttentionAsync(connection, AttentionWindowSql.TopBy("v_attention_events", "client_app_name", bounded: true), serverId, topFloor, topCeiling,
            r => new AttentionTopItem(r.GetString(0), r.GetInt64(1)));

        var hosts = await QueryAttentionAsync(connection, AttentionWindowSql.TopBy("v_attention_events", "client_hostname", bounded: true), serverId, topFloor, topCeiling,
            r => new AttentionTopItem(r.GetString(0), r.GetInt64(1)));

        return new AttentionWindow(runs, counts, groups, statements, databases, applications, topsFromLookback, hosts);
    }

    /// <summary>
    /// The <c>get_client_timeouts</c> read (#5640): the same six shared statements as <see cref="GetAttentionWindowAsync"/> with the
    /// ceiling on (<c>$3</c> = <paramref name="windowEnd"/>, naive UTC, so an as_of read does not count what came after it), plus the
    /// newest <paramref name="sampleLimit"/> sampled events (one more is fetched, as the truncation signal). The runs, counts and sample
    /// groups are read first; the top lists and the sampled events then cover exactly the span the evaluator counts
    /// (<see cref="ClientTimeoutsRead.DetailSpan"/>), so they never hold rows outside it. Dumb by contract:
    /// <see cref="ClientTimeoutsRead"/> judges.
    /// </summary>
    public async Task<(AttentionWindow Window, List<ClientTimeoutSample> Samples)> GetAttentionRangeAsync(
        int serverId, DateTime windowStart, DateTime windowEnd, int sampleLimit)
    {
        using var connection = await OpenConnectionAsync();

        /* The collection log stamps a run when it starts; its rows carry the time the runner took a moment later. A range that ends
           between the two must still count that run's rows, the allowance AttentionEvaluator.ReadSpan itself makes (RowSkewSeconds).
           Only the run list stops at the range end; the rows get the skew past it, and ReadSpan cuts them at the newest run + skew. */
        var rowCeiling = windowEnd.AddSeconds(AttentionEvaluator.RowSkewSeconds);

        var runs = await QueryAttentionAsync(connection, AttentionWindowSql.Runs("v_collection_log", bounded: true), serverId, windowStart, windowEnd,
            r => r.GetDateTime(0));
        var counts = await QueryAttentionAsync(connection, AttentionWindowSql.Counts("v_attention_events", bounded: true), serverId, windowStart, rowCeiling,
            r => new AttentionCountRow(r.GetDateTime(0), r.GetInt64(1)));
        var groups = await QueryAttentionAsync(connection, AttentionWindowSql.SampleGroups("v_attention_events", bounded: true), serverId, windowStart, rowCeiling,
            r => new AttentionSampleGroup(
                r.GetDateTime(0),
                r.IsDBNull(1) ? null : r.GetString(1),
                r.IsDBNull(2) ? null : r.GetString(2),
                r.GetInt64(3)));
        /* The top lists and the sampled events cover exactly the rows ReadSpan counts (#5640): the span boundary (exclusive) to the
           newest run plus the skew, never the base run's samples from before the span. No span (fewer than two runs) is "not
           observed", whose answer never shows the detail, so it is not read. */
        var span = ClientTimeoutsRead.DetailSpan(runs);
        if (span is null)
        {
            return (new AttentionWindow(runs, counts, groups, [], [], []), []);
        }

        var (spanFloor, spanCeiling) = span.Value;
        var statements = await QueryAttentionAsync(connection, AttentionWindowSql.TopStatements("v_attention_events", bounded: true), serverId, spanFloor, spanCeiling,
            r => new AttentionTopItem(r.GetString(0), r.GetInt64(1), r.IsDBNull(2) ? null : r.GetString(2)));
        var databases = await QueryAttentionAsync(connection, AttentionWindowSql.TopBy("v_attention_events", "database_name", bounded: true), serverId, spanFloor, spanCeiling,
            r => new AttentionTopItem(r.GetString(0), r.GetInt64(1)));
        var applications = await QueryAttentionAsync(connection, AttentionWindowSql.TopBy("v_attention_events", "client_app_name", bounded: true), serverId, spanFloor, spanCeiling,
            r => new AttentionTopItem(r.GetString(0), r.GetInt64(1)));
        var hosts = await QueryAttentionAsync(connection, AttentionWindowSql.TopBy("v_attention_events", "client_hostname", bounded: true), serverId, spanFloor, spanCeiling,
            r => new AttentionTopItem(r.GetString(0), r.GetInt64(1)));
        var samples = await QueryAttentionAsync(connection, AttentionWindowSql.Samples("v_attention_events", sampleLimit + 1), serverId, spanFloor, spanCeiling,
            r => new ClientTimeoutSample(
                r.IsDBNull(0) ? null : r.GetDateTime(0),
                r.GetDateTime(1),
                r.IsDBNull(2) ? null : r.GetString(2),
                r.IsDBNull(3) ? null : r.GetString(3),
                r.IsDBNull(4) ? null : r.GetString(4),
                r.IsDBNull(5) ? null : Convert.ToInt32(r.GetValue(5), System.Globalization.CultureInfo.InvariantCulture),
                r.IsDBNull(6) ? null : Convert.ToInt64(r.GetValue(6), System.Globalization.CultureInfo.InvariantCulture),
                r.IsDBNull(7) ? null : r.GetString(7),
                r.IsDBNull(8) ? null : r.GetString(8)));

        return (new AttentionWindow(runs, counts, groups, statements, databases, applications, TopHosts: hosts), samples);
    }

    /// <summary>
    /// The server's Client Timeouts baseline (#5736), the DuckDB twin of <c>DarlingAlertReadAdapter.GetAttentionBaselineAsync</c>: the same
    /// per-minute bucket statement over <c>v_collection_log</c> and <c>v_attention_events</c> (the archive views, so the 7 days read through
    /// parquet when the live table holds fewer), then the shared <see cref="AttentionBaselineMath.Compute"/>.
    /// </summary>
    public async Task<AttentionBaseline?> GetAttentionBaselineAsync(int serverId, DateTime nowUtc)
    {
        using var connection = await OpenConnectionAsync();
        var from = nowUtc.AddDays(-AttentionBaselineMath.LookbackDays);
        var minutes = await QueryAttentionAsync(connection, AttentionWindowSql.BaselineMinutes("v_collection_log", "v_attention_events"), serverId, from, nowUtc,
            r => new AttentionMinute(r.GetDateTime(0), r.GetInt64(1)));
        return AttentionBaselineMath.Compute(minutes);
    }

    private static async Task<List<T>> QueryAttentionAsync<T>(
        PerformanceMonitorLite.Database.LockedConnection connection, string sql, int serverId, DateTime floor, DateTime ceiling, Func<DbDataReader, T> map)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = floor });
        command.Parameters.Add(new DuckDBParameter { Value = ceiling });

        var rows = new List<T>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(map(reader));
        }

        return rows;
    }

    private static async Task<List<T>> QueryAttentionAsync<T>(
        PerformanceMonitorLite.Database.LockedConnection connection, string sql, int serverId, DateTime floor, Func<DbDataReader, T> map)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = floor });

        var rows = new List<T>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(map(reader));
        }

        return rows;
    }
}
