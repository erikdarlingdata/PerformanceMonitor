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
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #5582: how many members the group dimension of a Query Store RankedTimeSeries panel has, so the compiler can bound the
/// single-scan base CTE (buckets x members rows, at about 436 bytes of temp file each) before it picks that shape
/// (<see cref="ComposeLimits.MaxSingleScanBaseRows"/>). One lookup, on the connection the wide-table eligibility check already holds.
///
/// <para>For <c>server</c> the count is exact: the servers in scope. For <c>database_name</c> and <c>module_name</c> it is
/// <c>pg_stats.n_distinct</c> of the wide parent (<c>inherited = true</c> when the parent is partitioned, since a partitioned
/// parent holds no rows of its own and its statistics are the inherited ones). A negative <c>n_distinct</c> is a fraction of the
/// rows, so it is multiplied by <c>pg_class.reltuples</c>. That counts all retention, not the window, which would err high; but
/// <c>n_distinct</c> comes from a sample of the table, and for a column with a long tail of rare values a sample misses most of
/// the tail, so the estimate runs LOW, and for a window it can run low by more than retention runs high. A low guess picks the
/// single scan on a panel whose hash aggregate can pass the 1 GB <c>temp_file_limit</c> and fail it. Each <c>n_distinct</c>-derived
/// count is therefore multiplied by <see cref="NDistinctSafetyFactor"/> before the product. No statistics row, or a parent never
/// analyzed, is unknown, and so is any dimension that cannot be bounded this way (<c>query_hash</c>); unknown compiles two scans.</para>
///
/// <para>Why 4: <see cref="ComposeLimits.MaxSingleScanBaseRows"/> is 1.0 M rows at about 436 bytes each, about 436 MB, which sits
/// about 2.4x under the 1 GB temp limit. A factor of 4 on the member guess lifts a guess that is up to about 9x low (4 x 2.4)
/// above the limit's reach, and the choice stays safe. Field numbers on a large store (78.56 M rows, one day of 8.71 M rows):
/// <c>module_name</c> estimated 638 against 1,113 real for the day (1.7x low); <c>query_hash</c> estimated 12,186 against
/// 91,937 (7.5x low; not used here, it is already unknown); <c>database_name</c> estimated 132, and the
/// <c>(database_name, module_name)</c> pair 52,970 real against a product of estimates of 84,216. The server count is exact and
/// takes no factor.</para>
/// </summary>
internal static class QueryStoreGroupMembers
{
    /// <summary>The table the members are counted on: the wide table, a plain table before V171 and a partitioned parent after it.</summary>
    internal const string WideTable = "query_store_interval_wide";

    /// <summary>#5582: the multiplier on every member count that comes from <c>pg_stats.n_distinct</c>, per dimension, before the product.
    /// A sampled <c>n_distinct</c> runs low for a long-tailed column (1.7x low in the field for <c>module_name</c>); 4 keeps the choice safe
    /// to an estimate about 9x low. See the type summary.</summary>
    internal const double NDistinctSafetyFactor = 4d;

    /// <summary>One group dimension's member count before the factor: <see cref="Value"/> is null when unknown, and
    /// <see cref="FromNDistinct"/> says it is a <c>pg_stats</c> estimate (factored) rather than exact (the server count).</summary>
    internal readonly record struct MemberCount(double? Value, bool FromNDistinct);

    /// <summary>The <c>n_distinct</c> of column <c>$1</c> on table <c>$2</c> of the <c>collect</c> schema, and that table's <c>reltuples</c>.
    /// No row: no statistics. The <c>inherited</c> flag follows the table's kind: true for a partitioned parent, false for a plain table.</summary>
    internal const string NDistinctSql = @"
SELECT s.n_distinct::double precision, c.reltuples::double precision
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_stats AS s
  ON s.schemaname = n.nspname
 AND s.tablename = c.relname
 AND s.attname = $1
 AND s.inherited = (c.relkind = 'p')
WHERE n.nspname = 'collect'
  AND c.relname = $2;";

    /// <summary>
    /// The members of the plan's group dimensions (the product when there are several), or null when unknown. Never throws except for
    /// cancellation: a fault leaves it unknown, which compiles the two-scan text.
    /// </summary>
    internal static async Task<long?> ResolveAsync(
        NpgsqlConnection connection, PanelPlan plan, int serversInScope, CancellationToken cancellationToken)
    {
        if (plan.Mode != PanelMode.RankedTimeSeries)
        {
            return null;
        }

        try
        {
            var counts = new List<MemberCount>();
            foreach (var dimension in plan.GroupBy)
            {
                if (dimension.ViaModuleJoin || dimension.TrailingSpaceHistory)
                {
                    return null;
                }

                if (string.Equals(dimension.Name, MeasureCatalog.ServerDimensionName, StringComparison.Ordinal))
                {
                    counts.Add(new MemberCount(serversInScope, false));
                }
                else if (string.Equals(dimension.Column, "database_name", StringComparison.Ordinal)
                    || string.Equals(dimension.Column, "module_name", StringComparison.Ordinal))
                {
                    counts.Add(new MemberCount(await NDistinctMembersAsync(connection, dimension.Column, cancellationToken), true));
                }
                else
                {
                    return null;
                }
            }

            return Combine(counts);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ReadScope.Current?.Logger?.LogDebug(ex, "The #5582 Query Store group-member count could not be read; the panel compiles the two-scan text.");
            return null;
        }
    }

    /// <summary>
    /// The product of the dimensions' member counts, each <c>n_distinct</c>-derived one multiplied by <see cref="NDistinctSafetyFactor"/>
    /// first, or null when any is unknown (null or under one). No store needed.
    /// </summary>
    internal static long? Combine(IEnumerable<MemberCount> counts)
    {
        var product = 1d;
        foreach (var count in counts)
        {
            if (count.Value is not { } known || known < 1d)
            {
                return null;
            }

            product *= count.FromNDistinct ? known * NDistinctSafetyFactor : known;
        }

        return (long)Math.Ceiling(Math.Min(product, long.MaxValue / 4d));
    }

    /// <summary>One column's member count from <c>pg_stats</c>, or null when there is no statistics row or no usable <c>reltuples</c>.</summary>
    internal static async Task<double?> NDistinctMembersAsync(NpgsqlConnection connection, string column, CancellationToken cancellationToken, string table = WideTable)
    {
        await using var command = new NpgsqlCommand(NDistinctSql, connection) { CommandTimeout = McpCommandDeadlines.ReadSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Name, Value = column });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Name, Value = table });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken) || reader.IsDBNull(0))
        {
            return null;
        }

        var nDistinct = reader.GetDouble(0);
        if (nDistinct >= 0d)
        {
            return nDistinct;
        }

        var reltuples = reader.IsDBNull(1) ? -1d : reader.GetDouble(1);
        return reltuples > 0d ? -nDistinct * reltuples : null;
    }
}
