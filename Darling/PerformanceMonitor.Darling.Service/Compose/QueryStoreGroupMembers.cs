/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
/// rows, so it is multiplied by <c>pg_class.reltuples</c>. That counts all retention, not the window, so it is too high for a
/// window, which errs toward the two-scan text. No statistics row, or a parent never analyzed, is unknown, and so is any dimension
/// that cannot be bounded this way (<c>query_hash</c>); unknown also compiles two scans.</para>
/// </summary>
internal static class QueryStoreGroupMembers
{
    /// <summary>The table the members are counted on: the wide table, a plain table before V171 and a partitioned parent after it.</summary>
    internal const string WideTable = "query_store_interval_wide";

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
            var product = 1d;
            foreach (var dimension in plan.GroupBy)
            {
                double? members;
                if (dimension.ViaModuleJoin || dimension.TrailingSpaceHistory)
                {
                    return null;
                }

                if (string.Equals(dimension.Name, MeasureCatalog.ServerDimensionName, StringComparison.Ordinal))
                {
                    members = serversInScope;
                }
                else if (string.Equals(dimension.Column, "database_name", StringComparison.Ordinal)
                    || string.Equals(dimension.Column, "module_name", StringComparison.Ordinal))
                {
                    members = await NDistinctMembersAsync(connection, dimension.Column, cancellationToken);
                }
                else
                {
                    return null;
                }

                if (members is not { } known || known < 1d)
                {
                    return null;
                }

                product *= known;
            }

            return (long)Math.Ceiling(Math.Min(product, long.MaxValue / 4d));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ReadScope.Current?.Logger?.LogDebug(ex, "The #5582 Query Store group-member count could not be read; the panel compiles the two-scan text.");
            return null;
        }
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
