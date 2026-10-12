/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

public partial class ViewerServerTab
{
    /// <summary>
    /// Starts the "where does this grid's data start" probe for a PostgreSQL collector (#4966), beside the grid's read. The
    /// caller awaits it after the grid is drawn, through <see cref="DataStartOrNullAsync"/> or <see cref="ShowEventDataStartAsync"/>,
    /// and never inside the read's <c>Task.WhenAll</c>: a probe that throws then costs the banner and not the rows.
    ///
    /// <para>Returns a completed null for a window no longer than <see cref="DurationTrendRouting.TruncationSlack"/> (90 minutes):
    /// no banner can show for one, so it runs no query. Takes the collector's name and reads its table from
    /// <see cref="CollectorCatalog.All"/>, because the two differ (<c>pg_blocking</c> writes <c>pg_blocking_edges</c>).</para>
    /// </summary>
    private Task<DateTime?> StartPgDataStartProbe(string collectorName, DateTime startUtc, DateTime endUtc)
    {
        if (endUtc - startUtc <= DurationTrendRouting.TruncationSlack)
        {
            return Task.FromResult<DateTime?>(null);
        }

        var collector = CollectorCatalog.All.FirstOrDefault(c => string.Equals(c.Name, collectorName, StringComparison.Ordinal));
        return collector is null
            ? Task.FromResult<DateTime?>(null)
            : _dataService.GetPgDataStartAsync(collector.TargetTable, _server.ServerId, startUtc, endUtc);
    }
}
