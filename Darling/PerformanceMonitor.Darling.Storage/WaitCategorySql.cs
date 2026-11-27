/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>#9013: the wait-type to cost-category CASE, in one place. FinOps' wait-category summary and the metrics
/// port's wait statement both build their CASE here, so a wait type cannot land in one category on the FinOps tab and
/// another on the scrape.</summary>
public static class WaitCategorySql
{
    /// <summary>The categories <see cref="CaseExpression"/> can return, in the order the CASE tests them.</summary>
    public static readonly IReadOnlyList<string> Categories =
        new[] { "CPU", "Parallelism", "Storage", "Memory", "Network", "Locks", "Latches", "Other" };

    /// <summary>The CASE expression over <paramref name="waitTypeColumn"/>, which must already be rtrimmed (the
    /// collector trimmed the trailing space from #4884 on, and the callers merge both spellings first).</summary>
    public static string CaseExpression(string waitTypeColumn) => $@"CASE
            WHEN {waitTypeColumn} = 'SOS_SCHEDULER_YIELD' THEN 'CPU'
            WHEN {waitTypeColumn} IN ('CXPACKET', 'CXCONSUMER', 'CXSYNC_PORT', 'CXSYNC_CONSUMER', 'HTDELETE', 'HTBUILD', 'HTREPARTITION') THEN 'Parallelism'
            WHEN {waitTypeColumn} ILIKE 'PAGEIOLATCH%'
            OR   {waitTypeColumn} IN ('WRITELOG', 'IO_COMPLETION', 'ASYNC_IO_COMPLETION') THEN 'Storage'
            WHEN {waitTypeColumn} IN ('RESOURCE_SEMAPHORE', 'RESOURCE_SEMAPHORE_QUERY_COMPILE', 'CMEMTHREAD') THEN 'Memory'
            WHEN {waitTypeColumn} = 'ASYNC_NETWORK_IO' THEN 'Network'
            WHEN {waitTypeColumn} ILIKE 'LCK_M_%' THEN 'Locks'
            WHEN {waitTypeColumn} ILIKE 'LATCH_%'
            OR   {waitTypeColumn} ILIKE 'PAGELATCH_%' THEN 'Latches'
            ELSE 'Other'
        END";
}
