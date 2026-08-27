/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The store half of the #2511 engine-capability answer, Lite's side: reads this server's probed engine
/// edition and, when the collector serving a read cannot run on that engine, returns the
/// <c>not_collected</c> envelope. The DECISION and the WORDS both come from
/// <see cref="CollectorEngineCapability"/>, so this file and its Darling twin
/// (<c>DarlingEngineCapability</c>) hold no capability knowledge and no message text of their own — that is
/// what makes the two SKUs byte-identical here by construction rather than by pinning.
///
/// <para><b>Why <c>not_collected</c> rather than <c>unavailable</c>.</b> <c>not_collected</c> means "the
/// input names something this server does not collect", which is exactly true and final for an engine gap.
/// <c>unavailable</c> means "supported here, just not retrievable now", and sends an operator hunting for a
/// collector to restart — the defect #2511 was filed about, not the fix for it.</para>
///
/// <para><b>Called only on the miss path</b>, never before the read: a server whose registry row says one
/// engine while its collected rows say another still gets its DATA rather than a confident explanation of
/// why it cannot have any.</para>
/// </summary>
internal static class McpEngineCapability
{
    /// <summary>
    /// The <c>not_collected</c> envelope when <paramref name="collectorName"/> cannot run on this server's
    /// engine, or <c>null</c> when it can — in which case the caller falls through to its own
    /// <c>empty</c>/<c>unavailable</c> miss, unchanged.
    ///
    /// <para>A store read that FAILS answers null, deliberately: this runs on a path that has already found
    /// no data, and turning a capability probe into a read error would replace one honest miss with a worse
    /// one.</para>
    /// </summary>
    public static async Task<string?> NotCollectedStatusAsync(
        LocalDataService dataService,
        int serverId,
        string serverName,
        string collectorName)
    {
        int engineEdition;
        try
        {
            engineEdition = await dataService.GetSqlEngineEditionAsync(serverId);
        }
        catch (Exception)
        {
            return null;
        }

        /* engineKind: null — "make no claim on the engine-KIND axis" (#2530). Lite has no PostgreSQL target
           seam at all: nothing in this SKU ever sets CollectorTargetEngine.PostgreSql, its DuckDB schema
           generator emits SQL Server definitions only, and its servers table has no engine_kind column to
           read. Passing the constant MonitoredEngineKind.SqlServer would be true today and would be a lie
           the compiler could not find on the day that changes; null is behaviour-identical here (a SQL
           Server collector on a SQL Server target makes no kind claim either way) and cannot become wrong.
           The parameter is REQUIRED rather than defaulted so that decision is visible at the call site
           instead of being made by omission. */
        var message = CollectorEngineCapability.NotCollectedMessage(serverName, engineEdition, engineKind: null, collectorName);
        return message is null ? null : McpHelpers.Status("not_collected", message);
    }
}
