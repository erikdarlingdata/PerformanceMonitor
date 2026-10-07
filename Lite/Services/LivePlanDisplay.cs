/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// #4348: the statement filter for the plans a Lite window fetches live from the monitored server and puts on
/// screen or into a saved file. Those plans are not collected, so the collection-time filter never sees them. Every
/// display site that calls one of the live plan fetches (<c>LocalDataService.FetchQueryPlanOnDemandAsync</c>,
/// <c>FetchProcedurePlanOnDemandAsync</c>, <c>FetchQueryStorePlanAsync</c>) passes the result through
/// <see cref="Filter"/> at the call, not inside <c>LocalDataService</c>: the display site is where the plan leaves
/// the app's own code. <c>StatementScrubLivePlanDisplayTests</c> scans every caller.
/// </summary>
internal static class LivePlanDisplay
{
    /// <summary>
    /// The plan with the statements the filter names withheld (their values and literals too); the same instance
    /// when nothing is named; the whole-document marker when the plan cannot be judged. Null and empty pass
    /// through, so a caller's "no plan found" check still works after it.
    /// </summary>
    public static string? Filter(string? planXml) => SensitiveStatements.Xml(planXml);
}
