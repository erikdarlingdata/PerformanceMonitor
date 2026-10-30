/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #6651: the words the Viewer shares with the web pages for a few figures whose label the web changed (#5704, #5705). XAML binds them
/// with <c>{x:Static}</c> so the wording is one value a test can read, not a string spread over several files.
/// </summary>
public static class ViewerLabels
{
    /// <summary>W19: the FinOps Utilization CPU figure is SQL Server's own CPU, not the host's total, and says so (the web page's words,
    /// <c>finops/utilization.js</c>).</summary>
    public const string AvgSqlServerCpu = "Avg SQL Server CPU";

    /// <summary>W19: see <see cref="AvgSqlServerCpu"/>.</summary>
    public const string P95SqlServerCpu = "P95 SQL Server CPU";

    /// <summary>W19: see <see cref="AvgSqlServerCpu"/>.</summary>
    public const string MaxSqlServerCpu = "Max SQL Server CPU";

    /// <summary>W19: the grid header of <see cref="AvgSqlServerCpu"/>, which carries the unit.</summary>
    public const string AvgSqlServerCpuPercent = AvgSqlServerCpu + " %";

    /// <summary>W19: see <see cref="AvgSqlServerCpuPercent"/>.</summary>
    public const string P95SqlServerCpuPercent = P95SqlServerCpu + " %";

    /// <summary>W19: see <see cref="AvgSqlServerCpuPercent"/>.</summary>
    public const string MaxSqlServerCpuPercent = MaxSqlServerCpu + " %";

    /// <summary>W19: the Memory tab's plan cache figure is the sum of every cache store, not the plan cache proper (the web page's words,
    /// <c>server-tabs.js</c>).</summary>
    public const string PlanCacheAllStores = "Plan cache (all cache stores)";

    /// <summary>W8: what the Settings window says after the uncorroborated-route choice, in words for a person. The page that shows which
    /// value is in force is Alert Settings; no MCP tool or key name appears here.</summary>
    public const string UncorroboratedRouteNote =
        "Digest and Page take effect in the running service within one collection sweep, no restart, and win over darling.json's analysis.uncorroboratedRoute; "
        + "the third choice clears the store column so the file governs again. The Alert Settings page shows which one is in force.";
}
