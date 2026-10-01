/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitorLite.Services;

/// <summary>
/// What the get_running_jobs MCP tool tells the caller about why the running_jobs collector is not running (#2559). The
/// Running Jobs tab passes the same text to the shared wording, so the tab and the tool say the same thing.
/// </summary>
internal static class RunningJobsSkipCauses
{
    internal const string Text =
        "For this collector the gate is: this is an AWS RDS instance, where the Agent job "
        + "tables are not reachable to a monitoring login at all and no grant changes that. "
        + "Since #2559 msdb access is NOT a gate — a login without it now attempts and is "
        + "reported as a permission denial, so the grant takes effect on the next cycle "
        + "rather than the next reconnect.";
}
