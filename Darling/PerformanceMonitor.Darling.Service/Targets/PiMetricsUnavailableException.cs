/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// The AWS RDS/Aurora API calls behind the CPU collector (#2719) — <c>rds:DescribeDBInstances</c>,
/// <c>rds:DescribeDBClusters</c>, <c>pi:GetResourceMetrics</c> — could not be completed, as distinct from
/// completing and finding nothing. A separate type from <see cref="RdsLogUnavailableException"/> rather than
/// a shared one carrying a different message: the two APIs need different IAM grants, and
/// <c>DarlingWorker</c>'s catch handler names the grant in its PERMISSIONS log line, so conflating them would
/// tell an operator to add the wrong policy statement. The authorization-refusal matching itself is NOT
/// duplicated — it delegates to <see cref="RdsLogUnavailableException.IsAuthorizationRefusal"/>, which is
/// already engine/API-agnostic (it matches on the SDK's own denial message shapes, not on which call was
/// denied).
/// </summary>
public sealed class PiMetricsUnavailableException : Exception
{
    public PiMetricsUnavailableException(string message, bool isAuthorizationFailure, Exception innerException)
        : base(message, innerException)
        => IsAuthorizationFailure = isAuthorizationFailure;

    /// <summary>Only this case degrades to PERMISSIONS; see <see cref="RdsLogUnavailableException.IsAuthorizationFailure"/>
    /// for the identical reasoning — a throttle, a failover, or a resolution failure stays loud.</summary>
    public bool IsAuthorizationFailure { get; }
}
