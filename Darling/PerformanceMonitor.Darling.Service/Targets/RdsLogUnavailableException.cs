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
/// The RDS log could not be READ — as distinct from being read and holding nothing (#2633).
///
/// <para>
/// This type exists because those two states were the same value. <c>IngestAsync</c> caught every failure,
/// logged a warning and returned <c>0</c> rows, and the runner turned <c>0</c> into a
/// <c>collection_log</c> row reading <b>SUCCESS</b> with the note "no new auto_explain plans in the RDS log
/// window" — a positive claim that the log was opened and was empty. On the monitoring host the truth was
/// <c>rds:DescribeDBLogFiles</c> denied by IAM: nothing was opened at all.
/// </para>
///
/// <para>
/// That is a REGRESSION against the route this one replaced. The <c>pg_read_file</c> path answers the same
/// situation with <c>PERMISSIONS</c> and a message naming the grant; the managed path, which is the one a
/// real fleet is on, was the one that went quiet. And the app-log warning is not a substitute:
/// <c>collection_log</c> is where collection health is read, and it was saying this collector was fine.
/// </para>
///
/// <para>
/// Tolerating the failure stays right — one target's IAM gap must not take the cycle down. What changes is
/// that the cycle now says which kind of nothing it found.
/// </para>
/// </summary>
public sealed class RdsLogUnavailableException : Exception
{
    public RdsLogUnavailableException(string message, bool isAuthorizationFailure, Exception innerException)
        : base(message, innerException)
        => IsAuthorizationFailure = isAuthorizationFailure;

    /// <summary>
    /// The call was DENIED rather than failing for some other reason.
    ///
    /// <para>Only this case degrades to <c>PERMISSIONS</c>. Everything else — a throttle, a failover, an
    /// endpoint that stopped resolving — stays loud, because the store's own rule is that an unclassified
    /// failure must be loud rather than quietly swallowed, and a permanent-sounding status on a transient
    /// fault is how a real outage gets read as a configuration choice.</para>
    /// </summary>
    public bool IsAuthorizationFailure { get; }

    /// <summary>
    /// Whether an exception from the AWS SDK is an authorization refusal.
    ///
    /// <para>Matched on the MESSAGE as well as the type, because the SDK reports this in more than one
    /// shape depending on the call: an <c>AmazonServiceException</c> carrying an
    /// <c>AccessDenied</c>/<c>AccessDeniedException</c> error code, and — as measured on the fleet — a
    /// plain "User: … is not authorized to perform: rds:DescribeDBLogFiles … because no identity-based
    /// policy allows" message. Deliberately not matched on the HTTP status alone: 403 also covers an
    /// expired credential, which is a different thing to tell an operator.</para>
    /// </summary>
    public static bool IsAuthorizationRefusal(Exception ex)
    {
        ArgumentNullException.ThrowIfNull(ex);

        for (var current = ex; current is not null; current = current.InnerException)
        {
            var message = current.Message ?? string.Empty;

            if (message.Contains("is not authorized to perform", StringComparison.OrdinalIgnoreCase)
                || message.Contains("no identity-based policy allows", StringComparison.OrdinalIgnoreCase)
                || message.Contains("AccessDenied", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }
}
