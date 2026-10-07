/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// The pair that identifies one assumed AWS role (#5452): the role ARN and the external ID sent with it, if any. Two
/// servers with the same pair share one assumed-role session. A record struct, so equality is by value and ordinal:
/// an ARN and an external ID are case-sensitive.
///
/// <para>The external ID is a secret. The compiler-written <c>ToString()</c> would print every member, so
/// <see cref="PrintMembers"/> is written by hand: it prints the role ARN and <c>ExternalId = set</c> or
/// <c>ExternalId = none</c>, never the value. Anything that formats a key (a log line, an exception, a debugger
/// watch) is safe by construction.</para>
/// </summary>
public readonly record struct AwsRoleKey(string RoleArn, string? ExternalId)
{
    /// <summary>True when an external ID is set. A null or empty one means none, and none is sent with the request.</summary>
    public bool HasExternalId => !string.IsNullOrEmpty(ExternalId);

    private bool PrintMembers(StringBuilder builder)
    {
        builder.Append("RoleArn = ").Append(RoleArn).Append(", ExternalId = ").Append(HasExternalId ? "set" : "none");
        return true;
    }
}
