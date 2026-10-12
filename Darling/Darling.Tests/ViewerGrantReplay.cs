/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Darling.Service;

namespace Darling.Tests;

/// <summary>
/// The viewer role's provisioned grants, taken from the product's <see cref="DarlingManagedRoles.BuildProvisioningSql"/>
/// and retargeted at a scratch role (#5137 review L3, shared with #5239): every GRANT / REVOKE that names
/// <c>viewer</c>, in order, so a live test runs as a role holding exactly what a deployment's viewer holds.
/// <para>The provisioning batch names the store's database (<c>darling</c> on a managed store), and a replay that kept
/// that name would rewrite the CONNECT ACL of the shared cluster's <c>darling</c> database from every test that
/// replays: two sessions rewriting one catalog row at once make the second fail with XX000 "tuple concurrently
/// updated" (#5560). So every replay is pointed at the test's own scratch database, through
/// <see cref="OnDatabase"/>.</para>
/// </summary>
internal static class ViewerGrantReplay
{
    public static List<string> StatementsFor(string roleName, string scratchDatabase)
    {
        var provisioning = DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);
        var viewerStatements = new List<string>();
        /* Drop the comment lines BEFORE splitting: the provisioning prose contains semicolons. */
        var uncommented = string.Join('\n', provisioning.Split('\n').Where(l => !l.TrimStart().StartsWith("--", StringComparison.Ordinal)));
        foreach (var raw in uncommented.Split(';'))
        {
            var statement = raw.Trim();
            var isGrant = statement.StartsWith("GRANT ", StringComparison.Ordinal);
            if (!isGrant && !statement.StartsWith("REVOKE ", StringComparison.Ordinal))
            {
                continue;
            }

            var marker = isGrant ? " TO " : " FROM ";
            var at = statement.LastIndexOf(marker, StringComparison.Ordinal);
            if (at < 0 || statement.Contains("EXECUTE ON FUNCTION", StringComparison.Ordinal))
            {
                continue;
            }

            var targets = statement[(at + marker.Length)..].Split(',').Select(t => t.Trim()).ToList();
            if (!targets.Contains("viewer", StringComparer.Ordinal))
            {
                continue;
            }

            viewerStatements.Add(OnDatabase(statement[..at], scratchDatabase) + marker + roleName);
        }

        return viewerStatements;
    }

    /// <summary>
    /// A replayed GRANT / REVOKE head (everything before <c>TO</c> / <c>FROM</c>), with the managed store's database
    /// name swapped for <paramref name="scratchDatabase"/> when it is a database-level privilege. Other statements
    /// come back unchanged: they name schemas and tables, which live in the scratch database already.
    /// </summary>
    internal static string OnDatabase(string statementHead, string scratchDatabase)
    {
        var shared = ProvisioningTarget.Managed.DatabaseIdentifier;
        return statementHead.EndsWith(" ON DATABASE " + shared, StringComparison.Ordinal)
            ? statementHead[..^shared.Length] + "\"" + scratchDatabase.Replace("\"", "\"\"", StringComparison.Ordinal) + "\""
            : statementHead;
    }
}
