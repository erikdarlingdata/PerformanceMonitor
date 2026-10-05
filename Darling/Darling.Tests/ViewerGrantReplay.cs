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
/// </summary>
internal static class ViewerGrantReplay
{
    public static List<string> StatementsFor(string roleName)
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

            viewerStatements.Add(statement[..at] + marker + roleName);
        }

        return viewerStatements;
    }
}
