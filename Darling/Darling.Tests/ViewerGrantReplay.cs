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
using PerformanceMonitor.Darling.Storage;

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
    public static List<string> StatementsFor(string roleName, string scratchDatabase) =>
        StatementsFor(roleName, scratchDatabase, "viewer");

    /// <summary>The same replay for another managed role (<c>admin</c>, the role a desktop viewer seat with
    /// <c>postgres.connectAs = "admin"</c> connects as; #5632 Phase A's investigation writes are its grants).</summary>
    public static List<string> StatementsFor(string roleName, string scratchDatabase, string managedRole)
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
            if (at < 0 || CannotReplayOnScratchStore(statement))
            {
                continue;
            }

            var targets = statement[(at + marker.Length)..].Split(',').Select(t => t.Trim()).ToList();
            if (!targets.Contains(managedRole, StringComparer.Ordinal))
            {
                continue;
            }

            viewerStatements.Add(OnDatabase(statement[..at], scratchDatabase) + marker + roleName);
        }

        return viewerStatements;
    }

    /// <summary>The provisioning batch split into its statements, comment lines dropped first (the prose contains semicolons).</summary>
    internal static List<string> BatchStatements()
    {
        var provisioning = DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);
        var uncommented = string.Join('\n', provisioning.Split('\n').Where(l => !l.TrimStart().StartsWith("--", StringComparison.Ordinal)));
        return uncommented.Split(';').Select(s => s.Trim()).ToList();
    }

    /// <summary>
    /// The statements of the provisioning batch that cannot run on a scratch store: a GRANT of EXECUTE on a function the
    /// batch itself creates later (the scratch store holds the migrations only), and the one REVOKE of the external alert
    /// submission function from the three roles (#5654; the function is created by the batch too), matched as the whole
    /// statement, and the revoke that precedes the grant of the MCP queue function (#6230; the batch creates it too). Every other function statement (any REVOKE, and a GRANT of anything but EXECUTE) is replayed, so one
    /// that is added to the batch is exercised by every live test that replays the grants.
    /// </summary>
    internal static bool CannotReplayOnScratchStore(string statement) =>
        statement.Contains("EXECUTE ON FUNCTION", StringComparison.Ordinal)
        || statement.StartsWith($"REVOKE ALL ON FUNCTION {PgSchemaGenerator.ConfigSchema}.request_mcp_command(", StringComparison.Ordinal)
        || statement.StartsWith($"REVOKE ALL ON FUNCTION {PgSchemaGenerator.ConfigSchema}.read_mcp_command_result(", StringComparison.Ordinal)
        || string.Equals(statement, SubmitFunctionRevoke, StringComparison.Ordinal);

    /// <summary>The batch's REVOKE of the submission function from admin, viewer, web and mcp, as the batch writes it.</summary>
    internal static readonly string SubmitFunctionRevoke =
        $"REVOKE ALL ON FUNCTION {PgSchemaGenerator.ConfigSchema}.submit_external_alert({DarlingManagedRoles.ExternalAlertSubmitSignature}) FROM "
        + string.Join(", ", DarlingManagedPostgres.AdminRoleName, DarlingManagedPostgres.ViewerRoleName, DarlingManagedPostgres.WebRoleName, DarlingManagedPostgres.McpRoleName);

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
