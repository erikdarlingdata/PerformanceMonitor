/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;

namespace Darling.Tests;

/// <summary>#6600: which login the managed-shaped web live tests give the dashboard's admin arm. <c>None</c> builds no arm (the switch off, or a
/// store you bring yourself), <c>Admin</c> an arm on a role that holds what the managed <c>admin</c> role holds, and <c>Refused</c> an arm whose
/// login the store rejects, so a request that opened it would fail.</summary>
internal enum AdminArmShape { None, Admin, Refused }

/// <summary>#6600: the arm a web live harness builds beside its read-only web login, and the cleanup that drops the arm's role. The web logs in
/// as the role the test names (the managed <c>viewer</c>); the arm is a second role named <c>&lt;web role&gt;_arm</c>.</summary>
internal static class AdminArmLive
{
    private const string ArmPassword = "admin-arm-live-pw";

    internal static async Task<WebAdminArm?> BuildAsync(
        NpgsqlDataSource owner, ScratchPostgres scratch, string webRole, AdminArmShape shape, CancellationToken ct)
    {
        if (shape == AdminArmShape.None)
        {
            return null;
        }

        var armRole = webRole + "_arm";
        await ExecAsync(owner, $"CREATE ROLE {armRole} LOGIN NOSUPERUSER PASSWORD '{ArmPassword}'", ct);
        foreach (var statement in ViewerGrantReplay.StatementsFor(armRole, scratch.DatabaseName, "admin"))
        {
            await ExecAsync(owner, statement, ct);
        }

        var armConnection = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            Username = armRole,
            Password = shape == AdminArmShape.Refused ? "not-the-password" : ArmPassword,
            SearchPath = DarlingManagedPostgres.SearchPath,
            Pooling = false,
        }.ConnectionString;
        return new WebAdminArm(() => armConnection);
    }

    /// <summary>The statements that drop the web role and, when one was built, the arm's role.</summary>
    internal static string DropRolesSql(string webRole) =>
        $"DROP OWNED BY {webRole}; DROP ROLE IF EXISTS {webRole}; " +
        $"DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{webRole}_arm') THEN DROP OWNED BY {webRole}_arm; DROP ROLE {webRole}_arm; END IF; END $$";

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }
}
