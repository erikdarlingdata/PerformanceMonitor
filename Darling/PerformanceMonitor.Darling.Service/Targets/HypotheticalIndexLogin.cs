/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Npgsql;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// The login the PostgreSQL hypothetical-index check runs under (#6170), and the connection it signs in with.
///
/// <para>The check plans statements taken from <c>pg_stat_statements</c>. Planning can evaluate a function a database role created
/// when its arguments are constants, under the roles of the login that plans. The monitoring login usually holds
/// <c>pg_monitor</c> or <c>pg_read_all_stats</c>, so the check does not use it. It uses this login, set up the way the remediation
/// login is: a login name and an <c>env:</c> or <c>file:</c> reference to its password, per server. With neither set, the check is
/// refused and says what to set up. There is no fallback to the monitoring login.</para>
/// </summary>
internal static class HypotheticalIndexLogin
{
    /// <summary>What a DBA sees in <c>pg_stat_activity.application_name</c> for the check's connection.</summary>
    public const string ApplicationName = "PerformanceMonitorDarling-HypotheticalIndex";

    /// <summary>The grants the login needs, as one sentence the refusals and the docs share.</summary>
    public const string GrantsText =
        "CONNECT on the database, USAGE on the schemas of the statements to test (and on the schema hypopg is installed in), "
        + "and SELECT on the tables those statements read. It needs no write grants and no pg_monitor, pg_read_all_stats, "
        + "pg_read_all_data or superuser role.";

    /// <summary>
    /// The connection string for the check, or the one sentence that says why there is none. The host, port, database and TLS settings are the
    /// monitoring connection's; the login and password are this login's. The password reference is read here, on the Darling host, and
    /// is never put in a message.
    /// </summary>
    public static (string? ConnectionString, string? Refusal) Resolve(
        string displayName, string? userName, string? passwordReference, string monitoringConnectionString)
    {
        if (string.IsNullOrWhiteSpace(userName) || string.IsNullOrWhiteSpace(passwordReference))
        {
            return (null,
                $"The hypothetical-index check needs its own PostgreSQL login on '{displayName}', and none is set. The check never runs under the "
                + "monitoring login. Create a login with " + GrantsText + " Then set hypotheticalIndexUsername and "
                + "hypotheticalIndexEncryptedPassword for this server: the login name, and an env:NAME or file:path reference to its password.");
        }

        if (!DarlingSecretSource.IsReference(passwordReference))
        {
            return (null,
                $"The hypothetical-index login on '{displayName}' has a stored password. The check uses an env:NAME or file:path reference to the "
                + "password only, and never a stored password. Set hypotheticalIndexEncryptedPassword to a reference.");
        }

        string password;
        try
        {
            password = DarlingSecretSource.Resolve(passwordReference, "hypotheticalIndexEncryptedPassword");
        }
        catch (Exception ex) when (ex is InvalidOperationException or ArgumentException)
        {
            /* The exception text can carry the file path, and a path is as private as the reference. */
            return (null,
                $"The password reference for the hypothetical-index login on '{displayName}' could not be read: the environment variable is "
                + "not set or the file is missing, unreadable or empty.");
        }

        var builder = new NpgsqlConnectionStringBuilder(monitoringConnectionString)
        {
            Username = userName.Trim(),
            Password = password.Trim(),
            ApplicationName = ApplicationName,
            /* A rare action on a login that is not for sharing: no pooled connection outlives it. */
            Pooling = false,
        };
        return (builder.ConnectionString, null);
    }
}
