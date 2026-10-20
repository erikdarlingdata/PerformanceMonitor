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

    /// <summary>What the check's sign-in needs from the server, and what to change when the server does not offer it. The rule the remediation login follows.</summary>
    public const string SignInRule =
        "The hypothetical-index login signs in with SCRAM-SHA-256 only (set password_encryption = 'scram-sha-256', set the login's password again, "
        + "and give it a scram-sha-256 line in pg_hba.conf). Unless the server entry verifies the server's certificate in full (encryption not "
        + "Optional and Trust server certificate off), it also needs SCRAM channel binding, which needs TLS turned on at the server (ssl = on).";

    /// <summary>
    /// Npgsql's own refusals of a sign-in the connection string rules out (#6170): the server asked for a method other than SCRAM-SHA-256
    /// (<c>RequireAuth</c>), or offered SCRAM without channel binding when it is required. Npgsql raises both as a plain
    /// <see cref="NpgsqlException"/>, so the text is what tells them apart from other connection failures. The same test the remediation login uses.
    /// </summary>
    public static bool IsSignInRuleRefusal(NpgsqlException ex) =>
        ex is not PostgresException
        && ex.InnerException is null
        && (ex.Message.Contains("authentication method is not allowed", StringComparison.Ordinal)
            || ex.Message.Contains(nameof(ChannelBinding), StringComparison.Ordinal));

    /// <summary>
    /// The connection string for the check, or the one sentence that says why there is none. The host, port, database, TLS mode, timeouts and
    /// the search_path pin are the monitoring connection's; the login and password are this login's. The password is resolved here, on the Darling
    /// host, through <see cref="DarlingSecrets.ResolveHypotheticalIndexPassword"/> (a reference to one of the service's own secrets is
    /// refused), and is never put in a message.
    ///
    /// <para>Built explicitly, the way <c>RemediationConnection.BuildPostgresConnectionString</c> builds the remediation connection, so a
    /// setting the monitoring string happens to carry cannot reach the check's sign-in: SCRAM-SHA-256 only (<c>RequireAuth</c>), and SCRAM
    /// channel binding unless the certificate is verified in full. The server is then refused before the password is sent when it asks for
    /// a password in clear text or MD5, or for none.</para>
    /// </summary>
    public static (string? ConnectionString, string? Refusal) Resolve(MonitoredServer server, string monitoringConnectionString)
    {
        ArgumentNullException.ThrowIfNull(server);
        var displayName = server.DisplayName;
        var userName = server.HypotheticalIndexUsername;
        var reference = server.HypotheticalIndexEncryptedPassword;
        if (string.IsNullOrWhiteSpace(userName) || string.IsNullOrWhiteSpace(reference))
        {
            return (null,
                $"The hypothetical-index check needs its own PostgreSQL login on '{displayName}', and none is set. The check never runs under the "
                + "monitoring login. Create a login with " + GrantsText + " Its password must be a SCRAM-SHA-256 password. Then set "
                + "hypotheticalIndexUsername and hypotheticalIndexEncryptedPassword for this server: the login name, and an env:NAME or "
                + "file:path reference to its password.");
        }

        if (!DarlingSecretSource.IsReference(reference))
        {
            return (null,
                $"The hypothetical-index login on '{displayName}' has a stored password. The check uses an env:NAME or file:path reference to the "
                + "password only, and never a stored password. Set hypotheticalIndexEncryptedPassword to a reference.");
        }

        string password;
        try
        {
            password = DarlingSecrets.ResolveHypotheticalIndexPassword(server);
        }
        catch (Exception ex) when (ex is InvalidOperationException or ArgumentException)
        {
            /* The exception text can carry the file path, and a path is as private as the reference. */
            return (null,
                $"The password reference for the hypothetical-index login on '{displayName}' could not be read: the reference is refused "
                + "(it names a file or variable the service keeps its own secrets in), the environment variable is not set, or the file is "
                + "missing, unreadable or empty.");
        }

        var monitoring = new NpgsqlConnectionStringBuilder(monitoringConnectionString);
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = monitoring.Host,
            Database = monitoring.Database,
            Username = userName.Trim(),
            /* Not trimmed here: the value is exactly what DarlingSecretSource.Resolve returns, as for every other server secret (it trims a file's contents; an environment variable is used as set). */
            Password = password,
            ApplicationName = ApplicationName,
            Timeout = monitoring.Timeout,
            CommandTimeout = monitoring.CommandTimeout,
            /* A rare action on a login that is not for sharing: no pooled connection outlives it. */
            Pooling = false,
            /* The monitoring connection's startup options, which carry the search_path pin CreatePinnedConnection checks. */
            Options = monitoring.Options,
            SslMode = monitoring.SslMode,
            RequireAuth = "ScramSHA256",
            ChannelBinding = monitoring.SslMode == SslMode.VerifyFull ? ChannelBinding.Prefer : ChannelBinding.Require,
        };
        if (monitoring.Port > 0)
        {
            builder.Port = monitoring.Port;
        }

        return (builder.ConnectionString, null);
    }
}
