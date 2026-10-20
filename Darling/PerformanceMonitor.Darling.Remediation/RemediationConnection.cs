/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Microsoft.Data.SqlClient;
using Npgsql;

namespace PerformanceMonitor.Darling.Remediation;

/// <summary>
/// The parts of a monitored SQL Server's entry that decide how a connection to it is made (#5632, step 6): the same fields the service's
/// <c>MonitoredServerConnection</c> reads, copied here as values because this assembly does not reference the service. A caller fills it from
/// the monitored server's entry; the monitoring connection and the remediation connection then reach the server the same way.
/// </summary>
public sealed record RemediationServer(
    string Host,
    string? Database = null,
    string? EncryptMode = null,
    bool TrustServerCertificate = false,
    bool MultiSubnetFailover = false,
    int Port = 0);

/// <summary>
/// Builds the remediation connection (#5632, step 6): the posture of the service's <c>BuildRemediationConnectionString</c> (same host, TLS
/// and certificate settings, SQL authentication, a 15-second connect budget, a 30-second command budget, no MARS, never read-only intent)
/// with the remediation login and an application name that says so.
/// </summary>
public static class RemediationConnection
{
    /// <summary>What a DBA sees in <c>sys.dm_exec_sessions.program_name</c>. Pinned equal to the service's constant.</summary>
    public const string ApplicationName = "PerformanceMonitorDarling-Remediation";

    internal static string BuildConnectionString(RemediationServer server, RemediationLogin login, string? databaseOverride = null)
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = server.Host,
            InitialCatalog = !string.IsNullOrWhiteSpace(databaseOverride) ? databaseOverride
                : string.IsNullOrWhiteSpace(server.Database) ? "master" : server.Database,
            ApplicationName = ApplicationName,
            ConnectTimeout = 15,
            CommandTimeout = 30,
            TrustServerCertificate = server.TrustServerCertificate,
            MultipleActiveResultSets = false,
            /* A remediation is rare and its login is not for sharing: no pooled connection outlives the action. */
            Pooling = false,
            ApplicationIntent = ApplicationIntent.ReadWrite,
            MultiSubnetFailover = server.MultiSubnetFailover,
            UserID = login.UserName,
            Password = login.Secret,
        };

        builder.Encrypt = server.EncryptMode?.Trim().ToUpperInvariant() switch
        {
            "STRICT" => SqlConnectionEncryptOption.Strict,
            "OPTIONAL" => SqlConnectionEncryptOption.Optional,
            _ => SqlConnectionEncryptOption.Mandatory,
        };
        return builder.ConnectionString;
    }

    /// <summary>The database the PostgreSQL remediation connection always opens (#5632). The server entry's own database is not used.</summary>
    public const string PostgresDatabase = "postgres";

    /// <summary>
    /// The search_path the PostgreSQL remediation connection signs in with (#5632). A setting sent at sign-in outranks one set with
    /// ALTER DATABASE or ALTER ROLE, so no database or role setting can put another schema's functions or operators in front of the
    /// built-in ones, for the remediation statements or for Npgsql's own start-up queries.
    /// </summary>
    public const string PostgresSearchPathOption = "-c search_path=pg_catalog,pg_temp";

    /// <summary>
    /// The remediation connection to a monitored PostgreSQL server (#5632): the same host and port as the service's PostgreSQL monitoring
    /// connection, and the same TLS mode (an Optional mode prefers TLS, a trusted certificate requires it, anything else verifies the
    /// certificate in full); the remediation login, a 15-second connect budget, a 30-second command budget, no pooled connection, and the
    /// application name that says so in <c>pg_stat_activity.application_name</c>.
    /// <para>It differs from the monitoring connection in four ways, because its login can end sessions:</para>
    /// <list type="bullet">
    /// <item>It always opens the <c>postgres</c> database. pg_stat_activity and pg_terminate_backend are cluster-wide, so that is enough,
    /// and the settings of a database somebody else owns never apply to this connection.</item>
    /// <item>It sets <c>search_path</c> to <c>pg_catalog, pg_temp</c> at sign-in (<see cref="PostgresSearchPathOption"/>).</item>
    /// <item>It accepts SCRAM-SHA-256 sign-in only: a server that asks for a password in clear text or MD5, or asks for none, is refused
    /// before the password is sent.</item>
    /// <item>Unless the certificate is verified in full, it requires SCRAM channel binding, which ties the sign-in to the TLS session, so it
    /// fails when the TLS session does not end at the server the password was set on. Channel binding needs TLS, so a server without TLS
    /// is refused.</item>
    /// </list>
    /// </summary>
    internal static string BuildPostgresConnectionString(RemediationServer server, RemediationLogin login)
    {
        var sslMode = server.EncryptMode?.Trim().ToUpperInvariant() == "OPTIONAL"
            ? SslMode.Prefer
            : server.TrustServerCertificate ? SslMode.Require : SslMode.VerifyFull;
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = server.Host,
            Database = PostgresDatabase,
            Username = login.UserName,
            Password = login.Secret,
            ApplicationName = ApplicationName,
            Timeout = 15,
            CommandTimeout = 30,
            Pooling = false,
            Options = PostgresSearchPathOption,
            SslMode = sslMode,
            RequireAuth = "ScramSHA256",
            ChannelBinding = sslMode == SslMode.VerifyFull ? ChannelBinding.Prefer : ChannelBinding.Require,
        };
        if (server.Port > 0)
            builder.Port = server.Port;
        return builder.ConnectionString;
    }
}
