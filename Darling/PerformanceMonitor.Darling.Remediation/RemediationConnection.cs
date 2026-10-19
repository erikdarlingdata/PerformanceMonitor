/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Microsoft.Data.SqlClient;

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
    bool MultiSubnetFailover = false);

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
}
