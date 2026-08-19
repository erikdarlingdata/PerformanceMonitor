/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Microsoft.Data.SqlClient;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Builds the SqlClient connection string for a monitored server, mirroring Lite's
/// ServerConnection.BuildConnectionString shape (MARS on for the collection loop, 15-second
/// connect budget, Encrypt fail-closed to Mandatory for unknown modes) so the two SKUs present
/// the same connection posture to monitored servers — only the ApplicationName differs.
/// </summary>
public static class MonitoredServerConnection
{
    public static string BuildConnectionString(MonitoredServer server, string? resolvedPassword = null)
    {
        if (server is null)
        {
            throw new ArgumentNullException(nameof(server));
        }

        if (server.IsPostgres)
        {
            return BuildPostgresConnectionString(server, resolvedPassword);
        }

        var builder = new SqlConnectionStringBuilder
        {
            DataSource = server.Host,
            InitialCatalog = string.IsNullOrWhiteSpace(server.Database) ? "master" : server.Database,
            ApplicationName = "PerformanceMonitorDarling",
            ConnectTimeout = 15,
            CommandTimeout = 60,
            TrustServerCertificate = server.TrustServerCertificate,
            MultipleActiveResultSets = true,
            ApplicationIntent = server.ReadOnlyIntent ? ApplicationIntent.ReadOnly : ApplicationIntent.ReadWrite,
            MultiSubnetFailover = server.MultiSubnetFailover,
        };

        /* Encrypt fail-closed: unknown/blank modes get Mandatory, matching Lite. */
        builder.Encrypt = server.EncryptMode?.Trim().ToUpperInvariant() switch
        {
            "STRICT" => SqlConnectionEncryptOption.Strict,
            "OPTIONAL" => SqlConnectionEncryptOption.Optional,
            _ => SqlConnectionEncryptOption.Mandatory,
        };

        if (server.UsesSqlAuth)
        {
            builder.UserID = server.Username;
            builder.Password = resolvedPassword
                ?? throw new InvalidOperationException($"Server '{server.DisplayName}' uses sql auth but no password was resolved.");
        }
        else
        {
            builder.IntegratedSecurity = true;
        }

        return builder.ConnectionString;
    }

    /// <summary>
    /// The PostgreSQL equivalent, keeping the same posture the SQL Server path establishes: a
    /// 15-second connect budget, a 60-second command budget, TLS required unless explicitly relaxed,
    /// and an application name the DBA can see in <c>pg_stat_activity</c>.
    /// <para>Deliberate differences from the SQL Server builder, each because the concept does not
    /// exist here: there is no MARS (Npgsql multiplexes differently), no ApplicationIntent (a
    /// PostgreSQL read replica is a separate endpoint, not a routing hint — point the entry at the
    /// reader's own host), and no MultiSubnetFailover.</para>
    /// <para>Integrated auth is rejected rather than silently ignored. Npgsql can do Kerberos, but a
    /// Windows service account authenticating to Aurora is not a path anyone has configured here, and
    /// quietly producing a connection string that cannot authenticate would fail later and less
    /// clearly than failing now.</para>
    /// </summary>
    private static string BuildPostgresConnectionString(MonitoredServer server, string? resolvedPassword)
    {
        if (!server.UsesSqlAuth)
        {
            throw new InvalidOperationException(
                $"Server '{server.DisplayName}' is a PostgreSQL target, which requires auth \"sql\" with a username "
                + "and password (integrated/Kerberos auth is not supported for PostgreSQL targets).");
        }

        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = server.Host,
            /* "postgres" is the maintenance database every cluster has. Per-database collectors
               override this; the connect probe and every instance-wide view work from here. */
            Database = string.IsNullOrWhiteSpace(server.Database) ? "postgres" : server.Database,
            Username = server.Username,
            Password = resolvedPassword
                ?? throw new InvalidOperationException($"Server '{server.DisplayName}' uses sql auth but no password was resolved."),
            ApplicationName = "PerformanceMonitorDarling",
            Timeout = 15,
            CommandTimeout = 60,
            /* Same fail-closed intent as the SQL Server path: anything but an explicit opt-out gets
               TLS. TrustServerCertificate maps to VerifyFull-vs-Require rather than to disabling TLS —
               Aurora presents an RDS CA that a stock trust store does not know, which is the case
               TrustServerCertificate exists to cover. */
            SslMode = server.EncryptMode?.Trim().ToUpperInvariant() == "OPTIONAL"
                ? SslMode.Prefer
                : server.TrustServerCertificate ? SslMode.Require : SslMode.VerifyFull,
        };

        if (server.Port > 0)
        {
            builder.Port = server.Port;
        }

        return builder.ConnectionString;
    }
}
