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
using PerformanceMonitor.Darling.Storage;

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
            /* tcp: so SqlClient never falls back to a named pipe over SMB with this process's Windows credentials (#6352). */
            DataSource = PerformanceMonitor.Common.ServerHostSetting.TcpDataSource(server.Host),
            InitialCatalog = string.IsNullOrWhiteSpace(server.Database) ? "master" : server.Database,
            ApplicationName = PerformanceMonitor.Common.MonitorApplicationNames.Darling,
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
        else if (server.UsesServicePrincipal)
        {
            /* Microsoft Entra service principal (non-interactive), mirroring Lite's ApplyAuthentication: the
               application/client id is the user, the client secret is the password — resolved from
               EncryptedPassword exactly like a SQL password (RequiresResolvedSecret covers both). #3484. */
            builder.Authentication = SqlAuthenticationMethod.ActiveDirectoryServicePrincipal;
            builder.UserID = server.Username;
            builder.Password = resolvedPassword
                ?? throw new InvalidOperationException($"Server '{server.DisplayName}' uses service-principal auth but no client secret was resolved.");
        }
        else if (server.UsesManagedIdentity)
        {
            /* Azure managed identity (non-interactive), mirroring Lite: no secret. A user-assigned identity
               names its client id in Username; a system-assigned one leaves it blank. #3484. */
            builder.Authentication = SqlAuthenticationMethod.ActiveDirectoryManagedIdentity;
            if (!string.IsNullOrWhiteSpace(server.Username))
            {
                builder.UserID = server.Username;
            }
        }
        else
        {
            builder.IntegratedSecurity = true;
        }

        return builder.ConnectionString;
    }

    /// <summary>
    /// #6254: the connection string for reading Query Store from the server's readable replica, or null when the server has none,
    /// is a PostgreSQL target (never has one) or is an Azure SQL Database (<paramref name="isAzureSqlDatabase"/>: it reads Query
    /// Store from itself in this version, the same gate Lite's runner applies). It is the server's own connection string
    /// (<see cref="BuildConnectionString"/>) with the host replaced by the replica (a host, <c>host,port</c> or listener name, taken
    /// as typed) and <c>ApplicationIntent=ReadOnly</c>: the same login, authentication, encryption, certificate trust,
    /// MultiSubnetFailover, database and timeouts. The ONE place the replica connection is built, so no caller assembles its own.
    ///
    /// <para><b>The replica and the password come from one place.</b> The replica is read from
    /// <see cref="MonitoredServer.ConnectionIdentity"/>, the identity the stored password's seal is opened with
    /// (<see cref="PasswordBinding.ForServer"/>), not from the free <see cref="MonitoredServer.QueryStoreReplica"/> property, and the
    /// password is resolved here from the same server object (<see cref="DarlingSecrets.ResolvePassword(MonitoredServer, out bool, IPasswordKeyRing?)"/>),
    /// never passed in: so a password opened for one replica host cannot be sent to another. A stored <c>env:</c>/<c>file:</c>
    /// reference is not covered by a seal, so one is used for the replica only when darling.json declares it
    /// (<see cref="MonitoredServer.EncryptedPasswordDeclaredByFile"/>); a store that holds one without the file's word refuses.
    /// A refused or unresolvable password throws <see cref="InvalidOperationException"/>, as the primary connection's does.</para>
    /// This builds a string and opens nothing.
    /// </summary>
    public static string? BuildQueryStoreReplicaConnectionString(MonitoredServer server, bool isAzureSqlDatabase, IPasswordKeyRing? ring = null)
    {
        if (server is null)
        {
            throw new ArgumentNullException(nameof(server));
        }

        if (server.IsPostgres || isAzureSqlDatabase
            || QueryStoreReplicaSetting.Normalize(server.ConnectionIdentity.QueryStoreReplica) is not { } replica)
        {
            return null;
        }

        /* The last gate, for a value that reached the store without the surfaces' check (a direct write to the table). */
        if (QueryStoreReplicaSetting.Refusal(replica) is { } replicaRefusal)
        {
            throw new InvalidOperationException($"servers['{server.DisplayName}'].queryStoreReplica: {replicaRefusal}");
        }

        var password = DarlingSecrets.ResolveReplicaPassword(server, ring);
        var builder = new SqlConnectionStringBuilder(BuildConnectionString(server, password))
        {
            DataSource = PerformanceMonitor.Common.ServerHostSetting.TcpDataSource(replica),
            ApplicationIntent = ApplicationIntent.ReadOnly,
        };

        return builder.ConnectionString;
    }

    /// <summary>
    /// The connection string for a #2138 phase-1 REMEDIATION action: the same posture as
    /// <see cref="BuildConnectionString"/>, on the server's second, opt-in remediation identity.
    ///
    /// <para><b>A separate function rather than a parameter on the one above</b>, because the two differ in
    /// ways a boolean would have to be read correctly at every call site: the credential is always SQL auth
    /// (there is no integrated arm to fall into), the identity is not the monitoring one, and the
    /// application name is deliberately different. A <c>useRemediationCredential: true</c> flag on the main
    /// builder would put the write identity one mistyped argument away from every collector.</para>
    ///
    /// <para><b>The ApplicationName is the audit trail on the server's side.</b> A DBA reading
    /// <c>sys.dm_exec_sessions</c> during an incident needs to be able to tell this apart from the
    /// collection connections, and "the monitoring tool" answering for both would make the one connection
    /// that can change a plan indistinguishable from the forty that cannot. It is also what makes an XE
    /// or Profiler filter on this feature possible at all.</para>
    ///
    /// <para><b>No MARS, and a tighter command budget.</b> The collection loop wants multiple active result
    /// sets; a remediation runs one statement. And 60 seconds is a collection budget — a
    /// <c>sp_query_store_force_plan</c> that has not returned in 30 is not going to, and holding the
    /// connection longer only delays the journal row that says so.</para>
    ///
    /// <para>Postgres targets throw rather than returning something: Query Store plan forcing is a SQL
    /// Server concept, so a PostgreSQL target reaching here is a caller that skipped the engine gate, and
    /// the #2213 lesson is that the failure has to be loud at the boundary rather than an
    /// <c>ArgumentException</c> from a driver parsing the wrong keyword shape.</para>
    /// </summary>
    public static string BuildRemediationConnectionString(
        MonitoredServer server, string resolvedRemediationPassword)
    {
        if (server is null)
        {
            throw new ArgumentNullException(nameof(server));
        }

        if (string.IsNullOrWhiteSpace(resolvedRemediationPassword))
        {
            throw new ArgumentException(
                "A remediation connection requires the remediation credential's password.",
                nameof(resolvedRemediationPassword));
        }

        if (server.IsPostgres)
        {
            throw new InvalidOperationException(
                $"Server '{server.DisplayName}' is a PostgreSQL target; Query Store plan remediation is a " +
                "SQL Server concept and this call site should have been engine-gated.");
        }

        if (!server.HasRemediationCredential)
        {
            throw new InvalidOperationException(
                $"Server '{server.DisplayName}' has no remediation credential, so no remediation connection " +
                "can be built for it.");
        }

        var builder = new SqlConnectionStringBuilder
        {
            /* tcp: so SqlClient never falls back to a named pipe over SMB with this process's Windows credentials (#6352). */
            DataSource = PerformanceMonitor.Common.ServerHostSetting.TcpDataSource(server.Host),
            InitialCatalog = string.IsNullOrWhiteSpace(server.Database) ? "master" : server.Database,
            ApplicationName = RemediationApplicationName,
            ConnectTimeout = 15,
            CommandTimeout = 30,
            TrustServerCertificate = server.TrustServerCertificate,
            MultipleActiveResultSets = false,
            /* Never ReadOnly, whatever the server's ReadOnlyIntent says. A remediation connection routed to
               a read-only secondary by an intent hint would fail the write with a message about the replica
               rather than about the routing, and the monitoring entry's intent is a COLLECTION preference
               that has no business steering a write. */
            ApplicationIntent = ApplicationIntent.ReadWrite,
            MultiSubnetFailover = server.MultiSubnetFailover,
        };

        builder.Encrypt = server.EncryptMode?.Trim().ToUpperInvariant() switch
        {
            "STRICT" => SqlConnectionEncryptOption.Strict,
            "OPTIONAL" => SqlConnectionEncryptOption.Optional,
            _ => SqlConnectionEncryptOption.Mandatory,
        };

        builder.UserID = server.RemediationUsername;
        builder.Password = resolvedRemediationPassword;

        return builder.ConnectionString;
    }

    /// <summary>
    /// The <c>ApplicationName</c> a remediation connection presents. A named constant because it is the
    /// only thing a DBA on the far end can filter on, so it is a documented interface rather than a string
    /// — and because a test can then assert the remediation and collection connections do not share it.
    /// </summary>
    public const string RemediationApplicationName = "PerformanceMonitorDarling-Remediation";

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

        builder.Options = PinSearchPath(builder.Options);

        /* #6143: Npgsql's own Search Path keyword is sent as a separate startup parameter, which the server applies
           after the -c options and so would replace the pin. Cleared explicitly, whatever the stored string said. */
        builder.SearchPath = null;

        return builder.ConnectionString;
    }

    /// <summary>The startup option every monitoring connection to a PostgreSQL target carries (#6143).</summary>
    public const string PinnedSearchPathOption = "-c search_path=pg_catalog,pg_temp";

    /// <summary>
    /// Adds <see cref="PinnedSearchPathOption"/> to a connection's startup <c>Options</c>, keeping any option already
    /// there (#6143).
    /// <para>The collectors connect to every database on a target, and a database or role setting
    /// (<c>ALTER DATABASE ... SET search_path</c>, <c>ALTER ROLE ... SET search_path</c>) would otherwise decide how
    /// each unqualified function, operator and relation name in their SQL resolves. A setting sent at sign-in takes
    /// precedence over both, so collector SQL resolves against pg_catalog, with pg_temp last. Extension objects are
    /// named by the schema they were created in instead (<c>CollectorContext.PgExtensionSchema</c>).</para>
    /// <para>The pin is always the LAST option: the server applies the options in order, so a later
    /// <c>-c search_path=...</c> replaces an earlier one, and an option already present that merely contains the pin's
    /// text (<c>-c search_path=pg_catalog,pg_temp,public</c>) is overridden rather than taken for the pin.</para>
    /// </summary>
    public static string PinSearchPath(string? options)
    {
        if (string.IsNullOrWhiteSpace(options))
        {
            return PinnedSearchPathOption;
        }

        var trimmed = options.TrimEnd();
        return EndsWithPinOption(trimmed) ? trimmed : trimmed + " " + PinnedSearchPathOption;
    }

    /* True when the pin is the whole string or its last two tokens: preceded by whitespace that no backslash escapes
       (the server splits options on unescaped whitespace, so an escaped space would join the pin to the token before). */
    /// <summary>Whether <paramref name="options"/> ends with <see cref="PinnedSearchPathOption"/> as its own option, as
    /// <see cref="PinSearchPath"/> leaves it (#6143).</summary>
    internal static bool CarriesPin(string? options) =>
        !string.IsNullOrWhiteSpace(options) && EndsWithPinOption(options.TrimEnd());

    private static bool EndsWithPinOption(string options)
    {
        if (!options.EndsWith(PinnedSearchPathOption, StringComparison.Ordinal))
        {
            return false;
        }

        var start = options.Length - PinnedSearchPathOption.Length;
        if (start == 0)
        {
            return true;
        }

        if (!DarlingServerConnector.IsPgListSpace(options[start - 1]))
        {
            return false;
        }

        var backslashes = 0;
        for (var i = start - 2; i >= 0 && options[i] == '\\'; i--)
        {
            backslashes++;
        }

        return backslashes % 2 == 0;
    }
}
