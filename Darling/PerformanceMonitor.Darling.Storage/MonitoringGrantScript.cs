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
using System.Text;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>Which optional group of grants a <see cref="GrantEntry"/> belongs to.</summary>
public enum GrantSet
{
    /// <summary>What every SQL Server target needs (the first block of docs/permissions.md).</summary>
    Base,

    /// <summary>The per-database grants the FinOps Index Analysis tab needs (sp_IndexCleanup).</summary>
    FinOps,

    /// <summary>Azure SQL Database: a contained user and database-scoped grants, since there are no server logins.</summary>
    AzureSqlDatabase,
}

/// <summary>Where a grant is made.</summary>
public enum GrantScope
{
    /// <summary>In master, on the server.</summary>
    Server,

    /// <summary>In msdb, for the SQL Agent job tables.</summary>
    Msdb,

    /// <summary>In every user database (the FinOps grants).</summary>
    EachDatabase,

    /// <summary>In the one database an Azure SQL Database entry connects to.</summary>
    Database,
}

/// <summary>
/// One grant. <see cref="Required"/> is false for the grants docs/permissions.md marks "Optional" (ALTER TRACE, ALTER SETTINGS, the
/// msdb job reads): the script includes them, the connection test does not warn about them.
/// </summary>
public sealed record GrantEntry(GrantSet Set, GrantScope Scope, string Permission, string? OnObject, bool Required, string Why)
{
    /// <summary>The statement that grants this to <paramref name="principal"/> (a bare name, bracket-quoted here).</summary>
    public string Statement(string principal) => GrantPrefix + MonitoringGrantScript.QuoteName(principal) + ";";

    /// <summary>The statement up to the principal: <c>GRANT ... TO </c>, for a script that quotes the principal itself.</summary>
    public string GrantPrefix => OnObject is null ? $"GRANT {Permission} TO " : $"GRANT {Permission} ON {OnObject} TO ";
}

/// <summary>The kind of principal a script creates.</summary>
public enum GrantPrincipalKind
{
    /// <summary>A SQL Server login with a password (SQL authentication).</summary>
    SqlLogin,

    /// <summary>A Windows login: the service account, the machine account or a gMSA.</summary>
    WindowsLogin,

    /// <summary>A Microsoft Entra identity, created FROM EXTERNAL PROVIDER (Azure).</summary>
    ExternalProvider,
}

/// <summary>Who the script grants to, and the comment lines that explain the choice.</summary>
public sealed record GrantPrincipal(GrantPrincipalKind Kind, string Name, IReadOnlyList<string> Notes, bool IsPlaceholder = false);

/// <summary>What the principal choice reads. Pure data, so the choice is testable without a machine.</summary>
public sealed record GrantPrincipalInputs(
    string? Auth,
    string? Username,
    string? Host,
    /* The account the Darling service runs as, when the caller can read it; null when it cannot tell. */
    string? ServiceAccount,
    /* The machine the service runs on, and its domain (NetBIOS), as the caller sees them. */
    string MachineName,
    string? DomainName,
    /* True when the caller looked for the service on its own machine and found none (the Viewer on another machine): the service's account
       is unknown, so a Windows-authentication script names a placeholder rather than the caller's own machine account. */
    bool ServiceNotOnThisMachine = false);

/// <summary>
/// #6472: the monitoring login and grants as one list in code, and the script built from it. The list is the Darling reading of
/// docs/permissions.md (the base block, the FinOps per-database block and the Azure SQL Database block); docs/permissions.md stays
/// the human-readable copy. The CLI (<c>--grant-script</c>), the Viewer's Copy grant script button and the connection test's missing
/// grant warning all read this one list.
/// </summary>
public static class MonitoringGrantScript
{
    /// <summary>The Windows service's virtual account, the principal for a SQL Server on the same machine.</summary>
    public const string ServiceVirtualAccount = @"NT SERVICE\PerformanceMonitor Darling";

    /// <summary>The Windows service's registered name; the key under <c>HKLM\SYSTEM\CurrentControlSet\Services</c>.</summary>
    public const string ServiceName = "PerformanceMonitor Darling";

    /// <summary>The Windows account the script names when the caller cannot tell what the service runs as.</summary>
    public const string PlaceholderServiceAccount = @"DOMAIN\your-service-account";

    /// <summary>The login name the script uses when the server entry names none.</summary>
    public const string PlaceholderLogin = "YourLogin";

    /// <summary>Every grant, in the order the script emits them.</summary>
    public static readonly IReadOnlyList<GrantEntry> All = new GrantEntry[]
    {
        new(GrantSet.Base, GrantScope.Server, "VIEW SERVER STATE", null, true,
            "Server-scoped DMVs, the required core. Also implies VIEW DATABASE STATE in every database."),
        new(GrantSet.Base, GrantScope.Server, "CONNECT ANY DATABASE", null, true,
            "Enter every current and future database, with no per-database users to maintain."),
        new(GrantSet.Base, GrantScope.Server, "VIEW ANY DEFINITION", null, true,
            "Catalog visibility. Catalog views hide rows instead of raising an error, so without this the index, object, identity column capacity and availability group collectors see nothing. The identity check also stops on a database where it is missing, rather than report it clean."),
        new(GrantSet.Base, GrantScope.Server, "ALTER ANY EVENT SESSION", null, true,
            "The deadlock, blocked-process and attention Extended Events sessions."),
        new(GrantSet.Base, GrantScope.Server, "ALTER TRACE", null, false,
            "Optional: the default-trace collector reads sys.traces. ALTER TRACE is not read-only; withhold it and the collector records a permissions skip."),
        new(GrantSet.Base, GrantScope.Server, "ALTER SETTINGS", null, false,
            "Optional: lets the service set 'blocked process threshold (s)' to 5 when it is 0."),
        new(GrantSet.Base, GrantScope.Msdb, "SELECT", "dbo.sysjobs", false, "Optional: SQL Agent job monitoring and failed-job alerts. Direct table grants, not SQLAgentReaderRole."),
        new(GrantSet.Base, GrantScope.Msdb, "SELECT", "dbo.sysjobsteps", false, "Optional: names the Agent job and step on a Long-Running Query alert."),
        new(GrantSet.Base, GrantScope.Msdb, "SELECT", "dbo.sysjobactivity", false, "Optional: SQL Agent job monitoring."),
        new(GrantSet.Base, GrantScope.Msdb, "SELECT", "dbo.sysjobhistory", false, "Optional: SQL Agent job monitoring."),
        new(GrantSet.Base, GrantScope.Msdb, "SELECT", "dbo.sysjobschedules", false, "Optional: SQL Agent job monitoring."),
        new(GrantSet.Base, GrantScope.Msdb, "SELECT", "dbo.syscategories", false, "Optional: SQL Agent job monitoring."),
        new(GrantSet.Base, GrantScope.Msdb, "SELECT", "dbo.sysalerts", false, "Optional: SQL Server Agent alert definitions and counters (not read on Azure SQL Managed Instance or Amazon RDS)."),
        new(GrantSet.Base, GrantScope.Msdb, "SELECT", "dbo.sysnotifications", false, "Optional: the operators each Agent alert notifies."),
        new(GrantSet.Base, GrantScope.Msdb, "SELECT", "dbo.syssessions", false, "Optional: SQL Agent job monitoring."),
        new(GrantSet.Base, GrantScope.Msdb, "EXECUTE", "dbo.agent_datetime", false, "Optional: SQL Agent job monitoring."),
        new(GrantSet.Base, GrantScope.Msdb, "SELECT", "dbo.backupset", false, "Optional: the last full and log backup per database. Without it the database_protection collector records a permissions skip."),
        new(GrantSet.FinOps, GrantScope.EachDatabase, "VIEW DATABASE STATE", null, true, "FinOps Index Analysis runs sp_IndexCleanup in each database you inspect."),
        new(GrantSet.FinOps, GrantScope.EachDatabase, "VIEW DEFINITION", null, true, "FinOps Index Analysis."),
        new(GrantSet.FinOps, GrantScope.EachDatabase, "SELECT", "sys.sql_expression_dependencies", true,
            "FinOps Index Analysis. Only db_owner has this by default, and VIEW DEFINITION does not include it."),
        new(GrantSet.AzureSqlDatabase, GrantScope.Database, "VIEW DATABASE STATE", null, true, "The database-scoped DMVs."),
        new(GrantSet.AzureSqlDatabase, GrantScope.Database, "VIEW DEFINITION", null, true,
            "The database-scoped form of catalog visibility: without it sys.tables, sys.indexes and sys.identity_columns return no rows."),
        new(GrantSet.AzureSqlDatabase, GrantScope.Database, "ALTER ANY DATABASE EVENT SESSION", null, true,
            "Lets the service create its Extended Events sessions in the database."),
    };

    /// <summary>The server-scoped grants the connection test checks: the required base grants.</summary>
    public static IReadOnlyList<GrantEntry> RequiredServerGrants { get; } =
        All.Where(e => e.Set == GrantSet.Base && e.Scope == GrantScope.Server && e.Required).ToList();

    /// <summary>The database-scoped grants the connection test checks on an Azure SQL Database.</summary>
    public static IReadOnlyList<GrantEntry> RequiredAzureGrants { get; } =
        All.Where(e => e.Set == GrantSet.AzureSqlDatabase && e.Required).ToList();

    /// <summary>Brackets a name for T-SQL: <c>]</c> doubles.</summary>
    public static string QuoteName(string name) => "[" + name.Replace("]", "]]", StringComparison.Ordinal) + "]";

    private static readonly System.Buffers.SearchValues<char> s_hostCut = System.Buffers.SearchValues.Create(",\\");

    private static string Literal(string value) => "N'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

    /// <summary>
    /// True when <paramref name="host"/> (a SQL Server host text: <c>tcp:</c> prefix, <c>,port</c> and <c>\instance</c> allowed) names
    /// the machine <paramref name="machineName"/>: a dot, (local), localhost, a loopback address, the machine name, or the machine name
    /// with a domain suffix.
    /// </summary>
    public static bool IsSameMachine(string? host, string machineName)
    {
        var text = (host ?? "").Trim();
        if (text.StartsWith("tcp:", StringComparison.OrdinalIgnoreCase))
        {
            text = text[4..];
        }

        var cut = text.AsSpan().IndexOfAny(s_hostCut);
        if (cut >= 0)
        {
            text = text[..cut];
        }

        text = text.Trim();
        if (text.Length == 0)
        {
            return false;
        }

        return text is "." or "(local)" or "localhost" or "127.0.0.1" or "::1" or "[::1]"
            || text.Equals(machineName, StringComparison.OrdinalIgnoreCase)
            || text.StartsWith(machineName + ".", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// True for an Azure SQL Database host (<c>name.database.windows.net</c>). An Azure SQL Managed Instance host also ends in
    /// <c>.database.windows.net</c> but carries a DNS zone label (<c>name.zone.database.windows.net</c>, public endpoint
    /// <c>name.public.zone.database.windows.net</c>), and it works like on-premises, so it is not matched here.
    /// </summary>
    public static bool IsAzureSqlDatabaseHost(string? host)
    {
        var text = (host ?? "").Trim();
        if (text.StartsWith("tcp:", StringComparison.OrdinalIgnoreCase))
        {
            text = text[4..];
        }

        var cut = text.IndexOf(',', StringComparison.Ordinal);
        if (cut >= 0)
        {
            text = text[..cut];
        }

        text = text.Trim();
        const string suffix = ".database.windows.net";
        return text.Length > suffix.Length
            && text.EndsWith(suffix, StringComparison.OrdinalIgnoreCase)
            && text.IndexOf('.', StringComparison.Ordinal) == text.Length - suffix.Length;
    }

    /// <summary>The account the Darling Windows service is configured to run as (<c>sc qc</c>'s SERVICE_START_NAME), read from the service's
    /// registry key, which any user can read. Null when this is not the service's machine, not Windows, or the key cannot be read. A read of
    /// one registry value; it never throws.</summary>
    public static string? ReadServiceAccountName()
    {
        if (!OperatingSystem.IsWindows())
        {
            return null;
        }

        try
        {
            using var key = Microsoft.Win32.Registry.LocalMachine.OpenSubKey(@"SYSTEM\CurrentControlSet\Services\" + ServiceName);
            return key?.GetValue("ObjectName") as string;
        }
        catch (Exception ex) when (ex is System.Security.SecurityException or UnauthorizedAccessException or System.IO.IOException)
        {
            return null;
        }
    }

    /// <summary>
    /// The principal for this install. SQL authentication names the login. Entra authentication names an identity placeholder. Windows
    /// authentication is the account the service runs as: the virtual account for a SQL Server on the same machine, the machine account
    /// (<c>DOMAIN\MACHINE$</c>) for a remote one, or the service's own account when the caller read it and it is a domain account or a
    /// gMSA. When the caller cannot tell, the notes list the choices and the most likely one is used.
    /// </summary>
    public static GrantPrincipal ChoosePrincipal(GrantPrincipalInputs input)
    {
        ArgumentNullException.ThrowIfNull(input);
        var auth = (input.Auth ?? "integrated").Trim().ToLowerInvariant();
        var username = string.IsNullOrWhiteSpace(input.Username) ? null : input.Username.Trim();

        if (auth == "sql")
        {
            return new GrantPrincipal(
                GrantPrincipalKind.SqlLogin,
                username ?? PlaceholderLogin,
                username is null
                    ? new[] { "The server entry names no SQL login: replace " + PlaceholderLogin + " with the login the entry uses." }
                    : new[] { "Principal: the SQL login this server entry connects with. Set its password below; the script never holds the real one." });
        }

        if (auth is "serviceprincipal" or "managedidentity")
        {
            var placeholder = auth == "managedidentity" ? "your-managed-identity-name" : "your-app-registration-name";
            return new GrantPrincipal(
                GrantPrincipalKind.ExternalProvider,
                placeholder,
                new[] { "Principal: the Entra identity this server entry connects with. Replace " + placeholder + " with its display name; an Entra admin must already be set on the logical server." });
        }

        var domain = string.IsNullOrWhiteSpace(input.DomainName) || input.DomainName.Equals(input.MachineName, StringComparison.OrdinalIgnoreCase)
            ? "DOMAIN"
            : input.DomainName.Trim();
        var machineAccount = $@"{domain}\{input.MachineName.ToUpperInvariant()}$";
        var local = IsSameMachine(input.Host, input.MachineName);
        var service = string.IsNullOrWhiteSpace(input.ServiceAccount) ? null : input.ServiceAccount.Trim();

        /* A domain account or a gMSA the service really runs as: that is the principal, whatever machine the server is on. */
        if (service is not null
            && !service.StartsWith(@"NT SERVICE\", StringComparison.OrdinalIgnoreCase)
            && !service.StartsWith(@"NT AUTHORITY\", StringComparison.OrdinalIgnoreCase)
            && !service.Equals("LocalSystem", StringComparison.OrdinalIgnoreCase))
        {
            var kind = service.EndsWith('$') ? "gMSA" : "account";
            return new GrantPrincipal(
                GrantPrincipalKind.WindowsLogin,
                service,
                new[] { $"Principal: the {kind} the Darling service runs as ({service})." });
        }

        /* The caller is not on the service's machine, so neither the virtual account nor this machine's account is known to be right. */
        if (service is null && input.ServiceNotOnThisMachine)
        {
            return new GrantPrincipal(
                GrantPrincipalKind.WindowsLogin,
                PlaceholderServiceAccount,
                new[]
                {
                    "Principal: a PLACEHOLDER. The account the Darling service runs as could not be read from this machine, so replace " + PlaceholderServiceAccount + " before you run this.",
                    @"On the service's machine, run PerformanceMonitor.Darling.Service.exe --grant-script --host <host> to print this script with the real account.",
                    @"  NT SERVICE\PerformanceMonitor Darling   the default; a SQL Server on the same machine as the service",
                    @"  DOMAIN\MACHINE$   the machine account of the service's machine; a SQL Server on another machine",
                    @"  DOMAIN\account or DOMAIN\gmsa$   when the service's Log On account was changed (see Darling/README.md)",
                },
                IsPlaceholder: true);
        }

        var choices = new[]
        {
            @"Other accounts, if the service is configured differently:",
            $@"  NT SERVICE\PerformanceMonitor Darling   the default; a SQL Server on the same machine as the service",
            $@"  {machineAccount}   the machine account; a SQL Server on another machine (check the domain name)",
            @"  DOMAIN\account or DOMAIN\gmsa$   when the service's Log On account was changed (see Darling/README.md)",
        };

        return local
            ? new GrantPrincipal(
                GrantPrincipalKind.WindowsLogin,
                ServiceVirtualAccount,
                new[] { "Principal: the service's virtual account. This SQL Server is on the same machine as the service." }.Concat(choices).ToList())
            : new GrantPrincipal(
                GrantPrincipalKind.WindowsLogin,
                machineAccount,
                new[] { "Principal: the machine account of the Darling service's machine. This SQL Server is on another machine, where the virtual account authenticates as the machine account." }.Concat(choices).ToList());
    }

    /// <summary>
    /// The script. An Azure SQL Database (<paramref name="azureSqlDatabase"/>) gets the contained user and the database grants; any other
    /// server gets the login, the base grants and the msdb grants, plus the FinOps per-database grants when <paramref name="finOps"/>.
    /// Running it twice is clean: the login and user are created only when missing, and a repeated GRANT is a no-op.
    /// </summary>
    public static string Build(GrantPrincipal principal, bool finOps, bool azureSqlDatabase, string? serverLabel = null)
    {
        ArgumentNullException.ThrowIfNull(principal);
        var name = principal.Name;
        var q = QuoteName(name);
        var sb = new StringBuilder();
        void Line(string text = "") => sb.Append(text).Append('\n');

        /* Everything variable that sits inside the header comment is made inert: a comment closer in a server name or an account
           name must not end the comment and leave the rest as live T-SQL in a script the operator runs as a sysadmin. */
        static string InComment(string t) => t.Replace("*/", "* /", StringComparison.Ordinal).Replace("/*", "/ *", StringComparison.Ordinal).Replace('\r', ' ').Replace('\n', ' ');
        Line("/* Performance Monitor Darling: the login and grants for " + (serverLabel is { Length: > 0 } label ? InComment(label) : "a monitored server") + ".");
        foreach (var note in principal.Notes)
        {
            Line("   " + InComment(note));
        }

        Line("   Review it, then run it as a sysadmin" + (azureSqlDatabase ? " connected to the database you monitor, not master" : "") + ". It is safe to run twice. */");
        Line();

        if (azureSqlDatabase)
        {
            AppendAzure(sb, principal, q, finOps);
            return sb.ToString();
        }

        Line("USE [master];");
        Line();
        if (principal.Kind == GrantPrincipalKind.SqlLogin)
        {
            Line("/* Replace the password placeholder before running. */");
            Line("IF NOT EXISTS");
            Line("(");
            Line("    SELECT");
            Line("        1/0");
            Line("    FROM sys.server_principals AS sp");
            Line("    WHERE sp.name = " + Literal(name));
            Line(")");
            Line("BEGIN");
            Line("    CREATE LOGIN " + q + " WITH PASSWORD = N'<strong password>', CHECK_POLICY = ON;");
            Line("END;");
        }
        else
        {
            /* An Entra identity on a Managed Instance is a server login FROM EXTERNAL PROVIDER; a Windows account is FROM WINDOWS. */
            var source = principal.Kind == GrantPrincipalKind.ExternalProvider ? "EXTERNAL PROVIDER" : "WINDOWS";
            Line("IF NOT EXISTS");
            Line("(");
            Line("    SELECT");
            Line("        1/0");
            Line("    FROM sys.server_principals AS sp");
            Line("    WHERE sp.name = " + Literal(name));
            Line(")");
            Line("BEGIN");
            Line("    CREATE LOGIN " + q + " FROM " + source + ";");
            Line("END;");
        }

        Line();
        foreach (var entry in All.Where(e => e.Set == GrantSet.Base && e.Scope == GrantScope.Server))
        {
            Line("/* " + entry.Why + " */");
            Line(entry.Statement(name));
            Line();
        }

        Line("/* Optional: SQL Agent job monitoring and failed-job alerts. Skip this block if the server has no msdb Agent surface (AWS RDS has its own rules). */");
        Line("USE [msdb];");
        Line();
        AppendCreateUser(sb, name, q, "    CREATE USER " + q + " FOR LOGIN " + q + ";");
        foreach (var entry in All.Where(e => e.Set == GrantSet.Base && e.Scope == GrantScope.Msdb))
        {
            Line(entry.Statement(name));
        }

        Line();
        Line("USE [master];");
        if (finOps)
        {
            Line();
            AppendFinOps(sb, name);
        }

        return sb.ToString();
    }

    private static void AppendCreateUser(StringBuilder sb, string name, string q, string createStatement)
    {
        sb.Append("IF NOT EXISTS\n(\n    SELECT\n        1/0\n    FROM sys.database_principals AS dp\n    WHERE dp.name = ")
            .Append(Literal(name)).Append("\n)\nBEGIN\n").Append(createStatement).Append("\nEND;\n\n");
    }

    private static void AppendFinOps(StringBuilder sb, string name)
    {
        /* The block docs/permissions.md gives for "all databases", built from the list: a cursor over the online read-write user
           databases that are not single-user and that the caller can open, each one in a TRY/CATCH that prints the error and goes on (not
           sp_MSforeachdb, which can skip databases and replaces every question mark in the batch, one inside the login's name too). The login goes in as a parameter of each database's sp_executesql and is quoted there with QUOTENAME,
           so its name is never spliced into the per-database text. The per-database text sits inside a string, so each quote in it
           doubles once. */
        string InString(string t) => t.Replace("'", "''", StringComparison.Ordinal);
        var inner = new StringBuilder();
        inner.Append("DECLARE\n");
        inner.Append("    @statement nvarchar(max) = N'';\n\n");
        inner.Append("IF NOT EXISTS\n(\n    SELECT\n        1/0\n    FROM sys.database_principals AS dp\n    WHERE dp.name = @login\n)\n");
        inner.Append("BEGIN\n");
        inner.Append("    SET @statement += N'CREATE USER ' + QUOTENAME(@login) + N' FOR LOGIN ' + QUOTENAME(@login) + N';';\n");
        inner.Append("END;\n\n");
        foreach (var entry in All.Where(e => e.Set == GrantSet.FinOps))
        {
            inner.Append("SET @statement += N'").Append(entry.GrantPrefix).Append("' + QUOTENAME(@login) + N';';\n");
        }

        inner.Append("\nEXECUTE sys.sp_executesql\n    @statement;");

        sb.Append("/* FinOps Index Analysis: the per-database grants, in every online read-write user database. */\n");
        sb.Append("DECLARE\n");
        sb.Append("    @login sysname = ").Append(Literal(name)).Append(",\n");
        sb.Append("    @database_name sysname,\n");
        sb.Append("    @procedure nvarchar(600),\n");
        sb.Append("    @sql nvarchar(max) = N'\n").Append(InString(inner.ToString())).Append("\n';\n\n");
        sb.Append("DECLARE\n    database_cursor CURSOR LOCAL FAST_FORWARD FOR\n");
        sb.Append("    SELECT\n        d.name\n    FROM sys.databases AS d\n");
        sb.Append("    WHERE d.database_id > 4\n");
        sb.Append("    AND   d.state = 0\n");
        sb.Append("    AND   DATABASEPROPERTYEX(d.name, N'Updateability') = N'READ_WRITE'\n");
        sb.Append("    AND   d.user_access <> 1\n");
        sb.Append("    AND   HAS_DBACCESS(d.name) = 1\n");
        sb.Append("    ORDER BY\n        d.database_id;\n\n");
        sb.Append("OPEN database_cursor;\n\n");
        sb.Append("FETCH NEXT\nFROM database_cursor\nINTO @database_name;\n\n");
        sb.Append("WHILE @@FETCH_STATUS = 0\n");
        sb.Append("BEGIN\n");
        sb.Append("    PRINT N'Granting in ' + QUOTENAME(@database_name);\n");
        sb.Append("    SET @procedure = QUOTENAME(@database_name) + N'.sys.sp_executesql';\n");
        sb.Append("    BEGIN TRY\n");
        sb.Append("        EXECUTE @procedure\n");
        sb.Append("            @sql,\n");
        sb.Append("            N'@login sysname',\n");
        sb.Append("            @login = @login;\n");
        sb.Append("    END TRY\n");
        sb.Append("    BEGIN CATCH\n");
        sb.Append("        PRINT N'Skipped ' + QUOTENAME(@database_name) + N': ' + ERROR_MESSAGE();\n");
        sb.Append("    END CATCH;\n\n");
        sb.Append("    FETCH NEXT\n    FROM database_cursor\n    INTO @database_name;\n");
        sb.Append("END;\n\n");
        sb.Append("CLOSE database_cursor;\n");
        sb.Append("DEALLOCATE database_cursor;\n");
    }

    private static void AppendAzure(StringBuilder sb, GrantPrincipal principal, string q, bool finOps)
    {
        var name = principal.Name;
        var create = principal.Kind switch
        {
            GrantPrincipalKind.ExternalProvider => "    CREATE USER " + q + " FROM EXTERNAL PROVIDER;",
            GrantPrincipalKind.SqlLogin => "    CREATE USER " + q + " WITH PASSWORD = N'<strong password>';",
            _ => "    CREATE USER " + q + " FROM EXTERNAL PROVIDER;",
        };

        if (principal.Kind == GrantPrincipalKind.SqlLogin)
        {
            sb.Append("/* Replace the password placeholder before running. Azure SQL Database has no server logins: this is a contained database user. */\n");
        }
        else if (principal.Kind == GrantPrincipalKind.WindowsLogin)
        {
            sb.Append("/* Azure SQL Database does not take a Windows account. Use an Entra identity, and replace the name below. */\n");
        }

        AppendCreateUser(sb, name, q, create);
        foreach (var entry in All.Where(e => e.Set == GrantSet.AzureSqlDatabase))
        {
            sb.Append(entry.Statement(name)).Append('\n');
        }

        if (finOps)
        {
            sb.Append('\n').Append("/* FinOps Index Analysis: the one grant the base set does not give. */\n");
            foreach (var entry in All.Where(e => e.Set == GrantSet.FinOps && e.OnObject is not null))
            {
                sb.Append(entry.Statement(name)).Append('\n');
            }
        }
    }

    /// <summary>
    /// The query the connection test runs: one row, one column per entry of <paramref name="entries"/> in order, then the name of who connected; each entry's column is 1 (held), 0
    /// (missing) or NULL (the name is not valid on this version, so no claim). Server entries use the server securable; database entries
    /// use the current database.
    /// </summary>
    public static string BuildPermissionCheckSql(IReadOnlyList<GrantEntry> entries)
    {
        var columns = entries.Select((e, i) => e.Scope == GrantScope.Server
            ? $"HAS_PERMS_BY_NAME(NULL, NULL, N'{e.Permission}') AS g{i}"
            : $"HAS_PERMS_BY_NAME(DB_NAME(), N'DATABASE', N'{e.Permission}') AS g{i}")
            .ToList();

        /* The last column names who connected, so a missing grant's statement is for the login that lacks it. */
        columns.Add(entries.Any(e => e.Scope == GrantScope.Server) ? "SUSER_SNAME() AS who" : "USER_NAME() AS who");
        return "SELECT " + string.Join(", ", columns) + ";";
    }

    /// <summary>
    /// The grants a permission check found missing, each with the statement that adds it (to a placeholder login: the test knows the
    /// connecting login's name, but the grant belongs to whichever principal the service uses, so the caller passes the right one).
    /// <paramref name="held"/> aligns with <paramref name="entries"/>; only a value of exactly 0 counts as missing.
    /// </summary>
    public static IReadOnlyList<(GrantEntry Entry, string Statement)> MissingGrants(
        IReadOnlyList<GrantEntry> entries, IReadOnlyList<int?> held, string principal)
    {
        var missing = new List<(GrantEntry, string)>();
        for (var i = 0; i < entries.Count && i < held.Count; i++)
        {
            if (held[i] == 0)
            {
                missing.Add((entries[i], entries[i].Statement(principal)));
            }
        }

        return missing;
    }

    /// <summary>
    /// The warning the connection test adds for missing grants, or null when none is missing. One sentence per grant, each with the
    /// statement that adds it, so the operator can paste it.
    /// </summary>
    public static string? DescribeMissingGrants(IReadOnlyList<(GrantEntry Entry, string Statement)> missing)
    {
        if (missing.Count == 0)
        {
            return null;
        }

        var lines = missing.Select(m => $"{m.Entry.Permission} is missing ({m.Statement})");
        return "Grants missing for the connecting login: " + string.Join("; ", lines)
            + ". Collection that needs them returns empty or skipped results until they are granted. The --grant-script command prints the full script.";
    }
}
