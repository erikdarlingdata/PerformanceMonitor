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
    public string Statement(string principal) =>
        OnObject is null
            ? $"GRANT {Permission} TO {MonitoringGrantScript.QuoteName(principal)};"
            : $"GRANT {Permission} ON {OnObject} TO {MonitoringGrantScript.QuoteName(principal)};";
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
public sealed record GrantPrincipal(GrantPrincipalKind Kind, string Name, IReadOnlyList<string> Notes);

/// <summary>What the principal choice reads. Pure data, so the choice is testable without a machine.</summary>
public sealed record GrantPrincipalInputs(
    string? Auth,
    string? Username,
    string? Host,
    /* The account the Darling service runs as, when the caller can read it; null when it cannot tell. */
    string? ServiceAccount,
    /* The machine the service runs on, and its domain (NetBIOS), as the caller sees them. */
    string MachineName,
    string? DomainName);

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
            "Catalog visibility. Catalog views hide rows instead of raising an error, so without this the index, object and availability group collectors see nothing."),
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
        new(GrantSet.Base, GrantScope.Msdb, "SELECT", "dbo.syssessions", false, "Optional: SQL Agent job monitoring."),
        new(GrantSet.Base, GrantScope.Msdb, "EXECUTE", "dbo.agent_datetime", false, "Optional: SQL Agent job monitoring."),
        new(GrantSet.FinOps, GrantScope.EachDatabase, "VIEW DATABASE STATE", null, true, "FinOps Index Analysis runs sp_IndexCleanup in each database you inspect."),
        new(GrantSet.FinOps, GrantScope.EachDatabase, "VIEW DEFINITION", null, true, "FinOps Index Analysis."),
        new(GrantSet.FinOps, GrantScope.EachDatabase, "SELECT", "sys.sql_expression_dependencies", true,
            "FinOps Index Analysis. Only db_owner has this by default, and VIEW DEFINITION does not include it."),
        new(GrantSet.AzureSqlDatabase, GrantScope.Database, "VIEW DATABASE STATE", null, true, "The database-scoped DMVs."),
        new(GrantSet.AzureSqlDatabase, GrantScope.Database, "VIEW DEFINITION", null, true,
            "The database-scoped form of catalog visibility: without it sys.tables and sys.indexes return no rows."),
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

        var cut = text.IndexOfAny(new[] { ',', '\\' });
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

    /// <summary>True for an Azure SQL Database host (<c>*.database.windows.net</c>).</summary>
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

        return text.Trim().EndsWith(".database.windows.net", StringComparison.OrdinalIgnoreCase);
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

        Line("/* Performance Monitor Darling: the login and grants for " + (serverLabel is { Length: > 0 } label ? label : "a monitored server") + ".");
        foreach (var note in principal.Notes)
        {
            Line("   " + note);
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
            Line("IF NOT EXISTS");
            Line("(");
            Line("    SELECT");
            Line("        1/0");
            Line("    FROM sys.server_principals AS sp");
            Line("    WHERE sp.name = " + Literal(name));
            Line(")");
            Line("BEGIN");
            Line("    CREATE LOGIN " + q + " FROM WINDOWS;");
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
            AppendFinOps(sb, name, q);
        }

        return sb.ToString();
    }

    private static void AppendCreateUser(StringBuilder sb, string name, string q, string createStatement)
    {
        sb.Append("IF NOT EXISTS\n(\n    SELECT\n        1/0\n    FROM sys.database_principals AS dp\n    WHERE dp.name = ")
            .Append(Literal(name)).Append("\n)\nBEGIN\n").Append(createStatement).Append("\nEND;\n\n");
    }

    private static void AppendFinOps(StringBuilder sb, string name, string q)
    {
        /* The block docs/permissions.md gives for "all databases", built from the list. The statements sit inside a string, so each
           quote doubles once for the string; the principal name's own quote doubles first (Literal), then again for the string. */
        string InString(string t) => t.Replace("'", "''", StringComparison.Ordinal);
        sb.Append("/* FinOps Index Analysis: the per-database grants, in every writable user database. */\n");
        sb.Append("EXECUTE sys.sp_MSforeachdb N'\n");
        sb.Append("USE [?];\n");
        sb.Append("IF DB_ID() > 4 AND DATABASEPROPERTYEX(DB_NAME(), ''Updateability'') = ''READ_WRITE''\n");
        sb.Append("BEGIN\n");
        sb.Append("    IF NOT EXISTS (SELECT 1/0 FROM sys.database_principals AS dp WHERE dp.name = ").Append(InString(Literal(name))).Append(")\n");
        sb.Append("        CREATE USER ").Append(InString(q)).Append(" FOR LOGIN ").Append(InString(q)).Append(";\n");
        foreach (var entry in All.Where(e => e.Set == GrantSet.FinOps))
        {
            sb.Append("    ").Append(InString(entry.Statement(name))).Append('\n');
        }

        sb.Append("END';\n");
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
