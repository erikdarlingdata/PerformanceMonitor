/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

public static partial class DarlingCliCommands
{
    /// <summary>The verb <see cref="GrantScriptAsync"/> handles (#6472): print the login and grant script for a server.</summary>
    public static bool IsGrantScriptVerb(string arg) =>
        string.Equals(arg, "--grant-script", StringComparison.OrdinalIgnoreCase);

    /// <summary>What <c>--grant-script</c> was asked for. <see cref="ServerName"/> picks a registered server; <see cref="Host"/> describes
    /// one not yet added. Exactly one of the two is set.</summary>
    internal sealed record GrantScriptArgs(
        string? ServerName, string? Host, string? Auth, string? Username, string? ServiceAccount, bool FinOps, bool Azure, bool Rds, string? ConfigPath);

    /// <summary>The grammar <c>--grant-script</c> prints when its arguments are wrong. Pure ASCII.</summary>
    public static string GrantScriptUsageText() =>
        "Usage:" + Environment.NewLine +
        "  --grant-script <server-name> [--finops] [--azure] [--rds] [--service-account <account>] [--config <path>]" + Environment.NewLine +
        "      Print the T-SQL that creates the monitoring login and grants what Darling needs, for a server this install monitors." + Environment.NewLine +
        "      The principal follows the server entry: its SQL login, or for Windows authentication the account the service runs as." + Environment.NewLine +
        "  --grant-script --host <host> [--auth sql|integrated|windows|serviceprincipal|managedidentity] [--username <login>] [--finops] [--azure] [--rds] [--service-account <account>]" + Environment.NewLine +
        "      The same for a host not yet added. Connects to no monitored server." + Environment.NewLine +
        "  --finops adds the per-database grants the FinOps Index Analysis tab needs. --azure (implied by a *.database.windows.net host)" + Environment.NewLine +
        "  prints the Azure SQL Database form: a contained user and database-scoped grants." + Environment.NewLine +
        "  --rds (implied by a *.rds.amazonaws.com host) prints the Amazon RDS for SQL Server form: no CONNECT ANY DATABASE, ALTER SETTINGS or" + Environment.NewLine +
        "  refused msdb grants, and a CREATE USER in each current user database. Run it as the RDS master user. --service-account names the account the service" + Environment.NewLine +
        "  runs as when it is a domain account or a gMSA and this machine cannot tell. Run any other output as a sysadmin; it is safe to run twice." + Environment.NewLine +
        "The script never holds a password: a SQL login gets a placeholder to replace.";

    /// <summary>The <c>--auth</c> word as the registry stores it: lower case, with "windows" (the word <c>--add-server</c> takes) read as "integrated".</summary>
    internal static string GrantScriptAuthWord(string value)
    {
        var word = value.Trim().ToLowerInvariant();
        return word == "windows" ? "integrated" : word;
    }

    /// <summary>Parses <c>--grant-script</c>'s arguments strictly: one bare server name or <c>--host</c>, the flags, and the options that take
    /// a value. Anything else that starts with '-' is refused.</summary>
    internal static bool TryParseGrantScriptArgs(string[] rest, out GrantScriptArgs? parsed, out string? errorMessage)
    {
        parsed = null;
        errorMessage = null;
        string? name = null, host = null, auth = null, username = null, serviceAccount = null, configPath = null;
        bool finOps = false, azure = false, rds = false;
        for (var i = 0; i < rest.Length; i++)
        {
            var arg = rest[i];
            switch (arg.ToLowerInvariant())
            {
                case "--finops":
                    finOps = true;
                    continue;
                case "--azure":
                    azure = true;
                    continue;
                case "--rds":
                    rds = true;
                    continue;
                case "--host" or "--auth" or "--username" or "--service-account" or "--config":
                    if (i + 1 >= rest.Length || rest[i + 1].StartsWith("--", StringComparison.Ordinal))
                    {
                        errorMessage = $"{arg} needs a value.";
                        return false;
                    }

                    var value = rest[++i];
                    switch (arg.ToLowerInvariant())
                    {
                        case "--host":
                            host = value;
                            break;
                        case "--auth":
                            auth = GrantScriptAuthWord(value);
                            break;
                        case "--username":
                            username = value;
                            break;
                        case "--service-account":
                            serviceAccount = value;
                            break;
                        default:
                            configPath = value;
                            break;
                    }

                    continue;
            }

            if (arg.StartsWith('-'))
            {
                errorMessage = $"Unknown option {arg}.";
                return false;
            }

            if (name is not null)
            {
                errorMessage = $"--grant-script takes ONE server name; got '{name}' and '{arg}'.";
                return false;
            }

            name = arg;
        }

        if ((name is null) == (host is null))
        {
            errorMessage = "Name a monitored server, or give --host for one not yet added (not both).";
            return false;
        }

        if (auth is not null and not ("sql" or "integrated" or "serviceprincipal" or "managedidentity"))
        {
            errorMessage = $"--auth must be sql, integrated (or windows), serviceprincipal or managedidentity; got '{auth}'.";
            return false;
        }

        if (name is not null && (auth is not null || username is not null))
        {
            errorMessage = "--auth and --username describe a host not yet added; a registered server already has its own.";
            return false;
        }

        parsed = new GrantScriptArgs(name, host, auth, username, serviceAccount, finOps, azure, rds, configPath);
        return true;
    }

    /// <summary>The wording for the registry list <c>--grant-script</c> reads: it needs the entry's login and auth, nothing more.</summary>
    private static readonly RegistryListWording GrantScriptRegistryWording = new(
        "using darling.json's own server list instead.",
        "They are not part of this script; the store governs which servers actually run.");

    /// <summary>Where <c>--grant-script</c> writes. A console shows Unicode as it is. A redirected stdout is encoded with the console's code page
    /// (437, 850, 1252), and Windows maps some non-ASCII characters to ASCII look-alikes there (U+FF07 and U+02BC become a quote, U+FF3D a
    /// closing bracket, U+FF0A U+FF0F the end of a comment) after the script has already doubled its quotes and brackets, so a login or display
    /// name holding them would reach the file with an unescaped quote or bracket. UTF-8 never turns a non-ASCII character into an ASCII byte,
    /// and SSMS and sqlcmd read a UTF-8 file. The only verb that prints SQL built from operator or registry text; <c>--drop-xe-sessions --print-sql</c>
    /// prints a fixed script.</summary>
    public static TextWriter OpenGrantScriptOutput() =>
        Console.IsOutputRedirected
            ? new StreamWriter(Console.OpenStandardOutput(), new UTF8Encoding(encoderShouldEmitUTF8Identifier: false)) { AutoFlush = true }
            : Console.Out;

    /// <summary>
    /// <c>--grant-script</c> (#6472): prints the login and grant script for a monitored server, or for a host not yet added. It connects to no monitored SQL Server (the <c>server-name</c> form reads the store registry)
    /// and prints no password. The server list is the store's registry, with darling.json's own list when the store cannot be
    /// reached, as <c>--validate-config</c> resolves it. Exit 0 with the script on stdout; 1 for an argument, configuration or name problem.
    /// </summary>
    public static async Task<int> GrantScriptAsync(
        string[] rest, TextWriter output, TextWriter error, CancellationToken cancellationToken,
        Func<string?>? readServiceAccount = null, string? machineName = null, string? domainName = null)
    {
        if (!TryParseGrantScriptArgs(rest, out var args, out var argError))
        {
            error.WriteLine(argError);
            output.WriteLine(GrantScriptUsageText());
            return 1;
        }

        string host, label;
        string? auth, username;
        if (args!.ServerName is { } typed)
        {
            DarlingConfig config;
            try
            {
                config = LoadConfigForGrantScript(args.ConfigPath);
            }
            catch (Exception ex)
            {
                error.WriteLine($"Could not load configuration: {ex.Message}");
                return 1;
            }

            var targets = await ResolveValidationTargetsAsync(config, error, error, cancellationToken, GrantScriptRegistryWording);
            if (targets is null)
            {
                return 1;
            }

            var matches = MatchDropXeSessionsTarget(targets, typed);
            if (matches.Count != 1)
            {
                error.WriteLine(matches.Count == 0
                    ? $"No monitored server named '{typed}' is in the store's registry or the configuration."
                    : $"'{typed}' names {matches.Count} monitored servers: {string.Join(", ", matches.Select(m => m.StorageName))}. Use the storage name.");
                error.WriteLine("For a host not yet added, use --grant-script --host <host>.");
                return 1;
            }

            var server = matches[0];
            if (server.IsPostgres)
            {
                error.WriteLine($"'{server.DisplayName}' is a PostgreSQL target. It needs pg_monitor, not this script; see the Darling operator guide.");
                return 1;
            }

            host = server.Host;
            label = server.DisplayName;
            auth = server.Auth;
            username = server.Username;
        }
        else
        {
            host = args.Host!;
            label = host;
            auth = args.Auth ?? "integrated";
            username = args.Username;
        }

        /* No --service-account and no registry key: the service is not installed on this machine, so its account is unknown here and the script
           names a placeholder (as the Viewer's Add Server dialog does) rather than this machine's own account. */
        var serviceAccount = args.ServiceAccount ?? (readServiceAccount ?? MonitoringGrantScript.ReadServiceAccountName)();
        var inputs = new GrantPrincipalInputs(
            auth, username, host,
            serviceAccount,
            machineName ?? Environment.MachineName,
            domainName ?? Environment.UserDomainName,
            ServiceNotOnThisMachine: serviceAccount is null);
        var azure = args.Azure || MonitoringGrantScript.IsAzureSqlDatabaseHost(host);
        var rds = args.Rds || MonitoringGrantScript.IsAwsRdsHost(host);
        output.Write(MonitoringGrantScript.Build(MonitoringGrantScript.ChoosePrincipal(inputs), args.FinOps, azure, label, awsRds: rds));
        return 0;
    }
}
