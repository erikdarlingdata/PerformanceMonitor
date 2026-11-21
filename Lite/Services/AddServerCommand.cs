/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Principal;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>The non-secret fields <c>--add-server</c> takes as flags. There is no password flag: an argument shows in the process list.</summary>
internal sealed record AddServerFlags(string? Host = null, string? DisplayName = null, string? Auth = null, string? Username = null, string? Database = null)
{
    internal bool Any => Host is not null || DisplayName is not null || Auth is not null || Username is not null || Database is not null;
}

/// <summary>
/// #9237: <c>PerformanceMonitorLite.exe --add-server</c> (also <c>--add-servers</c>) adds monitored servers without the window,
/// like Darling's verb of the same name. A headless Lite runs as a user nobody signs in as, so there was no window to add a
/// server or store its password in. The verb runs as that user: it writes the shared servers.json, stores the password in that
/// user's Credential Manager (target <c>PerformanceMonitorLite_</c> plus the server id) and records that user's host confirmation.
///
/// <para>The servers come from a JSON array on standard input (fields <c>host</c>, <c>display_name</c>, <c>auth</c>,
/// <c>username</c>, <c>password</c>, <c>database</c>, as Darling's) or from flags for one server. A password is only ever read
/// from standard input, one line when flags name the server: never an argument, never echoed, never logged. There is no hidden
/// keyboard prompt, because Lite is a GUI-subsystem exe and a shell does not wait for one, so the shell and the exe would both read
/// the keyboard.</para>
///
/// <para>Every entry is checked (host rule, credential rule, id rule, connection test) before anything is written, and one refused
/// entry ends the run with exit code 1 and no change. A server already listed (same address, database and read-only intent) is
/// updated in place, with the new password, and gets no second entry. A running Lite keeps the server list in memory and writes
/// it back, so the verb refuses to run beside one: <c>--stop</c> first. The new server is collected from the next start.</para>
/// </summary>
internal static class AddServerCommand
{
    internal const int ExitAdded = 0;
    internal const int ExitRefused = 1;

    /// <summary>One server to add, as read from JSON or flags and normalised.</summary>
    internal sealed record Entry(string Host, string? DisplayName, string AuthenticationType, string? Username, string? Password, string? Database);

    /// <summary>What the verb needs from the machine, so a test runs it with none of it.</summary>
    internal sealed record Machine(
        Func<ServerManager> ManagerFactory,
        Func<bool> LiteIsRunning,
        Func<ServerConnection, string?, string?, Task<string?>> TestConnection,
        string AccountName,
        string HostRecordFolder);

    internal static string AuthWord(string value) => value.Trim().ToLowerInvariant() switch
    {
        "windows" or "integrated" => AuthenticationTypes.Windows,
        "sql" or "sqlserver" or "sql server" => AuthenticationTypes.SqlServer,
        "serviceprincipal" or "service principal" => AuthenticationTypes.ServicePrincipal,
        "managedidentity" or "managed identity" => AuthenticationTypes.ManagedIdentity,
        "entradefault" or "entradefaultcredential" => AuthenticationTypes.EntraDefaultCredential,
        _ => string.Empty
    };

    /// <summary>
    /// Parses the arguments after the verb: <c>--host</c>, <c>--name</c>, <c>--auth</c>, <c>--username</c> (or <c>--login</c>) and
    /// <c>--database</c>, each as <c>--flag value</c> or <c>--flag=value</c>. A password flag is refused with the reason, and so is
    /// anything else.
    /// </summary>
    internal static bool TryParseArgs(IEnumerable<string> args, out AddServerFlags flags, out string? errorMessage)
    {
        errorMessage = null;
        string? host = null, displayName = null, auth = null, username = null, database = null;
        var rest = args
            .Where(a => !string.Equals(a, LiteStartupPlan.AddServerArg, StringComparison.OrdinalIgnoreCase)
                && !string.Equals(a, LiteStartupPlan.AddServersArg, StringComparison.OrdinalIgnoreCase))
            .ToArray();

        for (var i = 0; i < rest.Length; i++)
        {
            var arg = rest[i];
            var name = arg;
            string? value = null;
            var equals = arg.IndexOf('=');
            if (equals > 0)
            {
                name = arg[..equals];
                value = arg[(equals + 1)..];
            }

            if (!name.StartsWith('-'))
            {
                errorMessage = $"Unexpected argument '{arg}'. --add-server takes --host, --name, --auth, --username (--login) and --database.";
                break;
            }

            if (name.Equals("--password", StringComparison.OrdinalIgnoreCase) || name.Equals("--pwd", StringComparison.OrdinalIgnoreCase))
            {
                errorMessage = "There is no password flag: an argument shows in the process list and in shell history. "
                    + "Pipe the password on stdin (one line, with flags) or pipe a JSON array on stdin.";
                break;
            }

            if (!(name.Equals("--host", StringComparison.OrdinalIgnoreCase) || name.Equals("--name", StringComparison.OrdinalIgnoreCase)
                || name.Equals("--auth", StringComparison.OrdinalIgnoreCase) || name.Equals("--username", StringComparison.OrdinalIgnoreCase)
                || name.Equals("--login", StringComparison.OrdinalIgnoreCase) || name.Equals("--database", StringComparison.OrdinalIgnoreCase)))
            {
                errorMessage = $"Unknown option '{name}'. --add-server takes --host, --name, --auth, --username (--login) and --database.";
                break;
            }

            if (value is null)
            {
                if (i + 1 >= rest.Length || rest[i + 1].StartsWith('-'))
                {
                    errorMessage = $"{name} needs a value: {name} <value>";
                    break;
                }

                value = rest[++i];
            }

            switch (name.ToLowerInvariant())
            {
                case "--host": host = value; break;
                case "--name": displayName = value; break;
                case "--auth": auth = value; break;
                case "--username":
                case "--login": username = value; break;
                default: database = value; break;
            }
        }

        flags = new AddServerFlags(host, displayName, auth, username, database);
        return errorMessage is null;
    }

    /// <summary>Normalises the entries: the JSON array, or the one entry the flags name with its password read from <paramref name="input"/>.</summary>
    internal static (List<Entry>? Entries, string? Problem) ReadEntries(AddServerFlags flags, TextReader input, bool inputRedirected)
    {
        if (flags.Any)
        {
            if (string.IsNullOrWhiteSpace(flags.Host))
                return (null, "--host is required when a server is given as flags: --add-server --host <host> [--name <name>] [--auth windows|sql|serviceprincipal|managedidentity|entradefault] [--username <login>] [--database <db>]");

            var auth = string.IsNullOrWhiteSpace(flags.Auth) ? AuthenticationTypes.Windows : AuthWord(flags.Auth);
            if (auth.Length == 0)
                return (null, AuthRefusal(flags.Auth!));

            string? password = null;
            if (auth is AuthenticationTypes.SqlServer or AuthenticationTypes.ServicePrincipal)
            {
                if (!inputRedirected)
                    return (null, "A password is read from stdin, and stdin is not redirected, so no server was added. "
                        + "There is no keyboard prompt (this program has no console of its own, so a shell and the program would both read the keys). "
                        + "Pipe the password as one line, or pipe a JSON array with a password field.");

                password = input.ReadLine();
                if (string.IsNullOrEmpty(password))
                    return (null, "No password was read from stdin, so no server was added.");
            }

            return ([new Entry(flags.Host.Trim(), NullIfBlank(flags.DisplayName), auth, NullIfBlank(flags.Username), password, NullIfBlank(flags.Database))], null);
        }

        string json;
        try
        {
            json = input.ReadToEnd();
        }
        catch (IOException)
        {
            json = string.Empty;
        }

        if (string.IsNullOrWhiteSpace(json))
            return (null, UsageText());

        JsonElement root;
        try
        {
            using var doc = JsonDocument.Parse(json);
            root = doc.RootElement.Clone();
        }
        catch (JsonException ex)
        {
            return (null, $"stdin is not valid JSON ({ex.Message.Split('.')[0]}). It must be an array of servers.");
        }

        if (root.ValueKind != JsonValueKind.Array)
            return (null, "stdin must be a JSON array of servers, even for one server.");

        var entries = new List<Entry>();
        var number = 0;
        foreach (var item in root.EnumerateArray())
        {
            number++;
            if (item.ValueKind != JsonValueKind.Object)
                return (null, $"Entry {number} is not an object.");

            string? Field(string name)
            {
                foreach (var property in item.EnumerateObject())
                {
                    if (property.Name.Equals(name, StringComparison.OrdinalIgnoreCase) && property.Value.ValueKind == JsonValueKind.String)
                        return property.Value.GetString();
                }

                return null;
            }

            var host = NullIfBlank(Field("host"));
            if (host is null)
                return (null, $"Entry {number}: host is required.");

            var engine = NullIfBlank(Field("engine"));
            if (engine is not null && !engine.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
                return (null, $"Entry {number}: Lite monitors SQL Server only, so engine '{engine}' is refused.");

            var authText = NullIfBlank(Field("auth"));
            var auth = authText is null ? AuthenticationTypes.Windows : AuthWord(authText);
            if (auth.Length == 0)
                return (null, $"Entry {number}: {AuthRefusal(authText!)}");

            entries.Add(new Entry(host, NullIfBlank(Field("display_name")), auth, NullIfBlank(Field("username")), Field("password"), NullIfBlank(Field("database"))));
        }

        return entries.Count == 0 ? (null, "The array is empty, so no server was added.") : (entries, null);
    }

    private static string AuthRefusal(string word) =>
        $"Authentication '{word}' is not supported here. Use windows, sql, serviceprincipal, managedidentity or entradefault. "
        + "Microsoft Entra MFA and device code need someone to sign in, which this verb does not have: add that server in the window.";

    private static string? NullIfBlank(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    internal static string UsageText() =>
        "Nothing arrived on stdin, so no server was added." + System.Environment.NewLine
        + "--add-server reads a JSON ARRAY of servers from stdin: [{\"host\":\"sql01\",\"auth\":\"sql\",\"username\":\"login\",\"password\":\"...\"}]" + System.Environment.NewLine
        + "or one server from flags: --host <host> [--name <name>] [--auth windows|sql|serviceprincipal|managedidentity|entradefault] [--username <login>] [--database <db>]," + System.Environment.NewLine
        + "with the password piped on stdin as one line. There is no password flag. Run it as the Windows user that runs Lite: the password is stored for that user.";

    /// <summary>
    /// Runs the verb over <paramref name="env"/>. Exit 0 when every entry was added or updated, 1 when the run was refused (the reason
    /// on <paramref name="error"/>, nothing changed).
    /// </summary>
    internal static async Task<int> RunAsync(
        IEnumerable<string> args, TextReader input, bool inputRedirected, TextWriter output, TextWriter error, Machine env)
    {
        if (!TryParseArgs(args, out var flags, out var argError))
        {
            error.WriteLine(argError);
            return ExitRefused;
        }

        /* A running Lite holds the server list in memory and writes it back on its own changes (a favorite, a last-connected time,
           a setting), so a server added to the file now would be lost, and it would not be collected until a restart anyway. */
        if (env.LiteIsRunning())
        {
            error.WriteLine("Performance Monitor Lite is running. It keeps the server list in memory and writes it back, so a server added now would be lost. "
                + "Run PerformanceMonitorLite.exe --stop, then --add-server again, then start Lite: it collects from the new server from that start.");
            return ExitRefused;
        }

        var (entries, problem) = ReadEntries(flags, input, inputRedirected);
        if (entries is null)
        {
            if (problem == UsageText())
                output.WriteLine(problem);
            else
                error.WriteLine(problem);
            return ExitRefused;
        }

        /* The rules that need no server list run first, so a refused entry never even opens (or creates) servers.json. */
        var pending = new List<(Entry Entry, string Label, ServerConnection Candidate)>();
        var number = 0;
        foreach (var entry in entries)
        {
            number++;
            var label = $"Entry {number} ({entry.Host})";
            var candidate = new ServerConnection { ServerName = entry.Host, DatabaseName = entry.Database };

            if (ServerManager.HostRefusal(entry.Host, null) is { } hostRefusal)
            {
                error.WriteLine($"{label}: {hostRefusal}");
                return ExitRefused;
            }

            if (ServerEntryRules.CredentialRefusal(entry.AuthenticationType, entry.Username, entry.Password) is { } credentialRefusal)
            {
                error.WriteLine($"{label}: {credentialRefusal}");
                return ExitRefused;
            }

            if (entry.AuthenticationType == AuthenticationTypes.SqlServer && string.IsNullOrEmpty(entry.Password))
            {
                error.WriteLine($"{label}: a password is required for SQL Server authentication.");
                return ExitRefused;
            }

            if (pending.Any(p => ServerManager.IsSameServerIgnoringCase(p.Candidate, candidate)))
            {
                error.WriteLine($"{label}: the same server is listed twice.");
                return ExitRefused;
            }

            pending.Add((entry, label, candidate));
        }

        var manager = env.ManagerFactory();
        var planned = new List<(Entry Entry, ServerConnection Server, bool Exists)>();
        foreach (var (entry, label, candidate) in pending)
        {
            var existing = manager.GetAllServers().FirstOrDefault(s => ServerManager.IsSameServer(s, candidate));
            if (existing is null && manager.FindServerIdHolder(candidate) is { } holder)
            {
                error.WriteLine($"{label}: {ServerManager.DescribeIdHolder(candidate, holder)}");
                return ExitRefused;
            }

            var server = existing ?? new ServerConnection { ServerName = entry.Host, DatabaseName = entry.Database };
            if (existing is null)
                server.DisplayName = entry.DisplayName ?? entry.Host;
            else if (entry.DisplayName is not null)
                server.DisplayName = entry.DisplayName;
            server.AuthenticationType = entry.AuthenticationType;
            server.CredentialProfileId = null;
            server.AzureClientId = entry.AuthenticationType == AuthenticationTypes.ServicePrincipal ? entry.Username : null;
            if (entry.AuthenticationType != AuthenticationTypes.ManagedIdentity)
                server.ManagedIdentityClientId = null;

            var failure = await env.TestConnection(server, entry.Username, entry.Password);
            if (failure is not null)
            {
                error.WriteLine($"{label}: could not connect: {failure}");
                return ExitRefused;
            }

            planned.Add((entry, server, existing is not null));
        }

        /* Everything passed: only now is anything written. */
        foreach (var (entry, server, exists) in planned)
        {
            try
            {
                if (exists)
                {
                    manager.UpdateServer(server, entry.Username, entry.Password);
                    manager.ConfirmHost(server);
                }
                else
                {
                    manager.AddServer(server, entry.Username, entry.Password);
                }
            }
            catch (Exception ex)
            {
                error.WriteLine($"{ManagerLabel(server)}: {ex.Message}");
                return ExitRefused;
            }

            output.WriteLine($"{(exists ? "Updated" : "Added")} {server.DisplayName} ({server.ServerName}).");
        }

        output.WriteLine($"Credentials were stored for the Windows account {env.AccountName}.");
        output.WriteLine($"Server list: {manager.ConfigFilePath}");
        output.WriteLine($"Host confirmation: {Path.Combine(env.HostRecordFolder, ServerHostBindings.FileName)}");
        output.WriteLine("Start Lite to collect from these servers. A Lite that is already running does not see them until it is stopped and started again.");
        return ExitAdded;
    }

    private static string ManagerLabel(ServerConnection server) => ServerManager.NameForMessage(server);

    /// <summary>The process run: joins the parent's console, then <see cref="RunAsync"/> over the real machine.</summary>
    internal static int RunProcess(string[] args)
    {
        if (!ParentConsole.TryOpen(out var output, out var error) || output is null)
        {
            return ExitRefused;
        }

        var input = ParentConsole.OpenInput(out var redirected);
        var hostFolder = Path.Combine(
            System.Environment.GetFolderPath(System.Environment.SpecialFolder.LocalApplicationData), DataRootMigration.DataRootName, "config");
        var env = new Machine(
            /* Made only once an entry has passed the rules that need no list: creating it creates servers.json, and the shared folder. */
            () => new ServerManager(
                App.ResolveSharedConfigDirectory(hostFolder),
                hostBindingsPath: Path.Combine(hostFolder, ServerHostBindings.FileName)),
            () => OtherLiteRunning(),
            (server, username, password) => ServerEntryRules.TestConnectionAsync(server, username, password, CancellationToken.None),
            WindowsIdentity.GetCurrent().Name,
            hostFolder);
        return RunAsync(args, input, redirected, output, error, env).GetAwaiter().GetResult();
    }

    /// <summary>True when another Lite process exists in any session and for any user: the shared servers.json is theirs to rewrite too.</summary>
    private static bool OtherLiteRunning()
    {
        var self = System.Environment.ProcessId;
        foreach (var process in Process.GetProcessesByName("PerformanceMonitorLite"))
        {
            using (process)
            {
                if (process.Id != self)
                    return true;
            }
        }

        return false;
    }
}
