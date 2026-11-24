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
/// password prompt, because Lite is a GUI-subsystem exe and a shell does not wait for one, so the shell and the exe would both read
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

    private const string RunningLiteRefusal =
        "Performance Monitor Lite is running. It keeps the server list in memory and writes it back, so a server added now would be lost. "
        + "Run PerformanceMonitorLite.exe --stop, then --add-server again, then start Lite: it collects from the new server from that start.";

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
                /* The position, never the text: a stray value can be a password typed in the wrong place. */
                errorMessage = $"Unexpected argument {i + 1}. --add-server takes --host, --name, --auth, --username (--login) and --database, each with a value.";
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
                /* The position, never the text: a password typed as an option (-PSecret, -Password:Secret) would be echoed back. */
                errorMessage = $"Unknown option at argument {i + 1}. --add-server takes --host, --name, --auth, --username (--login) and --database.";
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
                        + "There is no password prompt (this program has no console of its own, so a shell and the program would both read the keys). "
                        + "Pipe the password as one line, or pipe a JSON array with a password field.");

                password = input.ReadLine();
                if (string.IsNullOrEmpty(password))
                    return (null, "No password was read from stdin, so no server was added.");
            }

            return ([new Entry(flags.Host.Trim(), NullIfBlank(flags.DisplayName), auth, NullIfBlank(flags.Username), password, NullIfBlank(flags.Database))], null);
        }

        if (!inputRedirected)
            return (null, "No server was added: stdin is not redirected, so there is no JSON array to read, and no flag names a server. "
                + "There is no prompt (this program has no console of its own, so a shell and the program would both read the keys). "
                + "Pipe a JSON array of servers on stdin, or name one server with --host.");

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
            /* Only the position: the driver's own text quotes the input from the bad token to the end, and that can be a password. */
            return (null, $"The JSON on stdin is not valid at line {(ex.LineNumber ?? 0) + 1}, byte {(ex.BytePositionInLine ?? 0) + 1}. It must be an array of servers.");
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
            error.WriteLine(RunningLiteRefusal);
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
            var listed = manager.GetAllServers();
            var existing = listed.FirstOrDefault(s => ServerManager.IsSameServer(s, candidate));
            if (existing is null && listed.FirstOrDefault(s => ServerManager.IsSameServerIgnoringCase(s, candidate)) is { } spelledAnotherWay)
            {
                /* The listed entry is the same server in another letter case. The id hashes the spelling, so it would not collide and a
                   second entry for the one server would be added. A case-blind match here would skip the ordinal host check, so it is refused. */
                error.WriteLine($"{label}: the listed entry '{ServerHostChange.Shown(ServerManager.NameForMessage(spelledAnotherWay))}' (host '{ServerHostChange.Shown(spelledAnotherWay.ServerName)}', "
                    + $"database '{ServerHostChange.Shown(spelledAnotherWay.DatabaseName)}') is this server spelled in another letter case, so nothing was changed. "
                    + "Add this server with the host and database spelled as the entry has them.");
                return ExitRefused;
            }

            if (existing is null && manager.FindServerIdHolder(candidate) is { } holder)
            {
                error.WriteLine($"{label}: {ServerManager.DescribeIdHolder(candidate, holder)}");
                return ExitRefused;
            }

            if (existing is not null)
            {
                /* The storage name joins the host and the database, so a host such as "tcp:sql01" matches an entry for host "tcp" and
                   database "sql01". It is the same server only when the host and the database each match. */
                if (!string.Equals(existing.ServerName, entry.Host, StringComparison.Ordinal)
                    || !string.Equals(BlankToEmpty(existing.DatabaseName), BlankToEmpty(entry.Database), StringComparison.Ordinal))
                {
                    error.WriteLine($"{label}: the listed entry '{ServerHostChange.Shown(ServerManager.NameForMessage(existing))}' (host '{ServerHostChange.Shown(existing.ServerName)}', "
                        + $"database '{ServerHostChange.Shown(existing.DatabaseName)}') has the same storage name as this server but a different host or database, so nothing was changed. "
                        + "Remove that entry, or add this server with the host and database spelled as the entry has them.");
                    return ExitRefused;
                }

                /* Two entries with one Id: an update replaces the first of them, which may not be the one matched here. */
                if (listed.FirstOrDefault(s => !ReferenceEquals(s, existing) && s.Id == existing.Id) is { } sameId)
                {
                    error.WriteLine($"{label}: the listed entries '{ServerHostChange.Shown(ServerManager.NameForMessage(existing))}' and '{ServerHostChange.Shown(ServerManager.NameForMessage(sameId))}' "
                        + "have the same Id, so an update could replace the wrong one and nothing was changed. Remove one of them in the Add Server window or Manage Servers and run this again.");
                    return ExitRefused;
                }

                /* A saved password is written only for a server whose Id is a GUID, so the entry would be "updated" with no password stored. */
                if (entry.AuthenticationType is AuthenticationTypes.SqlServer or AuthenticationTypes.ServicePrincipal && !ServerCredentialIds.IsUsable(existing.Id))
                {
                    error.WriteLine($"{label}: the listed entry for this server has an Id that is not a GUID, so Lite stores no password for it and nothing was changed. "
                        + "Remove the entry and add the server again to give it a normal Id.");
                    return ExitRefused;
                }

                /* The entry's own settings are in a file any local account can change. The connection test below and the stored password
                   would use them, so one this Windows account has not confirmed is refused here, before anything connects. */
                var unconfirmed = UnconfirmedSettings(existing, manager.HostChangeIfSaved(existing), keepsManagedIdentityClientId: entry.AuthenticationType == AuthenticationTypes.ManagedIdentity);
                if (unconfirmed.Count > 0)
                {
                    error.WriteLine($"{label}: the listed entry for this server holds {string.Join(", ", unconfirmed)} that this Windows account has not confirmed, so nothing was changed. "
                        + "Confirm the setting in the Add Server window, or remove the entry and add the server again.");
                    return ExitRefused;
                }
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

        /* The tests above take seconds each: a Lite started meanwhile would write the list back over this run. */
        if (env.LiteIsRunning())
        {
            error.WriteLine(RunningLiteRefusal);
            return ExitRefused;
        }

        /* Everything passed: only now is anything written. The manager saves the list and confirms the host before it writes the
           credential, so a refused or failed credential write part way through would leave a partial result: the run undoes it. */
        var undo = WriteUndo.Take(manager, Path.Combine(env.HostRecordFolder, ServerHostBindings.FileName), planned.Select(p => p.Server).ToList());
        var results = new List<string>();
        var reached = 0;
        foreach (var (entry, server, exists) in planned)
        {
            reached++;
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
                foreach (var line in undo.Restore(reached))
                    error.WriteLine(line);
                return ExitRefused;
            }

            results.Add($"{(exists ? "Updated" : "Added")} {server.DisplayName} ({server.ServerName}).");
        }

        /* The manager reports a failed write of the host record instead of throwing, and the run would then say Added. */
        if (manager.HostRecordSaveProblem is { } recordProblem)
        {
            error.WriteLine(recordProblem);
            foreach (var line in undo.Restore(reached))
                error.WriteLine(line);
            return ExitRefused;
        }

        foreach (var line in results)
            output.WriteLine(line);

        output.WriteLine($"Credentials were stored for the Windows account {env.AccountName}.");
        output.WriteLine($"Server list: {manager.ConfigFilePath}");
        output.WriteLine($"Host confirmation: {Path.Combine(env.HostRecordFolder, ServerHostBindings.FileName)}");
        output.WriteLine("Start Lite to collect from these servers. A Lite that is already running does not see them until it is stopped and started again.");
        return ExitAdded;
    }

    /// <summary>
    /// The settings a listed entry holds that this Windows account's host record does not confirm, which the Add Server window would ask
    /// about: a Query Store replica, a utility database, a certificate trusted without a check, and encryption that is Optional (or
    /// weaker than the confirmed one), and a managed identity client id the run keeps (<paramref name="keepsManagedIdentityClientId"/>).
    /// Every setting counts as unconfirmed when the record is not for this host. Empty when <paramref name="change"/> is null (the
    /// record confirms the entry).
    /// </summary>
    internal static List<string> UnconfirmedSettings(ServerConnection server, ServerHostChange? change, bool keepsManagedIdentityClientId)
    {
        var names = new List<string>();
        if (change is null)
            return names;

        /* A record for another host (the entry was renamed, or its Id is shared) confirms nothing about this one. */
        var neverConfirmed = change.HostChanged;
        bool Asked(string setting) => change.Settings.Any(s => s.Name == setting);

        if (change.ReplicaChanged || (neverConfirmed && !string.IsNullOrWhiteSpace(server.QueryStoreReplica)))
            names.Add("a Query Store replica");
        if (!string.IsNullOrWhiteSpace(server.UtilityDatabase) && (neverConfirmed || Asked(ServerHostBindings.UtilityDatabaseSetting)))
            names.Add("a utility database");
        if (server.TrustServerCertificate && (neverConfirmed || Asked(ServerHostBindings.CertificateSetting)))
            names.Add("TrustServerCertificate");
        if (neverConfirmed ? ServerHostBindings.EncryptRank(server.EncryptMode) == 0 : Asked(ServerHostBindings.EncryptionSetting))
            names.Add("encryption that is Optional or weaker than the confirmed one");
        if (keepsManagedIdentityClientId && !string.IsNullOrWhiteSpace(server.ManagedIdentityClientId)
            && (neverConfirmed || Asked(ServerHostBindings.ManagedIdentityClientIdSetting)))
            names.Add("a managed identity client id");
        return names;
    }

    /// <summary>
    /// What the run holds before it writes: the server list file and the host record file as bytes (absent is remembered as absent)
    /// and each listed server's saved credential. <see cref="Restore"/> puts them back.
    /// </summary>
    private sealed class WriteUndo
    {
        private readonly CredentialService _credentials;
        private readonly (string Description, string Path, byte[]? Bytes)[] _files;
        private readonly List<(int Index, ServerConnection Server, CredentialReadOutcome Outcome, (string Username, string Password)? Credential)> _saved = [];

        private WriteUndo(CredentialService credentials, (string Description, string Path, byte[]? Bytes)[] files)
        {
            _credentials = credentials;
            _files = files;
        }

        internal static WriteUndo Take(ServerManager manager, string hostRecordPath, List<ServerConnection> servers)
        {
            static byte[]? Bytes(string path) => File.Exists(path) ? File.ReadAllBytes(path) : null;
            var undo = new WriteUndo(manager.CredentialService, [
                ("the server list", manager.ConfigFilePath, Bytes(manager.ConfigFilePath)),
                ("the host confirmation", hostRecordPath, Bytes(hostRecordPath))]);
            for (var i = 0; i < servers.Count; i++)
            {
                if (!ServerCredentialIds.IsUsable(servers[i].Id))
                    continue;
                var outcome = manager.CredentialService.TryReadCredential(servers[i].Id, out var credential, out _);
                undo._saved.Add((i, servers[i], outcome, credential));
            }

            return undo;
        }

        /// <summary>Puts the files back and the credentials of the first <paramref name="reached"/> servers; returns the lines to print.</summary>
        internal List<string> Restore(int reached)
        {
            var problems = new List<string>();
            foreach (var (description, path, bytes) in _files)
            {
                try
                {
                    if (bytes is null)
                    {
                        if (File.Exists(path))
                            File.Delete(path);
                    }
                    else
                    {
                        File.WriteAllBytes(path, bytes);
                    }
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    problems.Add($"{description} ({path}) could not be put back and may still hold the changes of this run.");
                }
            }

            foreach (var (_, server, outcome, credential) in _saved.Where(saved => saved.Index < reached))
            {
                var name = ManagerLabel(server);
                try
                {
                    var restored = outcome switch
                    {
                        CredentialReadOutcome.Found => _credentials.SaveCredential(server.Id, credential!.Value.Username, credential.Value.Password),
                        CredentialReadOutcome.NotFound => _credentials.TryDeleteCredential(server.Id).Succeeded,
                        _ => false
                    };
                    if (!restored)
                        problems.Add($"the saved credential for {name} could not be put back and may hold the password of this run.");
                }
                catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or IOException)
                {
                    problems.Add($"the saved credential for {name} could not be put back and may hold the password of this run.");
                }
            }

            return
            [
                problems.Count == 0
                    ? "The server list, the host confirmation and the saved credentials were put back as they were before this run."
                    : "This run could not put everything back: " + string.Join(" ", problems)
            ];
        }
    }

    /// <summary>The storage name reads a blank database as none, so the same-server check does too.</summary>
    private static string BlankToEmpty(string? value) => string.IsNullOrWhiteSpace(value) ? "" : value;

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
