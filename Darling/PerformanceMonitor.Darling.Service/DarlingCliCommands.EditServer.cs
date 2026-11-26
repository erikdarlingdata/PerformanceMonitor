/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/* --edit-server (#9693): changes a registered server's settings from the service host's command line. After the first start the
   registry lives in the store, so a headless host with the web dashboard and MCP off has no other way to change one. The verb owns
   the argument grammar and the printed answer only: the change itself is the MCP edit_server path (parse, plan, probe, the store's
   edit function), and enabled is the MCP enable_server and disable_server path. */
public static partial class DarlingCliCommands
{
    /// <summary>The verb <see cref="EditServerAsync"/> handles: <c>--edit-server &lt;name&gt; --set &lt;field&gt;=&lt;value&gt;</c>.</summary>
    public static bool IsEditServerVerb(string arg) =>
        string.Equals(arg, "--edit-server", StringComparison.OrdinalIgnoreCase);

    /// <summary>How a <c>--set</c> value is put into the edit request.</summary>
    internal enum EditServerValueKind
    {
        Text,
        Boolean,
        Integer,
        Auth,
    }

    /// <summary>One field <c>--set</c> takes: the name the operator types (the darling.json key, which is also what the start-up drift
    /// warning prints), the edit_server key it becomes, and how its value is read.</summary>
    internal sealed record EditServerField(string Name, string EditKey, EditServerValueKind Kind);

    /// <summary>The field <c>--set enabled=</c> names. It is not an edit_server key: the verb sends it to enable_server or disable_server.</summary>
    internal const string EditServerEnabledField = "enabled";

    /// <summary>Every field <c>--set</c> takes besides <see cref="EditServerEnabledField"/>. A field edit_server takes and this list
    /// leaves out (the AWS role and external ID, plan capture, the alert delivery override) is not settable here.</summary>
    internal static readonly IReadOnlyList<EditServerField> EditServerFields =
    [
        new("name", "display_name", EditServerValueKind.Text),
        new("host", "host", EditServerValueKind.Text),
        new("port", "port", EditServerValueKind.Integer),
        new("database", "database", EditServerValueKind.Text),
        new("auth", "auth", EditServerValueKind.Auth),
        new("username", "username", EditServerValueKind.Text),
        new("encryptMode", "encrypt_mode", EditServerValueKind.Text),
        new("trustServerCertificate", "trust_server_certificate", EditServerValueKind.Boolean),
        new("readOnlyIntent", "read_only_intent", EditServerValueKind.Boolean),
        new("multiSubnetFailover", "multi_subnet_failover", EditServerValueKind.Boolean),
        new("queryStoreReplica", "query_store_replica", EditServerValueKind.Text),
        new("monthlyCostUsd", "monthly_cost_usd", EditServerValueKind.Text),
    ];

    /// <summary>The field a <c>--set</c> name means, by its darling.json key or its edit_server key, ignoring case. Null when it is neither.</summary>
    internal static EditServerField? FindEditServerField(string name) =>
        EditServerFields.FirstOrDefault(f =>
            string.Equals(f.Name, name, StringComparison.OrdinalIgnoreCase)
            || string.Equals(f.EditKey, name, StringComparison.OrdinalIgnoreCase));

    /// <summary>True when <c>--edit-server</c> can set the darling.json key <paramref name="fileKey"/>.</summary>
    internal static bool IsEditServerSettable(string fileKey) => FindEditServerField(fileKey) is not null;

    /// <summary>What the arguments ask for: the server, the edit_server request built from the <c>--set</c> pairs (null when they name
    /// only <c>enabled</c>), and the enabled state when one is named.</summary>
    internal sealed record EditServerRequest(string ServerName, string? ConfigPath, JsonObject? Changes, bool? Enabled);

    private const string EditServerPasswordRefusal =
        "A password is never an argument, so --set does not take one. When a change needs the server's password, --edit-server asks for it.";

    private static string EditServerFieldList() =>
        string.Join(", ", EditServerFields.Select(f => f.Name)) + ", " + EditServerEnabledField;

    public static string EditServerUsageText() =>
        "Usage:" + Environment.NewLine +
        "  " + OperatorVerbForm.EntryPointHere() + " --edit-server <server name> --set <field>=<value> [--set <field>=<value> ...] [--config <path to darling.json>]" + Environment.NewLine +
        Environment.NewLine +
        "Changes a registered server's settings in the store, with the checks the MCP edit_server tool makes. Editing darling.json" + Environment.NewLine +
        "does not do this: after the first start the registry is in the store." + Environment.NewLine +
        Environment.NewLine +
        "Fields: " + EditServerFieldList() + "." + Environment.NewLine +
        "  auth is windows (integrated means the same), sql, serviceprincipal or managedidentity." + Environment.NewLine +
        "  encryptMode is Optional, Mandatory or Strict. port is for a PostgreSQL server." + Environment.NewLine +
        "  trustServerCertificate, readOnlyIntent, multiSubnetFailover and enabled are true or false." + Environment.NewLine +
        Environment.NewLine +
        "A password is never an argument, so --set password=... is refused. A change to how a server with a saved password is" + Environment.NewLine +
        "reached needs that password again: the verb asks for it (hidden at a console, or one line from stdin when input is" + Environment.NewLine +
        "redirected), tests the connection with it, and saves it with the change. A change that fails its test saves nothing." + Environment.NewLine +
        Environment.NewLine +
        "It works while the service runs, and the service applies the change on its next config poll. No restart is needed." + Environment.NewLine +
        Environment.NewLine +
        "Example:" + Environment.NewLine +
        "  " + OperatorVerbForm.EntryPointHere() + " --edit-server sql01 --set trustServerCertificate=true --set encryptMode=Mandatory";

    /// <summary>
    /// Parses the arguments after <c>--edit-server</c>: the server name first, then <c>--set field=value</c> (or <c>--set=field=value</c>)
    /// one or more times and an optional <c>--config &lt;path&gt;</c>. A password field, a field that is not settable, a field named
    /// twice and a value its kind cannot read are each refused, and no refusal repeats a value. Pure, so the grammar is testable.
    /// </summary>
    internal static bool TryParseEditServerArgs(string[] rest, out EditServerRequest? request, out string? errorMessage)
    {
        request = null;
        errorMessage = null;
        if (rest is null || rest.Length == 0 || rest[0].StartsWith("--", StringComparison.Ordinal) || string.IsNullOrWhiteSpace(rest[0]))
        {
            errorMessage = "--edit-server needs the server's name first." + Environment.NewLine + Environment.NewLine + EditServerUsageText();
            return false;
        }

        string? configPath = null;
        bool? enabled = null;
        var changes = new JsonObject();
        var named = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 1; i < rest.Length; i++)
        {
            var arg = rest[i];
            string? inline = null;
            var equals = arg.IndexOf('=', StringComparison.Ordinal);
            if (arg.StartsWith("--", StringComparison.Ordinal) && equals > 0)
            {
                inline = arg[(equals + 1)..];
                arg = arg[..equals];
            }

            var isSet = string.Equals(arg, "--set", StringComparison.OrdinalIgnoreCase);
            if (!isSet && !string.Equals(arg, "--config", StringComparison.OrdinalIgnoreCase))
            {
                errorMessage = "--edit-server does not take that argument." + Environment.NewLine + Environment.NewLine + EditServerUsageText();
                return false;
            }

            var value = inline ?? (i + 1 < rest.Length ? rest[++i] : null);
            if (value is null)
            {
                errorMessage = isSet ? "--set needs <field>=<value> after it." : "--config needs the path to darling.json after it.";
                return false;
            }

            if (!isSet)
            {
                configPath = value;
                continue;
            }

            var split = value.IndexOf('=', StringComparison.Ordinal);
            var field = (split < 0 ? value : value[..split]).Trim();
            if (field.Equals("password", StringComparison.OrdinalIgnoreCase) || field.Equals("encryptedPassword", StringComparison.OrdinalIgnoreCase))
            {
                errorMessage = EditServerPasswordRefusal;
                return false;
            }

            if (split <= 0)
            {
                errorMessage = "--set takes <field>=<value>. Fields: " + EditServerFieldList() + ".";
                return false;
            }

            var isEnabled = field.Equals(EditServerEnabledField, StringComparison.OrdinalIgnoreCase);
            var known = isEnabled ? null : FindEditServerField(field);
            if (!isEnabled && known is null)
            {
                errorMessage = $"--edit-server cannot set '{field}'. Fields: " + EditServerFieldList() + ".";
                return false;
            }

            var canonical = isEnabled ? EditServerEnabledField : known!.Name;
            if (!named.Add(canonical))
            {
                errorMessage = $"--set names {canonical} more than once.";
                return false;
            }

            var text = value[(split + 1)..];
            if (isEnabled || known!.Kind == EditServerValueKind.Boolean)
            {
                if (!bool.TryParse(text.Trim(), out var flag))
                {
                    errorMessage = $"{canonical} takes true or false.";
                    return false;
                }

                if (isEnabled)
                {
                    enabled = flag;
                }
                else
                {
                    changes[known!.EditKey] = flag;
                }

                continue;
            }

            switch (known.Kind)
            {
                case EditServerValueKind.Integer:
                    if (!int.TryParse(text.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var number))
                    {
                        errorMessage = $"{canonical} takes a whole number.";
                        return false;
                    }

                    changes[known.EditKey] = number;
                    break;
                case EditServerValueKind.Auth:
                    changes[known.EditKey] = AddServerAuthWord(text);
                    break;
                default:
                    changes[known.EditKey] = text;
                    break;
            }
        }

        if (changes.Count == 0 && enabled is null)
        {
            errorMessage = "--edit-server needs at least one --set <field>=<value>." + Environment.NewLine + Environment.NewLine + EditServerUsageText();
            return false;
        }

        request = new EditServerRequest(rest[0], configPath, changes.Count == 0 ? null : changes, enabled);
        return true;
    }

    /// <summary>The name <c>--set</c> uses for an edit_server key, for the answer's list of changed fields.</summary>
    private static string EditServerNameOfKey(string editKey) =>
        EditServerFields.FirstOrDefault(f => string.Equals(f.EditKey, editKey, StringComparison.Ordinal))?.Name ?? editKey;

    /// <summary>
    /// Turns an edit_server answer into the verb's lines and exit code: 0 for <c>updated</c> and <c>unchanged</c>, 1 for every answer
    /// that wrote nothing. <c>StorageName</c> is the saved server's storage name, which finds it again after a rename.
    /// Only the answer's own fields are printed, never the request, so a typed password is not in a line.
    /// </summary>
    internal static (IReadOnlyList<string> Lines, int ExitCode, string? StorageName) FormatEditServerOutcome(string answerJson)
    {
        JsonObject? answer;
        try
        {
            answer = JsonNode.Parse(answerJson) as JsonObject;
        }
        catch (JsonException)
        {
            answer = null;
        }

        string? Text(string key) => answer?[key] is JsonValue v && v.TryGetValue<string>(out var s) ? s : null;

        var status = Text("status");
        var display = Text("display_name") ?? Text("server") ?? "The server";
        switch (status)
        {
            case DarlingMcpServerAdminTools.EditStatus.Updated:
            {
                var changed = (answer!["changed"] as JsonArray)?.Select(c => EditServerNameOfKey(c?.GetValue<string>() ?? "")).ToList() ?? [];
                var lines = new List<string> { $"  Changed {display}: {string.Join(", ", changed)}." };
                if (answer["tested"] is JsonValue tested && tested.TryGetValue<bool>(out var wasTested) && wasTested)
                {
                    lines.Add("  The new settings were tested against the server before they were saved.");
                }

                if (Text("query_store_replica_test") is { Length: > 0 } replicaTest)
                {
                    lines.Add("  Query Store replica: " + replicaTest);
                }

                lines.Add("  The service applies the change on its next config poll. No restart is needed.");
                return (lines, 0, Text("server"));
            }

            case DarlingMcpServerAdminTools.EditStatus.Unchanged:
                return ([$"  {display} already has these values. Nothing was changed."], 0, Text("server"));
            case null:
                return (["  Not changed: " + (Text("message") ?? Text("error") ?? "the store's answer could not be read.")], 1, null);
            default:
            {
                var lines = new List<string> { $"  Not changed ({status}): {Text("message") ?? "no reason was given."}" };
                if (answer!["candidates"] is JsonArray candidates)
                {
                    lines.AddRange(candidates.OfType<JsonObject>().Select(c =>
                        $"    {(c["display_name"] as JsonValue)?.GetValue<string>()} ({(c["server"] as JsonValue)?.GetValue<string>()})"));
                }

                return (lines, 1, null);
            }
        }
    }

    /// <summary>
    /// The verb's body over its two writes, so a test drives it without a store. <paramref name="edit"/> takes an edit_server
    /// request and returns its answer; <paramref name="setEnabled"/> takes a server name and the enabled state and returns the
    /// enable_server or disable_server answer. When the edit answers that it needs the server's password, the password is read
    /// with <paramref name="readPassword"/> and the same request is sent once more with it. The enabled state is applied only
    /// after the edit, if there is one, has succeeded.
    /// </summary>
    internal static async Task<(IReadOnlyList<string> Lines, int ExitCode)> RunEditServerAsync(
        EditServerRequest request,
        Func<string, Task<string>> edit,
        Func<string, bool, Task<string>> setEnabled,
        Func<string, string?> readPassword)
    {
        var lines = new List<string>();
        var target = request.ServerName;
        if (request.Changes is { } changes)
        {
            var answer = await edit(changes.ToJsonString());
            if (DarlingMcpServerAdminTools.IsPasswordNeededAnswer(answer))
            {
                var typed = readPassword($"  Password for {request.ServerName} (the change is tested with it, then saved with it): ");
                if (string.IsNullOrEmpty(typed))
                {
                    lines.Add("  Not changed: this change needs the server's password, and none was entered.");
                    return (lines, 1);
                }

                var withPassword = (JsonObject)changes.DeepClone();
                withPassword["password"] = typed;
                answer = await edit(withPassword.ToJsonString());
            }

            var (editLines, exitCode, storageName) = FormatEditServerOutcome(answer);
            lines.AddRange(editLines);
            if (exitCode != 0)
            {
                if (request.Enabled is not null)
                {
                    lines.Add("  enabled was not changed either.");
                }

                return (lines, exitCode);
            }

            target = storageName ?? target;
        }

        if (request.Enabled is { } enable)
        {
            JsonObject? answer;
            try
            {
                answer = JsonNode.Parse(await setEnabled(target, enable)) as JsonObject;
            }
            catch (JsonException)
            {
                answer = null;
            }

            string? Text(string key) => answer?[key] is JsonValue v && v.TryGetValue<string>(out var s) ? s : null;

            if (Text("status") is "changed" or "unchanged")
            {
                lines.Add("  " + (Text("what_changed") ?? $"enabled is {(enable ? "true" : "false")}."));
            }
            else
            {
                lines.Add("  enabled was not changed: " + (Text("message") ?? Text("error") ?? "the store's answer could not be read."));
                return (lines, 1);
            }
        }

        return (lines, 0);
    }

    /// <summary>
    /// <c>--edit-server &lt;name&gt; --set &lt;field&gt;=&lt;value&gt; ... [--config &lt;path&gt;]</c> (#9693). Runs as the store owner, like
    /// <c>--add-server</c>, and seals a typed password with the key the service keeps. It does not stop or signal the service: the
    /// store bumps its config version on the write, and the running service reloads the registry on its next config poll.
    /// </summary>
    public static async Task<int> EditServerAsync(
        string[] rest, TextReader input, TextWriter output, TextWriter error, CancellationToken cancellationToken,
        Func<string, string?>? readPassword = null)
    {
        if (rest.Length == 0 || IsHelpVerb(rest[0]))
        {
            output.WriteLine(EditServerUsageText());
            return rest.Length == 0 ? 1 : 0;
        }

        if (!TryParseEditServerArgs(rest, out var request, out var argError))
        {
            output.WriteLine(argError);
            return 1;
        }

        DarlingConfig config;
        try
        {
            config = DarlingConfig.Load(request!.ConfigPath);
        }
        catch (Exception ex)
        {
            error.WriteLine($"Could not load configuration: {ex.Message}");
            return 1;
        }

        var postgres = config.Postgres;
        if (postgres is null)
        {
            error.WriteLine("postgres section is required.");
            return 1;
        }

        /* The same platform rule as --add-server: only a managed store's credential (DPAPI) needs Windows. */
        if (postgres.Managed && !OperatingSystem.IsWindows())
        {
            error.WriteLine("A managed Postgres store keeps its credential in DPAPI, so --edit-server needs Windows. "
                + "A bring-your-own store (postgres.connectionString) works on any platform.");
            return 1;
        }

        if (!TryBuildStoreConnectionString(postgres, out var connectionString, out var unusable))
        {
            error.WriteLine(unusable);
            return 1;
        }

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            error.WriteLine(postgres.Managed && OperatingSystem.IsWindows()
                ? DarlingStoreBootstrapEvidence.MissingStoreCredentialMessage(postgres)
                : "postgres.connectionString is empty, so there is no store to change a server in.");
            return 1;
        }

        output.WriteLine();
        output.WriteLine("PerformanceMonitor Darling: change a registered server (--edit-server)");
        output.WriteLine();

        /* The connection test of a Windows-authentication server is also run by the service, as its own account, when the service
           can be asked: the same test --add-server makes. */
        var identityNotes = new List<string>();
        await using var tester = OperatingSystem.IsWindows()
            ? new DarlingServiceConnectionTest.LazyStoreServiceConnectionTester(connectionString)
            : null;
        var consoleIdentity = DarlingServiceConnectionTest.ConsoleIdentity();
        async Task<ConnectionProbeResult> ProbeAsync(MonitoredServer server, CancellationToken token)
        {
            var (probe, notes) = await DarlingServiceConnectionTest.TestAsync(
                server, tester, DarlingMcpServerAdminTools.ProbeAsConsoleUserAsync, consoleIdentity, OperatingSystem.IsWindows(), token);
            identityNotes.AddRange(notes);
            return probe;
        }

        IReadOnlyList<string> lines;
        int exitCode;
        try
        {
            await using var dataSource = NpgsqlDataSource.Create(
                DarlingStoreConnection.PinSessionTimeZoneUtc(
                    DarlingStoreConnection.WithApplicationName(connectionString, DarlingManagedPostgres.CliApplicationName)));
            var sealKey = await DarlingCliSealKey.ResolveAsync(config, DarlingConfig.ResolveConfigPath(request.ConfigPath), dataSource, null, cancellationToken);
            (lines, exitCode) = await RunEditServerAsync(
                request,
                changesJson => DarlingMcpServerAdminTools.EditServerByNameAsync(
                    dataSource, request.ServerName, changesJson, ProbeAsync, sealKey.Ring, null, cancellationToken),
                (server, enable) => enable
                    ? DarlingMcpCollectionStateTools.EnableServer(dataSource, server, null, cancellationToken)
                    : DarlingMcpCollectionStateTools.DisableServer(dataSource, server, null, cancellationToken),
                readPassword ?? (label => ReadPasswordFromConsole(label, input ?? TextReader.Null, output)));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            error.WriteLine($"Could not reach the store: {ex.Message}");
            return 1;
        }

        foreach (var line in lines)
        {
            output.WriteLine(line);
        }

        foreach (var note in identityNotes)
        {
            output.WriteLine("    " + note);
        }

        return exitCode;
    }
}
