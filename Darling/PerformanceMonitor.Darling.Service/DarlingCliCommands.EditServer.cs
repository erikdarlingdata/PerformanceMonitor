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
using PerformanceMonitor.Common;
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
        "A password is never an argument, so --set does not take one, and what you typed is now in your shell history and visible to other processes on this host: change that password. When a change needs the server's password, --edit-server asks for it.";

    /// <summary>The field names <c>--set</c> refuses as a secret, whatever the case: the shapes a password is named by in a
    /// darling.json entry, a connection string and a service-principal entry.</summary>
    private static readonly HashSet<string> EditServerSecretFields = new(StringComparer.OrdinalIgnoreCase)
    {
        "password", "encryptedPassword", "encrypted_password", "pwd", "passwd", "secret", "clientSecret", "client_secret",
    };

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

        if (StoreConfigProvider.HasControlCharacter(rest[0]))
        {
            errorMessage = "The server's name holds a character that does not print (a control or format character), which the verb does not take.";
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
            if (EditServerSecretFields.Contains(field))
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
                errorMessage = $"--edit-server cannot set '{StoreConfigProvider.DriftText(field)}'. Fields: " + EditServerFieldList() + ".";
                return false;
            }

            var canonical = isEnabled ? EditServerEnabledField : known!.Name;
            if (!named.Add(canonical))
            {
                errorMessage = $"--set names {canonical} more than once.";
                return false;
            }

            var text = value[(split + 1)..];
            if (StoreConfigProvider.HasControlCharacter(text))
            {
                errorMessage = $"{canonical} holds a character that does not print (a newline, a tab or another control character, a line separator, or a format character such as a right-to-left override or a zero-width space), which the verb does not take.";
                return false;
            }

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

    /// <summary>The one registered server the verb edits, resolved once from the name the operator typed: what is printed before
    /// the password is asked for, and the <c>modified_at</c> the edit carries as <c>expected_modified_at</c>, so a row that moved
    /// while the prompt waited is refused and the password is never used against a server the operator did not see.</summary>
    internal sealed record EditServerTarget(
        int ServerId, string StorageName, string DisplayName, string Host, int Port, string Auth, string? Username, string ModifiedAt)
    {
        /// <summary>The server as the operator is shown it. Control characters in a value are escaped.</summary>
        internal string Describe()
        {
            var where = StoreConfigProvider.DriftText(Host) + (Port > 0 ? ":" + Port.ToString(CultureInfo.InvariantCulture) : "");
            var login = string.IsNullOrEmpty(Username) ? $"{Auth} authentication" : $"login {StoreConfigProvider.DriftText(Username)} ({Auth})";
            return $"{StoreConfigProvider.DriftValue(DisplayName)} (id {ServerId.ToString(CultureInfo.InvariantCulture)}) at {where}, {login}";
        }

        /// <summary>Where the edit connects once <paramref name="changes"/> (an edit_server request) is applied: the host, port,
        /// authentication and login a typed password is sent to for the connection test. Each value is in quotes with the
        /// characters that do not print escaped, so a host or a login cannot add words to the line.</summary>
        internal string DescribeAfter(JsonObject changes)
        {
            string? Changed(string key) => changes[key] is JsonValue v ? (v.TryGetValue<string>(out var s) ? s : v.ToJsonString()) : null;

            var port = changes["port"] is JsonValue p && p.TryGetValue<int>(out var number) ? number : Port;
            var username = changes.ContainsKey("username") ? Changed("username") : Username;
            return $"host {StoreConfigProvider.DriftValue(Changed("host") ?? Host)}, "
                + (port > 0 ? $"port {port.ToString(CultureInfo.InvariantCulture)}, " : "")
                + $"authentication {StoreConfigProvider.DriftValue(Changed("auth") ?? Auth)}, "
                + (string.IsNullOrEmpty(username) ? "no login name" : $"login {StoreConfigProvider.DriftValue(username)}");
        }
    }

    /// <summary>
    /// Finds the one server <paramref name="name"/> names, by its exact display name or storage name and never by a part of one:
    /// a write verb that can ask for a password must not guess. A name that matches nothing, a part of a name and a name several
    /// servers share each answer with the sentence to print and no target.
    /// </summary>
    internal static (EditServerTarget? Target, string? Refusal) ResolveEditServerTarget(
        IReadOnlyList<PerformanceMonitor.Darling.Storage.RegisteredServer> servers, string name)
    {
        var match = DarlingMcpServerAdminTools.ResolveForRemoval(servers, name, nameClash: NameClash.Refuse);
        string List(IEnumerable<PerformanceMonitor.Darling.Storage.RegisteredServer> candidates) =>
            string.Join(Environment.NewLine, candidates.Select(c => $"    {StoreConfigProvider.DriftValue(c.DisplayName)} ({StoreConfigProvider.DriftValue(c.ServerName)})"));
        if (match.Candidates.Count == 0)
        {
            return (null, "  Not changed: no registered server is named that. Nothing was changed.");
        }

        if (match.MatchedBy != "exact")
        {
            return (null, "  Not changed: no registered server is named exactly that, and --edit-server never matches a part of a name. Servers whose names contain it:"
                + Environment.NewLine + List(match.Candidates));
        }

        if (match.Candidates.Count > 1)
        {
            return (null, "  Not changed: more than one registered server has that name. Use the storage name in parentheses:"
                + Environment.NewLine + List(match.Candidates));
        }

        var only = match.Candidates[0];
        return (new EditServerTarget(only.ServerId, only.ServerName, only.DisplayName ?? only.ServerName, "", 0, "", null, ""), null);
    }

    /// <summary>
    /// The verb's body over its two writes, so a test drives it without a store. <paramref name="edit"/> takes an edit_server
    /// request and returns its answer; <paramref name="setEnabled"/> takes the server's id and the enabled state and returns the
    /// enable_server or disable_server answer, so the enabled step reaches the row found at the start and never a name looked up
    /// again. When the edit answers that it needs the server's password, <paramref name="announce"/> first prints where the
    /// password will be sent (the host, port, authentication and login the edit sets), then the password is read with
    /// <paramref name="readPassword"/> and the same request is sent once more with it. Both sends carry the target's
    /// <c>modified_at</c> as <c>expected_modified_at</c>, so a server that changed while the password was typed is refused before
    /// the password is used. A server that changed during the connection test is refused by the write, after the password was
    /// sent: <paramref name="passwordWasSent"/> tells the two apart, and without it the line says only that nothing was saved.
    /// The enabled state is applied only after the edit, if there is one, has succeeded. An enabled step that fails keeps the
    /// edit's lines and says the settings were saved. Every line returned has the characters that do not print escaped
    /// (<see cref="StoreConfigProvider.DriftText"/>), whatever a store answer, a server's name or a driver's message holds.
    /// </summary>
    internal static async Task<(IReadOnlyList<string> Lines, int ExitCode)> RunEditServerAsync(
        EditServerRequest request,
        Func<string, Task<string>> edit,
        Func<int, bool, Task<string>> setEnabled,
        Func<string, string?> readPassword,
        EditServerTarget target,
        Action<string>? announce = null,
        Func<bool>? passwordWasSent = null)
    {
        var (lines, exitCode) = await RunEditServerCoreAsync(request, edit, setEnabled, readPassword, target, announce, passwordWasSent);
        return (lines.Select(StoreConfigProvider.DriftText).ToList(), exitCode);
    }

    private static async Task<(List<string> Lines, int ExitCode)> RunEditServerCoreAsync(
        EditServerRequest request,
        Func<string, Task<string>> edit,
        Func<int, bool, Task<string>> setEnabled,
        Func<string, string?> readPassword,
        EditServerTarget target,
        Action<string>? announce,
        Func<bool>? passwordWasSent)
    {
        var lines = new List<string>();
        var saved = false;
        if (request.Changes is { } changes)
        {
            var first = (JsonObject)changes.DeepClone();
            first["expected_modified_at"] = target.ModifiedAt;

            var answer = await edit(first.ToJsonString());
            if (DarlingMcpServerAdminTools.IsPasswordNeededAnswer(answer))
            {
                announce?.Invoke($"  The password is sent to {target.DescribeAfter(changes)} to test the change.");
                var typed = readPassword($"  This change sets how {target.Describe()} is reached, and a saved password is never sent with changed connection settings unless it is typed again. Password for it (tested with the change, then saved with it): ");
                if (string.IsNullOrEmpty(typed))
                {
                    lines.Add("  Not changed: this change needs the server's password, and none was entered.");
                    return (lines, 1);
                }

                var withPassword = (JsonObject)first.DeepClone();
                withPassword["password"] = typed;
                answer = await edit(withPassword.ToJsonString());
                if (StatusOf(answer) == DarlingMcpServerAdminTools.EditStatus.Conflict)
                {
                    lines.Add(passwordWasSent switch
                    {
                        null => "  The server changed after its password was asked for, so the password was not saved.",
                        var sent when sent() => "  The server changed while the connection was tested. The password was sent for that test, to the host named above, and it was not saved.",
                        _ => "  The server changed while the password prompt was open, so the password was not used.",
                    });
                }
            }

            var (editLines, exitCode, _) = FormatEditServerOutcome(answer);
            lines.AddRange(editLines);
            if (exitCode != 0)
            {
                if (request.Enabled is not null)
                {
                    lines.Add("  enabled was not changed either.");
                }

                return (lines, exitCode);
            }

            saved = StatusOf(answer) == DarlingMcpServerAdminTools.EditStatus.Updated;
        }

        if (request.Enabled is { } enable)
        {
            JsonObject? answer;
            try
            {
                answer = JsonNode.Parse(await setEnabled(target.ServerId, enable)) as JsonObject;
            }
            catch (JsonException)
            {
                answer = null;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                lines.Add("  enabled was not changed: could not reach the store: " + ex.Message);
                if (saved)
                {
                    lines.Add("  The settings above were saved.");
                }

                return (lines, 1);
            }

            string? Text(string key) => answer?[key] is JsonValue v && v.TryGetValue<string>(out var s) ? s : null;

            if (Text("status") is "changed" or "unchanged")
            {
                lines.Add("  " + (Text("what_changed") ?? $"enabled is {(enable ? "true" : "false")}."));
            }
            else
            {
                lines.Add("  enabled was not changed: " + (Text("message") ?? Text("error") ?? "the store's answer could not be read."));
                if (saved)
                {
                    lines.Add("  The settings above were saved.");
                }

                return (lines, 1);
            }
        }

        return (lines, 0);
    }

    /// <summary>
    /// The answer for an edit send that threw. A send that carried the typed password answers with one fixed sentence and the
    /// exception's type and never its message: a driver or store message can repeat the password, and taking the password out of
    /// a message would show which of its words was the password. A send without a password keeps the message.
    /// </summary>
    internal static string EditServerSendFailure(string changesJson, Exception failure)
    {
        bool carriesPassword;
        try
        {
            carriesPassword = JsonNode.Parse(changesJson) is JsonObject sent && sent.ContainsKey("password");
        }
        catch (JsonException)
        {
            carriesPassword = true;
        }

        return McpHelpers.FormatError("edit_server", carriesPassword
            ? new InvalidOperationException($"the change failed after the password was typed ({failure.GetType().Name}). The failure's own text is not printed, because it can repeat the password. Nothing was reported as saved: run the verb again to see the server's settings.")
            : failure);
    }

    private static string? StatusOf(string answerJson)
    {
        try
        {
            return JsonNode.Parse(answerJson) is JsonObject o && o["status"] is JsonValue v && v.TryGetValue<string>(out var s) ? s : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>The write the edit makes, with one more check in its transaction: the password key the typed password was sealed to
    /// is still the store's current key (#9693). A key reset between the choice of the key and the write would leave a password
    /// sealed to a retired key that opens for nobody, so the write is refused and nothing is saved.</summary>
    internal sealed class KeyCheckedEditStore(DarlingMcpServerAdminTools.IServerEditStore inner, IPasswordKeyRing ring) : DarlingMcpServerAdminTools.IServerEditStore
    {
        public Task<DarlingMcpServerAdminTools.ServerEditRow?> ReadRowAsync(int serverId, CancellationToken cancellationToken) =>
            inner.ReadRowAsync(serverId, cancellationToken);

        public Task<List<string>> LoadOtherStorageKeysAsync(int serverId, CancellationToken cancellationToken) =>
            inner.LoadOtherStorageKeysAsync(serverId, cancellationToken);

        public Task<DarlingMcpServerAdminTools.ServerEditWrite> WriteAsync(
            int serverId, DateTime expectedModifiedAt, IReadOnlyList<DarlingMcpServerAdminTools.EditColumnValue> sets,
            string? newStorageKey, string? actualStorageKey, DarlingMcpServerAdminTools.HeldCheck? heldCheck, CancellationToken cancellationToken)
        {
            if (!sets.Any(s => s.Column == "encrypted_password"))
            {
                return inner.WriteAsync(serverId, expectedModifiedAt, sets, newStorageKey, actualStorageKey, heldCheck, cancellationToken);
            }

            DarlingMcpServerAdminTools.HeldCheck checkKey = async (connection, transaction, token) =>
            {
                string? currentKeyId;
                await using (var command = new NpgsqlCommand("SELECT key_id FROM config.password_key WHERE state = 'current' FOR SHARE;", connection, transaction))
                {
                    command.CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds;
                    currentKeyId = (string?)await command.ExecuteScalarAsync(token);
                }

                return PasswordKeyChangedReason(currentKeyId, ring) ?? (heldCheck is null ? null : await heldCheck(connection, transaction, token));
            };
            return inner.WriteAsync(serverId, expectedModifiedAt, sets, newStorageKey, actualStorageKey, checkKey, cancellationToken);
        }
    }

    /// <summary>A logger that writes each line the edit path logs to the verb's own output, so the driver's reason for a failed
    /// connection test and the one-line audit of an edit (the id and the field names, never a value) reach the operator.
    /// Exceptions are not printed.</summary>
    private sealed class EditServerOutputLogger(TextWriter output) : Microsoft.Extensions.Logging.ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => logLevel >= Microsoft.Extensions.Logging.LogLevel.Information;

        public void Log<TState>(
            Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel))
            {
                output.WriteLine("    " + StoreConfigProvider.DriftText(formatter(state, null)));
            }
        }
    }

    /// <summary>
    /// <c>--edit-server &lt;name&gt; --set &lt;field&gt;=&lt;value&gt; ... [--config &lt;path&gt;]</c> (#9693). Runs as the store owner, like
    /// <c>--add-server</c>, and seals a typed password with the key the service keeps. It does not stop or signal the service: the
    /// store bumps its config version on the write, and the running service reloads the registry on its next config poll.
    /// The server is found once, by its exact name, and printed with the store and the configuration file before anything is
    /// sent. The password is read as bytes (<see cref="ReadLoginPassword"/>), the password key is chosen again after it is typed and
    /// checked again in the write's transaction, and the write goes by the server's id with the row's <c>modified_at</c> as the expected value.
    /// </summary>
    public static async Task<int> EditServerAsync(
        string[] rest, Stream input, TextWriter output, TextWriter error, CancellationToken cancellationToken,
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
            error.WriteLine($"Could not load configuration: {StoreConfigProvider.DriftText(ex.Message)}");
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
        var probes = 0;
        await using var tester = OperatingSystem.IsWindows()
            ? new DarlingServiceConnectionTest.LazyStoreServiceConnectionTester(connectionString)
            : null;
        var consoleIdentity = DarlingServiceConnectionTest.ConsoleIdentity();
        async Task<ConnectionProbeResult> ProbeAsync(MonitoredServer server, CancellationToken token)
        {
            probes++;
            var (probe, notes) = await DarlingServiceConnectionTest.TestAsync(
                server, tester, DarlingMcpServerAdminTools.ProbeAsConsoleUserAsync, consoleIdentity, OperatingSystem.IsWindows(), token);
            identityNotes.AddRange(notes);
            return probe;
        }

        var configPath = DarlingConfig.ResolveConfigPath(request.ConfigPath);
        var logger = new EditServerOutputLogger(output);
        IReadOnlyList<string> lines;
        int exitCode;
        try
        {
            await using var dataSource = NpgsqlDataSource.Create(
                DarlingStoreConnection.PinSessionTimeZoneUtc(
                    DarlingStoreConnection.WithApplicationName(connectionString, DarlingManagedPostgres.CliApplicationName)));
            var editStore = new DarlingMcpServerAdminTools.PostgresServerEditStore(dataSource);

            /* The server is found once, by its exact name, and from here on it is its id: the password is asked for, and later sent,
               for this row and no other. */
            var definitions = await DarlingMcpServerAdminTools.LoadDefinitionsForRemovalAsync(dataSource);
            var (found, refusal) = ResolveEditServerTarget(definitions.Select(d => d.Server).ToList(), request.ServerName);
            if (found is null)
            {
                output.WriteLine(refusal);
                return 1;
            }

            var row = await editStore.ReadRowAsync(found.ServerId, cancellationToken);
            if (row is null)
            {
                output.WriteLine("  Not changed: that server's definition no longer exists.");
                return 1;
            }

            var target = found with
            {
                Host = row.Host,
                Port = row.Port,
                Auth = DarlingMcpServerAdminTools.AuthWord(row.Auth),
                Username = row.Username,
                DisplayName = row.Name,
                ModifiedAt = DarlingMcpServerAdminTools.ModifiedAtToken(row.ModifiedAt),
            };

            /* Before the first send and before any prompt: the store and the configuration file this run reaches, the key a typed
               password is sealed to, and the server it is for, so a configuration or a name that is not the one the operator meant
               is seen before a password is spent on it. */
            var firstKey = await DarlingCliSealKey.ResolveAsync(config, configPath, dataSource, null, cancellationToken);
            output.WriteLine("  " + StoreConfigProvider.DriftText(StoreAndKeyLine(dataSource.ConnectionString, firstKey.Ring)));
            output.WriteLine($"  Configuration: {StoreConfigProvider.DriftText(configPath ?? "(none)")}");
            output.WriteLine($"  Server: {target.Describe()}.");
            output.WriteLine();

            DarlingCliSealKey.Decision? sealedWith = null;
            async Task<string> EditAsync(string changesJson)
            {
                /* The key is chosen again for every send, so the one after the password prompt is the store's key now. */
                var key = await DarlingCliSealKey.ResolveAsync(config, configPath, dataSource, null, cancellationToken);
                sealedWith = key;
                try
                {
                    return await DarlingMcpServerAdminTools.EditServerCoreAsync(
                        new KeyCheckedEditStore(editStore, key.Ring), target.ServerId, changesJson, ProbeAsync, key.Ring, logger, cancellationToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    return EditServerSendFailure(changesJson, ex);
                }
            }

            (lines, exitCode) = await RunEditServerAsync(
                request,
                EditAsync,
                (serverId, enable) => DarlingMcpCollectionStateTools.SetServerEnabledByIdAsync(dataSource, serverId, enable, null, cancellationToken),
                readPassword ?? ((Func<string, string?>)(label =>
                {
                    var typed = ReadLoginPassword(label, input, output, Console.IsInputRedirected, out var inputRefusal);
                    if (inputRefusal is not null)
                    {
                        output.WriteLine("  " + inputRefusal);
                    }

                    return typed;
                })),
                target,
                line => output.WriteLine(line),
                () => probes > 0);

            foreach (var line in lines)
            {
                output.WriteLine(line);
            }

            foreach (var note in identityNotes)
            {
                output.WriteLine("    " + StoreConfigProvider.DriftText(note));
            }

            if (sealedWith is { Ring: DarlingCliSealKey.PublishedKeyRing { SealedCount: > 0 }, Notice: { } notice })
            {
                output.WriteLine(StoreConfigProvider.DriftText(notice));
            }

            return exitCode;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            error.WriteLine($"Could not reach the store: {StoreConfigProvider.DriftText(ex.Message)}");
            return 1;
        }
    }
}
