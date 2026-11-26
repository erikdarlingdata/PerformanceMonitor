/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.IO;
using System.Security.Principal;
using System.Text.Json;
using System.Text.Json.Nodes;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// What the running Lite lends the server admin tools (#9640): the app's own removal of a server, and a refresh of the server list its window shows.
/// The removal is the one Manage Servers' Delete and the sidebar's Remove run, so the two cannot drift (it drops the server's health, alert state,
/// tag assignments, long-query trace session and saved credentials, then the list entry). A host with no window (a test) gives none, and
/// <c>remove_server</c> then says it cannot remove.
/// </summary>
public sealed class McpServerAdminHooks
{
    /// <summary>Removes <paramref name="server"/> as the window does. False when a part of the clean-up gave up on a busy store (the server is removed all the same).</summary>
    public Func<ServerConnection, Task<bool>>? RemoveServer { get; init; }

    /// <summary>Called after the server list changed, so the window shows it. Null when there is no window.</summary>
    public Action? ServersChanged { get; init; }

    /// <summary>Runs a change to a server's own fields on the window's UI thread, where the Edit Server and Manage Servers windows read and
    /// save them. Null when there is no window (a test), where <see cref="Save"/> serialises on one lock instead.</summary>
    public Action<Action>? OnUiThread { get; init; }

    /// <summary>Whether <c>add_servers</c> and <c>edit_server</c> may save Windows authentication over a connection whose server identity is not verified
    /// (<see cref="McpSettings.AllowWindowsAuthOverUnverifiedTls"/>). False unless the operator turned it on, which is also what a host with no settings gives.</summary>
    public bool AllowWindowsAuthOverUnverifiedTls { get; init; }

    private static readonly object HeadlessSaveLock = new();

    /// <summary>Runs <paramref name="work"/>, a change to the fields of a live <see cref="ServerConnection"/> and its save, on the UI thread when
    /// <paramref name="hooks"/> has one, so no window reads or saves the server halfway through it. Without one, under one lock shared by every caller.
    /// ServerManager's and ScheduleManager's own saves are each atomic under their own lock, so only the in-place field change needs this.</summary>
    internal static void Save(McpServerAdminHooks? hooks, Action work)
    {
        if (hooks?.OnUiThread is { } onUi)
        {
            onUi(work);
            return;
        }

        lock (HeadlessSaveLock)
        {
            work();
        }
    }
}

/// <summary>
/// The server admin tools (#9640): Lite's twins of Darling's <c>add_servers</c> and <c>remove_server</c>. Both change which servers Lite connects to,
/// and <c>add_servers</c> saves a login, so they are served only with <c>mcp_allow_server_admin</c> AND <c>mcp_allow_writes</c> on
/// (<see cref="McpWritePolicy.ServerAdminTools"/>).
///
/// <para><c>add_servers</c> runs <see cref="AddServerCommand"/>'s own parse and save over the running Lite's server manager, the code behind
/// <c>PerformanceMonitorLite.exe --add-server</c>: the same fields, the same checks, the same password storage. <c>remove_server</c> runs the
/// removal the Manage Servers window runs (<see cref="McpServerAdminHooks.RemoveServer"/>).</para>
/// </summary>
[McpServerToolType]
public sealed class McpServerRegistryTools
{
    [McpServerTool(Name = "add_servers"), Description(
        "Write: adds servers (a JSON ARRAY) to what Lite monitors, through the code behind --add-server: every entry is checked and connection-tested first, " +
        "then saved one at a time. Served only when mcp_allow_server_admin and mcp_allow_writes are both true. One refused or unreachable entry " +
        "refuses the whole call before anything is saved, and the answer lists every refused entry with its number. A server already listed (same host, database and " +
        "read-only intent) is updated in place, with the new password. A password is stored in Windows Credential Manager and never returned. <<GUIDE>> " +
        "Darling instead saves the entries that pass. If a save fails, the entries before it stay saved, the answer names them (remove_server removes one) and the failed entry changes nothing. " +
        "Each object: host (REQUIRED); display_name (defaults to host); database; auth (\"Windows\" default, \"SQL\", \"ServicePrincipal\" (username is the " +
        "client id, password the secret), \"ManagedIdentity\" or \"EntraDefault\"); username and password (REQUIRED for \"SQL\" and \"ServicePrincipal\"); " +
        "encrypt_mode (\"Optional\"|\"Mandatory\"|\"Strict\"); trust_server_certificate, read_only_intent, multi_subnet_failover (booleans); engine, " +
        "only \"sqlserver\" (Lite monitors SQL Server only, so Darling's port, aws_role_arn, aws_external_id and query_store_replica are not read). " +
        "The password travels to this endpoint in the request. Returns {status:\"ok\", requested, added, updated, results:[{server, host, database, " +
        "status:\"added\"|\"updated\"}]}, or {status:\"refused\", message, requested, added, updated} (added and updated count what stayed saved, and are 0 when nothing was). " +
        "An entry key this tool does not read (name for display_name, a misspelt key, or one of Darling's) is dropped and the entry is still processed: a saved row adds " +
        "ignored_keys:[{key, did_you_mean}] (key names only; did_you_mean is the key it reads that was likely meant, or null), and a refusal message names them. " +
        "An entry with Windows authentication (the default) and trust_server_certificate true or encrypt_mode Optional is refused before any connection is tried, as " +
        "{status:\"invalid\", message (naming each such entry), hints:{parameter:\"servers_json\"}} with nothing saved: Windows authentication would present Lite's own Windows account " +
        "to a host the connection does not verify. Use auth \"SQL\", or leave trust_server_certificate false and use encrypt_mode Mandatory or Strict; " +
        "an operator can allow it with mcp_allow_windows_auth_over_unverified_tls in settings.json. The new server is collected from the next collection pass.")]
    public static async Task<string> AddServers(
        ServerManager serverManager,
        McpServerAdminHooks hooks,
        [Description("A JSON ARRAY of server objects to add, e.g. [{\"host\":\"sql01\",\"auth\":\"SQL\",\"username\":\"monitor\",\"password\":\"...\"}].")] string servers_json)
    {
        try
        {
            return await AddServersCore(serverManager, hooks, servers_json, ServerEntryRules.TestConnectionAsync);
        }
        catch (Exception ex)
        {
            AppLogger.Error("MCP", $"add_servers failed: {ex.GetType().Name}");
            return McpHelpers.FormatError("add_servers", ex);
        }
    }

    /// <summary>The testable core: parse and save through <see cref="AddServerCommand"/> over <paramref name="serverManager"/>, with the connection test injected.</summary>
    internal static async Task<string> AddServersCore(
        ServerManager serverManager, McpServerAdminHooks? hooks, string? serversJson,
        Func<ServerConnection, string?, string?, CancellationToken, Task<string?>> testConnection)
    {
        if (string.IsNullOrWhiteSpace(serversJson))
        {
            return McpHelpers.Refusal("servers_json", "servers_json is required: a JSON array of server objects.");
        }

        var (entries, problem) = AddServerCommand.ReadEntries(new AddServerFlags(), new StringReader(serversJson), inputRedirected: true);
        if (entries is null)
        {
            return McpHelpers.Refusal("servers_json", InArgumentWords(problem));
        }

        /* #9640: Windows authentication over a connection nothing verifies is refused before any connection test or write, so the Windows account
           Lite runs as never reaches a host the TLS settings do not confirm. A listed server's own TLS settings count for a field the entry leaves out. */
        var unverified = new List<string>();
        var listed = serverManager.GetAllServers();
        var number = 0;
        foreach (var entry in entries)
        {
            number++;
            var candidate = new ServerConnection { ServerName = entry.Host, DatabaseName = entry.Database, ReadOnlyIntent = entry.ReadOnlyIntent ?? false };
            var existing = listed.FirstOrDefault(s => ServerManager.IsSameServer(s, candidate));
            if (McpUnverifiedWindowsAuthScreen.Refusal(
                hooks?.AllowWindowsAuthOverUnverifiedTls ?? false, entry.AuthenticationType,
                entry.EncryptMode ?? existing?.EncryptMode, entry.TrustServerCertificate ?? existing?.TrustServerCertificate ?? false,
                $"Entry {number} ({entry.Host}) not added") is { } refusal)
            {
                unverified.Add(refusal);
            }
        }

        if (unverified.Count > 0)
        {
            return McpHelpers.Refusal("servers_json", string.Join(" ", unverified.Concat(AddServerCommand.UnreadNotes(entries))) + " Nothing was saved.");
        }

        var hostFolder = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), DataRootMigration.DataRootName, "config");
        var env = new AddServerCommand.Machine(
            () => serverManager,
            () => false,
            (server, username, password) => testConnection(server, username, password, CancellationToken.None),
            WindowsIdentity.GetCurrent().Name,
            hostFolder,
            InsideRunningLite: true);

        var saved = new AddServerCommand.SaveLog();
        var errors = new StringWriter();
        var exit = await AddServerCommand.SaveEntriesAsync(entries, errors, env, saved);
        if (exit != AddServerCommand.ExitAdded)
        {
            /* #9780: a key the parse did not read can be why the entry was refused (a misspelt trust_server_certificate), so the refusal names it. */
            foreach (var note in AddServerCommand.UnreadNotes(entries))
            {
                errors.WriteLine(note);
            }

            /* The entries saved before a failed write stay saved (AddServerCommand's live undo), so the list may have changed. */
            if (saved.Count > 0)
            {
                hooks?.ServersChanged?.Invoke();
            }

            return JsonSerializer.Serialize(new
            {
                status = "refused",
                message = InArgumentWords(errors.ToString().Trim()),
                requested = entries.Count,
                added = saved.Count(row => !row.Updated),
                updated = saved.Count(row => row.Updated),
            }, McpHelpers.JsonOptions);
        }

        hooks?.ServersChanged?.Invoke();
        AppLogger.Info("MCP", $"add_servers saved {saved.Count} server(s)");
        return JsonSerializer.Serialize(new
        {
            status = "ok",
            requested = entries.Count,
            added = saved.Count(row => !row.Updated),
            updated = saved.Count(row => row.Updated),
            results = saved.Select(ResultRow),
            note = "A password is stored for the Windows account Lite runs as and is never returned. The new server is collected from the next collection pass.",
        }, McpHelpers.JsonOptions);
    }

    /// <summary>
    /// One saved entry of the answer's <c>results</c>. An entry whose keys the parse did not all read carries <c>ignored_keys</c> (#9780):
    /// <c>[{key, did_you_mean}]</c>, each a key name and the key the parse reads that it most likely meant (null when none is near). A clean entry has no
    /// such field. Names only, never a value.
    /// </summary>
    private static object ResultRow(AddServerCommand.Saved row)
    {
        var status = row.Updated ? "updated" : "added";
        if (row.UnreadKeys is not { Count: > 0 } keys)
        {
            return new { server = row.DisplayName, host = row.Host, database = row.Database, status };
        }

        return new
        {
            server = row.DisplayName,
            host = row.Host,
            database = row.Database,
            status,
            ignored_keys = keys.Select(k => new { key = k.Key, did_you_mean = k.DidYouMean }),
        };
    }

    /// <summary>The command's messages talk about stdin, where a client passed the argument.</summary>
    private static string InArgumentWords(string? text) =>
        (text ?? string.Empty).Replace("on stdin", "in servers_json", StringComparison.Ordinal).Replace("stdin", "servers_json", StringComparison.Ordinal);

    [McpServerTool(Name = "remove_server"), Description(
        "Write: removes a monitored server, with no confirm step, as Manage Servers' Delete does: its saved login, its long-query trace Extended Events session on the server, " +
        "its collection health, alert state and tag assignments, then its entry in the server list. Served only when mcp_allow_server_admin and mcp_allow_writes are both true. " +
        "Already-collected history is NOT deleted. The name is matched exactly first (display name or storage name, any letter case). A part of a name removes nothing. <<GUIDE>> " +
        "A part of a name that fits one server is returned as {status:\"partial_match\"} to re-issue with its full name; when several servers match, nothing is removed and the candidates are listed. " +
        "Returns {status:\"removed\", server (the storage name), display_name, matched_by:\"exact\", previous (the removed entry in the keys add_servers takes; " +
        "a SQL or service-principal password cannot be read back, so it carries the placeholder <password>), undo (the add_servers arguments that re-add it), undo_note}, " +
        "{status:\"partial_match\", matched_by, candidates:[{server, display_name}]}, {status:\"ambiguous\", candidates:[{server, display_name}]} or {status:\"not_found\", message}.")]
    public static async Task<string> RemoveServer(
        ServerManager serverManager,
        McpServerAdminHooks hooks,
        [Description("The server to remove: its display name or storage name. A part of a name removes nothing: the answer lists the match to re-issue by its full name.")] string server_name)
    {
        try
        {
            return await RemoveServerCore(serverManager, hooks, server_name);
        }
        catch (Exception ex)
        {
            AppLogger.Error("MCP", $"remove_server failed: {ex.GetType().Name}");
            return McpHelpers.FormatError("remove_server", ex);
        }
    }

    internal static async Task<string> RemoveServerCore(ServerManager serverManager, McpServerAdminHooks? hooks, string? serverName)
    {
        if (string.IsNullOrWhiteSpace(serverName))
        {
            return McpHelpers.Refusal("server_name", "server_name is required.");
        }

        if (hooks?.RemoveServer is null)
        {
            return McpHelpers.Status("unavailable", "This host cannot remove a server: it has no server removal to run. Nothing was removed.", new { operation = "remove_server" });
        }

        /* Every listed server, the disabled ones too: a server whose collection is off can still be removed. A write never guesses, so a tie is refused. */
        var servers = serverManager.GetAllServers();
        var match = ServerResolver.MatchCandidates(servers, serverName);
        if (match.Candidates.Count == 0)
        {
            return JsonSerializer.Serialize(new { status = "not_found", message = ServerResolver.MissSentence(servers, serverName) + " Nothing was removed." }, McpHelpers.JsonOptions);
        }

        if (match.Candidates.Count > 1)
        {
            return JsonSerializer.Serialize(new
            {
                status = "ambiguous",
                message = $"'{serverName.Trim()}' matches {match.Candidates.Count} monitored servers ({(match.MatchedBy == "exact" ? "the same name on more than one registration" : "as a partial name")}); " +
                          "nothing was removed. Re-issue remove_server with ONE candidate's full server name.",
                matched_by = match.MatchedBy,
                candidates = match.Candidates.Select(c => new { server = c.ServerName, display_name = c.DisplayName }),
            }, McpHelpers.JsonOptions);
        }

        var candidate = match.Candidates[0];

        /* A write never guesses (#9640): a name that fits only part of one server's name is not enough to remove it, so the one candidate is
           shown and its full name asked for. The exact tiers (display name, host, storage name) name the server, so they go on. */
        if (match.MatchedBy == "partial")
        {
            return JsonSerializer.Serialize(new
            {
                status = "partial_match",
                message = $"'{serverName.Trim()}' fits only part of one monitored server's name; nothing was removed. " +
                          "Re-issue remove_server with that server's full name (the candidate's server value).",
                matched_by = match.MatchedBy,
                candidates = match.Candidates.Select(c => new { server = c.ServerName, display_name = c.DisplayName }),
            }, McpHelpers.JsonOptions);
        }

        var (target, duplicate) = ServerResolver.PickByStorageName(servers, candidate.ServerName);
        if (target is null)
        {
            return duplicate!;
        }

        /* Read before the removal drops the saved login: the username only, never the password. A server whose Id is not a GUID has no
           saved login to read (ServerCredentialIds), the same rule edit_server and the connection use. */
        string? username = null;
        if (ServerCredentialIds.Allows(target.Id, target.DisplayName)
            && serverManager.CredentialService.TryReadCredential(target.Id, out var credential, out _) == CredentialReadOutcome.Found)
        {
            username = credential!.Value.Username;
        }

        var previous = PreviousOf(target, username);
        var clean = await hooks.RemoveServer(target);
        AppLogger.Info("MCP", $"remove_server removed the server '{RemoteCollectorService.GetServerNameForStorage(target)}'");

        var undoText = new JsonObject { ["servers_json"] = new JsonArray(previous.DeepClone()).ToJsonString() }.ToJsonString();
        return JsonSerializer.Serialize(new
        {
            status = "removed",
            server = candidate.ServerName,
            display_name = candidate.DisplayName,
            matched_by = match.MatchedBy,
            previous = previous,
            undo = undoText,
            undo_note = "The undo re-adds the server with a fresh connection test. A SQL or service-principal row needs the real password in place of <password>. " +
                "Collected history stays in the store and is shown again once the server is added back. Alert and tag state the removal dropped is not brought back."
                + (target.AuthenticationType is AuthenticationTypes.EntraMFA or AuthenticationTypes.EntraDeviceCode
                    ? " This server signs in with Microsoft Entra MFA or device code, which add_servers does not take, so this undo cannot be replayed: add the server again in the Add Server window."
                    : ""),
            cleanup = clean ? "complete" : "incomplete: the server's tag assignments were not cleared (the store was busy), and they come back if it is added again",
        }, McpHelpers.JsonOptions);
    }

    /// <summary>The removed entry in the keys <c>add_servers</c> takes. A password is never read, so the placeholder stands in for it.</summary>
    private static JsonObject PreviousOf(ServerConnection server, string? username)
    {
        var auth = server.AuthenticationType switch
        {
            AuthenticationTypes.SqlServer => "SQL",
            AuthenticationTypes.EntraDefaultCredential => "EntraDefault",
            var other => other,
        };
        var row = new JsonObject { ["host"] = server.ServerName, ["display_name"] = server.DisplayName };
        if (!string.IsNullOrWhiteSpace(server.DatabaseName))
        {
            row["database"] = server.DatabaseName;
        }

        row["auth"] = auth;
        if (server.AuthenticationType == AuthenticationTypes.SqlServer)
        {
            row["username"] = username;
            row["password"] = "<password>";
        }
        else if (server.AuthenticationType == AuthenticationTypes.ServicePrincipal)
        {
            row["username"] = username ?? server.AzureClientId;
            row["password"] = "<password>";
        }

        row["encrypt_mode"] = server.EncryptMode;
        row["trust_server_certificate"] = server.TrustServerCertificate;
        row["read_only_intent"] = server.ReadOnlyIntent;
        row["multi_subnet_failover"] = server.MultiSubnetFailover;
        return row;
    }
}
