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
        "Write: adds servers (a JSON ARRAY) to what Lite monitors, through the code behind --add-server: every entry is checked and connection-tested, " +
        "then all are saved at once. Served only when mcp_allow_server_admin and mcp_allow_writes are both true. One refused or unreachable entry " +
        "refuses the whole call and nothing is saved (unlike Darling, which reports each entry). A server already listed (same host, database and " +
        "read-only intent) is updated in place, with the new password. A password is stored in Windows Credential Manager and never returned. <<GUIDE>> " +
        "Each object: host (REQUIRED); display_name (defaults to host); database; auth (\"Windows\" default, \"SQL\", \"ServicePrincipal\" (username is the " +
        "client id, password the secret), \"ManagedIdentity\" or \"EntraDefault\"); username and password (REQUIRED for \"SQL\" and \"ServicePrincipal\"); " +
        "encrypt_mode (\"Optional\"|\"Mandatory\"|\"Strict\"); trust_server_certificate, read_only_intent, multi_subnet_failover (booleans); engine, " +
        "only \"sqlserver\" (Lite monitors SQL Server only, so Darling's port, aws_role_arn, aws_external_id and query_store_replica are not read). " +
        "The password travels to this endpoint in the request. Returns {status:\"ok\", requested, added, updated, results:[{server, host, database, " +
        "status:\"added\"|\"updated\"}]}, or {status:\"refused\", message} with nothing saved. The new server is collected from the next collection pass.")]
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
            results = saved.Select(row => new { server = row.DisplayName, host = row.Host, database = row.Database, status = row.Updated ? "updated" : "added" }),
            note = "A password is stored for the Windows account Lite runs as and is never returned. The new server is collected from the next collection pass.",
        }, McpHelpers.JsonOptions);
    }

    /// <summary>The command's messages talk about stdin, where a client passed the argument.</summary>
    private static string InArgumentWords(string? text) =>
        (text ?? string.Empty).Replace("on stdin", "in servers_json", StringComparison.Ordinal).Replace("stdin", "servers_json", StringComparison.Ordinal);

    [McpServerTool(Name = "remove_server"), Description(
        "Write: removes a monitored server, with no confirm step, as Manage Servers' Delete does: its saved login, its long-query trace Extended Events session on the server, " +
        "its collection health, alert state and tag assignments, then its entry in the server list. Served only when mcp_allow_server_admin and mcp_allow_writes are both true. " +
        "Already-collected history is NOT deleted. The name is matched exactly first (display name or storage name, any letter case), else as a part of a name; " +
        "when several servers match, nothing is removed and the candidates are listed. <<GUIDE>> " +
        "Returns {status:\"removed\", server (the storage name), display_name, matched_by:\"exact\"|\"partial\", previous (the removed entry in the keys add_servers takes; " +
        "a SQL or service-principal password cannot be read back, so it carries the placeholder <password>), undo (the add_servers arguments that re-add it), undo_note}, " +
        "{status:\"ambiguous\", candidates:[{server, display_name}]} or {status:\"not_found\", message}.")]
    public static async Task<string> RemoveServer(
        ServerManager serverManager,
        McpServerAdminHooks hooks,
        [Description("The server to remove: its display name or storage name. A partial name is accepted only when it matches exactly one server.")] string server_name)
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
        var target = servers.First(s => RemoteCollectorService.GetServerNameForStorage(s) == candidate.ServerName);

        /* Read before the removal drops the saved login: the username only, never the password. */
        string? username = null;
        if (serverManager.CredentialService.TryReadCredential(target.Id, out var credential, out _) == CredentialReadOutcome.Found)
        {
            username = credential!.Value.Username;
        }

        var previous = PreviousOf(target, username);
        var clean = await hooks.RemoveServer(target);
        AppLogger.Info("MCP", "remove_server removed a server");

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
                "Collected history stays in the store and is shown again once the server is added back. Alert and tag state the removal dropped is not brought back.",
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
