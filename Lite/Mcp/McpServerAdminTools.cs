/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Text.Json;
using System.Text.Json.Nodes;
using ModelContextProtocol.Server;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// #9640: the server registry writes Lite's Manage Servers window already has, as twins of Darling's
/// <c>edit_server</c> and <c>set_server_excluded_databases</c>. Each saves through the code the window's Save uses
/// (<see cref="ServerEditSave"/> for an edit, <see cref="ExcludedDatabasesDialog.PersistSelection"/> for the excluded
/// databases), on the running app's own <see cref="ServerManager"/>, so the app picks the change up as it does after a Save.
/// <c>edit_server</c> is in <see cref="McpWritePolicy.ServerAdminTools"/> (served only with mcp_allow_server_admin as well);
/// <c>set_server_excluded_databases</c> is a plain write.
/// </summary>
[McpServerToolType]
public sealed class McpServerAdminTools
{
    /// <summary>The keys <c>edit_server</c> reads from changes_json. Darling's, minus the PostgreSQL and AWS ones Lite has no field for, plus the fields Lite's window sets that Darling's has no field for.</summary>
    internal static readonly string[] EditKeys =
    {
        "display_name", "host", "database", "read_only_intent", "auth", "username", "password", "encrypt_mode",
        "trust_server_certificate", "multi_subnet_failover", "query_store_replica", "monthly_cost_usd",
        "is_enabled", "is_favorite", "description", "utility_database", "alert_delivery_mode",
        "azure_tenant_id", "managed_identity_client_id"
    };

    private const string EditTool = "edit_server";
    private const string ExcludedTool = "set_server_excluded_databases";

    [McpServerTool(Name = "edit_server"), Description(
        "Write: edits one monitored server's settings in place, as the Edit Server window's Save does: name, address, database, authentication, TLS posture, cost and the other fields the window sets. The server keeps its id, tags and history. " +
        "<<GUIDE>>" +
        "Changes ONLY the fields named in changes_json, a JSON object. Keys: display_name, host, database, read_only_intent, auth (\"Windows\", \"SQL\", \"ServicePrincipal\", \"ManagedIdentity\", \"EntraDefault\"), username (a service principal's client id), password, " +
        "encrypt_mode (\"Optional\", \"Mandatory\", \"Strict\"), trust_server_certificate, multi_subnet_failover, query_store_replica (empty or null removes it), monthly_cost_usd (null clears it, stored as 0), " +
        "and the Lite window's own fields: is_enabled (collection on or off), is_favorite, description, utility_database, alert_delivery_mode (\"summary\", \"per_event\" or null to inherit the global default), azure_tenant_id (service principal), managed_identity_client_id (managed identity). " +
        "Not here: Darling's port, aws_role_arn, aws_external_id and expected_modified_at (Lite has no such fields), the server's id (it is derived from host, database and read-only intent), excluded databases (use set_server_excluded_databases), and credential profiles and Microsoft Entra MFA or device code sign-in (use the Edit Server window). Any other key is status \"invalid\". " +
        "PASSWORD: omit it, or leave it blank, to keep the stored one; a typed one replaces it. It is never returned or logged. Changing where or how a server is reached, for a SQL or service principal server, uses the stored password unless you send one, and Lite refuses the edit when this Windows user has not confirmed those settings for the server and no password is sent (the Edit Server window asks that question; confirm there). " +
        "When collection is on and a connection setting changed, the new settings are tried first and saved only if the server answers (status \"connection_failed\", nothing saved). " +
        "An edit of host, auth, trust_server_certificate or encrypt_mode that leaves Windows authentication with trust_server_certificate true or encrypt_mode Optional is refused as invalid before any connection is tried, because Windows authentication would present Lite's own Windows account to a host the connection does not verify; an operator can allow it with mcp_allow_windows_auth_over_unverified_tls in settings.json. " +
        "Returns updated or unchanged, or invalid / not_found / ambiguous / collides / connection_failed / refused with nothing saved. updated returns {status, server, display_name, changed:[field names], tested, previous, undo, undo_note}: previous holds the changed fields as they were, undo is the edit_server call that sets them back (never the password). " +
        "Served only when both mcp_allow_writes and mcp_allow_server_admin are true. server_name is a display name, host or storage name, or id:<server_id>, and a partial name is accepted only when it matches exactly one server. A name more than one server answers to is refused as ambiguous with each candidate's server_id.")]
    public static async Task<string> EditServer(
        ServerManager serverManager,
        McpServerAdminHooks hooks,
        [Description("The monitored server to edit: its display name or storage name, or id:<server_id>. A partial name is accepted only when it matches exactly one server.")] string server_name,
        [Description("A JSON object with ONLY the fields to change (e.g. {\"display_name\":\"Orders\",\"monthly_cost_usd\":120}). See the tool description for the keys.")] string changes_json,
        CancellationToken cancellationToken = default)
    {
        try
        {
            return await EditAsync(serverManager, server_name, changes_json, ServerEntryRules.TestConnectionAsync, cancellationToken, hooks);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(EditTool, ex);
        }
    }

    [McpServerTool(Name = "set_server_excluded_databases"), Description(
        "Write: replaces ONE server's excluded-databases list, the databases the collectors skip on that server, as the Manage Servers window's Excluded Databases Save does. The list you send is the whole new list; an empty list clears it. " +
        "At most 256 entries of at most 128 characters each, no control characters; repeats (any casing) are kept once. An entry may hold * as a wildcard: * matches any run of characters, so archive_* skips every database whose name starts with archive_. No other wildcard exists, and an entry made only of * is refused. " +
        "Data already collected stays. Collection picks the list up on its next pass. Served only when mcp_allow_writes is true. The answer gives the previous list and the exact call that restores it.")]
    public static string SetServerExcludedDatabases(
        ServerManager serverManager,
        McpServerAdminHooks hooks,
        [Description("Server name (a display name or storage name), or a unique part of it.")] string server_name,
        [Description("The complete new list of database names or patterns to exclude. Empty clears the list.")] string[] databases)
    {
        try
        {
            return SetExcluded(serverManager, server_name, databases, hooks);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError(ExcludedTool, ex);
        }
    }

    /// <summary>The refusal envelope with the tool's own name: <c>{status, tool, message}</c>.</summary>
    private static string Refuse(string tool, string status, string message, string? parameter = null) =>
        new JsonObject
        {
            ["status"] = status,
            ["tool"] = tool,
            ["message"] = message,
            ["hints"] = parameter is null ? null : new JsonObject { ["parameter"] = parameter },
        }.ToJsonString();

    /// <summary>The one server a name answers to over EVERY listed server (a disabled one too), or the refusal to return.</summary>
    private static (ServerConnection? Server, string? Error) Find(ServerManager serverManager, string tool, string? serverName)
    {
        if (string.IsNullOrWhiteSpace(serverName))
        {
            return (null, Refuse(tool, "invalid", "server_name is required: a display name or storage name from list_servers.", "server_name"));
        }

        var all = serverManager.GetAllServers();
        var (resolved, error) = ServerResolver.ResolveIn(all, serverName);
        if (error is not null)
        {
            return (null, error);
        }

        return ServerResolver.PickByStorageName(all, resolved.ServerName);
    }

    /// <summary>
    /// The one server a name picks for <c>edit_server</c> over EVERY listed server (a disabled one too), or the answer to return
    /// (#9693). The name goes through <see cref="ServerResolver.MatchForEdit"/>: an edit can change where a saved password is
    /// sent, so a name that more than one server answers to is refused as <c>ambiguous</c> with each candidate's
    /// <c>server_id</c>, and <c>id:&lt;server_id&gt;</c> names one of them.
    /// </summary>
    private static (ServerConnection? Server, string? Error) FindForEdit(ServerManager serverManager, string? serverName)
    {
        if (string.IsNullOrWhiteSpace(serverName))
        {
            return (null, Refuse(EditTool, "invalid", "server_name is required: a display name or storage name from list_servers.", "server_name"));
        }

        var all = serverManager.GetAllServers();
        var match = ServerResolver.MatchForEdit(all, serverName);
        if (match.Candidates.Count == 1)
        {
            return ServerResolver.PickByStorageName(all, match.Candidates[0].ServerName);
        }

        if (match.Candidates.Count == 0)
        {
            return (null, McpHelpers.Refusal("server_name", ServerResolver.MissSentence(all, serverName)));
        }

        return (null, new JsonObject
        {
            ["status"] = "ambiguous",
            ["tool"] = EditTool,
            ["message"] = $"'{serverName.Trim()}' matches {match.Candidates.Count} monitored servers " +
                          $"({(match.MatchedBy == "exact" ? "as the display name, the host or the storage name of more than one server" : "as a part of their names")}); " +
                          $"nothing was changed. Re-issue edit_server with ONE candidate: its server_id as {ServerResolver.ServerIdNamePrefix}<server_id>, or a name that no other server has.",
            ["matched_by"] = match.MatchedBy,
            ["candidates"] = new JsonArray(match.Candidates.Select(c => (JsonNode?)new JsonObject
            {
                ["server_id"] = c.ServerId,
                ["server"] = c.ServerName,
                ["display_name"] = c.DisplayName,
                ["kind"] = c.Kind,
            }).ToArray()),
            ["hints"] = new JsonObject { ["parameter"] = "server_name" },
        }.ToJsonString());
    }

    /// <summary>The name the undo call of an edit passes (#9693): the storage name while <see cref="ServerResolver.MatchForEdit"/>
    /// finds one server by it, else <c>id:&lt;server_id&gt;</c>, so the undo is never refused as ambiguous when another server has
    /// that storage name as its display name or host.</summary>
    private static string UndoServerName(ServerManager serverManager, string storage) =>
        ServerResolver.MatchForEdit(serverManager.GetAllServers(), storage).Candidates.Count == 1
            ? storage
            : ServerResolver.ServerIdNamePrefix + RemoteCollectorService.GetDeterministicHashCode(storage).ToString(System.Globalization.CultureInfo.InvariantCulture);

    private static string AuthWordOut(string authenticationType) => authenticationType switch
    {
        AuthenticationTypes.Windows => "Windows",
        AuthenticationTypes.SqlServer => "SQL",
        AuthenticationTypes.ServicePrincipal => "ServicePrincipal",
        AuthenticationTypes.ManagedIdentity => "ManagedIdentity",
        AuthenticationTypes.EntraDefaultCredential => "EntraDefault",
        _ => authenticationType,
    };

    private static JsonNode? ModeOut(AlertNotificationMode? mode) => mode switch
    {
        AlertNotificationMode.Summary => "summary",
        AlertNotificationMode.PerEvent => "per_event",
        _ => null,
    };

    /// <summary>The fields edit_server can change, as they stand on a server, keyed by their changes_json names (the password is never among them).</summary>
    private static Dictionary<string, JsonNode?> Fields(ServerConnection s) => new(StringComparer.Ordinal)
    {
        ["display_name"] = s.DisplayName,
        ["host"] = s.ServerName,
        ["database"] = s.DatabaseName,
        ["read_only_intent"] = s.ReadOnlyIntent,
        ["auth"] = AuthWordOut(s.AuthenticationType),
        ["username"] = s.AuthenticationType == AuthenticationTypes.ServicePrincipal ? s.AzureClientId : null,
        ["encrypt_mode"] = ServerEntryRules.EncryptBoxIndexFor(s.EncryptMode) switch { 0 => "Optional", 2 => "Strict", _ => "Mandatory" },
        ["trust_server_certificate"] = s.TrustServerCertificate,
        ["multi_subnet_failover"] = s.MultiSubnetFailover,
        ["query_store_replica"] = string.IsNullOrWhiteSpace(s.QueryStoreReplica) ? null : s.QueryStoreReplica,
        ["monthly_cost_usd"] = s.MonthlyCostUsd,
        ["is_enabled"] = s.IsEnabled,
        ["is_favorite"] = s.IsFavorite,
        ["description"] = string.IsNullOrEmpty(s.Description) ? null : s.Description,
        ["utility_database"] = string.IsNullOrWhiteSpace(s.UtilityDatabase) ? null : s.UtilityDatabase,
        ["alert_delivery_mode"] = ModeOut(s.AlertDeliveryModeOverride),
        ["azure_tenant_id"] = string.IsNullOrWhiteSpace(s.AzureTenantId) ? null : s.AzureTenantId,
        ["managed_identity_client_id"] = string.IsNullOrWhiteSpace(s.ManagedIdentityClientId) ? null : s.ManagedIdentityClientId,
    };

    /// <summary>A copy of the settings an edit touches, so a save that throws can put them back on the live list entry.</summary>
    private static ServerConnection Copy(ServerConnection s) => new()
    {
        Id = s.Id,
        ServerName = s.ServerName,
        DisplayName = s.DisplayName,
        DatabaseName = s.DatabaseName,
        ReadOnlyIntent = s.ReadOnlyIntent,
        AuthenticationType = s.AuthenticationType,
        AzureClientId = s.AzureClientId,
        AzureTenantId = s.AzureTenantId,
        ManagedIdentityClientId = s.ManagedIdentityClientId,
        EncryptMode = s.EncryptMode,
        TrustServerCertificate = s.TrustServerCertificate,
        MultiSubnetFailover = s.MultiSubnetFailover,
        QueryStoreReplica = s.QueryStoreReplica,
        MonthlyCostUsd = s.MonthlyCostUsd,
        IsEnabled = s.IsEnabled,
        IsFavorite = s.IsFavorite,
        Description = s.Description,
        UtilityDatabase = s.UtilityDatabase,
        AlertDeliveryModeOverride = s.AlertDeliveryModeOverride,
        CredentialProfileId = s.CredentialProfileId,
    };

    private static void CopyOnto(ServerConnection target, ServerConnection s)
    {
        target.ServerName = s.ServerName;
        target.DisplayName = s.DisplayName;
        target.DatabaseName = s.DatabaseName;
        target.ReadOnlyIntent = s.ReadOnlyIntent;
        target.AuthenticationType = s.AuthenticationType;
        target.AzureClientId = s.AzureClientId;
        target.AzureTenantId = s.AzureTenantId;
        target.ManagedIdentityClientId = s.ManagedIdentityClientId;
        target.EncryptMode = s.EncryptMode;
        target.TrustServerCertificate = s.TrustServerCertificate;
        target.MultiSubnetFailover = s.MultiSubnetFailover;
        target.QueryStoreReplica = s.QueryStoreReplica;
        target.MonthlyCostUsd = s.MonthlyCostUsd;
        target.IsEnabled = s.IsEnabled;
        target.IsFavorite = s.IsFavorite;
        target.Description = s.Description;
        target.UtilityDatabase = s.UtilityDatabase;
        target.AlertDeliveryModeOverride = s.AlertDeliveryModeOverride;
    }

    private static string? Text(JsonElement value) => value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    private static string? NullIfBlank(string? text) => string.IsNullOrWhiteSpace(text) ? null : text.Trim();

    /// <summary>
    /// edit_server over an explicit connection test, so a test supplies one that touches no server. The test gets the settings
    /// to try, the user name and the password, and returns null when the server answered or the failure text.
    /// </summary>
    internal static async Task<string> EditAsync(
        ServerManager serverManager,
        string? serverName,
        string? changesJson,
        Func<ServerConnection, string?, string?, CancellationToken, Task<string?>> testConnection,
        CancellationToken cancellationToken,
        McpServerAdminHooks? hooks = null)
    {
        var (stored, findError) = FindForEdit(serverManager, serverName);
        if (stored is null)
        {
            return findError!;
        }

        if (string.IsNullOrWhiteSpace(changesJson))
        {
            return Refuse(EditTool, "invalid", "changes_json is required: a JSON object with only the fields to change.", "changes_json");
        }

        JsonElement root;
        try
        {
            using var doc = JsonDocument.Parse(changesJson);
            root = doc.RootElement.Clone();
        }
        catch (JsonException)
        {
            /* Not the parser's own text: it quotes the input, and the input can hold a password. */
            return Refuse(EditTool, "invalid", "changes_json is not valid JSON. It must be an object such as {\"display_name\":\"Orders\"}.", "changes_json");
        }

        if (root.ValueKind != JsonValueKind.Object)
        {
            return Refuse(EditTool, "invalid", "changes_json must be a JSON object such as {\"display_name\":\"Orders\"}.", "changes_json");
        }

        var given = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        foreach (var property in root.EnumerateObject())
        {
            var key = EditKeys.FirstOrDefault(k => k.Equals(property.Name, StringComparison.OrdinalIgnoreCase));
            if (key is null)
            {
                return Refuse(EditTool, "invalid",
                    $"'{property.Name}' is not a field edit_server changes. The keys are: {string.Join(", ", EditKeys)}. A server's id cannot be changed, and the excluded databases are set with set_server_excluded_databases.", property.Name);
            }

            given[key] = property.Value;
        }

        if (given.Count == 0)
        {
            return Refuse(EditTool, "invalid", "changes_json names no field to change.", "changes_json");
        }

        /* The server as it will be, built from the stored settings plus what the call names. Nothing touches the live entry before every rule has passed. */
        var next = Copy(stored);
        var useProfile = !string.IsNullOrEmpty(stored.CredentialProfileId);

        string BadType(string key, string wanted) => Refuse(EditTool, "invalid", $"{key} must be {wanted}.", key);

        foreach (var (key, value) in given)
        {
            var isNull = value.ValueKind == JsonValueKind.Null;
            switch (key)
            {
                case "display_name":
                    if (!isNull && value.ValueKind != JsonValueKind.String) return BadType(key, "text");
                    /* #9787: the one four-field rule every writer applies. A field the call does not name is not checked, so a row saved before the rule stays editable. */
                    if (ServerFieldText.Refusal("display_name", Text(value)) is { } displayRefusal)
                        return Refuse(EditTool, "invalid", displayRefusal, key);
                    next.DisplayName = NullIfBlank(Text(value)) ?? string.Empty;
                    break;
                case "host":
                    if (value.ValueKind != JsonValueKind.String || string.IsNullOrWhiteSpace(value.GetString()))
                        return Refuse(EditTool, "invalid", "host must be the server's address (text, not blank).", key);
                    if (ServerFieldText.Refusal("host", value.GetString()) is { } hostFieldRefusal)
                        return Refuse(EditTool, "invalid", hostFieldRefusal, key);
                    next.ServerName = value.GetString()!.Trim();
                    break;
                case "database":
                    if (!isNull && value.ValueKind != JsonValueKind.String) return BadType(key, "text, or null for none");
                    if (ServerFieldText.Refusal("database", Text(value)) is { } databaseRefusal)
                        return Refuse(EditTool, "invalid", databaseRefusal, key);
                    next.DatabaseName = NullIfBlank(Text(value));
                    break;
                case "read_only_intent":
                case "trust_server_certificate":
                case "multi_subnet_failover":
                case "is_enabled":
                case "is_favorite":
                    if (value.ValueKind is not (JsonValueKind.True or JsonValueKind.False)) return BadType(key, "true or false");
                    var flag = value.ValueKind == JsonValueKind.True;
                    switch (key)
                    {
                        case "read_only_intent": next.ReadOnlyIntent = flag; break;
                        case "trust_server_certificate": next.TrustServerCertificate = flag; break;
                        case "multi_subnet_failover": next.MultiSubnetFailover = flag; break;
                        case "is_enabled": next.IsEnabled = flag; break;
                        default: next.IsFavorite = flag; break;
                    }
                    break;
                case "auth":
                    if (value.ValueKind != JsonValueKind.String) return BadType(key, "text");
                    var auth = AddServerCommand.AuthWord(value.GetString()!);
                    if (auth.Length == 0)
                        return Refuse(EditTool, "invalid", AddServerCommand.AuthRefusal(value.GetString()!), key);
                    next.AuthenticationType = auth;
                    break;
                case "encrypt_mode":
                    if (value.ValueKind != JsonValueKind.String) return BadType(key, "\"Optional\", \"Mandatory\" or \"Strict\"");
                    var encrypt = AddServerCommand.EncryptWord(value.GetString()!);
                    if (encrypt.Length == 0)
                        return Refuse(EditTool, "invalid", AddServerCommand.EncryptRefusal(value.GetString()!), key);
                    next.EncryptMode = encrypt;
                    break;
                case "query_store_replica":
                    if (!isNull && value.ValueKind != JsonValueKind.String) return BadType(key, "text, or null to remove it");
                    next.QueryStoreReplica = NullIfBlank(Text(value));
                    break;
                case "monthly_cost_usd":
                    if (isNull)
                    {
                        next.MonthlyCostUsd = 0m;
                        break;
                    }

                    if (value.ValueKind != JsonValueKind.Number) return BadType(key, "a number, zero or more, or null to clear it");
                    var (cost, costError) = AddServerDialog.ParseMonthlyCostText(value.GetRawText());
                    if (costError is not null) return Refuse(EditTool, "invalid", costError, key);
                    next.MonthlyCostUsd = cost;
                    break;
                case "description":
                    if (!isNull && value.ValueKind != JsonValueKind.String) return BadType(key, "text");
                    /* The window's description box is one line, so a line break or tab is as unwelcome here as any other control character. */
                    if (Text(value) is { } descriptionText && descriptionText.Any(char.IsControl))
                        return Refuse(EditTool, "invalid", "description must not contain control characters (line breaks, tabs and the like).", key);
                    next.Description = (Text(value) ?? string.Empty).Trim();
                    break;
                case "utility_database":
                    if (!isNull && value.ValueKind != JsonValueKind.String) return BadType(key, "text, or null for none");
                    next.UtilityDatabase = NullIfBlank(Text(value));
                    break;
                case "alert_delivery_mode":
                    if (isNull)
                    {
                        next.AlertDeliveryModeOverride = null;
                        break;
                    }

                    var mode = (Text(value) ?? string.Empty).Trim().ToLowerInvariant();
                    if (mode is "summary") next.AlertDeliveryModeOverride = AlertNotificationMode.Summary;
                    else if (mode is "per_event" or "perevent") next.AlertDeliveryModeOverride = AlertNotificationMode.PerEvent;
                    else return BadType(key, "\"summary\", \"per_event\" or null");
                    break;
                case "azure_tenant_id":
                    if (!isNull && value.ValueKind != JsonValueKind.String) return BadType(key, "text, or null for none");
                    next.AzureTenantId = NullIfBlank(Text(value));
                    break;
                case "managed_identity_client_id":
                    if (!isNull && value.ValueKind != JsonValueKind.String) return BadType(key, "text, or null for none");
                    next.ManagedIdentityClientId = NullIfBlank(Text(value));
                    break;
                case "username":
                    if (!isNull && value.ValueKind != JsonValueKind.String) return BadType(key, "text");
                    if (ServerFieldText.Refusal("username", Text(value)) is { } usernameRefusal)
                        return Refuse(EditTool, "invalid", usernameRefusal, key);
                    break;
                case "password":
                    if (!isNull && value.ValueKind != JsonValueKind.String) return BadType(key, "text");
                    break;
            }
        }

        /* A blank display name falls back to the host, as the Edit Server window does on save. */
        if (given.ContainsKey("display_name") && string.IsNullOrEmpty(next.DisplayName))
        {
            next.DisplayName = next.ServerName;
        }

        var typedUsername = given.TryGetValue("username", out var u) ? NullIfBlank(Text(u)) : null;
        var typedPassword = given.TryGetValue("password", out var p) ? Text(p) : null;
        if (string.IsNullOrEmpty(typedPassword))
        {
            typedPassword = null;
        }

        /* The same host and replica rules the window applies: a host or replica that is kept is not held to them, a changed one is. */
        if (given.ContainsKey("host") && ServerManager.HostRefusal(next.ServerName, stored.ServerName) is { } hostRefusal)
        {
            return Refuse(EditTool, "invalid", hostRefusal, "host");
        }

        if (given.ContainsKey("query_store_replica") && ServerManager.ReplicaRefusal(next.QueryStoreReplica, stored.QueryStoreReplica ?? "") is { } replicaRefusal)
        {
            return Refuse(EditTool, "invalid", replicaRefusal, "query_store_replica");
        }

        /* Credentials are the window's own rule. A profile-backed server has no per-server credential to change here. */
        var credentialKeys = new[] { "auth", "username", "password", "azure_tenant_id", "managed_identity_client_id" };
        if (useProfile && credentialKeys.FirstOrDefault(given.ContainsKey) is { } profileKey)
        {
            return Refuse(EditTool, "invalid",
                $"This server signs in with a credential profile, so {profileKey} is not changed here. Change it in the Edit Server window.", profileKey);
        }

        var storedCredential = !useProfile && ServerCredentialIds.Allows(stored.Id, stored.DisplayName)
            ? serverManager.CredentialService.GetCredential(stored.Id)
            : null;
        string? username = null;
        string? password = null;
        if (!useProfile)
        {
            switch (next.AuthenticationType)
            {
                case AuthenticationTypes.SqlServer:
                    username = typedUsername ?? storedCredential?.Username;
                    password = typedPassword ?? storedCredential?.Password;
                    if (ServerEntryRules.CredentialRefusal(next.AuthenticationType, username, password) is { } sqlRefusal)
                        return Refuse(EditTool, "invalid", sqlRefusal, "username");
                    if (string.IsNullOrEmpty(password))
                        return Refuse(EditTool, "invalid", "password is required for SQL Server authentication: no password is stored for this server, so send one.", "password");
                    break;
                case AuthenticationTypes.ServicePrincipal:
                    username = typedUsername ?? (stored.AuthenticationType == AuthenticationTypes.ServicePrincipal ? NullIfBlank(stored.AzureClientId) : null) ?? storedCredential?.Username;
                    password = typedPassword ?? storedCredential?.Password;
                    if (ServerEntryRules.CredentialRefusal(next.AuthenticationType, username, password) is { } spRefusal)
                        return Refuse(EditTool, "invalid", spRefusal.StartsWith("Client secret", StringComparison.Ordinal) ? spRefusal + " Send password." : spRefusal + " Send username.", username is null ? "username" : "password");
                    next.AzureClientId = username;
                    break;
                case AuthenticationTypes.EntraMFA:
                    username = typedUsername ?? storedCredential?.Username;
                    break;
            }

            if (next.AuthenticationType != AuthenticationTypes.ServicePrincipal)
            {
                next.AzureClientId = null;
                next.AzureTenantId = null;
            }

            if (next.AuthenticationType != AuthenticationTypes.ManagedIdentity)
            {
                next.ManagedIdentityClientId = null;
            }

            if (typedPassword is not null && next.AuthenticationType is not (AuthenticationTypes.SqlServer or AuthenticationTypes.ServicePrincipal))
            {
                return Refuse(EditTool, "invalid", $"password does not apply to {AuthWordOut(next.AuthenticationType)} authentication. Nothing was changed.", "password");
            }
        }

        /* The id every collected row is stored under comes from the host, database and read-only intent: another server may hold the one this edit would make. */
        var candidate = new ServerConnection { ServerName = next.ServerName, DatabaseName = next.DatabaseName, ReadOnlyIntent = next.ReadOnlyIntent, Id = stored.Id };
        if (serverManager.FindServerIdHolderForEdit(stored, candidate) is { } holder)
        {
            return Refuse(EditTool, "collides", ServerManager.DescribeIdHolder(candidate, holder, isEdit: true), "host");
        }

        var baseline = Fields(stored);
        var before = Fields(stored);
        var after = Fields(next);
        /* The user name of a SQL or Entra MFA login lives in the credential store, and a service principal's client id is the user name too. */
        before["username"] = stored.AuthenticationType == AuthenticationTypes.ServicePrincipal ? NullIfBlank(stored.AzureClientId) : storedCredential?.Username;
        after["username"] = next.AuthenticationType is AuthenticationTypes.SqlServer or AuthenticationTypes.ServicePrincipal or AuthenticationTypes.EntraMFA ? username : null;
        var changed = EditKeys
            .Where(k => before.ContainsKey(k) && !JsonNode.DeepEquals(before[k], after[k]))
            .ToList();
        /* A typed password is always a change: the call never compares it with the stored one, so the answer does not depend on what is stored. */
        var passwordChanged = typedPassword is not null;
        if (changed.Count == 0 && !passwordChanged)
        {
            return new JsonObject
            {
                ["status"] = "unchanged",
                ["tool"] = EditTool,
                ["server"] = stored.DisplayName,
                ["message"] = "The request equals the stored values, so nothing was written.",
            }.ToJsonString();
        }

        /* #9640: Darling's rule. An edit that changes where or how the server is reached must not leave Windows authentication over a connection
           nothing verifies, unless the operator allowed it. It runs before the question and the connection test, so Windows credentials never
           reach an unverified host. An edit of other fields leaves a server that already has this posture as it is. */
        if (changed.Any(k => k is "host" or "auth" or "trust_server_certificate" or "encrypt_mode")
            && McpUnverifiedWindowsAuthScreen.Refusal(
                hooks?.AllowWindowsAuthOverUnverifiedTls ?? false, next.AuthenticationType, next.EncryptMode, next.TrustServerCertificate, "Not saved") is { } unverifiedRefusal)
        {
            return Refuse(EditTool, "invalid", unverifiedRefusal + " Nothing was saved.");
        }

        /* The window asks before it connects or saves with settings this Windows user has not confirmed, unless the user typed the secret. A tool call
           cannot answer that question, so it is refused with the window's own question, and the user confirms it in the window. */
        var typedSecret = typedPassword is not null
            && next.AuthenticationType is AuthenticationTypes.SqlServer or AuthenticationTypes.ServicePrincipal;
        var question = HostConfirmationGate.Question(
            serverManager.HostChangeIfSaved(Copy(next)), isEdit: true,
            HostConfirmationGate.CredentialWords(next.AuthenticationType, null), typedSecret);
        if (question is not null)
        {
            return Refuse(EditTool, "refused",
                "Nothing was saved. These settings are not the ones this Windows user confirmed for the server, and the Edit Server window asks about that before it connects or saves; a tool call cannot answer. "
                + question + " Confirm it in the Edit Server window, or send password with the change where the server has one.");
        }

        var interactive = next.AuthenticationType is AuthenticationTypes.EntraMFA or AuthenticationTypes.EntraDeviceCode;
        var connectionKeys = new[] { "host", "database", "read_only_intent", "auth", "username", "password", "encrypt_mode", "trust_server_certificate", "multi_subnet_failover", "azure_tenant_id", "managed_identity_client_id", "is_enabled" };
        var reachChanged = passwordChanged || changed.Any(k => connectionKeys.Contains(k));
        var tested = false;
        if (next.IsEnabled && reachChanged && !useProfile)
        {
            if (interactive)
            {
                return Refuse(EditTool, "refused",
                    "This server signs in with Microsoft Entra MFA or device code, which needs a person to sign in, so a connection change cannot be tried here. Nothing was saved. Edit it in the Edit Server window.");
            }

            var failure = await testConnection(next, username, password, cancellationToken);
            tested = true;
            if (failure is not null)
            {
                return Refuse(EditTool, "connection_failed",
                    $"Could not connect to {next.ServerName} with the new settings, so nothing was saved: {failure} To save without a working connection, set is_enabled to false in the same change.");
            }
        }

        /* #9640: the live server's fields change in place, so the change and its save run where the windows read and save them. */
        var changedWhileRunning = false;
        var written = stored;
        McpServerAdminHooks.Save(hooks, () =>
        {
            /* The connection test above can take a while, and a window Save, a second edit or an add_servers update may land meanwhile.
               Writing the copy made before it would undo that change, so a server that no longer reads as it did is left alone. The
               comparison is against what the list holds now: add_servers swaps a copy into the list, after which the instance read at
               the start is detached and never sees a later change. A server removed meanwhile is a change too. */
            var live = serverManager.GetServerById(stored.Id);
            if (live is null)
            {
                changedWhileRunning = true;
                return;
            }

            var now = Fields(live);
            if (baseline.Any(kv => !JsonNode.DeepEquals(kv.Value, now[kv.Key])))
            {
                changedWhileRunning = true;
                return;
            }

            written = live;
            var snapshot = Copy(live);
            CopyOnto(live, next);
            try
            {
                ServerEditSave.Persist(serverManager, live, useProfile, username, password);
            }
            catch
            {
                CopyOnto(live, snapshot);
                throw;
            }
        });

        if (changedWhileRunning)
        {
            return Refuse(EditTool, "refused", "Nothing was saved. The server changed while the call ran, so the edit was not applied over that change. Read the server again and call again.");
        }

        var previous = new JsonObject();
        var undoChanges = new JsonObject();
        foreach (var key in changed)
        {
            previous[key] = before[key]?.DeepClone();
            undoChanges[key] = before[key]?.DeepClone();
        }

        var undoNote = passwordChanged
            ? "The password cannot be read back, so undo does not carry it: add password to the call if the earlier password is needed."
            : null;
        var current = new JsonObject();
        foreach (var key in changed)
        {
            current[key] = after[key]?.DeepClone();
        }

        var storage = RemoteCollectorService.GetServerNameForStorage(written);
        AppLogger.Info("MCP", $"{EditTool} saved server '{storage}': changed {string.Join(", ", passwordChanged ? changed.Append("password") : changed)}");
        return new JsonObject
        {
            ["status"] = "updated",
            ["tool"] = EditTool,
            ["server"] = storage,
            ["display_name"] = written.DisplayName,
            ["changed"] = new JsonArray(changed.Select(k => (JsonNode?)JsonValue.Create(k)).ToArray()),
            ["password_changed"] = passwordChanged,
            ["tested"] = tested,
            ["previous"] = previous,
            ["current"] = current,
            ["undo"] = undoChanges.Count == 0
                ? null
                : new JsonObject
                {
                    ["tool"] = EditTool,
                    ["arguments"] = new JsonObject { ["server_name"] = UndoServerName(serverManager, storage), ["changes_json"] = undoChanges.ToJsonString() },
                },
            ["undo_note"] = undoNote,
            ["note"] = useProfile && reachChanged && next.IsEnabled
                ? "This server signs in with a credential profile, so the new settings were not tried first."
                : null,
        }.ToJsonString();
    }

    /// <summary>set_server_excluded_databases over a server manager, so a test runs it with a throwaway one.</summary>
    internal static string SetExcluded(ServerManager serverManager, string? serverName, string[]? databases, McpServerAdminHooks? hooks = null)
    {
        if (databases is null)
        {
            return Refuse(ExcludedTool, "invalid", "databases is required: send the whole list of database names to exclude, or an empty list to clear it. Nothing was changed.", "databases");
        }

        if (ExcludedDatabaseNames.Refusal(databases, "databases") is { } refusal)
        {
            return Refuse(ExcludedTool, "invalid", refusal + " Nothing was changed.", "databases");
        }

        var (server, error) = Find(serverManager, ExcludedTool, serverName);
        if (server is null)
        {
            return error!;
        }

        var wanted = databases
            .Where(n => !string.IsNullOrWhiteSpace(n))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        var previous = (server.ExcludedDatabases ?? new List<string>()).ToList();
        McpServerAdminHooks.Save(hooks, () => ExcludedDatabasesDialog.PersistSelection(serverManager, server, wanted, xeSessionsMode: false));

        var after = server.ExcludedDatabases ?? new List<string>();
        var storage = RemoteCollectorService.GetServerNameForStorage(server);
        var listChanged = !previous.SequenceEqual(after, StringComparer.Ordinal);
        if (listChanged)
        {
            AppLogger.Info("MCP", $"{ExcludedTool} saved server '{storage}': changed excluded_databases");
        }

        var name = string.IsNullOrWhiteSpace(server.DisplayName) ? server.ServerName : server.DisplayName;
        JsonArray Strings(IEnumerable<string> names) => new(names.Select(n => (JsonNode?)JsonValue.Create(n)).ToArray());
        return new JsonObject
        {
            ["status"] = listChanged ? "changed" : "unchanged",
            ["tool"] = ExcludedTool,
            ["server"] = name,
            ["storage_name"] = storage,
            ["what_changed"] = after.Count == 0
                ? $"{name} has no excluded databases now; every database is collected."
                : $"{name} now excludes {CountText.Plural(after.Count, "database")}: {string.Join(", ", after)}.",
            ["previous"] = new JsonObject { ["excluded_databases"] = Strings(previous) },
            ["current"] = new JsonObject { ["excluded_databases"] = Strings(after) },
            ["undo"] = new JsonObject
            {
                ["tool"] = ExcludedTool,
                ["arguments"] = new JsonObject { ["server_name"] = storage, ["databases"] = Strings(previous) },
            },
            ["takes_effect"] = "Collection reads the list on its next pass. Data already collected stays.",
        }.ToJsonString();
    }
}
